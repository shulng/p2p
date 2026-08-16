"""P2P 节点 - 整合 ICE、QUIC、KCP、信令"""
from __future__ import annotations

import asyncio
import struct
import pickle
import time
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional, Tuple, Any
from loguru import logger

from .config import (
    P2PConfig,
    TransportProtocol,
    ConnectionRole,
)
from .types import (
    ConnectionState,
    Message,
    MessageType,
    PeerInfo,
    SessionDescription,
    IceCandidate,
    ConnectionStats,
    generate_peer_id,
)
from .ice_manager import IceManager
from .kcp_transport import KCPTransport
from .quic_transport import QUICTransport
from .signaling_client import SignalingClient, SignalingEvents


class P2PNode:
    """P2P 节点 - 整合所有模块"""

    def __init__(
        self,
        config: P2PConfig,
        on_message: Optional[Callable[[Message], None]] = None,
        on_peer_connected: Optional[Callable[[PeerInfo], None]] = None,
        on_peer_disconnected: Optional[Callable[[str], None]] = None,
        on_state_changed: Optional[Callable[[str, ConnectionState], None]] = None,
    ):
        self.config = config
        self.peer_id: str = generate_peer_id()
        self.config.signaling.peer_id = self.peer_id
        
        # 回调
        self.on_message = on_message
        self.on_peer_connected = on_peer_connected
        self.on_peer_disconnected = on_peer_disconnected
        self.on_state_changed = on_state_changed
        
        # 状态
        self._running: bool = False
        self.state: ConnectionState = ConnectionState.DISCONNECTED
        
        # 模块
        self._signaling: Optional[SignalingClient] = None
        self._ice: Optional[IceManager] = None
        self._quic: Optional[QUICTransport] = None
        self._kcp: Optional[KCPTransport] = None
        
        # 连接的 Peer: peer_id -> {transport, state, ...}
        self._peers: Dict[str, Dict[str, Any]] = {}
        
        # 主动连接用
        self._target_peer_id: Optional[str] = None
        self._negotiation_lock: asyncio.Lock = asyncio.Lock()
        
        # 本地地址
        self._kcp_local_addr: Optional[Tuple[str, int]] = None
        self._quic_local_addr: Optional[Tuple[str, int]] = None

    def _set_state(self, state: ConnectionState) -> None:
        if self.state != state:
            old = self.state
            self.state = state
            logger.info(f"[P2PNode {self.peer_id}] State: {old} -> {state}")
            if self.on_state_changed:
                self.on_state_changed(self.peer_id, state)

    async def initialize(self) -> None:
        """初始化节点"""
        logger.info(f"[P2PNode] Initializing {self.peer_id}, transport={self.config.transport.value}")

        # 1. 初始化 ICE (使用 DataChannel 进行数据传输，支持 TURN 中继)
        self._ice = IceManager(
            config=self.config.ice,
            on_ice_candidate=self._on_ice_candidate,
            on_connection_state=self._on_ice_state,
            on_ice_gathering_done=self._on_ice_gathering_done,
            on_remote_address=self._on_ice_remote_addr,
        )
        self._ice.on_data_received = self._on_ice_data
        logger.info("[P2PNode] ICE manager initialized (using Cloudflare TURN)")
        
        # 4. 初始化信令
        events = SignalingEvents(
            on_offer=self._signal_on_offer,
            on_answer=self._signal_on_answer,
            on_ice_candidate=self._signal_on_ice_candidate,
            on_peer_joined=self._signal_on_peer_joined,
            on_peer_left=self._signal_on_peer_left,
            on_room_info=self._signal_on_room_info,
            on_connected=self._signal_on_connected,
            on_disconnected=self._signal_on_disconnected,
        )
        self._signaling = SignalingClient(self.config.signaling, events)
        
        self._running = True
        self._set_state(ConnectionState.CONNECTING)
        logger.info("[P2PNode] Node initialized")

    async def connect_to_signaling(self) -> bool:
        """连接到信令服务器"""
        if not self._signaling:
            return False
        return await self._signaling.connect()

    async def join_room(
        self,
        room_id: str,
        role: Optional[ConnectionRole] = None,
    ) -> bool:
        """加入房间"""
        if role:
            self.config.role = role
        return await self._signaling.join_room(room_id, self.config.role)

    async def connect_to_peer(self, target_peer_id: str) -> bool:
        """
        连接到指定 Peer (作为发起方)
        """
        if not self._signaling or not self._signaling.is_connected:
            logger.error("[P2PNode] Not connected to signaling server")
            return False
        
        async with self._negotiation_lock:
            self._target_peer_id = target_peer_id
            self.config.role = ConnectionRole.INITIATOR
            
            logger.info(f"[P2PNode] Connecting to peer {target_peer_id}...")
            
            # 1. 创建 SDP Offer
            offer = await self._ice.create_offer()
            
            # 2. 通过信令发送 Offer
            await self._signaling.send_offer(target_peer_id, offer)
            
            logger.info(f"[P2PNode] Offer sent to {target_peer_id}, waiting for answer...")
            
            # 3. 等待 Answer (通过信令回调中的 Event 来通知)
            # 在 _signal_on_answer 中处理
            answer_event = asyncio.Event()
            self._wait_answer_event = (answer_event, target_peer_id)
            
            try:
                await asyncio.wait_for(answer_event.wait(), timeout=30.0)
            except asyncio.TimeoutError:
                logger.warning("[P2PNode] Timed out waiting for answer")
                return False
            
            # 4. 等待 ICE 连接
            logger.info("[P2PNode] Waiting for ICE connection...")
            ice_ok = await self._ice.wait_for_connection(timeout=30.0)
            
            if ice_ok:
                logger.info("[P2PNode] ICE connection established")
                await self._establish_transports(target_peer_id)
                return True
            else:
                logger.warning("[P2PNode] ICE connection failed")
                # 尝试使用回退方式
                return False

    async def _signal_on_offer(self, from_peer_id: str, offer: SessionDescription) -> None:
        """收到 Offer (作为响应方)"""
        logger.info(f"[P2PNode] Received offer from {from_peer_id}")
        
        async with self._negotiation_lock:
            self._target_peer_id = from_peer_id
            self.config.role = ConnectionRole.RESPONDER
            
            # 创建 Answer
            answer = await self._ice.create_answer(offer)
            
            # 发送 Answer
            await self._signaling.send_answer(from_peer_id, answer)
            
            logger.info(f"[P2PNode] Answer sent to {from_peer_id}")
            
            # 等待 ICE 连接
            ice_ok = await self._ice.wait_for_connection(timeout=30.0)
            if ice_ok:
                logger.info("[P2PNode] ICE connection established (responder)")
                await self._establish_transports(from_peer_id)

    async def _signal_on_answer(self, from_peer_id: str, answer: SessionDescription) -> None:
        """收到 Answer"""
        logger.info(f"[P2PNode] Received answer from {from_peer_id}")
        await self._ice.set_remote_description(answer)
        
        # 通知等待的协程
        if hasattr(self, "_wait_answer_event"):
            event, expected_peer = self._wait_answer_event
            if from_peer_id == expected_peer:
                event.set()
                delattr(self, "_wait_answer_event")

    async def _signal_on_ice_candidate(self, from_peer_id: str, candidate: IceCandidate) -> None:
        """收到远端 ICE 候选"""
        logger.debug(f"[P2PNode] ICE candidate from {from_peer_id}: {candidate.candidate[:50]}...")
        await self._ice.add_ice_candidate(candidate)

    def _on_ice_candidate(self, candidate: IceCandidate) -> None:
        """本地产生 ICE 候选 - 通过信令发送"""
        if self._signaling and self._target_peer_id:
            asyncio.create_task(
                self._signaling.send_ice_candidate(self._target_peer_id, candidate)
            )

    def _on_ice_state(self, state: ConnectionState) -> None:
        """ICE 状态变化"""
        logger.info(f"[P2PNode] ICE state: {state}, ice_state={self._ice.ice_state}")

    def _on_ice_gathering_done(self) -> None:
        """ICE 候选收集完成"""
        logger.info(
            f"[P2PNode] ICE gathering done. "
            f"Host={len(self._ice.get_host_candidates())}, "
            f"SRFLX={len(self._ice.get_server_reflexive_candidates())}, "
            f"Relay(TURN)={len(self._ice.get_relay_candidates())}"
        )
        
        # 输出 Cloudflare TURN 的中继候选
        relay_candidates = self._ice.get_relay_candidates()
        if relay_candidates:
            logger.info("[P2PNode] Cloudflare TURN relay candidates available:")
            for i, c in enumerate(relay_candidates):
                logger.info(f"  [{i}] {c.candidate[:100]}")

    def _on_ice_remote_addr(self, addr: Tuple[str, int]) -> None:
        """ICE 获取到远端地址"""
        logger.info(f"[P2PNode] ICE remote address: {addr}")

    async def _establish_transports(self, peer_id: str) -> None:
        """在 ICE 连接成功后建立传输层 (使用 DataChannel)"""
        remote_addr = self._ice.selected_address

        logger.info(f"[P2PNode] Establishing DataChannel with {peer_id}")

        # 等待 DataChannel 打开
        dc_ok = await self._ice.wait_for_data_channel(timeout=15.0)
        if not dc_ok:
            logger.warning("[P2PNode] DataChannel failed to open")
            return

        peer_data = {
            "peer_id": peer_id,
            "state": ConnectionState.CONNECTED,
            "ice_addr": remote_addr,
            "transport": "datachannel",
        }

        self._peers[peer_id] = peer_data
        self._set_state(ConnectionState.CONNECTED)

        remote_ip = remote_addr[0] if remote_addr else "unknown"
        remote_port = remote_addr[1] if remote_addr else 0

        peer_info = PeerInfo(
            peer_id=peer_id,
            role=ConnectionRole.RESPONDER
            if self.config.role == ConnectionRole.INITIATOR
            else ConnectionRole.INITIATOR,
            address=remote_ip,
            port=remote_port,
            transport=self.config.transport,
        )
        
        if self.on_peer_connected:
            self.on_peer_connected(peer_info)
        
        logger.info(f"[P2PNode] Successfully connected to peer {peer_id}")

    async def _signal_on_peer_joined(self, peer: PeerInfo) -> None:
        """有新 Peer 加入房间"""
        logger.info(f"[P2PNode] Peer joined: {peer.peer_id} (role={peer.role})")
        
        # 如果是发起方，且对方是响应方，自动发起连接
        if (
            self.config.role == ConnectionRole.INITIATOR
            and peer.role == ConnectionRole.RESPONDER
            and peer.peer_id not in self._peers
        ):
            logger.info(f"[P2PNode] Auto-connecting to responder {peer.peer_id}")
            asyncio.create_task(self.connect_to_peer(peer.peer_id))

    async def _signal_on_peer_left(self, peer_id: str) -> None:
        """Peer 离开"""
        logger.info(f"[P2PNode] Peer left: {peer_id}")
        if peer_id in self._peers:
            del self._peers[peer_id]
            if self.on_peer_disconnected:
                self.on_peer_disconnected(peer_id)

    def _signal_on_room_info(self, peers: List[PeerInfo]) -> None:
        """房间信息更新"""
        logger.info(
            f"[P2PNode] Room has {len(peers)} peers: "
            f"{[p.peer_id for p in peers]}"
        )

    def _signal_on_connected(self) -> None:
        """信令连接成功"""
        logger.info("[P2PNode] Signaling connected")
        self._set_state(ConnectionState.CONNECTING)

    def _signal_on_disconnected(self) -> None:
        """信令断开"""
        logger.warning("[P2PNode] Signaling disconnected")

    def _on_kcp_data(self, data: bytes) -> None:
        """收到 KCP 数据"""
        self._handle_transport_data(data, TransportProtocol.KCP)

    def _on_kcp_state(self, state: ConnectionState) -> None:
        """KCP 状态变化"""
        logger.debug(f"[P2PNode] KCP state: {state}")

    def _on_quic_data(self, data: bytes) -> None:
        """收到 QUIC 数据"""
        self._handle_transport_data(data, TransportProtocol.QUIC)

    def _on_quic_state(self, state: ConnectionState) -> None:
        """QUIC 状态变化"""
        logger.debug(f"[P2PNode] QUIC state: {state}")

    def _on_ice_data(self, data: bytes) -> None:
        """收到 DataChannel 数据"""
        self._handle_transport_data(data, self.config.transport)

    def _handle_transport_data(self, data: bytes, transport: TransportProtocol) -> None:
        """处理从传输层收到的数据"""
        try:
            msg = self._decode_message(data)
            logger.debug(
                f"[P2PNode] Received {msg.msg_type.value} via {transport.value} "
                f"from {msg.sender_id}"
            )
            if self.on_message:
                self.on_message(msg)
        except Exception as e:
            # 如果不是 Message 格式，当作原始二进制数据
            msg = Message.create(
                msg_type=MessageType.DATA_BINARY,
                sender_id="unknown",
                receiver_id=self.peer_id,
                payload=data,
            )
            if self.on_message:
                self.on_message(msg)

    def _encode_message(self, msg: Message) -> bytes:
        """编码消息为二进制"""
        header = struct.pack(
            "!BQQ",  # 先尝试简单编码
            0,  # 版本
            0,  # 预留
            len(msg.payload) if isinstance(msg.payload, bytes) else 0,
        )
        # 使用 pickle 简化实现
        return pickle.dumps({
            "msg_id": msg.msg_id,
            "msg_type": msg.msg_type.value,
            "sender_id": msg.sender_id,
            "receiver_id": msg.receiver_id,
            "payload": msg.payload,
            "timestamp": msg.timestamp.isoformat(),
            "seq": msg.seq,
        })

    def _decode_message(self, data: bytes) -> Message:
        """从二进制解码消息"""
        obj = pickle.loads(data)
        return Message(
            msg_id=obj["msg_id"],
            msg_type=MessageType(obj["msg_type"]),
            sender_id=obj["sender_id"],
            receiver_id=obj["receiver_id"],
            payload=obj["payload"],
            seq=obj["seq"],
        )

    async def send_to_peer(
        self,
        peer_id: str,
        msg_type: MessageType,
        payload: Any = None,
        prefer_transport: Optional[TransportProtocol] = None,
    ) -> bool:
        """发送消息到指定 Peer (通过 ICE DataChannel)"""
        if peer_id not in self._peers:
            logger.warning(f"[P2PNode] Unknown peer: {peer_id}")
            return False

        msg = Message.create(
            msg_type=msg_type,
            sender_id=self.peer_id,
            receiver_id=peer_id,
            payload=payload,
        )
        data = self._encode_message(msg)

        # 通过 ICE DataChannel 发送
        if self._ice:
            return await self._ice.send_data(data)

        logger.warning(f"[P2PNode] No transport available for {peer_id}")
        return False

    async def send_text(self, peer_id: str, text: str) -> bool:
        """发送文本消息"""
        return await self.send_to_peer(peer_id, MessageType.DATA_TEXT, text)

    async def send_json(self, peer_id: str, obj: Any) -> bool:
        """发送 JSON 数据"""
        return await self.send_to_peer(peer_id, MessageType.DATA_JSON, obj)

    async def send_bytes(self, peer_id: str, data: bytes) -> bool:
        """发送二进制数据"""
        return await self.send_to_peer(peer_id, MessageType.DATA_BINARY, data)

    def get_connected_peers(self) -> List[str]:
        """获取已连接的 Peer ID 列表"""
        return [pid for pid, data in self._peers.items()
                if data["state"] == ConnectionState.CONNECTED]

    def get_connection_stats(self, peer_id: Optional[str] = None) -> Dict[str, Any]:
        """获取连接统计"""
        if peer_id:
            if peer_id not in self._peers:
                return {}
            peers_to_check = {peer_id: self._peers[peer_id]}
        else:
            peers_to_check = self._peers
        
        result = {}
        for pid, data in peers_to_check.items():
            info = {
                "state": data["state"].value,
                "ice_addr": data.get("ice_addr"),
                "transport": self.config.transport.value,
            }
            if data.get("kcp") and hasattr(data["kcp"], "get_stats"):
                info["kcp_stats"] = data["kcp"].get_stats()
            if data.get("quic") and hasattr(data["quic"], "get_stats"):
                info["quic_stats"] = data["quic"].get_stats()
            result[pid] = info
        
        # ICE 统计
        if self._ice:
            result["_ice"] = {
                "state": self._ice.ice_state,
                "gathering_done": self._ice.gathering_done,
                "local_candidates": len(self._ice.local_candidates),
                "remote_candidates": len(self._ice.remote_candidates),
                "selected_address": self._ice.selected_address,
            }
        
        return result

    async def close(self) -> None:
        """关闭节点"""
        logger.info(f"[P2PNode] Closing node {self.peer_id}")
        self._running = False
        self._set_state(ConnectionState.CLOSED)
        
        # 关闭传输层
        for pid, data in list(self._peers.items()):
            if data.get("kcp") and data["kcp"] is not self._kcp:
                try:
                    await data["kcp"].close()
                except Exception:
                    pass
        
        self._peers.clear()
        
        if self._kcp:
            await self._kcp.close()
        if self._quic:
            await self._quic.close()
        if self._ice:
            await self._ice.close()
        if self._signaling:
            await self._signaling.close()
        
        logger.info(f"[P2PNode] Node {self.peer_id} closed")
