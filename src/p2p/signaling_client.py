"""信令客户端 - 与信令服务器通信"""
from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional, Any
from loguru import logger

try:
    import websockets
    WEBSOCKETS_AVAILABLE = True
except ImportError:
    WEBSOCKETS_AVAILABLE = False

from .config import SignalingConfig, ConnectionRole
from .types import (
    MessageType,
    SessionDescription,
    IceCandidate,
    generate_peer_id,
    PeerInfo,
)


@dataclass
class SignalingEvents:
    """信令事件回调"""
    on_offer: Optional[Callable[[str, SessionDescription], None]] = None
    on_answer: Optional[Callable[[str, SessionDescription], None]] = None
    on_ice_candidate: Optional[Callable[[str, IceCandidate], None]] = None
    on_peer_joined: Optional[Callable[[PeerInfo], None]] = None
    on_peer_left: Optional[Callable[[str], None]] = None
    on_room_info: Optional[Callable[[List[PeerInfo]], None]] = None
    on_connected: Optional[Callable[[], None]] = None
    on_disconnected: Optional[Callable[[], None]] = None


class SignalingClient:
    """WebSocket 信令客户端"""

    def __init__(self, config: SignalingConfig, events: SignalingEvents):
        if not WEBSOCKETS_AVAILABLE:
            raise RuntimeError("websockets not installed")
        
        self.config = config
        self.events = events
        
        self.peer_id: str = config.peer_id or generate_peer_id()
        self._ws = None
        self._connected: bool = False
        self._running: bool = False
        self._recv_task: Optional[asyncio.Task] = None
        self._reconnect_attempts: int = 0
        
        # 房间中的 Peer 列表
        self.room_peers: List[PeerInfo] = []
        
        self._connected_event: asyncio.Event = asyncio.Event()
        self._join_event: asyncio.Event = asyncio.Event()

    @property
    def is_connected(self) -> bool:
        return self._connected

    async def connect(self) -> bool:
        """连接到信令服务器"""
        try:
            logger.info(f"[SignalingClient] Connecting to {self.config.server_url}")
            self._ws = await websockets.connect(
                self.config.server_url,
                ping_interval=20,
                ping_timeout=10,
            )
            
            self._connected = True
            self._running = True
            self._reconnect_attempts = 0
            self._connected_event.set()
            
            logger.info(f"[SignalingClient] Connected as {self.peer_id}")
            
            if self.events.on_connected:
                self.events.on_connected()
            
            # 启动接收循环
            self._recv_task = asyncio.create_task(self._recv_loop())
            
            return True
            
        except Exception as e:
            logger.error(f"[SignalingClient] Connect failed: {e}")
            await self._schedule_reconnect()
            return False

    async def join_room(
        self,
        room_id: str,
        role: ConnectionRole = ConnectionRole.INITIATOR,
    ) -> bool:
        """加入房间"""
        self.config.room_id = room_id
        
        try:
            await self._send({
                "type": MessageType.SIGNAL_JOIN.value,
                "peer_id": self.peer_id,
                "room_id": room_id,
                "role": role.value,
            })
            
            # 等待 JOIN 确认
            try:
                await asyncio.wait_for(self._join_event.wait(), timeout=10.0)
                logger.info(f"[SignalingClient] Joined room {room_id} as {role}")
                return True
            except asyncio.TimeoutError:
                logger.warning("[SignalingClient] Join room timeout")
                return False
                
        except Exception as e:
            logger.error(f"[SignalingClient] Join room error: {e}")
            return False

    async def _send(self, msg: dict) -> None:
        """发送消息到服务器"""
        if not self._ws or not self._connected:
            raise RuntimeError("Not connected to signaling server")
        
        await self._ws.send(json.dumps(msg))

    async def send_offer(self, to_peer_id: str, offer: SessionDescription) -> None:
        """发送 SDP Offer"""
        await self._send({
            "type": MessageType.SIGNAL_OFFER.value,
            "to": to_peer_id,
            "from": self.peer_id,
            "sdp_type": offer.sdp_type,
            "sdp": offer.sdp,
        })
        logger.info(f"[SignalingClient] Sent offer to {to_peer_id}")

    async def send_answer(self, to_peer_id: str, answer: SessionDescription) -> None:
        """发送 SDP Answer"""
        await self._send({
            "type": MessageType.SIGNAL_ANSWER.value,
            "to": to_peer_id,
            "from": self.peer_id,
            "sdp_type": answer.sdp_type,
            "sdp": answer.sdp,
        })
        logger.info(f"[SignalingClient] Sent answer to {to_peer_id}")

    async def send_ice_candidate(self, to_peer_id: str, candidate: IceCandidate) -> None:
        """发送 ICE 候选"""
        await self._send({
            "type": MessageType.SIGNAL_ICE_CANDIDATE.value,
            "to": to_peer_id,
            "from": self.peer_id,
            "candidate": candidate.candidate,
            "sdp_mid": candidate.sdp_mid,
            "sdp_mline_index": candidate.sdp_mline_index,
        })

    async def _recv_loop(self) -> None:
        """接收消息循环"""
        try:
            while self._running and self._ws:
                try:
                    raw_msg = await self._ws.recv()
                    msg = json.loads(raw_msg)
                    await self._handle_message(msg)
                except websockets.exceptions.ConnectionClosed:
                    logger.warning("[SignalingClient] Server closed connection")
                    break
                except json.JSONDecodeError:
                    logger.warning("[SignalingClient] Received invalid JSON")
                except Exception as e:
                    logger.error(f"[SignalingClient] Recv error: {e}")
                    
        finally:
            self._connected = False
            self._connected_event.clear()
            
            if self.events.on_disconnected:
                self.events.on_disconnected()
            
            # 尝试重连
            if self._running:
                await self._schedule_reconnect()

    async def _handle_message(self, msg: dict) -> None:
        """处理接收到的消息"""
        msg_type = msg.get("type", "")
        from_peer = msg.get("from")
        
        if msg_type == MessageType.SIGNAL_JOIN.value:
            # JOIN 确认
            if msg.get("success"):
                self._join_event.set()
                
        elif msg_type == MessageType.SIGNAL_OFFER.value:
            # 收到 Offer
            if from_peer and self.events.on_offer:
                offer = SessionDescription(
                    sdp_type=msg.get("sdp_type", "offer"),
                    sdp=msg.get("sdp", ""),
                )
                self.events.on_offer(from_peer, offer)
                
        elif msg_type == MessageType.SIGNAL_ANSWER.value:
            # 收到 Answer
            if from_peer and self.events.on_answer:
                answer = SessionDescription(
                    sdp_type=msg.get("sdp_type", "answer"),
                    sdp=msg.get("sdp", ""),
                )
                self.events.on_answer(from_peer, answer)
                
        elif msg_type == MessageType.SIGNAL_ICE_CANDIDATE.value:
            # 收到 ICE 候选
            if from_peer and self.events.on_ice_candidate:
                candidate = IceCandidate(
                    candidate=msg.get("candidate", ""),
                    sdp_mid=msg.get("sdp_mid"),
                    sdp_mline_index=msg.get("sdp_mline_index"),
                )
                self.events.on_ice_candidate(from_peer, candidate)
                
        elif msg_type == MessageType.SIGNAL_ROOM_INFO.value:
            # 房间信息更新
            peers_data = msg.get("peers", [])
            peers = []
            for p in peers_data:
                try:
                    role = ConnectionRole(p.get("role")) if p.get("role") else None
                except ValueError:
                    role = None
                peers.append(PeerInfo(
                    peer_id=p["peer_id"],
                    role=role or ConnectionRole.INITIATOR,
                ))
            
            # 检测新加入/离开的 Peer
            old_ids = {p.peer_id for p in self.room_peers}
            new_ids = {p.peer_id for p in peers}
            
            joined = new_ids - old_ids
            left = old_ids - new_ids
            
            for p in peers:
                if p.peer_id in joined and self.events.on_peer_joined:
                    if p.peer_id != self.peer_id:
                        self.events.on_peer_joined(p)
            
            for pid in left:
                if self.events.on_peer_left:
                    self.events.on_peer_left(pid)
            
            self.room_peers = peers
            
            if self.events.on_room_info:
                self.events.on_room_info(peers)
                
        elif msg_type == MessageType.CTRL_ERROR.value:
            logger.warning(f"[SignalingClient] Server error: {msg.get('error')}")

    async def _schedule_reconnect(self) -> None:
        """调度重连"""
        if not self._running:
            return
        
        max_attempts = self.config.max_reconnect_attempts
        interval = self.config.reconnect_interval
        
        self._reconnect_attempts += 1
        if self._reconnect_attempts > max_attempts:
            logger.error("[SignalingClient] Max reconnect attempts reached")
            self._running = False
            return
        
        delay = interval * min(2 ** (self._reconnect_attempts - 1), 30)
        logger.info(
            f"[SignalingClient] Reconnecting in {delay:.1f}s "
            f"(attempt {self._reconnect_attempts}/{max_attempts})"
        )
        
        await asyncio.sleep(delay)
        await self.connect()

    async def close(self) -> None:
        """关闭信令客户端"""
        logger.info("[SignalingClient] Closing")
        self._running = False
        
        if self._recv_task and not self._recv_task.done():
            self._recv_task.cancel()
            try:
                await self._recv_task
            except asyncio.CancelledError:
                pass
        
        if self._ws:
            try:
                await self._ws.close()
            except Exception:
                pass
            self._ws = None
        
        self._connected = False
        logger.info("[SignalingClient] Closed")
