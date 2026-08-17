"""P2P 节点 - 整合 ICE、KCP、信令

支持多 Peer 同时连接：每个远端 Peer 拥有独立的 IceManager / RTCPeerConnection，
通过 peer_id 路由信令、ICE 候选与数据。数据统一通过 KCP 传输。
"""

from __future__ import annotations

import asyncio
import base64
import contextlib
import json
from collections.abc import Awaitable, Callable
from typing import Any

from loguru import logger

from .config import (
    ConnectionRole,
    P2PConfig,
    TransportProtocol,
)
from .ice.ice_manager import IceManager
from .signaling.client import SignalingClient, SignalingEvents
from .transport.hybrid import CHANNEL_DATA, KCPDataTransport
from .types import (
    ConnectionState,
    IceCandidate,
    Message,
    MessageType,
    PeerInfo,
    SessionDescription,
    generate_peer_id,
)


class P2PNode:
    """P2P 节点 - 整合所有模块，支持多 Peer 并发连接"""

    def __init__(
        self,
        config: P2PConfig,
        on_message: Callable[[Message], Awaitable[None] | None] | None = None,
        on_peer_connected: Callable[[PeerInfo], None] | None = None,
        on_peer_disconnected: Callable[[str], None] | None = None,
        on_state_changed: Callable[[str, ConnectionState], None] | None = None,
    ):
        self.config = config
        self.peer_id: str = generate_peer_id()
        self.config.signaling.peer_id = self.peer_id
        # KCP 传输 (数据统一走 KCP, 控制走 SCTP)
        self._kcp_transports: dict[str, KCPDataTransport] = {}

        # 回调
        self.on_message = on_message
        self.on_peer_connected = on_peer_connected
        self.on_peer_disconnected = on_peer_disconnected
        self.on_state_changed = on_state_changed

        # 状态
        self._running: bool = False
        self.state: ConnectionState = ConnectionState.DISCONNECTED

        # 模块
        self._signaling: SignalingClient | None = None
        # 多 Peer：每个 peer_id 拥有独立的 IceManager / RTCPeerConnection
        self._ice_managers: dict[str, IceManager] = {}
        # 多 Peer 消息有序处理：per-peer 队列 + worker，保证同一 peer 的消息按到达顺序处理
        # （TCP 流转发强依赖字节顺序，create_task 并发会导致数据乱序）
        self._peer_msg_queues: dict[str, asyncio.Queue[Message | None]] = {}
        self._peer_msg_workers: dict[str, asyncio.Task[Any]] = {}

        # 连接的 Peer: peer_id -> {transport, state, ...}
        self._peers: dict[str, dict[str, Any]] = {}

        # 每 peer 的协商锁，避免不同 peer 互相阻塞
        self._negotiation_locks: dict[str, asyncio.Lock] = {}
        # 每 peer 的 answer 等待事件: peer_id -> asyncio.Event
        self._wait_answer_events: dict[str, asyncio.Event] = {}

    def _set_state(self, state: ConnectionState) -> None:
        if self.state != state:
            old = self.state
            self.state = state
            logger.info(f"[P2PNode {self.peer_id}] State: {old} -> {state}")
            if self.on_state_changed:
                self.on_state_changed(self.peer_id, state)

    # ========== 多 Peer IceManager 管理 ==========

    def _get_negotiation_lock(self, peer_id: str) -> asyncio.Lock:
        """获取指定 peer 的协商锁（不同 peer 可并行协商）"""
        if peer_id not in self._negotiation_locks:
            self._negotiation_locks[peer_id] = asyncio.Lock()
        return self._negotiation_locks[peer_id]

    def _get_or_create_ice(self, peer_id: str) -> IceManager:
        """获取或为指定 peer 创建独立的 IceManager

        每个 IceManager 拥有独立的 RTCPeerConnection，回调通过闭包绑定 peer_id，
        从而实现多 Peer 并发连接。
        """
        if peer_id in self._ice_managers:
            return self._ice_managers[peer_id]

        ice = IceManager(
            config=self.config.ice,
            # 用默认参数绑定 peer_id，避免闭包延迟绑定问题
            on_ice_candidate=lambda cand: self._on_ice_candidate(peer_id, cand),
            on_connection_state=lambda state: self._on_ice_state(peer_id, state),
            on_ice_gathering_done=lambda: self._on_ice_gathering_done(peer_id),
            on_remote_address=lambda addr: self._on_ice_remote_addr(peer_id, addr),
        )
        ice.on_data_received = lambda data: self._on_ice_data(peer_id, data)
        self._ice_managers[peer_id] = ice
        logger.info(f"[P2PNode] Created IceManager for peer {peer_id}")
        return ice

    async def initialize(self) -> None:
        """初始化节点"""
        logger.info(
            f"[P2PNode] Initializing {self.peer_id}, transport={self.config.transport.value}"
        )

        # 初始化信令（IceManager 按需在 connect_to_peer / _signal_on_offer 中创建）
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
        logger.info("[P2PNode] Node initialized (multi-peer capable)")

    async def connect_to_signaling(self) -> bool:
        """连接到信令服务器"""
        if not self._signaling:
            return False
        return await self._signaling.connect()

    async def join_room(
        self,
        room_id: str,
        role: ConnectionRole | None = None,
    ) -> bool:
        """加入房间"""
        if role:
            self.config.role = role
        if not self._signaling:
            logger.error("[P2PNode] Not initialized")
            return False
        return await self._signaling.join_room(room_id, self.config.role)

    async def connect_to_peer(self, target_peer_id: str) -> bool:
        """连接到指定 Peer (作为发起方，为该 peer 创建独立 IceManager)"""
        if not self._signaling or not self._signaling.is_connected:
            logger.error("[P2PNode] Not connected to signaling server")
            return False

        lock = self._get_negotiation_lock(target_peer_id)
        async with lock:
            # 已连接则跳过
            if target_peer_id in self._peers:
                logger.info(f"[P2PNode] Already connected to {target_peer_id}")
                return True

            logger.info(f"[P2PNode] Connecting to peer {target_peer_id}...")

            # 为该 peer 创建独立 IceManager
            ice = self._get_or_create_ice(target_peer_id)

            # 1. 创建 SDP Offer
            offer = await ice.create_offer()

            # 2. 先注册 Answer 事件再发送 Offer，避免竞态：
            #    _signal_on_answer 经 create_task 异步调度，若 Answer 在 send_offer
            #    让出事件循环后、注册事件前到达，事件会被静默丢弃并导致 30s 超时误判失败。
            answer_event = asyncio.Event()
            self._wait_answer_events[target_peer_id] = answer_event

            # 3. 通过信令发送 Offer
            await self._signaling.send_offer(target_peer_id, offer)

            logger.info(f"[P2PNode] Offer sent to {target_peer_id}, waiting for answer...")

            # 4. 等待 Answer（per-peer event）
            try:
                await asyncio.wait_for(answer_event.wait(), timeout=30.0)
            except asyncio.TimeoutError:
                logger.warning(f"[P2PNode] Timed out waiting for answer from {target_peer_id}")
                await self._fail_peer_connection(target_peer_id, "answer timeout")
                return False

            # 4. 等待 ICE 连接
            logger.info(f"[P2PNode] Waiting for ICE connection with {target_peer_id}...")
            ice_ok = await ice.wait_for_connection(timeout=30.0)

            if not ice_ok:
                logger.warning(f"[P2PNode] ICE connection failed with {target_peer_id}")
                await self._fail_peer_connection(target_peer_id, "ICE failed")
                return False

            logger.info(f"[P2PNode] ICE connection established with {target_peer_id}")
            ok = await self._establish_transports(target_peer_id)
            if not ok:
                await self._fail_peer_connection(target_peer_id, "transport establishment failed")
                return False
            return True

    async def _signal_on_offer(self, from_peer_id: str, offer: SessionDescription) -> None:
        """收到 Offer (作为响应方，为该 from_peer_id 创建独立 IceManager)"""
        logger.info(f"[P2PNode] Received offer from {from_peer_id}")

        lock = self._get_negotiation_lock(from_peer_id)
        async with lock:
            # 已连接则跳过（重复 offer）
            if from_peer_id in self._peers:
                logger.info(f"[P2PNode] Already connected to {from_peer_id}, ignoring offer")
                return

            # 为该 peer 创建独立 IceManager
            ice = self._get_or_create_ice(from_peer_id)

            # 创建 Answer
            answer = await ice.create_answer(offer)

            # 发送 Answer
            if self._signaling:
                await self._signaling.send_answer(from_peer_id, answer)

            logger.info(f"[P2PNode] Answer sent to {from_peer_id}")

            # 等待 ICE 连接
            ice_ok = await ice.wait_for_connection(timeout=30.0)
            if ice_ok:
                logger.info(f"[P2PNode] ICE connection established (responder) with {from_peer_id}")
                ok = await self._establish_transports(from_peer_id)
                if not ok:
                    await self._fail_peer_connection(from_peer_id, "transport establishment failed")
            else:
                logger.warning(f"[P2PNode] ICE connection failed with {from_peer_id}")
                await self._fail_peer_connection(from_peer_id, "ICE failed")

    async def _signal_on_answer(self, from_peer_id: str, answer: SessionDescription) -> None:
        """收到 Answer，路由到对应 peer 的 IceManager"""
        logger.info(f"[P2PNode] Received answer from {from_peer_id}")
        ice = self._ice_managers.get(from_peer_id)
        if ice:
            await ice.set_remote_description(answer)

        # 通知等待该 peer answer 的协程
        event = self._wait_answer_events.get(from_peer_id)
        if event:
            event.set()

    async def _signal_on_ice_candidate(self, from_peer_id: str, candidate: IceCandidate) -> None:
        """收到远端 ICE 候选，路由到对应 peer 的 IceManager"""
        logger.debug(f"[P2PNode] ICE candidate from {from_peer_id}: {candidate.candidate[:50]}...")
        ice = self._ice_managers.get(from_peer_id)
        if ice:
            await ice.add_ice_candidate(candidate)
        else:
            logger.warning(f"[P2PNode] No IceManager for {from_peer_id}, dropping candidate")

    # ========== IceManager 回调（带 peer_id）==========

    def _on_ice_candidate(self, peer_id: str, candidate: IceCandidate) -> None:
        """本地产生 ICE 候选 - 通过信令发送给指定 peer"""
        if not self._signaling:
            return
        task = asyncio.create_task(self._signaling.send_ice_candidate(peer_id, candidate))

        def _log_candidate_error(t: asyncio.Task[Any]) -> None:
            if t.cancelled():
                return
            exc = t.exception()
            if exc is not None:
                logger.warning(f"[P2PNode] Failed to send ICE candidate to {peer_id}: {exc}")

        task.add_done_callback(_log_candidate_error)

    def _on_ice_state(self, peer_id: str, state: ConnectionState) -> None:
        """某 peer 的 ICE 状态变化

        断开/失败时调用 _cleanup_peer 完整清理 IceManager/锁/事件，
        并通过 was_connected 标志避免与 _signal_on_peer_left 双触发 on_peer_disconnected。
        """
        ice = self._ice_managers.get(peer_id)
        ice_state = ice.ice_state if ice else "unknown"
        logger.info(f"[P2PNode] ICE state for {peer_id}: {state}, ice_state={ice_state}")

        # 连接断开/失败时清理
        if state in (
            ConnectionState.DISCONNECTED,
            ConnectionState.FAILED,
            ConnectionState.CLOSED,
        ):
            # _cleanup_peer 是 async，但本回调是 sync；用 create_task 调度
            # 并捕获清理过程中的异常，避免任务异常被 asyncio 静默丢弃。
            cleanup_task = asyncio.create_task(self._cleanup_and_notify(peer_id))

            def _log_cleanup_error(task: asyncio.Task[Any]) -> None:
                if task.cancelled():
                    return
                exc = task.exception()
                if exc is not None:
                    logger.error(f"[P2PNode] Cleanup error for {peer_id}: {exc}")

            cleanup_task.add_done_callback(_log_cleanup_error)

    async def _cleanup_and_notify(self, peer_id: str) -> None:
        """ICE 断开后的清理 + 通知（幂等：仅在 was_connected 时通知一次）"""
        was_connected = await self._cleanup_peer(peer_id)
        if was_connected and self.on_peer_disconnected:
            self.on_peer_disconnected(peer_id)

    def _on_ice_gathering_done(self, peer_id: str) -> None:
        """某 peer 的 ICE 候选收集完成"""
        ice = self._ice_managers.get(peer_id)
        if not ice:
            return
        logger.info(
            f"[P2PNode] ICE gathering done for {peer_id}. "
            f"Host={len(ice.get_host_candidates())}, "
            f"SRFLX={len(ice.get_server_reflexive_candidates())}, "
            f"Relay(TURN)={len(ice.get_relay_candidates())}"
        )

        relay_candidates = ice.get_relay_candidates()
        if relay_candidates:
            logger.info(f"[P2PNode] Cloudflare TURN relay candidates for {peer_id}:")
            for i, c in enumerate(relay_candidates):
                logger.info(f"  [{i}] {c.candidate[:100]}")

    def _on_ice_remote_addr(self, peer_id: str, addr: tuple[str, int]) -> None:
        """某 peer 的 ICE 远端地址"""
        logger.info(f"[P2PNode] ICE remote address for {peer_id}: {addr}")

    async def _establish_transports(self, peer_id: str) -> bool:
        """在 ICE 连接成功后建立传输层 (使用 DataChannel)

        Returns: True 表示成功建立并已触发 on_peer_connected，False 表示失败（调用方应清理）
        """
        ice = self._ice_managers.get(peer_id)
        if not ice:
            logger.warning(f"[P2PNode] No IceManager for {peer_id}")
            return False

        remote_addr = ice.selected_address

        logger.info(f"[P2PNode] Establishing DataChannel with {peer_id}")

        # 等待 DataChannel 打开
        dc_ok = await ice.wait_for_data_channel(timeout=15.0)
        if not dc_ok:
            logger.warning(f"[P2PNode] DataChannel failed to open with {peer_id}")
            return False

        # 初始化 KCP 传输
        await self._init_kcp_transport(peer_id, ice)

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
        return True

    # ========== KCP 传输 ==========

    async def _init_kcp_transport(self, peer_id: str, ice: IceManager) -> None:
        """初始化 KCP 传输:注入 SCTP 回调,RESPONDER 绑定端口,双方交换地址"""
        kcp_transport = KCPDataTransport(
            role=self.config.role,
            kcp_config=self.config.kcp,
        )
        # 注入 SCTP 发送回调（必须同步:DataChannel.send 本身同步,且保证 TCP 字节序）
        kcp_transport.on_sctp_ready(ice.send_data_sync)

        # 设置数据接收回调:所有通道(CTRL/DATA)收到的都是序列化 Message,
        # 统一交给 _handle_transport_data 解码并路由到 on_message(per-peer 有序队列)
        def _on_kcp_data(data: bytes, _channel: str) -> None:
            self._handle_transport_data(data, self.config.transport, peer_id)

        kcp_transport.on_data = _on_kcp_data

        self._kcp_transports[peer_id] = kcp_transport

        # RESPONDER 先绑定 KCP 端口
        if self.config.role == ConnectionRole.RESPONDER:
            await kcp_transport.start_server()

        # 双方通过 SCTP 交换地址
        await kcp_transport.exchange_address()

        # 等待地址交换完成(INITIATOR 会自动发起直连)
        await kcp_transport.wait_for_addr_exchange(timeout=10.0)

    async def send_to_peer_kcp(
        self, peer_id: str, data: bytes, channel: str = CHANNEL_DATA
    ) -> bool:
        """通过 KCP 传输发送数据

        Args:
            peer_id: 目标 peer
            data: 数据
            channel: CHANNEL_CONTROL / CHANNEL_DATA
        """
        kcp_transport = self._kcp_transports.get(peer_id)
        if kcp_transport:
            return await kcp_transport.send(data, channel)
        # KCP 传输未就绪 → 走默认 SCTP
        ice = self._ice_managers.get(peer_id)
        if ice:
            return await ice.send_data(data)
        return False

    async def send_to_peer_channel(
        self,
        peer_id: str,
        msg_type: MessageType,
        payload: Any,
        channel: str = CHANNEL_DATA,
    ) -> bool:
        """序列化 Message 后通过指定通道发送

        Args:
            peer_id: 目标 peer
            msg_type: 消息类型
            payload: 消息负载
            channel: CHANNEL_CONTROL / CHANNEL_DATA
        """
        msg = Message.create(
            msg_type=msg_type,
            sender_id=self.peer_id,
            receiver_id=peer_id,
            payload=payload,
        )
        data = self._encode_message(msg)
        return await self.send_to_peer_kcp(peer_id, data, channel)

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
            connect_task = asyncio.create_task(self.connect_to_peer(peer.peer_id))

            def _log_connect_error(t: asyncio.Task[Any]) -> None:
                if t.cancelled():
                    return
                exc = t.exception()
                if exc is not None:
                    logger.error(f"[P2PNode] Auto-connect to {peer.peer_id} failed: {exc}")

            connect_task.add_done_callback(_log_connect_error)

    async def _signal_on_peer_left(self, peer_id: str) -> None:
        """Peer 离开房间（幂等：复用 _cleanup_and_notify，与 ICE 断开路径一致）"""
        logger.info(f"[P2PNode] Peer left: {peer_id}")
        await self._cleanup_and_notify(peer_id)

    async def _cleanup_peer(self, peer_id: str) -> bool:
        """清理指定 peer 的所有资源（IceManager / 锁 / 事件 / peer 状态 / 消息队列）

        幂等：重复调用安全。用于连接失败、ICE 断开、peer 离开等所有清理路径。

        Returns:
            该 peer 在被清理前是否处于已连接状态。
        """
        was_connected = self._peers.pop(peer_id, None) is not None
        self._wait_answer_events.pop(peer_id, None)
        self._negotiation_locks.pop(peer_id, None)
        ice = self._ice_managers.pop(peer_id, None)
        if ice:
            try:
                await ice.close()
            except Exception as e:
                logger.debug(f"[P2PNode] Error closing IceManager for {peer_id}: {e}")
        # 清理 KCP 传输
        kcp_transport = self._kcp_transports.pop(peer_id, None)
        if kcp_transport:
            try:
                await kcp_transport.close()
            except Exception as e:
                logger.debug(f"[P2PNode] Error closing KCPDataTransport for {peer_id}: {e}")
        # 停止消息 worker：发送 None 停止信号（无上限队列不会满）
        queue = self._peer_msg_queues.pop(peer_id, None)
        if queue is not None:
            queue.put_nowait(None)
        worker = self._peer_msg_workers.pop(peer_id, None)
        if worker is not None:
            worker.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await worker
        return was_connected

    async def _fail_peer_connection(self, peer_id: str, reason: str) -> None:
        """连接失败的统一清理：关闭 IceManager 并清理所有中间状态"""
        logger.warning(f"[P2PNode] Failing connection with {peer_id}: {reason}")
        await self._cleanup_peer(peer_id)

    def _signal_on_room_info(self, peers: list[PeerInfo]) -> None:
        """房间信息更新"""
        logger.info(f"[P2PNode] Room has {len(peers)} peers: {[p.peer_id for p in peers]}")

    def _signal_on_connected(self) -> None:
        """信令连接成功"""
        logger.info("[P2PNode] Signaling connected")
        self._set_state(ConnectionState.CONNECTING)

    def _signal_on_disconnected(self) -> None:
        """信令断开"""
        logger.warning("[P2PNode] Signaling disconnected")

    def _on_ice_data(self, peer_id: str, data: bytes) -> None:
        """收到某 peer 的 DataChannel 数据"""
        kcp_transport = self._kcp_transports.get(peer_id)
        if kcp_transport:
            # 先经 KCPDataTransport 过滤(管理消息 vs 业务数据)
            kcp_transport.on_sctp_data(data)
        else:
            self._handle_transport_data(data, self.config.transport, peer_id)

    def _handle_transport_data(
        self,
        data: bytes,
        transport: TransportProtocol,
        peer_id: str | None = None,
    ) -> None:
        """处理从传输层收到的数据

        关键：同一 peer 的消息必须按到达顺序处理（TCP 字节流强依赖顺序）。
        通过 per-peer 队列 + 单 worker 串行 await，避免 create_task 并发导致乱序。
        """
        msg: Message | None = None
        try:
            msg = self._decode_message(data)
            logger.debug(
                f"[P2PNode] Received {msg.msg_type.value} via {transport.value} "
                f"from {msg.sender_id}"
            )
        except Exception:
            # 解码失败：包装成原始二进制消息（sender 用 peer_id 兜底，非数据包来源）
            msg = Message.create(
                msg_type=MessageType.DATA_BINARY,
                sender_id=peer_id or "unknown",
                receiver_id=self.peer_id,
                payload=data,
            )

        if self.on_message:
            # 按 sender_id 路由到 per-peer 有序队列
            queue_key = msg.sender_id or (peer_id or "unknown")
            self._enqueue_message(queue_key, msg)

    def _enqueue_message(self, peer_id: str, msg: Message) -> None:
        """将消息放入 per-peer 队列（无上限队列，不丢消息）"""
        queue = self._get_or_create_msg_queue(peer_id)
        queue.put_nowait(msg)

    def _get_or_create_msg_queue(self, peer_id: str) -> asyncio.Queue[Message | None]:
        """获取或创建 per-peer 消息队列 + worker"""
        if peer_id not in self._peer_msg_queues:
            # 无上限队列：不主动丢消息，内存增长由对端发送速率决定
            queue: asyncio.Queue[Message | None] = asyncio.Queue()
            self._peer_msg_queues[peer_id] = queue
            self._peer_msg_workers[peer_id] = asyncio.create_task(self._msg_worker(peer_id, queue))
            logger.debug(f"[P2PNode] Created message queue+worker for {peer_id}")
        return self._peer_msg_queues[peer_id]

    async def _msg_worker(self, peer_id: str, queue: asyncio.Queue[Message | None]) -> None:
        """per-peer 消息处理 worker：按队列顺序 await 处理，保证消息不乱序"""
        while True:
            msg = await queue.get()
            if msg is None:  # 停止信号
                queue.task_done()
                break
            try:
                if self.on_message:
                    result = self.on_message(msg)
                    if asyncio.iscoroutine(result):
                        await result
            except Exception as e:
                logger.error(f"[P2PNode] Message handler error for {peer_id}: {e}")
            finally:
                queue.task_done()

    @staticmethod
    def _encode_payload(payload: Any) -> Any:
        """递归将 payload 编码为可 JSON 序列化的结构

        - bytes -> {"__type__": "bytes", "__data__": "<base64>"}（解码时还原为 bytes）
        - dict / list 递归处理内嵌的 bytes（如隧道消息 {..., "data": b'...'}）
        - 其余原样返回（str / None / 数值等，须为 JSON 可序列化类型）
        """
        if isinstance(payload, bytes):
            return {"__type__": "bytes", "__data__": base64.b64encode(payload).decode("ascii")}
        if isinstance(payload, dict):
            return {k: P2PNode._encode_payload(v) for k, v in payload.items()}
        if isinstance(payload, (list, tuple)):
            return [P2PNode._encode_payload(item) for item in payload]
        return payload

    @staticmethod
    def _decode_payload(payload: Any) -> Any:
        """将 JSON 结构还原为原始 payload"""
        if isinstance(payload, dict):
            # 仅当存在 __type__ 标记时才解码为 bytes，避免与用户合法字典冲突
            if payload.get("__type__") == "bytes" and "__data__" in payload:
                return base64.b64decode(payload["__data__"])
            return {k: P2PNode._decode_payload(v) for k, v in payload.items()}
        if isinstance(payload, list):
            return [P2PNode._decode_payload(item) for item in payload]
        return payload

    def _encode_message(self, msg: Message) -> bytes:
        """编码消息为 JSON 字节串（替代 pickle，避免反序列化安全风险）

        仅支持 JSON 可序列化的 payload；bytes 通过 base64 内嵌。
        """
        return json.dumps(
            {
                "msg_id": msg.msg_id,
                "msg_type": msg.msg_type.value,
                "sender_id": msg.sender_id,
                "receiver_id": msg.receiver_id,
                "payload": self._encode_payload(msg.payload),
                "timestamp": msg.timestamp.isoformat(),
                "seq": msg.seq,
            },
            ensure_ascii=False,
        ).encode("utf-8")

    def _decode_message(self, data: bytes) -> Message:
        """从 JSON 字节串解码消息"""
        obj = json.loads(data.decode("utf-8"))
        return Message(
            msg_id=obj["msg_id"],
            msg_type=MessageType(obj["msg_type"]),
            sender_id=obj["sender_id"],
            receiver_id=obj["receiver_id"],
            payload=self._decode_payload(obj["payload"]),
            seq=obj["seq"],
        )

    async def send_to_peer(
        self,
        peer_id: str,
        msg_type: MessageType,
        payload: Any = None,
        _prefer_transport: TransportProtocol | None = None,
    ) -> bool:
        """发送消息到指定 Peer (通过该 peer 的 ICE DataChannel)

        Args:
            peer_id: 目标 peer 标识。
            msg_type: 消息类型。
            payload: 消息负载。
            _prefer_transport: 预留的传输偏好参数（当前固定走 ICE
                DataChannel，保留以维持 API 契约）。
        """
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

        # 通过该 peer 的独立 IceManager DataChannel 发送
        ice = self._ice_managers.get(peer_id)
        if ice:
            return await ice.send_data(data)

        logger.warning(f"[P2PNode] No IceManager for {peer_id}")
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

    def get_connected_peers(self) -> list[str]:
        """获取已连接的 Peer ID 列表"""
        return [
            pid for pid, data in self._peers.items() if data["state"] == ConnectionState.CONNECTED
        ]

    def get_connection_stats(self, peer_id: str | None = None) -> dict[str, Any]:
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
            ice = self._ice_managers.get(pid)
            if ice:
                info["ice_state"] = ice.ice_state
                info["local_candidates"] = len(ice.local_candidates)
                info["remote_candidates"] = len(ice.remote_candidates)
                info["selected_address"] = ice.selected_address
            result[pid] = info

        return result

    async def close(self) -> None:
        """关闭节点"""
        logger.info(f"[P2PNode] Closing node {self.peer_id}")
        self._running = False
        self._set_state(ConnectionState.CLOSED)

        # 关闭所有 peer 的 IceManager
        for pid in list(self._ice_managers.keys()):
            await self._cleanup_peer(pid)

        self._peers.clear()
        self._ice_managers.clear()
        self._kcp_transports.clear()
        self._wait_answer_events.clear()
        self._negotiation_locks.clear()
        self._peer_msg_queues.clear()
        self._peer_msg_workers.clear()

        if self._signaling:
            await self._signaling.close()

        logger.info(f"[P2PNode] Node {self.peer_id} closed")
