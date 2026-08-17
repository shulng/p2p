"""信令客户端 - 与信令服务器通信"""

from __future__ import annotations

import asyncio
import contextlib
import json
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

from loguru import logger

from .._compat import WEBSOCKETS_AVAILABLE, websockets
from .._utils import cancel_task
from ..config import ConnectionRole, SignalingConfig
from ..types import (
    IceCandidate,
    MessageType,
    PeerInfo,
    SessionDescription,
    generate_peer_id,
)

# 回调类型：可为同步函数或异步协程函数
CallbackT = Callable[..., Awaitable[None] | None]


@dataclass
class SignalingEvents:
    """信令事件回调

    所有回调既支持同步函数，也支持异步协程函数。回调在 _handle_message
    中通过 _dispatch 统一调度：异步回调用 create_task，同步回调直接调用。
    """

    on_offer: CallbackT | None = None
    on_answer: CallbackT | None = None
    on_ice_candidate: CallbackT | None = None
    on_peer_joined: CallbackT | None = None
    on_peer_left: CallbackT | None = None
    on_room_info: CallbackT | None = None
    on_connected: CallbackT | None = None
    on_disconnected: CallbackT | None = None


def _dispatch(callback: CallbackT | None, *args: object) -> None:
    """调度回调：异步回调交给事件循环，同步回调直接执行。

    若回调返回协程，则创建任务；否则视为同步回调直接调用。
    """
    if callback is None:
        return
    result = callback(*args)
    if asyncio.iscoroutine(result):
        asyncio.create_task(result)


class SignalingClient:
    """WebSocket 信令客户端"""

    def __init__(self, config: SignalingConfig, events: SignalingEvents):
        if not WEBSOCKETS_AVAILABLE:
            raise RuntimeError("websockets not installed")

        self.config = config
        self.events = events

        self.peer_id: str = config.peer_id or generate_peer_id()
        self._ws: Any = None
        self._connected: bool = False
        self._running: bool = False
        self._recv_task: asyncio.Task[Any] | None = None
        self._reconnect_attempts: int = 0

        # 房间中的 Peer 列表
        self.room_peers: list[PeerInfo] = []

        self._connected_event: asyncio.Event = asyncio.Event()
        self._join_event: asyncio.Event = asyncio.Event()

    @property
    def is_connected(self) -> bool:
        """是否已连接到信令服务器。"""
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

            _dispatch(self.events.on_connected)

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
            await self._send(
                {
                    "type": MessageType.SIGNAL_JOIN.value,
                    "peer_id": self.peer_id,
                    "room_id": room_id,
                    "role": role.value,
                }
            )

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

    async def _send(self, msg: dict[str, Any]) -> None:
        """发送消息到服务器"""
        if not self._ws or not self._connected:
            raise RuntimeError("Not connected to signaling server")

        await self._ws.send(json.dumps(msg))

    async def send_offer(self, to_peer_id: str, offer: SessionDescription) -> None:
        """发送 SDP Offer"""
        await self._send(
            {
                "type": MessageType.SIGNAL_OFFER.value,
                "to": to_peer_id,
                "from": self.peer_id,
                "sdp_type": offer.sdp_type,
                "sdp": offer.sdp,
            }
        )
        logger.info(f"[SignalingClient] Sent offer to {to_peer_id}")

    async def send_answer(self, to_peer_id: str, answer: SessionDescription) -> None:
        """发送 SDP Answer"""
        await self._send(
            {
                "type": MessageType.SIGNAL_ANSWER.value,
                "to": to_peer_id,
                "from": self.peer_id,
                "sdp_type": answer.sdp_type,
                "sdp": answer.sdp,
            }
        )
        logger.info(f"[SignalingClient] Sent answer to {to_peer_id}")

    async def send_ice_candidate(self, to_peer_id: str, candidate: IceCandidate) -> None:
        """发送 ICE 候选"""
        await self._send(
            {
                "type": MessageType.SIGNAL_ICE_CANDIDATE.value,
                "to": to_peer_id,
                "from": self.peer_id,
                "candidate": candidate.candidate,
                "sdp_mid": candidate.sdp_mid,
                "sdp_mline_index": candidate.sdp_mline_index,
            }
        )

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

            _dispatch(self.events.on_disconnected)

            # 尝试重连
            if self._running:
                await self._schedule_reconnect()

    # ========== 消息分发 ==========

    async def _handle_message(self, msg: dict[str, Any]) -> None:
        """处理接收到的消息（按类型分发到各 handler）"""
        msg_type = msg.get("type", "")

        handlers = {
            MessageType.SIGNAL_JOIN.value: self._handle_join_confirm,
            MessageType.SIGNAL_OFFER.value: self._handle_offer,
            MessageType.SIGNAL_ANSWER.value: self._handle_answer,
            MessageType.SIGNAL_ICE_CANDIDATE.value: self._handle_ice_candidate,
            MessageType.SIGNAL_ROOM_INFO.value: self._handle_room_info,
            MessageType.CTRL_ERROR.value: self._handle_error,
        }

        handler = handlers.get(msg_type)
        if handler:
            # 所有 handler 均为同步函数；若未来引入异步 handler，
            # 此处需将 handler 返回值提升为 Awaitable 后再 await。
            handler(msg)
        else:
            logger.debug(f"[SignalingClient] Unhandled message type: {msg_type}")

    def _handle_join_confirm(self, msg: dict[str, Any]) -> None:
        """JOIN 确认"""
        if msg.get("success"):
            self._join_event.set()

    def _handle_offer(self, msg: dict[str, Any]) -> None:
        """收到 Offer"""
        from_peer = msg.get("from")
        if from_peer and self.events.on_offer:
            offer = SessionDescription(
                sdp_type=msg.get("sdp_type", "offer"),
                sdp=msg.get("sdp", ""),
            )
            _dispatch(self.events.on_offer, from_peer, offer)

    def _handle_answer(self, msg: dict[str, Any]) -> None:
        """收到 Answer"""
        from_peer = msg.get("from")
        if from_peer and self.events.on_answer:
            answer = SessionDescription(
                sdp_type=msg.get("sdp_type", "answer"),
                sdp=msg.get("sdp", ""),
            )
            _dispatch(self.events.on_answer, from_peer, answer)

    def _handle_ice_candidate(self, msg: dict[str, Any]) -> None:
        """收到 ICE 候选"""
        from_peer = msg.get("from")
        if from_peer and self.events.on_ice_candidate:
            candidate = IceCandidate(
                candidate=msg.get("candidate", ""),
                sdp_mid=msg.get("sdp_mid"),
                sdp_mline_index=msg.get("sdp_mline_index"),
            )
            _dispatch(self.events.on_ice_candidate, from_peer, candidate)

    def _handle_room_info(self, msg: dict[str, Any]) -> None:
        """房间信息更新"""
        peers_data = msg.get("peers", [])
        peers = self._parse_peers(peers_data)

        # 检测新加入/离开的 Peer
        old_ids = {p.peer_id for p in self.room_peers}
        new_ids = {p.peer_id for p in peers}

        joined = new_ids - old_ids
        left = old_ids - new_ids

        for p in peers:
            if p.peer_id in joined and self.events.on_peer_joined and p.peer_id != self.peer_id:
                _dispatch(self.events.on_peer_joined, p)

        for pid in left:
            if self.events.on_peer_left:
                _dispatch(self.events.on_peer_left, pid)

        self.room_peers = peers

        if self.events.on_room_info:
            self.events.on_room_info(peers)

    def _handle_error(self, msg: dict[str, Any]) -> None:
        """服务器错误消息"""
        logger.warning(f"[SignalingClient] Server error: {msg.get('error')}")

    @staticmethod
    def _parse_peers(peers_data: list[dict[str, Any]]) -> list[PeerInfo]:
        """将服务器返回的 peer 字典列表解析为 PeerInfo 列表"""
        peers: list[PeerInfo] = []
        for p in peers_data:
            try:
                role = ConnectionRole(p.get("role")) if p.get("role") else None
            except ValueError:
                role = None
            peers.append(
                PeerInfo(
                    peer_id=p["peer_id"],
                    role=role or ConnectionRole.INITIATOR,
                )
            )
        return peers

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

        await cancel_task(self._recv_task)

        if self._ws:
            with contextlib.suppress(Exception):
                await self._ws.close()
            self._ws = None

        self._connected = False
        logger.info("[SignalingClient] Closed")
