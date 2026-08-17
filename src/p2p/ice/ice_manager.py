"""ICE 管理模块 - 集成 TURN (支持 Cloudflare TURN: turn.cloudflare.com:3478)"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from typing import TYPE_CHECKING, Any

from loguru import logger

# 仅在类型检查阶段无条件导入 aiortc 符号，避免基于 pyright 在
# try/except ImportError 场景下报"可能未绑定"；运行时该块不执行。
if TYPE_CHECKING:
    from aiortc import (
        RTCDataChannel,
        RTCPeerConnection,
        RTCSessionDescription,
    )
    from aiortc.rtcconfiguration import RTCConfiguration, RTCIceServer
    from aiortc.sdp import candidate_from_sdp

try:
    from aiortc import (
        RTCDataChannel,
        RTCPeerConnection,
        RTCSessionDescription,
    )
    from aiortc.rtcconfiguration import RTCConfiguration, RTCIceServer
    from aiortc.sdp import candidate_from_sdp

    _aiortc_import_ok = True
except ImportError:
    _aiortc_import_ok = False
    logger.warning("aiortc not installed, ICE/TURN features will be limited")

# 供模块内使用的功能开关：try/except 中不允许对全大写"常量"赋值两次，
# 因此先用小写变量承载导入状态，再一次性赋值给对外常量，消除重定义告警。
AIORTC_AVAILABLE: bool = _aiortc_import_ok

from ..config import IceConfig
from ..types import (
    ConnectionState,
    SessionDescription,
    generate_peer_id,
)
from ..types import (
    IceCandidate as LocalIceCandidate,
)

from .candidate_extractor import (
    CandidatePairExtractor,
    default_pair_extractor,
)


class IceManager:
    """ICE 连接管理器 - 处理 STUN/TURN 候选地址收集和连接"""

    def __init__(
        self,
        config: IceConfig,
        on_ice_candidate: Callable[[LocalIceCandidate], None] | None = None,
        on_connection_state: Callable[[ConnectionState], None] | None = None,
        on_ice_gathering_done: Callable[[], None] | None = None,
        on_remote_address: Callable[[tuple[str, int]], None] | None = None,
        pair_extractor: CandidatePairExtractor | None = None,
    ):
        self.config = config
        self.on_ice_candidate = on_ice_candidate
        self.on_connection_state = on_connection_state
        self.on_ice_gathering_done = on_ice_gathering_done
        self.on_remote_address = on_remote_address
        self.on_data_received: Callable[[bytes], None] | None = None
        # 候选对提取器：注入可替换实现，默认使用 aiortc 私有实现；
        # 也可注入 NullPairExtractor 以显式禁用 KCP 直连。
        self._pair_extractor: CandidatePairExtractor = (
            pair_extractor or default_pair_extractor()
        )

        # 状态
        self.state: ConnectionState = ConnectionState.DISCONNECTED
        self.ice_state: str | None = None
        self.gathering_done: bool = False

        # PeerConnection (aiortc)
        self._pc: RTCPeerConnection | None = None
        self._peer_id: str = generate_peer_id()

        # DataChannel
        self._data_channel: RTCDataChannel | None = None
        self._data_channel_open: asyncio.Event = asyncio.Event()

        # 事件
        self._gathering_done_event: asyncio.Event = asyncio.Event()
        self._connected_event: asyncio.Event = asyncio.Event()

        # 收集到的候选地址
        self.local_candidates: list[LocalIceCandidate] = []
        self.remote_candidates: list[LocalIceCandidate] = []

        # 选中的候选对
        self.selected_local_candidate: LocalIceCandidate | None = None
        self.selected_remote_candidate: LocalIceCandidate | None = None
        self.selected_address: tuple[str, int] | None = None  # (host, port)

    @property
    def peer_id(self) -> str:
        """当前 ICE 会话的 peer 标识。"""
        return self._peer_id

    def _set_state(self, state: ConnectionState) -> None:
        if self.state != state:
            old_state = self.state
            self.state = state
            logger.info(f"[ICE] State changed: {old_state} -> {state}")
            if self.on_connection_state:
                self.on_connection_state(state)

    def _build_rtc_config(self) -> RTCConfiguration | None:
        """构建 aiortc RTCConfiguration"""
        if not AIORTC_AVAILABLE:
            return None

        ice_servers: list[RTCIceServer] = []
        for turn_cfg in self.config.ice_servers:
            # 单独构建可选的 str 字段，避免把 dict 整体标注为 `str | list[str]`
            # 导致基于 pyright 将 username/credential 误判为 list[str] 而无法赋值。
            ice_servers.append(
                RTCIceServer(
                    urls=[turn_cfg.url],
                    **({"username": turn_cfg.username} if turn_cfg.username else {}),
                    **({"credential": turn_cfg.credential} if turn_cfg.credential else {}),
                )
            )

        # 特别为 Cloudflare TURN 配置
        has_cloudflare = any(s.use_cloudflare for s in self.config.ice_servers)
        if has_cloudflare:
            logger.info("[ICE] Using Cloudflare TURN servers (turn.cloudflare.com:3478)")

        return RTCConfiguration(iceServers=ice_servers)

    async def create_offer(self) -> SessionDescription:
        """
        创建 SDP Offer (发起方)
        """
        if not AIORTC_AVAILABLE:
            raise RuntimeError("aiortc not installed")

        self._set_state(ConnectionState.CONNECTING)

        rtc_config = self._build_rtc_config()
        self._pc = RTCPeerConnection(configuration=rtc_config)

        # 注册事件
        self._register_events()

        # 添加一个数据通道来触发 ICE 并用于数据传输
        try:
            self._data_channel = self._pc.createDataChannel("p2p-data", ordered=True)
            self._register_data_channel_events(self._data_channel)
            logger.debug("[ICE] Created data channel 'p2p-data'")
        except Exception as e:
            logger.warning(f"[ICE] Could not create data channel: {e}")

        # 创建 Offer
        offer = await self._pc.createOffer()
        await self._pc.setLocalDescription(offer)

        logger.info("[ICE] Created SDP offer")

        # 等待候选收集完成
        try:
            await asyncio.wait_for(
                self._gathering_done_event.wait(),
                timeout=self.config.gather_timeout,
            )
        except asyncio.TimeoutError:
            logger.warning("[ICE] ICE gathering timeout, using current candidates")

        sdp = self._pc.localDescription
        return SessionDescription(sdp_type=sdp.type, sdp=sdp.sdp)

    async def create_answer(self, offer: SessionDescription) -> SessionDescription:
        """
        处理 Offer 并创建 Answer (响应方)
        """
        if not AIORTC_AVAILABLE:
            raise RuntimeError("aiortc not installed")

        self._set_state(ConnectionState.CONNECTING)

        rtc_config = self._build_rtc_config()
        self._pc = RTCPeerConnection(configuration=rtc_config)

        # 注册事件
        self._register_events()

        # 处理远端 Offer
        await self._pc.setRemoteDescription(
            RTCSessionDescription(sdp=offer.sdp, type=offer.sdp_type)
        )

        # 创建 Answer
        answer = await self._pc.createAnswer()
        await self._pc.setLocalDescription(answer)

        logger.info("[ICE] Created SDP answer")

        # 等待候选收集
        try:
            await asyncio.wait_for(
                self._gathering_done_event.wait(),
                timeout=self.config.gather_timeout,
            )
        except asyncio.TimeoutError:
            logger.warning("[ICE] ICE gathering timeout")

        sdp = self._pc.localDescription
        return SessionDescription(sdp_type=sdp.type, sdp=sdp.sdp)

    async def set_remote_description(self, desc: SessionDescription) -> None:
        """设置远端会话描述"""
        if not self._pc:
            logger.warning("[ICE] set_remote_description called without peer connection")
            return

        await self._pc.setRemoteDescription(RTCSessionDescription(sdp=desc.sdp, type=desc.sdp_type))
        logger.info(f"[ICE] Remote description set, type={desc.sdp_type}")

    async def add_ice_candidate(self, candidate: LocalIceCandidate) -> None:
        """添加远端 ICE 候选"""
        if not self._pc:
            logger.warning("[ICE] add_ice_candidate called without peer connection")
            return

        try:
            ice_candidate = candidate_from_sdp(candidate.candidate)
            await self._pc.addIceCandidate(ice_candidate)
            self.remote_candidates.append(candidate)
            logger.debug(f"[ICE] Added remote candidate: {candidate.candidate[:60]}...")
        except Exception as e:
            logger.error(f"[ICE] Failed to add ICE candidate: {e}")

    def _register_events(self) -> None:
        """注册 aiortc 事件"""
        pc = self._pc
        if pc is None:
            return

        @pc.on("icecandidate")
        async def on_ice_candidate(candidate: Any) -> None:
            if candidate:
                local_cand = LocalIceCandidate(
                    candidate=candidate.candidate,
                    sdp_mid=candidate.sdpMid,
                    sdp_mline_index=candidate.sdpMLineIndex,
                )
                self.local_candidates.append(local_cand)

                if self.on_ice_candidate:
                    self.on_ice_candidate(local_cand)

                logger.debug(f"[ICE] Local candidate: {candidate.candidate[:60]}...")
            else:
                # None 候选 = 收集完成
                self.gathering_done = True
                self._gathering_done_event.set()
                logger.info("[ICE] ICE candidate gathering completed")
                if self.on_ice_gathering_done:
                    self.on_ice_gathering_done()

        @pc.on("iceconnectionstatechange")
        async def on_ice_state_change() -> None:
            state = pc.iceConnectionState
            self.ice_state = state
            logger.info(f"[ICE] ICE connection state: {state}")

            state_map = {
                "new": ConnectionState.CONNECTING,
                "checking": ConnectionState.CHECKING,
                "connected": ConnectionState.CONNECTED_ICE,
                "completed": ConnectionState.CONNECTED_ICE,
                "failed": ConnectionState.FAILED,
                "disconnected": ConnectionState.DISCONNECTED,
                "closed": ConnectionState.CLOSED,
            }

            mapped_state = state_map.get(state, ConnectionState.CONNECTING)
            self._set_state(mapped_state)

            if state in ("connected", "completed"):
                self._connected_event.set()
                await self._extract_selected_candidates()

        @pc.on("connectionstatechange")
        async def on_connection_state() -> None:
            state = pc.connectionState
            logger.info(f"[ICE] Connection state: {state}")

        @pc.on("datachannel")
        def on_datachannel(channel: Any) -> None:
            logger.info(f"[ICE] Remote data channel received: {channel.label}")
            self._data_channel = channel
            self._register_data_channel_events(channel)
            # 如果 DataChannel 已经是 open 状态，直接设置事件
            if hasattr(channel, "readyState") and channel.readyState == "open":
                logger.info("[ICE] DataChannel already open")
                self._data_channel_open.set()

    def _register_data_channel_events(self, channel: Any) -> None:
        """注册 DataChannel 事件"""

        @channel.on("open")  # type: ignore[untyped-decorator]
        def on_open() -> None:
            logger.info("[ICE] DataChannel opened")
            self._data_channel_open.set()

        @channel.on("message")  # type: ignore[untyped-decorator]
        def on_message(message: Any) -> None:
            if isinstance(message, str):
                message = message.encode("utf-8")
            if self.on_data_received:
                self.on_data_received(message)

        @channel.on("close")  # type: ignore[untyped-decorator]
        def on_close() -> None:
            logger.info("[ICE] DataChannel closed")
            self._data_channel_open.clear()

    async def send_data(self, data: bytes) -> bool:
        """通过 DataChannel 发送数据"""
        return self.send_data_sync(data)

    def send_data_sync(self, data: bytes) -> bool:
        """同步发送数据（DataChannel.send 本身为同步写入 SCTP 缓冲）

        供混合传输的 SCTP 同步回调使用，保证字节序严格一致（TCP 流转发依赖）。
        """
        if not self._data_channel or not self._data_channel_open.is_set():
            return False
        try:
            self._data_channel.send(data)
            return True
        except Exception as e:
            logger.error(f"[ICE] DataChannel send error: {e}")
            return False

    async def wait_for_data_channel(self, timeout: float = 15.0) -> bool:
        """等待 DataChannel 打开"""
        # 如果已经 open，直接返回
        if self._data_channel_open.is_set():
            return True
        # 如果 DataChannel 存在且 readyState=open，直接设置
        if (
            self._data_channel
            and hasattr(self._data_channel, "readyState")
            and self._data_channel.readyState == "open"
        ):
            self._data_channel_open.set()
            return True
        try:
            await asyncio.wait_for(self._data_channel_open.wait(), timeout=timeout)
            return True
        except asyncio.TimeoutError:
            # 最后再检查一次 readyState
            if (
                self._data_channel
                and hasattr(self._data_channel, "readyState")
                and self._data_channel.readyState == "open"
            ):
                self._data_channel_open.set()
                return True
            logger.warning("[ICE] DataChannel open timeout")
            return False

    async def _extract_selected_candidates(self) -> None:
        """提取被选中的 ICE 候选对（用于 KCP 直连）。

        委托给注入的 ``CandidatePairExtractor``，实现与底层 WebRTC 库
        （aiortc）的私有 API 解耦：本方法不接触任何内部属性，读取逻辑
        全部收敛到提取器实现中。
        """
        if not self._pc:
            return

        pair = await self._pair_extractor.extract(self._pc)
        if pair is None:
            return

        remote = pair.remote
        local = pair.local
        self.selected_local_candidate = LocalIceCandidate(
            candidate=f"{local.protocol} {local.ip}:{local.port}",
        )
        self.selected_remote_candidate = LocalIceCandidate(
            candidate=f"{remote.protocol} {remote.ip}:{remote.port}",
        )
        self.selected_address = (remote.ip, remote.port)

        logger.info(
            f"[ICE] Selected pair: {local.ip}:{local.port} "
            f"-> {remote.ip}:{remote.port} (via {local.type})"
        )
        if self.on_remote_address and self.selected_address:
            self.on_remote_address(self.selected_address)

    async def wait_for_connection(self, timeout: float | None = None) -> bool:
        """等待 ICE 连接成功"""
        if timeout is None:
            timeout = self.config.connectivity_check_timeout

        try:
            await asyncio.wait_for(self._connected_event.wait(), timeout=timeout)
            return True
        except asyncio.TimeoutError:
            logger.warning("[ICE] Connection timeout")
            return False

    def get_relay_candidates(self) -> list[LocalIceCandidate]:
        """获取 TURN 中继候选地址"""
        return [c for c in self.local_candidates if "typ relay" in c.candidate.lower()]

    def get_server_reflexive_candidates(self) -> list[LocalIceCandidate]:
        """获取 STUN 服务器反射候选"""
        return [c for c in self.local_candidates if "typ srflx" in c.candidate.lower()]

    def get_host_candidates(self) -> list[LocalIceCandidate]:
        """获取主机候选地址"""
        return [c for c in self.local_candidates if "typ host" in c.candidate.lower()]

    async def close(self) -> None:
        """关闭 ICE 管理器"""
        logger.info("[ICE] Closing ICE manager")
        self._set_state(ConnectionState.CLOSED)

        if self._pc:
            try:
                await self._pc.close()
            except Exception as e:
                logger.debug(f"[ICE] Error closing peer connection: {e}")
            self._pc = None

        logger.info("[ICE] ICE manager closed")
