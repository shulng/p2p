"""ICE 管理模块 - 集成 TURN (支持 Cloudflare TURN: turn.cloudflare.com:3478)"""
from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass, field
from typing import Callable, List, Optional, Dict, Any, Tuple
from loguru import logger

try:
    from aiortc import RTCPeerConnection, RTCSessionDescription, RTCIceCandidate
    from aiortc.rtcconfiguration import RTCConfiguration, RTCIceServer
    AIORTC_AVAILABLE = True
except ImportError:
    AIORTC_AVAILABLE = False
    logger.warning("aiortc not installed, ICE/TURN features will be limited")

from .config import IceConfig, TurnServerConfig
from .types import (
    ConnectionState,
    SessionDescription,
    IceCandidate as LocalIceCandidate,
    generate_peer_id,
)


class IceManager:
    """ICE 连接管理器 - 处理 STUN/TURN 候选地址收集和连接"""

    def __init__(
        self,
        config: IceConfig,
        on_ice_candidate: Optional[Callable[[LocalIceCandidate], None]] = None,
        on_connection_state: Optional[Callable[[ConnectionState], None]] = None,
        on_ice_gathering_done: Optional[Callable[[], None]] = None,
        on_remote_address: Optional[Callable[[Tuple[str, int]], None]] = None,
    ):
        self.config = config
        self.on_ice_candidate = on_ice_candidate
        self.on_connection_state = on_connection_state
        self.on_ice_gathering_done = on_ice_gathering_done
        self.on_remote_address = on_remote_address
        
        # 状态
        self.state: ConnectionState = ConnectionState.DISCONNECTED
        self.ice_state: Optional[str] = None
        self.gathering_done: bool = False
        
        # PeerConnection (aiortc)
        self._pc: Optional["RTCPeerConnection"] = None
        self._peer_id: str = generate_peer_id()
        
        # 事件
        self._gathering_done_event: asyncio.Event = asyncio.Event()
        self._connected_event: asyncio.Event = asyncio.Event()
        
        # 收集到的候选地址
        self.local_candidates: List[LocalIceCandidate] = []
        self.remote_candidates: List[LocalIceCandidate] = []
        
        # 选中的候选对
        self.selected_local_candidate: Optional[LocalIceCandidate] = None
        self.selected_remote_candidate: Optional[LocalIceCandidate] = None
        self.selected_address: Optional[Tuple[str, int]] = None  # (host, port)

    @property
    def peer_id(self) -> str:
        return self._peer_id

    def _set_state(self, state: ConnectionState) -> None:
        if self.state != state:
            old_state = self.state
            self.state = state
            logger.info(f"[ICE] State changed: {old_state} -> {state}")
            if self.on_connection_state:
                self.on_connection_state(state)

    def _build_rtc_config(self) -> Optional["RTCConfiguration"]:
        """构建 aiortc RTCConfiguration"""
        if not AIORTC_AVAILABLE:
            return None
        
        ice_servers = []
        for turn_cfg in self.config.ice_servers:
            server_kwargs = {"urls": [turn_cfg.url]}
            
            if turn_cfg.username:
                server_kwargs["username"] = turn_cfg.username
            if turn_cfg.credential:
                server_kwargs["credential"] = turn_cfg.credential
            
            ice_servers.append(RTCIceServer(**server_kwargs))
        
        # 特别为 Cloudflare TURN 配置
        has_cloudflare = any(s.use_cloudflare for s in self.config.ice_servers)
        if has_cloudflare:
            logger.info("[ICE] Using Cloudflare TURN servers (turn.cloudflare.com:3478)")
        
        return RTCConfiguration(
            iceServers=ice_servers,
            iceTransportPolicy=self.config.ice_transport_policy,
        )

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
        
        # 添加一个数据通道来触发 ICE
        try:
            data_channel = self._pc.createDataChannel("p2p-ice")
            logger.debug("[ICE] Created data channel for ICE triggering")
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
        
        await self._pc.setRemoteDescription(
            RTCSessionDescription(sdp=desc.sdp, type=desc.sdp_type)
        )
        logger.info(f"[ICE] Remote description set, type={desc.sdp_type}")

    async def add_ice_candidate(self, candidate: LocalIceCandidate) -> None:
        """添加远端 ICE 候选"""
        if not self._pc:
            logger.warning("[ICE] add_ice_candidate called without peer connection")
            return
        
        try:
            ice_candidate = RTCIceCandidate(
                candidate=candidate.candidate,
                sdpMid=candidate.sdp_mid,
                sdpMLineIndex=candidate.sdp_mline_index,
            )
            await self._pc.addIceCandidate(ice_candidate)
            self.remote_candidates.append(candidate)
            logger.debug(f"[ICE] Added remote candidate: {candidate.candidate[:60]}...")
        except Exception as e:
            logger.error(f"[ICE] Failed to add ICE candidate: {e}")

    def _register_events(self) -> None:
        """注册 aiortc 事件"""
        if not self._pc:
            return
        
        @self._pc.on("icecandidate")
        async def on_ice_candidate(candidate):
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

        @self._pc.on("iceconnectionstatechange")
        async def on_ice_state_change():
            state = self._pc.iceConnectionState
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

        @self._pc.on("connectionstatechange")
        async def on_connection_state():
            state = self._pc.connectionState
            logger.info(f"[ICE] Connection state: {state}")

    async def _extract_selected_candidates(self) -> None:
        """尝试提取选中的候选地址（用于直接 UDP 传输）"""
        if not self._pc:
            return
        
        try:
            # 从 aiortc 内部获取选中的候选对
            transports = getattr(self._pc, "_transports", {})
            for name, transport in transports.items():
                ice_transport = getattr(transport, "iceTransport", None)
                if ice_transport:
                    # 获取选中的候选对
                    selected_pair = getattr(ice_transport, "_selected_pair", None)
                    if selected_pair:
                        local = selected_pair.localCandidate
                        remote = selected_pair.remoteCandidate
                        
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
                        break
        except Exception as e:
            logger.debug(f"[ICE] Could not extract selected candidates: {e}")

    async def wait_for_connection(self, timeout: Optional[float] = None) -> bool:
        """等待 ICE 连接成功"""
        if timeout is None:
            timeout = self.config.connectivity_check_timeout
        
        try:
            await asyncio.wait_for(self._connected_event.wait(), timeout=timeout)
            return True
        except asyncio.TimeoutError:
            logger.warning("[ICE] Connection timeout")
            return False

    def get_relay_candidates(self) -> List[LocalIceCandidate]:
        """获取 TURN 中继候选地址"""
        return [
            c for c in self.local_candidates
            if "typ relay" in c.candidate.lower()
        ]

    def get_server_reflexive_candidates(self) -> List[LocalIceCandidate]:
        """获取 STUN 服务器反射候选"""
        return [
            c for c in self.local_candidates
            if "typ srflx" in c.candidate.lower()
        ]

    def get_host_candidates(self) -> List[LocalIceCandidate]:
        """获取主机候选地址"""
        return [
            c for c in self.local_candidates
            if "typ host" in c.candidate.lower()
        ]

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
