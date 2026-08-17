"""KCP 传输管理器 - 数据统一走 KCP,控制信令走 SCTP (DataChannel)

通道策略:
  - 控制信令 (control): SCTP (DataChannel) — 始终可用,含 TURN 场景
  - 数据 (data):         KCP 直连 — 低延迟,失败降级 SCTP

工作流程:
  1. ICE 建立后 SCTP 通道可用
  2. RESPONDER 绑定 KCP 监听端口
  3. 通过 SCTP 交换双方的 KCP 地址
  4. INITIATOR 用收到的地址发起 KCP 直连
  5. 数据发送走 KCP,直连不可用则降级到 SCTP
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import socket
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from loguru import logger

from .._utils import spawn_task, wait_event
from ..config import ConnectionRole, KcpConfig
from .kcp import KCPTransport

# 通道类型
CHANNEL_CONTROL = "control"  # 控制信令 → SCTP
CHANNEL_DATA = "data"  # 数据 → KCP

# SCTP 上的 KCP 传输管理消息类型(通过 DataChannel 发送)
KCP_MSG_ADDR = "kcp.addr"  # 交换 KCP 监听地址


@dataclass
class KcpTransportStats:
    """KCP 传输统计"""

    sctp_bytes_sent: int = 0
    sctp_bytes_recv: int = 0
    kcp_bytes_sent: int = 0
    kcp_bytes_recv: int = 0
    kcp_ready: bool = False


class KCPDataTransport:
    """KCP 传输管理器 — 数据统一走 KCP,控制走 SCTP

    使用方式:
      1. ICE 建立后调用 ``on_sctp_ready(send_cb)``
      2. RESPONDER 调用 ``start_server()`` 绑定 KCP 端口
      3. 双方调用 ``exchange_address()`` 通过 SCTP 交换地址
      4. INITIATOR 收到地址后自动发起 KCP 连接
      5. 发送数据时调用 ``send(data, channel)``,接收端通过 ``on_data`` 回调
    """

    def __init__(
        self,
        role: ConnectionRole,
        kcp_config: KcpConfig | None = None,
    ):
        self.role = role
        self.kcp_config = kcp_config or KcpConfig()

        # 传输实例
        self._kcp: KCPTransport | None = None

        # 通道就绪状态
        self._kcp_ready = False
        self._sctp_ready = False

        # SCTP 发送回调 (由 P2PNode 注入: bytes -> bool)
        self._sctp_send: Callable[[bytes], bool] | None = None

        # 数据接收回调: (data: bytes, channel: str) -> None
        self.on_data: Callable[[bytes, str], None] | None = None

        # 对端的 KCP 地址 + conv(RESPONDER 分配,INITIATOR 复用)
        self._remote_kcp_addr: tuple[str, int] | None = None
        self._remote_kcp_conv: int | None = None

        # 本地 KCP 监听地址
        self._local_kcp_addr: tuple[str, int] | None = None

        # 统计
        self.stats = KcpTransportStats()

        # 地址交换事件
        self._addr_exchanged: asyncio.Event = asyncio.Event()

    # ========== 属性 ==========

    @property
    def kcp_ready(self) -> bool:
        """KCP 直连通道是否就绪。"""
        return self._kcp_ready

    @property
    def sctp_ready(self) -> bool:
        """SCTP (DataChannel) 通道是否就绪。"""
        return self._sctp_ready

    @property
    def active_channels(self) -> list[str]:
        """当前可用的通道列表"""
        channels: list[str] = []
        if self._sctp_ready:
            channels.append(CHANNEL_CONTROL)
        if self._kcp_ready:
            channels.append(CHANNEL_DATA)
        return channels

    # ========== SCTP 集成 ==========

    def on_sctp_ready(self, sctp_send_cb: Callable[[bytes], bool]) -> None:
        """SCTP (DataChannel) 就绪,注入发送回调"""
        self._sctp_ready = True
        self._sctp_send = sctp_send_cb
        logger.info("[KCP] SCTP channel ready")

    def on_sctp_data(self, data: bytes) -> None:
        """SCTP 收到数据 — 判断是管理消息还是业务数据"""
        # 尝试解析为 KCP 管理消息
        try:
            text = data.decode("utf-8", errors="strict")
            # 仅当 type 字段精确等于 KCP_MSG_ADDR 时才作为管理消息处理，
            # 避免业务 JSON 中恰好包含 "kcp.addr" 子串时被误判并丢弃数据。
            if text.startswith("{"):
                msg = json.loads(text)
                if msg.get("type") == KCP_MSG_ADDR:
                    self._handle_addr_exchange(msg)
                    return
        except (UnicodeDecodeError, json.JSONDecodeError):
            pass

        # 不是管理消息 → 作为 control 通道数据回调
        self.stats.sctp_bytes_recv += len(data)
        if self.on_data:
            self.on_data(data, CHANNEL_CONTROL)

    # ========== 服务端绑定 (RESPONDER) ==========

    async def start_server(self) -> None:
        """RESPONDER 端:绑定 KCP 监听端口"""
        if self.role != ConnectionRole.RESPONDER:
            logger.warning("[KCP] start_server() only for RESPONDER")
            return

        try:
            self._kcp = KCPTransport(
                config=self.kcp_config,
                on_data_received=self._on_kcp_data,
            )
            self._local_kcp_addr = await self._kcp.bind()
            # 开始接受连接(异步等待握手)，统一经 spawn_task 接管异常
            spawn_task(self._kcp.accept_connection(), context="KCP accept_connection")
            logger.info(f"[KCP] server bound on {self._local_kcp_addr}")
        except Exception as e:
            logger.warning(f"[KCP] bind failed: {e}")
            self._kcp = None

    # ========== 地址交换 ==========

    @staticmethod
    def _reachable_addr(addr: tuple[str, int] | None) -> tuple[str, int] | None:
        """将 0.0.0.0/空 绑定地址转换为对端可达的真实 IP

        KCP 直连是快速路径(同机/LAN 可达,跨 NAT 失败则降级 SCTP)。
        广播 0.0.0.0 会导致对端 connect 报错(WinError 10049),需替换为真实本机 IP。
        """
        if not addr:
            return None
        host, port = addr
        if host and host != "0.0.0.0" and host != "::":
            return addr
        # 探测本机出口 IP(不真正发包,仅让 OS 选路填充源地址)
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.connect(("8.8.8.8", 80))
            real_ip = s.getsockname()[0]
            s.close()
            return (real_ip, port)
        except Exception:
            return ("127.0.0.1", port)

    async def exchange_address(self) -> None:
        """通过 SCTP 发送本地的 KCP 地址给对端"""
        if not self._sctp_ready or not self._sctp_send:
            logger.warning("[KCP] SCTP not ready, cannot exchange address")
            return

        # RESPONDER 需要先 start_server 才有地址
        # INITIATOR 的地址在 connect 时由 KCP 库自动分配
        # 广播前把 0.0.0.0 替换为真实可达 IP,避免对端 connect 失败
        kcp_adv = self._reachable_addr(self._local_kcp_addr)
        # KCP 协议要求双方 conv 一致才能通信:RESPONDER 把自己的 conv 一起广播,
        # INITIATOR 用它构造 KCPTransport(否则 conv 不匹配,握手包被对端 KCP 丢弃)
        kcp_conv = self._kcp.conv_id if self._kcp else None
        addr_msg = {
            "type": KCP_MSG_ADDR,
            "kcp_addr": list(kcp_adv) if kcp_adv else None,
            "kcp_conv": kcp_conv,
            "ts": time.time(),
        }
        payload = json.dumps(addr_msg).encode("utf-8")
        if self._sctp_send(payload):
            logger.info(f"[KCP] Sent addr: kcp={kcp_adv}, conv={kcp_conv}")

    def _handle_addr_exchange(self, msg: dict[str, Any]) -> None:
        """收到对端的 KCP 地址"""
        kcp = msg.get("kcp_addr")
        # KCP conv 由 RESPONDER 分配,INITIATOR 必须复用以匹配会话
        self._remote_kcp_conv = msg.get("kcp_conv")
        if kcp:
            self._remote_kcp_addr = (kcp[0], kcp[1])
            logger.info(f"[KCP] Remote addr: {self._remote_kcp_addr}, conv={self._remote_kcp_conv}")

        # 地址已收到,立即完成交换等待(直连在后台进行,不阻塞 wait_for_addr_exchange)
        self._addr_exchanged.set()

        # INITIATOR 收到地址后发起直连(后台,失败自动降级 SCTP)
        if self.role == ConnectionRole.INITIATOR and self._remote_kcp_addr:
            spawn_task(self._initiate_direct_connect(), context="KCP initiate direct connect")

    async def _initiate_direct_connect(self) -> None:
        """INITIATOR 端:用收到的地址发起 KCP 直连"""
        if self._remote_kcp_addr:
            try:
                # 用 RESPONDER 广播的 conv 构造,确保 KCP 会话 ID 一致
                self._kcp = KCPTransport(
                    config=self.kcp_config,
                    conv_id=self._remote_kcp_conv,
                    on_data_received=self._on_kcp_data,
                )
                await self._kcp.bind()
                if await self._kcp.connect(self._remote_kcp_addr):
                    self._kcp_ready = True
                    self.stats.kcp_ready = True
                    logger.info("[KCP] direct connect established")
                else:
                    logger.warning("[KCP] direct connect failed, fallback to SCTP")
            except Exception as e:
                logger.warning(f"[KCP] connect error: {e}")

        self._addr_exchanged.set()
        self._log_status()

    def _log_status(self) -> None:
        """打印当前通道状态"""
        channels = []
        if self._sctp_ready:
            channels.append("SCTP")
        if self._kcp_ready:
            channels.append("KCP")
        logger.info(f"[KCP] Active channels: {', '.join(channels) if channels else 'none'}")

    # ========== 数据接收回调 ==========

    def _on_kcp_data(self, data: bytes) -> None:
        """KCP 收到数据 → data 通道"""
        self.stats.kcp_bytes_recv += len(data)
        if self.on_data:
            self.on_data(data, CHANNEL_DATA)

    # ========== 数据发送 ==========

    async def send(self, data: bytes, channel: str = CHANNEL_DATA) -> bool:
        """按通道类型选择传输协议发送数据

        Args:
            data: 要发送的数据
            channel: CHANNEL_CONTROL / CHANNEL_DATA

        Returns:
            True 表示发送成功
        """
        if channel == CHANNEL_CONTROL:
            return self._send_sctp(data)

        if channel == CHANNEL_DATA:
            # 优先 KCP,降级 SCTP
            if self._kcp_ready and self._kcp:
                ok = await self._kcp.send(data)
                if ok:
                    self.stats.kcp_bytes_sent += len(data)
                    return True
                logger.debug("[KCP] send failed, fallback to SCTP")
                self._kcp_ready = False
                self.stats.kcp_ready = False
            return self._send_sctp(data)

        logger.warning(f"[KCP] Unknown channel: {channel}")
        return False

    def _send_sctp(self, data: bytes) -> bool:
        """通过 SCTP (DataChannel) 发送"""
        if not self._sctp_ready or not self._sctp_send:
            logger.warning("[KCP] SCTP not ready")
            return False
        ok = self._sctp_send(data)
        if ok:
            self.stats.sctp_bytes_sent += len(data)
        return ok

    # ========== 生命周期 ==========

    async def wait_for_addr_exchange(self, timeout: float = 10.0) -> bool:
        """等待地址交换完成"""
        return await wait_event(
            self._addr_exchanged, timeout=timeout, context="KCP address exchange"
        )

    def get_stats(self) -> KcpTransportStats:
        """返回当前传输统计，并同步通道就绪状态。"""
        self.stats.kcp_ready = self._kcp_ready
        return self.stats

    async def close(self) -> None:
        """关闭所有通道"""
        logger.info("[KCP] Closing all channels")

        if self._kcp:
            with contextlib.suppress(Exception):
                await self._kcp.close()
            self._kcp = None
            self._kcp_ready = False

        self._sctp_ready = False
        self._sctp_send = None
        logger.info("[KCP] All channels closed")
