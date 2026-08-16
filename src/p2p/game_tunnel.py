"""通用 P2P 隧道 - 通过 P2P 连接转发 TCP/UDP 流量

用途：在两个节点间转发任意 TCP/UDP 流量（游戏联机、服务代理等）
架构：
  节点A → localhost:PORT → P2P DataChannel → 节点B → localhost:PORT (目标服务)

支持：
  - TCP 流量转发（如 Minecraft Java、Terraria、SSH）
  - UDP 流量转发（如 Minecraft Bedrock、饥荒联机版）
  - 同时转发 TCP + UDP（both 模式，端口可独立配置）
"""
from __future__ import annotations

import asyncio
import time
import uuid
from dataclasses import dataclass, field
from typing import Callable, Dict, Optional, Tuple, Any
from loguru import logger

from .config import P2PConfig, TransportProtocol, ConnectionRole, IceConfig
from .node import P2PNode
from .types import Message, MessageType, ConnectionState


# 隧道消息类型（复用 Message 结构，payload 为 dict）
TUNNEL_TCP_OPEN = "tunnel.tcp.open"     # 打开 TCP 隧道
TUNNEL_TCP_CLOSE = "tunnel.tcp.close"   # 关闭 TCP 隧道
TUNNEL_TCP_DATA = "tunnel.tcp.data"     # TCP 隧道数据
TUNNEL_UDP_DATA = "tunnel.udp.data"     # UDP 数据（双向）
TUNNEL_UDP_CLOSE = "tunnel.udp.close"   # 关闭 UDP 会话

# UDP 会话空闲超时（秒），超时后清理 HOST 端的 relay
UDP_SESSION_TIMEOUT = 60.0


@dataclass
class TunnelConfig:
    """通用隧道配置"""
    # 本地监听地址（CLIENT 端，用户/游戏客户端连接这里）
    local_listen_host: str = "127.0.0.1"
    local_listen_port: int = 0  # TCP 监听端口，0 表示必填

    # 远端转发目标（HOST 端，连接到本地目标服务）
    remote_forward_host: str = "127.0.0.1"
    remote_forward_port: int = 0  # TCP 转发端口，0 表示必填

    # 协议类型: tcp / udp / both
    protocol: str = "tcp"

    # UDP 专用端口（可选，both 模式下 TCP/UDP 可用不同端口；None 时与 TCP 端口相同）
    local_listen_port_udp: Optional[int] = None
    remote_forward_port_udp: Optional[int] = None

    # 隧道名称（仅用于日志显示）
    name: str = "tunnel"


class _UdpClientProtocol(asyncio.DatagramProtocol):
    """CLIENT 端本地 UDP 监听协议

    接收本地 UDP 客户端数据包，按源地址分配 conn_id 并通过 P2P 转发。
    """

    def __init__(
        self,
        on_datagram: Callable[[Tuple[str, int], bytes], None],
    ):
        self.on_datagram = on_datagram
        self.transport: Optional[asyncio.DatagramTransport] = None

    def connection_made(self, transport: asyncio.DatagramTransport) -> None:
        self.transport = transport

    def datagram_received(self, data: bytes, addr: Tuple[str, int]) -> None:
        self.on_datagram(addr, data)

    def error_received(self, exc: Exception) -> None:
        logger.error(f"[Tunnel-UDP] Listen error: {exc}")


class _UdpRelayProtocol(asyncio.DatagramProtocol):
    """HOST 端 UDP 中继协议

    为每个 UDP 会话创建一个到本地目标服务的 UDP socket，
    收到本地服务回包后通过回调转发回 CLIENT。
    """

    def __init__(
        self,
        conn_id: str,
        on_datagram: Callable[[str, bytes], None],
        on_close: Callable[[str], None],
    ):
        self.conn_id = conn_id
        self.on_datagram = on_datagram
        self.on_close = on_close
        self.transport: Optional[asyncio.DatagramTransport] = None
        self.last_activity: float = time.monotonic()

    def connection_made(self, transport: asyncio.DatagramTransport) -> None:
        self.transport = transport
        self.last_activity = time.monotonic()

    def datagram_received(self, data: bytes, addr: Tuple[str, int]) -> None:
        self.last_activity = time.monotonic()
        self.on_datagram(self.conn_id, data)

    def error_received(self, exc: Exception) -> None:
        logger.error(f"[Tunnel-UDP] Relay error #{self.conn_id}: {exc}")

    def connection_lost(self, exc: Optional[Exception]) -> None:
        self.on_close(self.conn_id)


class GameTunnel:
    """通用 P2P 隧道 - 在 P2P 连接上转发 TCP/UDP 流量

    角色：
      - HOST (RESPONDER): 运行目标服务端的人，接收 P2P 数据转发到本地服务
      - CLIENT (INITIATOR): 运行客户端的人，本地起监听供用户客户端接入
    """

    def __init__(
        self,
        p2p_config: P2PConfig,
        tunnel_config: TunnelConfig,
        role: ConnectionRole,
    ):
        self.p2p_config = p2p_config
        self.tunnel_config = tunnel_config
        self.role = role

        # TCP 隧道: conn_id -> (reader, writer)
        self._tcp_tunnels: Dict[str, Tuple[Optional[asyncio.StreamReader], Optional[asyncio.StreamWriter]]] = {}
        # TCP 隧道建立前的数据缓冲: conn_id -> list[bytes]
        self._tcp_pending: Dict[str, list] = {}

        # UDP 会话 (CLIENT 端): client_addr -> conn_id, conn_id -> client_addr
        self._udp_client_to_conn: Dict[Tuple[str, int], str] = {}
        self._udp_conn_to_client: Dict[str, Tuple[str, int]] = {}

        # UDP 会话 (HOST 端): conn_id -> _UdpRelayProtocol
        self._udp_relays: Dict[str, _UdpRelayProtocol] = {}

        # 本地服务器
        self._tcp_server: Optional[asyncio.AbstractServer] = None
        self._udp_listen_transport: Optional[asyncio.DatagramTransport] = None
        self._udp_cleanup_task: Optional[asyncio.Task] = None

        # P2P 节点
        self._node: Optional[P2PNode] = None
        self._peer_id: Optional[str] = None  # 首个连接的 peer（CLIENT 端单 peer 用）
        self._peer_ids: set = set()  # 所有已连接的 peer（HOST 端多 peer 用）
        self._conn_to_peer: Dict[str, str] = {}  # conn_id -> peer_id（回包路由）
        self._connected: bool = False
        self._peer_connected_event: asyncio.Event = asyncio.Event()

        # 统计
        self._bytes_forwarded: int = 0
        self._connections_count: int = 0

    async def start(self, signaling_url: str, room_id: str) -> None:
        """启动隧道"""
        proto = self.tunnel_config.protocol.lower()
        logger.info(f"=== Tunnel Starting ({self.role.value}) ===")
        logger.info(f"Name: {self.tunnel_config.name} | Protocol: {proto.upper()}")

        # 校验端口配置
        self._validate_config()

        # 设置 P2P 节点
        self._node = P2PNode(
            config=self.p2p_config,
            on_message=self._on_p2p_message,
            on_peer_connected=self._on_peer_connected,
            on_peer_disconnected=self._on_peer_disconnected,
        )
        await self._node.initialize()
        await self._node.connect_to_signaling()
        await self._node.join_room(room_id, self.role)
        logger.info(f"Joined room: {room_id}")

        # 等待 P2P 连接建立
        logger.info("Waiting for P2P connection...")
        await self._peer_connected_event.wait()

        if self.role == ConnectionRole.RESPONDER:
            # HOST 端：无需本地监听，直接接收 P2P 数据转发到本地服务
            logger.info(
                f"Ready! Forwarding P2P -> "
                f"{self.tunnel_config.remote_forward_host}:"
                f"{self._remote_tcp_port()}"
                + (f" (udp {self._remote_udp_port()})" if proto in ("udp", "both") else "")
            )
        else:
            # CLIENT 端：启动本地监听
            if proto in ("tcp", "both"):
                await self._start_local_tcp_server()
                logger.info(
                    f"TCP: connect your client to "
                    f"{self.tunnel_config.local_listen_host}:{self.tunnel_config.local_listen_port}"
                )
            if proto in ("udp", "both"):
                await self._start_local_udp_server()
                logger.info(
                    f"UDP: connect your client to "
                    f"{self.tunnel_config.local_listen_host}:{self._local_udp_port()}"
                )
                self._udp_cleanup_task = asyncio.create_task(self._cleanup_udp_sessions())

        logger.info("=== Tunnel Active ===")

    def _validate_config(self) -> None:
        """校验配置（错误信息对齐 CLI 子命令 server/client 的 --tcp/--udp 参数名）"""
        proto = self.tunnel_config.protocol.lower()
        if proto not in ("tcp", "udp", "both"):
            raise ValueError(f"Invalid protocol: {self.tunnel_config.protocol}")

        if self.role == ConnectionRole.RESPONDER:
            # SERVER 端需要目标端口
            if proto in ("tcp", "both") and not self.tunnel_config.remote_forward_port:
                raise ValueError("SERVER 需要 --tcp PORT 指定 TCP 目标端口")
            if proto in ("udp", "both") and not self._remote_udp_port():
                raise ValueError("SERVER 需要 --udp PORT 指定 UDP 目标端口")
        else:
            # CLIENT 端需要本地监听端口
            if proto in ("tcp", "both") and not self.tunnel_config.local_listen_port:
                raise ValueError("CLIENT 需要 --tcp PORT 指定本地 TCP 监听端口")
            if proto in ("udp", "both") and not self._local_udp_port():
                raise ValueError("CLIENT 需要 --udp PORT 指定本地 UDP 监听端口")

    def _local_udp_port(self) -> int:
        return self.tunnel_config.local_listen_port_udp or self.tunnel_config.local_listen_port

    def _remote_udp_port(self) -> int:
        return self.tunnel_config.remote_forward_port_udp or self.tunnel_config.remote_forward_port

    def _remote_tcp_port(self) -> int:
        return self.tunnel_config.remote_forward_port

    # ========== TCP 转发 ==========

    async def _start_local_tcp_server(self) -> None:
        """CLIENT 端：启动本地 TCP 监听"""
        self._tcp_server = await asyncio.start_server(
            self._handle_local_tcp_connection,
            self.tunnel_config.local_listen_host,
            self.tunnel_config.local_listen_port,
        )
        logger.info(
            f"[Tunnel-TCP] Local server listening on "
            f"{self.tunnel_config.local_listen_host}:{self.tunnel_config.local_listen_port}"
        )

    async def _handle_local_tcp_connection(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        """处理本地 TCP 客户端连接"""
        conn_id = uuid.uuid4().hex[:12]
        peer_addr = writer.get_extra_info("peername")
        logger.info(f"[Tunnel-TCP] Local connection #{conn_id} from {peer_addr}")

        self._tcp_tunnels[conn_id] = (reader, writer)
        self._connections_count += 1
        # CLIENT 端：记录该连接对应的目标 peer（用于回包路由）
        if self._peer_id:
            self._conn_to_peer[conn_id] = self._peer_id

        await self._send_tunnel_message(TUNNEL_TCP_OPEN, conn_id, b"")

        try:
            while conn_id in self._tcp_tunnels:
                data = await reader.read(65536)
                if not data:
                    break
                self._bytes_forwarded += len(data)
                await self._send_tunnel_message(TUNNEL_TCP_DATA, conn_id, data)
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.error(f"[Tunnel-TCP] Local read error #{conn_id}: {e}")
        finally:
            await self._send_tunnel_message(TUNNEL_TCP_CLOSE, conn_id, b"")
            writer.close()
            try:
                await writer.wait_closed()
            except Exception:
                pass
            self._tcp_tunnels.pop(conn_id, None)
            logger.info(f"[Tunnel-TCP] Local connection #{conn_id} closed")

    async def _handle_remote_tcp_open(self, conn_id: str) -> None:
        """HOST 端：远端打开 TCP 隧道 -> 连接本地目标服务"""
        try:
            reader, writer = await asyncio.open_connection(
                self.tunnel_config.remote_forward_host,
                self.tunnel_config.remote_forward_port,
            )
            self._tcp_tunnels[conn_id] = (reader, writer)
            logger.info(
                f"[Tunnel-TCP] Connected to local target #{conn_id} -> "
                f"{self.tunnel_config.remote_forward_host}:{self.tunnel_config.remote_forward_port}"
            )

            # 刷新缓冲数据
            if conn_id in self._tcp_pending:
                for buffered in self._tcp_pending.pop(conn_id):
                    writer.write(buffered)
                await writer.drain()

            # 启动转发循环：本地服务 -> P2P
            asyncio.create_task(self._forward_tcp_local_to_remote(conn_id, reader))
        except Exception as e:
            logger.error(f"[Tunnel-TCP] Failed to connect to local target #{conn_id}: {e}")
            self._tcp_pending.pop(conn_id, None)
            self._conn_to_peer.pop(conn_id, None)
            await self._send_tunnel_message(TUNNEL_TCP_CLOSE, conn_id, b"")

    async def _forward_tcp_local_to_remote(self, conn_id: str, reader: asyncio.StreamReader) -> None:
        """HOST 端：从本地目标服务读取数据，转发到远端"""
        try:
            while conn_id in self._tcp_tunnels:
                data = await reader.read(65536)
                if not data:
                    break
                self._bytes_forwarded += len(data)
                await self._send_tunnel_message(TUNNEL_TCP_DATA, conn_id, data)
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.error(f"[Tunnel-TCP] Forward error #{conn_id}: {e}")
        finally:
            await self._send_tunnel_message(TUNNEL_TCP_CLOSE, conn_id, b"")
            self._tcp_tunnels.pop(conn_id, None)
            logger.info(f"[Tunnel-TCP] Remote forwarding #{conn_id} closed")

    async def _handle_remote_tcp_data(self, conn_id: str, data: bytes) -> None:
        """处理远端 TCP 数据"""
        tunnel = self._tcp_tunnels.get(conn_id)
        if tunnel and tunnel[1]:
            try:
                tunnel[1].write(data)
                await tunnel[1].drain()
            except Exception as e:
                logger.error(f"[Tunnel-TCP] Write error #{conn_id}: {e}")
                await self._handle_remote_tcp_close(conn_id)
        else:
            # 隧道尚未建立，缓冲数据
            if conn_id not in self._tcp_pending:
                self._tcp_pending[conn_id] = []
            self._tcp_pending[conn_id].append(data)

    async def _handle_remote_tcp_close(self, conn_id: str) -> None:
        """远端关闭 TCP 隧道"""
        tunnel = self._tcp_tunnels.pop(conn_id, None)
        self._tcp_pending.pop(conn_id, None)
        self._conn_to_peer.pop(conn_id, None)
        if tunnel and tunnel[1]:
            try:
                tunnel[1].close()
                await tunnel[1].wait_closed()
            except Exception:
                pass
            logger.info(f"[Tunnel-TCP] Connection #{conn_id} closed by remote")

    # ========== UDP 转发 ==========

    async def _start_local_udp_server(self) -> None:
        """CLIENT 端：启动本地 UDP 监听"""
        loop = asyncio.get_event_loop()
        transport, protocol = await loop.create_datagram_endpoint(
            lambda: _UdpClientProtocol(on_datagram=self._on_local_udp_datagram),
            local_addr=(self.tunnel_config.local_listen_host, self._local_udp_port()),
        )
        self._udp_listen_transport = transport
        logger.info(
            f"[Tunnel-UDP] Local server listening on "
            f"{self.tunnel_config.local_listen_host}:{self._local_udp_port()}"
        )

    def _on_local_udp_datagram(self, addr: Tuple[str, int], data: bytes) -> None:
        """CLIENT 端：收到本地 UDP 客户端数据包"""
        conn_id = self._udp_client_to_conn.get(addr)
        if conn_id is None:
            conn_id = uuid.uuid4().hex[:12]
            self._udp_client_to_conn[addr] = conn_id
            self._udp_conn_to_client[conn_id] = addr
            self._connections_count += 1
            # CLIENT 端：记录该会话对应的目标 peer（用于回包路由）
            if self._peer_id:
                self._conn_to_peer[conn_id] = self._peer_id
            logger.debug(f"[Tunnel-UDP] New session #{conn_id} from {addr}")

        self._bytes_forwarded += len(data)
        asyncio.create_task(self._send_tunnel_message(TUNNEL_UDP_DATA, conn_id, data))

    async def _handle_remote_udp_data(self, conn_id: str, data: bytes) -> None:
        """处理远端 UDP 数据"""
        if self.role == ConnectionRole.INITIATOR:
            # CLIENT 端：收到来自 HOST 的回包，转发回本地客户端
            addr = self._udp_conn_to_client.get(conn_id)
            if addr and self._udp_listen_transport:
                self._bytes_forwarded += len(data)
                self._udp_listen_transport.sendto(data, addr)
            else:
                logger.warning(f"[Tunnel-UDP] No client for #{conn_id}, dropping {len(data)} bytes")
        else:
            # HOST 端：收到 CLIENT 的数据，转发到本地目标服务
            relay = self._udp_relays.get(conn_id)
            if relay is None or relay.transport is None:
                # 创建新的 UDP relay 连接到本地目标
                try:
                    loop = asyncio.get_event_loop()
                    transport, protocol = await loop.create_datagram_endpoint(
                        lambda: _UdpRelayProtocol(
                            conn_id=conn_id,
                            on_datagram=self._on_udp_reay_datagram,
                            on_close=self._on_udp_relay_close,
                        ),
                        remote_addr=(
                            self.tunnel_config.remote_forward_host,
                            self._remote_udp_port(),
                        ),
                    )
                    relay = protocol
                    self._udp_relays[conn_id] = relay
                    logger.info(
                        f"[Tunnel-UDP] New relay #{conn_id} -> "
                        f"{self.tunnel_config.remote_forward_host}:{self._remote_udp_port()}"
                    )
                except Exception as e:
                    logger.error(f"[Tunnel-UDP] Failed to create relay #{conn_id}: {e}")
                    return

            relay.last_activity = time.monotonic()
            self._bytes_forwarded += len(data)
            relay.transport.sendto(data)

    def _on_udp_reay_datagram(self, conn_id: str, data: bytes) -> None:
        """HOST 端：本地目标服务回包 -> 通过 P2P 转发回 CLIENT"""
        self._bytes_forwarded += len(data)
        asyncio.create_task(self._send_tunnel_message(TUNNEL_UDP_DATA, conn_id, data))

    def _on_udp_relay_close(self, conn_id: str) -> None:
        """HOST 端：UDP relay 关闭"""
        self._udp_relays.pop(conn_id, None)
        asyncio.create_task(self._send_tunnel_message(TUNNEL_UDP_CLOSE, conn_id, b""))
        logger.info(f"[Tunnel-UDP] Relay #{conn_id} closed")

    async def _cleanup_udp_sessions(self) -> None:
        """HOST 端：定期清理空闲的 UDP relay"""
        while True:
            await asyncio.sleep(10)
            now = time.monotonic()
            expired = [
                cid for cid, relay in self._udp_relays.items()
                if now - relay.last_activity > UDP_SESSION_TIMEOUT
            ]
            for cid in expired:
                relay = self._udp_relays.pop(cid, None)
                if relay and relay.transport:
                    relay.transport.close()
                logger.info(f"[Tunnel-UDP] Cleaned up idle session #{cid}")

    async def _handle_remote_udp_close(self, conn_id: str) -> None:
        """处理远端关闭 UDP 会话"""
        if self.role == ConnectionRole.INITIATOR:
            # CLIENT 端：清理本地映射
            addr = self._udp_conn_to_client.pop(conn_id, None)
            if addr:
                self._udp_client_to_conn.pop(addr, None)
        else:
            # HOST 端：关闭 relay
            relay = self._udp_relays.pop(conn_id, None)
            if relay and relay.transport:
                relay.transport.close()

    # ========== P2P 消息处理 ==========

    async def _on_p2p_message(self, msg: Message) -> None:
        """处理从 P2P 收到的隧道消息"""
        if not isinstance(msg.payload, dict):
            return

        tunnel_type = msg.payload.get("tunnel_type")
        conn_id = msg.payload.get("conn_id", "")
        data = msg.payload.get("data", b"")

        # 记录 conn_id -> 来源 peer（用于回包路由，支持多 CLIENT 连同一 HOST）
        if tunnel_type in (TUNNEL_TCP_OPEN, TUNNEL_TCP_DATA, TUNNEL_UDP_DATA) and msg.sender_id:
            self._conn_to_peer[conn_id] = msg.sender_id

        if tunnel_type == TUNNEL_TCP_OPEN:
            asyncio.create_task(self._handle_remote_tcp_open(conn_id))
        elif tunnel_type == TUNNEL_TCP_DATA:
            await self._handle_remote_tcp_data(conn_id, data)
        elif tunnel_type == TUNNEL_TCP_CLOSE:
            await self._handle_remote_tcp_close(conn_id)
        elif tunnel_type == TUNNEL_UDP_DATA:
            await self._handle_remote_udp_data(conn_id, data)
        elif tunnel_type == TUNNEL_UDP_CLOSE:
            await self._handle_remote_udp_close(conn_id)

    async def _send_tunnel_message(self, tunnel_type: str, conn_id: str, data: bytes) -> None:
        """通过 P2P 发送隧道消息（按 conn_id 路由到对应 peer）"""
        if not self._node:
            return
        # 优先用 conn_id 映射的 peer（多 peer 路由），回退到首个 peer（CLIENT 端单 peer）
        peer_id = self._conn_to_peer.get(conn_id) or self._peer_id
        if not peer_id:
            return
        await self._node.send_to_peer(
            peer_id,
            MessageType.DATA_JSON,
            {
                "tunnel_type": tunnel_type,
                "conn_id": conn_id,
                "data": data,
            },
        )

    def _on_peer_connected(self, peer_info) -> None:
        """P2P 对端连接成功（支持多 peer）"""
        if self._peer_id is None:
            self._peer_id = peer_info.peer_id  # 首个 peer
        self._peer_ids.add(peer_info.peer_id)
        self._connected = True
        self._peer_connected_event.set()
        logger.success(
            f"[Tunnel] P2P connected to {peer_info.peer_id} "
            f"(total peers: {len(self._peer_ids)})"
        )

    def _on_peer_disconnected(self, peer_id: str) -> None:
        """P2P 对端断开（清理该 peer 相关的隧道与映射）"""
        self._peer_ids.discard(peer_id)
        self._connected = len(self._peer_ids) > 0
        logger.warning(
            f"[Tunnel] P2P disconnected from {peer_id} "
            f"(remaining peers: {len(self._peer_ids)})"
        )
        # 清理该 peer 的 conn_id -> peer_id 映射，并关闭对应隧道
        for conn_id, pid in list(self._conn_to_peer.items()):
            if pid == peer_id:
                self._conn_to_peer.pop(conn_id, None)
                asyncio.create_task(self._handle_remote_tcp_close(conn_id))
                asyncio.create_task(self._handle_remote_udp_close(conn_id))
        # 兜底：关闭所有 TCP/UDP 隧道（单 peer 场景）
        if not self._peer_ids:
            for conn_id in list(self._tcp_tunnels.keys()):
                asyncio.create_task(self._handle_remote_tcp_close(conn_id))
            for conn_id in list(self._udp_relays.keys()):
                asyncio.create_task(self._handle_remote_udp_close(conn_id))

    def get_stats(self) -> Dict[str, Any]:
        """获取隧道统计"""
        return {
            "connected": self._connected,
            "peer_id": self._peer_id,
            "peer_count": len(self._peer_ids),
            "active_tcp": len(self._tcp_tunnels),
            "active_udp": len(self._udp_relays) if self.role == ConnectionRole.RESPONDER else len(self._udp_conn_to_client),
            "total_connections": self._connections_count,
            "bytes_forwarded": self._bytes_forwarded,
            "bytes_forwarded_mb": round(self._bytes_forwarded / 1024 / 1024, 2),
        }

    async def stop(self) -> None:
        """停止隧道"""
        logger.info("[Tunnel] Stopping...")

        # 关闭所有 TCP 隧道
        for conn_id, (reader, writer) in list(self._tcp_tunnels.items()):
            if writer:
                try:
                    writer.close()
                    await writer.wait_closed()
                except Exception:
                    pass
        self._tcp_tunnels.clear()
        self._tcp_pending.clear()

        # 关闭 UDP relay
        for relay in self._udp_relays.values():
            if relay.transport:
                try:
                    relay.transport.close()
                except Exception:
                    pass
        self._udp_relays.clear()
        self._udp_client_to_conn.clear()
        self._udp_conn_to_client.clear()
        self._conn_to_peer.clear()
        self._peer_ids.clear()

        # 关闭 UDP 清理任务
        if self._udp_cleanup_task:
            self._udp_cleanup_task.cancel()
            try:
                await self._udp_cleanup_task
            except asyncio.CancelledError:
                pass

        # 关闭本地 TCP 服务器
        if self._tcp_server:
            self._tcp_server.close()
            await self._tcp_server.wait_closed()
            self._tcp_server = None

        # 关闭本地 UDP 监听
        if self._udp_listen_transport:
            self._udp_listen_transport.close()
            self._udp_listen_transport = None

        # 关闭 P2P 节点
        if self._node:
            await self._node.close()

        stats = self.get_stats()
        logger.info(
            f"[Tunnel] Stopped. "
            f"Total connections: {stats['total_connections']}, "
            f"Bytes forwarded: {stats['bytes_forwarded_mb']} MB"
        )
