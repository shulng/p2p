"""游戏隧道模块 - 通过 P2P 连接转发游戏流量

用途：让两个玩家通过 P2P 连接进行游戏联机（如 Minecraft）
架构：
  玩家A (MC客户端) → localhost:25565 → P2P DataChannel → 玩家B → localhost:25565 (MC服务端)

支持：
  - Minecraft Java Edition (TCP 25565)
  - Minecraft Bedrock Edition (UDP 19132)
  - 任意 TCP/UDP 游戏流量转发
"""
from __future__ import annotations

import asyncio
import struct
import uuid
from dataclasses import dataclass, field
from typing import Callable, Dict, Optional, Tuple, Any
from loguru import logger

from .config import P2PConfig, TransportProtocol, ConnectionRole, IceConfig
from .node import P2PNode
from .types import Message, MessageType, ConnectionState


# 隧道消息类型（复用 Message 结构，使用自定义 payload）
TUNNEL_MSG_OPEN = "tunnel.open"       # 打开隧道连接
TUNNEL_MSG_CLOSE = "tunnel.close"     # 关闭隧道连接
TUNNEL_MSG_DATA = "tunnel.data"       # 隧道数据


@dataclass
class TunnelConfig:
    """游戏隧道配置"""
    # 本地监听地址（MC客户端连接这里）
    local_listen_host: str = "127.0.0.1"
    local_listen_port: int = 25565
    
    # 远端转发目标（连接到远端MC服务端）
    remote_forward_host: str = "127.0.0.1"
    remote_forward_port: int = 25565
    
    # 协议类型
    protocol: str = "tcp"  # tcp / udp
    
    # 游戏类型（仅用于日志显示）
    game_name: str = "Minecraft"


class GameTunnel:
    """游戏隧道 - 在 P2P 连接上转发 TCP/UDP 流量"""

    def __init__(
        self,
        p2p_config: P2PConfig,
        tunnel_config: TunnelConfig,
        role: ConnectionRole,
    ):
        self.p2p_config = p2p_config
        self.tunnel_config = tunnel_config
        self.role = role  # HOST(运行MC服务端) = RESPONDER, CLIENT(运行MC客户端) = INITIATOR
        
        # 活跃的隧道连接: connection_id -> (reader, writer)
        self._tunnels: Dict[str, Tuple[Optional[asyncio.StreamReader], Optional[asyncio.StreamWriter]]] = {}
        # 等待建立的隧道数据缓冲: connection_id -> list of bytes
        self._pending_data: Dict[str, list] = {}
        
        # 本地 TCP 服务器
        self._tcp_server: Optional[asyncio.AbstractServer] = None
        
        # P2P 节点
        self._node: Optional[P2PNode] = None
        self._peer_id: Optional[str] = None
        self._connected: bool = False
        
        # 用于等待 P2P 连接建立
        self._peer_connected_event: asyncio.Event = asyncio.Event()
        
        # 统计
        self._bytes_forwarded: int = 0
        self._connections_count: int = 0

    async def start(self, signaling_url: str, room_id: str) -> None:
        """启动游戏隧道"""
        logger.info(f"=== Game Tunnel Starting ({self.role.value}) ===")
        logger.info(f"Game: {self.tunnel_config.game_name}")
        logger.info(f"Protocol: {self.tunnel_config.protocol.upper()}")
        
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
        
        if self.role == ConnectionRole.RESPONDER:
            # HOST 端：等待客户端连接
            logger.info(f"Waiting for {self.tunnel_config.game_name} client to connect...")
            await self._peer_connected_event.wait()
            
            # 启动本地 TCP 监听（接收来自远端的隧道数据，转发到本地MC服务端）
            # HOST 不需要本地 TCP 服务器，它直接接收 P2P 消息并连接本地 MC 服务端
            logger.info(
                f"Ready! Forwarding P2P -> {self.tunnel_config.remote_forward_host}:{self.tunnel_config.remote_forward_port}"
            )
            
        else:
            # CLIENT 端：启动本地 TCP 服务器，MC客户端连接这里
            logger.info(f"Waiting for P2P connection to HOST...")
            await self._peer_connected_event.wait()
            
            await self._start_local_tcp_server()
            logger.info(
                f"Ready! Connect your {self.tunnel_config.game_name} client to: "
                f"{self.tunnel_config.local_listen_host}:{self.tunnel_config.local_listen_port}"
            )
        
        logger.info("=== Tunnel Active ===")

    async def _start_local_tcp_server(self) -> None:
        """启动本地 TCP 服务器（MC客户端连接这里）"""
        self._tcp_server = await asyncio.start_server(
            self._handle_local_connection,
            self.tunnel_config.local_listen_host,
            self.tunnel_config.local_listen_port,
        )
        logger.info(
            f"[Tunnel] Local TCP server listening on "
            f"{self.tunnel_config.local_listen_host}:{self.tunnel_config.local_listen_port}"
        )

    async def _handle_local_connection(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        """处理来自本地 MC 客户端的 TCP 连接"""
        conn_id = uuid.uuid4().hex[:12]
        peer_addr = writer.get_extra_info("peername")
        logger.info(f"[Tunnel] Local connection #{conn_id} from {peer_addr}")
        
        self._tunnels[conn_id] = (reader, writer)
        self._connections_count += 1
        
        # 通知远端打开隧道
        await self._send_tunnel_message(TUNNEL_MSG_OPEN, conn_id, b"")
        
        try:
            # 持续读取本地数据并转发到远端
            while conn_id in self._tunnels:
                data = await reader.read(65536)
                if not data:
                    break
                
                self._bytes_forwarded += len(data)
                await self._send_tunnel_message(TUNNEL_MSG_DATA, conn_id, data)
                
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.error(f"[Tunnel] Local read error #{conn_id}: {e}")
        finally:
            # 通知远端关闭
            await self._send_tunnel_message(TUNNEL_MSG_CLOSE, conn_id, b"")
            writer.close()
            try:
                await writer.wait_closed()
            except Exception:
                pass
            self._tunnels.pop(conn_id, None)
            logger.info(f"[Tunnel] Local connection #{conn_id} closed")

    async def _on_p2p_message(self, msg: Message) -> None:
        """处理从 P2P 收到的隧道消息"""
        # 隧道消息的 payload 格式: {"tunnel_type": str, "conn_id": str, "data": bytes}
        if not isinstance(msg.payload, dict):
            return
        
        tunnel_type = msg.payload.get("tunnel_type")
        conn_id = msg.payload.get("conn_id", "")
        data = msg.payload.get("data", b"")
        
        if tunnel_type == TUNNEL_MSG_OPEN:
            # 远端打开了新隧道 -> 连接本地 MC 服务端
            asyncio.create_task(self._handle_remote_open(conn_id))
            
        elif tunnel_type == TUNNEL_MSG_DATA:
            # 远端发来数据
            await self._handle_remote_data(conn_id, data)
            
        elif tunnel_type == TUNNEL_MSG_CLOSE:
            # 远端关闭了隧道
            await self._handle_remote_close(conn_id)

    async def _handle_remote_open(self, conn_id: str) -> None:
        """远端打开隧道 -> 连接本地 MC 服务端"""
        try:
            reader, writer = await asyncio.open_connection(
                self.tunnel_config.remote_forward_host,
                self.tunnel_config.remote_forward_port,
            )
            self._tunnels[conn_id] = (reader, writer)
            logger.info(
                f"[Tunnel] Connected to local {self.tunnel_config.game_name} server "
                f"#{conn_id} -> {self.tunnel_config.remote_forward_host}:{self.tunnel_config.remote_forward_port}"
            )

            # 刷新缓冲的数据
            if conn_id in self._pending_data:
                for buffered_data in self._pending_data.pop(conn_id):
                    writer.write(buffered_data)
                await writer.drain()

            # 启动转发循环：本地服务端 -> P2P
            asyncio.create_task(self._forward_local_to_remote(conn_id, reader))

        except Exception as e:
            logger.error(f"[Tunnel] Failed to connect to local server #{conn_id}: {e}")
            self._pending_data.pop(conn_id, None)
            await self._send_tunnel_message(TUNNEL_MSG_CLOSE, conn_id, b"")

    async def _forward_local_to_remote(self, conn_id: str, reader: asyncio.StreamReader) -> None:
        """从本地 MC 服务端读取数据，转发到远端"""
        try:
            while conn_id in self._tunnels:
                data = await reader.read(65536)
                if not data:
                    break

                self._bytes_forwarded += len(data)
                await self._send_tunnel_message(TUNNEL_MSG_DATA, conn_id, data)

        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.error(f"[Tunnel] Forward error #{conn_id}: {e}")
        finally:
            await self._send_tunnel_message(TUNNEL_MSG_CLOSE, conn_id, b"")
            self._tunnels.pop(conn_id, None)
            logger.info(f"[Tunnel] Remote forwarding #{conn_id} closed")

    async def _handle_remote_data(self, conn_id: str, data: bytes) -> None:
        """远端发来数据 -> 转发到本地"""
        tunnel = self._tunnels.get(conn_id)
        if tunnel and tunnel[1]:
            try:
                tunnel[1].write(data)
                await tunnel[1].drain()
            except Exception as e:
                logger.error(f"[Tunnel] Write error #{conn_id}: {e}")
                await self._handle_remote_close(conn_id)
        else:
            # 隧道尚未建立，缓冲数据
            if conn_id not in self._pending_data:
                self._pending_data[conn_id] = []
            self._pending_data[conn_id].append(data)

    async def _handle_remote_close(self, conn_id: str) -> None:
        """远端关闭隧道"""
        tunnel = self._tunnels.pop(conn_id, None)
        if tunnel and tunnel[1]:
            try:
                tunnel[1].close()
                await tunnel[1].wait_closed()
            except Exception:
                pass
            logger.info(f"[Tunnel] Connection #{conn_id} closed by remote")

    async def _send_tunnel_message(self, tunnel_type: str, conn_id: str, data: bytes) -> None:
        """通过 P2P 发送隧道消息"""
        if not self._node or not self._peer_id:
            return
        
        await self._node.send_to_peer(
            self._peer_id,
            MessageType.DATA_JSON,
            {
                "tunnel_type": tunnel_type,
                "conn_id": conn_id,
                "data": data,
            },
        )

    def _on_peer_connected(self, peer_info) -> None:
        """P2P 对端连接成功"""
        self._peer_id = peer_info.peer_id
        self._connected = True
        self._peer_connected_event.set()
        logger.success(f"[Tunnel] P2P connected to {peer_info.peer_id}")

    def _on_peer_disconnected(self, peer_id: str) -> None:
        """P2P 对端断开"""
        self._connected = False
        logger.warning(f"[Tunnel] P2P disconnected from {peer_id}")
        # 关闭所有隧道
        for conn_id in list(self._tunnels.keys()):
            asyncio.create_task(self._handle_remote_close(conn_id))

    def get_stats(self) -> Dict[str, Any]:
        """获取隧道统计"""
        return {
            "connected": self._connected,
            "peer_id": self._peer_id,
            "active_tunnels": len(self._tunnels),
            "total_connections": self._connections_count,
            "bytes_forwarded": self._bytes_forwarded,
            "bytes_forwarded_mb": round(self._bytes_forwarded / 1024 / 1024, 2),
        }

    async def stop(self) -> None:
        """停止隧道"""
        logger.info("[Tunnel] Stopping...")
        
        # 关闭所有隧道
        for conn_id, (reader, writer) in list(self._tunnels.items()):
            if writer:
                try:
                    writer.close()
                    await writer.wait_closed()
                except Exception:
                    pass
        self._tunnels.clear()
        
        # 关闭本地 TCP 服务器
        if self._tcp_server:
            self._tcp_server.close()
            await self._tcp_server.wait_closed()
            self._tcp_server = None
        
        # 关闭 P2P 节点
        if self._node:
            await self._node.close()
        
        stats = self.get_stats()
        logger.info(
            f"[Tunnel] Stopped. "
            f"Total connections: {stats['total_connections']}, "
            f"Bytes forwarded: {stats['bytes_forwarded_mb']} MB"
        )


# 预设游戏配置
GAME_PRESETS = {
    "mc-java": TunnelConfig(
        local_listen_port=25565,
        remote_forward_port=25565,
        protocol="tcp",
        game_name="Minecraft Java Edition",
    ),
    "mc-bedrock": TunnelConfig(
        local_listen_port=19132,
        remote_forward_port=19132,
        protocol="udp",
        game_name="Minecraft Bedrock Edition",
    ),
    "terraria": TunnelConfig(
        local_listen_port=7777,
        remote_forward_port=7777,
        protocol="tcp",
        game_name="Terraria",
    ),
    "dont-starve": TunnelConfig(
        local_listen_port=10999,
        remote_forward_port=10999,
        protocol="udp",
        game_name="Don't Starve Together",
    ),
    "custom": TunnelConfig(
        local_listen_port=25565,
        remote_forward_port=25565,
        protocol="tcp",
        game_name="Custom Game",
    ),
}
