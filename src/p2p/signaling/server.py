"""信令服务器 - 用于 P2P 节点之间交换信令消息"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
from dataclasses import dataclass
from typing import Any, TypeAlias

from loguru import logger

from .._compat import WEBSOCKETS_AVAILABLE, websockets
from ..config import ConnectionRole
from ..types import MessageType, generate_peer_id

# 兼容层暴露的 ServerConnection 为运行时变量（值可能为 Any 或具体类），
# 无法直接作为类型注解使用。此处以 TypeAlias 显式声明连接类型，
# 统一用 Any 规避不同 websockets 版本间类型 stub 的差异。
WSConnection: TypeAlias = Any


@dataclass
class SignalingClientConnection:
    """信令客户端连接"""

    peer_id: str
    ws: WSConnection
    room_id: str | None = None
    role: ConnectionRole | None = None


class SignalingServer:
    """WebSocket 信令服务器"""

    def __init__(self, host: str = "0.0.0.0", port: int = 8765):
        if not WEBSOCKETS_AVAILABLE:
            raise RuntimeError("websockets not installed. pip install websockets")

        self.host = host
        self.port = port
        self._server: Any = None

        # 客户端连接: peer_id -> SignalingClientConnection
        self._clients: dict[str, SignalingClientConnection] = {}
        # 房间: room_id -> set of peer_ids
        self._rooms: dict[str, set[str]] = {}

    async def handle_client(self, ws: WSConnection) -> None:
        """处理客户端连接"""
        client_conn = await self._wait_for_join(ws)
        if client_conn is None:
            return

        # 加入成功，开始处理后续消息
        try:
            async for raw_msg in ws:
                try:
                    msg = json.loads(raw_msg)
                    await self._handle_message(client_conn, msg)
                except json.JSONDecodeError:
                    await client_conn.ws.send(
                        json.dumps(
                            {
                                "type": MessageType.CTRL_ERROR.value,
                                "error": "Invalid JSON",
                            }
                        )
                    )
                except Exception as e:
                    logger.error(f"[Signaling] Error handling message: {e}")
        except websockets.exceptions.ConnectionClosed:
            logger.debug("[Signaling] Client connection closed")
        except Exception as e:
            logger.error(f"[Signaling] Client error: {e}")
        finally:
            await self._handle_disconnect(client_conn)

    async def _wait_for_join(self, ws: WSConnection) -> SignalingClientConnection | None:
        """等待并处理客户端 join 请求，返回建立好的连接或 None"""
        try:
            async for raw_msg in ws:
                try:
                    msg = json.loads(raw_msg)
                except json.JSONDecodeError:
                    await ws.send(
                        json.dumps(
                            {
                                "type": MessageType.CTRL_ERROR.value,
                                "error": "Invalid JSON",
                            }
                        )
                    )
                    continue

                msg_type = msg.get("type", "")

                if msg_type != MessageType.SIGNAL_JOIN.value:
                    # 尚未加入房间时，拒绝其他类型消息
                    await ws.send(
                        json.dumps(
                            {
                                "type": MessageType.CTRL_ERROR.value,
                                "error": "Please join a room first",
                            }
                        )
                    )
                    continue

                client_conn = self._register_join(ws, msg)
                # 发送 join 确认
                await ws.send(
                    json.dumps(
                        {
                            "type": MessageType.SIGNAL_JOIN.value,
                            "peer_id": client_conn.peer_id,
                            "room_id": client_conn.room_id,
                            "success": True,
                        }
                    )
                )
                # 通知房间中的其他 Peer
                if client_conn.room_id:
                    await self._broadcast_room_info(client_conn.room_id)
                return client_conn
        except websockets.exceptions.ConnectionClosed:
            logger.debug("[Signaling] Client disconnected before join")
        except Exception as e:
            logger.error(f"[Signaling] Join error: {e}")
        return None

    def _register_join(
        self, ws: WSConnection, msg: dict[str, Any]
    ) -> SignalingClientConnection:
        """登记 join 消息并返回客户端连接对象"""
        peer_id = msg.get("peer_id") or generate_peer_id()
        room_id = msg.get("room_id")
        role_str = msg.get("role", ConnectionRole.INITIATOR.value)

        try:
            role = ConnectionRole(role_str)
        except ValueError:
            role = ConnectionRole.INITIATOR

        client_conn = SignalingClientConnection(
            peer_id=peer_id,
            ws=ws,
            room_id=room_id,
            role=role,
        )

        self._clients[peer_id] = client_conn

        # 加入房间
        if room_id:
            if room_id not in self._rooms:
                self._rooms[room_id] = set()
            self._rooms[room_id].add(peer_id)

        logger.info(f"[Signaling] Peer {peer_id} joined room {room_id} as {role}")
        return client_conn

    async def _handle_message(self, sender: SignalingClientConnection, msg: dict[str, Any]) -> None:
        """处理客户端消息"""
        msg_type = msg.get("type", "")
        target_peer_id = msg.get("to")

        # 心跳
        if msg_type == MessageType.SIGNAL_PING.value:
            await sender.ws.send(
                json.dumps(
                    {
                        "type": MessageType.SIGNAL_PONG.value,
                        "timestamp": msg.get("timestamp"),
                    }
                )
            )
            return

        # 房间信息查询
        if msg_type == MessageType.SIGNAL_ROOM_INFO.value:
            room_id = sender.room_id
            if room_id:
                await self._send_room_info(sender, room_id)
            return

        # 点对点信令消息转发
        if target_peer_id and target_peer_id in self._clients:
            target = self._clients[target_peer_id]

            # 添加发送者信息
            forward_msg = dict(msg)
            forward_msg["from"] = sender.peer_id

            try:
                await target.ws.send(json.dumps(forward_msg))
                logger.debug(
                    f"[Signaling] Forwarded {msg_type} from {sender.peer_id} -> {target_peer_id}"
                )
            except Exception as e:
                logger.error(f"[Signaling] Forward error: {e}")
                await sender.ws.send(
                    json.dumps(
                        {
                            "type": MessageType.CTRL_ERROR.value,
                            "error": f"Failed to send to {target_peer_id}",
                        }
                    )
                )
        elif target_peer_id:
            await sender.ws.send(
                json.dumps(
                    {
                        "type": MessageType.CTRL_ERROR.value,
                        "error": f"Peer {target_peer_id} not found",
                    }
                )
            )

    async def _handle_disconnect(self, client: SignalingClientConnection) -> None:
        """处理客户端断开"""
        peer_id = client.peer_id
        room_id = client.room_id

        if peer_id in self._clients:
            del self._clients[peer_id]

        if room_id and room_id in self._rooms:
            self._rooms[room_id].discard(peer_id)
            if not self._rooms[room_id]:
                del self._rooms[room_id]

        logger.info(f"[Signaling] Peer {peer_id} disconnected from room {room_id}")

        # 通知房间成员更新
        if room_id:
            await self._broadcast_room_info(room_id)

    def _build_room_peers(self, room_id: str) -> list[dict[str, Any]]:
        """构建房间内的 peer 列表（供 room_info 消息使用）。

        集中处理 peer 字段组装，避免 ``_broadcast_room_info`` 与
        ``_send_room_info`` 各自维护重复逻辑（DRY）。
        """
        if room_id not in self._rooms:
            return []
        peers: list[dict[str, Any]] = []
        for peer_id in self._rooms[room_id]:
            if peer_id in self._clients:
                c = self._clients[peer_id]
                peers.append(
                    {
                        "peer_id": peer_id,
                        "role": c.role.value if c.role else None,
                    }
                )
        return peers

    async def _broadcast_room_info(self, room_id: str) -> None:
        """广播房间信息给所有成员"""
        if room_id not in self._rooms:
            return

        peers = self._build_room_peers(room_id)

        room_info = {
            "type": MessageType.SIGNAL_ROOM_INFO.value,
            "room_id": room_id,
            "peers": peers,
        }

        for peer_id in self._rooms[room_id]:
            if peer_id in self._clients:
                with contextlib.suppress(Exception):
                    await self._clients[peer_id].ws.send(json.dumps(room_info))

    async def _send_room_info(self, client: SignalingClientConnection, room_id: str) -> None:
        """发送房间信息给指定客户端"""
        if room_id not in self._rooms:
            return

        peers = self._build_room_peers(room_id)

        await client.ws.send(
            json.dumps(
                {
                    "type": MessageType.SIGNAL_ROOM_INFO.value,
                    "room_id": room_id,
                    "peers": peers,
                }
            )
        )

    async def start(self) -> None:
        """启动信令服务器"""
        self._server = await websockets.serve(
            self.handle_client,
            self.host,
            self.port,
        )
        logger.info(f"[Signaling] Server started on ws://{self.host}:{self.port}")

    async def serve_forever(self) -> None:
        """持续运行"""
        if not self._server:
            await self.start()
        await self._server.wait_closed()

    async def stop(self) -> None:
        """停止服务器"""
        if self._server:
            self._server.close()
            await self._server.wait_closed()
            logger.info("[Signaling] Server stopped")
            self._server = None


async def _async_main() -> None:
    """信令服务器异步入口"""
    parser = argparse.ArgumentParser(description="P2P Signaling Server")
    parser.add_argument("--host", default="0.0.0.0", help="Bind host")
    parser.add_argument("--port", type=int, default=8765, help="Bind port")
    args = parser.parse_args()

    logger.info(f"Starting P2P Signaling Server on {args.host}:{args.port}")
    server = SignalingServer(host=args.host, port=args.port)
    await server.start()
    await server.serve_forever()


def main() -> None:
    """信令服务器同步入口 (用于 CLI script)"""
    asyncio.run(_async_main())


if __name__ == "__main__":
    main()
