"""信令服务器 - 用于 P2P 节点之间交换信令消息"""
from __future__ import annotations

import asyncio
import json
import uuid
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set
from loguru import logger

try:
    import websockets
    from websockets.server import WebSocketServerProtocol
    WEBSOCKETS_AVAILABLE = True
except ImportError:
    WEBSOCKETS_AVAILABLE = False

from .types import MessageType, RoomInfo, PeerInfo, generate_peer_id
from .config import ConnectionRole


@dataclass
class SignalingClientConnection:
    """信令客户端连接"""
    peer_id: str
    ws: "WebSocketServerProtocol"
    room_id: Optional[str] = None
    role: Optional[ConnectionRole] = None


class SignalingServer:
    """WebSocket 信令服务器"""

    def __init__(self, host: str = "0.0.0.0", port: int = 8765):
        if not WEBSOCKETS_AVAILABLE:
            raise RuntimeError("websockets not installed. pip install websockets")
        
        self.host = host
        self.port = port
        self._server = None
        
        # 客户端连接: peer_id -> SignalingClientConnection
        self._clients: Dict[str, SignalingClientConnection] = {}
        # 房间: room_id -> set of peer_ids
        self._rooms: Dict[str, Set[str]] = {}

    async def handle_client(self, ws: "WebSocketServerProtocol") -> None:
        """处理客户端连接"""
        client_conn: Optional[SignalingClientConnection] = None
        
        try:
            # 等待客户端发送 join 消息
            async for raw_msg in ws:
                try:
                    msg = json.loads(raw_msg)
                except json.JSONDecodeError:
                    await ws.send(json.dumps({
                        "type": MessageType.CTRL_ERROR.value,
                        "error": "Invalid JSON",
                    }))
                    continue
                
                msg_type = msg.get("type", "")
                
                if msg_type == MessageType.SIGNAL_JOIN.value:
                    # 加入房间
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
                    
                    # 发送确认
                    await ws.send(json.dumps({
                        "type": MessageType.SIGNAL_JOIN.value,
                        "peer_id": peer_id,
                        "room_id": room_id,
                        "success": True,
                    }))
                    
                    # 通知房间中的其他 Peer
                    await self._broadcast_room_info(room_id)
                    # 开始处理其他消息
                    break
                
                else:
                    await ws.send(json.dumps({
                        "type": MessageType.CTRL_ERROR.value,
                        "error": "Please join a room first",
                    }))
            
            # 处理后续消息
            if client_conn:
                async for raw_msg in ws:
                    try:
                        msg = json.loads(raw_msg)
                        await self._handle_message(client_conn, msg)
                    except json.JSONDecodeError:
                        await client_conn.ws.send(json.dumps({
                            "type": MessageType.CTRL_ERROR.value,
                            "error": "Invalid JSON",
                        }))
                    except Exception as e:
                        logger.error(f"[Signaling] Error handling message: {e}")
                        
        except websockets.exceptions.ConnectionClosed:
            logger.debug("[Signaling] Client connection closed")
        except Exception as e:
            logger.error(f"[Signaling] Client error: {e}")
        finally:
            if client_conn:
                await self._handle_disconnect(client_conn)

    async def _handle_message(
        self, sender: SignalingClientConnection, msg: dict
    ) -> None:
        """处理客户端消息"""
        msg_type = msg.get("type", "")
        target_peer_id = msg.get("to")
        
        # 心跳
        if msg_type == MessageType.SIGNAL_PING.value:
            await sender.ws.send(json.dumps({
                "type": MessageType.SIGNAL_PONG.value,
                "timestamp": msg.get("timestamp"),
            }))
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
                    f"[Signaling] Forwarded {msg_type} from "
                    f"{sender.peer_id} -> {target_peer_id}"
                )
            except Exception as e:
                logger.error(f"[Signaling] Forward error: {e}")
                await sender.ws.send(json.dumps({
                    "type": MessageType.CTRL_ERROR.value,
                    "error": f"Failed to send to {target_peer_id}",
                }))
        elif target_peer_id:
            await sender.ws.send(json.dumps({
                "type": MessageType.CTRL_ERROR.value,
                "error": f"Peer {target_peer_id} not found",
            }))

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

    async def _broadcast_room_info(self, room_id: str) -> None:
        """广播房间信息给所有成员"""
        if room_id not in self._rooms:
            return
        
        peers = []
        for peer_id in self._rooms[room_id]:
            if peer_id in self._clients:
                c = self._clients[peer_id]
                peers.append({
                    "peer_id": peer_id,
                    "role": c.role.value if c.role else None,
                })
        
        room_info = {
            "type": MessageType.SIGNAL_ROOM_INFO.value,
            "room_id": room_id,
            "peers": peers,
        }
        
        for peer_id in self._rooms[room_id]:
            if peer_id in self._clients:
                try:
                    await self._clients[peer_id].ws.send(json.dumps(room_info))
                except Exception:
                    pass

    async def _send_room_info(self, client: SignalingClientConnection, room_id: str) -> None:
        """发送房间信息给指定客户端"""
        if room_id not in self._rooms:
            return
        
        peers = []
        for peer_id in self._rooms[room_id]:
            if peer_id in self._clients:
                c = self._clients[peer_id]
                peers.append({
                    "peer_id": peer_id,
                    "role": c.role.value if c.role else None,
                })
        
        await client.ws.send(json.dumps({
            "type": MessageType.SIGNAL_ROOM_INFO.value,
            "room_id": room_id,
            "peers": peers,
        }))

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


async def _async_main():
    """信令服务器异步入口"""
    import argparse
    
    parser = argparse.ArgumentParser(description="P2P Signaling Server")
    parser.add_argument("--host", default="0.0.0.0", help="Bind host")
    parser.add_argument("--port", type=int, default=8765, help="Bind port")
    args = parser.parse_args()
    
    logger.info(f"Starting P2P Signaling Server on {args.host}:{args.port}")
    server = SignalingServer(host=args.host, port=args.port)
    await server.start()
    await server.serve_forever()


def main():
    """信令服务器同步入口 (用于 CLI script)"""
    asyncio.run(_async_main())


if __name__ == "__main__":
    main()
