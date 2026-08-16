"""P2P 主程序入口 - 提供 CLI 示例"""
from __future__ import annotations

import asyncio
import argparse
import sys
import signal
from typing import Optional
from loguru import logger

from .config import (
    P2PConfig,
    TransportProtocol,
    ConnectionRole,
    IceConfig,
)
from .node import P2PNode
from .types import Message, MessageType, PeerInfo, ConnectionState
from .game_tunnel import GameTunnel, TunnelConfig, GAME_PRESETS


async def run_initiator(
    signaling_url: str,
    room_id: str,
    transport: TransportProtocol,
):
    """作为发起方运行"""
    logger.info("=== Running as INITIATOR ===")
    
    config = P2PConfig(
        transport=transport,
        role=ConnectionRole.INITIATOR,
        ice=IceConfig.with_cloudflare_turn(),
    )
    config.signaling.server_url = signaling_url
    
    node = P2PNode(
        config=config,
        on_message=lambda msg: logger.info(
            f"[MSG] {msg.msg_type.value}: {msg.payload}"
        ),
        on_peer_connected=lambda peer: logger.success(
            f"[CONNECTED] Peer {peer.peer_id} @ {peer.address}:{peer.port}"
        ),
        on_peer_disconnected=lambda pid: logger.warning(
            f"[DISCONNECTED] Peer {pid}"
        ),
        on_state_changed=lambda pid, state: logger.info(
            f"[STATE] {pid}: {state.value}"
        ),
    )
    
    await node.initialize()
    
    if not await node.connect_to_signaling():
        logger.error("Failed to connect to signaling server")
        return 1
    
    if not await node.join_room(room_id, ConnectionRole.INITIATOR):
        logger.error("Failed to join room")
        return 1
    
    logger.info("Waiting for responder to join...")
    logger.info("Tip: Start another instance with --role responder in the same room")
    
    # 等待连接
    try:
        while not node.get_connected_peers():
            await asyncio.sleep(1.0)
        
        # 连接成功，开始发送测试消息
        peer_id = node.get_connected_peers()[0]
        logger.success(f"Connected! Will send test messages to {peer_id}")
        
        seq = 0
        try:
            while True:
                seq += 1
                text_msg = f"Hello from initiator! seq={seq}"
                await node.send_text(peer_id, text_msg)
                logger.info(f"Sent: {text_msg}")
                await asyncio.sleep(2.0)
        except KeyboardInterrupt:
            pass
            
    finally:
        await node.close()
    
    return 0


async def run_responder(
    signaling_url: str,
    room_id: str,
    transport: TransportProtocol,
):
    """作为响应方运行"""
    logger.info("=== Running as RESPONDER ===")
    
    config = P2PConfig(
        transport=transport,
        role=ConnectionRole.RESPONDER,
        ice=IceConfig.with_cloudflare_turn(),
    )
    config.signaling.server_url = signaling_url
    
    received_count = 0
    
    def on_message(msg: Message):
        nonlocal received_count
        received_count += 1
        if msg.msg_type == MessageType.DATA_TEXT:
            logger.info(
                f"[RECV #{received_count}] {msg.sender_id}: {msg.payload}"
            )
        elif msg.msg_type == MessageType.DATA_JSON:
            logger.info(
                f"[RECV JSON #{received_count}] {msg.payload}"
            )
        else:
            logger.info(
                f"[RECV {msg.msg_type.value} #{received_count}] len={len(msg.payload) if msg.payload else 0}"
            )
    
    node = P2PNode(
        config=config,
        on_message=on_message,
        on_peer_connected=lambda peer: logger.success(
            f"[CONNECTED] Peer {peer.peer_id} @ {peer.address}:{peer.port}"
        ),
        on_peer_disconnected=lambda pid: logger.warning(
            f"[DISCONNECTED] Peer {pid}"
        ),
    )
    
    await node.initialize()
    
    if not await node.connect_to_signaling():
        logger.error("Failed to connect to signaling server")
        return 1
    
    if not await node.join_room(room_id, ConnectionRole.RESPONDER):
        logger.error("Failed to join room")
        return 1
    
    logger.info("Waiting for initiator connection...")
    
    try:
        # 等待连接
        while not node.get_connected_peers():
            await asyncio.sleep(1.0)
        
        peer_id = node.get_connected_peers()[0]
        logger.success(f"Initiator {peer_id} connected!")
        
        # 回消息
        seq = 0
        try:
            while True:
                seq += 1
                await asyncio.sleep(3.0)
                if peer_id in node.get_connected_peers():
                    await node.send_text(peer_id, f"Echo from responder seq={seq}")
        except KeyboardInterrupt:
            pass
            
    finally:
        await node.close()
    
    return 0


async def run_benchmark(
    signaling_url: str,
    room_id: str,
    role: ConnectionRole,
    transport: TransportProtocol,
):
    """性能测试模式"""
    logger.info(f"=== Running BENCHMARK as {role.value} ===")
    
    config = P2PConfig(
        transport=transport,
        role=role,
        ice=IceConfig.with_cloudflare_turn(),
    )
    config.signaling.server_url = signaling_url
    
    start_time = None
    total_bytes = 0
    msg_count = 0
    
    def on_message(msg: Message):
        nonlocal total_bytes, msg_count, start_time
        if start_time is None:
            start_time = asyncio.get_event_loop().time()
        
        if isinstance(msg.payload, bytes):
            total_bytes += len(msg.payload)
        else:
            import pickle
            total_bytes += len(pickle.dumps(msg.payload))
        msg_count += 1
        
        elapsed = asyncio.get_event_loop().time() - start_time
        if elapsed > 0:
            speed = total_bytes / elapsed / 1024 / 1024
            logger.info(
                f"[BENCH] Received {msg_count} msgs, "
                f"{total_bytes/1024/1024:.2f} MB, "
                f"{speed:.2f} MB/s"
            )
    
    node = P2PNode(
        config=config,
        on_message=on_message,
    )
    
    await node.initialize()
    await node.connect_to_signaling()
    await node.join_room(room_id, role)
    
    logger.info("Waiting for peer connection...")
    while not node.get_connected_peers():
        await asyncio.sleep(0.5)
    
    peer_id = node.get_connected_peers()[0]
    logger.success(f"Connected to {peer_id}")
    
    if role == ConnectionRole.INITIATOR:
        # 发送方：发送大量数据
        data_size = 64 * 1024  # 64KB per message
        data = b"X" * data_size
        
        start_time = asyncio.get_event_loop().time()
        count = 0
        total_sent = 0
        
        try:
            duration = 30.0  # 30秒测试
            while asyncio.get_event_loop().time() - start_time < duration:
                await node.send_bytes(peer_id, data)
                count += 1
                total_sent += data_size
                
                if count % 100 == 0:
                    elapsed = asyncio.get_event_loop().time() - start_time
                    speed = total_sent / elapsed / 1024 / 1024
                    logger.info(
                        f"[BENCH] Sent {count} msgs, "
                        f"{total_sent/1024/1024:.2f} MB, "
                        f"{speed:.2f} MB/s"
                    )
                    
        except KeyboardInterrupt:
            pass
        
        elapsed = asyncio.get_event_loop().time() - start_time
        logger.success(
            f"[BENCH FINAL] Sent {count} msgs, "
            f"{total_sent/1024/1024:.2f} MB in {elapsed:.1f}s, "
            f"avg {total_sent/elapsed/1024/1024:.2f} MB/s"
        )
        
        await asyncio.sleep(2.0)  # 等待对方接收完毕
    
    else:
        # 接收方：持续等待
        logger.info("Receiving benchmark data... (Ctrl+C to stop)")
        try:
            while True:
                await asyncio.sleep(1.0)
        except KeyboardInterrupt:
            pass
    
    await node.close()
    return 0


async def run_game_tunnel(
    signaling_url: str,
    room_id: str,
    role: ConnectionRole,
    game: str,
    local_port: Optional[int],
    remote_port: Optional[int],
):
    """游戏隧道模式 - 用于 Minecraft 等游戏联机"""
    # 获取游戏预设配置
    tunnel_config = GAME_PRESETS.get(game, GAME_PRESETS["custom"]).__class__(
        **GAME_PRESETS.get(game, GAME_PRESETS["custom"]).__dict__
    )

    # 覆盖自定义端口
    if local_port:
        tunnel_config.local_listen_port = local_port
    if remote_port:
        tunnel_config.remote_forward_port = remote_port

    p2p_config = P2PConfig(
        transport=TransportProtocol.AUTO,
        role=role,
        ice=IceConfig.with_cloudflare_turn(),
    )
    p2p_config.signaling.server_url = signaling_url

    tunnel = GameTunnel(p2p_config, tunnel_config, role)

    # 定期打印统计
    async def print_stats():
        while True:
            await asyncio.sleep(30)
            stats = tunnel.get_stats()
            logger.info(
                f"[STATS] Active tunnels: {stats['active_tunnels']}, "
                f"Total: {stats['total_connections']}, "
                f"Forwarded: {stats['bytes_forwarded_mb']} MB"
            )

    stats_task = asyncio.create_task(print_stats())

    try:
        await tunnel.start(signaling_url, room_id)
        # 保持运行
        while True:
            await asyncio.sleep(1.0)
    except KeyboardInterrupt:
        pass
    finally:
        stats_task.cancel()
        await tunnel.stop()
    return 0


def main():
    """CLI 入口"""
    parser = argparse.ArgumentParser(
        description="P2P Communication Tool (QUIC + KCP + Cloudflare TURN)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # 1. 启动信令服务器 (终端1)
  $ p2p-signaling --port 8765

  # 2. P2P 通信测试
  $ p2p --mode initiator --room test123 --transport auto
  $ p2p --mode responder --room test123 --transport auto

  # 3. Minecraft 联机 (Java Edition, TCP 25565)
  #    HOST 端 (运行 MC 服务端的人):
  $ p2p --mode game --game mc-java --role host --room mc-room-001
  #    CLIENT 端 (运行 MC 客户端的人):
  $ p2p --mode game --game mc-java --role client --room mc-room-001
  #    然后 MC 客户端连接 -> 127.0.0.1:25565

  # 4. 自定义端口
  $ p2p --mode game --game custom --role host --room my-room --remote-port 25566
  $ p2p --mode game --game custom --role client --room my-room --local-port 25566

  # 性能测试
  $ p2p --mode benchmark --role initiator --room bench1
  $ p2p --mode benchmark --role responder --room bench1
        """,
    )

    parser.add_argument(
        "--mode",
        choices=["initiator", "responder", "benchmark", "game"],
        default="initiator",
        help="运行模式: initiator|responder|benchmark|game (默认: initiator)",
    )
    parser.add_argument(
        "--transport",
        choices=["quic", "kcp", "auto"],
        default="auto",
        help="传输协议 (默认: auto)",
    )
    parser.add_argument(
        "--signaling",
        default="ws://localhost:8765",
        help="信令服务器地址 (默认: ws://localhost:8765)",
    )
    parser.add_argument(
        "--room",
        default="default-room",
        help="房间 ID (默认: default-room)",
    )
    parser.add_argument(
        "--role",
        choices=["initiator", "responder", "host", "client"],
        default=None,
        help="角色: initiator|responder (通信), host|client (游戏)",
    )
    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default="INFO",
        help="日志级别",
    )
    # 游戏模式参数
    parser.add_argument(
        "--game",
        choices=list(GAME_PRESETS.keys()),
        default="mc-java",
        help="游戏预设: mc-java|mc-bedrock|terraria|dont-starve|custom (默认: mc-java)",
    )
    parser.add_argument(
        "--local-port",
        type=int,
        default=None,
        help="本地监听端口 (client 端, MC客户端连接此端口)",
    )
    parser.add_argument(
        "--remote-port",
        type=int,
        default=None,
        help="远端转发端口 (host 端, MC服务端运行的端口)",
    )
    
    args = parser.parse_args()
    
    # 配置日志
    logger.remove()
    logger.add(
        sys.stderr,
        level=args.log_level,
        format="<green>{time:HH:mm:ss.SSS}</green> | <level>{level: <8}</level> | <cyan>{message}</cyan>",
    )
    
    transport_map = {
        "quic": TransportProtocol.QUIC,
        "kcp": TransportProtocol.KCP,
        "auto": TransportProtocol.AUTO,
    }
    transport = transport_map[args.transport]

    role_map = {
        "initiator": ConnectionRole.INITIATOR,
        "responder": ConnectionRole.RESPONDER,
        "host": ConnectionRole.RESPONDER,
        "client": ConnectionRole.INITIATOR,
    }

    try:
        if args.mode == "initiator":
            rc = asyncio.run(
                run_initiator(args.signaling, args.room, transport)
            )
        elif args.mode == "responder":
            rc = asyncio.run(
                run_responder(args.signaling, args.room, transport)
            )
        elif args.mode == "benchmark":
            role = role_map[args.role] if args.role else ConnectionRole.INITIATOR
            rc = asyncio.run(
                run_benchmark(args.signaling, args.room, role, transport)
            )
        elif args.mode == "game":
            if not args.role or args.role not in ("host", "client"):
                logger.error("Game mode requires --role host or --role client")
                rc = 1
            else:
                role = role_map[args.role]
                rc = asyncio.run(
                    run_game_tunnel(
                        args.signaling,
                        args.room,
                        role,
                        args.game,
                        args.local_port,
                        args.remote_port,
                    )
                )
        else:
            parser.print_help()
            rc = 1
            
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
        rc = 0
    except Exception as e:
        logger.exception(f"Unexpected error: {e}")
        rc = 1
    
    sys.exit(rc)


if __name__ == "__main__":
    main()
