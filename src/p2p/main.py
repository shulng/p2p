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
from .game_tunnel import GameTunnel, TunnelConfig


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
    protocol: str,
    local_port: Optional[int],
    remote_port: Optional[int],
    local_port_udp: Optional[int],
    remote_port_udp: Optional[int],
    name: str,
):
    """通用隧道模式 - 转发任意 TCP/UDP 流量（游戏联机、服务代理等）"""
    tunnel_config = TunnelConfig(
        protocol=protocol,
        local_listen_port=local_port or 0,
        remote_forward_port=remote_port or 0,
        local_listen_port_udp=local_port_udp,
        remote_forward_port_udp=remote_port_udp,
        name=name,
    )

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
                f"[STATS] Peers: {stats['peer_count']}, "
                f"TCP: {stats['active_tcp']}, UDP: {stats['active_udp']}, "
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

  # 3. 通用 TCP 隧道 (如 Minecraft Java, Terraria, SSH)
  #    HOST 端 (运行目标服务的人):
  $ p2p --mode game --role host --room my-room --protocol tcp --remote-port 25565
  #    CLIENT 端 (运行客户端的人):
  $ p2p --mode game --role client --room my-room --protocol tcp --local-port 25565
  #    然后客户端连接 -> 127.0.0.1:25565

  # 4. UDP 隧道 (如 Minecraft Bedrock, 饥荒联机版)
  $ p2p --mode game --role host --room my-room --protocol udp --remote-port 19132
  $ p2p --mode game --role client --room my-room --protocol udp --local-port 19132

  # 5. 同时转发 TCP + UDP (端口可不同)
  $ p2p --mode game --role host --room my-room --protocol both --remote-port 25565 --remote-port-udp 19132
  $ p2p --mode game --role client --room my-room --protocol both --local-port 25565 --local-port-udp 19132

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
    # 隧道模式参数
    parser.add_argument(
        "--protocol",
        choices=["tcp", "udp", "both"],
        default="tcp",
        help="隧道协议: tcp|udp|both (默认: tcp)",
    )
    parser.add_argument(
        "--local-port",
        type=int,
        default=None,
        help="CLIENT 端本地监听端口 (TCP，或 UDP 未单独指定时)",
    )
    parser.add_argument(
        "--remote-port",
        type=int,
        default=None,
        help="HOST 端远端转发端口 (TCP，或 UDP 未单独指定时)",
    )
    parser.add_argument(
        "--local-port-udp",
        type=int,
        default=None,
        help="CLIENT 端本地 UDP 监听端口 (仅 both/udp 模式，默认与 --local-port 相同)",
    )
    parser.add_argument(
        "--remote-port-udp",
        type=int,
        default=None,
        help="HOST 端远端 UDP 转发端口 (仅 both/udp 模式，默认与 --remote-port 相同)",
    )
    parser.add_argument(
        "--name",
        default="tunnel",
        help="隧道名称 (仅用于日志显示)",
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
                logger.error("Tunnel mode requires --role host or --role client")
                rc = 1
            else:
                role = role_map[args.role]
                rc = asyncio.run(
                    run_game_tunnel(
                        args.signaling,
                        args.room,
                        role,
                        args.protocol,
                        args.local_port,
                        args.remote_port,
                        args.local_port_udp,
                        args.remote_port_udp,
                        args.name,
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
