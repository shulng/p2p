"""P2P 主程序入口 - 子命令 CLI"""

from __future__ import annotations

import argparse
import asyncio
import sys
import time
from dataclasses import dataclass

from loguru import logger

from ._utils import build_p2p_config, spawn_task
from .config import ConnectionRole
from .node import P2PNode
from .tunnel.game_tunnel import GameTunnel, TunnelConfig
from .types import Message, MessageType

DEFAULT_SIGNALING = "ws://localhost:8765"


def _setup_logger(level: str = "INFO") -> None:
    logger.remove()
    logger.add(
        sys.stderr,
        level=level,
        format=(
            "<green>{time:HH:mm:ss.SSS}</green> | "
            "<level>{level: <8}</level> | <cyan>{message}</cyan>"
        ),
    )


def _base_config(role: ConnectionRole, signaling: str) -> "P2PConfig":
    """构建 P2P 配置（统一委托给公共模块 :func:`build_p2p_config`）。"""
    return build_p2p_config(role, signaling)


# ============== chat ==============


async def _run_chat(signaling: str, room: str, as_side: str) -> int:
    role = ConnectionRole.INITIATOR if as_side == "a" else ConnectionRole.RESPONDER
    cfg = _base_config(role, signaling)

    if as_side == "a":
        logger.info("=== chat: side A (initiator, 主动发测试消息) ===")
    else:
        logger.info("=== chat: side B (responder, 回显) ===")

    recv_seq = 0

    def on_message(msg: Message) -> None:
        nonlocal recv_seq
        recv_seq += 1
        if msg.msg_type == MessageType.DATA_TEXT:
            logger.info(f"[recv #{recv_seq}] {msg.sender_id}: {msg.payload}")
        else:
            size = len(msg.payload) if isinstance(msg.payload, bytes) else 0
            logger.info(f"[recv {msg.msg_type.value} #{recv_seq}] len={size}B")

    node = P2PNode(
        config=cfg,
        on_message=on_message,
        on_peer_connected=lambda peer: logger.success(
            f"[CONNECTED] Peer {peer.peer_id} @ {peer.address}:{peer.port}"
        ),
        on_peer_disconnected=lambda pid: logger.warning(f"[DISCONNECTED] Peer {pid}"),
        on_state_changed=lambda pid, state: logger.info(f"[STATE] {pid}: {state.value}"),
    )

    await node.initialize()
    if not await node.connect_to_signaling():
        logger.error("Failed to connect signaling")
        return 1
    if not await node.join_room(room, role):
        logger.error("Failed to join room")
        return 1

    try:
        while not node.get_connected_peers():
            await asyncio.sleep(0.5)
        peer_id = node.get_connected_peers()[0]
        logger.success(f"Connected to {peer_id}")

        if as_side == "a":
            seq = 0
            while True:
                seq += 1
                await node.send_text(peer_id, f"hello seq={seq}")
                logger.info(f"sent hello seq={seq}")
                await asyncio.sleep(2.0)
        else:
            seq = 0
            while True:
                await asyncio.sleep(3.0)
                if peer_id in node.get_connected_peers():
                    seq += 1
                    await node.send_text(peer_id, f"reply seq={seq}")
    except KeyboardInterrupt:
        pass
    finally:
        await node.close()
    return 0


# ============== bench ==============


async def _run_bench(  # pylint: disable=too-many-locals
    signaling: str, room: str, as_side: str, duration: float
) -> int:
    role = ConnectionRole.INITIATOR if as_side == "a" else ConnectionRole.RESPONDER
    cfg = _base_config(role, signaling)
    logger.info(f"=== bench as side {as_side} ({role.value}), transport=KCP ===")

    start_time = None
    total_bytes = 0
    msg_count = 0

    def on_message(msg: Message) -> None:
        nonlocal total_bytes, msg_count, start_time
        if start_time is None:
            start_time = time.monotonic()
        # 与发送端编码方式保持一致：bytes 统计原始长度，
        # 非 bytes 按实际大小估算（bench 场景为 bytes，此处为防御）。
        if isinstance(msg.payload, bytes):
            total_bytes += len(msg.payload)
        else:
            total_bytes += len(str(msg.payload).encode("utf-8"))
        msg_count += 1
        elapsed = time.monotonic() - start_time
        if elapsed > 0 and msg_count % 100 == 0:
            logger.info(
                f"[BENCH] {msg_count} msgs, "
                f"{total_bytes / 1024 / 1024:.2f} MB, "
                f"{total_bytes / elapsed / 1024 / 1024:.2f} MB/s"
            )

    node = P2PNode(config=cfg, on_message=on_message)
    await node.initialize()
    await node.connect_to_signaling()
    await node.join_room(room, role)

    logger.info("Waiting for peer...")
    while not node.get_connected_peers():
        await asyncio.sleep(0.3)
    peer_id = node.get_connected_peers()[0]
    logger.success(f"Connected to {peer_id}")

    if as_side == "a":
        chunk = b"X" * (64 * 1024)
        start_time = time.monotonic()
        sent_count = 0
        try:
            while time.monotonic() - start_time < duration:
                await node.send_bytes(peer_id, chunk)
                sent_count += 1
        except KeyboardInterrupt:
            pass
        elapsed = time.monotonic() - start_time
        total_sent = sent_count * len(chunk)
        logger.success(
            f"[BENCH SENT] {sent_count} msgs, "
            f"{total_sent / 1024 / 1024:.2f} MB in {elapsed:.1f}s, "
            f"{total_sent / elapsed / 1024 / 1024:.2f} MB/s"
        )
        await asyncio.sleep(2.0)
    else:
        logger.info("Receiving... (Ctrl+C to stop)")
        try:
            while True:
                await asyncio.sleep(1.0)
        except KeyboardInterrupt:
            pass

    await node.close()
    return 0


# ============== tunnel (server / client) ==============


@dataclass
class _TunnelArgs:
    """隧道启动参数（收敛 CLI 传入的配置）。"""

    signaling: str
    room: str
    is_server: bool
    tcp_port: int | None
    udp_port: int | None
    name: str


def _infer_protocol(tcp_port: int | None, udp_port: int | None) -> str:
    """根据端口参数推断隧道协议"""
    if tcp_port and udp_port:
        return "both"
    if udp_port and not tcp_port:
        return "udp"
    return "tcp"


def _print_tunnel_mapping(
    is_server: bool, protocol: str, tcp_port: int | None, udp_port: int | None
) -> None:
    """打印隧道端口映射信息"""
    role_label = "SERVER" if is_server else "CLIENT"
    logger.info(f"=== tunnel {role_label} ===")
    logger.info(f"protocol = {protocol}")
    if is_server:
        if tcp_port:
            logger.info(f"P2P → forward to TCP 127.0.0.1:{tcp_port}")
        if udp_port:
            logger.info(f"P2P → forward to UDP 127.0.0.1:{udp_port}")
    else:
        if tcp_port:
            logger.info(f"Listen TCP 127.0.0.1:{tcp_port} → P2P")
        if udp_port:
            logger.info(f"Listen UDP 127.0.0.1:{udp_port} → P2P")


async def _run_tunnel(args: _TunnelArgs) -> int:
    signaling = args.signaling
    room = args.room
    is_server = args.is_server
    tcp_port = args.tcp_port
    udp_port = args.udp_port
    name = args.name

    protocol = _infer_protocol(tcp_port, udp_port)
    role = ConnectionRole.RESPONDER if is_server else ConnectionRole.INITIATOR
    role_label = "SERVER" if is_server else "CLIENT"

    tunnel_cfg = TunnelConfig(
        protocol=protocol,
        name=name or role_label,
        local_listen_port=tcp_port or 0,
        remote_forward_port=tcp_port or 0,
        local_listen_port_udp=udp_port,
        remote_forward_port_udp=udp_port,
    )

    # 打印实际映射
    _print_tunnel_mapping(is_server, protocol, tcp_port, udp_port)

    p2p_cfg = build_p2p_config(role, signaling)

    tunnel = GameTunnel(p2p_cfg, tunnel_cfg, role)

    async def print_stats() -> None:
        while True:
            await asyncio.sleep(30)
            s = tunnel.get_stats()
            logger.info(
                f"[STATS] peers={s['peer_count']} tcp={s['active_tcp']} udp={s['active_udp']} "
                f"total={s['total_connections']} bytes={s['bytes_forwarded_mb']}MB"
            )

    stats_task = spawn_task(print_stats(), context="Tunnel print stats")
    try:
        await tunnel.start(signaling, room)
        while True:
            await asyncio.sleep(1.0)
    except KeyboardInterrupt:
        pass
    finally:
        stats_task.cancel()
        await tunnel.stop()
    return 0


# ============== CLI ==============


def build_parser() -> argparse.ArgumentParser:
    """构建 CLI 参数解析器。"""
    p = argparse.ArgumentParser(
        prog="p2p",
        description="P2P Tunnel (KCP + SCTP via DataChannel + Cloudflare TURN)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # 1) 信令服务器 (终端1)
  $ p2p-signaling --port 8765

  # 2) P2P 通信测试 (side A 发消息, side B 回)
  $ p2p chat my-room --as a
  $ p2p chat my-room --as b

  # 3) 性能测试 (30s)
  $ p2p bench my-room --as a
  $ p2p bench my-room --as b

  # 4) TCP 隧道 (SERVER 跑服务端, 端口 25565)
  $ p2p server my-room --tcp 25565
  $ p2p client my-room --tcp 25565
  # 用户客户端连接 127.0.0.1:25565

  # 5) UDP 隧道 (MC 基岩版端口 19132)
  $ p2p server my-room --udp 19132
  $ p2p client my-room --udp 19132

  # 6) 同时 TCP + UDP (端口可不同)
  $ p2p server my-room --tcp 25565 --udp 19132
  $ p2p client my-room --tcp 25565 --udp 19132

  # 7) 指定信令服务器
  $ p2p server my-room -s ws://signaling.example.com:8765 --tcp 25565
""",
    )
    p.add_argument("--log-level", choices=["DEBUG", "INFO", "WARNING", "ERROR"], default="INFO")

    sub = p.add_subparsers(dest="cmd", required=True, metavar="<cmd>")

    # --- chat ---
    sp = sub.add_parser("chat", help="P2P 通信测试 (A 发消息, B 回)")
    sp.add_argument("room", help="房间 ID")
    sp.add_argument(
        "--as",
        dest="as_side",
        choices=["a", "b"],
        required=True,
        help="a=initiator(发测试消息), b=responder(回消息)",
    )
    sp.add_argument(
        "-s",
        "--signaling",
        default=DEFAULT_SIGNALING,
        help=f"信令服务器 (默认: {DEFAULT_SIGNALING})",
    )

    # --- bench ---
    sp = sub.add_parser("bench", help="性能测试 (A 发大数据, B 收)")
    sp.add_argument("room", help="房间 ID")
    sp.add_argument(
        "--as",
        dest="as_side",
        choices=["a", "b"],
        required=True,
        help="a=sender, b=receiver",
    )
    sp.add_argument("-s", "--signaling", default=DEFAULT_SIGNALING)
    sp.add_argument("--duration", type=float, default=30.0, help="A 端发送时长（秒，默认 30）")

    # --- server ---
    sp = sub.add_parser("server", help="SERVER 端：将 P2P 流量转发到本地服务端口")
    sp.add_argument("room", help="房间 ID（与 client 端相同）")
    sp.add_argument("-s", "--signaling", default=DEFAULT_SIGNALING)
    sp.add_argument(
        "--tcp",
        dest="tcp_port",
        type=int,
        default=None,
        help="本地目标 TCP 端口（SERVER 侧服务端监听端口）",
    )
    sp.add_argument(
        "--udp",
        dest="udp_port",
        type=int,
        default=None,
        help="本地目标 UDP 端口（SERVER 侧服务端监听端口）",
    )
    sp.add_argument("--name", default="", help="日志显示名")

    # --- client ---
    sp = sub.add_parser("client", help="CLIENT 端：起本地监听，通过 P2P 连到 SERVER 端服务")
    sp.add_argument("room", help="房间 ID（与 server 端相同）")
    sp.add_argument("-s", "--signaling", default=DEFAULT_SIGNALING)
    sp.add_argument(
        "--tcp",
        dest="tcp_port",
        type=int,
        default=None,
        help="本地监听 TCP 端口（用户客户端连这个）",
    )
    sp.add_argument(
        "--udp",
        dest="udp_port",
        type=int,
        default=None,
        help="本地监听 UDP 端口（用户客户端连这个）",
    )
    sp.add_argument("--name", default="", help="日志显示名")

    return p


def main() -> None:
    """CLI 入口：解析参数、配置日志并运行隧道。"""
    parser = build_parser()
    args = parser.parse_args()

    _setup_logger(args.log_level)

    try:
        if args.cmd == "chat":
            rc = asyncio.run(_run_chat(args.signaling, args.room, args.as_side))
        elif args.cmd == "bench":
            rc = asyncio.run(_run_bench(args.signaling, args.room, args.as_side, args.duration))
        elif args.cmd == "server":
            if not (args.tcp_port or args.udp_port):
                logger.error("server 需要至少一个: --tcp PORT 或 --udp PORT")
                rc = 1
            else:
                rc = asyncio.run(
                    _run_tunnel(
                        _TunnelArgs(
                            signaling=args.signaling,
                            room=args.room,
                            is_server=True,
                            tcp_port=args.tcp_port,
                            udp_port=args.udp_port,
                            name=args.name,
                        )
                    )
                )
        elif args.cmd == "client":
            if not (args.tcp_port or args.udp_port):
                logger.error("client 需要至少一个: --tcp PORT 或 --udp PORT")
                rc = 1
            else:
                rc = asyncio.run(
                    _run_tunnel(
                        _TunnelArgs(
                            signaling=args.signaling,
                            room=args.room,
                            is_server=False,
                            tcp_port=args.tcp_port,
                            udp_port=args.udp_port,
                            name=args.name,
                        )
                    )
                )
        else:
            parser.print_help()
            rc = 1
    except KeyboardInterrupt:
        logger.info("Interrupted")
        rc = 0
    except Exception as e:
        logger.exception(f"Unexpected error: {e}")
        rc = 1

    sys.exit(rc)


if __name__ == "__main__":
    main()
