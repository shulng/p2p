"""P2P 库 - KCP + Cloudflare TURN

提供点对点通信能力，支持：
- KCP 可靠 UDP 传输 (纯 Python 实现)
- ICE/STUN/TURN NAT 穿透 (基于 aiortc)
- Cloudflare TURN 服务器 (turn.cloudflare.com:3478)
- WebSocket 信令服务器和客户端
"""

__version__ = "0.1.0"

from ._utils import (
    build_p2p_config,
    cancel_task,
    dispatch,
    set_state,
    spawn_task,
    wait_event,
    wait_for_result,
)
from .config import (
    ConnectionRole,
    IceConfig,
    KcpConfig,
    P2PConfig,
    SignalingConfig,
    TransportProtocol,
    TurnServerConfig,
)
from .ice.ice_manager import IceManager
from .node import P2PNode
from .signaling.client import SignalingClient, SignalingEvents
from .signaling.server import SignalingServer
from .transport.hybrid import (
    CHANNEL_CONTROL,
    CHANNEL_DATA,
    KCPDataTransport,
    KcpTransportStats,
)
from .transport.kcp import KCPTransport
from .transport.kcp_core import KCP
from .tunnel.game_tunnel import GameTunnel, TunnelConfig
from .types import (
    ConnectionState,
    ConnectionStats,
    IceCandidate,
    Message,
    MessageType,
    PeerInfo,
    RoomInfo,
    SessionDescription,
    TransportStats,
    generate_id,
    generate_peer_id,
)

__all__ = [
    "CHANNEL_CONTROL",
    "CHANNEL_DATA",
    "KCP",
    "build_p2p_config",
    "cancel_task",
    "ConnectionRole",
    "ConnectionState",
    "ConnectionStats",
    "GameTunnel",
    "IceCandidate",
    "IceConfig",
    "IceManager",
    "KCPDataTransport",
    "KCPTransport",
    "KcpConfig",
    "KcpTransportStats",
    "Message",
    "dispatch",
    "set_state",
    "spawn_task",
    "wait_event",
    "wait_for_result",
    "MessageType",
    "P2PConfig",
    "P2PNode",
    "PeerInfo",
    "RoomInfo",
    "SessionDescription",
    "SignalingClient",
    "SignalingConfig",
    "SignalingEvents",
    "SignalingServer",
    "TransportProtocol",
    "TransportStats",
    "TunnelConfig",
    "TurnServerConfig",
    "generate_id",
    "generate_peer_id",
]
