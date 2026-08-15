"""P2P 库 - QUIC + KCP + Cloudflare TURN

提供点对点通信能力，支持：
- QUIC 可靠传输 (基于 aioquic)
- KCP 可靠 UDP 传输 (纯 Python 实现)
- ICE/STUN/TURN NAT 穿透 (基于 aiortc)
- Cloudflare TURN 服务器 (turn.cloudflare.com:3478)
- WebSocket 信令服务器和客户端
"""

__version__ = "0.1.0"

from .config import (
    P2PConfig,
    IceConfig,
    QuicConfig,
    KcpConfig,
    TurnServerConfig,
    SignalingConfig,
    TransportProtocol,
    ConnectionRole,
)
from .types import (
    ConnectionState,
    MessageType,
    Message,
    PeerInfo,
    SessionDescription,
    IceCandidate,
    TransportStats,
    ConnectionStats,
    RoomInfo,
    generate_peer_id,
)
from .kcp import KCP
from .kcp_transport import KCPTransport
from .quic_transport import QUICTransport
from .ice_manager import IceManager
from .signaling_client import SignalingClient, SignalingEvents
from .signaling_server import SignalingServer
from .node import P2PNode

__all__ = [
    # Config
    "P2PConfig",
    "IceConfig",
    "QuicConfig",
    "KcpConfig",
    "TurnServerConfig",
    "SignalingConfig",
    "TransportProtocol",
    "ConnectionRole",
    # Types
    "ConnectionState",
    "MessageType",
    "Message",
    "PeerInfo",
    "SessionDescription",
    "IceCandidate",
    "TransportStats",
    "ConnectionStats",
    "RoomInfo",
    "generate_peer_id",
    # KCP
    "KCP",
    "KCPTransport",
    # QUIC
    "QUICTransport",
    # ICE
    "IceManager",
    # Signaling
    "SignalingClient",
    "SignalingEvents",
    "SignalingServer",
    # Node
    "P2PNode",
]
