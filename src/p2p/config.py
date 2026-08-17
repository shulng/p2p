"""P2P 配置模块"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum


class TransportProtocol(str, Enum):
    """传输协议类型"""

    KCP = "kcp"


class ConnectionRole(str, Enum):
    """连接角色"""

    INITIATOR = "initiator"
    RESPONDER = "responder"


@dataclass
class TurnServerConfig:
    """TURN 服务器配置"""

    url: str = "turn:turn.cloudflare.com:3478"
    username: str = ""
    credential: str = ""
    # Cloudflare TURN 特殊配置
    use_cloudflare: bool = True


@dataclass
class KcpConfig:
    """KCP 传输配置"""

    enabled: bool = True
    host: str = "0.0.0.0"
    port: int = 0
    # KCP 参数
    nodelay: bool = True
    interval: int = 10  # ms
    resend: int = 2
    nc: bool = True  # 无拥塞控制
    # 窗口大小
    sndwnd: int = 1024
    rcvwnd: int = 1024
    # MTU
    mtu: int = 1400


@dataclass
class IceConfig:
    """ICE 配置"""

    ice_servers: list[TurnServerConfig] = field(default_factory=list)
    ice_transport_policy: str = "all"  # all / relay
    gather_timeout: float = 10.0
    connectivity_check_timeout: float = 15.0

    @classmethod
    def with_cloudflare_turn(cls) -> "IceConfig":
        """使用 Cloudflare TURN 服务器创建配置"""
        return cls(
            ice_servers=[
                TurnServerConfig(url="stun:stun.cloudflare.com:3478", use_cloudflare=True),
                TurnServerConfig(
                    url="turn:turn.cloudflare.com:3478?transport=udp",
                    use_cloudflare=True,
                ),
                TurnServerConfig(
                    url="turn:turn.cloudflare.com:3478?transport=tcp",
                    use_cloudflare=True,
                ),
            ]
        )


@dataclass
class SignalingConfig:
    """信令服务器配置"""

    server_url: str = "ws://localhost:8765"
    reconnect_interval: float = 2.0
    max_reconnect_attempts: int = 10
    room_id: str | None = None
    room_role: ConnectionRole = ConnectionRole.INITIATOR
    peer_id: str | None = None


@dataclass
class P2PConfig:
    """P2P 主配置"""

    transport: TransportProtocol = TransportProtocol.KCP
    role: ConnectionRole = ConnectionRole.INITIATOR
    kcp: KcpConfig = field(default_factory=KcpConfig)
    ice: IceConfig = field(default_factory=IceConfig.with_cloudflare_turn)
    signaling: SignalingConfig = field(default_factory=SignalingConfig)
    # 日志级别
    log_level: str = "INFO"
