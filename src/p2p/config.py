"""P2P 配置模块"""
from dataclasses import dataclass, field
from typing import List, Optional
from enum import Enum


class TransportProtocol(str, Enum):
    """传输协议类型"""
    QUIC = "quic"
    KCP = "kcp"
    AUTO = "auto"


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
class QuicConfig:
    """QUIC 传输配置"""
    enabled: bool = True
    host: str = "0.0.0.0"
    port: int = 0  # 0表示随机端口
    # 重试和超时设置(秒)
    idle_timeout: float = 30.0
    handshake_timeout: float = 10.0
    # 拥塞控制
    max_data: int = 10485760  # 10MB
    max_stream_data: int = 1048576  # 1MB per stream
    max_streams_bidi: int = 100
    max_streams_uni: int = 100


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
    ice_servers: List[TurnServerConfig] = field(default_factory=list)
    ice_transport_policy: str = "all"  # all / relay
    gather_timeout: float = 10.0
    connectivity_check_timeout: float = 15.0

    @classmethod
    def with_cloudflare_turn(cls) -> "IceConfig":
        """使用 Cloudflare TURN 服务器创建配置"""
        return cls(
            ice_servers=[
                TurnServerConfig(
                    url="stun:stun.cloudflare.com:3478",
                    use_cloudflare=True
                ),
                TurnServerConfig(
                    url="stun:turn.cloudflare.com:3478",
                    use_cloudflare=True
                ),
                TurnServerConfig(
                    url="turn:turn.cloudflare.com:3478?transport=udp",
                    use_cloudflare=True
                ),
                TurnServerConfig(
                    url="turn:turn.cloudflare.com:3478?transport=tcp",
                    use_cloudflare=True
                ),
            ]
        )


@dataclass
class SignalingConfig:
    """信令服务器配置"""
    server_url: str = "ws://localhost:8765"
    reconnect_interval: float = 2.0
    max_reconnect_attempts: int = 10
    room_id: Optional[str] = None
    peer_id: Optional[str] = None


@dataclass
class P2PConfig:
    """P2P 主配置"""
    transport: TransportProtocol = TransportProtocol.AUTO
    role: ConnectionRole = ConnectionRole.INITIATOR
    quic: QuicConfig = field(default_factory=QuicConfig)
    kcp: KcpConfig = field(default_factory=KcpConfig)
    ice: IceConfig = field(default_factory=IceConfig.with_cloudflare_turn)
    signaling: SignalingConfig = field(default_factory=SignalingConfig)
    # 日志级别
    log_level: str = "INFO"
