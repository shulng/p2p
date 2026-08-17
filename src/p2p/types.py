"""P2P 类型定义模块"""

import uuid
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any

from .config import ConnectionRole, TransportProtocol


def generate_peer_id() -> str:
    """生成唯一的 Peer ID"""
    return f"peer-{uuid.uuid4().hex[:12]}"


class MessageType(str, Enum):
    """消息类型"""

    # 信令消息
    SIGNAL_OFFER = "signal.offer"
    SIGNAL_ANSWER = "signal.answer"
    SIGNAL_ICE_CANDIDATE = "signal.ice_candidate"
    SIGNAL_JOIN = "signal.join"
    SIGNAL_LEAVE = "signal.leave"
    SIGNAL_PING = "signal.ping"
    SIGNAL_PONG = "signal.pong"
    SIGNAL_ROOM_INFO = "signal.room_info"

    # P2P 数据消息
    DATA_TEXT = "data.text"
    DATA_BINARY = "data.binary"
    DATA_JSON = "data.json"
    DATA_FILE = "data.file"

    # 控制消息
    CTRL_HEARTBEAT = "ctrl.heartbeat"
    CTRL_ACK = "ctrl.ack"
    CTRL_TRANSPORT_SWITCH = "ctrl.transport_switch"
    CTRL_ERROR = "ctrl.error"


class ConnectionState(str, Enum):
    """连接状态"""

    DISCONNECTED = "disconnected"
    CONNECTING = "connecting"
    CONNECTED = "connected"
    CHECKING = "checking"  # ICE 检查中
    CONNECTED_ICE = "connected_ice"  # ICE 连接成功
    RECONNECTING = "reconnecting"
    FAILED = "failed"
    CLOSED = "closed"


@dataclass
class PeerInfo:
    """Peer 信息"""

    peer_id: str
    role: ConnectionRole
    address: str | None = None
    port: int | None = None
    transport: TransportProtocol | None = None
    connected_at: datetime | None = None
    user_data: dict[str, Any] = field(default_factory=dict)


@dataclass
class Message:
    """P2P 消息"""

    msg_id: str
    msg_type: MessageType
    sender_id: str
    receiver_id: str
    payload: Any = None
    timestamp: datetime = field(default_factory=datetime.now)
    seq: int = 0

    @classmethod
    def create(
        cls,
        msg_type: MessageType,
        sender_id: str,
        receiver_id: str,
        payload: Any = None,
    ) -> "Message":
        """工厂方法：生成一条带随机 msg_id 的消息。"""
        return cls(
            msg_id=uuid.uuid4().hex,
            msg_type=msg_type,
            sender_id=sender_id,
            receiver_id=receiver_id,
            payload=payload,
        )


@dataclass
class IceCandidate:
    """ICE 候选地址"""

    candidate: str
    sdp_mid: str | None = None
    sdp_mline_index: int | None = None


@dataclass
class SessionDescription:
    """会话描述 (SDP)"""

    sdp_type: str  # offer / answer
    sdp: str


@dataclass
class TransportStats:
    """传输层统计"""

    bytes_sent: int = 0
    bytes_received: int = 0
    packets_sent: int = 0
    packets_received: int = 0
    packets_lost: int = 0
    rtt_ms: float = 0.0
    jitter_ms: float = 0.0
    last_updated: datetime = field(default_factory=datetime.now)


@dataclass
class ConnectionStats:
    """连接统计"""

    peer_id: str
    state: ConnectionState
    transport: TransportProtocol
    ice_state: str | None = None
    local_address: str | None = None
    remote_address: str | None = None
    kcp_stats: TransportStats | None = None
    connected_since: datetime | None = None


@dataclass
class RoomInfo:
    """房间信息"""

    room_id: str
    peers: list[PeerInfo] = field(default_factory=list)
    created_at: datetime = field(default_factory=datetime.now)
