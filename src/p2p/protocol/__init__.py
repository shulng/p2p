"""消息处理层：消息编解码与 per-peer 有序路由。

承载节点（``P2PNode``）的消息级处理组件：
- ``message_codec``：Message 与字节流的序列化/反序列化
- ``message_router``：per-peer 消息有序分发
"""

from __future__ import annotations

from .message_codec import decode_message, decode_payload, encode_message, encode_payload
from .message_router import OrderedMessageRouter

__all__ = [
    "OrderedMessageRouter",
    "decode_message",
    "decode_payload",
    "encode_message",
    "encode_payload",
]
