"""消息编解码模块

负责 ``Message`` 与传输字节流之间的序列化/反序列化，以及 payload 中
bytes 的 base64 内嵌与还原。纯逻辑、无网络依赖，可独立单元测试。

- ``encode_message`` / ``decode_message``：Message <-> JSON 字节串
- ``encode_payload`` / ``decode_payload``：递归处理嵌套 bytes

从 ``P2PNode`` 的「编解码」职责中提取，使节点门面更聚焦于协调各模块。
"""

from __future__ import annotations

import base64
import json
from typing import Any

from .types import Message, MessageType


def encode_payload(payload: Any) -> Any:
    """递归将 payload 编码为可 JSON 序列化的结构

    - bytes -> {"__type__": "bytes", "__data__": "<base64>"}（解码时还原为 bytes）
    - dict / list 递归处理内嵌的 bytes（如隧道消息 {..., "data": b'...'}）
    - 其余原样返回（str / None / 数值等，须为 JSON 可序列化类型）
    """
    if isinstance(payload, bytes):
        return {"__type__": "bytes", "__data__": base64.b64encode(payload).decode("ascii")}
    if isinstance(payload, dict):
        return {k: encode_payload(v) for k, v in payload.items()}
    if isinstance(payload, (list, tuple)):
        return [encode_payload(item) for item in payload]
    return payload


def decode_payload(payload: Any) -> Any:
    """将 JSON 结构还原为原始 payload"""
    if isinstance(payload, dict):
        # 仅当存在 __type__ 标记时才解码为 bytes，避免与用户合法字典冲突
        if payload.get("__type__") == "bytes" and "__data__" in payload:
            return base64.b64decode(payload["__data__"])
        return {k: decode_payload(v) for k, v in payload.items()}
    if isinstance(payload, list):
        return [decode_payload(item) for item in payload]
    return payload


def encode_message(msg: Message) -> bytes:
    """编码消息为 JSON 字节串（替代 pickle，避免反序列化安全风险）

    仅支持 JSON 可序列化的 payload；bytes 通过 base64 内嵌。
    """
    return json.dumps(
        {
            "msg_id": msg.msg_id,
            "msg_type": msg.msg_type.value,
            "sender_id": msg.sender_id,
            "receiver_id": msg.receiver_id,
            "payload": encode_payload(msg.payload),
            "timestamp": msg.timestamp.isoformat(),
            "seq": msg.seq,
        },
        ensure_ascii=False,
    ).encode("utf-8")


def decode_message(data: bytes) -> Message:
    """从 JSON 字节串解码消息"""
    obj = json.loads(data.decode("utf-8"))
    return Message(
        msg_id=obj["msg_id"],
        msg_type=MessageType(obj["msg_type"]),
        sender_id=obj["sender_id"],
        receiver_id=obj["receiver_id"],
        payload=decode_payload(obj["payload"]),
        seq=obj["seq"],
    )


__all__ = [
    "decode_message",
    "decode_payload",
    "encode_message",
    "encode_payload",
]
