"""ICE 候选对提取器（适配器层）

用于从底层 WebRTC 库提取「被选中的 ICE 候选对」的 IP/端口信息，
供 KCP 直连使用。通过抽象接口 ``CandidatePairExtractor`` 将脆弱的
底层私有 API 读取集中收敛到独立实现中：

- ``AiortcPairExtractor``：读取 aiortc 内部私有属性（``_transports`` /
  ``iceTransport`` / ``_selected_pair``）。aiortc 目前没有公开的 ICE
  候选对查询 API（其 ``getStats()`` 只返回 RTP 流统计），因此只能依赖
  内部结构。升级或更换 WebRTC 库时，仅需替换本模块的实现。
- ``NullPairExtractor``：返回 ``None``（降级方案），KCP 直连不可用时
  上层会自动回退到 SCTP/DataChannel。

``IceManager`` 面向 ``CandidatePairExtractor`` 接口编程，不直接接触
任何底层库的内部结构，从而降低脆性耦合。
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Protocol

from loguru import logger


@dataclass(frozen=True)
class CandidateAddr:
    """候选地址（IP / 端口 / 协议 / 类型）"""

    ip: str
    port: int
    protocol: str = "udp"
    type: str = "unknown"


@dataclass(frozen=True)
class SelectedPair:
    """被选中的 ICE 候选对"""

    local: CandidateAddr
    remote: CandidateAddr


class CandidatePairExtractor(Protocol):
    """候选对提取器抽象接口"""

    async def extract(self, pc: object) -> SelectedPair | None:
        """从给定的 PeerConnection 提取被选中的候选对，提取失败返回 None。"""
        ...


class AiortcPairExtractor:
    """基于 aiortc 私有属性的候选对提取器

    所有对 aiortc 内部结构的读取都集中在本类。aiortc 的
    ``RTCPeerConnection.getStats()`` 不包含 ICE 候选对信息，故必须读取
    内部对象图。读取全程容错：任何一层属性缺失时静默跳过并返回 None，
    不影响 ICE 连接（KCP 直连失败自动降级 SCTP）。
    """

    async def extract(self, pc: object) -> SelectedPair | None:
        try:
            transports = getattr(pc, "_transports", {})
            for transport in transports.values():
                ice_transport = getattr(transport, "iceTransport", None)
                if not ice_transport:
                    continue
                selected_pair = getattr(ice_transport, "_selected_pair", None)
                if not selected_pair:
                    continue
                local = getattr(selected_pair, "localCandidate", None)
                remote = getattr(selected_pair, "remoteCandidate", None)
                if not local or not remote:
                    continue
                local_ip = getattr(local, "ip", None)
                local_port = getattr(local, "port", None)
                remote_ip = getattr(remote, "ip", None)
                remote_port = getattr(remote, "port", None)
                if not (local_ip and local_port and remote_ip and remote_port):
                    continue
                return SelectedPair(
                    local=CandidateAddr(
                        ip=str(local_ip),
                        port=int(local_port),
                        protocol=getattr(local, "protocol", "udp"),
                        type=getattr(local, "type", "unknown"),
                    ),
                    remote=CandidateAddr(
                        ip=str(remote_ip),
                        port=int(remote_port),
                        protocol=getattr(remote, "protocol", "udp"),
                        type=getattr(remote, "type", "unknown"),
                    ),
                )
        except Exception as e:
            logger.debug(f"[ICE] Could not extract selected candidates: {e}")
        return None


class NullPairExtractor:
    """降级提取器：始终返回 None（KCP 直连不可用，走 SCTP 兜底）"""

    async def extract(self, pc: object) -> SelectedPair | None:
        return None


def default_pair_extractor() -> CandidatePairExtractor:
    """返回默认的候选对提取器（aiortc 私有实现）。"""
    return AiortcPairExtractor()


__all__ = [
    "AiortcPairExtractor",
    "CandidateAddr",
    "CandidatePairExtractor",
    "NullPairExtractor",
    "SelectedPair",
    "default_pair_extractor",
]
