"""应用层：通用 P2P 隧道（TCP/UDP 流量转发）。"""

from __future__ import annotations

from .game_tunnel import GameTunnel, TunnelConfig

__all__ = ["GameTunnel", "TunnelConfig"]
