"""websockets 兼容层

统一封装 websockets 库的导入与版本差异处理，避免在多个模块中重复
try/except 导入逻辑。

websockets 库在 14.0 前后对连接对象类型（``WebSocketServerProtocol`` →
``ServerConnection``）和模块结构（``websockets.server`` /
``websockets.asyncio.server``）有较大调整，且各版本类型 stub 定义不完整
（如 ``__aiter__``、``send`` 等方法在 stub 中缺失），因此这里以 ``Any``
暴露连接类型，避免类型标注与特定版本强耦合，同时保持运行时行为一致。
"""

from __future__ import annotations

from typing import Any

try:
    import websockets

    _websockets_available = True
except ImportError:
    websockets = None  # type: ignore[assignment]
    _websockets_available = False

# 常量只赋值一次，避免触发基于 pyright 的 reportConstantRedefinition
WEBSOCKETS_AVAILABLE = _websockets_available

# 连接对象类型：websockets 14+ 为 ServerConnection，低版本为
# WebSocketServerProtocol。因 stub 不完整，统一用 Any 规避版本差异。
# 变量名保留 PascalCase 以与 websockets 类名一致（有意为之）。
ServerConnection: Any = None  # pylint: disable=invalid-name
if WEBSOCKETS_AVAILABLE:
    try:
        from websockets.server import ServerConnection as _SC

        ServerConnection = _SC
    except ImportError:
        try:
            from websockets.legacy.server import WebSocketServerProtocol as _WSP

            ServerConnection = _WSP
        except ImportError:
            ServerConnection = Any

__all__ = ["WEBSOCKETS_AVAILABLE", "ServerConnection", "websockets"]
