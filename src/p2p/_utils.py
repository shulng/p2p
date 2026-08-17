"""P2P 项目公共工具函数

集中放置被多个模块复用的通用逻辑，避免重复代码（DRY）：

- ``cancel_task``：安全取消一个 asyncio 任务（忽略 CancelledError）
- ``call_async``：同步上下文安全地调度一个协程到事件循环
- ``max_val`` / ``min_val``：数值下界/上界裁剪（替代手写 if/else，配合 PLR1730）
"""

from __future__ import annotations

import asyncio
import concurrent.futures
from typing import Any

from loguru import logger


async def cancel_task(task: asyncio.Task[Any] | None) -> None:
    """安全取消一个 asyncio 任务，并等待其结束。

    若任务不存在、已完成或已取消，则静默返回。等待期间会吞掉
    ``asyncio.CancelledError``，避免在调用方重复 try/except。
    """
    if task is None or task.done():
        return
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass
    except Exception as e:  # pragma: no cover - 防御性兜底
        logger.debug(f"Error while awaiting cancelled task: {e}")


def run_coroutine_threadsafe(
    coro: Any, loop: asyncio.AbstractEventLoop
) -> concurrent.futures.Future[Any] | None:
    """在另一个线程中安全地调度协程到指定事件循环。

    用于同步回调（如 ICE 事件、DataChannel 回调）中触发异步操作。
    返回新创建的 Future；若事件循环已关闭则返回 None。
    """
    try:
        return asyncio.run_coroutine_threadsafe(coro, loop)
    except RuntimeError:
        logger.debug("Event loop is closed, cannot schedule coroutine")
        return None


def max_val(low: float, x: float) -> int | float:
    """取 ``max(low, x)`` 的命名封装（提升可读性，配合 pylint PLR1730）。"""
    return max(low, x)


def min_val(high: float, x: float) -> int | float:
    """取 ``min(high, x)`` 的命名封装（提升可读性，配合 pylint PLR1730）。"""
    return min(high, x)


__all__ = [
    "cancel_task",
    "max_val",
    "min_val",
    "run_coroutine_threadsafe",
]
