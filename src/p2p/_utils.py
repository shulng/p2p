"""P2P 项目公共工具函数

集中放置被多个模块复用的通用逻辑，避免重复代码（DRY）：

- ``cancel_task``：安全取消一个 asyncio 任务（忽略 CancelledError）
"""

from __future__ import annotations

import asyncio
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


__all__ = [
    "cancel_task",
]
