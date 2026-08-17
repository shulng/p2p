"""per-peer 有序消息路由模块

负责将来自同一 peer 的消息按到达顺序串行交给回调处理（TCP 字节流强依赖
字节顺序，并发 ``create_task`` 会导致乱序）。为每个 peer 维护一个无上限
队列与单 worker，worker 按队列顺序 await 处理。

从 ``P2PNode`` 的「队列」职责中提取，节点门面只需在收数据时调用
``submit()``，在清理 peer 时调用 ``stop_peer()`` 即可。
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import Awaitable, Callable
from typing import Any

from loguru import logger

from ..types import Message


class OrderedMessageRouter:
    """per-peer 消息有序路由：同一 peer 的消息按到达顺序串行处理。

    通过「队列 + 单 worker」实现顺序保证，避免并发处理导致乱序。
    队列采用无上限设计，不主动丢消息，内存增长由对端发送速率决定。
    """

    def __init__(
        self,
        on_message: Callable[[Message], Awaitable[None] | None] | None = None,
    ) -> None:
        self._on_message = on_message
        self._queues: dict[str, asyncio.Queue[Message | None]] = {}
        self._workers: dict[str, asyncio.Task[Any]] = {}

    @property
    def has_callbacks(self) -> bool:
        return self._on_message is not None

    def submit(self, peer_id: str, msg: Message) -> None:
        """将消息放入该 peer 的队列（首次调用会创建队列与 worker）"""
        queue = self._get_or_create_queue(peer_id)
        queue.put_nowait(msg)

    async def stop_peer(self, peer_id: str) -> None:
        """停止该 peer 的 worker 并清理其队列"""
        queue = self._queues.pop(peer_id, None)
        worker = self._workers.pop(peer_id, None)
        if worker is None:
            return
        if queue is not None:
            queue.put_nowait(None)  # 停止信号
        worker.cancel()
        # 等待 worker 结束；触发 CancelledError 属预期行为，静默忽略
        with contextlib.suppress(asyncio.CancelledError):
            await worker

    async def stop_all(self) -> None:
        """停止全部 worker 并清空队列"""
        for peer_id in list(self._queues.keys()):
            await self.stop_peer(peer_id)

    def _get_or_create_queue(self, peer_id: str) -> asyncio.Queue[Message | None]:
        if peer_id not in self._queues:
            queue: asyncio.Queue[Message | None] = asyncio.Queue()
            self._queues[peer_id] = queue
            self._workers[peer_id] = asyncio.create_task(self._worker(peer_id, queue))
            logger.debug(f"[OrderedMessageRouter] Created queue+worker for {peer_id}")
        return self._queues[peer_id]

    async def _worker(self, peer_id: str, queue: asyncio.Queue[Message | None]) -> None:
        """per-peer worker：按队列顺序 await 处理，保证消息不乱序"""
        while True:
            msg = await queue.get()
            if msg is None:  # 停止信号
                queue.task_done()
                break
            try:
                if self._on_message:
                    result = self._on_message(msg)
                    if asyncio.iscoroutine(result):
                        await result
            except Exception as e:
                logger.error(f"[OrderedMessageRouter] Handler error for {peer_id}: {e}")
            finally:
                queue.task_done()


__all__ = ["OrderedMessageRouter"]
