"""P2P 项目统一公共调用模块

集中放置被多个模块复用的通用操作逻辑，作为整个项目的「统一调用入口」，
确保各模块通过一致的接口进行任务管理、超时等待、回调分发与状态管理，
避免重复实现与分散管理（DRY）。

统一约定：
- 参数传递：全部以位置/关键字参数显式传入，返回值为可预期的基础类型。
- 返回值格式：成功返回明确的值（``True`` / 目标值），失败返回 ``None``
  / ``False``，不抛出业务无关的异常。
- 异常处理：异步任务统一由 :func:`spawn_task` 接管，异常只记录日志不
  向上抛，避免任务被 asyncio 静默丢弃；等待类操作超时统一返回 ``None``。
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from typing import Any, TypeVar, cast

from loguru import logger

from .config import ConnectionRole, IceConfig, P2PConfig

# 事件等待的目标值类型
_T = TypeVar("_T")

# ============== 任务管理（统一调用入口） ==============


async def cancel_task(task: asyncio.Task[Any] | None) -> None:
    """安全取消一个 asyncio 任务，并等待其结束。

    若任务不存在、已完成或已取消，则静默返回。等待期间会吞掉
    ``asyncio.CancelledError``，避免在调用方重复 try/except。

    Args:
        task: 要取消的 asyncio 任务，可为 ``None``。
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


def _log_task_exception(
    task: asyncio.Task[Any],
    *,
    context: str,
    error_level: str = "error",
) -> None:
    """记录后台任务执行后的异常（若存在）。"""
    if task.cancelled():
        return
    exc = task.exception()
    if exc is not None:
        if error_level == "debug":
            logger.debug(f"[{context}] task error: {exc}")
        else:
            logger.error(f"[{context}] task error: {exc}")


def spawn_task(
    coro: Awaitable[Any],
    *,
    name: str | None = None,
    context: str | None = None,
    log_error: bool = True,
) -> asyncio.Task[Any]:
    """创建后台任务，并统一接管异常处理（返回的 task 不会产生未捕获异常）。

    这是项目中所有「fire-and-forget」后台任务的统一入口。调用方无需
    再重复 ``asyncio.create_task(...)`` + ``add_done_callback(...)`` 的样板。

    Args:
        coro: 要并发执行的协程对象。
        name: 可选的 task 名称（用于调试）。
        context: 异常日志前缀，默认使用 ``name``。
        log_error: 是否在任务异常时记录日志（默认记录 error）。

    Returns:
        创建好的 :class:`asyncio.Task`。调用方仍可自由选择是否 ``await``
        或 ``cancel`` 它；异常已被内部消化，不会成为“僵尸异常”。
    """
    task = asyncio.create_task(coro, name=name)
    if log_error:
        ctx = context or name or "task"
        task.add_done_callback(
            lambda t: _log_task_exception(t, context=ctx)  # type: ignore[arg-type]
        )
    return task


# ============== 超时等待（统一调用入口） ==============


async def wait_event(
    event: asyncio.Event,
    timeout: float | None,
    *,
    context: str = "event",
) -> bool:
    """等待一个 :class:`asyncio.Event` 置位，带统一超时语义。

    Args:
        event: 要等待的事件。
        timeout: 超时秒数；为 ``None`` 表示无限等待。
        context: 日志上下文前缀（超时告警用）。

    Returns:
        ``True`` 表示事件已置位；``False`` 表示等待超时。
    """
    if timeout is None:
        await event.wait()
        return True
    try:
        await asyncio.wait_for(event.wait(), timeout=timeout)
        return True
    except asyncio.TimeoutError:
        logger.warning(f"[{context}] wait timed out after {timeout}s")
        return False


async def wait_for_result(
    coro: Awaitable[_T],
    timeout: float | None,
    *,
    default: _T | None = None,
    context: str = "operation",
) -> _T | None:
    """等待一个协程完成，超时返回 ``default``（默认 ``None``）。

    统一项目中「带超时执行」的调用模式：超时不抛异常，而是返回默认值，
    由调用方依据返回值判断结果，避免在各处重复 ``try/except TimeoutError``。

    Args:
        coro: 要执行的协程。
        timeout: 超时秒数；``None`` 表示不设超时。
        default: 超时时返回的默认值。
        context: 日志上下文前缀（超时告警用）。

    Returns:
        协程的返回值；若超时则返回 ``default``。
    """
    if timeout is None:
        return await coro
    try:
        return await asyncio.wait_for(coro, timeout=timeout)
    except asyncio.TimeoutError:
        logger.warning(f"[{context}] timed out after {timeout}s")
        return default


# ============== 回调分发（统一调用入口） ==============


def dispatch(callback: Callable[..., Any] | None, *args: object) -> None:
    """统一调度一个回调：兼容同步函数与异步协程函数。

    所有模块接收外部回调时，都应通过本函数调用，保证：
    - 同步回调直接执行；
    - 返回协程的异步回调交给 :func:`spawn_task` 后台运行（异常被接管）。

    Args:
        callback: 回调对象；为 ``None`` 时静默返回。
        *args: 传给回调的位置参数。
    """
    if callback is None:
        return
    result = callback(*args)
    if asyncio.iscoroutine(result):
        spawn_task(cast(Awaitable[Any], result), context=getattr(callback, "__name__", "callback"))


# ============== 状态管理（统一调用入口） ==============


def set_state(
    holder: Any,
    new_state: Any,
    *,
    attribute: str = "state",
    context: str = "",
) -> bool:
    """更新对象的连接状态，并仅在状态变化时记录日志。

    统一项目中各模块（``P2PNode`` / ``KCPTransport`` / ``IceManager``）
    重复的 ``_set_state`` 模式：比较新旧状态、写属性、打印状态迁移日志。
    状态变更时的自定义动作由调用方在返回后自行处理（例如触发回调）。

    Args:
        holder: 状态持有对象。
        new_state: 新状态值。
        attribute: 状态属性名（默认 ``state``）。
        context: 日志前缀，如 ``P2PNode`` / ``KCP`` / ``ICE``。

    Returns:
        ``True`` 表示状态发生了实际变化；``False`` 表示状态未变。
    """
    if getattr(holder, attribute) == new_state:
        return False
    old_state = getattr(holder, attribute)
    setattr(holder, attribute, new_state)
    prefix = f"[{context}] " if context else ""
    logger.info(f"{prefix}State changed: {old_state} -> {new_state}")
    return True


# ============== 配置构造（统一调用入口） ==============


def build_p2p_config(
    role: ConnectionRole,
    signaling_url: str,
) -> P2PConfig:
    """构建 P2P 节点配置（统一入口）。

    收敛各模块（CLI / 编程式调用）重复的 ``P2PConfig`` + Cloudflare TURN
    构造逻辑，确保角色与信令地址的配置方式一致，避免重复实现。

    Args:
        role: 节点连接角色（INITIATOR / RESPONDER）。
        signaling_url: 信令服务器 WebSocket 地址。

    Returns:
        已配置好角色、Cloudflare TURN 与信令地址的 :class:`P2PConfig`。
    """
    cfg = P2PConfig(
        role=role,
        ice=IceConfig.with_cloudflare_turn(),
    )
    cfg.signaling.server_url = signaling_url
    return cfg


def safe_close(awaitable_or_callable: Any) -> None:
    """安全地关闭一个资源：优先 ``await``，否则调用后吞掉异常。

    统一项目中各模块重复的 ``try/except`` + close 样板。
    既支持协程（``await``），也支持同步 close 方法。

    Args:
        awaitable_or_callable: 可等待对象（协程）或可调用对象。
    """
    try:
        result = awaitable_or_callable()
        if asyncio.iscoroutine(result):
            spawn_task(cast(Awaitable[Any], result), context="close")
    except Exception as e:  # pragma: no cover - 防御性兜底
        logger.debug(f"Error while closing resource: {e}")


__all__ = [
    "build_p2p_config",
    "cancel_task",
    "dispatch",
    "safe_close",
    "set_state",
    "spawn_task",
    "wait_event",
    "wait_for_result",
]
