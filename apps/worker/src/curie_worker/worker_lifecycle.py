"""@spec PROTECTED-HOOK-SOURCE-2."""

from __future__ import annotations

import asyncio
import logging
import os
import threading
from collections.abc import Awaitable, Callable
from typing import NoReturn

logger = logging.getLogger(__name__)


def fatal_worker_exit(log: logging.Logger, code: str, primary: str) -> NoReturn:
    """@spec PROTECTED-HOOK-SOURCE-2."""

    def emit() -> None:
        """@spec PROTECTED-HOOK-SOURCE-2."""
        try:
            log.error("%s primary=%s", code, primary)
        # a failed diagnostic must never prevent fatal process exit.
        except Exception:  # noqa: BLE001
            pass

    try:
        diagnostic = threading.Thread(target=emit, daemon=True)
        diagnostic.start()
        diagnostic.join(timeout=0.1)
    finally:
        # A blocked diagnostic thread dies with this process, never outliving cleanup.
        os._exit(1)


class WorkerLifecycleUnavailable(RuntimeError):
    """@spec PROTECTED-HOOK-SOURCE-2."""


async def worker_warning(log: logging.Logger, code: str, primary: str) -> None:
    """@spec PROTECTED-HOOK-SOURCE-2."""

    def emit() -> None:
        """@spec PROTECTED-HOOK-SOURCE-2."""
        try:
            log.warning("%s primary=%s", code, primary)
        # a failed diagnostic must never prevent remaining resource cleanup.
        except Exception:  # noqa: BLE001
            pass

    diagnostic = asyncio.create_task(asyncio.to_thread(emit))
    _, pending = await asyncio.wait({diagnostic}, timeout=5)
    if pending:
        fatal_worker_exit(log, "worker_diagnostic_deadline", primary)
    try:
        diagnostic.result()
    # diagnostic task faults must not interrupt cleanup.
    except Exception:  # noqa: BLE001
        pass


class WorkerResources:
    """@spec PROTECTED-HOOK-SOURCE-2."""

    def __init__(self) -> None:
        """@spec PROTECTED-HOOK-SOURCE-2."""
        self._closers: dict[str, tuple[int, Callable[[], Awaitable[None]]]] = {}
        self._tasks: dict[str, asyncio.Task[None]] = {}
        self._stops: dict[str, Callable[[], None]] = {}
        self._cleanup: asyncio.Task[None] | None = None
        self._primary: BaseException | None = None

    def register_close(
        self, name: str, close: Callable[[], Awaitable[None]], *, order: int
    ) -> None:
        """@spec PROTECTED-HOOK-SOURCE-2."""
        if self._cleanup is not None or name in self._closers:
            raise WorkerLifecycleUnavailable("worker_resource_registration_invalid")
        self._closers[name] = (order, close)

    def register_task(self, name: str, task: asyncio.Task[None]) -> None:
        """@spec PROTECTED-HOOK-SOURCE-2."""
        if self._cleanup is not None or name in self._tasks:
            raise WorkerLifecycleUnavailable("worker_task_registration_invalid")
        self._tasks[name] = task

    def register_stop(self, name: str, callback: Callable[[], None]) -> None:
        """@spec PROTECTED-HOOK-SOURCE-2."""
        if self._cleanup is not None or name in self._stops:
            raise WorkerLifecycleUnavailable("worker_stop_registration_invalid")
        self._stops[name] = callback

    def _cause(self) -> str:
        """@spec PROTECTED-HOOK-SOURCE-2."""
        return (
            "cancelled"
            if isinstance(self._primary, asyncio.CancelledError)
            else "error"
            if self._primary is not None
            else "none"
        )

    def _fatal(self, code: str) -> NoReturn:
        """@spec PROTECTED-HOOK-SOURCE-2."""
        fatal_worker_exit(logger, code, self._cause())

    async def _join_tasks(self, deadline: float) -> bool:
        """@spec PROTECTED-HOOK-SOURCE-2."""
        pending = {task for task in self._tasks.values() if not task.done()}
        if pending:
            _, pending = await asyncio.wait(
                pending, timeout=min(5, max(0, deadline - asyncio.get_running_loop().time()))
            )
        for task in pending:
            task.cancel()
        if pending:
            _, pending = await asyncio.wait(
                pending, timeout=max(0, deadline - asyncio.get_running_loop().time())
            )
        if pending:
            self._fatal("worker_producer_join_deadline")
        failed = False
        for task in self._tasks.values():
            if not task.cancelled() and task.exception() is not None:
                failed = True
        return failed

    async def _close_resources(self) -> None:
        """@spec PROTECTED-HOOK-SOURCE-2."""
        failed = False
        deadline = asyncio.get_running_loop().time() + 10
        for stop in self._stops.values():
            try:
                stop()
            # attempt every owned stop even when one callback fails.
            except Exception:  # noqa: BLE001
                failed = True
                await worker_warning(logger, "worker_stop_unavailable", self._cause())
        failed = await self._join_tasks(deadline) or failed
        for _, close in sorted(self._closers.values(), key=lambda item: item[0]):
            # Capture synchronous callback faults as well as coroutine failures.
            try:
                task = asyncio.ensure_future(close())
                _, pending = await asyncio.wait({task}, timeout=5)
                if pending:
                    self._fatal("worker_resource_close_deadline")
                task.result()
            # close every owned resource and redact secondary failures.
            except (Exception, asyncio.CancelledError):  # noqa: BLE001
                failed = True
                await worker_warning(logger, "worker_resource_close_unavailable", self._cause())
        if failed:
            if self._primary is not None:
                await worker_warning(logger, "worker_secondary_cleanup_unavailable", self._cause())
            raise WorkerLifecycleUnavailable("worker_cleanup_unavailable")

    async def aclose(self, primary: BaseException | None = None) -> None:
        """@spec PROTECTED-HOOK-SOURCE-2."""
        if self._cleanup is not None and self._cleanup.done():
            self._primary = primary
        elif self._primary is None:
            self._primary = primary
        if self._cleanup is None:
            self._cleanup = asyncio.create_task(self._close_resources())
        while not self._cleanup.done():
            try:
                await asyncio.shield(self._cleanup)
            except asyncio.CancelledError as error:
                if self._primary is None:
                    self._primary = error
            # defer cleanup faults to the sanitized result path.
            except Exception:  # noqa: BLE001
                break
        try:
            self._cleanup.result()
        # preserve the primary fault and sanitize secondary cleanup failures.
        except (Exception, asyncio.CancelledError):  # noqa: BLE001
            if self._primary is None:
                raise WorkerLifecycleUnavailable("worker_cleanup_unavailable") from None
        if self._primary is not None:
            raise self._primary
