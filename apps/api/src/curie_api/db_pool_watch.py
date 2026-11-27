"""Who holds the API's main database pool, and for how long (#5995 decision 4).

When every connection of the main pool is checked out and none comes back,
each route and background loop that needs one fails with SQLAlchemy's pool
``TimeoutError`` after 30 seconds, while ``/ready`` (on its own liveness pool)
stays green. Without this module nothing says which code holds the connections.

``PoolWatch`` attaches to an engine after it is built, through SQLAlchemy pool
events only, so it never changes how the engine is created:

- ``checkout`` records when a connection left the pool, the request route or
  asyncio task that took it, the trace id in scope, and a few ``curie_api`` and
  ``curie_internal`` frames of the caller's stack.
- ``checkin`` and ``detach`` forget it. A connection held past the threshold
  that comes back (or is detached) before any pass named it is logged once on
  its way out of the watch. ``engine_disposed`` forgets every checkout, since
  the pool that lent them has been replaced.
- ``observe`` (one pass of ``run_forever``) records three gauges
  (``curie.db.pool.checked_out``, ``curie.db.pool.capacity`` and
  ``curie.db.pool.checked_out.age``) and logs one WARNING for each checkout
  that has just crossed the threshold.

Each checkout is named at most once, so a connection that never returns
produces one line, not one per pass. The log line goes to the service's
structured stderr log; the OTLP copy keeps only the message template, as for
every API log line (``curie_telemetry.logging``).

SQLAlchemy has no pool event for a checkout that timed out waiting, so this
module does not count timeouts. ``checked_out`` equal to ``capacity`` is the
exhaustion signal.
"""

from __future__ import annotations

import asyncio
import logging
import sys
import time
from collections.abc import Callable
from contextvars import ContextVar
from dataclasses import dataclass
from types import FrameType
from typing import Any

import greenlet  # type: ignore[import-untyped]
from curie_telemetry import record_metric
from opentelemetry import trace
from sqlalchemy import event
from sqlalchemy.ext.asyncio import AsyncEngine
from sqlalchemy.pool import QueuePool

logger = logging.getLogger(__name__)

# Patched by tests that need a deterministic clock; production reads the
# monotonic clock.
_monotonic: Callable[[], float] = time.monotonic

# The request route that is running, as "METHOD /route/template". The HTTP
# telemetry middleware sets it for the life of each request, so a checkout made
# on behalf of a request can name it. Tasks the request starts inherit it.
HOLDER_ROUTE: ContextVar[str | None] = ContextVar("curie_api_db_pool_holder_route", default=None)

# Frames are kept only from these packages: the platform code that asked for a
# connection, not SQLAlchemy or asyncio around it.
OWN_PACKAGES = ("/curie_api/", "/curie_internal/")
_MAX_FRAMES = 6
# The per file name cache is cleared when it reaches this size, so code
# compiled at run time under fresh file names cannot grow it without bound.
_MAX_KEPT_FILES = 4096
_THIS_FILE = __file__
_ATTRIBUTES = {"service.name": "curie-api", "pool": "main"}

_Frame = tuple[str, int, str]


@dataclass(slots=True)
class _Checkout:
    """One connection that is out of the pool right now."""

    started: float
    route: str | None
    task: str | None
    trace_id: str | None
    frames: tuple[_Frame, ...]
    named: bool = False


@dataclass(frozen=True, slots=True)
class PoolSnapshot:
    """What one ``observe`` pass recorded."""

    checked_out: int
    capacity: int | None
    oldest_age_s: float
    named: int


def _caller_frames(packages: tuple[str, ...], kept: dict[str, bool]) -> tuple[_Frame, ...]:
    """Return up to six platform frames of the code that took the connection.

    The async engine runs the pool inside a greenlet, whose own frame chain
    stops at the greenlet boundary. The caller's coroutine frames hang off the
    parent greenlet's current frame, so the walk continues there once.

    ``kept`` caches the package decision per file name, so a frame costs one
    dict lookup rather than a substring search per package.
    """

    frames: list[_Frame] = []
    frame: FrameType | None = sys._getframe(1)
    parent = greenlet.getcurrent().parent
    while len(frames) < _MAX_FRAMES:
        if frame is None:
            if parent is None:
                break
            frame, parent = parent.gr_frame, None
            continue
        code = frame.f_code
        filename = code.co_filename
        keep = kept.get(filename)
        if keep is None:
            keep = filename != _THIS_FILE and any(package in filename for package in packages)
            if len(kept) >= _MAX_KEPT_FILES:
                kept.clear()
            kept[filename] = keep
        if keep:
            frames.append((filename, frame.f_lineno, code.co_name))
        frame = frame.f_back
    return tuple(frames)


def _task_name() -> str | None:
    """Name the asyncio task by its coroutine, such as ``Loop.run_forever``."""

    try:
        task = asyncio.current_task()
    except RuntimeError:
        return None
    if task is None:
        return None
    coroutine = task.get_coro()
    name = getattr(coroutine, "__qualname__", None)
    return str(name) if name else task.get_name()


def _trace_id() -> str | None:
    context = trace.get_current_span().get_span_context()
    return f"{context.trace_id:032x}" if context.is_valid else None


def _render_stack(frames: tuple[_Frame, ...], packages: tuple[str, ...]) -> str:
    """Render frames innermost first, each path from its package directory."""

    rendered = []
    for filename, lineno, function in frames:
        start = max(filename.rfind(package) for package in packages)
        rendered.append(f"{filename[start + 1 :]}:{lineno}:{function}")
    return " <- ".join(rendered) or "unknown"


class PoolWatch:
    """Track the checkouts of one engine's pool and report long holders."""

    def __init__(
        self,
        engine: AsyncEngine,
        *,
        hold_warning_s: float,
        packages: tuple[str, ...] = OWN_PACKAGES,
    ) -> None:
        if not hold_warning_s > 0:
            raise ValueError("hold_warning_s must be positive")
        self._engine = engine.sync_engine
        self._hold_warning_s = hold_warning_s
        self._packages = packages
        # File name -> whether its frames are kept (see _MAX_KEPT_FILES).
        self._kept: dict[str, bool] = {}
        # Keyed by SQLAlchemy's connection record, which stays the same object
        # from checkout to checkin or detach.
        self._held: dict[Any, _Checkout] = {}
        for name, listener in self._listeners():
            event.listen(self._engine, name, listener)

    def _listeners(self) -> tuple[tuple[str, Callable[..., None]], ...]:
        return (
            ("checkout", self._on_checkout),
            ("checkin", self._on_checkin),
            ("detach", self._on_detach),
            ("engine_disposed", self._on_disposed),
        )

    def close(self) -> None:
        """Remove the listeners. Idempotent."""

        for name, listener in self._listeners():
            if event.contains(self._engine, name, listener):
                event.remove(self._engine, name, listener)

    def _on_checkout(self, _dbapi_connection: Any, record: Any, _proxy: Any) -> None:
        # A listener that raises would fail the checkout itself, so a defect
        # here must cost the diagnosis, never the request.
        try:
            self._held[record] = _Checkout(
                started=_monotonic(),
                route=HOLDER_ROUTE.get(),
                task=_task_name(),
                trace_id=_trace_id(),
                frames=_caller_frames(self._packages, self._kept),
            )
        except Exception:  # noqa: BLE001 - observability must not fail a checkout
            logger.debug("db pool watch could not record a checkout", exc_info=True)

    def _on_checkin(self, _dbapi_connection: Any, record: Any) -> None:
        self._forget(record, "released")

    def _on_detach(self, _dbapi_connection: Any, record: Any) -> None:
        # The async dialect detaches a connection that was garbage collected
        # without being returned, so this is also how a leaked one leaves.
        self._forget(record, "detached")

    def _on_disposed(self, _engine: Any) -> None:
        # dispose() replaces the pool. Connections still out belong to the old
        # one, block nothing in the new one, and are not counted by it.
        self._held.clear()

    def _forget(self, record: Any, state: str) -> None:
        """Stop tracking a checkout, naming it first if it outlived the threshold."""

        try:
            checkout = self._held.pop(record, None)
            if checkout is not None and not checkout.named:
                held = _monotonic() - checkout.started
                if held >= self._hold_warning_s:
                    self._warn(state, checkout, held)
        except Exception:  # noqa: BLE001 - observability must not fail a checkin
            logger.debug("db pool watch could not record a %s", state, exc_info=True)

    def _warn(self, state: str, checkout: _Checkout, held: float) -> None:
        checkout.named = True
        pool = self._engine.pool
        checked_out = pool.checkedout() if isinstance(pool, QueuePool) else len(self._held)
        logger.warning(
            "db pool checkout held past threshold state=%s pool=main held_s=%.1f "
            "threshold_s=%g checked_out=%d capacity=%s route=%r task=%r trace_id=%s stack=%s",
            state,
            held,
            self._hold_warning_s,
            checked_out,
            _capacity(pool),
            checkout.route,
            checkout.task,
            checkout.trace_id,
            _render_stack(checkout.frames, self._packages),
        )

    def observe(self) -> PoolSnapshot:
        """Record the pool gauges and name each checkout newly past the threshold."""

        now = _monotonic()
        pool = self._engine.pool
        # A dict copy is one C call, so a checkin that runs from a garbage
        # collection callback cannot change the dict under this loop.
        held = self._held.copy()
        checked_out = pool.checkedout() if isinstance(pool, QueuePool) else len(held)
        capacity = _capacity(pool)
        oldest = max((now - checkout.started for checkout in held.values()), default=0.0)
        record_metric("curie.db.pool.checked_out", checked_out, attributes=_ATTRIBUTES)
        if capacity is not None:
            record_metric("curie.db.pool.capacity", capacity, attributes=_ATTRIBUTES)
        record_metric("curie.db.pool.checked_out.age", max(0.0, oldest), attributes=_ATTRIBUTES)
        named = 0
        for checkout in held.values():
            age = now - checkout.started
            if not checkout.named and age >= self._hold_warning_s:
                self._warn("held", checkout, age)
                named += 1
        return PoolSnapshot(checked_out, capacity, max(0.0, oldest), named)

    async def run_forever(self, interval_s: float) -> None:
        """Observe now and then every ``interval_s`` seconds until cancelled."""

        while True:
            try:
                self.observe()
            except Exception:  # noqa: BLE001 - one bad pass must not end the watch
                logger.exception("db pool watch pass failed")
            await asyncio.sleep(interval_s)


def _capacity(pool: Any) -> int | None:
    """Pool size plus maximum overflow: the checkouts served without waiting.

    ``QueuePool`` exposes its size but not its overflow limit, so this reads
    the ``_max_overflow`` attribute it keeps. A negative value means the
    overflow is unlimited, which has no capacity to report.
    """

    if not isinstance(pool, QueuePool):
        return None
    max_overflow = int(getattr(pool, "_max_overflow", 0))
    if max_overflow < 0:
        return None
    return int(pool.size()) + max_overflow
