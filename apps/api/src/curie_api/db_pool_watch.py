"""Watch the main database pool for a wedge nothing else releases (#5995).

The pool becomes starved when a sample finds it at capacity and nobody,
including a short probe checkout, has been handed a connection since the
previous sample. Once a sample has seen the pool at capacity, only a handout
counts as recovery, even when a later sample reads below capacity: a slot
freed by a discarded connection whose replacement cannot be opened serves
nobody. A busy pool that keeps handing out
connections is never unhealthy, however long its callers wait: the criterion
is that the pool stopped turning over, not that a caller waited long.

Readiness fails once that has lasted db_pool_ready_starved_seconds. Its 30 s
default equals the default pool timeout, so a stall shorter than one pool
timeout never fails readiness; a request that arrived during starvation may
still be waiting out its own pool timeout. The chart's readiness probe then
needs 24 consecutive failures at 5 s, so one replica does not leave the
Service on a short stall.

Kubernetes restarts a container on a failing liveness probe, not a failing
readiness probe, so the restart backstop is /health. It fails only after
db_pool_restart_starved_seconds (120 s) of starvation while the database still
answers on the separate liveness pool: an unreachable database never triggers
a restart, which could not help and would leave an API that refuses to boot
without its database. The chart's liveness probe needs 12 consecutive failures
at 15 s, so a wedged pod restarts about five minutes after the wedge begins.
The client and server connection bounds (db.py) release a stuck connection:
a lock wait at the lock timeout, a vanished peer through keepalive in about
60 s, and a reply that never arrives within about 150 s, twice the command
timeout. Any handout clears the starved state and the kubelet's failure
count, so a wedge those bounds can cure never reaches a restart. The restart
exists for holders nothing else releases, such as a quarantined identity fence
lease, which keeps its connection until the process exits by design.

Every watch result the endpoints read is in memory. A watch that is disabled
or has stopped ticking reports healthy: a broken monitor never fails a probe.
"""

from __future__ import annotations

import asyncio
import logging
import sysconfig
from pathlib import Path
from time import monotonic
from typing import Any, cast

from curie_telemetry import record_metric
from sqlalchemy import event, text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker
from sqlalchemy.pool import QueuePool

from .config import Settings

_LOG = logging.getLogger(__name__)

POOL_EXHAUSTED = "Database pool is exhausted"
POOL_WEDGED = "Database pool is wedged"

# The discriminator's own bound on asking the liveness pool for SELECT 1.
_DATABASE_CHECK_TIMEOUT_S = 2.0
# How often the exhaustion line repeats while the pool stays at capacity.
_LOG_EVERY_S = 30.0
# Bound on one holder's await chain walk.
_MAX_FRAMES = 64
_ATTRIBUTES = {"service.name": "curie-api", "pool": "main"}
_PACKAGE = Path(__file__).resolve().parent
_LIBRARY_ROOTS = tuple(
    sorted(
        {
            str(Path(sysconfig.get_paths()[name]).resolve())
            for name in ("stdlib", "platstdlib", "purelib", "platlib")
        }
    )
)


def _next_awaited(awaited: Any, frame: Any) -> Any:
    """What a suspended coroutine or async generator frame is waiting on.

    An ``@asynccontextmanager`` entering or leaving its body awaits
    ``anext(self.gen)``, an object with no frame of its own; the generator it
    drives is reachable only through the context manager, so the walk hops to
    it there rather than stopping one level too high.
    """
    nested = getattr(awaited, "cr_await", None)
    if nested is None:
        nested = getattr(awaited, "ag_await", None)
    if (
        frame.f_code.co_name in {"__aenter__", "__aexit__"}
        and Path(frame.f_code.co_filename).name == "contextlib.py"
        and not hasattr(nested, "cr_frame")
        and not hasattr(nested, "ag_frame")
    ):
        generator = getattr(frame.f_locals.get("self"), "gen", None)
        if hasattr(generator, "ag_frame"):
            return generator
    return nested


def _frames(task: asyncio.Task[Any] | None) -> list[tuple[str, int, str]] | None:
    """The suspended task's coroutine frames, outermost first; None if unwalkable.

    Follows coroutines and async generators, at most _MAX_FRAMES deep; any
    other shape ends the walk with what was collected.
    """
    if task is None or task.done():
        return None
    frames: list[tuple[str, int, str]] = []
    try:
        awaited: Any = task.get_coro()
        while awaited is not None and len(frames) < _MAX_FRAMES:
            frame = getattr(awaited, "cr_frame", None) or getattr(awaited, "ag_frame", None)
            if frame is None:
                break
            frames.append((frame.f_code.co_filename, frame.f_lineno, frame.f_code.co_name))
            awaited = _next_awaited(awaited, frame)
    except Exception:  # noqa: BLE001 - a diagnostic walk never fails the tick
        pass
    return frames or None


def _in_package(filename: str) -> str | None:
    try:
        relative = Path(filename).resolve().relative_to(_PACKAGE)
    except ValueError:
        return None
    return f"{_PACKAGE.name}/{relative.as_posix()}"


def _library(filename: str) -> bool:
    resolved = str(Path(filename).resolve())
    return resolved.startswith(_LIBRARY_ROOTS) or not Path(filename).is_absolute()


def describe_holder(task: asyncio.Task[Any] | None) -> str:
    """Where a checked out connection's task is: its own code, then the await.

    The site is the innermost frame in this package, else the innermost frame
    outside the standard library and installed libraries (by file name only).
    """
    frames = _frames(task)
    if frames is None:
        return "unknown"
    site = "unknown"
    for filename, line, function in reversed(frames):
        relative = _in_package(filename)
        if relative is not None:
            site = f"{relative}:{line}:{function}"
            break
    else:
        for filename, line, function in reversed(frames):
            if not _library(filename):
                site = f"{Path(filename).name}:{line}:{function}"
                break
    filename, line, function = frames[-1]
    return f"{site}|awaiting={Path(filename).name}:{line}:{function}"


class PoolWatch:
    """Sample the main pool, decide starvation, report it to the probes."""

    def __init__(
        self,
        engine: AsyncEngine,
        liveness: async_sessionmaker[AsyncSession],
        capacity: int,
        settings: Settings,
    ) -> None:
        self._engine = engine
        self._pool = cast(QueuePool, engine.sync_engine.pool)
        self._liveness = liveness
        self._capacity = capacity
        self._interval = settings.db_pool_watch_interval_seconds
        self._probe_timeout = settings.db_pool_probe_timeout_seconds
        self._ready_after = settings.db_pool_ready_starved_seconds
        self._restart_after = settings.db_pool_restart_starved_seconds
        # Keyed by connection record: checkout time and the checking out task.
        self._checkouts: dict[Any, tuple[float, asyncio.Task[Any] | None]] = {}
        self._handouts = 0
        self._handouts_seen = 0
        self._last_handout = monotonic()
        # The last sample below capacity. Starvation is measured from the later
        # of this and the last handout, so a pool that sat idle and then filled
        # without a single checkout (every slot still connecting) is not
        # reported as starved since the idle period began.
        self._last_below_capacity = self._last_handout
        self._starved = False
        # True while the previous sample saw the pool at capacity or starved:
        # until a handout, a lower count is not taken as recovery.
        self._watching = False
        self._database_reachable = False
        self._last_tick: float | None = None
        self._logged_at: float | None = None
        self._task: asyncio.Task[None] | None = None
        self._listeners: list[tuple[str, Any]] = [
            ("checkout", self._checked_out),
            ("checkin", self._released),
            ("detach", self._released),
        ]
        for name, callback in self._listeners:
            event.listen(self._pool, name, callback)

    def _checked_out(self, dbapi_connection: Any, record: Any, proxy: Any) -> None:
        # Runs inside SQLAlchemy's greenlet, where current_task() is still the
        # task checking the connection out. O(1): no stack walking here.
        now = monotonic()
        self._checkouts[record] = (now, asyncio.current_task())
        self._handouts += 1
        self._last_handout = now

    def _released(self, dbapi_connection: Any, record: Any) -> None:
        self._checkouts.pop(record, None)

    def start(self) -> None:
        if self._interval > 0 and self._task is None:
            self._task = asyncio.create_task(self.run_forever())

    def close(self) -> None:
        """Remove the pool listeners."""
        for name, callback in self._listeners:
            if event.contains(self._pool, name, callback):
                event.remove(self._pool, name, callback)

    async def stop(self) -> None:
        task, self._task = self._task, None
        if task is not None:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        self.close()

    async def run_forever(self) -> None:
        while True:
            try:
                await self.tick()
            except Exception as error:  # noqa: BLE001 - the watch must keep sampling
                _LOG.error("database pool watch tick failed error=%s", type(error).__name__)
            await asyncio.sleep(self._interval)

    async def tick(self) -> None:
        checked_out = self._pool.checkedout()
        at_capacity = checked_out >= self._capacity
        if at_capacity or self._watching:
            # Once a sample has seen the pool at capacity, only a handout is
            # recovery: a slot freed by a discarded connection whose
            # replacement cannot be opened reads below capacity yet serves
            # nobody, so a dip alone proves nothing and the probe decides.
            if self._handouts == self._handouts_seen:
                await self._probe()
            self._starved = self._handouts == self._handouts_seen
            if self._starved:
                self._database_reachable = await self._database_answers()
            now = monotonic()
            if (at_capacity or self._starved) and (
                self._logged_at is None or now - self._logged_at >= _LOG_EVERY_S
            ):
                self._logged_at = now
                self._log_exhaustion(checked_out)
            self._watching = at_capacity or self._starved
        else:
            # A pool below capacity that is not being watched can hand out a
            # connection; no database call is made, so the watch adds no load.
            # The log limiter is not reset here, so a pool oscillating at its
            # capacity still logs at most once per window.
            self._starved = False
            self._last_below_capacity = monotonic()
        self._handouts_seen = self._handouts
        record_metric("curie.db.pool.checked_out", checked_out, attributes=_ATTRIBUTES)
        record_metric(
            "curie.db.pool.oldest_checkout.age", self.oldest_checkout_age(), attributes=_ATTRIBUTES
        )
        record_metric("curie.db.pool.starved.age", self.starved_for(), attributes=_ATTRIBUTES)
        self._last_tick = monotonic()

    async def _probe(self) -> None:
        """Try one checkout; a success is recorded as a handout by the listener."""
        connection = self._engine.connect()
        try:
            async with asyncio.timeout(self._probe_timeout):
                await connection.start()
        except Exception:  # noqa: BLE001 - any failure means nothing was handed out
            return
        await connection.close()

    async def _database_answers(self) -> bool:
        try:
            async with asyncio.timeout(_DATABASE_CHECK_TIMEOUT_S):
                async with self._liveness() as session:
                    await session.execute(text("SELECT 1"))
        except Exception:  # noqa: BLE001 - unreachable is the answer
            return False
        return True

    def oldest_checkout_age(self) -> float:
        starts = [started for started, _ in list(self._checkouts.values())]
        return max(0.0, monotonic() - min(starts)) if starts else 0.0

    def starved_for(self) -> float:
        """Seconds starved: since the later of the last handout and the last
        sample below capacity, else 0 when not starved."""
        if not self._starved:
            return 0.0
        return max(0.0, monotonic() - max(self._last_handout, self._last_below_capacity))

    @property
    def database_reachable(self) -> bool:
        return self._database_reachable

    def _fresh(self) -> bool:
        if self._interval <= 0 or self._last_tick is None:
            return False
        stale_after = 3 * self._interval + self._probe_timeout + _DATABASE_CHECK_TIMEOUT_S
        return monotonic() - self._last_tick <= stale_after

    def readiness_failure(self) -> str | None:
        if self._fresh() and self.starved_for() >= self._ready_after:
            return POOL_EXHAUSTED
        return None

    def liveness_failure(self) -> str | None:
        if (
            self._restart_after > 0
            and self._fresh()
            and self._database_reachable
            and self.starved_for() >= self._restart_after
        ):
            return POOL_WEDGED
        return None

    def _log_exhaustion(self, checked_out: int) -> None:
        now = monotonic()
        held = sorted(list(self._checkouts.values()), key=lambda item: item[0])
        groups: dict[str, list[float]] = {}
        for started, task in held[: self._capacity]:
            groups.setdefault(describe_holder(task), []).append(now - started)
        holders = ",".join(
            f"{len(ages)}x{int(ages[0])}s@{site}" if len(ages) > 1 else f"{int(ages[0])}s@{site}"
            for site, ages in groups.items()
        )
        # Pre-rendered from a closed vocabulary (counts, ages and code
        # locations), so the holder list survives the OTLP body, which keeps
        # only a format string's template.
        _LOG.error(
            f"database pool exhausted pool=main checked_out={checked_out} "
            f"capacity={self._capacity} "
            f"oldest_checkout_age_s={int(self.oldest_checkout_age())} "
            f"starved_for_s={int(self.starved_for())} holders=[{holders}]"
        )
