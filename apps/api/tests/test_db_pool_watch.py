"""The main pool watch names an exhausted pool and decides the probes (#5995).

AC3: "While the main pool is exhausted, a metric shows checked-out = 15 and an
oldest-checkout age, and a log line names the holding code path."

Every case uses the production engine factories, a real Postgres and the
production capacity of 15. Connections are really checked out by real tasks,
and ticks are driven directly rather than waited for.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import socket
import time
from collections.abc import Iterator
from typing import Any

import pytest
from _tcp_blackhole import silent_listener, within
from curie_api import db, db_pool_watch, identity_fence
from curie_api.config import get_settings
from curie_api.db_pool_watch import POOL_EXHAUSTED, POOL_WEDGED, PoolWatch
from curie_internal.valkey import AsyncRedis
from curie_telemetry import build_resource, configure_meter_provider
from curie_telemetry import metrics as telemetry_metrics
from curie_test_support.static_shell import wait_until
from identity_writer_lifetime_support import blocking, run
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricReader
from sqlalchemy import event, make_url, text
from sqlalchemy.ext.asyncio import (
    AsyncConnection,
    AsyncEngine,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.pool import NullPool

CAPACITY = 15
FENCE_LOCK = 2250220
_GAUGES = (
    "curie.db.pool.checked_out",
    "curie.db.pool.oldest_checkout.age",
    "curie.db.pool.starved.age",
)


@pytest.fixture
def watch_env(migrated: None, monkeypatch: pytest.MonkeyPatch) -> Iterator[Any]:
    def apply(**values: object) -> None:
        settings = {
            "DB_POOL_WATCH_INTERVAL_SECONDS": 0.05,
            "DB_POOL_PROBE_TIMEOUT_SECONDS": 0.2,
            "DB_POOL_READY_STARVED_SECONDS": 0.1,
            "DB_POOL_RESTART_STARVED_SECONDS": 0.1,
            **values,
        }
        for name, value in settings.items():
            monkeypatch.setenv(name, str(value))
        get_settings.cache_clear()

    yield apply
    monkeypatch.undo()
    get_settings.cache_clear()


@pytest.fixture
def pool_metrics(monkeypatch: pytest.MonkeyPatch) -> Iterator[InMemoryMetricReader]:
    reader = InMemoryMetricReader()
    provider = MeterProvider(
        metric_readers=[reader],
        resource=build_resource(
            "curie-api",
            service_version="0.7.0",
            service_instance_id="acme-api-pool-watch",
            deployment_environment="test",
        ),
    )
    monkeypatch.setattr(telemetry_metrics, "_provider", None)
    monkeypatch.setattr(telemetry_metrics, "_instruments", {})
    configure_meter_provider(provider)
    try:
        yield reader
    finally:
        provider.shutdown()


@pytest.fixture
def watch_log() -> Iterator[list[str]]:
    lines: list[str] = []

    class Collect(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            if record.levelno == logging.ERROR:
                lines.append(record.getMessage())

    handler = Collect()
    logger = logging.getLogger("curie_api.db_pool_watch")
    logger.addHandler(handler)
    try:
        yield lines
    finally:
        logger.removeHandler(handler)


def gauges(reader: InMemoryMetricReader) -> dict[str, float]:
    values: dict[str, float] = {}
    data = reader.get_metrics_data()
    for resource_metrics in data.resource_metrics if data is not None else ():
        for scope_metrics in resource_metrics.scope_metrics:
            for metric in scope_metrics.metrics:
                if metric.name not in _GAUGES:
                    continue
                for point in metric.data.data_points:
                    assert dict(point.attributes) == {"service.name": "curie-api", "pool": "main"}
                    values[metric.name] = point.value
    return values


async def hold_connection_in_test(
    engine: AsyncEngine, parked: asyncio.Event, release: asyncio.Event
) -> None:
    """A holder outside the API package; the log line must still name it."""
    async with engine.connect() as connection:
        await connection.execute(text("SELECT 1"))
        parked.set()
        await release.wait()


async def read_identity_behind_writer(
    factory: async_sessionmaker[Any], backend: asyncio.Future[int], release: asyncio.Event
) -> None:
    """A holder parked inside a real API function, waiting on the fence lock."""
    async with factory() as session:
        backend.set_result(await session.scalar(text("SELECT pg_backend_pid()")))
        await identity_fence.lock_identity_reader(session)
        await release.wait()


async def mutate_identity_behind_writer(
    factory: async_sessionmaker[Any], client: Any, release: asyncio.Event
) -> None:
    """A real fence mutation, parked on the advisory lock the writer holds."""
    async with identity_fence.mutation(factory, client):
        await release.wait()


class Holders:
    """Real tasks holding real main pool connections, each waited until parked.

    The pool's own checked out count includes a connection still being opened,
    so it cannot tell when a holder is really parked; each holder says so.
    """

    def __init__(self, engine: AsyncEngine) -> None:
        self.engine = engine
        self.tasks: list[tuple[asyncio.Task[None], asyncio.Event, asyncio.Event]] = []

    def hold(self) -> None:
        parked, release = asyncio.Event(), asyncio.Event()
        task = asyncio.create_task(hold_connection_in_test(self.engine, parked, release))
        self.tasks.append((task, parked, release))

    async def hold_behind_writer(self, writer: AsyncConnection) -> None:
        """Park one reader on the fence lock ``writer`` holds, seen in Postgres."""
        backend: asyncio.Future[int] = asyncio.get_running_loop().create_future()
        parked, release = asyncio.Event(), asyncio.Event()
        task = asyncio.create_task(
            read_identity_behind_writer(async_sessionmaker(self.engine), backend, release)
        )
        self.tasks.append((task, parked, release))
        holder_pid = await writer.scalar(text("SELECT pg_backend_pid()"))
        async with within(10, "the reader waiting on the fence lock"):
            await blocking(writer, await backend, holder_pid)  # type: ignore[arg-type]
        parked.set()

    async def hold_fence_mutation(self, writer: AsyncConnection, client: Any) -> None:
        """Park one real fence mutation on the lock ``writer`` holds, seen in Postgres."""
        pool = self.engine.sync_engine.pool
        backend: asyncio.Future[int] = asyncio.get_running_loop().create_future()
        parked, release = asyncio.Event(), asyncio.Event()
        task = asyncio.create_task(
            mutate_identity_behind_writer(async_sessionmaker(self.engine), client, release)
        )
        self.tasks.append((task, parked, release))

        def checked_out(dbapi_connection: Any, record: Any, proxy: Any) -> None:
            # The fence's own connection; its server pid is known without a query.
            if asyncio.current_task() is task and not backend.done():
                backend.set_result(dbapi_connection.driver_connection.get_server_pid())

        event.listen(pool, "checkout", checked_out)
        try:
            holder_pid = await writer.scalar(text("SELECT pg_backend_pid()"))
            async with within(10, "the fence mutation waiting on its lock"):
                await blocking(writer, await backend, holder_pid)  # type: ignore[arg-type]
        finally:
            event.remove(pool, "checkout", checked_out)
        parked.set()

    async def fill(self) -> None:
        """Wait until every started holder is parked, and the pool is full."""
        async with within(10, "filling the pool"):
            for _, parked, _ in self.tasks:
                await parked.wait()
        assert self.engine.pool.checkedout() == CAPACITY  # type: ignore[attr-defined]

    async def release_one(self) -> None:
        task, _, release = self.tasks.pop(0)
        release.set()
        await task

    async def release_all(self) -> None:
        for _, _, release in self.tasks:
            release.set()
        for task, _, _ in self.tasks:
            with contextlib.suppress(Exception):
                await task
        self.tasks.clear()


def build_watch(engine: AsyncEngine, liveness: async_sessionmaker[Any]) -> PoolWatch:
    return PoolWatch(engine, liveness, CAPACITY, get_settings())


def test_exhausted_pool_reports_holders_metrics_and_probes(
    watch_env: Any, pool_metrics: InMemoryMetricReader, watch_log: list[str]
) -> None:
    # No lock timeout and a long fence deadline keep the fence holders parked.
    watch_env(DB_LOCK_TIMEOUT_SECONDS=0, IDENTITY_FENCE_ACQUIRE_TIMEOUT_SECONDS=60)

    async def check() -> None:
        url = get_settings().database_url
        engine = db.create_engine()
        liveness_engine = db.create_liveness_engine()
        writer = create_async_engine(url, poolclass=NullPool)
        watch = build_watch(engine, async_sessionmaker(liveness_engine))
        holders = Holders(engine)
        settings = get_settings()
        client = AsyncRedis.from_url(settings.valkey_dsn(), **settings.valkey_client_options())
        try:
            async with writer.connect() as stuck:
                await stuck.execute(text("SELECT pg_advisory_lock(:key)"), {"key": FENCE_LOCK})
                for _ in range(CAPACITY - 2):
                    holders.hold()
                await holders.hold_behind_writer(stuck)
                await holders.hold_fence_mutation(stuck, client)
                await holders.fill()
                await watch.tick()
                await watch.tick()
                values = gauges(pool_metrics)
                assert values["curie.db.pool.checked_out"] == 15
                assert values["curie.db.pool.oldest_checkout.age"] > 0
                assert values["curie.db.pool.starved.age"] > 0
                assert watch.starved_for() >= 0.1
                assert watch.database_reachable is True
                assert watch.readiness_failure() == POOL_EXHAUSTED
                assert watch.liveness_failure() == POOL_WEDGED
                assert len(watch_log) == 1
                line = watch_log[0]
                assert line.startswith(
                    "database pool exhausted pool=main checked_out=15 capacity=15 "
                )
                assert "curie_api/identity_fence.py:" in line
                assert ":lock_identity_reader|awaiting=" in line
                # Named inside the @asynccontextmanager body, not at its __aenter__.
                assert ":mutation|awaiting=" in line
                assert "contextlib.py" not in line
                assert "test_db_pool_watch.py:" in line
                assert ":hold_connection_in_test|awaiting=" in line
                assert "SELECT" not in line
                await stuck.execute(text("SELECT pg_advisory_unlock(:key)"), {"key": FENCE_LOCK})
            await holders.release_all()
            # A holder that invalidates its connection leaves by another route.
            async with engine.connect() as extra:
                await extra.invalidate()
            await watch.tick()
            assert engine.pool.checkedout() == 0  # type: ignore[attr-defined]
            assert watch.oldest_checkout_age() == 0
            assert watch.starved_for() == 0
            assert watch.readiness_failure() is None
            assert watch.liveness_failure() is None
            assert gauges(pool_metrics) == {name: 0 for name in _GAUGES}
            assert len(watch_log) == 1
        finally:
            await holders.release_all()
            await client.delete(identity_fence.FENCE_KEY)
            await client.aclose()
            await watch.stop()
            await engine.dispose()
            await liveness_engine.dispose()
            await writer.dispose()

    run(check)


def test_busy_pool_that_keeps_turning_over_stays_ready(
    watch_env: Any, watch_log: list[str]
) -> None:
    watch_env(DB_POOL_READY_STARVED_SECONDS=0.01, DB_POOL_RESTART_STARVED_SECONDS=0.01)

    async def check() -> None:
        engine = db.create_engine()
        liveness_engine = db.create_liveness_engine()
        watch = build_watch(engine, async_sessionmaker(liveness_engine))
        holders = Holders(engine)
        try:
            for _ in range(CAPACITY):
                holders.hold()
            await holders.fill()
            for _ in range(10):
                await watch.tick()
                assert engine.pool.checkedout() == CAPACITY  # type: ignore[attr-defined]
                assert watch.readiness_failure() is None
                assert watch.liveness_failure() is None
                await holders.release_one()
                # The pool leaves capacity and returns within the log window.
                await watch.tick()
                assert engine.pool.checkedout() == CAPACITY - 1  # type: ignore[attr-defined]
                holders.hold()
                await holders.fill()
            # Callers have waited longer than the threshold; turnover is what counts.
            assert watch.oldest_checkout_age() > 0.01
            # One exhaustion line per window, however often the pool returns to capacity.
            assert len(watch_log) == 1
        finally:
            await holders.release_all()
            await watch.stop()
            await engine.dispose()
            await liveness_engine.dispose()

    run(check)


def test_starvation_is_timed_from_the_last_sample_below_capacity(
    watch_env: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A pool idle for ten minutes that then fills is not starved for ten minutes.

    Every slot fills with a connection still being opened (the server never
    answers the startup), so the pool reaches capacity without a single
    handout. The watch's clock is moved forward ten minutes after it was
    built, as if the pool had sat idle that long.
    """
    real_url = get_settings().database_url

    async def check() -> None:
        async with silent_listener() as listener:
            silent = make_url(real_url).set(host="127.0.0.1", port=listener.port)
            watch_env(
                DATABASE_URL=silent.render_as_string(hide_password=False),
                DB_CONNECT_TIMEOUT_SECONDS=30,
                DB_POOL_READY_STARVED_SECONDS=300,
                DB_POOL_RESTART_STARVED_SECONDS=300,
            )
            engine = db.create_engine()
            reachable = create_async_engine(real_url, poolclass=NullPool)
            watch = build_watch(engine, async_sessionmaker(reachable))
            real_clock = time.monotonic
            monkeypatch.setattr(db_pool_watch, "monotonic", lambda: real_clock() + 600)
            connecting: list[asyncio.Task[Any]] = []
            try:
                await watch.tick()
                assert watch.starved_for() == 0
                pool = engine.sync_engine.pool
                connecting = [
                    asyncio.create_task(engine.connect().start()) for _ in range(CAPACITY)
                ]
                await asyncio.to_thread(
                    wait_until,
                    lambda: pool.checkedout() == CAPACITY,  # type: ignore[attr-defined]
                    10,
                    "every slot to be opening a connection",
                )
                await watch.tick()
                assert watch.database_reachable is True
                assert 0 < watch.starved_for() < 60
                assert watch.readiness_failure() is None
                assert watch.liveness_failure() is None
            finally:
                for task in connecting:
                    task.cancel()
                for task in connecting:
                    with contextlib.suppress(asyncio.CancelledError, Exception):
                        await task
                await watch.stop()
                await engine.dispose()
                await reachable.dispose()

    run(check)


def test_unreachable_database_never_fails_liveness(watch_env: Any) -> None:
    watch_env()
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        dead_port = probe.getsockname()[1]

    async def check() -> None:
        engine = db.create_engine()
        dead = make_url(get_settings().database_url).set(host="127.0.0.1", port=dead_port)
        unreachable = create_async_engine(dead, poolclass=NullPool)
        watch = build_watch(engine, async_sessionmaker(unreachable))
        holders = Holders(engine)
        try:
            for _ in range(CAPACITY):
                holders.hold()
            await holders.fill()
            await watch.tick()
            await watch.tick()
            assert watch.readiness_failure() == POOL_EXHAUSTED
            assert watch.database_reachable is False
            assert watch.liveness_failure() is None
        finally:
            await holders.release_all()
            await watch.stop()
            await engine.dispose()
            await unreachable.dispose()

    run(check)


def test_disabled_watch_reports_healthy(watch_env: Any) -> None:
    watch_env(DB_POOL_WATCH_INTERVAL_SECONDS=0)

    async def check() -> None:
        engine = db.create_engine()
        liveness_engine = db.create_liveness_engine()
        watch = build_watch(engine, async_sessionmaker(liveness_engine))
        holders = Holders(engine)
        try:
            for _ in range(CAPACITY):
                holders.hold()
            await holders.fill()
            await watch.tick()
            await watch.tick()
            assert watch.starved_for() > 0
            assert watch.readiness_failure() is None
            assert watch.liveness_failure() is None
        finally:
            await holders.release_all()
            await watch.stop()
            await engine.dispose()
            await liveness_engine.dispose()

    run(check)


def test_stale_watch_reports_healthy(watch_env: Any) -> None:
    watch_env()

    async def check() -> None:
        engine = db.create_engine()
        liveness_engine = db.create_liveness_engine()
        watch = build_watch(engine, async_sessionmaker(liveness_engine))
        holders = Holders(engine)
        try:
            for _ in range(CAPACITY):
                holders.hold()
            await holders.fill()
            await watch.tick()
            await watch.tick()
            assert watch.readiness_failure() == POOL_EXHAUSTED
            # No further tick: once the last one is too old the watch stands down.
            await asyncio.to_thread(
                wait_until,
                lambda: watch.readiness_failure() is None and watch.liveness_failure() is None,
                6,
                "a stale watch to report healthy",
            )
            assert watch.starved_for() > 0
        finally:
            await holders.release_all()
            await watch.stop()
            await engine.dispose()
            await liveness_engine.dispose()

    run(check)
