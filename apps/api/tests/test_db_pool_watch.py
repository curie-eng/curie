"""The main pool watch names who holds the API's database connections (#5995).

Every test checks real connections out of an engine built by the production
``create_engine()`` against the test Postgres. Time is the only thing faked: the
watch reads a clock the tests advance, so no test sleeps.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator, Iterator, Mapping
from contextlib import asynccontextmanager
from typing import Any

import pytest
from curie_api import db_pool_watch as watch_module
from curie_api.config import get_settings
from curie_api.db import create_engine
from curie_api.db_pool_watch import HOLDER_ROUTE, OWN_PACKAGES, PoolWatch
from curie_telemetry import record_metric as declared_record_metric
from curie_test_support.postgres import pg_connect_or_skip
from fastapi.testclient import TestClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

_POOL = {"service.name": "curie-api", "pool": "main"}
# The holder lives in this test file, so the watch also keeps frames from the
# tests directory next to the platform packages it keeps in production.
_WITH_TESTS = (*OWN_PACKAGES, "/apps/api/tests/")


class _Clock:
    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now


class _Records(logging.Handler):
    def __init__(self) -> None:
        super().__init__(level=logging.DEBUG)
        self.records: list[logging.LogRecord] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.records.append(record)

    def warnings(self) -> list[str]:
        return [record.getMessage() for record in self.records if record.levelno == logging.WARNING]


@pytest.fixture
def clock(monkeypatch: pytest.MonkeyPatch) -> _Clock:
    fake = _Clock()
    monkeypatch.setattr(watch_module, "_monotonic", fake)
    return fake


@pytest.fixture
def points(monkeypatch: pytest.MonkeyPatch) -> list[tuple[str, float, dict[str, str]]]:
    """Every point the watch records, after the declared manifest accepted it."""

    recorded: list[tuple[str, float, dict[str, str]]] = []

    def record(name: str, value: float = 1, *, attributes: Mapping[str, str] | None = None) -> None:
        declared_record_metric(name, value, attributes=attributes)
        recorded.append((name, float(value), dict(attributes or {})))

    monkeypatch.setattr(watch_module, "record_metric", record)
    return recorded


@pytest.fixture
def records() -> Iterator[_Records]:
    handler = _Records()
    target = logging.getLogger("curie_api.db_pool_watch")
    previous = target.level
    target.addHandler(handler)
    target.setLevel(logging.DEBUG)
    try:
        yield handler
    finally:
        target.removeHandler(handler)
        target.setLevel(previous)


def _latest(points: list[tuple[str, float, dict[str, str]]], name: str) -> float:
    values = [value for metric, value, attributes in points if metric == name]
    assert values, f"no {name} point"
    assert all(attributes == _POOL for metric, _, attributes in points if metric == name)
    return values[-1]


@asynccontextmanager
async def _main_engine() -> AsyncIterator[AsyncEngine]:
    engine = create_engine()
    try:
        await pg_connect_or_skip(engine)
        yield engine
    finally:
        await engine.dispose()


async def _hold_for_the_test(engine: AsyncEngine) -> AsyncConnection:
    """The holder the warning must name: it checks out and keeps a connection."""

    connection = await engine.connect()
    await connection.execute(text("SELECT 1"))
    return connection


def test_gauges_follow_checkouts_and_checkins(
    clock: _Clock, points: list[tuple[str, float, dict[str, str]]]
) -> None:
    async def go() -> None:
        async with _main_engine() as engine:
            watch = PoolWatch(engine, hold_warning_s=120)
            try:
                watch.observe()
                assert _latest(points, "curie.db.pool.checked_out") == 0
                assert _latest(points, "curie.db.pool.checked_out.age") == 0
                # The production engine keeps SQLAlchemy's defaults: 5 + 10.
                assert _latest(points, "curie.db.pool.capacity") == 15

                first = await _hold_for_the_test(engine)
                clock.now += 30
                second = await _hold_for_the_test(engine)
                clock.now += 5
                watch.observe()
                assert _latest(points, "curie.db.pool.checked_out") == 2
                assert _latest(points, "curie.db.pool.checked_out.age") == 35

                await first.close()
                watch.observe()
                assert _latest(points, "curie.db.pool.checked_out") == 1
                assert _latest(points, "curie.db.pool.checked_out.age") == 5

                await second.close()
                watch.observe()
                assert _latest(points, "curie.db.pool.checked_out") == 0
                assert _latest(points, "curie.db.pool.checked_out.age") == 0
            finally:
                watch.close()

    asyncio.run(go())


def test_a_connection_held_past_the_threshold_is_named_once(
    clock: _Clock,
    points: list[tuple[str, float, dict[str, str]]],
    records: _Records,
) -> None:
    del points

    async def go() -> None:
        async with _main_engine() as engine:
            watch = PoolWatch(engine, hold_warning_s=0.5, packages=_WITH_TESTS)
            try:
                held = await _hold_for_the_test(engine)
                clock.now += 0.6
                for _ in range(3):
                    watch.observe()
                    clock.now += 10
                await held.close()
            finally:
                watch.close()

    asyncio.run(go())
    (line,) = records.warnings()
    assert "state=held" in line
    assert "held_s=0.6" in line
    assert "threshold_s=0.5" in line
    assert "checked_out=1 capacity=15" in line
    assert "task='test_a_connection_held_past_the_threshold_is_named_once.<locals>.go'" in line
    assert "route=None" in line
    # The innermost kept frame is the test's own holder, found across the
    # greenlet boundary the async engine checks out behind.
    innermost = line.split("stack=", 1)[1].split(" <- ")[0]
    assert innermost.startswith("apps/api/tests/test_db_pool_watch.py:")
    assert innermost.endswith(":_hold_for_the_test")


def test_no_warning_below_the_threshold(
    clock: _Clock,
    points: list[tuple[str, float, dict[str, str]]],
    records: _Records,
) -> None:
    async def go() -> None:
        async with _main_engine() as engine:
            watch = PoolWatch(engine, hold_warning_s=120)
            try:
                held = await _hold_for_the_test(engine)
                clock.now += 119
                watch.observe()
                assert _latest(points, "curie.db.pool.checked_out.age") == 119
                await held.close()
                watch.observe()
            finally:
                watch.close()

    asyncio.run(go())
    assert records.warnings() == []


def test_a_long_hold_returned_between_passes_is_named_on_its_way_back(
    clock: _Clock,
    points: list[tuple[str, float, dict[str, str]]],
    records: _Records,
) -> None:
    del points

    async def go() -> None:
        async with _main_engine() as engine:
            watch = PoolWatch(engine, hold_warning_s=120, packages=_WITH_TESTS)
            try:
                held = await _hold_for_the_test(engine)
                clock.now += 125
                await held.close()
                watch.observe()
            finally:
                watch.close()

    asyncio.run(go())
    (line,) = records.warnings()
    assert "state=released" in line
    assert "held_s=125.0" in line
    assert line.split("stack=", 1)[1].split(" <- ")[0].endswith(":_hold_for_the_test")


def test_a_long_hold_detached_between_passes_is_named_on_its_way_out(
    clock: _Clock,
    points: list[tuple[str, float, dict[str, str]]],
    records: _Records,
) -> None:
    async def go() -> None:
        async with _main_engine() as engine:
            watch = PoolWatch(engine, hold_warning_s=120, packages=_WITH_TESTS)
            try:
                held = await _hold_for_the_test(engine)
                clock.now += 125
                # How the async dialect drops a connection that was garbage
                # collected without being returned.
                await held.run_sync(lambda connection: connection.detach())
                watch.observe()
                assert _latest(points, "curie.db.pool.checked_out.age") == 0
                await held.close()
            finally:
                watch.close()

    asyncio.run(go())
    (line,) = records.warnings()
    assert "state=detached" in line
    assert line.split("stack=", 1)[1].split(" <- ")[0].endswith(":_hold_for_the_test")


def test_a_disposed_pool_forgets_the_connections_it_lent(
    clock: _Clock,
    points: list[tuple[str, float, dict[str, str]]],
    records: _Records,
) -> None:
    async def go() -> None:
        async with _main_engine() as engine:
            watch = PoolWatch(engine, hold_warning_s=120)
            try:
                old = await _hold_for_the_test(engine)
                clock.now += 300
                await engine.dispose()
                fresh = await _hold_for_the_test(engine)
                clock.now += 5
                watch.observe()
                # Only the replacement pool's checkout is counted and aged.
                assert _latest(points, "curie.db.pool.checked_out") == 1
                assert _latest(points, "curie.db.pool.checked_out.age") == 5
                await old.close()
                await fresh.close()
            finally:
                watch.close()

    asyncio.run(go())
    assert records.warnings() == []


def test_the_app_names_the_route_holding_a_main_pool_connection(
    _disposable_db: Any,
    monkeypatch: pytest.MonkeyPatch,
    records: _Records,
    auth_headers: dict[str, str],
) -> None:
    # Any checkout counts as long: the route's own checkout is named when it
    # comes back, through the watch the lifespan attached to the main engine.
    monkeypatch.setenv("CURIE_DB_POOL_HOLD_WARNING_S", "1e-9")
    get_settings.cache_clear()
    try:
        from curie_api.main import create_app

        with TestClient(create_app()) as client:
            records.records.clear()
            assert client.get("/agents", headers=auth_headers).status_code == 200
            named = [line for line in records.warnings() if "route='GET /agents'" in line]
            assert named, records.warnings()
            assert "curie_api/" in named[0].split("stack=", 1)[1]
    finally:
        get_settings.cache_clear()


def test_the_app_runs_the_watch_on_its_main_engine(
    _disposable_db: Any,
    monkeypatch: pytest.MonkeyPatch,
    records: _Records,
) -> None:
    monkeypatch.setenv("CURIE_DB_POOL_HOLD_WARNING_S", "1e-9")
    monkeypatch.setenv("CURIE_DB_POOL_WATCH_INTERVAL_S", "0.01")
    get_settings.cache_clear()
    try:
        from curie_api.main import create_app

        with TestClient(create_app()) as client:
            app_state: Any = client.app.state  # type: ignore[attr-defined]

            async def hold_until_named() -> str:
                named = asyncio.Event()

                class _Signal(logging.Handler):
                    def emit(self, record: logging.LogRecord) -> None:
                        if "route='TEST holder'" in record.getMessage():
                            named.set()

                signal = _Signal(level=logging.WARNING)
                watch_logger = logging.getLogger("curie_api.db_pool_watch")
                watch_logger.addHandler(signal)
                try:
                    # The same marker the HTTP middleware sets, so this checkout
                    # is told apart from the app's own background loops.
                    HOLDER_ROUTE.set("TEST holder")
                    async with app_state.engine.connect() as held:
                        await held.execute(text("SELECT 1"))
                        await asyncio.wait_for(named.wait(), 10)
                finally:
                    watch_logger.removeHandler(signal)
                return "named"

            assert client.portal.call(hold_until_named) == "named"
        lines = [line for line in records.warnings() if "route='TEST holder'" in line]
        assert len(lines) == 1
        assert "state=held" in lines[0]
    finally:
        get_settings.cache_clear()
