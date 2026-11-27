"""How long the API can hold one database connection (#5995).

Every test drives the production engine builders (`db.create_engine`,
`db.create_liveness_engine`) against the real test Postgres. The dead-network
cases put a TCP relay between the client and the real server and stop
forwarding on the open flows, which is what a broken flow looks like to both
ends: no FIN, no RST, no reply.
"""

from __future__ import annotations

import asyncio
import time
import uuid
from collections.abc import AsyncIterator
from typing import Any

import pytest
from _connection_bounds_support import OUTER_DEADLINE_S
from curie_api import db
from curie_api.config import get_settings
from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncEngine

pytestmark = [pytest.mark.anyio, pytest.mark.usefixtures("migrated")]


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


@pytest.fixture
async def engines() -> AsyncIterator[list[AsyncEngine]]:
    """Engines a test builds, disposed at teardown even when the test fails."""

    built: list[AsyncEngine] = []
    yield built
    for engine in built:
        await engine.dispose()


async def _show(engine: AsyncEngine, setting: str) -> str:
    async with engine.connect() as connection:
        return str((await connection.execute(text(f"SHOW {setting}"))).scalar_one())


async def _backend_pid(engine: AsyncEngine) -> int:
    async with engine.connect() as connection:
        return int((await connection.execute(text("SELECT pg_backend_pid()"))).scalar_one())


async def test_api_and_liveness_connections_start_with_the_default_bounds(
    connection_bounds: Any, engines: list[AsyncEngine]
) -> None:
    connection_bounds()
    main, liveness = db.create_engine(), db.create_liveness_engine()
    engines += [main, liveness]
    for engine in (main, liveness):
        assert await _show(engine, "lock_timeout") == "30s"
        assert await _show(engine, "idle_in_transaction_session_timeout") == "10min"


async def test_bound_settings_reach_real_connections_and_zero_leaves_them_unset(
    connection_bounds: Any, engines: list[AsyncEngine]
) -> None:
    connection_bounds(
        CURIE_API_DB_LOCK_TIMEOUT_S="1.5",
        CURIE_API_DB_IDLE_IN_TRANSACTION_TIMEOUT_S="0",
    )
    engine = db.create_engine()
    engines.append(engine)
    assert await _show(engine, "lock_timeout") == "1500ms"
    # 0 sends nothing, so the server's own default (off on the test server) applies.
    assert await _show(engine, "idle_in_transaction_session_timeout") == "0"


async def test_a_lock_wait_is_refused_at_lock_timeout_and_the_connection_stays_usable(
    connection_bounds: Any, engines: list[AsyncEngine]
) -> None:
    connection_bounds(CURIE_API_DB_LOCK_TIMEOUT_S="0.3", CURIE_API_DB_COMMAND_TIMEOUT_S="10")
    engine = db.create_engine()
    engines.append(engine)
    key = uuid.uuid4().int & 0x7FFFFFFFFFFFFFFF
    async with engine.connect() as holder:
        await holder.execute(text("SELECT pg_advisory_lock(:key)"), {"key": key})
        async with engine.connect() as waiter:
            started = time.monotonic()
            with pytest.raises(DBAPIError) as refused:
                await asyncio.wait_for(
                    waiter.execute(text("SELECT pg_advisory_xact_lock(:key)"), {"key": key}),
                    OUTER_DEADLINE_S,
                )
            elapsed = time.monotonic() - started
            await waiter.rollback()
            assert (await waiter.execute(text("SELECT 1"))).scalar_one() == 1
        await holder.execute(text("SELECT pg_advisory_unlock(:key)"), {"key": key})
    # 55P03 is lock_not_available: Postgres refused the wait, not a client deadline.
    assert getattr(refused.value.orig, "sqlstate", None) == "55P03"
    assert elapsed < 5
    assert engine.pool.checkedout() == 0  # type: ignore[attr-defined]


async def test_an_idle_transaction_is_ended_by_the_server_and_the_pool_recovers(
    connection_bounds: Any, engines: list[AsyncEngine]
) -> None:
    connection_bounds(CURIE_API_DB_IDLE_IN_TRANSACTION_TIMEOUT_S="0.3")
    engine = db.create_engine()
    engines.append(engine)
    ended = asyncio.Event()
    async with engine.connect() as connection:
        pid = (await connection.execute(text("SELECT pg_backend_pid()"))).scalar_one()
        raw = await connection.get_raw_connection()
        raw.driver_connection.add_termination_listener(lambda _: ended.set())  # type: ignore[union-attr]
        # The transaction is open and nothing runs on it; the server ends it.
        await asyncio.wait_for(ended.wait(), OUTER_DEADLINE_S)
        with pytest.raises(DBAPIError):
            await connection.execute(text("SELECT 1"))
    assert engine.pool.checkedout() == 0  # type: ignore[attr-defined]
    assert await _backend_pid(engine) != pid


async def test_a_statement_over_command_timeout_fails_and_releases_its_connection(
    connection_bounds: Any, engines: list[AsyncEngine]
) -> None:
    connection_bounds(CURIE_API_DB_COMMAND_TIMEOUT_S="0.5")
    engine = db.create_engine()
    engines.append(engine)
    marker = f"command-timeout-{uuid.uuid4().hex}"
    async with engine.connect() as connection:
        # Checked out and in use first, so only the statement can time out.
        await connection.execute(text("SELECT 1"))
        started = time.monotonic()
        with pytest.raises(TimeoutError):
            await connection.execute(text(f"SELECT pg_sleep(5) /* {marker} */"))
        assert time.monotonic() - started < 4
    assert engine.pool.checkedout() == 0  # type: ignore[attr-defined]
    async with engine.connect() as connection:
        # asyncpg canceled the statement server side, so it is not still running.
        running = await connection.execute(
            text(
                "SELECT count(*) FROM pg_stat_activity "
                "WHERE state = 'active' AND query LIKE :pattern AND pid <> pg_backend_pid()"
            ),
            {"pattern": f"%{marker}%"},
        )
        assert running.scalar_one() == 0


async def test_a_blackholed_database_flow_is_released_and_later_checkouts_succeed(
    connection_bounds: Any,
    engines: list[AsyncEngine],
    tcp_blackhole: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    url = make_url(get_settings().database_url)
    relay = tcp_blackhole(url.host or "127.0.0.1", url.port or 5432)
    monkeypatch.setenv(
        "DATABASE_URL",
        url.set(host="127.0.0.1", port=relay.port).render_as_string(hide_password=False),
    )
    connection_bounds(CURIE_API_DB_COMMAND_TIMEOUT_S="0.5")
    engine = db.create_engine()
    engines.append(engine)
    before = await _backend_pid(engine)
    assert relay.cut() == 1

    async def query_on_the_dead_flow() -> None:
        with pytest.raises(TimeoutError):
            async with engine.connect() as connection:
                await connection.execute(text("SELECT 1"))

    started = time.monotonic()
    await asyncio.wait_for(query_on_the_dead_flow(), OUTER_DEADLINE_S)
    assert time.monotonic() - started < 8
    assert engine.pool.checkedout() == 0  # type: ignore[attr-defined]
    # The dead connection was discarded: the next checkout opens a new flow.
    after = await asyncio.wait_for(_backend_pid(engine), OUTER_DEADLINE_S)
    assert after != before


async def test_a_flow_that_dies_inside_a_transaction_is_released_and_replaced(
    connection_bounds: Any,
    engines: list[AsyncEngine],
    tcp_blackhole: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    url = make_url(get_settings().database_url)
    relay = tcp_blackhole(url.host or "127.0.0.1", url.port or 5432)
    monkeypatch.setenv(
        "DATABASE_URL",
        url.set(host="127.0.0.1", port=relay.port).render_as_string(hide_password=False),
    )
    connection_bounds(CURIE_API_DB_COMMAND_TIMEOUT_S="0.5")
    engine = db.create_engine()
    engines.append(engine)

    async def request_whose_flow_dies() -> int:
        # The production session context, as a route or background loop uses it:
        # the transaction is open and the connection checked out when the flow dies.
        async with db.create_sessionmaker(engine)() as session:
            pid = int((await session.execute(text("SELECT pg_backend_pid()"))).scalar_one())
            assert relay.cut() == 1
            with pytest.raises(TimeoutError):
                await session.execute(text("SELECT 1"))
        return pid

    started = time.monotonic()
    before = await asyncio.wait_for(request_whose_flow_dies(), OUTER_DEADLINE_S)
    assert time.monotonic() - started < 8
    assert engine.pool.checkedout() == 0  # type: ignore[attr-defined]
    after = await asyncio.wait_for(_backend_pid(engine), OUTER_DEADLINE_S)
    assert after != before
