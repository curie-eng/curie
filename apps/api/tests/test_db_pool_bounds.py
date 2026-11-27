"""Every API database connection is bounded (#5995).

Each case builds its engine through the production factories in
``curie_api.db`` against the suite's real Postgres, with the bounds set the
way an operator sets them, through the environment. Nothing here mocks a
driver, a pool or the server: a dead network path is a real TCP relay that
stops relaying, a held lock is a real advisory lock on another connection, and
a terminated session is the server's own idle transaction timeout.
"""

from __future__ import annotations

import asyncio
import contextlib
import socket
import time
import uuid
from collections.abc import Iterator
from typing import Any

import pytest
from _tcp_blackhole import Blackhole, within
from curie_api import db
from curie_api.config import get_settings
from curie_test_support.static_shell import wait_until
from identity_writer_lifetime_support import run
from sqlalchemy import make_url, text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine, create_async_engine
from sqlalchemy.pool import NullPool

_SESSION_SETTINGS = (
    "statement_timeout",
    "lock_timeout",
    "idle_in_transaction_session_timeout",
    "tcp_keepalives_idle",
    "tcp_keepalives_interval",
    "tcp_keepalives_count",
)
_BOUND_VARIABLES = (
    "DB_POOL_TIMEOUT_SECONDS",
    "DB_CONNECT_TIMEOUT_SECONDS",
    "DB_COMMAND_TIMEOUT_SECONDS",
    "DB_STATEMENT_TIMEOUT_SECONDS",
    "DB_LOCK_TIMEOUT_SECONDS",
    "DB_IDLE_IN_TRANSACTION_TIMEOUT_SECONDS",
    "DB_TCP_KEEPALIVE_IDLE_SECONDS",
    "DB_TCP_KEEPALIVE_INTERVAL_SECONDS",
    "DB_TCP_KEEPALIVE_COUNT",
    "DB_TCP_USER_TIMEOUT_SECONDS",
    "VALKEY_SOCKET_TIMEOUT_SECONDS",
    "VALKEY_SOCKET_CONNECT_TIMEOUT_SECONDS",
    "IDENTITY_FENCE_ACQUIRE_TIMEOUT_SECONDS",
)


@pytest.fixture
def bounds(migrated: None, monkeypatch: pytest.MonkeyPatch) -> Iterator[Any]:
    """Set bounds through the environment; every other bound is its default."""
    for name in _BOUND_VARIABLES:
        monkeypatch.delenv(name, raising=False)

    def apply(**values: object) -> None:
        for name, value in values.items():
            monkeypatch.setenv(name, str(value))
        get_settings.cache_clear()

    get_settings.cache_clear()
    yield apply
    monkeypatch.undo()
    get_settings.cache_clear()


async def session_settings(connection: AsyncConnection) -> dict[str, str]:
    rows = await connection.execute(
        text("SELECT name, setting FROM pg_settings WHERE name = ANY(:names)"),
        {"names": list(_SESSION_SETTINGS)},
    )
    return {name: setting for name, setting in rows}


async def live_socket(connection: AsyncConnection) -> Any:
    raw = await connection.get_raw_connection()
    return raw.driver_connection._transport.get_extra_info("socket")


def sqlstate(error: DBAPIError) -> str | None:
    return getattr(error.orig, "sqlstate", None)


def test_blackholed_query_releases_its_connection_within_the_bound(bounds: Any) -> None:
    """A reply that never arrives fails the call and frees its pool slot.

    The query is running on the server before the flow dies, so only the
    client's command timeout can end the wait: the statement timeout is off
    and the server's reply is dropped. Observed with asyncpg 0.32.0 and
    SQLAlchemy 2.0.52: the builtin TimeoutError reaches the caller unwrapped
    after about twice the command timeout, asyncpg closes the connection and
    SQLAlchemy invalidates it, so the slot returns to the pool.
    """
    real = make_url(get_settings().database_url)
    sleeping = "SELECT pg_sleep(30)"

    async def check() -> None:
        # Autocommit gives every poll its own transaction, so each one reads a
        # fresh activity snapshot instead of the first poll's frozen one.
        observer = create_async_engine(
            get_settings().database_url, poolclass=NullPool, isolation_level="AUTOCOMMIT"
        )
        backend: int | None = None
        async with Blackhole(real.host or "127.0.0.1", real.port or 5432) as relay:
            url = real.set(host="127.0.0.1", port=relay.port)
            bounds(
                DATABASE_URL=url.render_as_string(hide_password=False),
                DB_COMMAND_TIMEOUT_SECONDS=1,
                DB_STATEMENT_TIMEOUT_SECONDS=0,
            )
            engine = db.create_engine()
            try:
                async with within(15, "a query on a dead flow"), engine.connect() as connection:
                    backend = await connection.scalar(text("SELECT pg_backend_pid()"))
                    query = asyncio.create_task(connection.execute(text(sleeping)))
                    async with observer.connect() as watching:
                        # Each check is a real round trip, so the loop paces itself.
                        while not await watching.scalar(
                            text(
                                "SELECT count(*) FROM pg_stat_activity"
                                " WHERE pid = :pid AND state = 'active' AND query = :query"
                                " AND wait_event = 'PgSleep'"
                            ),
                            {"pid": backend, "query": sleeping},
                        ):
                            assert not query.done()
                    relay.freeze()
                    started = time.monotonic()
                    with pytest.raises(TimeoutError):
                        await query
                    elapsed = time.monotonic() - started
                assert elapsed < 6, elapsed
                assert engine.pool.checkedout() == 0  # type: ignore[attr-defined]
                async with within(5, "a fresh checkout after the dead flow"):
                    async with engine.connect() as connection:
                        assert await connection.scalar(text("SELECT 1")) == 1
            finally:
                await relay.close()
                await engine.dispose()
                if backend is not None:
                    async with observer.connect() as cleanup:
                        await cleanup.execute(
                            text("SELECT pg_terminate_backend(:pid)"), {"pid": backend}
                        )
                await observer.dispose()

    run(check)


def test_session_bounds_follow_the_settings(bounds: Any) -> None:
    bounds(
        DB_STATEMENT_TIMEOUT_SECONDS=7,
        DB_LOCK_TIMEOUT_SECONDS=3,
        DB_IDLE_IN_TRANSACTION_TIMEOUT_SECONDS=11,
        DB_TCP_KEEPALIVE_IDLE_SECONDS=17,
        DB_TCP_KEEPALIVE_INTERVAL_SECONDS=4,
        DB_TCP_KEEPALIVE_COUNT=5,
    )

    async def check() -> None:
        engine = db.create_engine()
        try:
            async with engine.connect() as connection:
                assert await session_settings(connection) == {
                    "statement_timeout": "7000",
                    "lock_timeout": "3000",
                    "idle_in_transaction_session_timeout": "11000",
                    "tcp_keepalives_idle": "17",
                    "tcp_keepalives_interval": "4",
                    "tcp_keepalives_count": "5",
                }
        finally:
            await engine.dispose()

    run(check)


def test_statement_timeout_cancels_and_the_connection_is_reused(bounds: Any) -> None:
    bounds(DB_STATEMENT_TIMEOUT_SECONDS=0.5)

    async def check() -> None:
        engine = db.create_engine()
        try:
            async with within(10, "a long statement"), engine.connect() as connection:
                started = time.monotonic()
                with pytest.raises(DBAPIError) as raised:
                    await connection.execute(text("SELECT pg_sleep(5)"))
                assert sqlstate(raised.value) == "57014"
                assert time.monotonic() - started < 3
                await connection.rollback()
                assert not connection.invalidated
                assert await connection.scalar(text("SELECT 1")) == 1
        finally:
            await engine.dispose()

    run(check)


def test_lock_timeout_bounds_an_advisory_lock_wait(bounds: Any) -> None:
    bounds(DB_LOCK_TIMEOUT_SECONDS=0.5)
    key = uuid.uuid4().int % (2**62)

    async def check() -> None:
        holder = create_async_engine(get_settings().database_url, poolclass=NullPool)
        engine = db.create_engine()
        try:
            async with holder.connect() as held:
                await held.execute(text("SELECT pg_advisory_lock(:key)"), {"key": key})
                async with within(10, "an advisory lock wait"), engine.connect() as connection:
                    started = time.monotonic()
                    with pytest.raises(DBAPIError) as raised:
                        await connection.execute(
                            text("SELECT pg_advisory_xact_lock(:key)"), {"key": key}
                        )
                    assert sqlstate(raised.value) == "55P03"
                    assert time.monotonic() - started < 3
                    await connection.rollback()
                    assert await connection.scalar(text("SELECT 1")) == 1
                await held.execute(text("SELECT pg_advisory_unlock(:key)"), {"key": key})
        finally:
            await engine.dispose()
            await holder.dispose()

    run(check)


def test_idle_transaction_is_terminated_and_the_pool_recovers(bounds: Any) -> None:
    bounds(DB_IDLE_IN_TRANSACTION_TIMEOUT_SECONDS=0.5)

    async def check() -> None:
        engine = db.create_engine()
        try:
            connection = await engine.connect()
            await connection.begin()
            await connection.execute(text("SELECT 1"))
            driver = (await connection.get_raw_connection()).driver_connection
            # The server ends the idle session itself; observe the closed socket.
            await asyncio.to_thread(
                wait_until, driver.is_closed, 10, "the idle transaction was terminated"
            )
            with pytest.raises(DBAPIError):
                await connection.execute(text("SELECT 1"))
            with contextlib.suppress(Exception):
                await connection.close()
            assert engine.pool.checkedout() == 0  # type: ignore[attr-defined]
            async with within(5, "a fresh checkout"), engine.connect() as fresh:
                assert await fresh.scalar(text("SELECT 1")) == 1
        finally:
            await engine.dispose()

    run(check)


def test_client_socket_keepalive_and_user_timeout(bounds: Any) -> None:
    """asyncpg 0.32.0 sets only TCP_NODELAY; SO_KEEPALIVE is 0 unless set (observed)."""
    bounds(
        DB_TCP_KEEPALIVE_IDLE_SECONDS=17,
        DB_TCP_KEEPALIVE_INTERVAL_SECONDS=4,
        DB_TCP_KEEPALIVE_COUNT=5,
        DB_TCP_USER_TIMEOUT_SECONDS=9,
    )

    async def check() -> None:
        engine = db.create_engine()
        try:
            async with engine.connect() as connection:
                sock = await live_socket(connection)
                assert sock.getsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE) != 0
                assert sock.getsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE) == 17
                assert sock.getsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL) == 4
                assert sock.getsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT) == 5
                assert sock.getsockopt(socket.IPPROTO_TCP, socket.TCP_USER_TIMEOUT) == 9000
        finally:
            await engine.dispose()

    run(check)


def test_socket_options_skip_a_unix_domain_socket(bounds: Any) -> None:
    """A unix socket DATABASE_URL connects: the TCP options are not applied.

    Observed on Linux: SO_KEEPALIVE is accepted on a unix socket, the TCP
    keepalive options raise EOPNOTSUPP.
    """
    bounds(DB_TCP_KEEPALIVE_IDLE_SECONDS=17, DB_TCP_USER_TIMEOUT_SECONDS=9)
    options = db._socket_options(get_settings())
    assert options
    left, right = socket.socketpair()
    try:
        assert left.family == socket.AF_UNIX
        db.apply_socket_options(left, options)
        assert left.getsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE) == 0
    finally:
        left.close()
        right.close()


def test_disabled_bounds_still_connect_and_query(bounds: Any) -> None:
    bounds(
        DB_COMMAND_TIMEOUT_SECONDS=0,
        DB_STATEMENT_TIMEOUT_SECONDS=0,
        DB_LOCK_TIMEOUT_SECONDS=0,
        DB_IDLE_IN_TRANSACTION_TIMEOUT_SECONDS=0,
        DB_TCP_KEEPALIVE_IDLE_SECONDS=0,
        DB_TCP_USER_TIMEOUT_SECONDS=0,
    )

    async def check() -> None:
        engine = db.create_engine()
        try:
            async with engine.connect() as connection:
                assert await connection.scalar(text("SELECT 1")) == 1
                sock = await live_socket(connection)
                assert sock.getsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE) == 0
        finally:
            await engine.dispose()

    run(check)


def test_default_bounds_are_the_documented_ones(bounds: Any) -> None:
    async def check() -> None:
        engine = db.create_engine()
        try:
            assert engine.pool.timeout() == 30  # type: ignore[attr-defined]
            async with engine.connect() as connection:
                assert await session_settings(connection) == {
                    "statement_timeout": "60000",
                    "lock_timeout": "30000",
                    "idle_in_transaction_session_timeout": "900000",
                    "tcp_keepalives_idle": "30",
                    "tcp_keepalives_interval": "10",
                    "tcp_keepalives_count": "3",
                }
                sock = await live_socket(connection)
                assert sock.getsockopt(socket.IPPROTO_TCP, socket.TCP_USER_TIMEOUT) == 60000
        finally:
            await engine.dispose()

    run(check)
    settings = get_settings()
    assert (
        settings.db_connect_timeout_seconds,
        settings.db_command_timeout_seconds,
        settings.valkey_socket_timeout_seconds,
        settings.valkey_socket_connect_timeout_seconds,
        settings.identity_fence_acquire_timeout_seconds,
    ) == (10, 75, 5, 2, 30)
    assert (db.MAIN_POOL_SIZE, db.MAIN_POOL_MAX_OVERFLOW) == (5, 10)


def test_liveness_engine_carries_the_same_bounds(bounds: Any) -> None:
    bounds(
        DB_STATEMENT_TIMEOUT_SECONDS=7,
        DB_LOCK_TIMEOUT_SECONDS=3,
        DB_IDLE_IN_TRANSACTION_TIMEOUT_SECONDS=11,
        DB_TCP_KEEPALIVE_IDLE_SECONDS=17,
        DB_TCP_KEEPALIVE_INTERVAL_SECONDS=4,
        DB_TCP_KEEPALIVE_COUNT=5,
        DB_TCP_USER_TIMEOUT_SECONDS=9,
    )

    async def check() -> None:
        engine: AsyncEngine = db.create_liveness_engine()
        try:
            async with engine.connect() as connection:
                assert await session_settings(connection) == {
                    "statement_timeout": "7000",
                    "lock_timeout": "3000",
                    "idle_in_transaction_session_timeout": "11000",
                    "tcp_keepalives_idle": "17",
                    "tcp_keepalives_interval": "4",
                    "tcp_keepalives_count": "5",
                }
                sock = await live_socket(connection)
                assert sock.getsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE) != 0
                assert sock.getsockopt(socket.IPPROTO_TCP, socket.TCP_USER_TIMEOUT) == 9000
        finally:
            await engine.dispose()

    run(check)
