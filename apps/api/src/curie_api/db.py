"""Async SQLAlchemy engine, session factory, and declarative base.

Tables live in the dedicated `curie` Postgres schema (config.db_schema); the
Alembic migration creates that schema. The engine and sessionmaker are built
lazily from Settings so tests can point them at the compose Postgres.
"""

import socket
from typing import Any

from sqlalchemy import MetaData, event
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import DeclarativeBase

from .config import Settings, get_settings

SCHEMA = get_settings().db_schema

# The main pool's shape. Its capacity is what the pool watch
# (db_pool_watch.py) calls exhausted.
MAIN_POOL_SIZE = 5
MAIN_POOL_MAX_OVERFLOW = 10


class Base(DeclarativeBase):
    metadata = MetaData(schema=SCHEMA)


def _connect_args(settings: Settings) -> dict[str, Any]:
    """asyncpg's own bounds on opening a connection and on each call (#5995)."""
    return {
        "timeout": settings.db_connect_timeout_seconds,
        "command_timeout": settings.db_command_timeout_seconds or None,
    }


def _session_bounds(settings: Settings) -> str:
    """One simple query of SET statements, built only from validated numbers."""
    statements = [
        f"SET {name} = {max(1, round(seconds * 1000))}"
        for name, seconds in (
            ("statement_timeout", settings.db_statement_timeout_seconds),
            ("lock_timeout", settings.db_lock_timeout_seconds),
            (
                "idle_in_transaction_session_timeout",
                settings.db_idle_in_transaction_timeout_seconds,
            ),
        )
        if seconds > 0
    ]
    if settings.db_tcp_keepalive_idle_seconds > 0:
        # The server side keepalive lets Postgres notice a dead client and
        # release the locks its session holds.
        statements += [
            f"SET tcp_keepalives_idle = {settings.db_tcp_keepalive_idle_seconds}",
            f"SET tcp_keepalives_interval = {settings.db_tcp_keepalive_interval_seconds}",
            f"SET tcp_keepalives_count = {settings.db_tcp_keepalive_count}",
        ]
    return "; ".join(statements)


def _socket_options(settings: Settings) -> list[tuple[int, int, int]]:
    """Client socket options; each is skipped where the platform lacks it."""
    options: list[tuple[int, int, int]] = []
    if settings.db_tcp_keepalive_idle_seconds > 0:
        options.append((socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1))
        for name, value in (
            ("TCP_KEEPIDLE", settings.db_tcp_keepalive_idle_seconds),
            ("TCP_KEEPINTVL", settings.db_tcp_keepalive_interval_seconds),
            ("TCP_KEEPCNT", settings.db_tcp_keepalive_count),
        ):
            if hasattr(socket, name):
                options.append((socket.IPPROTO_TCP, getattr(socket, name), value))
    if settings.db_tcp_user_timeout_seconds > 0 and hasattr(socket, "TCP_USER_TIMEOUT"):
        options.append(
            (
                socket.IPPROTO_TCP,
                socket.TCP_USER_TIMEOUT,
                settings.db_tcp_user_timeout_seconds * 1000,
            )
        )
    return options


def apply_socket_options(sock: Any, options: list[tuple[int, int, int]]) -> None:
    """Set the client socket options on a TCP socket; any other family is left as is.

    A unix domain socket (a DATABASE_URL with a host directory) rejects the
    TCP options, and a local socket has no peer to lose.
    """
    if sock.family not in (socket.AF_INET, socket.AF_INET6):
        return
    for level, name, value in options:
        sock.setsockopt(level, name, value)


def _bounded(engine: AsyncEngine, settings: Settings) -> AsyncEngine:
    """Bound every physical connection right after it opens.

    The session bounds are SET after connect rather than sent as startup
    parameters: a session mode connection pooler in front of a bring your own
    Postgres refuses startup parameters outside a short allowlist, while a SET
    works everywhere. asyncpg leaves SO_KEEPALIVE off on its socket, so the
    client side keepalive and user timeout are set here as well.
    """
    bounds = _session_bounds(settings)
    options = _socket_options(settings)

    def connected(dbapi_connection: Any, connection_record: Any) -> None:
        # The asyncpg version is locked and a test pins this transport shape.
        sock = dbapi_connection.driver_connection._transport.get_extra_info("socket")
        apply_socket_options(sock, options)
        if bounds:
            dbapi_connection.run_async(lambda connection: connection.execute(bounds))

    event.listen(engine.sync_engine, "connect", connected)
    return engine


def create_engine() -> AsyncEngine:
    settings = get_settings()
    return _bounded(
        create_async_engine(
            settings.database_url,
            pool_size=MAIN_POOL_SIZE,
            max_overflow=MAIN_POOL_MAX_OVERFLOW,
            pool_timeout=settings.db_pool_timeout_seconds,
            pool_pre_ping=True,
            connect_args=_connect_args(settings),
        ),
        settings,
    )


def create_liveness_engine() -> AsyncEngine:
    """Reserve a small database pool for readiness and worker heartbeats.

    It carries the main pool's connection bounds: it is the reference for
    whether the database is reachable, so it must survive dead flows too.
    """
    settings = get_settings()
    return _bounded(
        create_async_engine(
            settings.database_url,
            pool_size=4,
            max_overflow=0,
            pool_timeout=5,
            pool_pre_ping=True,
            connect_args=_connect_args(settings),
        ),
        settings,
    )


def create_source_gate_engine() -> AsyncEngine:
    """@spec PROTECTED-HOOK-SOURCE-2.

    Deliberately left without the main pool's bounds: this pool carries its own
    specification and its own acquisition deadlines.
    """
    return create_async_engine(
        get_settings().database_url,
        pool_size=4,
        max_overflow=0,
        pool_timeout=30,
        pool_pre_ping=True,
    )


def create_sessionmaker(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    return async_sessionmaker(engine, expire_on_commit=False)
