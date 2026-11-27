"""Async SQLAlchemy engine, session factory, and declarative base.

Tables live in the dedicated `curie` Postgres schema (config.db_schema); the
Alembic migration creates that schema. The engine and sessionmaker are built
lazily from Settings so tests can point them at the compose Postgres.
"""

from typing import Any

from sqlalchemy import MetaData
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import DeclarativeBase

from .config import Settings, get_settings

SCHEMA = get_settings().db_schema


class Base(DeclarativeBase):
    metadata = MetaData(schema=SCHEMA)


def _postgres_ms(seconds: float) -> str:
    """A Postgres duration setting in milliseconds, rounded up to at least 1 ms."""
    return str(max(1, round(seconds * 1000)))


def bounded_connect_args(settings: Settings) -> dict[str, Any]:
    """asyncpg connect arguments that bound how long one connection can be held (#5995).

    Used by the main pool (``create_engine``, behind ``app.state.sessionmaker``)
    and the liveness pool, so a holder that never finishes is ended within a
    configured bound instead of keeping its connection until the process restarts.

    ``lock_timeout`` and ``idle_in_transaction_session_timeout`` travel as
    ``server_settings`` in the startup packet, so every physical connection the
    pool opens has them from its first statement, including one opened after a
    dead connection was discarded. They are session defaults: a transaction's
    own ``SET LOCAL lock_timeout`` still wins for that transaction.
    ``command_timeout`` is asyncpg's client deadline on each statement, the bound
    that still fires when the server's reply is lost on the network. asyncpg
    exempts one exchange from it: closing prepared statements that SQLAlchemy's
    statement cache evicted, which asyncpg sends untimed right after the next
    statement is prepared. A 0 setting leaves its bound unset.
    """
    server_settings: dict[str, str] = {}
    if settings.db_lock_timeout_s:
        server_settings["lock_timeout"] = _postgres_ms(settings.db_lock_timeout_s)
    if settings.db_idle_in_transaction_timeout_s:
        server_settings["idle_in_transaction_session_timeout"] = _postgres_ms(
            settings.db_idle_in_transaction_timeout_s
        )
    return {
        "server_settings": server_settings,
        "command_timeout": settings.db_command_timeout_s or None,
    }


def create_engine() -> AsyncEngine:
    return create_async_engine(
        get_settings().database_url,
        pool_pre_ping=True,
        connect_args=bounded_connect_args(get_settings()),
    )


def create_liveness_engine() -> AsyncEngine:
    """Reserve a small database pool for readiness and worker heartbeats.

    It carries the same bounds as the main pool: its statements are short, and
    without a command timeout four connections lost on a dead network flow
    would keep readiness and every worker heartbeat failing until a restart.
    """
    settings = get_settings()
    return create_async_engine(
        settings.database_url,
        pool_size=4,
        max_overflow=0,
        pool_timeout=5,
        pool_pre_ping=True,
        connect_args=bounded_connect_args(settings),
    )


def create_source_gate_engine() -> AsyncEngine:
    """@spec PROTECTED-HOOK-SOURCE-2.

    Deliberately without ``bounded_connect_args``: SourceGate sets its own lock
    bound per hold and specifies that the guarded body is never time limited,
    so a session-wide timeout here would change that contract.
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
