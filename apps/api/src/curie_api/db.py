"""Async SQLAlchemy engine, session factory, and declarative base.

Tables live in the dedicated `curie` Postgres schema (config.db_schema); the
Alembic migration creates that schema. The engine and sessionmaker are built
lazily from Settings so tests can point them at the compose Postgres.
"""

from sqlalchemy import MetaData
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import DeclarativeBase

from .config import get_settings

SCHEMA = get_settings().db_schema


class Base(DeclarativeBase):
    metadata = MetaData(schema=SCHEMA)


def create_engine() -> AsyncEngine:
    return create_async_engine(get_settings().database_url, pool_pre_ping=True)


def create_liveness_engine() -> AsyncEngine:
    """Reserve a small database pool for readiness and worker heartbeats."""
    return create_async_engine(
        get_settings().database_url,
        pool_size=4,
        max_overflow=0,
        pool_timeout=5,
        pool_pre_ping=True,
    )


def create_source_gate_engine() -> AsyncEngine:
    """@spec PROTECTED-HOOK-SOURCE-2."""
    return create_async_engine(
        get_settings().database_url,
        pool_size=4,
        max_overflow=0,
        pool_timeout=30,
        pool_pre_ping=True,
    )


def create_sessionmaker(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    return async_sessionmaker(engine, expire_on_commit=False)
