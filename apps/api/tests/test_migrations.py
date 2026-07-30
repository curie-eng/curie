"""The Alembic version table must land in the app schema, not public.

A fresh install runs ``alembic upgrade head`` against a shared Postgres that
Langfuse also uses; Langfuse's Prisma baseline (P3005) requires an EMPTY public
schema on first boot, so a stray ``public.alembic_version`` crash-loops
langfuse-web forever. env.py pins ``version_table_schema`` to the curie schema
in both run paths; these tests guard that property against regression. They run
against the session's freshly-created, migrated disposable DB (conftest's
``migrated``), which is exactly a fresh-install shape.
"""

import asyncio

from curie_api.config import get_settings
from curie_api.db import SCHEMA
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine


def _column(sql: str) -> list[str]:
    async def run() -> list[str]:
        engine = create_async_engine(get_settings().database_url)
        try:
            async with engine.connect() as conn:
                result = await conn.execute(text(sql))
                return [row[0] for row in result.fetchall()]
        finally:
            await engine.dispose()

    return asyncio.run(run())


def test_alembic_version_lives_in_app_schema(migrated: None) -> None:
    schemas = _column("SELECT schemaname FROM pg_tables WHERE tablename = 'alembic_version'")
    # Exactly one alembic_version, and it is in the curie schema (never public).
    assert schemas == [SCHEMA], schemas


def test_public_schema_is_empty_after_migration(migrated: None) -> None:
    # The Langfuse Prisma baseline (P3005) refuses a non-empty public schema, so
    # our migrations must leave zero user tables there.
    public_tables = _column("SELECT tablename FROM pg_tables WHERE schemaname = 'public'")
    assert public_tables == [], public_tables
