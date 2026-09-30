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
import uuid

import pytest
from _migration_support import IsolatedMigrationDb, alembic_config
from alembic import command
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


def _insert_approval(approval_id: uuid.UUID, reply_placeholder: str | None) -> None:
    async def run() -> None:
        engine = create_async_engine(get_settings().database_url)
        try:
            async with engine.begin() as conn:
                await conn.execute(
                    text(
                        "INSERT INTO curie.approvals (id, conversation_id, author, "
                        "summary, reply_kind, reply_channel, reply_placeholder, "
                        "dedupe_key, status) "
                        "VALUES (:id, :conversation_id, :author, :summary, :reply_kind, "
                        ":reply_channel, :reply_placeholder, :dedupe_key, 'pending')"
                    ),
                    {
                        "id": approval_id,
                        "conversation_id": f"th-{approval_id.hex[:8]}",
                        "author": "U1",
                        "summary": "Approve the change",
                        "reply_kind": "slack",
                        "reply_channel": "C1",
                        "reply_placeholder": reply_placeholder,
                        "dedupe_key": uuid.uuid4().hex,
                    },
                )
        finally:
            await engine.dispose()

    asyncio.run(run())


def _set_reply_placeholder(approval_id: uuid.UUID, placeholder: str) -> None:
    async def run() -> None:
        engine = create_async_engine(get_settings().database_url)
        try:
            async with engine.begin() as conn:
                await conn.execute(
                    text(
                        "UPDATE curie.approvals SET reply_placeholder = :placeholder "
                        "WHERE id = :id"
                    ),
                    {"id": approval_id, "placeholder": placeholder},
                )
        finally:
            await engine.dispose()

    asyncio.run(run())


def _reply_placeholder(approval_id: uuid.UUID) -> str | None:
    async def run() -> str | None:
        engine = create_async_engine(get_settings().database_url)
        try:
            async with engine.connect() as conn:
                result = await conn.execute(
                    text(
                        "SELECT reply_placeholder FROM curie.approvals WHERE id = :id"
                    ),
                    {"id": approval_id},
                )
                row = result.one()
                return row[0]
        finally:
            await engine.dispose()

    return asyncio.run(run())


def test_alembic_version_lives_in_app_schema(migrated: None) -> None:
    schemas = _column(
        "SELECT schemaname FROM pg_tables WHERE tablename = 'alembic_version'"
    )
    # Exactly one alembic_version, and it is in the curie schema (never public).
    assert schemas == [SCHEMA], schemas


def test_public_schema_is_empty_after_migration(migrated: None) -> None:
    # The Langfuse Prisma baseline (P3005) refuses a non-empty public schema, so
    # our migrations must leave zero user tables there.
    public_tables = _column(
        "SELECT tablename FROM pg_tables WHERE schemaname = 'public'"
    )
    assert public_tables == [], public_tables


def test_reply_placeholder_becomes_nullable_without_rewriting_existing_strings(
    isolated_migration_db: IsolatedMigrationDb,
) -> None:
    """The post once migration preserves old edit targets while allowing new ones."""

    cfg = alembic_config()

    # Target the revision immediately before this migration. A relative revision
    # would silently stop testing this migration once a later one lands.
    isolated_migration_db.at("0024")
    existing_id = uuid.uuid4()
    _insert_approval(existing_id, "p-existing")

    command.upgrade(cfg, "0025")

    nullable = _column(
        "SELECT is_nullable FROM information_schema.columns "
        "WHERE table_schema = 'curie' AND table_name = 'approvals' "
        "AND column_name = 'reply_placeholder'"
    )
    assert nullable == ["YES"], nullable
    assert _reply_placeholder(existing_id) == "p-existing"

    post_once_id = uuid.uuid4()
    _insert_approval(post_once_id, None)
    assert _reply_placeholder(post_once_id) is None


def test_reply_placeholder_downgrade_refuses_null_rows(
    isolated_migration_db: IsolatedMigrationDb,
) -> None:
    cfg = alembic_config()
    isolated_migration_db.at("0025")

    approval_id = uuid.uuid4()
    _insert_approval(approval_id, None)

    with pytest.raises(RuntimeError, match="cannot restore a required reply placeholder"):
        command.downgrade(cfg, "0024")

    assert _reply_placeholder(approval_id) is None
    _set_reply_placeholder(approval_id, "p-fixed")
    command.downgrade(cfg, "0024")

    nullable = _column(
        "SELECT is_nullable FROM information_schema.columns "
        "WHERE table_schema = 'curie' AND table_name = 'approvals' "
        "AND column_name = 'reply_placeholder'"
    )
    assert nullable == ["NO"], nullable
    assert _reply_placeholder(approval_id) == "p-fixed"
