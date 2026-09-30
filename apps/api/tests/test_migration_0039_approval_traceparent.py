"""Migration 0039: private, bounded approval trace continuity."""

from __future__ import annotations

import asyncio
import uuid

import pytest
from _migration_support import IsolatedMigrationDb, alembic_config, sql_dicts
from alembic import command
from curie_api.config import get_settings
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import create_async_engine


def _seed_legacy_approval(approval_id: uuid.UUID) -> None:
    async def run() -> None:
        engine = create_async_engine(get_settings().database_url)
        try:
            async with engine.begin() as conn:
                await conn.execute(
                    text(
                        "INSERT INTO curie.approvals "
                        "(id, conversation_id, author, summary, reply_kind, "
                        "reply_channel, dedupe_key, status) VALUES "
                        "(:id, 'thread-legacy', 'U0REQUEST1', 'Approve action', "
                        "'slack', 'C0EXAMPLE1', :dedupe, 'pending')"
                    ),
                    {"id": approval_id, "dedupe": f"legacy-{approval_id.hex}"},
                )
        finally:
            await engine.dispose()

    asyncio.run(run())


def _write_traceparent(approval_id: uuid.UUID, value: str) -> None:
    async def run() -> None:
        engine = create_async_engine(get_settings().database_url)
        try:
            async with engine.begin() as conn:
                await conn.execute(
                    text(
                        "UPDATE curie.approvals SET traceparent = :value "
                        "WHERE id = :id"
                    ),
                    {"id": approval_id, "value": value},
                )
        finally:
            await engine.dispose()

    asyncio.run(run())


def test_0039_adds_nullable_bounded_private_carrier_and_round_trips(
    isolated_migration_db: IsolatedMigrationDb,
) -> None:
    cfg = alembic_config()
    isolated_migration_db.at("0038")
    approval_id = uuid.uuid4()
    _seed_legacy_approval(approval_id)

    command.upgrade(cfg, "head")

    assert sql_dicts(
        "SELECT is_nullable, character_maximum_length FROM information_schema.columns "
        "WHERE table_schema = 'curie' AND table_name = 'approvals' "
        "AND column_name = 'traceparent'"
    ) == [{"is_nullable": "YES", "character_maximum_length": 55}]
    assert sql_dicts(
        "SELECT traceparent FROM curie.approvals WHERE id = :id",
        {"id": approval_id},
    ) == [{"traceparent": None}]
    valid = "00-2123456789abcdef0123456789abcdef-2123456789abcdef-01"
    assert len(valid) == 55
    _write_traceparent(approval_id, valid)
    assert sql_dicts(
        "SELECT traceparent FROM curie.approvals WHERE id = :id",
        {"id": approval_id},
    ) == [{"traceparent": valid}]
    with pytest.raises(DBAPIError):
        _write_traceparent(approval_id, "x" * 56)

    command.downgrade(cfg, "0038")
    assert sql_dicts(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_schema = 'curie' AND table_name = 'approvals' "
        "AND column_name = 'traceparent'"
    ) == []

    command.upgrade(cfg, "head")
    assert sql_dicts(
        "SELECT is_nullable, character_maximum_length FROM information_schema.columns "
        "WHERE table_schema = 'curie' AND table_name = 'approvals' "
        "AND column_name = 'traceparent'"
    ) == [{"is_nullable": "YES", "character_maximum_length": 55}]
