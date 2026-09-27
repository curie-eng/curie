"""Migration 0071 adds `agents.memory_writes` (#1461).

`BOOLEAN NOT NULL DEFAULT false`: existing agents come out of the upgrade with
the memory tools off, and downgrade removes the column again.
"""

from __future__ import annotations

import asyncio
import uuid
from pathlib import Path
from typing import Any

from alembic import command
from alembic.config import Config
from alembic.script import ScriptDirectory
from curie_api.config import get_settings
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

ALEMBIC_DIR = Path(__file__).resolve().parents[1] / "alembic"
REVISION = "0071"
BELOW = "0070"


def _config() -> Config:
    config = Config()
    config.set_main_option("script_location", str(ALEMBIC_DIR))
    return config


def _sql(statement: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    async def run() -> list[dict[str, Any]]:
        engine = create_async_engine(get_settings().database_url)
        try:
            async with engine.begin() as connection:
                result = await connection.execute(text(statement), params or {})
                return [dict(row) for row in result.mappings().all()] if result.returns_rows else []
        finally:
            await engine.dispose()

    return asyncio.run(run())


def _column() -> dict[str, Any] | None:
    rows = _sql(
        "SELECT data_type, is_nullable, column_default FROM information_schema.columns "
        "WHERE table_schema = 'curie' AND table_name = 'agents' "
        "AND column_name = 'memory_writes'"
    )
    return rows[0] if rows else None


def _insert_agent() -> uuid.UUID:
    agent_id = uuid.uuid4()
    _sql(
        "INSERT INTO curie.agents (id, name) VALUES (:id, :name)",
        {"id": agent_id, "name": f"m0071-{agent_id.hex[:8]}"},
    )
    return agent_id


def test_0071_follows_0070() -> None:
    script = ScriptDirectory.from_config(_config())
    revision = script.get_revision(REVISION)
    assert revision is not None
    assert revision.down_revision == BELOW


def test_0071_adds_memory_writes_default_false_and_downgrade_drops_it(
    isolated_migration_db: None,
) -> None:
    config = _config()
    command.upgrade(config, BELOW)
    assert _column() is None
    # A row that predates the column must come out of the upgrade with it off.
    existing = _insert_agent()
    try:
        command.upgrade(config, REVISION)
        column = _column()
        assert column is not None
        assert column["data_type"] == "boolean"
        assert column["is_nullable"] == "NO"
        assert column["column_default"] == "false"
        rows = _sql("SELECT memory_writes FROM curie.agents WHERE id = :id", {"id": existing})
        assert rows == [{"memory_writes": False}]
        fresh = _insert_agent()
        rows = _sql("SELECT memory_writes FROM curie.agents WHERE id = :id", {"id": fresh})
        assert rows == [{"memory_writes": False}]

        command.downgrade(config, BELOW)
        assert _column() is None
    finally:
        # A failed assertion must not leave this private database below head.
        command.upgrade(config, "head")
