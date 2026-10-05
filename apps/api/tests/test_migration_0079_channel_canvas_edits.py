"""Migration 0079 adds curie.channel_canvas_edits (ADR 0200, #3819)."""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

from alembic import command
from alembic.config import Config
from alembic.script import ScriptDirectory
from curie_api.config import get_settings
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

ALEMBIC_DIR = Path(__file__).resolve().parents[1] / "alembic"

EXPECTED_COLUMNS = {
    "id",
    "agent_id",
    "deployment_id",
    "turn",
    "kind",
    "channel_address",
    "canvas_id",
    "section_id",
    "before_text",
    "after_text",
    "status",
    "error_code",
    "created_at",
    "completed_at",
}
# The status check is named so a rename cannot go unnoticed.
STATUS_CHECK = "channel_canvas_edits_status_ck"
CANVAS_INDEX = "ix_channel_canvas_edits_canvas_created"


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
                if not result.returns_rows:
                    return []
                return [dict(row) for row in result.mappings().all()]
        finally:
            await engine.dispose()

    return asyncio.run(run())


def _regclass(table: str) -> str | None:
    rows = _sql("SELECT to_regclass(:name)::text AS name", {"name": f"curie.{table}"})
    name = rows[0]["name"]
    return None if name is None else str(name)


def _columns() -> set[str]:
    rows = _sql(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_schema = 'curie' AND table_name = 'channel_canvas_edits'"
    )
    return {row["column_name"] for row in rows}


def _constraint_names() -> set[str]:
    rows = _sql(
        "SELECT conname FROM pg_constraint WHERE conrelid = to_regclass(:table)",
        {"table": "curie.channel_canvas_edits"},
    )
    return {row["conname"] for row in rows}


def _index_columns(index: str) -> list[str]:
    rows = _sql(
        "SELECT a.attname AS name FROM pg_index i "
        "JOIN pg_class c ON c.oid = i.indexrelid "
        "JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey) "
        "WHERE c.relname = :index AND c.relnamespace = 'curie'::regnamespace "
        "ORDER BY array_position(i.indkey::int2[], a.attnum)",
        {"index": index},
    )
    return [row["name"] for row in rows]


def test_0079_revises_0078() -> None:
    script = ScriptDirectory.from_config(_config())
    revision = script.get_revision("0079")
    assert revision is not None
    assert revision.down_revision == "0078"


def test_0079_upgrade_creates_the_table_check_and_index(isolated_migration_db: None) -> None:
    config = _config()
    command.upgrade(config, "0079")
    assert _regclass("channel_canvas_edits") is not None
    assert _columns() == EXPECTED_COLUMNS
    assert STATUS_CHECK in _constraint_names()
    assert _index_columns(CANVAS_INDEX) == ["canvas_id", "created_at"]


def test_0079_downgrade_drops_the_table(isolated_migration_db: None) -> None:
    config = _config()
    command.upgrade(config, "0079")
    assert _regclass("channel_canvas_edits") is not None
    try:
        command.downgrade(config, "0078")
        assert _regclass("channel_canvas_edits") is None
        # The agents FK target must outlive the downgrade.
        assert _regclass("agents") is not None
    finally:
        # A failed assertion must not leave this private database below head.
        command.upgrade(config, "head")
    assert _regclass("channel_canvas_edits") is not None
