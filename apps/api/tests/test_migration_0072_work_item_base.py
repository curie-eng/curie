"""Migration 0072 records a factory WorkItem's base (#3095, ADR 0186).

Four nullable columns: `base_branch`, `base_source` ('label' or 'default'),
`base_commit` and `base_label_ignored`. Branch, source and commit are set
together or not at all; NULL is a legacy row that keeps today's behaviour.
"""

from __future__ import annotations

import asyncio
import uuid
from pathlib import Path
from typing import Any

import pytest
from alembic import command
from alembic.config import Config
from alembic.script import ScriptDirectory
from curie_api.config import get_settings
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import create_async_engine

ALEMBIC_DIR = Path(__file__).resolve().parents[1] / "alembic"
REVISION = "0072"
BELOW = "0071"
COLUMNS = ("base_branch", "base_source", "base_commit", "base_label_ignored")
SHA = "a" * 40


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


def _columns() -> dict[str, dict[str, Any]]:
    rows = _sql(
        "SELECT column_name, data_type, is_nullable FROM information_schema.columns "
        "WHERE table_schema = 'curie' AND table_name = 'work_items' "
        "AND column_name IN ('base_branch', 'base_source', 'base_commit', 'base_label_ignored')"
    )
    return {row["column_name"]: row for row in rows}


_ISSUE = iter(range(72000, 73000))


def _insert_work_item(**base: Any) -> uuid.UUID:
    agent_id, item_id = uuid.uuid4(), uuid.uuid4()
    _sql(
        "INSERT INTO curie.agents (id, name) VALUES (:id, :name)",
        {"id": agent_id, "name": f"m0072-{agent_id.hex[:8]}"},
    )
    names = ", ".join(base)
    values = ", ".join(f":{name}" for name in base)
    _sql(
        "INSERT INTO curie.work_items (id, github_repository_id, github_issue_number, "
        "github_installation_id, agent_id, repo_full_name, conversation_id"
        + (f", {names}" if base else "")
        + ") VALUES (:id, 4401, :issue, 5501, :agent, 'acme-corp/acme-bot', :conversation"
        + (f", {values}" if base else "")
        + ")",
        {
            "id": item_id,
            "issue": next(_ISSUE),
            "agent": agent_id,
            "conversation": f"github:acme-corp/acme-bot#{item_id.hex[:6]}",
            **base,
        },
    )
    return item_id


def test_0072_follows_0071() -> None:
    script = ScriptDirectory.from_config(_config())
    revision = script.get_revision(REVISION)
    assert revision is not None
    assert revision.down_revision == BELOW


def test_0072_adds_nullable_base_columns_and_downgrade_drops_them(
    isolated_migration_db: None,
) -> None:
    config = _config()
    command.upgrade(config, BELOW)
    assert _columns() == {}
    legacy = _insert_work_item()
    try:
        command.upgrade(config, REVISION)
        columns = _columns()
        assert set(columns) == set(COLUMNS)
        assert all(column["data_type"] == "text" for column in columns.values())
        assert all(column["is_nullable"] == "YES" for column in columns.values())
        rows = _sql(
            "SELECT base_branch, base_source, base_commit, base_label_ignored "
            "FROM curie.work_items WHERE id = :id",
            {"id": legacy},
        )
        assert rows == [dict.fromkeys(COLUMNS)]

        recorded = _insert_work_item(
            base_branch="next", base_source="label", base_commit=SHA, base_label_ignored="main"
        )
        assert _sql(
            "SELECT base_branch, base_source FROM curie.work_items WHERE id = :id",
            {"id": recorded},
        ) == [{"base_branch": "next", "base_source": "label"}]
        _insert_work_item(base_branch="main", base_source="default", base_commit=SHA)

        command.downgrade(config, BELOW)
        assert _columns() == {}
    finally:
        command.upgrade(config, "head")


@pytest.mark.parametrize(
    "partial",
    [
        {"base_branch": "next"},
        {"base_branch": "next", "base_source": "label"},
        {"base_source": "default", "base_commit": SHA},
        {"base_commit": SHA},
        {"base_branch": "next", "base_source": "milestone", "base_commit": SHA},
    ],
)
def test_0072_refuses_a_partial_or_unknown_base(
    isolated_migration_db: None, partial: dict[str, str]
) -> None:
    command.upgrade(_config(), REVISION)

    with pytest.raises(IntegrityError):
        _insert_work_item(**partial)
