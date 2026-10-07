"""Migration 0080 separates scheduled history from manual hook fires (#4010)."""

from __future__ import annotations

import asyncio
import json
import uuid
from datetime import UTC, datetime
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

API_DIR = Path(__file__).resolve().parents[1]
BELOW = "0079"
REVISION = "0080"
MINUTE = datetime(2026, 10, 5, 16, 0, tzinfo=UTC)


def _config() -> Config:
    config = Config()
    config.set_main_option("script_location", str(API_DIR / "alembic"))
    return config


def _sql(statement: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    async def run() -> list[dict[str, Any]]:
        engine = create_async_engine(get_settings().database_url)
        try:
            async with engine.begin() as connection:
                result = await connection.execute(text(statement), params or {})
                return [dict(row) for row in result.mappings()] if result.returns_rows else []
        finally:
            await engine.dispose()

    return asyncio.run(run())


def _seed_agent_version() -> tuple[uuid.UUID, uuid.UUID]:
    agent_id, version_id = uuid.uuid4(), uuid.uuid4()
    _sql(
        "INSERT INTO curie.agents (id, name) VALUES (:id, :name)",
        {"id": agent_id, "name": f"acme-bot-{agent_id.hex[:8]}"},
    )
    _sql(
        "INSERT INTO curie.agent_versions "
        "(id, agent_id, version_label, bundle_ref, created_by) "
        "VALUES (:id, :agent_id, 'v1', NULL, 'acme')",
        {"id": version_id, "agent_id": agent_id},
    )
    return agent_id, version_id


def _insert(agent_id: uuid.UUID, version_id: uuid.UUID, slot: datetime, **fields: Any) -> uuid.UUID:
    run_id = uuid.uuid4()
    _sql(
        "INSERT INTO curie.hook_runs "
        "(id, agent_id, name, slot_utc, version_id, outcome, reason, started_at, ended_at"
        + (", source" if fields else "")
        + ") VALUES (:id, :agent, 'daily-digest', :slot, :version, 'failed', "
        "'turn_error', :slot, :slot" + (", :source" if fields else "") + ")",
        {"id": run_id, "agent": agent_id, "version": version_id, "slot": slot, **fields},
    )
    return run_id


def _source(run_id: uuid.UUID) -> str:
    return str(
        _sql("SELECT source FROM curie.hook_runs WHERE id = :id", {"id": run_id})[0]["source"]
    )


def _column() -> list[dict[str, Any]]:
    return _sql(
        "SELECT data_type, is_nullable, column_default FROM information_schema.columns "
        "WHERE table_schema = 'curie' AND table_name = 'hook_runs' AND column_name = 'source'"
    )


def test_0080_is_expand_after_0079_and_precedes_the_single_candidate_head() -> None:
    script = ScriptDirectory.from_config(_config())
    revision = script.get_revision(REVISION)
    assert revision is not None
    assert revision.down_revision == BELOW
    assert script.get_heads() == ["0092"]
    candidate = script.get_revision("0081")
    assert candidate is not None
    assert candidate.down_revision == REVISION
    kinds = json.loads((API_DIR / "src/curie_api/revision_kinds.json").read_text())
    assert kinds[REVISION] == "expand"


def test_0080_backfills_whole_minutes_and_fractional_slots_and_round_trips(
    isolated_migration_db: None,
) -> None:
    config = _config()
    command.upgrade(config, BELOW)
    assert _column() == []
    agent_id, version_id = _seed_agent_version()
    slots = (
        (MINUTE, "schedule"),
        (MINUTE.replace(second=1), "manual"),
        (MINUTE.replace(microsecond=1), "manual"),
    )
    recorded = [(_insert(agent_id, version_id, slot), expected) for slot, expected in slots]
    command.upgrade(config, REVISION)
    column = _column()[0]
    assert column["data_type"] == "text"
    assert column["is_nullable"] == "NO"
    assert "schedule" in column["column_default"]
    assert [_source(run_id) for run_id, _ in recorded] == [expected for _, expected in recorded]

    command.downgrade(config, BELOW)
    assert _column() == []
    assert _sql("SELECT count(*) AS count FROM curie.hook_runs")[0]["count"] == len(recorded)
    assert _sql("SELECT DISTINCT reason FROM curie.hook_runs") == [{"reason": "turn_error"}]
    command.upgrade(config, REVISION)
    assert [_source(run_id) for run_id, _ in recorded] == [expected for _, expected in recorded]


def test_0080_old_writer_defaults_to_schedule_and_explicit_manual_is_accepted(
    isolated_migration_db: None,
) -> None:
    command.upgrade(_config(), REVISION)
    agent_id, version_id = _seed_agent_version()
    # An older worker omits source, even if a row carries a fractional timestamp.
    older_writer = _insert(agent_id, version_id, MINUTE.replace(second=5))
    manual = _insert(agent_id, version_id, MINUTE.replace(second=6), source="manual")
    assert _source(older_writer) == "schedule"
    assert _source(manual) == "manual"


@pytest.mark.parametrize("source", [None, "cron", "unknown"])
def test_0080_rejects_null_or_unknown_source(
    isolated_migration_db: None, source: str | None
) -> None:
    command.upgrade(_config(), REVISION)
    agent_id, version_id = _seed_agent_version()
    with pytest.raises(IntegrityError):
        _insert(agent_id, version_id, MINUTE, source=source)
