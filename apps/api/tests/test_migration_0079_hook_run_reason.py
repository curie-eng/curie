"""Migration 0079 adds nullable curie.hook_runs.reason."""

from __future__ import annotations

import asyncio
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from alembic import command
from alembic.config import Config
from curie_api.config import get_settings
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

ALEMBIC_DIR = Path(__file__).resolve().parents[1] / "alembic"
BELOW = "0076"
REVISION = "0079"
SLOT = datetime(2026, 10, 5, 16, 0, tzinfo=UTC)
STARTED = datetime(2026, 10, 5, 16, 0, 5, tzinfo=UTC)


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


def _insert(agent_id: uuid.UUID, version_id: uuid.UUID, slot: datetime) -> uuid.UUID:
    run_id = uuid.uuid4()
    _sql(
        "INSERT INTO curie.hook_runs "
        "(id, agent_id, name, slot_utc, version_id, outcome, started_at, ended_at) "
        "VALUES (:id, :a, 'daily-digest', :slot, :v, 'failed', :started, :started)",
        {"id": run_id, "a": agent_id, "slot": slot, "v": version_id, "started": STARTED},
    )
    return run_id


def _reason(run_id: uuid.UUID) -> str | None:
    rows = _sql("SELECT reason FROM curie.hook_runs WHERE id = :id", {"id": run_id})
    return rows[0]["reason"]


def _has_reason_column() -> bool:
    rows = _sql(
        "SELECT 1 FROM information_schema.columns WHERE table_schema = 'curie' "
        "AND table_name = 'hook_runs' AND column_name = 'reason'"
    )
    return bool(rows)


def test_0079_leaves_existing_rows_without_a_reason(isolated_migration_db: None) -> None:
    config = _config()
    command.upgrade(config, BELOW)
    assert not _has_reason_column()
    agent_id, version_id = _seed_agent_version()
    run_id = _insert(agent_id, version_id, SLOT)

    command.upgrade(config, REVISION)
    assert _has_reason_column()
    assert _reason(run_id) is None


def test_0079_round_trip(isolated_migration_db: None) -> None:
    config = _config()
    command.upgrade(config, REVISION)
    assert _has_reason_column()
    try:
        command.downgrade(config, BELOW)
        assert not _has_reason_column()
    finally:
        command.upgrade(config, "head")
    assert _has_reason_column()
