"""Migration 0062 gives curie.hook_runs a claim lease and the reclaimed outcome."""

from __future__ import annotations

import asyncio
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
from alembic import command
from alembic.config import Config
from curie_api.config import get_settings
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import create_async_engine

ALEMBIC_DIR = Path(__file__).resolve().parents[1] / "alembic"
BELOW = "0061"
REVISION = "0062"
SLOT = datetime(2026, 9, 22, 16, 0, tzinfo=UTC)
STARTED = datetime(2026, 9, 22, 16, 0, 5, tzinfo=UTC)


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


def _insert(
    agent_id: uuid.UUID, version_id: uuid.UUID, slot: datetime, outcome: str | None
) -> uuid.UUID:
    run_id = uuid.uuid4()
    _sql(
        "INSERT INTO curie.hook_runs "
        "(id, agent_id, name, slot_utc, version_id, outcome, started_at) "
        "VALUES (:id, :a, 'daily-digest', :slot, :v, :outcome, :started)",
        {
            "id": run_id,
            "a": agent_id,
            "slot": slot,
            "v": version_id,
            "outcome": outcome,
            "started": STARTED,
        },
    )
    return run_id


def _lease(run_id: uuid.UUID) -> datetime | None:
    rows = _sql("SELECT lease_expires_at FROM curie.hook_runs WHERE id = :id", {"id": run_id})
    return rows[0]["lease_expires_at"]


def _has_lease_column() -> bool:
    rows = _sql(
        "SELECT 1 FROM information_schema.columns WHERE table_schema = 'curie' "
        "AND table_name = 'hook_runs' AND column_name = 'lease_expires_at'"
    )
    return bool(rows)


def test_0062_leaves_existing_claims_leaseless_and_allows_reclaimed(
    isolated_migration_db: None,
) -> None:
    config = _config()
    command.upgrade(config, BELOW)
    agent_id, version_id = _seed_agent_version()
    open_run = _insert(agent_id, version_id, SLOT, None)
    closed_run = _insert(agent_id, version_id, SLOT + timedelta(days=1), "ran")
    with pytest.raises(IntegrityError):
        _insert(agent_id, version_id, SLOT + timedelta(days=2), "reclaimed")

    command.upgrade(config, REVISION)
    # No backfill: the scheduler holds a leaseless claim for one configured
    # lease from its start, so no fixed interval here can undercut the budget.
    assert _lease(open_run) is None
    assert _lease(closed_run) is None
    _insert(agent_id, version_id, SLOT + timedelta(days=2), "reclaimed")
    with pytest.raises(IntegrityError) as excinfo:
        _insert(agent_id, version_id, SLOT + timedelta(days=3), "unknown")
    assert "hook_runs_outcome_ck" in str(excinfo.value)


def test_0062_round_trip(isolated_migration_db: None) -> None:
    config = _config()
    command.upgrade(config, REVISION)
    assert _has_lease_column()
    try:
        command.downgrade(config, BELOW)
        assert not _has_lease_column()
    finally:
        # A failed assertion must not leave this private database below head.
        command.upgrade(config, "head")
    assert _has_lease_column()
