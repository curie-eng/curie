"""Migration 0149 records webhook provenance and a wake-key generation.

The source check gains webhook and keeps schedule and manual. The wake-key
table stores only a generation. A downgrade that would drop either is refused,
and a clean downgrade returns to the 0148 check.
"""

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
from sqlalchemy.exc import DBAPIError, IntegrityError
from sqlalchemy.ext.asyncio import create_async_engine

API_DIR = Path(__file__).resolve().parents[1]
BELOW = "0148"
REVISION = "0149"
MINUTE = datetime(2026, 11, 7, 16, 0, tzinfo=UTC)
HOOK = "queue-ready"
WAKE_COLUMNS = {"agent_id", "hook", "generation", "created_at", "updated_at"}
FORBIDDEN_WAKE_COLUMNS = {"secret", "key", "token", "enabled", "disabled"}


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


def _insert_run(
    agent_id: uuid.UUID, version_id: uuid.UUID, slot: datetime, source: str
) -> uuid.UUID:
    run_id = uuid.uuid4()
    _sql(
        "INSERT INTO curie.hook_runs "
        "(id, agent_id, name, slot_utc, version_id, source, started_at) "
        "VALUES (:id, :agent, 'daily-digest', :slot, :version, :source, :slot)",
        {
            "id": run_id,
            "agent": agent_id,
            "slot": slot,
            "version": version_id,
            "source": source,
        },
    )
    return run_id


def _runs() -> list[dict[str, Any]]:
    return _sql(
        "SELECT id, agent_id, name, slot_utc, version_id, source, outcome, started_at "
        "FROM curie.hook_runs ORDER BY slot_utc, id"
    )


def _wake_keys() -> list[dict[str, Any]]:
    return _sql(
        "SELECT agent_id, hook, generation FROM curie.hook_wake_keys ORDER BY agent_id, hook"
    )


def _revision() -> str:
    return str(_sql("SELECT version_num FROM curie.alembic_version")[0]["version_num"])


def _wake_column_names() -> set[str]:
    rows = _sql(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_schema = 'curie' AND table_name = 'hook_wake_keys'"
    )
    return {str(row["column_name"]) for row in rows}


def test_0149_preserves_history_accepts_webhook_and_refuses_a_lossy_downgrade(
    isolated_migration_db: None,
) -> None:
    config = _config()
    agent_id: uuid.UUID | None = None
    try:
        command.upgrade(config, BELOW)
        agent_id, version_id = _seed_agent_version()
        schedule_id = _insert_run(agent_id, version_id, MINUTE, "schedule")
        manual_id = _insert_run(agent_id, version_id, MINUTE + timedelta(seconds=1), "manual")
        before = _runs()
        assert [row["source"] for row in before] == ["schedule", "manual"]
        assert [row["id"] for row in before] == [schedule_id, manual_id]

        command.upgrade(config, REVISION)
        assert _runs() == before
        assert _revision() == REVISION

        webhook_id = _insert_run(agent_id, version_id, MINUTE + timedelta(seconds=2), "webhook")
        assert _sql("SELECT source FROM curie.hook_runs WHERE id = :id", {"id": webhook_id}) == [
            {"source": "webhook"}
        ]
        with pytest.raises(IntegrityError) as unknown:
            _insert_run(agent_id, version_id, MINUTE + timedelta(seconds=3), "unknown")
        assert "hook_runs_source_ck" in str(unknown.value)

        _sql(
            "INSERT INTO curie.hook_wake_keys (agent_id, hook) VALUES (:agent, :hook)",
            {"agent": agent_id, "hook": HOOK},
        )
        stored = _sql(
            "SELECT generation FROM curie.hook_wake_keys WHERE agent_id = :agent AND hook = :hook",
            {"agent": agent_id, "hook": HOOK},
        )
        assert stored == [{"generation": 1}]
        columns = _wake_column_names()
        assert columns == WAKE_COLUMNS
        assert columns.isdisjoint(FORBIDDEN_WAKE_COLUMNS)
        with pytest.raises(IntegrityError) as zero:
            _sql(
                "INSERT INTO curie.hook_wake_keys (agent_id, hook, generation) "
                "VALUES (:agent, :hook, 0)",
                {"agent": agent_id, "hook": "generation-zero"},
            )
        assert getattr(zero.value.orig, "sqlstate", None) == "23514"
        maximum = 2**63 - 1
        _sql(
            "INSERT INTO curie.hook_wake_keys (agent_id, hook, generation) "
            "VALUES (:agent, :hook, :generation)",
            {"agent": agent_id, "hook": "generation-max", "generation": maximum},
        )
        assert _sql(
            "SELECT generation FROM curie.hook_wake_keys "
            "WHERE agent_id = :agent AND hook = :hook",
            {"agent": agent_id, "hook": "generation-max"},
        ) == [{"generation": maximum}]
        with pytest.raises(DBAPIError) as overflow:
            _sql(
                "INSERT INTO curie.hook_wake_keys (agent_id, hook, generation) "
                "VALUES (:agent, 'generation-overflow', 9223372036854775808)",
                {"agent": agent_id},
            )
        assert getattr(overflow.value.orig, "sqlstate", None) == "22003"
        _sql("DELETE FROM curie.hook_wake_keys WHERE hook = 'generation-max'")
        assert _wake_keys() == [{"agent_id": agent_id, "hook": HOOK, "generation": 1}]

        runs_with_webhook = _runs()
        keys_with_webhook = _wake_keys()
        with pytest.raises(RuntimeError):
            command.downgrade(config, BELOW)
        assert _runs() == runs_with_webhook
        assert _wake_keys() == keys_with_webhook
        assert _revision() == REVISION

        _sql("DELETE FROM curie.hook_runs WHERE source = 'webhook'")
        runs_without_webhook = _runs()
        assert webhook_id not in {row["id"] for row in runs_without_webhook}
        assert [row["source"] for row in runs_without_webhook] == ["schedule", "manual"]
        with pytest.raises(RuntimeError):
            command.downgrade(config, BELOW)
        assert _runs() == runs_without_webhook
        assert _wake_keys() == keys_with_webhook
        assert _revision() == REVISION

        _sql("DELETE FROM curie.hook_wake_keys")
        command.downgrade(config, BELOW)
        assert _revision() == BELOW
        assert _sql("SELECT to_regclass('curie.hook_wake_keys') AS name") == [{"name": None}]
        assert _runs() == runs_without_webhook
        with pytest.raises(IntegrityError) as restored:
            _insert_run(agent_id, version_id, MINUTE + timedelta(seconds=4), "webhook")
        assert "hook_runs_source_ck" in str(restored.value)
        assert [row["id"] for row in _runs()] == [schedule_id, manual_id]
    finally:
        # A refused downgrade must leave this private database usable at head.
        command.upgrade(config, "head")

    assert agent_id is not None
    _sql(
        "INSERT INTO curie.hook_wake_keys (agent_id, hook) VALUES (:agent, :hook)",
        {"agent": agent_id, "hook": HOOK},
    )
    _sql("DELETE FROM curie.agents WHERE id = :id", {"id": agent_id})
    assert _sql(
        "SELECT count(*) AS count FROM curie.hook_wake_keys WHERE agent_id = :agent",
        {"agent": agent_id},
    ) == [{"count": 0}]
