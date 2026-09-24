"""Migration 0055 allows per-agent execution deadlines up to 10800 s (#3071)."""

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
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import create_async_engine

ALEMBIC_DIR = Path(__file__).resolve().parents[1] / "alembic"
BELOW = "0054"
STARTED = datetime(2026, 9, 24, 12, tzinfo=UTC)


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


def _agent_column_exists() -> bool:
    return bool(
        _sql(
            "SELECT 1 FROM information_schema.columns WHERE table_schema = 'curie' "
            "AND table_name = 'agents' AND column_name = 'execution_deadline_seconds'"
        )
    )


def _insert_running(deadline_seconds: int) -> None:
    agent_id, item_id = uuid.uuid4(), uuid.uuid4()
    _sql(
        "INSERT INTO curie.agents (id, name) VALUES (:id, :name)",
        {"id": agent_id, "name": f"m0055-{agent_id.hex[:8]}"},
    )
    _sql(
        "INSERT INTO curie.work_items (id, github_repository_id, github_issue_number, "
        "github_installation_id, agent_id, repo_full_name, conversation_id, "
        "version, next_sequence) VALUES (:id, 101, :issue, 202, :agent, "
        "'acme-corp/acme-bot', 'slack:C0EXAMPLE1:1700000000.000100', 2, 2)",
        {"id": item_id, "issue": int(agent_id.int % 100000) + 1, "agent": agent_id},
    )
    _sql(
        "INSERT INTO curie.execution_requests (id, work_item_id, sequence, status, "
        "wait_deadline, started_at, execution_deadline, version, execution_attempts) "
        "VALUES (:id, :item, 1, 'running', :wait, :started, :deadline, 2, 1)",
        {
            "id": uuid.uuid4(),
            "item": item_id,
            "wait": STARTED - timedelta(seconds=1),
            "started": STARTED,
            "deadline": STARTED + timedelta(seconds=deadline_seconds),
        },
    )


def _rejected(deadline_seconds: int) -> None:
    with pytest.raises(DBAPIError) as excinfo:
        _insert_running(deadline_seconds)
    assert getattr(excinfo.value.orig, "sqlstate", None) == "23514"


def test_0055_relaxes_the_deadline_check_and_downgrade_restores_it(
    isolated_migration_db: None,
) -> None:
    config = _config()
    command.upgrade(config, "head")
    assert _agent_column_exists()
    _insert_running(90)
    _insert_running(10800)
    _rejected(10801)
    _rejected(0)
    try:
        _sql("DELETE FROM curie.execution_requests")
        command.downgrade(config, BELOW)
        assert not _agent_column_exists()
        _insert_running(1800)
        _rejected(90)
        _rejected(10800)
    finally:
        # A failed assertion must not leave this private database below head.
        _sql("DELETE FROM curie.execution_requests")
        command.upgrade(config, "head")
