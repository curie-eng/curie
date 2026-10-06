"""Migration 0055 allows per-agent execution deadlines up to 10800 s (#3071)."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta

import pytest
from _migration_support import IsolatedMigrationDb, alembic_config, sql_dicts
from alembic import command
from sqlalchemy.exc import DBAPIError

BELOW = "0054"
STARTED = datetime(2026, 9, 24, 12, tzinfo=UTC)


def _agent_column_exists() -> bool:
    return bool(
        sql_dicts(
            "SELECT 1 FROM information_schema.columns WHERE table_schema = 'curie' "
            "AND table_name = 'agents' AND column_name = 'execution_deadline_seconds'"
        )
    )


def _insert_running(deadline_seconds: int) -> None:
    agent_id, item_id = uuid.uuid4(), uuid.uuid4()
    sql_dicts(
        "INSERT INTO curie.agents (id, name) VALUES (:id, :name)",
        {"id": agent_id, "name": f"m0055-{agent_id.hex[:8]}"},
    )
    sql_dicts(
        "INSERT INTO curie.work_items (id, github_repository_id, github_issue_number, "
        "github_installation_id, agent_id, repo_full_name, conversation_id, "
        "version, next_sequence) VALUES (:id, 101, :issue, 202, :agent, "
        "'acme-corp/acme-bot', 'slack:C0EXAMPLE1:1700000000.000100', 2, 2)",
        {"id": item_id, "issue": int(agent_id.int % 100000) + 1, "agent": agent_id},
    )
    sql_dicts(
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
    isolated_migration_db: IsolatedMigrationDb,
) -> None:
    config = alembic_config()
    # The newest revision that still stores the GitHub WorkItem columns these
    # seeds write; 0090 replaces them with the tracker identity.
    isolated_migration_db.at("0089")
    assert _agent_column_exists()
    _insert_running(90)
    _insert_running(10800)
    _rejected(10801)
    _rejected(0)
    try:
        sql_dicts("DELETE FROM curie.execution_requests")
        command.downgrade(config, BELOW)
        assert not _agent_column_exists()
        _insert_running(1800)
        _rejected(90)
        _rejected(10800)
    finally:
        # A failed assertion must not leave this private database below head.
        sql_dicts("DELETE FROM curie.execution_requests")
        command.upgrade(config, "head")
