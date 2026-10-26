"""Migration 0136 allows execution deadlines up to 43200 s (#5067)."""

from __future__ import annotations

import re
import uuid
from datetime import UTC, datetime, timedelta

import pytest
from _migration_support import IsolatedMigrationDb, alembic_config, sql_dicts, stamped_revision
from alembic import command
from sqlalchemy.exc import DBAPIError

BELOW = "0131"
STARTED = datetime(2026, 10, 26, 12, tzinfo=UTC)


def _insert_agent(deadline_seconds: int | None) -> uuid.UUID:
    agent_id = uuid.uuid4()
    sql_dicts(
        "INSERT INTO curie.agents (id, name, execution_deadline_seconds) "
        "VALUES (:id, :name, :seconds)",
        {"id": agent_id, "name": f"m0136-{agent_id.hex[:8]}", "seconds": deadline_seconds},
    )
    return agent_id


def _insert_running(deadline_seconds: int) -> uuid.UUID:
    agent_id, item_id, request_id = _insert_agent(None), uuid.uuid4(), uuid.uuid4()
    sql_dicts(
        "INSERT INTO curie.work_items "
        "(id, tracker_kind, tracker_host, tracker_scope_id, tracker_issue_id, "
        "code_host_kind, code_host_host, repository_project_id, "
        "code_host_installation_id, agent_id, repository_path, conversation_id, "
        "version, next_sequence) VALUES "
        "(:id, 'github', 'github.com', '101', :issue, 'github', 'github.com', "
        "'101', 202, :agent, 'acme-corp/acme-bot', "
        "'slack:C0EXAMPLE1:1700000000.000100', 2, 2)",
        {"id": item_id, "issue": str(int(agent_id.int % 100000) + 1), "agent": agent_id},
    )
    sql_dicts(
        "INSERT INTO curie.execution_requests (id, work_item_id, sequence, status, "
        "wait_deadline, started_at, execution_deadline, version, execution_attempts) "
        "VALUES (:id, :item, 1, 'running', :wait, :started, :deadline, 2, 1)",
        {
            "id": request_id,
            "item": item_id,
            "wait": STARTED - timedelta(seconds=1),
            "started": STARTED,
            "deadline": STARTED + timedelta(seconds=deadline_seconds),
        },
    )
    return request_id


def _agent_rejected(deadline_seconds: int) -> None:
    with pytest.raises(DBAPIError) as excinfo:
        _insert_agent(deadline_seconds)
    assert getattr(excinfo.value.orig, "sqlstate", None) == "23514"


def _request_rejected(deadline_seconds: int) -> None:
    with pytest.raises(DBAPIError) as excinfo:
        _insert_running(deadline_seconds)
    assert getattr(excinfo.value.orig, "sqlstate", None) == "23514"


def _clear_long_deadlines() -> None:
    sql_dicts("DELETE FROM curie.execution_requests")
    sql_dicts("UPDATE curie.agents SET execution_deadline_seconds = NULL")


def test_0136_raises_both_deadline_checks_and_downgrade_refuses_longer_rows(
    isolated_migration_db: IsolatedMigrationDb,
) -> None:
    config = alembic_config()
    isolated_migration_db.at("0136")
    _insert_agent(43200)
    _insert_running(43200)
    _agent_rejected(43201)
    _request_rejected(43201)
    # Distinct counts so the error text can be checked for each: 2 agents and
    # 3 requests are past the old three-hour bound.
    long_agent = _insert_agent(20000)
    _insert_running(20000)
    _insert_running(11000)
    try:
        agents_before = sql_dicts(
            "SELECT id, execution_deadline_seconds FROM curie.agents "
            "WHERE execution_deadline_seconds IS NOT NULL ORDER BY id"
        )
        requests_before = sql_dicts(
            "SELECT id, started_at, execution_deadline FROM curie.execution_requests ORDER BY id"
        )
        assert len(agents_before) == 3
        assert len(requests_before) == 4
        assert long_agent in {row["id"] for row in agents_before}

        with pytest.raises(RuntimeError) as excinfo:
            command.downgrade(config, BELOW)
        message = str(excinfo.value)
        assert re.search(r"\b3\b", message), message
        assert re.search(r"\b4\b", message), message

        # The refusal alters no data and leaves the revision where it was.
        assert (
            sql_dicts(
                "SELECT id, execution_deadline_seconds FROM curie.agents "
                "WHERE execution_deadline_seconds IS NOT NULL ORDER BY id"
            )
            == agents_before
        )
        assert (
            sql_dicts(
                "SELECT id, started_at, execution_deadline "
                "FROM curie.execution_requests ORDER BY id"
            )
            == requests_before
        )
        assert stamped_revision() == "0136"

        _clear_long_deadlines()
        command.downgrade(config, BELOW)
        assert stamped_revision() == BELOW
        _insert_agent(10800)
        _insert_running(10800)
        _agent_rejected(10801)
        _request_rejected(10801)
    finally:
        # A failed assertion must not leave this private database below head.
        _clear_long_deadlines()
        command.upgrade(config, "head")


def test_0136_downgrade_succeeds_when_every_row_is_within_three_hours(
    isolated_migration_db: IsolatedMigrationDb,
) -> None:
    config = alembic_config()
    isolated_migration_db.at("0136")
    _insert_agent(10800)
    _insert_running(10800)
    try:
        command.downgrade(config, BELOW)
        assert stamped_revision() == BELOW
        assert sql_dicts(
            "SELECT 1 FROM curie.agents WHERE execution_deadline_seconds = 10800"
        )
        assert len(sql_dicts("SELECT 1 FROM curie.execution_requests")) == 1
    finally:
        _clear_long_deadlines()
        command.upgrade(config, "head")
