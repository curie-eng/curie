"""Migration selfimp_sched_0001 adds agent-created schedules (ADR 0248).

Expand-only: three new tables (`agent_schedules`, `agent_schedule_revisions`,
`agent_schedule_runs`) and four agent columns with defaults, so an agent that
predates the upgrade comes through with schedules off, UTC, 20 active and a
15-minute minimum. The downgrade to selfimp_skills_0001 removes all of it. The
`agent_schedules` check refuses a row with both or neither of cron and run_at.
"""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

import pytest
from _migration_support import (
    IsolatedMigrationDb,
    alembic_config,
    column_names,
    sql_dicts,
)
from alembic import command
from alembic.script import ScriptDirectory
from sqlalchemy.exc import IntegrityError

REVISION = "selfimp_sched_0001"
BELOW = "selfimp_skills_0001"
TABLES = ("agent_schedules", "agent_schedule_revisions", "agent_schedule_runs")
AGENT_COLUMNS = {
    "agent_schedules": ("boolean", "false"),
    "schedule_timezone": ("text", "'UTC'::text"),
    "schedule_max_active": ("integer", "20"),
    "schedule_min_interval_minutes": ("integer", "15"),
}


def _regclass(table: str) -> str | None:
    (row,) = sql_dicts("SELECT to_regclass(:name)::text AS name", {"name": f"curie.{table}"})
    return row["name"]


def _agent_columns() -> dict[str, dict[str, Any]]:
    rows = sql_dicts(
        "SELECT column_name, data_type, is_nullable, column_default "
        "FROM information_schema.columns "
        "WHERE table_schema = 'curie' AND table_name = 'agents' "
        "AND column_name = ANY(:names)",
        {"names": list(AGENT_COLUMNS)},
    )
    return {row["column_name"]: row for row in rows}


def _insert_agent() -> uuid.UUID:
    agent_id = uuid.uuid4()
    sql_dicts(
        "INSERT INTO curie.agents (id, name) VALUES (:id, :name)",
        {"id": agent_id, "name": f"msched-{agent_id.hex[:8]}"},
    )
    return agent_id


def _insert_schedule(agent_id: uuid.UUID, *, cron: str | None, run_at: str | None) -> None:
    sql_dicts(
        "INSERT INTO curie.agent_schedules (id, agent_id, revision, state, prompt, cron, run_at, "
        "timezone, destination_kind, destination_address, destination_adapter, created_by, "
        "created_source, created_conversation, created_turn, idempotency_key, effective_from, "
        "created_at, updated_at, updated_by) VALUES (:id, :agent, 1, 'active', 'hello', :cron, "
        "CAST(:run_at AS timestamptz), 'UTC', 'slack', 'C0EXAMPLE1', NULL, 'U0000SENDER', "
        "'agent', 'conv-1', 'evt-1#abcd1234', :key, now(), now(), now(), 'U0000SENDER')",
        {
            "id": uuid.uuid4(),
            "agent": agent_id,
            "cron": cron,
            "run_at": None if run_at is None else datetime.fromisoformat(run_at),
            "key": uuid.uuid4().hex,
        },
    )


def test_selfimp_sched_0001_follows_learned_skills() -> None:
    script = ScriptDirectory.from_config(alembic_config())
    revision = script.get_revision(REVISION)
    assert revision is not None
    assert revision.down_revision == BELOW


def test_upgrade_adds_tables_and_agent_columns_with_defaults(
    isolated_migration_db: IsolatedMigrationDb,
) -> None:
    config = alembic_config()
    isolated_migration_db.at(BELOW)
    for table in TABLES:
        assert _regclass(table) is None, table
    assert _agent_columns() == {}
    existing = _insert_agent()
    try:
        command.upgrade(config, REVISION)
        for table in TABLES:
            assert _regclass(table) is not None, table

        columns = _agent_columns()
        assert set(columns) == set(AGENT_COLUMNS)
        for name, (data_type, default) in AGENT_COLUMNS.items():
            assert columns[name]["data_type"] == data_type, name
            assert columns[name]["is_nullable"] == "NO", name
            assert columns[name]["column_default"] == default, name

        # An agent that predates the column comes through with the defaults.
        (row,) = sql_dicts(
            "SELECT agent_schedules, schedule_timezone, schedule_max_active, "
            "schedule_min_interval_minutes FROM curie.agents WHERE id = :id",
            {"id": existing},
        )
        assert row == {
            "agent_schedules": False,
            "schedule_timezone": "UTC",
            "schedule_max_active": 20,
            "schedule_min_interval_minutes": 15,
        }

        assert {
            "id",
            "agent_id",
            "revision",
            "state",
            "prompt",
            "cron",
            "run_at",
            "timezone",
            "destination_kind",
            "destination_address",
            "destination_adapter",
            "created_by",
            "created_source",
            "created_conversation",
            "created_turn",
            "idempotency_key",
            "effective_from",
            "created_at",
            "updated_at",
            "updated_by",
            "cancelled_at",
            "cancelled_by",
            "fired_at",
        } <= column_names("agent_schedules")
        assert {
            "schedule_id",
            "seq",
            "revision",
            "action",
            "state",
            "prompt",
            "cron",
            "run_at",
            "timezone",
            "author",
            "source",
            "conversation",
            "turn",
            "created_at",
        } <= column_names("agent_schedule_revisions")
        assert {
            "hook_run_id",
            "schedule_id",
            "agent_id",
            "revision",
            "prompt",
            "cron",
            "run_at",
            "timezone",
            "destination_kind",
            "destination_address",
            "destination_adapter",
            "created_at",
        } <= column_names("agent_schedule_runs")

        # agent_schedule_runs is keyed by hook run and cascades from hook_runs.
        (fk,) = sql_dicts(
            "SELECT confrelid::regclass::text AS target, confdeltype FROM pg_constraint "
            "WHERE conrelid = to_regclass('curie.agent_schedule_runs') AND contype = 'f' "
            "AND confrelid = to_regclass('curie.hook_runs')"
        )
        assert fk["target"] == "curie.hook_runs"
        # asyncpg returns the "char" column as bytes.
        assert fk["confdeltype"] in ("c", b"c")
        (pk,) = sql_dicts(
            "SELECT array_agg(a.attname ORDER BY a.attname)::text[] AS cols "
            "FROM pg_constraint c JOIN pg_attribute a "
            "ON a.attrelid = c.conrelid AND a.attnum = ANY(c.conkey) "
            "WHERE c.conrelid = to_regclass('curie.agent_schedule_revisions') "
            "AND c.contype = 'p'"
        )
        assert pk["cols"] == ["schedule_id", "seq"]
    finally:
        command.upgrade(config, "head")


def test_downgrade_removes_tables_and_agent_columns(
    isolated_migration_db: IsolatedMigrationDb,
) -> None:
    config = alembic_config()
    isolated_migration_db.at(REVISION)
    agent_id = _insert_agent()
    _insert_schedule(agent_id, cron="0 9 * * *", run_at=None)
    try:
        command.downgrade(config, BELOW)
        for table in TABLES:
            assert _regclass(table) is None, table
        assert _agent_columns() == {}
        # The agent row and the safety slice's tables outlive the downgrade.
        assert sql_dicts("SELECT id FROM curie.agents WHERE id = :id", {"id": agent_id})
        for table in ("schedule_daily_spend", "hook_runs", "schedule_controls"):
            assert _regclass(table) is not None, table
    finally:
        command.upgrade(config, "head")


def test_check_constraint_requires_exactly_one_of_cron_and_run_at(
    isolated_migration_db: IsolatedMigrationDb,
) -> None:
    isolated_migration_db.at(REVISION)
    agent_id = _insert_agent()
    # Liveness: each kind alone is accepted.
    _insert_schedule(agent_id, cron="0 9 * * *", run_at=None)
    _insert_schedule(agent_id, cron=None, run_at="2030-01-01T09:00:00+00:00")
    with pytest.raises(IntegrityError, match="violates check constraint"):
        _insert_schedule(agent_id, cron="0 9 * * *", run_at="2030-01-01T09:00:00+00:00")
    with pytest.raises(IntegrityError, match="violates check constraint"):
        _insert_schedule(agent_id, cron=None, run_at=None)
    (count,) = sql_dicts(
        "SELECT count(*) AS n FROM curie.agent_schedules WHERE agent_id = :a", {"a": agent_id}
    )
    assert count["n"] == 2


def test_idempotency_key_is_unique_per_agent(
    isolated_migration_db: IsolatedMigrationDb,
) -> None:
    isolated_migration_db.at(REVISION)
    first = _insert_agent()
    second = _insert_agent()
    statement = (
        "INSERT INTO curie.agent_schedules (id, agent_id, revision, state, prompt, cron, "
        "timezone, destination_kind, destination_address, created_by, created_source, "
        "idempotency_key, effective_from, created_at, updated_at) VALUES (:id, :agent, 1, "
        "'active', 'hello', '0 9 * * *', 'UTC', 'slack', 'C0EXAMPLE1', 'U1', 'agent', "
        "'same-key', now(), now(), now())"
    )
    sql_dicts(statement, {"id": uuid.uuid4(), "agent": first})
    # The same key on another agent is fine.
    sql_dicts(statement, {"id": uuid.uuid4(), "agent": second})
    with pytest.raises(IntegrityError, match="duplicate key"):
        sql_dicts(statement, {"id": uuid.uuid4(), "agent": first})
