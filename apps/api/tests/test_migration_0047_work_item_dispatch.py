"""Migration 0047 adds WorkItem dispatch, ownership, and snapshot columns."""

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
BELOW = "0046"
REVISION = "0047"
REPO = "acme-corp/acme-bot"
CONVERSATION = "slack:C0EXAMPLE1:1700000000.000100"
STAMP = datetime(2026, 9, 18, 12, tzinfo=UTC)
OWNER_LOST_OBSERVATION = (
    "claims=claim-a sandboxes=sbx-a absent_at=2026-09-18T12:00:10+00:00 "
    "observer=worker-a"
)
DISPATCH_COLUMNS = {
    "dispatch_generation",
    "published_generation",
    "dispatch_not_before",
    "dispatch_owner",
    "dispatch_epoch",
    "dispatch_lease_expires_at",
    "acquired_generation",
    "acquire_owner",
    "acquire_expires_at",
    "capacity_deferrals",
    "last_deferral_reason",
    "execution_attempts",
    "runtime_owner",
    "runtime_epoch",
    "runtime_heartbeat_expires_at",
    "terminate_published_at",
    "runtime_claim_name",
    "runtime_sandbox_name",
    "objective",
    "requester",
    "reply_kind",
    "reply_address",
    "reply_conversation_id",
}


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


def _rejects(statement: str, params: dict[str, Any]) -> None:
    with pytest.raises(DBAPIError) as excinfo:
        _sql(statement, params)
    assert getattr(excinfo.value.orig, "sqlstate", None) in {
        "23502",
        "23503",
        "23505",
        "23514",
        "P0001",
    }


def _seed_agent() -> uuid.UUID:
    agent_id = uuid.uuid4()
    _sql(
        "INSERT INTO curie.agents (id, name) VALUES (:id, :name)",
        {"id": agent_id, "name": f"work-item-dispatch-{agent_id.hex[:8]}"},
    )
    return agent_id


def _insert_work_item(agent_id: uuid.UUID, **overrides: Any) -> uuid.UUID:
    values: dict[str, Any] = {
        "id": uuid.uuid4(),
        "github_repository_id": 1001,
        "github_issue_number": 37,
        "github_installation_id": 2001,
        "agent_id": agent_id,
        "repo_full_name": REPO,
        "conversation_id": CONVERSATION,
        "publication_lineage_id": None,
        "cancelled_at": None,
        "version": 1,
        "next_sequence": 1,
        "created_at": STAMP,
        "updated_at": STAMP,
    }
    values.update(overrides)
    _sql(
        "INSERT INTO curie.work_items "
        "(id, github_repository_id, github_issue_number, github_installation_id, "
        "agent_id, repo_full_name, conversation_id, publication_lineage_id, "
        "cancelled_at, version, next_sequence, created_at, updated_at) VALUES "
        "(:id, :github_repository_id, :github_issue_number, "
        ":github_installation_id, :agent_id, :repo_full_name, "
        ":conversation_id, :publication_lineage_id, :cancelled_at, :version, "
        ":next_sequence, :created_at, :updated_at)",
        values,
    )
    return values["id"]


def _request_values(work_item_id: uuid.UUID, **overrides: Any) -> dict[str, Any]:
    values: dict[str, Any] = {
        "id": uuid.uuid4(),
        "work_item_id": work_item_id,
        "sequence": 1,
        "status": "waiting",
        "wait_deadline": STAMP + timedelta(hours=1),
        "started_at": None,
        "execution_deadline": None,
        "terminal_at": None,
        "terminal_cause": None,
        "termination_observation": None,
        "version": 1,
        "created_at": STAMP,
        "updated_at": STAMP,
        "dispatch_generation": 1,
        "published_generation": None,
        "dispatch_epoch": 0,
        "capacity_deferrals": 0,
        "execution_attempts": 0,
        "runtime_epoch": 0,
        "objective": None,
        "requester": None,
        "reply_kind": None,
        "reply_address": None,
        "reply_conversation_id": None,
    }
    values.update(overrides)
    return values


_INSERT_REQUEST_SQL = (
    "INSERT INTO curie.execution_requests "
    "(id, work_item_id, sequence, status, wait_deadline, started_at, "
    "execution_deadline, terminal_at, terminal_cause, termination_observation, "
    "version, created_at, updated_at, dispatch_generation, published_generation, "
    "dispatch_epoch, capacity_deferrals, execution_attempts, runtime_epoch, "
    "objective, requester, reply_kind, reply_address, reply_conversation_id) "
    "VALUES "
    "(:id, :work_item_id, :sequence, :status, :wait_deadline, :started_at, "
    ":execution_deadline, :terminal_at, :terminal_cause, :termination_observation, "
    ":version, :created_at, :updated_at, :dispatch_generation, "
    ":published_generation, :dispatch_epoch, :capacity_deferrals, "
    ":execution_attempts, :runtime_epoch, :objective, :requester, :reply_kind, "
    ":reply_address, :reply_conversation_id)"
)


def _insert_request(work_item_id: uuid.UUID, **overrides: Any) -> uuid.UUID:
    values = _request_values(work_item_id, **overrides)
    _sql(_INSERT_REQUEST_SQL, values)
    return values["id"]


def _reject_request(work_item_id: uuid.UUID, **overrides: Any) -> None:
    values = _request_values(work_item_id, **overrides)
    _rejects(_INSERT_REQUEST_SQL, values)


def _snapshot() -> dict[str, str]:
    return {
        "objective": "Implement the work item",
        "requester": "U0REQUEST1",
        "reply_kind": "slack",
        "reply_address": "C0EXAMPLE1",
        "reply_conversation_id": "1700000000.000100",
    }


def _constraint_rows(table_name: str) -> list[dict[str, Any]]:
    return _sql(
        "SELECT c.conname AS name, pg_get_constraintdef(c.oid) AS definition "
        "FROM pg_constraint c "
        "JOIN pg_class r ON r.oid = c.conrelid "
        "JOIN pg_namespace n ON n.oid = r.relnamespace "
        "WHERE n.nspname = 'curie' AND r.relname = :table_name "
        "ORDER BY c.contype, c.conname",
        {"table_name": table_name},
    )


def _function_body() -> str:
    rows = _sql(
        "SELECT p.prosrc AS function_body "
        "FROM pg_proc p "
        "JOIN pg_namespace n ON n.oid = p.pronamespace "
        "WHERE n.nspname = 'curie' "
        "AND p.proname = 'enforce_execution_requests_update_invariants'"
    )
    assert len(rows) == 1
    return str(rows[0]["function_body"]).lower()


def test_0047_adds_dispatch_columns_checks_indexes_and_trigger_rules(
    isolated_migration_db: None,
) -> None:
    config = _config()
    command.upgrade(config, BELOW)
    before = {
        row["column_name"]
        for row in _sql(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = 'curie' AND table_name = 'execution_requests'"
        )
    }
    assert DISPATCH_COLUMNS.isdisjoint(before)

    command.upgrade(config, REVISION)

    columns = {
        row["column_name"]
        for row in _sql(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = 'curie' AND table_name = 'execution_requests'"
        )
    }
    assert DISPATCH_COLUMNS <= columns

    request_defs = [
        str(row["definition"]).lower() for row in _constraint_rows("execution_requests")
    ]
    check_defs = "\n".join(
        definition for definition in request_defs if definition.startswith("check")
    )
    for token in (
        "dispatch_generation",
        "published_generation",
        "acquired_generation",
        "capacity_deferrals",
        "dispatch_epoch",
        "runtime_epoch",
        "execution_attempts",
        "owner_lost",
        "btrim",
        "65536",
    ):
        assert token in check_defs
    assert "issue_cancelled" in check_defs
    assert "execution_deadline" in check_defs
    assert any(
        row["name"] == "execution_requests_state_shape_ck"
        for row in _constraint_rows("execution_requests")
    )

    indexes = [
        row["indexdef"].lower()
        for row in _sql(
            "SELECT indexdef FROM pg_indexes WHERE schemaname = 'curie' "
            "AND tablename = 'execution_requests'"
        )
    ]
    dispatch_due = [
        definition
        for definition in indexes
        if "ix_execution_requests_dispatch_due" in definition
    ]
    runtime_liveness = [
        definition
        for definition in indexes
        if "ix_execution_requests_runtime_liveness" in definition
    ]
    assert len(dispatch_due) == 1
    assert "dispatch_not_before" in dispatch_due[0]
    assert "waiting" in dispatch_due[0]
    assert len(runtime_liveness) == 1
    assert "runtime_heartbeat_expires_at" in runtime_liveness[0]
    assert "running" in runtime_liveness[0]
    assert "cancellation_requested" in runtime_liveness[0]

    body = _function_body()
    for token in (
        "objective",
        "requester",
        "reply_kind",
        "reply_address",
        "reply_conversation_id",
        "dispatch_generation",
        "runtime_epoch",
        "dispatch_epoch",
        "capacity_deferrals",
        "execution_attempts",
    ):
        assert token in body


def test_0047_rejects_invalid_attempts_generations_snapshots_and_causes(
    isolated_migration_db: None,
) -> None:
    command.upgrade(_config(), REVISION)
    work_item_id = _insert_work_item(_seed_agent())

    _reject_request(work_item_id, execution_attempts=1, started_at=None)
    _reject_request(
        work_item_id,
        sequence=2,
        status="running",
        started_at=STAMP,
        execution_deadline=STAMP + timedelta(seconds=1800),
        execution_attempts=2,
    )
    _reject_request(work_item_id, sequence=3, dispatch_generation=0)
    _reject_request(
        work_item_id,
        sequence=4,
        dispatch_generation=1,
        published_generation=2,
    )
    _reject_request(
        work_item_id,
        sequence=5,
        objective="only objective",
        requester=None,
        reply_kind=None,
        reply_address=None,
        reply_conversation_id=None,
    )
    _reject_request(
        work_item_id,
        sequence=6,
        status="failed",
        started_at=STAMP,
        execution_deadline=STAMP + timedelta(seconds=1800),
        terminal_at=STAMP + timedelta(seconds=10),
        terminal_cause="owner_lost",
        termination_observation=None,
        execution_attempts=1,
    )
    _reject_request(
        work_item_id,
        sequence=7,
        status="cancellation_requested",
        started_at=STAMP,
        execution_deadline=STAMP + timedelta(seconds=1800),
        terminal_cause="unknown_cause",
        execution_attempts=1,
    )

    waiting_id = _insert_request(work_item_id, sequence=8, **_snapshot())
    _rejects(
        "UPDATE curie.execution_requests SET objective = :objective WHERE id = :id",
        {"id": waiting_id, "objective": "rewritten objective"},
    )
    _sql(
        "UPDATE curie.execution_requests SET dispatch_generation = 2 WHERE id = :id",
        {"id": waiting_id},
    )
    _rejects(
        "UPDATE curie.execution_requests SET dispatch_generation = 1 WHERE id = :id",
        {"id": waiting_id},
    )
    _sql(
        "UPDATE curie.execution_requests SET dispatch_epoch = 3, runtime_epoch = 2 "
        "WHERE id = :id",
        {"id": waiting_id},
    )
    _rejects(
        "UPDATE curie.execution_requests SET dispatch_epoch = 2 WHERE id = :id",
        {"id": waiting_id},
    )
    _rejects(
        "UPDATE curie.execution_requests SET runtime_epoch = 1 WHERE id = :id",
        {"id": waiting_id},
    )

    owned_item_id = _insert_work_item(_seed_agent(), github_issue_number=38)
    _insert_request(
        owned_item_id,
        status="cancellation_requested",
        started_at=STAMP,
        execution_deadline=STAMP + timedelta(seconds=1800),
        terminal_cause="owner_lost",
        execution_attempts=1,
    )
    failed_item_id = _insert_work_item(_seed_agent(), github_issue_number=39)
    _insert_request(
        failed_item_id,
        status="failed",
        started_at=STAMP,
        execution_deadline=STAMP + timedelta(seconds=1800),
        terminal_at=STAMP + timedelta(seconds=10),
        terminal_cause="owner_lost",
        termination_observation=OWNER_LOST_OBSERVATION,
        execution_attempts=1,
    )


def test_0047_downgrade_refuses_owner_lost_and_round_trips_when_clean(
    isolated_migration_db: None,
) -> None:
    config = _config()
    command.upgrade(config, REVISION)
    work_item_id = _insert_work_item(_seed_agent())
    _insert_request(
        work_item_id,
        status="failed",
        started_at=STAMP,
        execution_deadline=STAMP + timedelta(seconds=1800),
        terminal_at=STAMP + timedelta(seconds=10),
        terminal_cause="owner_lost",
        termination_observation=OWNER_LOST_OBSERVATION,
        execution_attempts=1,
    )

    with pytest.raises(Exception, match="owner_lost") as excinfo:
        command.downgrade(config, BELOW)
    assert "owner_lost" in str(excinfo.value)

    _sql("DELETE FROM curie.execution_requests")
    waiting_id = _insert_request(work_item_id)
    command.downgrade(config, BELOW)
    assert _sql(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_schema = 'curie' AND table_name = 'execution_requests' "
        "AND column_name = 'dispatch_generation'"
    ) == []
    assert _sql(
        "SELECT id::text FROM curie.execution_requests WHERE id = :id",
        {"id": waiting_id},
    ) == [{"id": str(waiting_id)}]

    command.upgrade(config, REVISION)
    assert _sql(
        "SELECT dispatch_generation, execution_attempts "
        "FROM curie.execution_requests WHERE id = :id",
        {"id": waiting_id},
    ) == [{"dispatch_generation": 1, "execution_attempts": 0}]
    indexes = [
        row["indexname"]
        for row in _sql(
            "SELECT indexname FROM pg_indexes WHERE schemaname = 'curie' "
            "AND tablename = 'execution_requests'"
        )
    ]
    assert "ix_execution_requests_dispatch_due" in indexes
    assert "ix_execution_requests_runtime_liveness" in indexes
