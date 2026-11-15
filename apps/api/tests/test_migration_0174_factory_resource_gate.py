"""Migration 0174 adds the factory resource gate columns (#5427, ADR 0269).

The upgrade adds two nullable JSONB columns to ``execution_requests`` and
leaves every existing value alone. Each holds a JSON object or SQL NULL,
never a JSON null or a scalar. The downgrade refuses while any request carries
a gate, proceeds when only delivered records exist, and removes exactly what
0174 added.
"""

from __future__ import annotations

import json
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from _migration_support import (
    IsolatedMigrationDb,
    alembic_config,
    column_names,
    constraint_exists,
    sql_dicts,
    sql_rows,
    stamped_revision,
)
from alembic import command
from sqlalchemy.exc import IntegrityError

REVISION = "0174"
BELOW = "0172"
STAMP = datetime(2026, 11, 15, 12, 0, tzinfo=UTC)
REQUEST_COLUMNS = {"delivered_limits", "resource_gate"}
REQUEST_CHECKS = (
    "execution_requests_delivered_limits_ck",
    "execution_requests_resource_gate_ck",
)
REFUSAL = "factory_resource_gate_state_present"
RECORD = {
    "runtime_epoch": 1,
    "memory_bytes": 1073741824,
    "ephemeral_storage_bytes": None,
    "scratch_bytes": {"workspace": 1073741824},
    "max_turns": 40,
    "history_capacity_bytes": 16777216,
    "execution_seconds": 1800,
}
GATE = {
    "condition": "memory_limit",
    "dimension": "memory",
    "volume": None,
    "failed_limit": 1073741824,
    "source_request_id": "00000000-0000-0000-0000-000000000001",
    "source_runtime_epoch": 1,
}


def _agent() -> uuid.UUID:
    agent_id = uuid.uuid4()
    sql_rows(
        "INSERT INTO curie.agents (id, name) VALUES (:id, :name)",
        {"id": agent_id, "name": f"m0174-{agent_id.hex[:8]}"},
    )
    return agent_id


def _work_item(agent_id: uuid.UUID, issue: int) -> uuid.UUID:
    item_id = uuid.uuid4()
    sql_rows(
        "INSERT INTO curie.work_items "
        "(id, tracker_kind, tracker_host, tracker_scope_id, tracker_issue_id, "
        "code_host_kind, code_host_host, repository_project_id, "
        "code_host_installation_id, agent_id, repository_path, conversation_id, "
        "version, next_sequence, created_at, updated_at) VALUES "
        "(:id, 'github', 'github.com', '101', :issue, 'github', 'github.com', "
        "'101', 202, :agent, 'acme-corp/acme-bot', :conversation, 4, 4, "
        ":stamp, :stamp)",
        {
            "id": item_id,
            "issue": str(issue),
            "agent": agent_id,
            "conversation": f"m0174-{item_id.hex[:8]}",
            "stamp": STAMP,
        },
    )
    return item_id


def _request(item_id: uuid.UUID, sequence: int, **columns: Any) -> uuid.UUID:
    """One cancelled request row, which is never active, with ``columns`` applied.

    JSON values are bound as text and cast, so the row holds exactly the
    document the test names.
    """

    values: dict[str, Any] = {
        "id": uuid.uuid4(),
        "work_item_id": item_id,
        "sequence": sequence,
        "status": "cancelled",
        "terminal_at": STAMP,
        "terminal_cause": "issue_cancelled",
        "created_at": STAMP,
        "updated_at": STAMP,
    }
    casts = {}
    for name, value in columns.items():
        if name in REQUEST_COLUMNS:
            values[name] = value if value is None else json.dumps(value)
            casts[name] = f"CAST(:{name} AS jsonb)"
        else:
            values[name] = value
    names = ", ".join(values)
    binds = ", ".join(casts.get(name, f":{name}") for name in values)
    sql_rows(f"INSERT INTO curie.execution_requests ({names}) VALUES ({binds})", values)
    return values["id"]


def _catalog_has_gate_columns() -> bool:
    return REQUEST_COLUMNS <= column_names("execution_requests") and all(
        constraint_exists(name) for name in REQUEST_CHECKS
    )


def test_existing_rows_are_unchanged_and_new_columns_are_null(
    isolated_migration_db: IsolatedMigrationDb,
) -> None:
    isolated_migration_db.at(BELOW)
    item_id = _work_item(_agent(), 10)
    _request(item_id, 1)
    _request(
        item_id,
        2,
        status="waiting",
        terminal_at=None,
        terminal_cause=None,
        wait_deadline=STAMP + timedelta(hours=1),
        blocked_on="history_capacity",
    )
    before = sql_dicts("SELECT * FROM curie.execution_requests ORDER BY sequence")

    command.upgrade(alembic_config(), REVISION)

    assert stamped_revision() == REVISION
    assert _catalog_has_gate_columns()
    after = sql_dicts("SELECT * FROM curie.execution_requests ORDER BY sequence")
    assert [
        {key: value for key, value in row.items() if key not in REQUEST_COLUMNS} for row in after
    ] == before
    assert [(row["delivered_limits"], row["resource_gate"]) for row in after] == [
        (None, None),
        (None, None),
    ]


def test_columns_hold_objects_and_refuse_json_null_and_scalars(
    isolated_migration_db: IsolatedMigrationDb,
) -> None:
    isolated_migration_db.at(REVISION)
    item_id = _work_item(_agent(), 20)
    stored = _request(item_id, 1, delivered_limits=RECORD, resource_gate=GATE)
    (row,) = sql_dicts(
        "SELECT delivered_limits, resource_gate FROM curie.execution_requests WHERE id = :id",
        {"id": stored},
    )
    assert (row["delivered_limits"], row["resource_gate"]) == (RECORD, GATE)

    for column, check in zip(sorted(REQUEST_COLUMNS), REQUEST_CHECKS, strict=True):
        for sequence, bad in enumerate(("null", "42", '"text"', "[1]"), start=2):
            with pytest.raises(IntegrityError, match=check):
                sql_rows(
                    "INSERT INTO curie.execution_requests "
                    f"(id, work_item_id, sequence, status, terminal_at, terminal_cause, {column}) "
                    "VALUES (:id, :item, :sequence, 'cancelled', :stamp, 'issue_cancelled', "
                    "CAST(:bad AS jsonb))",
                    {
                        "id": uuid.uuid4(),
                        "item": item_id,
                        "sequence": sequence,
                        "stamp": STAMP,
                        "bad": bad,
                    },
                )


def test_downgrade_refuses_while_a_gate_exists_and_succeeds_without(
    isolated_migration_db: IsolatedMigrationDb,
) -> None:
    config = alembic_config()
    isolated_migration_db.at(REVISION)
    item_id = _work_item(_agent(), 30)
    _request(item_id, 1, delivered_limits=RECORD)
    gated = _request(item_id, 2, resource_gate=GATE)
    before = sql_dicts("SELECT * FROM curie.execution_requests ORDER BY sequence")

    with pytest.raises(RuntimeError, match=REFUSAL):
        command.downgrade(config, BELOW)

    # Refused before any DDL: the revision, schema and rows are as they were.
    assert stamped_revision() == REVISION
    assert _catalog_has_gate_columns()
    assert sql_dicts("SELECT * FROM curie.execution_requests ORDER BY sequence") == before

    # A delivered record alone does not hold the downgrade: losing it only
    # means a later limit failure admits no successor.
    sql_rows("DELETE FROM curie.execution_requests WHERE id = :id", {"id": gated})
    command.downgrade(config, BELOW)

    assert stamped_revision() == BELOW
    assert not REQUEST_COLUMNS & column_names("execution_requests")
    assert not any(constraint_exists(name) for name in REQUEST_CHECKS)
    assert len(sql_rows("SELECT id FROM curie.execution_requests")) == 1

    command.upgrade(config, REVISION)
    assert _catalog_has_gate_columns()
