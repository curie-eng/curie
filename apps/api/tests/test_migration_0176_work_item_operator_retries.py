"""Migration 0176 adds the operator retry records (#5431, ADR 0269).

The upgrade creates ``work_item_operator_retries`` with its key, foreign keys,
uniqueness and CHECKs, and leaves every existing row alone. Deleting a WorkItem
removes its records. The downgrade drops exactly the table.
"""

from __future__ import annotations

import json
import uuid
from datetime import UTC, datetime
from typing import Any

import pytest
from _migration_support import (
    IsolatedMigrationDb,
    alembic_config,
    constraint_exists,
    sql_dicts,
    sql_rows,
    stamped_revision,
)
from alembic import command
from sqlalchemy.exc import IntegrityError

REVISION = "0176"
BELOW = "0175"
STAMP = datetime(2026, 11, 15, 12, 0, tzinfo=UTC)
TABLE = "work_item_operator_retries"
CHECKS = (
    "work_item_operator_retries_actor_kind_ck",
    "work_item_operator_retries_reason_ck",
    "work_item_operator_retries_disposition_ck",
    "work_item_operator_retries_expected_version_ck",
    "work_item_operator_retries_facts_sha_ck",
    "work_item_operator_retries_result_ck",
    "work_item_operator_retries_successor_key",
)
DIGEST = "a" * 64


def _table_exists() -> bool:
    (row,) = sql_rows("SELECT to_regclass('curie.work_item_operator_retries') IS NOT NULL")
    return bool(row[0])


def _work_item(issue: int) -> uuid.UUID:
    agent_id, item_id = uuid.uuid4(), uuid.uuid4()
    sql_rows(
        "INSERT INTO curie.agents (id, name) VALUES (:id, :name)",
        {"id": agent_id, "name": f"m0176-{agent_id.hex[:8]}"},
    )
    sql_rows(
        "INSERT INTO curie.work_items "
        "(id, tracker_kind, tracker_host, tracker_scope_id, tracker_issue_id, "
        "code_host_kind, code_host_host, repository_project_id, "
        "code_host_installation_id, agent_id, repository_path, conversation_id, "
        "version, next_sequence, created_at, updated_at) VALUES "
        "(:id, 'github', 'github.com', '101', :issue, 'github', 'github.com', "
        "'101', 202, :agent, 'acme-corp/acme-bot', :conversation, 4, 3, "
        ":stamp, :stamp)",
        {
            "id": item_id,
            "issue": str(issue),
            "agent": agent_id,
            "conversation": f"m0176-{item_id.hex[:8]}",
            "stamp": STAMP,
        },
    )
    return item_id


def _request(item_id: uuid.UUID, sequence: int) -> uuid.UUID:
    """One cancelled request row, which is never active."""

    request_id = uuid.uuid4()
    sql_rows(
        "INSERT INTO curie.execution_requests "
        "(id, work_item_id, sequence, status, terminal_at, terminal_cause, "
        "created_at, updated_at) VALUES "
        "(:id, :item, :sequence, 'cancelled', :stamp, 'issue_cancelled', :stamp, :stamp)",
        {"id": request_id, "item": item_id, "sequence": sequence, "stamp": STAMP},
    )
    return request_id


def _record(
    item_id: uuid.UUID, predecessor: uuid.UUID, successor: uuid.UUID, **overrides: Any
) -> uuid.UUID:
    values: dict[str, Any] = {
        "request_id": uuid.uuid4(),
        "work_item_id": item_id,
        "actor_kind": "platform_key",
        "actor_subject": None,
        "reason": "The provider outage is over",
        "expected_version": 3,
        "facts_sha256": DIGEST,
        "disposition": "implementation",
        "predecessor_request_id": predecessor,
        "successor_request_id": successor,
        "result": json.dumps({"replayed": False}),
    }
    values.update(overrides)
    sql_rows(
        "INSERT INTO curie.work_item_operator_retries "
        "(request_id, work_item_id, actor_kind, actor_subject, reason, expected_version, "
        "facts_sha256, disposition, predecessor_request_id, successor_request_id, result) "
        "VALUES (:request_id, :work_item_id, :actor_kind, :actor_subject, :reason, "
        ":expected_version, :facts_sha256, :disposition, :predecessor_request_id, "
        ":successor_request_id, CAST(:result AS jsonb))",
        values,
    )
    request_id = values["request_id"]
    assert isinstance(request_id, uuid.UUID)
    return request_id


def test_upgrade_creates_the_table_and_leaves_rows_alone(
    isolated_migration_db: IsolatedMigrationDb,
) -> None:
    isolated_migration_db.at(BELOW)
    item_id = _work_item(10)
    _request(item_id, 1)
    before = sql_dicts("SELECT * FROM curie.execution_requests ORDER BY sequence")
    assert not _table_exists()

    command.upgrade(alembic_config(), REVISION)

    assert stamped_revision() == REVISION
    assert _table_exists()
    assert all(constraint_exists(name) for name in CHECKS)
    assert sql_dicts("SELECT * FROM curie.execution_requests ORDER BY sequence") == before
    assert sql_rows("SELECT count(*) FROM curie.work_item_operator_retries")[0][0] == 0


@pytest.mark.parametrize(
    ("override", "constraint"),
    [
        ({"actor_kind": "state_token"}, "work_item_operator_retries_actor_kind_ck"),
        ({"reason": ""}, "work_item_operator_retries_reason_ck"),
        ({"reason": "x" * 501}, "work_item_operator_retries_reason_ck"),
        ({"disposition": "retry_backoff"}, "work_item_operator_retries_disposition_ck"),
        ({"expected_version": 0}, "work_item_operator_retries_expected_version_ck"),
        ({"facts_sha256": "not-a-digest"}, "work_item_operator_retries_facts_sha_ck"),
        ({"result": "[]"}, "work_item_operator_retries_result_ck"),
    ],
)
def test_checks_refuse_malformed_records(
    isolated_migration_db: IsolatedMigrationDb, override: dict[str, Any], constraint: str
) -> None:
    isolated_migration_db.at(REVISION)
    item_id = _work_item(20)
    predecessor, successor = _request(item_id, 1), _request(item_id, 2)
    with pytest.raises(IntegrityError, match=constraint):
        _record(item_id, predecessor, successor, **override)
    # The same record with valid values stores.
    _record(item_id, predecessor, successor, reason="x" * 500)


def test_key_and_successor_are_unique_and_a_deleted_work_item_cascades(
    isolated_migration_db: IsolatedMigrationDb,
) -> None:
    isolated_migration_db.at(REVISION)
    item_id = _work_item(30)
    predecessor, successor, other = _request(item_id, 1), _request(item_id, 2), _request(item_id, 3)
    key = _record(item_id, predecessor, successor)
    with pytest.raises(IntegrityError, match="work_item_operator_retries_pkey"):
        _record(item_id, predecessor, other, request_id=key)
    with pytest.raises(IntegrityError, match="work_item_operator_retries_successor_key"):
        _record(item_id, predecessor, successor)
    with pytest.raises(IntegrityError, match="work_item_operator_retries_work_item_id_fkey"):
        _record(uuid.uuid4(), predecessor, other)

    sql_rows("DELETE FROM curie.work_items WHERE id = :id", {"id": item_id})
    assert sql_rows("SELECT count(*) FROM curie.work_item_operator_retries")[0][0] == 0


def test_downgrade_drops_the_table_and_upgrade_restores_it(
    isolated_migration_db: IsolatedMigrationDb,
) -> None:
    config = alembic_config()
    isolated_migration_db.at(REVISION)
    item_id = _work_item(40)
    predecessor, successor = _request(item_id, 1), _request(item_id, 2)
    _record(item_id, predecessor, successor)
    requests = sql_dicts("SELECT * FROM curie.execution_requests ORDER BY sequence")

    command.downgrade(config, BELOW)

    assert stamped_revision() == BELOW
    assert not _table_exists()
    assert not any(constraint_exists(name) for name in CHECKS)
    # The requests a record named stay as they were.
    assert sql_dicts("SELECT * FROM curie.execution_requests ORDER BY sequence") == requests

    command.upgrade(config, REVISION)
    assert _table_exists()
