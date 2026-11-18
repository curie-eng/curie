"""Migration 0182 keeps new-only automatic retries out of older claims (ADR 0269).

The upgrade adds ``execution_requests.guarded_dispatch_not_before`` with a
CHECK that a guarded row's ``dispatch_not_before`` is at or past its waiting
deadline, a trigger that keeps that true for a writer that does not know the
column, and a partial index for the claim. Existing rows are unchanged. The
downgrade removes exactly what 0182 added and leaves a guarded row at its
deadline, where an older claim never matches it.
"""

from __future__ import annotations

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

REVISION = "0182"
BELOW = "0178"
COLUMN = "guarded_dispatch_not_before"
CHECK = "execution_requests_guarded_dispatch_ck"
STAMP = datetime(2026, 11, 18, 12, 0, tzinfo=UTC)
DEADLINE = STAMP + timedelta(hours=24)
DUE = STAMP + timedelta(seconds=30)


def _work_item(issue: int) -> uuid.UUID:
    agent_id, item_id = uuid.uuid4(), uuid.uuid4()
    sql_rows(
        "INSERT INTO curie.agents (id, name) VALUES (:id, :name)",
        {"id": agent_id, "name": f"m0182-{agent_id.hex[:8]}"},
    )
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
            "conversation": f"m0182-{item_id.hex[:8]}",
            "stamp": STAMP,
        },
    )
    return item_id


def _request(item_id: uuid.UUID, sequence: int, **columns: Any) -> uuid.UUID:
    """One request row; a cancelled one by default, which is never active."""

    values: dict[str, Any] = {
        "id": uuid.uuid4(),
        "work_item_id": item_id,
        "sequence": sequence,
        "status": "cancelled",
        "terminal_at": STAMP,
        "terminal_cause": "issue_cancelled",
        "wait_deadline": DEADLINE,
        "dispatch_not_before": STAMP,
        "created_at": STAMP,
        "updated_at": STAMP,
        **columns,
    }
    names = ", ".join(values)
    binds = ", ".join(f":{name}" for name in values)
    sql_rows(f"INSERT INTO curie.execution_requests ({names}) VALUES ({binds})", values)
    return values["id"]


def _times(request_id: uuid.UUID) -> tuple[Any, Any, Any]:
    (row,) = sql_dicts(
        f"SELECT wait_deadline, dispatch_not_before, {COLUMN} "
        "FROM curie.execution_requests WHERE id = :id",
        {"id": request_id},
    )
    return row["wait_deadline"], row["dispatch_not_before"], row[COLUMN]


def _installed() -> bool:
    return COLUMN in column_names("execution_requests") and constraint_exists(CHECK)


def test_existing_rows_are_unchanged_and_the_new_column_is_null(
    isolated_migration_db: IsolatedMigrationDb,
) -> None:
    isolated_migration_db.at(BELOW)
    item_id = _work_item(10)
    _request(item_id, 1)
    _request(item_id, 2, status="waiting", terminal_at=None, terminal_cause=None)
    before = sql_dicts("SELECT * FROM curie.execution_requests ORDER BY sequence")

    command.upgrade(alembic_config(), REVISION)

    assert stamped_revision() == REVISION
    assert _installed()
    after = sql_dicts("SELECT * FROM curie.execution_requests ORDER BY sequence")
    assert [{k: v for k, v in row.items() if k != COLUMN} for row in after] == before
    assert [row[COLUMN] for row in after] == [None, None]


def test_a_guarded_insert_stores_its_deadline_where_an_older_claim_reads(
    isolated_migration_db: IsolatedMigrationDb,
) -> None:
    isolated_migration_db.at(REVISION)
    item_id = _work_item(20)
    guarded = _request(item_id, 1, dispatch_not_before=DUE, **{COLUMN: DUE})
    plain = _request(item_id, 2, dispatch_not_before=DUE)

    assert _times(guarded) == (DEADLINE, DEADLINE, DUE)
    assert _times(plain) == (DEADLINE, DUE, None)


def test_an_unaware_reschedule_moves_into_the_guarded_time(
    isolated_migration_db: IsolatedMigrationDb,
) -> None:
    isolated_migration_db.at(REVISION)
    item_id = _work_item(30)
    guarded = _request(item_id, 1, **{COLUMN: DUE})
    plain = _request(item_id, 2, dispatch_not_before=DUE)
    later = DUE + timedelta(minutes=5)

    # What an older release's deferral writes: dispatch_not_before alone.
    sql_rows(
        "UPDATE curie.execution_requests SET dispatch_not_before = :later WHERE id IN (:a, :b)",
        {"later": later, "a": guarded, "b": plain},
    )
    assert _times(guarded) == (DEADLINE, DEADLINE, later)
    assert _times(plain) == (DEADLINE, later, None)

    # A writer that sets the guarded time itself keeps it.
    sql_rows(
        f"UPDATE curie.execution_requests SET {COLUMN} = :due WHERE id = :id",
        {"due": DUE, "id": guarded},
    )
    assert _times(guarded) == (DEADLINE, DEADLINE, DUE)


def test_the_check_refuses_a_guarded_row_dispatchable_before_its_deadline(
    isolated_migration_db: IsolatedMigrationDb,
) -> None:
    isolated_migration_db.at(REVISION)
    item_id = _work_item(40)
    guarded = _request(item_id, 1, **{COLUMN: DUE})
    sql_rows("ALTER TABLE curie.execution_requests DISABLE TRIGGER USER")
    try:
        with pytest.raises(IntegrityError, match=CHECK):
            sql_rows(
                "UPDATE curie.execution_requests SET dispatch_not_before = :due WHERE id = :id",
                {"due": DUE, "id": guarded},
            )
    finally:
        sql_rows("ALTER TABLE curie.execution_requests ENABLE TRIGGER USER")


def test_downgrade_removes_the_guard_and_leaves_guarded_rows_at_their_deadline(
    isolated_migration_db: IsolatedMigrationDb,
) -> None:
    config = alembic_config()
    isolated_migration_db.at(REVISION)
    item_id = _work_item(50)
    guarded = _request(item_id, 1, **{COLUMN: DUE})

    command.downgrade(config, BELOW)

    assert stamped_revision() == BELOW
    assert COLUMN not in column_names("execution_requests")
    assert not constraint_exists(CHECK)
    assert (
        sql_rows("SELECT 1 FROM pg_trigger WHERE tgname = 'execution_requests_guarded_dispatch'")
        == []
    )
    (row,) = sql_dicts(
        "SELECT wait_deadline, dispatch_not_before FROM curie.execution_requests WHERE id = :id",
        {"id": guarded},
    )
    assert row["dispatch_not_before"] == row["wait_deadline"] == DEADLINE

    command.upgrade(config, REVISION)
    assert _installed()
