"""Migration 0172 adds the shared automatic retry allowance (#5424, ADR 0269).

The upgrade counts the surviving owner loss successors of each WorkItem as its
used slots, leaves every existing column value alone, and lets an unstarted
expired request end ``retry_condition_expired`` only when it was blocked. The
counter never decreases. The downgrade refuses while any retry state exists and
removes exactly what 0172 added when none does.
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
from sqlalchemy.exc import DBAPIError, IntegrityError

REVISION = "0172"
BELOW = "0171"
STAMP = datetime(2026, 11, 1, 12, 0, tzinfo=UTC)
WORK_ITEM_COLUMNS = {"automatic_retries_used"}
REQUEST_COLUMNS = {
    "retry_of_request_id",
    "retry_origin_request_id",
    "blocked_on",
    "retry_safety",
    "retry_condition_reason",
    "retry_condition_retry_at",
    "retry_condition_scope_key",
}
REQUEST_CHECKS = (
    "execution_requests_retry_lineage_ck",
    "execution_requests_blocked_unstarted_ck",
    "execution_requests_retry_safety_ck",
    "execution_requests_retry_condition_ck",
)
REFUSAL = "factory_retry_state_present"


def _agent() -> uuid.UUID:
    agent_id = uuid.uuid4()
    sql_rows(
        "INSERT INTO curie.agents (id, name) VALUES (:id, :name)",
        {"id": agent_id, "name": f"m0172-{agent_id.hex[:8]}"},
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
            "conversation": f"m0172-{item_id.hex[:8]}",
            "stamp": STAMP,
        },
    )
    return item_id


def _request(
    item_id: uuid.UUID,
    sequence: int,
    *,
    status: str = "cancelled",
    owner_lost_retry: bool = False,
    **columns: Any,
) -> uuid.UUID:
    """One request row. The default is a cancelled request, which is never active."""

    values: dict[str, Any] = {
        "id": uuid.uuid4(),
        "work_item_id": item_id,
        "sequence": sequence,
        "status": status,
        "owner_lost_retry": owner_lost_retry,
        "wait_deadline": None if status in ("queued", "cancelled") else STAMP + timedelta(hours=1),
        "terminal_at": STAMP if status in ("cancelled", "expired") else None,
        "terminal_cause": "issue_cancelled" if status == "cancelled" else None,
        "created_at": STAMP,
        "updated_at": STAMP,
    }
    values.update(columns)
    names = ", ".join(values)
    binds = ", ".join(f":{name}" for name in values)
    sql_rows(f"INSERT INTO curie.execution_requests ({names}) VALUES ({binds})", values)
    return values["id"]


def _used(item_id: uuid.UUID) -> int:
    rows = sql_rows(
        "SELECT automatic_retries_used FROM curie.work_items WHERE id = :id", {"id": item_id}
    )
    return int(rows[0][0])


def test_backfill_counts_surviving_owner_lost_successors(
    isolated_migration_db: IsolatedMigrationDb,
) -> None:
    isolated_migration_db.at(BELOW)
    agent_id = _agent()
    items = {}
    for survivors in (0, 1, 2, 3):
        item_id = _work_item(agent_id, 10 + survivors)
        _request(item_id, 1)
        for extra in range(survivors):
            _request(item_id, 2 + extra, owner_lost_retry=True)
        items[survivors] = item_id

    command.upgrade(alembic_config(), REVISION)

    for survivors, item_id in items.items():
        assert _used(item_id) == survivors


def test_existing_rows_are_unchanged_by_upgrade(
    isolated_migration_db: IsolatedMigrationDb,
) -> None:
    isolated_migration_db.at(BELOW)
    agent_id = _agent()
    item_id = _work_item(agent_id, 20)
    first = _request(item_id, 1)
    second = _request(item_id, 2, owner_lost_retry=True)
    waiting = _request(item_id, 3, status="waiting", owner_lost_retry=True)
    items_before = sql_dicts("SELECT * FROM curie.work_items ORDER BY id")
    requests_before = sql_dicts("SELECT * FROM curie.execution_requests ORDER BY id")
    assert len(requests_before) == 3

    command.upgrade(alembic_config(), REVISION)

    items_after = sql_dicts("SELECT * FROM curie.work_items ORDER BY id")
    requests_after = sql_dicts("SELECT * FROM curie.execution_requests ORDER BY id")
    assert [
        {key: value for key, value in row.items() if key not in WORK_ITEM_COLUMNS}
        for row in items_after
    ] == items_before
    # Only the counter is new; the version and update stamp were not touched.
    assert items_after[0]["automatic_retries_used"] == 2
    assert items_after[0]["version"] == 4
    assert items_after[0]["updated_at"] == STAMP
    assert [
        {key: value for key, value in row.items() if key not in REQUEST_COLUMNS}
        for row in requests_after
    ] == requests_before
    # Historical owner loss successors keep null lineage.
    assert {row["id"] for row in requests_after} == {first, second, waiting}
    assert all(row[column] is None for row in requests_after for column in REQUEST_COLUMNS)


def test_shape_admits_retry_condition_expired_only_with_a_block(
    isolated_migration_db: IsolatedMigrationDb,
) -> None:
    isolated_migration_db.at(REVISION)
    agent_id = _agent()
    item_id = _work_item(agent_id, 30)

    with pytest.raises(IntegrityError, match="execution_requests_state_shape_ck"):
        _request(item_id, 1, status="expired", terminal_cause="retry_condition_expired")

    blocked = _request(
        item_id,
        2,
        status="expired",
        terminal_cause="retry_condition_expired",
        blocked_on="provider_usage_reset",
    )
    # capacity_wait_expired stays legal with or without a block.
    plain = _request(item_id, 3, status="expired", terminal_cause="capacity_wait_expired")
    held = _request(
        item_id,
        4,
        status="expired",
        terminal_cause="capacity_wait_expired",
        blocked_on="provider_credit",
    )
    rows = sql_rows(
        "SELECT id FROM curie.execution_requests WHERE id IN (:a, :b, :c)",
        {"a": blocked, "b": plain, "c": held},
    )
    assert len(rows) == 3

    # A started request is never blocked, and the typed block is never empty.
    with pytest.raises(IntegrityError, match="execution_requests_blocked_unstarted_ck"):
        _request(
            item_id,
            5,
            status="running",
            blocked_on="provider_credit",
            started_at=STAMP,
            execution_deadline=STAMP + timedelta(hours=1),
            execution_attempts=1,
        )
    with pytest.raises(IntegrityError, match="execution_requests_blocked_unstarted_ck"):
        _request(item_id, 6, status="waiting", blocked_on="")


def test_counter_never_decreases(isolated_migration_db: IsolatedMigrationDb) -> None:
    isolated_migration_db.at(REVISION)
    item_id = _work_item(_agent(), 40)
    assert _used(item_id) == 0

    for value in (1, 2, 2):
        sql_rows(
            "UPDATE curie.work_items SET automatic_retries_used = :value WHERE id = :id",
            {"value": value, "id": item_id},
        )
        assert _used(item_id) == value

    with pytest.raises(DBAPIError, match="never decreases"):
        sql_rows(
            "UPDATE curie.work_items SET automatic_retries_used = 1 WHERE id = :id",
            {"id": item_id},
        )
    with pytest.raises(DBAPIError, match="work_items_automatic_retries_used_ck"):
        sql_rows(
            "INSERT INTO curie.work_items "
            "(id, tracker_kind, tracker_host, tracker_scope_id, tracker_issue_id, "
            "code_host_kind, code_host_host, repository_project_id, "
            "code_host_installation_id, agent_id, repository_path, conversation_id, "
            "version, next_sequence, automatic_retries_used) VALUES "
            "(:id, 'github', 'github.com', '101', '41', 'github', 'github.com', "
            "'101', 202, :agent, 'acme-corp/acme-bot', 'm0172-negative', 1, 1, -1)",
            {"id": uuid.uuid4(), "agent": _agent()},
        )
    assert _used(item_id) == 2


def _seed_lineage(item_id: uuid.UUID) -> None:
    predecessor = _request(item_id, 1)
    _request(
        item_id,
        2,
        status="waiting",
        retry_of_request_id=predecessor,
        retry_origin_request_id=predecessor,
    )


def _seed_blocked(item_id: uuid.UUID) -> None:
    _request(item_id, 1)
    _request(item_id, 2, status="waiting", blocked_on="provider_credit")


def _seed_expired(item_id: uuid.UUID) -> None:
    _request(item_id, 1, status="expired", terminal_cause="retry_condition_expired", blocked_on="x")


def _catalog_has_retry_state() -> bool:
    return bool(
        REQUEST_COLUMNS <= column_names("execution_requests")
        and WORK_ITEM_COLUMNS <= column_names("work_items")
        and all(constraint_exists(name) for name in REQUEST_CHECKS)
        and sql_rows("SELECT 1 FROM pg_trigger WHERE tgname = 'work_items_retry_usage_monotonic'")
        and sql_rows("SELECT 1 FROM pg_indexes WHERE indexname = 'uq_execution_requests_retry_of'")
    )


@pytest.mark.parametrize("seed", [_seed_lineage, _seed_blocked, _seed_expired])
def test_downgrade_refuses_while_retry_state_exists_and_succeeds_without(
    isolated_migration_db: IsolatedMigrationDb, seed: Any
) -> None:
    config = alembic_config()
    isolated_migration_db.at(REVISION)
    item_id = _work_item(_agent(), 50)
    seed(item_id)
    before = sql_dicts("SELECT * FROM curie.execution_requests ORDER BY sequence")

    with pytest.raises(RuntimeError, match=REFUSAL):
        command.downgrade(config, BELOW)

    # Refused before any DDL: the revision, schema and rows are as they were.
    assert stamped_revision() == REVISION
    assert _catalog_has_retry_state()
    assert sql_dicts("SELECT * FROM curie.execution_requests ORDER BY sequence") == before

    # With the retry state gone the downgrade removes exactly what 0172 added.
    sql_rows("DELETE FROM curie.execution_requests WHERE work_item_id = :id", {"id": item_id})
    sql_rows(
        "UPDATE curie.work_items SET automatic_retries_used = 2 WHERE id = :id", {"id": item_id}
    )
    command.downgrade(config, BELOW)

    assert stamped_revision() == BELOW
    assert not REQUEST_COLUMNS & column_names("execution_requests")
    assert not WORK_ITEM_COLUMNS & column_names("work_items")
    assert not any(constraint_exists(name) for name in REQUEST_CHECKS)
    assert not constraint_exists("work_items_automatic_retries_used_ck")
    assert not sql_rows(
        "SELECT 1 FROM pg_trigger WHERE tgname = 'work_items_retry_usage_monotonic'"
    )
    assert not sql_rows(
        "SELECT 1 FROM pg_proc WHERE proname = 'enforce_work_items_retry_usage_monotonic'"
    )
    assert not sql_rows(
        "SELECT 1 FROM pg_indexes WHERE indexname = 'uq_execution_requests_retry_of'"
    )

    # The prior state shape is back: the new cause is refused again.
    with pytest.raises(IntegrityError, match="execution_requests_state_shape_ck"):
        _request(item_id, 7, status="expired", terminal_cause="retry_condition_expired")

    command.upgrade(config, REVISION)
    assert _catalog_has_retry_state()
    assert _used(item_id) == 0


def _live_shape() -> str:
    rows = sql_rows(
        "SELECT pg_get_constraintdef(oid) FROM pg_constraint "
        "WHERE conrelid = 'curie.execution_requests'::regclass "
        "AND conname = 'execution_requests_state_shape_ck'"
    )
    return str(rows[0][0])


def _recreate_shape(definition: str) -> None:
    sql_rows(
        "ALTER TABLE curie.execution_requests DROP CONSTRAINT execution_requests_state_shape_ck"
    )
    sql_rows(
        "ALTER TABLE curie.execution_requests "
        f"ADD CONSTRAINT execution_requests_state_shape_ck {definition}"
    )


def test_upgrade_accepts_a_restored_state_shape(
    isolated_migration_db: IsolatedMigrationDb,
) -> None:
    isolated_migration_db.at(BELOW)
    # As 0160 created it, then recreated from its own printed form, which is
    # how a dumped and restored database holds it.
    _recreate_shape(_live_shape())

    command.upgrade(alembic_config(), REVISION)

    assert stamped_revision() == REVISION
    assert _catalog_has_retry_state()


def test_upgrade_refuses_a_different_state_shape(
    isolated_migration_db: IsolatedMigrationDb,
) -> None:
    isolated_migration_db.at(BELOW)
    _recreate_shape(_live_shape().replace("'start_failed'", "'start_failed_x'", 1))

    with pytest.raises(RuntimeError, match="not the 0160 shape"):
        command.upgrade(alembic_config(), REVISION)

    assert stamped_revision() == BELOW
