"""Migration 0184 adds the factory question rounds and answer receipts (#5785).

The upgrade creates ``factory_question_rounds``,
``factory_question_round_counts`` (the lifetime count a cancellation keeps)
and ``factory_answer_receipts`` and lets ``factory_terminal_notices.applied_label`` hold
``curie-factory:questions``. A round whose comment was never posted may have
zero rounds used only while it is ``posting`` or after it settled ``cleared``.
A started request may complete with cause ``questions_asked``. The downgrade
refuses while a status comment row records the new label or a request records
the new cause, then restores the earlier CHECKs and drops the three tables.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta

import pytest
from _migration_support import (
    IsolatedMigrationDb,
    alembic_config,
    sql_rows,
    stamped_revision,
)
from alembic import command
from sqlalchemy.exc import IntegrityError

REVISION = "0184"
BELOW = "0183"
STAMP = datetime(2026, 11, 18, 12, 0, tzinfo=UTC)
DIGEST = "ab" * 32
QUESTIONS_LABEL = "curie-factory:questions"
TABLES = {"factory_question_rounds", "factory_question_round_counts", "factory_answer_receipts"}


def _tables() -> set[str]:
    return {
        row[0]
        for row in sql_rows(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'curie' "
            "AND table_name IN ('factory_question_rounds', 'factory_question_round_counts', "
            "'factory_answer_receipts')"
        )
    }


def _checks(table: str) -> dict[str, str]:
    return {
        row[0]: row[1]
        for row in sql_rows(
            "SELECT conname, pg_get_constraintdef(oid) FROM pg_constraint "
            "WHERE conrelid = CAST(:table AS regclass) AND contype = 'c'",
            {"table": f"curie.{table}"},
        )
    }


def _work_item(issue: int) -> tuple[uuid.UUID, uuid.UUID]:
    """One agent, WorkItem and execution request, as an earlier release writes them."""

    agent_id, item_id, request_id = uuid.uuid4(), uuid.uuid4(), uuid.uuid4()
    sql_rows(
        "INSERT INTO curie.agents (id, name) VALUES (:id, :name)",
        {"id": agent_id, "name": f"m0184-{agent_id.hex[:8]}"},
    )
    sql_rows(
        "INSERT INTO curie.work_items "
        "(id, tracker_kind, tracker_host, tracker_scope_id, tracker_issue_id, "
        "code_host_kind, code_host_host, repository_project_id, "
        "code_host_installation_id, agent_id, repository_path, conversation_id, "
        "version, next_sequence, created_at, updated_at) VALUES "
        "(:id, 'github', 'github.com', '101', :issue, 'github', 'github.com', "
        "'101', 202, :agent, 'acme-corp/acme-bot', :conversation, 1, 2, "
        ":stamp, :stamp)",
        {
            "id": item_id,
            "issue": str(issue),
            "agent": agent_id,
            "conversation": f"m0184-{item_id.hex[:8]}",
            "stamp": STAMP,
        },
    )
    sql_rows(
        "INSERT INTO curie.execution_requests (id, work_item_id, sequence, status) "
        "VALUES (:id, :item, 1, 'queued')",
        {"id": request_id, "item": item_id},
    )
    return item_id, request_id


def _round(item_id: uuid.UUID, request_id: uuid.UUID, *, state: str, rounds: int) -> None:
    sql_rows(
        "INSERT INTO curie.factory_question_rounds "
        "(work_item_id, rounds_used, state, asking_request_id, body_sha256) "
        "VALUES (:item, :rounds, :state, :request, :digest)",
        {
            "item": item_id,
            "rounds": rounds,
            "state": state,
            "request": request_id,
            "digest": DIGEST,
        },
    )


def _count(item_id: uuid.UUID, rounds: int, last: uuid.UUID | None = None) -> None:
    sql_rows(
        "INSERT INTO curie.factory_question_round_counts "
        "(work_item_id, rounds_used, last_asking_request_id) VALUES (:item, :rounds, :last)",
        {"item": item_id, "rounds": rounds, "last": last},
    )


def _notice(item_id: uuid.UUID, request_id: uuid.UUID, label: str) -> None:
    sql_rows(
        "INSERT INTO curie.factory_terminal_notices "
        "(execution_request_id, work_item_id, applied_label) VALUES (:request, :item, :label)",
        {"request": request_id, "item": item_id, "label": label},
    )


def _completed(item_id: uuid.UUID, cause: str) -> uuid.UUID:
    """A started request on ``item_id`` that completed with ``cause``."""

    request_id = uuid.uuid4()
    sql_rows(
        "INSERT INTO curie.execution_requests (id, work_item_id, sequence, status, "
        "wait_deadline, started_at, execution_deadline, terminal_at, terminal_cause, "
        "execution_attempts) "
        "VALUES (:id, :item, 2, 'completed', :deadline, :stamp, :deadline, :stamp, :cause, 1)",
        {
            "id": request_id,
            "item": item_id,
            "stamp": STAMP,
            "deadline": STAMP + timedelta(minutes=30),
            "cause": cause,
        },
    )
    return request_id


def test_upgrade_creates_both_tables_and_widens_the_label_check(
    isolated_migration_db: IsolatedMigrationDb,
) -> None:
    isolated_migration_db.at(BELOW)
    assert _tables() == set()
    assert (
        QUESTIONS_LABEL
        not in _checks("factory_terminal_notices")["factory_terminal_notices_applied_label_ck"]
    )

    command.upgrade(alembic_config(), REVISION)

    assert stamped_revision() == REVISION
    assert _tables() == TABLES
    label_check = _checks("factory_terminal_notices")["factory_terminal_notices_applied_label_ck"]
    assert QUESTIONS_LABEL in label_check
    assert "curie-factory:needs-human" in label_check
    rounds = _checks("factory_question_rounds")
    assert {
        "factory_question_rounds_rounds_used_ck",
        "factory_question_rounds_state_ck",
        "factory_question_rounds_posted_ck",
        "factory_question_rounds_body_sha256_ck",
    } <= set(rounds)
    assert "factory_answer_receipts_outcome_ck" in _checks("factory_answer_receipts")
    assert "factory_question_round_counts_rounds_used_ck" in _checks(
        "factory_question_round_counts"
    )
    item_id, request_id = _work_item(10)
    _notice(item_id, request_id, QUESTIONS_LABEL)


def test_rounds_used_is_zero_only_while_posting_or_cleared(
    isolated_migration_db: IsolatedMigrationDb,
) -> None:
    isolated_migration_db.at(REVISION)
    posting, request = _work_item(20)
    _round(posting, request, state="posting", rounds=0)
    cleared, request = _work_item(21)
    _round(cleared, request, state="cleared", rounds=0)
    for number, state in enumerate(("outstanding", "answered", "exhausted"), start=22):
        item_id, request = _work_item(number)
        with pytest.raises(IntegrityError, match="factory_question_rounds_posted_ck"):
            _round(item_id, request, state=state, rounds=0)
    item_id, request = _work_item(30)
    _round(item_id, request, state="outstanding", rounds=1)
    item_id, request = _work_item(31)
    with pytest.raises(IntegrityError, match="factory_question_rounds_rounds_used_ck"):
        _round(item_id, request, state="exhausted", rounds=4)
    item_id, request = _work_item(32)
    with pytest.raises(IntegrityError, match="factory_question_rounds_state_ck"):
        _round(item_id, request, state="waiting", rounds=1)


def test_rows_go_with_their_work_item(isolated_migration_db: IsolatedMigrationDb) -> None:
    isolated_migration_db.at(REVISION)
    item_id, request_id = _work_item(40)
    _round(item_id, request_id, state="outstanding", rounds=1)
    _count(item_id, 1, request_id)
    sql_rows(
        "INSERT INTO curie.factory_answer_receipts (work_item_id, comment_key, outcome) "
        "VALUES (:item, '9001', 'coalesced')",
        {"item": item_id},
    )
    with pytest.raises(IntegrityError, match="factory_answer_receipts_outcome_ck"):
        sql_rows(
            "INSERT INTO curie.factory_answer_receipts (work_item_id, comment_key, outcome) "
            "VALUES (:item, '9002', 'ignored')",
            {"item": item_id},
        )
    sql_rows("DELETE FROM curie.work_items WHERE id = :id", {"id": item_id})
    assert sql_rows("SELECT count(*) FROM curie.factory_question_rounds") == [(0,)]
    assert sql_rows("SELECT count(*) FROM curie.factory_question_round_counts") == [(0,)]
    assert sql_rows("SELECT count(*) FROM curie.factory_answer_receipts") == [(0,)]


def test_the_lifetime_count_holds_zero_to_three_rounds_and_outlives_the_question(
    isolated_migration_db: IsolatedMigrationDb,
) -> None:
    isolated_migration_db.at(REVISION)
    for number, rounds in enumerate((0, 3), start=45):
        item_id, _request = _work_item(number)
        _count(item_id, rounds)
    for number, rounds in enumerate((-1, 4), start=47):
        item_id, _request = _work_item(number)
        with pytest.raises(IntegrityError, match="factory_question_round_counts_rounds_used_ck"):
            _count(item_id, rounds)
    item_id, request_id = _work_item(49)
    _round(item_id, request_id, state="outstanding", rounds=2)
    _count(item_id, 2, request_id)
    # A cancellation deletes the question row; the count is its own row.
    sql_rows(
        "DELETE FROM curie.factory_question_rounds WHERE work_item_id = :item", {"item": item_id}
    )
    assert sql_rows(
        "SELECT rounds_used, last_asking_request_id FROM curie.factory_question_round_counts "
        "WHERE work_item_id = :item",
        {"item": item_id},
    ) == [(2, request_id)]


def test_downgrade_restores_the_earlier_schema(
    isolated_migration_db: IsolatedMigrationDb,
) -> None:
    isolated_migration_db.at(REVISION)
    item_id, request_id = _work_item(50)
    _round(item_id, request_id, state="outstanding", rounds=1)
    _count(item_id, 1, request_id)
    _notice(item_id, request_id, "curie-factory:needs-human")

    command.downgrade(alembic_config(), BELOW)

    assert stamped_revision() == BELOW
    assert _tables() == set()
    assert (
        QUESTIONS_LABEL
        not in _checks("factory_terminal_notices")["factory_terminal_notices_applied_label_ck"]
    )
    other, request = _work_item(51)
    with pytest.raises(IntegrityError, match="factory_terminal_notices_applied_label_ck"):
        _notice(other, request, QUESTIONS_LABEL)


def test_downgrade_refuses_while_a_notice_carries_the_questions_label(
    isolated_migration_db: IsolatedMigrationDb,
) -> None:
    isolated_migration_db.at(REVISION)
    item_id, request_id = _work_item(60)
    _notice(item_id, request_id, QUESTIONS_LABEL)

    with pytest.raises(RuntimeError, match="factory_question_label_present"):
        command.downgrade(alembic_config(), BELOW)

    assert stamped_revision() == REVISION
    assert _tables() == TABLES


def test_a_started_request_completes_with_questions_asked_only_after_the_upgrade(
    isolated_migration_db: IsolatedMigrationDb,
) -> None:
    isolated_migration_db.at(BELOW)
    item_id, _request = _work_item(70)
    with pytest.raises(IntegrityError, match="execution_requests_state_shape_ck"):
        _completed(item_id, "questions_asked")

    command.upgrade(alembic_config(), REVISION)

    _completed(item_id, "questions_asked")
    other, _request = _work_item(71)
    _completed(other, "completed")
    with pytest.raises(IntegrityError, match="execution_requests_state_shape_ck"):
        _completed(other, "question_rounds_exhausted")


def test_downgrade_refuses_while_a_request_completed_with_questions_asked(
    isolated_migration_db: IsolatedMigrationDb,
) -> None:
    isolated_migration_db.at(REVISION)
    item_id, _request = _work_item(80)
    _completed(item_id, "questions_asked")

    with pytest.raises(RuntimeError, match="factory_question_ending_present"):
        command.downgrade(alembic_config(), BELOW)

    assert stamped_revision() == REVISION
