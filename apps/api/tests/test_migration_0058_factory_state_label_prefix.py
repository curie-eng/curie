"""Migration 0058 widens the factory state label check (#3221).

Runs against a private database (``isolated_migration_db``), never the shared
one, per apps/api/CLAUDE.md. Seed at 0089, after the later revisions that add
columns and before 0090 replaces the GitHub WorkItem columns the seeds write.
"""

from __future__ import annotations

import uuid

import pytest
from _migration_support import IsolatedMigrationDb, alembic_config, sql_dicts
from alembic import command
from sqlalchemy.exc import IntegrityError

REPO = "acme-corp/acme-bot"
LEGACY = "curie:queued"
NEW = "curie-factory:queued"


def _revision() -> str:
    return str(sql_dicts("SELECT version_num FROM curie.alembic_version")[0]["version_num"])


def _seed_request(number: int) -> tuple[uuid.UUID, uuid.UUID]:
    agent_id, work_item_id, request_id = uuid.uuid4(), uuid.uuid4(), uuid.uuid4()
    sql_dicts(
        "INSERT INTO curie.agents (id, name) VALUES (:id, :name)",
        {"id": agent_id, "name": f"acme-bot-{agent_id.hex[:8]}"},
    )
    sql_dicts(
        "INSERT INTO curie.work_items "
        "(id, github_repository_id, github_issue_number, github_installation_id, "
        "agent_id, repo_full_name, conversation_id) "
        "VALUES (:id, 4401, :number, 5501, :agent, :repo, :conversation)",
        {
            "id": work_item_id,
            "number": number,
            "agent": agent_id,
            "repo": REPO,
            "conversation": f"issue-{number}",
        },
    )
    sql_dicts(
        "INSERT INTO curie.execution_requests "
        "(id, work_item_id, sequence, status, wait_deadline, objective, "
        "requester, reply_kind, reply_address, reply_conversation_id) "
        "VALUES (:id, :work_item, 1, 'waiting', "
        "clock_timestamp() + interval '30 seconds', :objective, "
        "'github:6601:octocat', 'github', :repo, :conversation)",
        {
            "id": request_id,
            "work_item": work_item_id,
            "objective": f"https://github.com/{REPO}/issues/{number}\n\nLabelled.",
            "repo": REPO,
            "conversation": f"issue-{number}",
        },
    )
    sql_dicts("UPDATE curie.work_items SET next_sequence = 2 WHERE id = :id", {"id": work_item_id})
    return work_item_id, request_id


def _seed_notice(work_item_id: uuid.UUID, request_id: uuid.UUID, applied_label: str) -> None:
    sql_dicts(
        "INSERT INTO curie.factory_terminal_notices "
        "(execution_request_id, work_item_id, card_token, applied_label) "
        "VALUES (:id, :work_item, :token, :label)",
        {
            "id": request_id,
            "work_item": work_item_id,
            "token": uuid.uuid4().hex + uuid.uuid4().hex,
            "label": applied_label,
        },
    )


def _applied(request_id: uuid.UUID) -> str | None:
    rows = sql_dicts(
        "SELECT applied_label FROM curie.factory_terminal_notices WHERE execution_request_id = :id",
        {"id": request_id},
    )
    assert len(rows) == 1, rows
    return rows[0]["applied_label"]


def _set_applied(request_id: uuid.UUID, label: str) -> None:
    sql_dicts(
        "UPDATE curie.factory_terminal_notices SET applied_label = :label "
        "WHERE execution_request_id = :id",
        {"id": request_id, "label": label},
    )


def test_0058_accepts_legacy_and_new_labels_and_downgrades_clean_rows(
    isolated_migration_db: IsolatedMigrationDb,
) -> None:
    """N-1 can still write a legacy name, and the new name is a successful write."""

    config = alembic_config()
    isolated_migration_db.at("0089")
    try:
        work_item_id, legacy_id = _seed_request(9101)
        _seed_notice(work_item_id, legacy_id, LEGACY)
        assert _applied(legacy_id) == LEGACY
        _set_applied(legacy_id, NEW)
        assert _applied(legacy_id) == NEW
        with pytest.raises(IntegrityError):
            _set_applied(legacy_id, "curie:custom")
        assert _applied(legacy_id) == NEW

        _set_applied(legacy_id, LEGACY)
        empty_item, empty_id = _seed_request(9102)
        _seed_notice(empty_item, empty_id, "")
        assert _applied(legacy_id) == LEGACY
        assert _applied(empty_id) == ""

        command.downgrade(config, "0057")

        assert _revision() == "0057"
        assert _applied(legacy_id) == LEGACY
        assert _applied(empty_id) == ""
        with pytest.raises(IntegrityError):
            _set_applied(legacy_id, NEW)
        assert _applied(legacy_id) == LEGACY
    finally:
        command.upgrade(config, "head")


def test_0058_downgrade_refuses_a_stored_new_state_label(
    isolated_migration_db: IsolatedMigrationDb,
) -> None:
    config = alembic_config()
    isolated_migration_db.at("0058")
    try:
        work_item_id, request_id = _seed_request(9103)
        _seed_notice(work_item_id, request_id, NEW)
        assert _applied(request_id) == NEW
        with pytest.raises(Exception):  # noqa: B017 (the migration raises on purpose)
            command.downgrade(config, "0057")
        # Later expand revisions can roll back. 0058 itself stays applied
        # because its downgrade refuses a stored new state label.
        assert _revision() == "0058"
        # Move off the stored value so the check runs, then write the new name again.
        _set_applied(request_id, "")
        _set_applied(request_id, NEW)
        assert _applied(request_id) == NEW
    finally:
        command.upgrade(config, "head")
