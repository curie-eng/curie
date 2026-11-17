"""Migration 0178 adds the factory Slack progress card's state (#5715).

The upgrade adds the card columns to ``factory_slack_threads`` as nullable
columns, leaves every existing thread and post as it was, and lets
``factory_slack_posts`` hold the ``card``, ``needs_person`` and ``ready``
kinds beside the earlier ones. The downgrade deletes posts of the new kinds,
which an older release cannot render, keeps every other post, restores the
earlier CHECK and drops the columns.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime

import pytest
from _migration_support import (
    IsolatedMigrationDb,
    alembic_config,
    column_names,
    sql_dicts,
    sql_rows,
    stamped_revision,
)
from alembic import command
from sqlalchemy.exc import IntegrityError

REVISION = "0178"
BELOW = "0177"
STAMP = datetime(2026, 11, 16, 12, 0, tzinfo=UTC)
COLUMNS = {
    "card_ts",
    "card_digest",
    "card_edited_at",
    "card_lease_owner",
    "card_lease_expires_at",
    "requester_slack_user",
}
OLD_KINDS = ("root", "phase", "pull_request", "terminal", "triage")
NEW_KINDS = ("card", "needs_person", "ready")


def _thread(issue: int) -> uuid.UUID:
    """One agent, WorkItem and kickoff thread, as an earlier release writes them."""

    agent_id, item_id = uuid.uuid4(), uuid.uuid4()
    sql_rows(
        "INSERT INTO curie.agents (id, name) VALUES (:id, :name)",
        {"id": agent_id, "name": f"m0178-{agent_id.hex[:8]}"},
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
            "conversation": f"m0178-{item_id.hex[:8]}",
            "stamp": STAMP,
        },
    )
    sql_rows(
        "INSERT INTO curie.factory_slack_threads "
        "(work_item_id, agent_id, channel_identity, channel, root_ts, first_sequence, "
        "origin) VALUES (:id, :agent, 'acme-factory', 'C0EXAMPLE1', :root, "
        "1, 'kickoff')",
        {"id": item_id, "agent": agent_id, "root": f"1800000000.{issue:06d}"},
    )
    return item_id


def _post(item_id: uuid.UUID, kind: str) -> None:
    sql_rows(
        "INSERT INTO curie.factory_slack_posts "
        "(work_item_id, post_key, kind, sequence, rank, ordinal, payload) "
        "VALUES (:id, :key, :kind, 1, 1, 0, CAST('{}' AS jsonb))",
        {"id": item_id, "key": f"{kind}:fixture", "kind": kind},
    )


def _kinds() -> list[str]:
    return [row[0] for row in sql_rows("SELECT kind FROM curie.factory_slack_posts ORDER BY id")]


def test_upgrade_adds_nullable_card_columns_and_leaves_rows_alone(
    isolated_migration_db: IsolatedMigrationDb,
) -> None:
    isolated_migration_db.at(BELOW)
    item_id = _thread(10)
    for kind in OLD_KINDS:
        _post(item_id, kind)
    threads = sql_dicts("SELECT * FROM curie.factory_slack_threads")
    posts = sql_dicts("SELECT * FROM curie.factory_slack_posts ORDER BY id")
    assert not COLUMNS & column_names("factory_slack_threads")

    command.upgrade(alembic_config(), REVISION)

    assert stamped_revision() == REVISION
    assert COLUMNS <= column_names("factory_slack_threads")
    (thread,) = sql_dicts("SELECT * FROM curie.factory_slack_threads")
    assert {name: thread[name] for name in COLUMNS} == dict.fromkeys(COLUMNS)
    assert {
        name: value
        for name, value in thread.items()
        if name not in COLUMNS | {"card_upgrade_cutoff"}
    } == threads[0]
    assert sql_dicts("SELECT * FROM curie.factory_slack_posts ORDER BY id") == posts


def test_progress_card_migration_freezes_the_upgrade_cutoff_for_later_threads(
    isolated_migration_db: IsolatedMigrationDb,
) -> None:
    isolated_migration_db.at(BELOW)
    old = _thread(11)
    (before,) = sql_rows("SELECT clock_timestamp()")
    command.upgrade(alembic_config(), REVISION)
    (after,) = sql_rows("SELECT clock_timestamp()")
    (cutoff,) = sql_rows(
        "SELECT card_upgrade_cutoff FROM curie.factory_slack_threads WHERE work_item_id = :id",
        {"id": old},
    )
    assert before[0] <= cutoff[0] <= after[0]
    new = _thread(12)
    sql_rows(
        "UPDATE curie.factory_slack_threads SET created_at = now() + interval '1 day' "
        "WHERE work_item_id = :id",
        {"id": new},
    )
    (later,) = sql_rows(
        "SELECT card_upgrade_cutoff, created_at FROM curie.factory_slack_threads "
        "WHERE work_item_id = :id",
        {"id": new},
    )
    assert later[0] == cutoff[0]
    assert later[1] > later[0]


def test_the_kind_check_accepts_the_card_kinds_and_refuses_an_unknown_one(
    isolated_migration_db: IsolatedMigrationDb,
) -> None:
    isolated_migration_db.at(REVISION)
    item_id = _thread(20)
    for kind in (*OLD_KINDS, *NEW_KINDS):
        _post(item_id, kind)
    assert _kinds() == [*OLD_KINDS, *NEW_KINDS]
    with pytest.raises(IntegrityError, match="factory_slack_posts_kind_ck"):
        _post(item_id, "progress")


def test_the_check_below_refuses_the_card_kinds(
    isolated_migration_db: IsolatedMigrationDb,
) -> None:
    isolated_migration_db.at(BELOW)
    item_id = _thread(30)
    with pytest.raises(IntegrityError, match="factory_slack_posts_kind_ck"):
        _post(item_id, "card")


def test_downgrade_deletes_only_card_kind_posts_and_drops_the_columns(
    isolated_migration_db: IsolatedMigrationDb,
) -> None:
    config = alembic_config()
    isolated_migration_db.at(REVISION)
    item_id = _thread(40)
    for kind in (*OLD_KINDS, *NEW_KINDS):
        _post(item_id, kind)
    sql_rows(
        "UPDATE curie.factory_slack_threads SET card_ts = '1800000000.000002', "
        "card_digest = 'digest', card_edited_at = :stamp, requester_slack_user = 'U0EXAMPLE1' "
        "WHERE work_item_id = :id",
        {"id": item_id, "stamp": STAMP},
    )
    kept = sql_dicts(
        "SELECT * FROM curie.factory_slack_posts WHERE kind NOT IN "
        "('card', 'needs_person', 'ready') ORDER BY id"
    )

    command.downgrade(config, BELOW)

    assert stamped_revision() == BELOW
    assert not COLUMNS & column_names("factory_slack_threads")
    assert "card_upgrade_cutoff" not in column_names("factory_slack_threads")
    assert sql_dicts("SELECT * FROM curie.factory_slack_posts ORDER BY id") == kept
    with pytest.raises(IntegrityError, match="factory_slack_posts_kind_ck"):
        _post(item_id, "ready")

    command.upgrade(config, REVISION)
    assert COLUMNS <= column_names("factory_slack_threads")
    _post(item_id, "card")
    assert _kinds()[-1] == "card"
