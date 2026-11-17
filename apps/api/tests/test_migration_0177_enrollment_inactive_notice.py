"""Migration 0177 admits the enrollment_inactive intake notice (#5713).

Upgrade replaces the intake-notice reason CHECK so a linked label actor with
no active enrollment can be recorded. An older reason survives the upgrade.
Downgrade restores the previous list and refuses while any enrollment_inactive
notice exists. factory_slack_refusal_posts stores the reason in its payload
and keeps no reason CHECK.
"""

from __future__ import annotations

import pytest
from _migration_support import (
    IsolatedMigrationDb,
    alembic_config,
    sql_rows,
    stamped_revision,
)
from alembic import command
from sqlalchemy.exc import IntegrityError

REVISION = "0177"
BELOW = "0176"
CHECK = "factory_intake_notices_reason_ck"


def _notice(reason: str, event: int) -> None:
    sql_rows(
        "INSERT INTO curie.factory_intake_notices "
        "(tracker_kind, tracker_host, tracker_scope_id, tracker_issue_id, "
        "label_event_id, repository_path, issue_url, label, reason) VALUES "
        "('github', 'github.com', '101', '17', :event, 'acme/widgets', "
        "'https://github.com/acme/widgets/issues/17', 'curie', :reason)",
        {"event": event, "reason": reason},
    )


def _reason_check() -> str:
    (row,) = sql_rows(
        "SELECT pg_get_constraintdef(oid) FROM pg_constraint WHERE conname = :name",
        {"name": CHECK},
    )
    return str(row[0])


def test_label_enrollment_inactive_upgrade_admits_the_reason(
    isolated_migration_db: IsolatedMigrationDb,
) -> None:
    isolated_migration_db.at(BELOW)
    _notice("personal_owner_required", 1)
    with pytest.raises(IntegrityError, match=CHECK):
        _notice("enrollment_inactive", 2)

    command.upgrade(alembic_config(), REVISION)

    assert stamped_revision() == REVISION
    assert "enrollment_inactive" in _reason_check()
    _notice("enrollment_inactive", 2)
    assert sql_rows(
        "SELECT reason FROM curie.factory_intake_notices ORDER BY label_event_id"
    ) == [("personal_owner_required",), ("enrollment_inactive",)]
    assert sql_rows(
        "SELECT conname FROM pg_constraint "
        "WHERE conrelid = 'curie.factory_slack_refusal_posts'::regclass AND contype = 'c'"
    ) == []


def test_label_enrollment_inactive_downgrade_refuses_while_a_row_exists(
    isolated_migration_db: IsolatedMigrationDb,
) -> None:
    isolated_migration_db.at(REVISION)
    _notice("enrollment_inactive", 3)

    with pytest.raises(RuntimeError, match="cannot downgrade while enrollment_inactive"):
        command.downgrade(alembic_config(), BELOW)

    assert stamped_revision() == REVISION
    assert "enrollment_inactive" in _reason_check()


def test_label_enrollment_inactive_downgrade_restores_the_previous_check(
    isolated_migration_db: IsolatedMigrationDb,
) -> None:
    isolated_migration_db.at(REVISION)
    _notice("personal_owner_required", 4)
    _notice("enrollment_inactive", 5)
    sql_rows(
        "DELETE FROM curie.factory_intake_notices WHERE reason = 'enrollment_inactive'"
    )

    command.downgrade(alembic_config(), BELOW)

    assert stamped_revision() == BELOW
    assert "enrollment_inactive" not in _reason_check()
    with pytest.raises(IntegrityError, match=CHECK):
        _notice("enrollment_inactive", 6)
    _notice("binding_ambiguous", 7)
