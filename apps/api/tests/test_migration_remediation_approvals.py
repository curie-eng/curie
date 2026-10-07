"""The remediation approvals migration: ``approvals_purpose_ck`` gains ``remediation``.

@spec AUTOMATED-REMEDIATION-15

AUTOMATED-REMEDIATION-15 (docs/superpowers/specs/2026-10-07-automated-remediation.md):
a not-admitted nomination creates one ``Approval`` with ``purpose``
``remediation`` ("the ``approvals_purpose_ck`` constraint gains the value").

Revision assumed (see ``.projects/plans/task-remediation-approvals.tests.md``):
``0092``, revising plan task 11's execution code revision ``0091``. If
another revision lands first, renumber and move ``BELOW`` and
``HEAD`` here and the head pins. The revision is otherwise
found by its source (``approvals_purpose_ck`` and ``remediation``), so a
renumbering only moves the two constants below. Any further columns the
implementation adds (for the card's failed check and observed value, or the
attach count) are not pinned here; the behavior tests pin what they carry.
"""

from __future__ import annotations

import uuid
from pathlib import Path
from typing import Any

import pytest
from _migration_support import IsolatedMigrationDb, alembic_config, sql_dicts, sql_rows
from alembic import command
from alembic.script import ScriptDirectory
from sqlalchemy.exc import IntegrityError

BELOW = "0091"
HEAD = "0092"


def _revision() -> tuple[str, str]:
    """The revision that adds ``remediation`` to the approval purposes, and its parent."""

    script = ScriptDirectory.from_config(alembic_config())
    found = [
        rev
        for rev in script.walk_revisions()
        if rev.path
        and "approvals_purpose_ck" in (source := Path(rev.path).read_text())
        and "remediation" in source
    ]
    assert len(found) == 1, (
        "no single revision adds 'remediation' to approvals_purpose_ck "
        f"(AUTOMATED-REMEDIATION-15): {[rev.revision for rev in found]}"
    )
    rev = found[0]
    assert isinstance(rev.down_revision, str), rev.down_revision
    return rev.revision, rev.down_revision


def _approval(purpose: str) -> uuid.UUID:
    approval_id = uuid.uuid4()
    sql_rows(
        "INSERT INTO curie.approvals (id, conversation_id, author, summary, reply_kind, "
        "reply_channel, dedupe_key, purpose) VALUES (:id, 'thread-1', 'U0EXAMPLE1', "
        "'scale example-api', 'slack', 'C0EXAMPLE01', :key, :purpose)",
        {"id": approval_id, "key": f"{purpose}:{approval_id}", "purpose": purpose},
    )
    return approval_id


def _snapshot(approval_id: uuid.UUID) -> dict[str, Any]:
    rows = sql_dicts("SELECT * FROM curie.approvals WHERE id = :id", {"id": approval_id})
    assert len(rows) == 1
    return rows[0]


def test_one_hand_written_revision_on_the_assumed_parent() -> None:
    """@spec AUTOMATED-REMEDIATION-15: one revision, ``HEAD`` on ``BELOW``, the only head."""

    revision, down = _revision()
    assert (revision, down) == (HEAD, BELOW)
    assert ScriptDirectory.from_config(alembic_config()).get_heads() == [HEAD]


def test_after_the_upgrade_the_remediation_purpose_is_accepted_and_an_unknown_one_is_not(
    isolated_migration_db: IsolatedMigrationDb,
) -> None:
    """@spec AUTOMATED-REMEDIATION-15: the check gains the value and stays closed."""

    revision, _ = _revision()
    isolated_migration_db.at(revision)

    for purpose in ("session", "publication", "remediation"):
        assert _snapshot(_approval(purpose))["purpose"] == purpose
    with pytest.raises(IntegrityError) as raised:
        _approval("example_unknown")
    assert "check constraint" in str(raised.value).lower()


def test_legacy_approvals_survive_the_upgrade_unchanged(
    isolated_migration_db: IsolatedMigrationDb,
) -> None:
    """@spec AUTOMATED-REMEDIATION-15: existing session and publication rows keep every value."""

    revision, down = _revision()
    isolated_migration_db.at(down)
    legacy = [_approval("session"), _approval("publication")]
    before = [_snapshot(approval_id) for approval_id in legacy]

    command.upgrade(alembic_config(), revision)

    after = [_snapshot(approval_id) for approval_id in legacy]
    assert [
        {key: row[key] for key in old} for row, old in zip(after, before, strict=True)
    ] == before


def test_upgrade_downgrade_upgrade_round_trip(isolated_migration_db: IsolatedMigrationDb) -> None:
    """@spec AUTOMATED-REMEDIATION-15: real Postgres upgrade, downgrade and upgrade.

    The downgrade restores the purpose check without ``remediation``; the other
    approvals stay.
    """

    revision, down = _revision()
    cfg = alembic_config()
    isolated_migration_db.at(down)
    session = _approval("session")

    command.upgrade(cfg, revision)
    command.downgrade(cfg, down)

    assert _snapshot(session)["purpose"] == "session"
    with pytest.raises(IntegrityError) as raised:
        _approval("remediation")
    assert "check constraint" in str(raised.value).lower()

    command.upgrade(cfg, revision)
    assert _snapshot(_approval("remediation"))["purpose"] == "remediation"
