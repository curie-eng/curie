"""The nomination's execution code migration (plan task 11, review round 1).

@spec AUTOMATED-REMEDIATION-18

AUTOMATED-REMEDIATION-18 (docs/superpowers/specs/2026-10-07-automated-remediation.md):
"An execution that ends ``failed``, ``indeterminate`` or ``refused`` after
admission gets no verifier and finishes ``not-recovered`` for reporting, with
the execution code." A refused forward execution has no ledger row, so the code
is recorded on the nomination: ``remediation_nominations.execution_code``
(nullable text, no default; existing rows read NULL).

The revision is the next free one on this stack: ``0091a``, revising the read
executions revision ``0090`` (``0092a`` is taken by another branch). It is found
by its id and checked to add ``execution_code``, so a renumbering at merge only
moves the two constants below. Every identifier is a placeholder.
"""

from __future__ import annotations

import uuid
from pathlib import Path
from typing import Any

from _migration_support import (
    IsolatedMigrationDb,
    alembic_config,
    column_names,
    sql_dicts,
    sql_rows,
)
from alembic import command
from alembic.script import ScriptDirectory

BELOW = "0090"
HEAD = "0091a"


def _revision() -> tuple[str, str]:
    script = ScriptDirectory.from_config(alembic_config())
    rev = script.get_revision(HEAD)
    assert rev is not None and rev.path, f"no alembic revision {HEAD}"
    source = Path(rev.path).read_text()
    assert "remediation_nominations" in source and "execution_code" in source, (
        f"revision {HEAD} does not add remediation_nominations.execution_code "
        "(AUTOMATED-REMEDIATION-18)"
    )
    assert isinstance(rev.down_revision, str), rev.down_revision
    return rev.revision, rev.down_revision


def _nomination() -> uuid.UUID:
    agent_id = uuid.uuid4()
    sql_rows(
        "INSERT INTO curie.agents (id, name) VALUES (:id, :name)",
        {"id": agent_id, "name": f"legacy-{agent_id.hex[:8]}"},
    )
    event_id = f"event-{uuid.uuid4().hex[:10]}"
    sql_rows(
        "INSERT INTO curie.remediation_nomination_submissions "
        "(event_id, agent_id, hook, block_sha256) VALUES (:e, :agent_id, 'alerts', :sha)",
        {"e": event_id, "agent_id": agent_id, "sha": "cd" * 32},
    )
    nomination_id = uuid.uuid4()
    sql_rows(
        "INSERT INTO curie.remediation_nominations "
        "(id, agent_id, hook, event_id, action, kind, arguments, arguments_sha256, target, "
        "reason, state) VALUES (:id, :agent_id, 'alerts', :e, 'scale-out-api', 'remediate', "
        "'{}', :sha, 'k8s:\"example-api\"', 'example reason', 'executing')",
        {"id": nomination_id, "agent_id": agent_id, "e": event_id, "sha": "ef" * 32},
    )
    return nomination_id


def _row(nomination_id: uuid.UUID) -> dict[str, Any]:
    rows = sql_dicts(
        "SELECT * FROM curie.remediation_nominations WHERE id = :id", {"id": nomination_id}
    )
    assert len(rows) == 1
    return rows[0]


def test_one_hand_written_revision_on_the_read_executions_head() -> None:
    """@spec AUTOMATED-REMEDIATION-18: one additive revision, directly on the stack's head."""

    revision, down = _revision()
    assert down == BELOW, f"revision {revision} revises {down}, expected {BELOW}"
    assert revision == HEAD
    script = ScriptDirectory.from_config(alembic_config())
    # The remediation approvals revision (task 10, 0092a) follows this one; the
    # head pin lives in test_migration_remediation_approvals.py.
    assert HEAD in {rev.revision for rev in script.iterate_revisions(script.get_heads()[0], BELOW)}


def test_the_upgrade_adds_a_nullable_text_execution_code(
    isolated_migration_db: IsolatedMigrationDb,
) -> None:
    """@spec AUTOMATED-REMEDIATION-18"""

    revision, _ = _revision()
    isolated_migration_db.at(revision)

    assert "execution_code" in column_names("remediation_nominations")
    (column,) = sql_dicts(
        "SELECT data_type, is_nullable, column_default FROM information_schema.columns "
        "WHERE table_schema = 'curie' AND table_name = 'remediation_nominations' "
        "AND column_name = 'execution_code'"
    )
    assert column == {"data_type": "text", "is_nullable": "YES", "column_default": None}


def test_existing_nominations_survive_and_read_null(
    isolated_migration_db: IsolatedMigrationDb,
) -> None:
    """@spec AUTOMATED-REMEDIATION-18: additive; nothing is backfilled."""

    revision, down = _revision()
    isolated_migration_db.at(down)
    nomination_id = _nomination()
    before = _row(nomination_id)

    command.upgrade(alembic_config(), revision)

    after = _row(nomination_id)
    assert after.pop("execution_code") is None
    assert after == before


def test_upgrade_downgrade_upgrade_round_trip(isolated_migration_db: IsolatedMigrationDb) -> None:
    """@spec AUTOMATED-REMEDIATION-18: real Postgres upgrade, downgrade and upgrade."""

    revision, down = _revision()
    cfg = alembic_config()
    isolated_migration_db.at(revision)
    nomination_id = _nomination()
    sql_rows(
        "UPDATE curie.remediation_nominations SET execution_code = 'agent_stopped' WHERE id = :id",
        {"id": nomination_id},
    )

    command.downgrade(cfg, down)
    assert "execution_code" not in column_names("remediation_nominations")
    assert _row(nomination_id)["state"] == "executing"

    command.upgrade(cfg, revision)
    assert _row(nomination_id)["execution_code"] is None
