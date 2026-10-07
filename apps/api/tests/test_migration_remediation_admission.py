"""The admission migration (automated remediation plan task 9).

@spec AUTOMATED-REMEDIATION-8 @spec AUTOMATED-REMEDIATION-11

docs/superpowers/specs/2026-10-07-automated-remediation.md:

* AUTOMATED-REMEDIATION-11: "A ``remediation_breakers`` row keyed by
  ``(agent_id, connector, tool, target key)`` opens on any verification outcome
  other than ``verified``"; only the administrative route closes it, recording
  the operator principal as the closing actor with a reason. The table holds
  ``id`` (the ``{breaker_id}`` of the close route), ``agent_id`` (cascading
  with the agent), ``connector``, ``tool``, ``target``, ``opened_at``,
  ``closed_at``, ``closed_by`` and ``close_reason``; at most one breaker is
  open per key.
* AUTOMATED-REMEDIATION-8: "A nomination failing any of checks 3 to 12 becomes
  an approval request whose card names the failed check." The check is
  recorded on the nomination as ``approval_reason`` (nullable text), one of the
  frozen ``approval_reasons`` of ``tests/vectors/remediation-codes.json``.

The revision is ``0093``, directly on the remediation approvals revision
``0092`` (plan task 10). If another revision lands first, renumber: only
``BELOW`` and ``HEAD`` move. The revision is located by its id and checked to
carry this change. Hand-written and additive (ADR 0117). Every identifier is
a placeholder.
"""

from __future__ import annotations

import json
import uuid
from pathlib import Path
from typing import Any

import pytest
from _migration_support import (
    IsolatedMigrationDb,
    alembic_config,
    column_names,
    sql_dicts,
    sql_rows,
)
from alembic import command
from alembic.script import ScriptDirectory
from sqlalchemy.exc import IntegrityError

BELOW = "0092"
HEAD = "0093"

_CODES = json.loads(
    (
        Path(__file__).resolve().parents[3] / "tests" / "vectors" / "remediation-codes.json"
    ).read_text("utf-8")
)
BREAKER_COLUMNS = {
    "id",
    "agent_id",
    "connector",
    "tool",
    "target",
    "opened_at",
    "closed_at",
    "closed_by",
    "close_reason",
}


def _revision() -> tuple[str, str]:
    script = ScriptDirectory.from_config(alembic_config())
    rev = script.get_revision(HEAD)
    assert rev is not None and rev.path, f"no alembic revision {HEAD}"
    source = Path(rev.path).read_text()
    assert "remediation_breakers" in source and "approval_reason" in source, (
        f"revision {HEAD} does not add remediation_breakers and "
        "remediation_nominations.approval_reason (AUTOMATED-REMEDIATION-8, -11)"
    )
    assert isinstance(rev.down_revision, str), rev.down_revision
    return rev.revision, rev.down_revision


def _agent() -> uuid.UUID:
    agent_id = uuid.uuid4()
    sql_rows(
        "INSERT INTO curie.agents (id, name) VALUES (:id, :name)",
        {"id": agent_id, "name": f"admission-{agent_id.hex[:8]}"},
    )
    return agent_id


def _nomination(agent_id: uuid.UUID, state: str = "received") -> uuid.UUID:
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
        "'{}', :sha, 'k8s:\"example-api\"', 'example reason', :state)",
        {
            "id": nomination_id,
            "agent_id": agent_id,
            "e": event_id,
            "sha": "ef" * 32,
            "state": state,
        },
    )
    return nomination_id


def _open_breaker(agent_id: uuid.UUID, target: str = 'k8s:"example-api"') -> uuid.UUID:
    breaker_id = uuid.uuid4()
    sql_rows(
        "INSERT INTO curie.remediation_breakers (id, agent_id, connector, tool, target, opened_at) "
        "VALUES (:id, :agent_id, 'k8s', 'scale_deployment', :target, now())",
        {"id": breaker_id, "agent_id": agent_id, "target": target},
    )
    return breaker_id


def _row(nomination_id: uuid.UUID) -> dict[str, Any]:
    rows = sql_dicts(
        "SELECT * FROM curie.remediation_nominations WHERE id = :id", {"id": nomination_id}
    )
    assert len(rows) == 1
    return rows[0]


def _tables() -> set[str]:
    return {
        row["table_name"]
        for row in sql_dicts(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'curie'"
        )
    }


def test_one_hand_written_revision_directly_on_the_stack_head() -> None:
    """@spec AUTOMATED-REMEDIATION-8 @spec AUTOMATED-REMEDIATION-11"""

    revision, down = _revision()
    assert down == BELOW, f"revision {revision} revises {down}, expected {BELOW}"
    script = ScriptDirectory.from_config(alembic_config())
    # The qualification revision (task 15, 0094) follows this one; the head pin
    # lives in test_migration_remediation_qualifications.py.
    assert HEAD in {rev.revision for rev in script.iterate_revisions(script.get_heads()[0], BELOW)}


def test_the_upgrade_adds_the_breakers_and_the_approval_reason(
    isolated_migration_db: IsolatedMigrationDb,
) -> None:
    """@spec AUTOMATED-REMEDIATION-11 @spec AUTOMATED-REMEDIATION-8"""

    revision, _ = _revision()
    isolated_migration_db.at(revision)

    assert BREAKER_COLUMNS <= column_names("remediation_breakers")
    (column,) = sql_dicts(
        "SELECT data_type, is_nullable FROM information_schema.columns "
        "WHERE table_schema = 'curie' AND table_name = 'remediation_nominations' "
        "AND column_name = 'approval_reason'"
    )
    assert column == {"data_type": "text", "is_nullable": "YES"}


def test_approval_reason_holds_only_a_frozen_approval_reason(
    isolated_migration_db: IsolatedMigrationDb,
) -> None:
    """@spec AUTOMATED-REMEDIATION-8: the reason is one of the frozen vocabulary."""

    revision, _ = _revision()
    isolated_migration_db.at(revision)
    nomination_id = _nomination(_agent(), state="approval_requested")
    for reason in _CODES["approval_reasons"]:
        sql_rows(
            "UPDATE curie.remediation_nominations SET approval_reason = :r WHERE id = :id",
            {"r": reason, "id": nomination_id},
        )
        assert _row(nomination_id)["approval_reason"] == reason
    with pytest.raises(IntegrityError):
        sql_rows(
            "UPDATE curie.remediation_nominations SET approval_reason = 'looked_fine' "
            "WHERE id = :id",
            {"id": nomination_id},
        )


def test_at_most_one_breaker_is_open_per_key_and_it_goes_with_the_agent(
    isolated_migration_db: IsolatedMigrationDb,
) -> None:
    """@spec AUTOMATED-REMEDIATION-11: keyed by agent, connector, tool and target key."""

    revision, _ = _revision()
    isolated_migration_db.at(revision)
    agent_id = _agent()
    _open_breaker(agent_id)
    _open_breaker(agent_id, target='k8s:"example-worker"')
    with pytest.raises(IntegrityError):
        _open_breaker(agent_id)

    sql_rows("DELETE FROM curie.agents WHERE id = :id", {"id": agent_id})
    assert sql_dicts("SELECT id FROM curie.remediation_breakers") == []


def test_existing_nominations_survive_and_read_null(
    isolated_migration_db: IsolatedMigrationDb,
) -> None:
    """@spec AUTOMATED-REMEDIATION-8: additive; nothing is backfilled."""

    revision, down = _revision()
    isolated_migration_db.at(down)
    nomination_id = _nomination(_agent(), state="approval_requested")
    before = _row(nomination_id)

    command.upgrade(alembic_config(), revision)

    after = _row(nomination_id)
    assert after.pop("approval_reason") is None
    assert after == before


def test_upgrade_downgrade_upgrade_round_trip(isolated_migration_db: IsolatedMigrationDb) -> None:
    """@spec AUTOMATED-REMEDIATION-8 @spec AUTOMATED-REMEDIATION-11: real Postgres."""

    revision, down = _revision()
    cfg = alembic_config()
    isolated_migration_db.at(revision)
    agent_id = _agent()
    nomination_id = _nomination(agent_id, state="approval_requested")
    sql_rows(
        "UPDATE curie.remediation_nominations SET approval_reason = 'breaker_open' WHERE id = :id",
        {"id": nomination_id},
    )
    _open_breaker(agent_id)

    command.downgrade(cfg, down)
    assert "remediation_breakers" not in _tables()
    assert "approval_reason" not in column_names("remediation_nominations")
    assert _row(nomination_id)["state"] == "approval_requested"

    command.upgrade(cfg, revision)
    assert "remediation_breakers" in _tables()
    assert _row(nomination_id)["approval_reason"] is None
