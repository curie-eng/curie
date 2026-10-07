"""The remediation ledger fields migration: schema, legacy rows and a round trip.

@spec AUTOMATED-REMEDIATION-14

AUTOMATED-REMEDIATION-14 (docs/superpowers/specs/2026-10-07-automated-remediation.md)
adds, additively, ``delivery_event_id``, ``nomination_id``,
``verification_outcome``, ``verified_at`` and ``actor_kind`` on
``agent_actions`` and ``actor_kind`` on ``action_audit_entries``, and closes
``authority_kind`` with a check constraint. Acceptance: "existing rows survive
unchanged".

The revision is the next free one on ``next`` when these tests were written:
``0089``, revising the nomination revision ``0088``. It is found by its source
(the one revision adding ``delivery_event_id``), so a renumbering at merge only
moves the two constants below.
"""

from __future__ import annotations

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

BELOW = "0088"
HEAD = "0089"

ACTION_COLUMNS = {
    "delivery_event_id",
    "nomination_id",
    "verification_outcome",
    "verified_at",
    "actor_kind",
}
AUDIT_COLUMNS = {"actor_kind"}


def _revision() -> tuple[str, str]:
    script = ScriptDirectory.from_config(alembic_config())
    found = [
        rev
        for rev in script.walk_revisions()
        if rev.path and "delivery_event_id" in Path(rev.path).read_text()
    ]
    assert len(found) == 1, (
        "expected exactly one alembic revision adding agent_actions.delivery_event_id "
        f"(AUTOMATED-REMEDIATION-14), found {[rev.revision for rev in found]}"
    )
    rev = found[0]
    assert isinstance(rev.down_revision, str), rev.down_revision
    return rev.revision, rev.down_revision


def _legacy_rows() -> dict[str, uuid.UUID]:
    """Rows as the ledger holds them before the migration: a model turn's call,
    a platform forward call, its audit entry and a probe execution.
    """

    agent_id = uuid.uuid4()
    sql_rows(
        "INSERT INTO curie.agents (id, name) VALUES (:id, :name)",
        {"id": agent_id, "name": f"legacy-{agent_id.hex[:8]}"},
    )
    model_turn = uuid.uuid4()
    sql_rows(
        "INSERT INTO curie.agent_actions (id, agent_id, conversation_id, call_id, tool, "
        "arguments, detail, status, dedupe_key) VALUES (:id, :agent_id, 'C1', 'toolu_01', "
        "'mcp__k8s__scale', CAST(:arguments AS jsonb), 'legacy call', 'succeeded', :key)",
        {
            "id": model_turn,
            "agent_id": agent_id,
            "arguments": '{"name": "api", "replicas": 10}',
            "key": f"event-{model_turn}:toolu_01",
        },
    )
    forward = uuid.uuid4()
    sql_rows(
        "INSERT INTO curie.agent_actions (id, agent_id, conversation_id, call_id, tool, "
        "status, dedupe_key, connector, connector_digest, authority_kind, authority_ref) "
        "VALUES (:id, :agent_id, :conversation, :key, 'mcp__k8s__scale', 'pending', :key, "
        "'k8s', :digest, 'approval', :ref)",
        {
            "id": forward,
            "agent_id": agent_id,
            "conversation": f"action-exec:{forward}",
            "key": f"exec:{forward}",
            "digest": "sha256:" + "ab" * 32,
            "ref": str(uuid.uuid4()),
        },
    )
    audit = uuid.uuid4()
    sql_rows(
        "INSERT INTO curie.action_audit_entries "
        "(id, action_id, action, actor, authorizer, authorized, reason) "
        "VALUES (:id, :action_id, 'refused_conflict', 'U-operator', 'static', false, 'moved')",
        {"id": audit, "action_id": model_turn},
    )
    probe = uuid.uuid4()
    sql_rows(
        "INSERT INTO curie.action_executions (id, kind, agent_id, connector, connector_digest, "
        "authority_kind, authority_ref, idempotency_key) VALUES (:id, 'probe', :agent_id, "
        "'k8s', :digest, 'capability_probe', 'probe-1', :key)",
        {
            "id": probe,
            "agent_id": agent_id,
            "digest": "sha256:" + "ab" * 32,
            "key": f"probe:{probe}",
        },
    )
    return {"model_turn": model_turn, "forward": forward, "audit": audit, "probe": probe}


def _snapshot(table: str, row_id: uuid.UUID) -> dict[str, Any]:
    rows = sql_dicts(f"SELECT * FROM curie.{table} WHERE id = :id", {"id": row_id})
    assert len(rows) == 1
    return rows[0]


def test_one_hand_written_revision_on_the_nomination_head() -> None:
    """@spec AUTOMATED-REMEDIATION-14: one additive revision, directly on next's head."""

    revision, down = _revision()
    assert down == BELOW, f"revision {revision} revises {down}, expected {BELOW}"
    assert revision == HEAD
    script = ScriptDirectory.from_config(alembic_config())
    assert script.get_heads() == [HEAD]


def test_the_upgrade_adds_the_columns(isolated_migration_db: IsolatedMigrationDb) -> None:
    """@spec AUTOMATED-REMEDIATION-14"""

    revision, _ = _revision()
    isolated_migration_db.at(revision)

    assert ACTION_COLUMNS <= column_names("agent_actions")
    assert AUDIT_COLUMNS <= column_names("action_audit_entries")
    types = {
        row["column_name"]: row["data_type"]
        for row in sql_dicts(
            "SELECT column_name, data_type FROM information_schema.columns "
            "WHERE table_schema = 'curie' AND table_name = 'agent_actions'"
        )
    }
    assert types["nomination_id"] == "uuid"
    assert types["verified_at"].startswith("timestamp")


def test_legacy_rows_survive_the_upgrade_unchanged(
    isolated_migration_db: IsolatedMigrationDb,
) -> None:
    """@spec AUTOMATED-REMEDIATION-14: "existing rows survive unchanged". Every
    column a row had keeps its value; the new columns read back NULL (no backfill).
    """

    revision, down = _revision()
    isolated_migration_db.at(down)
    ids = _legacy_rows()
    before = {
        "model_turn": _snapshot("agent_actions", ids["model_turn"]),
        "forward": _snapshot("agent_actions", ids["forward"]),
        "audit": _snapshot("action_audit_entries", ids["audit"]),
        "probe": _snapshot("action_executions", ids["probe"]),
    }

    command.upgrade(alembic_config(), revision)

    after = {
        "model_turn": _snapshot("agent_actions", ids["model_turn"]),
        "forward": _snapshot("agent_actions", ids["forward"]),
        "audit": _snapshot("action_audit_entries", ids["audit"]),
        "probe": _snapshot("action_executions", ids["probe"]),
    }
    for name, row in before.items():
        assert {key: after[name][key] for key in row} == row, name
    for name in ("model_turn", "forward"):
        assert {key: after[name][key] for key in ACTION_COLUMNS} == dict.fromkeys(ACTION_COLUMNS)
    assert after["audit"]["actor_kind"] is None


def test_after_the_upgrade_an_unknown_authority_kind_violates_the_check(
    isolated_migration_db: IsolatedMigrationDb,
) -> None:
    """@spec AUTOMATED-REMEDIATION-14: "an unknown ``authority_kind`` violates the check"."""

    revision, _ = _revision()
    isolated_migration_db.at(revision)

    with pytest.raises(IntegrityError) as raised:
        sql_rows(
            "INSERT INTO curie.agent_actions (id, conversation_id, call_id, tool, dedupe_key, "
            "authority_kind, authority_ref) VALUES (:id, 'C1', 'toolu_01', 'mcp__k8s__scale', "
            ":key, 'operator', 'ref-1')",
            {"id": uuid.uuid4(), "key": f"check-{uuid.uuid4()}"},
        )
    assert "check constraint" in str(raised.value).lower()


def test_upgrade_downgrade_upgrade_round_trip(isolated_migration_db: IsolatedMigrationDb) -> None:
    """@spec AUTOMATED-REMEDIATION-14: real Postgres upgrade, downgrade and upgrade.

    The downgrade drops only the new columns and the check; the ledger rows stay.
    """

    revision, down = _revision()
    cfg = alembic_config()
    isolated_migration_db.at(down)
    ids = _legacy_rows()

    command.upgrade(cfg, revision)
    command.downgrade(cfg, down)

    assert not (ACTION_COLUMNS & column_names("agent_actions"))
    assert not (AUDIT_COLUMNS & column_names("action_audit_entries"))
    assert _snapshot("agent_actions", ids["model_turn"])["status"] == "succeeded"
    assert _snapshot("action_audit_entries", ids["audit"])["reason"] == "moved"

    command.upgrade(cfg, revision)
    assert ACTION_COLUMNS <= column_names("agent_actions")
    assert AUDIT_COLUMNS <= column_names("action_audit_entries")
    assert _snapshot("agent_actions", ids["forward"])["authority_kind"] == "approval"
