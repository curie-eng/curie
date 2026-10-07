"""The read executions migration: ``read`` kind, ``not_before`` and the sample.

@spec AUTOMATED-REMEDIATION-12 (executor amendments E2 and E9)

AUTOMATED-REMEDIATION-12 (docs/superpowers/specs/2026-10-07-automated-remediation.md):
"``action_executions.kind`` gains ``read`` (the check constraint is replaced in
the same migration)"; E9: "Executions gain a nullable ``not_before``". The
sample the worker reports (``POST /action-executions/{id}/samples``) is stored
on the execution as ``sample`` (JSONB, ``{"sample", "value"}``), so the
verifier (plan task 11) reads it in the same transaction as the state. The
column names ``not_before`` and ``sample`` are the shapes these tests fix (see
``.projects/plans/task-remediation-read.tests.md``).

The revision is the next free one on this stack when these tests were written:
``0090``, revising the ledger fields revision ``0089`` (plan task 8, #4245). It
is found by its source (the one revision adding ``not_before``), so a
renumbering at merge only moves the two constants below.
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

BELOW = "0089"
HEAD = "0090"

NEW_COLUMNS = {"not_before", "sample"}
DIGEST = "sha256:" + "ab" * 32


def _revision() -> tuple[str, str]:
    script = ScriptDirectory.from_config(alembic_config())
    found = [
        rev
        for rev in script.walk_revisions()
        if rev.path and "not_before" in Path(rev.path).read_text()
    ]
    assert len(found) == 1, (
        "expected exactly one alembic revision adding action_executions.not_before "
        f"(AUTOMATED-REMEDIATION-12, executor amendment E9), found {[r.revision for r in found]}"
    )
    rev = found[0]
    assert isinstance(rev.down_revision, str), rev.down_revision
    return rev.revision, rev.down_revision


def _agent() -> uuid.UUID:
    agent_id = uuid.uuid4()
    sql_rows(
        "INSERT INTO curie.agents (id, name) VALUES (:id, :name)",
        {"id": agent_id, "name": f"legacy-{agent_id.hex[:8]}"},
    )
    return agent_id


def _probe(agent_id: uuid.UUID) -> uuid.UUID:
    probe = uuid.uuid4()
    sql_rows(
        "INSERT INTO curie.action_executions (id, kind, agent_id, connector, connector_digest, "
        "authority_kind, authority_ref, idempotency_key) VALUES (:id, 'probe', :agent_id, "
        "'k8s', :digest, 'capability_probe', 'probe-1', :key)",
        {"id": probe, "agent_id": agent_id, "digest": DIGEST, "key": f"probe:{probe}"},
    )
    return probe


def _read(agent_id: uuid.UUID, kind: str = "read") -> uuid.UUID:
    execution = uuid.uuid4()
    sql_rows(
        "INSERT INTO curie.action_executions (id, kind, agent_id, connector, tool, "
        "connector_digest, authority_kind, authority_ref, idempotency_key, not_before) "
        "VALUES (:id, :kind, :agent_id, 'example-metrics', 'query_value', :digest, "
        "'policy', 'policy:example', :key, now())",
        {
            "id": execution,
            "kind": kind,
            "agent_id": agent_id,
            "digest": DIGEST,
            "key": f"read:{execution}",
        },
    )
    return execution


def _snapshot(row_id: uuid.UUID) -> dict[str, Any]:
    rows = sql_dicts("SELECT * FROM curie.action_executions WHERE id = :id", {"id": row_id})
    assert len(rows) == 1
    return rows[0]


def test_one_hand_written_revision_on_the_ledger_fields_head() -> None:
    """@spec AUTOMATED-REMEDIATION-12: one additive revision, directly on the stack's head."""

    revision, down = _revision()
    assert down == BELOW, f"revision {revision} revises {down}, expected {BELOW}"
    assert revision == HEAD
    script = ScriptDirectory.from_config(alembic_config())
    assert script.get_heads() == [HEAD]


def test_the_upgrade_adds_not_before_and_the_sample(
    isolated_migration_db: IsolatedMigrationDb,
) -> None:
    """@spec AUTOMATED-REMEDIATION-12: a nullable timestamp and a nullable JSON sample."""

    revision, _ = _revision()
    isolated_migration_db.at(revision)

    assert NEW_COLUMNS <= column_names("action_executions")
    columns = {
        row["column_name"]: row
        for row in sql_dicts(
            "SELECT column_name, data_type, is_nullable FROM information_schema.columns "
            "WHERE table_schema = 'curie' AND table_name = 'action_executions'"
        )
    }
    assert columns["not_before"]["data_type"] == "timestamp with time zone"
    assert columns["not_before"]["is_nullable"] == "YES"
    assert columns["sample"]["data_type"] == "jsonb"
    assert columns["sample"]["is_nullable"] == "YES"


def test_after_the_upgrade_a_read_execution_is_accepted_and_an_unknown_kind_is_not(
    isolated_migration_db: IsolatedMigrationDb,
) -> None:
    """@spec AUTOMATED-REMEDIATION-12: "the check constraint is replaced in the same migration"."""

    revision, _ = _revision()
    isolated_migration_db.at(revision)
    agent_id = _agent()

    read = _read(agent_id)
    assert _snapshot(read)["kind"] == "read"
    with pytest.raises(IntegrityError) as raised:
        _read(agent_id, kind="example_unknown")
    assert "check constraint" in str(raised.value).lower()


def test_legacy_rows_survive_the_upgrade_unchanged(
    isolated_migration_db: IsolatedMigrationDb,
) -> None:
    """@spec AUTOMATED-REMEDIATION-12: existing executions keep every value; no backfill.

    A legacy execution reads ``not_before`` NULL, which the claim route treats
    as due (executor amendment E9 makes the column nullable for that reason).
    """

    revision, down = _revision()
    isolated_migration_db.at(down)
    probe = _probe(_agent())
    before = _snapshot(probe)

    command.upgrade(alembic_config(), revision)

    after = _snapshot(probe)
    assert {key: after[key] for key in before} == before
    assert after["not_before"] is None
    assert after["sample"] is None


def test_upgrade_downgrade_upgrade_round_trip(isolated_migration_db: IsolatedMigrationDb) -> None:
    """@spec AUTOMATED-REMEDIATION-12: real Postgres upgrade, downgrade and upgrade.

    The downgrade drops the two columns and restores the kind check without
    ``read``; the other executions stay.
    """

    revision, down = _revision()
    cfg = alembic_config()
    isolated_migration_db.at(down)
    agent_id = _agent()
    probe = _probe(agent_id)

    command.upgrade(cfg, revision)
    command.downgrade(cfg, down)

    assert not (NEW_COLUMNS & column_names("action_executions"))
    assert _snapshot(probe)["kind"] == "probe"
    with pytest.raises(IntegrityError) as raised:
        sql_rows(
            "INSERT INTO curie.action_executions (id, kind, agent_id, connector, "
            "connector_digest, authority_kind, authority_ref, idempotency_key) VALUES "
            "(:id, 'read', :agent_id, 'example-metrics', :digest, 'policy', 'policy:example', "
            ":key)",
            {"id": uuid.uuid4(), "agent_id": agent_id, "digest": DIGEST, "key": "read:down"},
        )
    assert "check constraint" in str(raised.value).lower()

    command.upgrade(cfg, revision)
    assert NEW_COLUMNS <= column_names("action_executions")
    assert _snapshot(_read(agent_id))["kind"] == "read"
