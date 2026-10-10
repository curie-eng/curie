"""The remediation qualification migration (plan task 15).

@spec AUTOMATED-REMEDIATION-22

AUTOMATED-REMEDIATION-22 (docs/superpowers/specs/2026-10-07-automated-remediation.md):
a ``remediation_qualifications`` row records, for one ``(agent_id, connector,
tool, connector_digest, verifier declaration digest)``, the evidence the API
checked when the record was written. The verifier runs it references live
beside it in ``remediation_qualification_verifier_runs``.

Revision: ``0094`` on plan task 9's admission revision ``0093a``. If another
revision lands first, renumber and move ``BELOW`` and ``HEAD`` here and the
head pins. The revision is otherwise found by its source (it creates
``remediation_qualifications``), so a renumbering only moves the two constants.
"""

from __future__ import annotations

import uuid
from pathlib import Path

import pytest
from _migration_support import IsolatedMigrationDb, alembic_config, sql_dicts, sql_rows
from alembic import command
from alembic.script import ScriptDirectory
from sqlalchemy.exc import DBAPIError

BELOW = "0093a"
HEAD = "0094"
TABLES = ("remediation_qualifications", "remediation_qualification_verifier_runs")


def _revision() -> tuple[str, str]:
    script = ScriptDirectory.from_config(alembic_config())
    found = [
        rev
        for rev in script.walk_revisions()
        if rev.path
        and "create_table" in (source := Path(rev.path).read_text())
        and '"remediation_qualifications"' in source
    ]
    assert len(found) == 1, (
        "no single revision creates remediation_qualifications "
        f"(AUTOMATED-REMEDIATION-22): {[rev.revision for rev in found]}"
    )
    rev = found[0]
    assert isinstance(rev.down_revision, str), rev.down_revision
    return rev.revision, rev.down_revision


def _tables() -> set[str]:
    return {
        row["table_name"]
        for row in sql_dicts(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'curie'"
        )
    }


def _columns(table: str) -> set[str]:
    return {
        row["column_name"]
        for row in sql_dicts(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = 'curie' AND table_name = :t",
            {"t": table},
        )
    }


def test_one_hand_written_revision_on_the_assumed_parent() -> None:
    """@spec AUTOMATED-REMEDIATION-22: one revision, ``HEAD`` on ``BELOW``; task 12's
    ``0095`` (remediation escalations) and task 16's ``0096`` (the tune
    refusal) are above it; ``0097`` (receipts) and ``0098`` (driver audit) follow it.
    #2911's tenant scope (``0101``) is the single head.
    """

    revision, down = _revision()
    assert (revision, down) == (HEAD, BELOW)
    assert ScriptDirectory.from_config(alembic_config()).get_heads() == ["0101"]


def test_the_record_is_keyed_by_what_it_qualifies(
    isolated_migration_db: IsolatedMigrationDb,
) -> None:
    """@spec AUTOMATED-REMEDIATION-22: the row names the agent, connector, tool,
    connector digest and verifier declaration digest, the operator principal that
    recorded it, the worst case statement and its evidence references; the run
    table carries the run's start the schedule is anchored on.
    """

    revision, _ = _revision()
    isolated_migration_db.at(revision)

    assert set(TABLES) <= _tables()
    assert {
        "id",
        "agent_id",
        "connector",
        "tool",
        "connector_digest",
        "verifier_sha256",
        "reversibility",
        "recorded_by",
        "worst_case",
        "evidence",
        "created_at",
    } <= _columns("remediation_qualifications")
    assert {"id", "agent_id", "qualification_id", "started_by", "started_at", "outcome"} <= (
        _columns("remediation_qualification_verifier_runs")
    )


def test_a_record_cascades_with_its_agent(isolated_migration_db: IsolatedMigrationDb) -> None:
    """@spec AUTOMATED-REMEDIATION-22: records are local to one agent; deleting the
    agent deletes them (the suite truncates agents between tests).
    """

    revision, _ = _revision()
    isolated_migration_db.at(revision)
    agent_id = uuid.uuid4()
    sql_rows(
        "INSERT INTO curie.agents (id, name) VALUES (:id, :name)",
        {"id": agent_id, "name": f"qual-{agent_id.hex[:6]}"},
    )
    sql_rows(
        "INSERT INTO curie.remediation_qualifications "
        "(id, agent_id, connector, tool, connector_digest, verifier_sha256, reversibility, "
        "recorded_by, worst_case, evidence) VALUES (:id, :agent_id, 'k8s', 'scale_deployment', "
        ":digest, :verifier, 'reversible', 'U0EXAMPLE7', 'worst case', '{}'::jsonb)",
        {
            "id": uuid.uuid4(),
            "agent_id": agent_id,
            "digest": "sha256:" + "ab" * 32,
            "verifier": "cd" * 32,
        },
    )

    sql_rows("DELETE FROM curie.agents WHERE id = :id", {"id": agent_id})

    assert sql_dicts("SELECT * FROM curie.remediation_qualifications") == []


def test_a_worst_case_statement_over_2000_characters_violates_a_check(
    isolated_migration_db: IsolatedMigrationDb,
) -> None:
    """@spec AUTOMATED-REMEDIATION-22: "a worst case statement (text, at most 2000
    characters)", held by the database too.
    """

    revision, _ = _revision()
    isolated_migration_db.at(revision)
    agent_id = uuid.uuid4()
    sql_rows(
        "INSERT INTO curie.agents (id, name) VALUES (:id, :name)",
        {"id": agent_id, "name": f"qual-{agent_id.hex[:6]}"},
    )
    with pytest.raises(DBAPIError):
        sql_rows(
            "INSERT INTO curie.remediation_qualifications "
            "(id, agent_id, connector, tool, connector_digest, verifier_sha256, reversibility, "
            "recorded_by, worst_case, evidence) VALUES (:id, :agent_id, 'k8s', "
            "'scale_deployment', :digest, :verifier, 'reversible', 'U0EXAMPLE7', :worst, "
            "'{}'::jsonb)",
            {
                "id": uuid.uuid4(),
                "agent_id": agent_id,
                "digest": "sha256:" + "ab" * 32,
                "verifier": "cd" * 32,
                "worst": "x" * 2001,
            },
        )


def test_upgrade_downgrade_upgrade_round_trip(isolated_migration_db: IsolatedMigrationDb) -> None:
    """@spec AUTOMATED-REMEDIATION-22: real Postgres upgrade, downgrade and upgrade."""

    revision, down = _revision()
    cfg = alembic_config()
    isolated_migration_db.at(down)
    assert not set(TABLES) & _tables()

    command.upgrade(cfg, revision)
    assert set(TABLES) <= _tables()
    command.downgrade(cfg, down)
    assert not set(TABLES) & _tables()
    command.upgrade(cfg, revision)
    assert set(TABLES) <= _tables()
