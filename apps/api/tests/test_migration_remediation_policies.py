"""The remediation policy migration: schema and a real Postgres round trip.

AUTOMATED-REMEDIATION-2 (docs/superpowers/specs/2026-10-07-automated-remediation.md)
adds one additive, hand-written revision with ``remediation_policies`` (one row per
bound hook, a positive generation) and ``remediation_policy_generations`` (one
immutable row per generation, kept while the agent exists, enforced by a trigger
like the one on ``hook_source_operations``).

The revision number is the next free one on ``next`` at implementation time,
checked against ``main``, so this file does not hard-code it: it finds the one
revision whose source creates the generations table and asserts it sits on the
current ``next`` head below it (0086 when this was written) and is the head.
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
from sqlalchemy.exc import DBAPIError

# The head of next when the tests were written; the new revision revises it.
BELOW = "0086"

POLICY_COLUMNS = {"agent_id", "hook", "generation", "operation_id", "armed", "active", "updated_at"}
GENERATION_COLUMNS = {
    "agent_id",
    "hook",
    "generation",
    "operation_id",
    "intent_sha256",
    "document",
    "armed",
    "active",
    "bound_by",
    "created_at",
}
DOCUMENT = {"route": "sre-oncall", "limits": {}, "actions": []}


def _revision() -> tuple[str, str]:
    """``(revision, down_revision)`` of the one revision adding the policy tables."""

    script = ScriptDirectory.from_config(alembic_config())
    found = [
        rev
        for rev in script.walk_revisions()
        if rev.path and "remediation_policy_generations" in Path(rev.path).read_text()
    ]
    assert len(found) == 1, (
        "expected exactly one alembic revision creating remediation_policy_generations "
        f"(AUTOMATED-REMEDIATION-2), found {[rev.revision for rev in found]}"
    )
    rev = found[0]
    assert isinstance(rev.down_revision, str), rev.down_revision
    return rev.revision, rev.down_revision


def _table_exists(table: str) -> bool:
    return bool(
        sql_rows(
            "SELECT 1 FROM information_schema.tables "
            "WHERE table_schema = 'curie' AND table_name = :t",
            {"t": table},
        )
    )


def _primary_key(table: str) -> list[str]:
    rows = sql_rows(
        "SELECT a.attname FROM pg_index i "
        "JOIN pg_class t ON t.oid = i.indrelid "
        "JOIN pg_namespace n ON n.oid = t.relnamespace "
        "JOIN unnest(i.indkey) WITH ORDINALITY AS k(attnum, ord) ON true "
        "JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = k.attnum "
        "WHERE n.nspname = 'curie' AND t.relname = :t AND i.indisprimary "
        "ORDER BY k.ord",
        {"t": table},
    )
    return [row[0] for row in rows]


def _insert_agent() -> uuid.UUID:
    agent_id = uuid.uuid4()
    sql_rows(
        "INSERT INTO curie.agents (id, name) VALUES (:id, :name)",
        {"id": agent_id, "name": f"remediation-{agent_id.hex[:8]}"},
    )
    return agent_id


def _insert_policy(agent_id: uuid.UUID, *, generation: int = 1) -> None:
    sql_rows(
        "INSERT INTO curie.remediation_policies "
        "(agent_id, hook, generation, operation_id, armed, active, updated_at) "
        "VALUES (:agent_id, 'alerts', :generation, :operation_id, false, true, now())",
        {"agent_id": agent_id, "generation": generation, "operation_id": uuid.uuid4()},
    )


def _insert_generation(agent_id: uuid.UUID, *, generation: int = 1) -> None:
    sql_rows(
        "INSERT INTO curie.remediation_policy_generations "
        "(agent_id, hook, generation, operation_id, intent_sha256, document, armed, "
        "active, bound_by, created_at) "
        "VALUES (:agent_id, 'alerts', :generation, :operation_id, :intent, "
        "CAST(:document AS jsonb), false, true, 'U0EXAMPLE1', now())",
        {
            "agent_id": agent_id,
            "generation": generation,
            "operation_id": uuid.uuid4(),
            "intent": "ab" * 32,
            "document": json.dumps(DOCUMENT),
        },
    )


def _generations(agent_id: uuid.UUID) -> list[dict[str, Any]]:
    return sql_dicts(
        "SELECT generation, bound_by, armed, active FROM curie.remediation_policy_generations "
        "WHERE agent_id = :agent_id ORDER BY generation",
        {"agent_id": agent_id},
    )


def test_one_hand_written_revision_on_the_next_head() -> None:
    """@spec AUTOMATED-REMEDIATION-2: one additive revision, directly on next's head."""

    revision, down = _revision()
    assert down == BELOW, f"revision {revision} revises {down}, expected {BELOW}"
    script = ScriptDirectory.from_config(alembic_config())
    assert script.get_heads() == [revision]


def test_the_upgrade_creates_both_tables_with_their_keys(
    isolated_migration_db: IsolatedMigrationDb,
) -> None:
    """@spec AUTOMATED-REMEDIATION-2"""

    revision, _ = _revision()
    isolated_migration_db.at(revision)

    assert POLICY_COLUMNS <= column_names("remediation_policies")
    assert GENERATION_COLUMNS <= column_names("remediation_policy_generations")
    assert _primary_key("remediation_policies") == ["agent_id", "hook"]
    assert _primary_key("remediation_policy_generations") == ["agent_id", "hook", "generation"]
    types = {
        row["column_name"]: row["data_type"]
        for row in sql_dicts(
            "SELECT column_name, data_type FROM information_schema.columns "
            "WHERE table_schema = 'curie' AND table_name = 'remediation_policy_generations'"
        )
    }
    assert types["document"] == "jsonb"
    assert types["generation"] == "bigint"


def test_generations_are_positive(isolated_migration_db: IsolatedMigrationDb) -> None:
    """@spec AUTOMATED-REMEDIATION-2: ``generation BIGINT > 0`` on both tables."""

    revision, _ = _revision()
    isolated_migration_db.at(revision)
    agent_id = _insert_agent()

    with pytest.raises(DBAPIError):
        _insert_policy(agent_id, generation=0)
    with pytest.raises(DBAPIError):
        _insert_generation(agent_id, generation=0)


def test_generation_rows_are_immutable_while_the_agent_exists(
    isolated_migration_db: IsolatedMigrationDb,
) -> None:
    """@spec AUTOMATED-REMEDIATION-2

    A trigger refuses deleting or rewriting a generation row while its agent
    exists; deleting the agent removes the policy and its generations.
    """

    revision, _ = _revision()
    isolated_migration_db.at(revision)
    agent_id = _insert_agent()
    _insert_policy(agent_id)
    _insert_generation(agent_id)
    before = _generations(agent_id)

    with pytest.raises(DBAPIError):
        sql_rows(
            "DELETE FROM curie.remediation_policy_generations WHERE agent_id = :agent_id",
            {"agent_id": agent_id},
        )
    with pytest.raises(DBAPIError):
        sql_rows(
            "UPDATE curie.remediation_policy_generations SET bound_by = 'U0EXAMPLE2' "
            "WHERE agent_id = :agent_id",
            {"agent_id": agent_id},
        )
    assert _generations(agent_id) == before

    sql_rows("DELETE FROM curie.agents WHERE id = :id", {"id": agent_id})
    assert _generations(agent_id) == []
    assert sql_rows(
        "SELECT 1 FROM curie.remediation_policies WHERE agent_id = :agent_id",
        {"agent_id": agent_id},
    ) == []


def test_upgrade_downgrade_upgrade_round_trip(isolated_migration_db: IsolatedMigrationDb) -> None:
    """@spec AUTOMATED-REMEDIATION-2: real Postgres upgrade, downgrade and upgrade.

    Additive: the downgrade drops exactly the new tables (and their trigger
    function) and leaves existing rows alone; the second upgrade recreates them.
    """

    revision, down = _revision()
    cfg = alembic_config()
    isolated_migration_db.at(down)
    agent_id = _insert_agent()
    assert not _table_exists("remediation_policies")

    command.upgrade(cfg, revision)
    assert _table_exists("remediation_policies")
    assert _table_exists("remediation_policy_generations")
    _insert_policy(agent_id)
    _insert_generation(agent_id)

    command.downgrade(cfg, down)
    assert not _table_exists("remediation_policies")
    assert not _table_exists("remediation_policy_generations")
    assert sql_rows("SELECT 1 FROM curie.agents WHERE id = :id", {"id": agent_id}) != []

    command.upgrade(cfg, revision)
    assert POLICY_COLUMNS <= column_names("remediation_policies")
    assert GENERATION_COLUMNS <= column_names("remediation_policy_generations")
    assert _generations(agent_id) == []
    _insert_policy(agent_id)
    _insert_generation(agent_id)
    assert len(_generations(agent_id)) == 1
