"""Migration 0151 adds the connector requester identity flag. @spec CONNECTOR-REQUESTER-1.

Upgrade gives every existing agent the flag off at version 1: both columns are
NOT NULL with server defaults, and the version carries a CHECK of at least 1.
The downgrade to 0149 drops exactly the two columns and keeps the agent rows.
"""

from __future__ import annotations

import uuid
from typing import Any

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
from alembic.script import ScriptDirectory
from sqlalchemy.exc import IntegrityError

BELOW = "0149"
REVISION = "0151"
FLAG = "connector_requester_identity"
VERSION = "connector_requester_identity_version"


def revision_ready() -> None:
    """An absent migration is a body assertion, not a setup error."""
    graph = ScriptDirectory.from_config(alembic_config())
    assert REVISION in {r.revision for r in graph.walk_revisions()}, (
        "connector requester identity migration 0151 is missing"
    )
    assert graph.get_revision(REVISION).down_revision == BELOW


def _agent() -> uuid.UUID:
    """A synthetic agent row using only columns that exist at both revisions."""
    agent_id = uuid.uuid4()
    sql_rows(
        "INSERT INTO curie.agents (id, name) VALUES (:id, :name)",
        {"id": agent_id, "name": f"synthetic-requester-{agent_id.hex[:8]}"},
    )
    return agent_id


def _flag_row(agent_id: uuid.UUID) -> dict[str, Any]:
    rows = sql_dicts(
        f"SELECT {FLAG} AS flag, {VERSION} AS version FROM curie.agents WHERE id = :id",
        {"id": agent_id},
    )
    assert len(rows) == 1, rows
    return rows[0]


def _columns() -> dict[str, dict[str, Any]]:
    return {
        row["column_name"]: row
        for row in sql_dicts(
            "SELECT column_name, data_type, is_nullable, column_default "
            "FROM information_schema.columns WHERE table_schema = 'curie' "
            "AND table_name = 'agents' AND column_name IN (:flag, :version)",
            {"flag": FLAG, "version": VERSION},
        )
    }


def _agent_checks() -> list[str]:
    return [
        row["definition"]
        for row in sql_dicts(
            "SELECT pg_get_constraintdef(c.oid) AS definition FROM pg_constraint c "
            "JOIN pg_class t ON t.oid = c.conrelid "
            "JOIN pg_namespace n ON n.oid = t.relnamespace "
            "WHERE n.nspname = 'curie' AND t.relname = 'agents' AND c.contype::text = 'c'"
        )
    ]


def test_0151_upgrade_defaults_existing_agents_off_and_downgrade_drops_the_columns(
    isolated_migration_db: IsolatedMigrationDb,
) -> None:
    """@spec CONNECTOR-REQUESTER-1: no existing agent is opted in by the upgrade.

    One isolated database for the whole lifecycle: upgrade, the column checks,
    then the downgrade, which drops even an opted-in row's flag.
    """
    revision_ready()
    config = alembic_config()
    isolated_migration_db.at(BELOW)
    existing = _agent()
    assert {FLAG, VERSION}.isdisjoint(column_names("agents"))

    command.upgrade(config, REVISION)

    assert stamped_revision() == REVISION
    columns = _columns()
    assert set(columns) == {FLAG, VERSION}
    assert columns[FLAG]["data_type"] == "boolean"
    assert columns[FLAG]["is_nullable"] == "NO"
    assert columns[FLAG]["column_default"] == "false"
    assert columns[VERSION]["data_type"] == "integer"
    assert columns[VERSION]["is_nullable"] == "NO"
    assert columns[VERSION]["column_default"] == "1"
    assert any(f"{VERSION} >= 1" in definition for definition in _agent_checks())

    assert _flag_row(existing) == {"flag": False, "version": 1}
    fresh = _agent()
    assert _flag_row(fresh) == {"flag": False, "version": 1}

    # The database refuses malformed flag state; each refusal leaves the row.
    for assignment in (f"{VERSION} = 0", f"{VERSION} = -1", f"{VERSION} = NULL", f"{FLAG} = NULL"):
        with pytest.raises(IntegrityError):
            sql_rows(f"UPDATE curie.agents SET {assignment} WHERE id = :id", {"id": fresh})
        assert _flag_row(fresh) == {"flag": False, "version": 1}, assignment
    sql_rows(f"UPDATE curie.agents SET {FLAG} = true, {VERSION} = 2 WHERE id = :id", {"id": fresh})
    assert _flag_row(fresh) == {"flag": True, "version": 2}

    command.downgrade(config, BELOW)

    assert stamped_revision() == BELOW
    assert {FLAG, VERSION}.isdisjoint(column_names("agents"))
    assert not any(VERSION in definition for definition in _agent_checks())
    kept = sql_dicts(
        "SELECT id FROM curie.agents WHERE id IN (:existing, :fresh)",
        {"existing": existing, "fresh": fresh},
    )
    assert {row["id"] for row in kept} == {existing, fresh}
