"""Migration 0121 adds curie.agent_access_policies (#4585, ADR 0220 section 8).

Expand-only: one new, empty table, so no existing agent gains a policy and
every agent keeps today's behavior. The downgrade drops the table, rows
included.
"""

from __future__ import annotations

from _migration_support import IsolatedMigrationDb, alembic_config, sql_dicts
from alembic import command
from alembic.script import ScriptDirectory

NAMED_CONSTRAINTS = {
    "agent_access_policies_pkey",
    "agent_access_policies_version_ck",
    "agent_access_policies_agent_fkey",
    "agent_access_policies_invocation_team_fkey",
    "agent_access_policies_approver_team_fkey",
}


def _regclass() -> str | None:
    (row,) = sql_dicts("SELECT to_regclass('curie.agent_access_policies')::text AS name")
    return row["name"]


def _constraints() -> set[str]:
    rows = sql_dicts(
        "SELECT conname FROM pg_constraint "
        "WHERE conrelid = to_regclass('curie.agent_access_policies')"
    )
    return {row["conname"] for row in rows}


def test_0121_revises_0120() -> None:
    revision = ScriptDirectory.from_config(alembic_config()).get_revision("0121")
    assert revision is not None
    assert revision.down_revision == "0120"


def test_0121_round_trip(isolated_migration_db: IsolatedMigrationDb) -> None:
    config = alembic_config()
    isolated_migration_db.at("0121")
    assert _regclass() is not None
    assert NAMED_CONSTRAINTS <= _constraints()
    try:
        command.downgrade(config, "0120")
        assert _regclass() is None
    finally:
        command.upgrade(config, "head")
