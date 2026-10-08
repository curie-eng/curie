"""Migration 0099 adds the per-agent runner step cap (#4175)."""

from __future__ import annotations

import uuid

import pytest
from _migration_support import IsolatedMigrationDb, alembic_config, column_names, sql_dicts
from alembic import command
from sqlalchemy.exc import DBAPIError

BELOW = "0098"


def _insert_agent(max_turns: int | None) -> None:
    agent_id = uuid.uuid4()
    sql_dicts(
        "INSERT INTO curie.agents (id, name, max_turns) VALUES (:id, :name, :max_turns)",
        {"id": agent_id, "name": f"m0099-{agent_id.hex[:8]}", "max_turns": max_turns},
    )


def _rejected(max_turns: int) -> None:
    with pytest.raises(DBAPIError) as excinfo:
        _insert_agent(max_turns)
    assert getattr(excinfo.value.orig, "sqlstate", None) == "23514"


def test_0099_bounds_the_step_cap_and_downgrade_drops_it(
    isolated_migration_db: IsolatedMigrationDb,
) -> None:
    config = alembic_config()
    isolated_migration_db.at("head")
    assert "max_turns" in column_names("agents")
    _insert_agent(None)
    _insert_agent(1)
    _insert_agent(1000)
    _rejected(0)
    _rejected(1001)
    try:
        command.downgrade(config, BELOW)
        assert "max_turns" not in column_names("agents")
    finally:
        # A failed assertion must not leave this private database below head.
        command.upgrade(config, "head")
    assert "max_turns" in column_names("agents")
