"""Migration 0054 tenant-scopes agents, bindings, versions and deployments (#2911)."""

from __future__ import annotations

import asyncio
import uuid
from pathlib import Path
from typing import Any

from alembic import command
from alembic.config import Config
from alembic.script import ScriptDirectory
from curie_api.config import get_settings
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

ALEMBIC_DIR = Path(__file__).resolve().parents[1] / "alembic"
DEFAULT_TENANT_UUID = uuid.UUID("00000000-0000-0000-0000-000000000001")

SCOPED_TABLES = ("agents", "agent_channels", "agent_versions", "deployments")

NEW_COLUMNS = {
    "agents": {
        "tenant_id",
        "status",
        "owning_team_id",
        "topic_policy_ref",
        "data_classification_ref",
        "retention_policy_ref",
    },
    "agent_channels": {"tenant_id", "provider_installation_id", "topic_id"},
    "agent_versions": {"tenant_id"},
    "deployments": {"tenant_id"},
}

# Stable names: the API classifies IntegrityError by constraint name.
NAMED_CONSTRAINTS = {
    "agents": {"agents_tenant_id_fkey", "agents_status_ck", "agents_owning_team_fkey"},
    "agent_channels": {
        "agent_channels_tenant_id_fkey",
        "agent_channels_provider_installation_fkey",
    },
    "agent_versions": {"agent_versions_tenant_id_fkey"},
    "deployments": {"deployments_tenant_id_fkey"},
    "provider_installations": {"provider_installations_tenant_id_id_key"},
}


def _config() -> Config:
    config = Config()
    config.set_main_option("script_location", str(ALEMBIC_DIR))
    return config


def _sql(statement: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    async def run() -> list[dict[str, Any]]:
        engine = create_async_engine(get_settings().database_url)
        try:
            async with engine.begin() as connection:
                result = await connection.execute(text(statement), params or {})
                if not result.returns_rows:
                    return []
                return [dict(row) for row in result.mappings().all()]
        finally:
            await engine.dispose()

    return asyncio.run(run())


def _regclass(table: str) -> str | None:
    rows = _sql("SELECT to_regclass(:name)::text AS name", {"name": f"curie.{table}"})
    name = rows[0]["name"]
    return None if name is None else str(name)


def _columns(table: str) -> dict[str, str]:
    rows = _sql(
        "SELECT column_name, is_nullable FROM information_schema.columns "
        "WHERE table_schema = 'curie' AND table_name = :table",
        {"table": table},
    )
    return {row["column_name"]: row["is_nullable"] for row in rows}


def _constraint_names(table: str) -> set[str]:
    rows = _sql(
        "SELECT conname FROM pg_constraint WHERE conrelid = CAST(:table AS regclass)",
        {"table": f"curie.{table}"},
    )
    return {row["conname"] for row in rows}


def _seed_pre_0054_rows() -> dict[str, uuid.UUID]:
    """One agent with a binding, version and deployment, written the 0053 way."""
    ids = {name: uuid.uuid4() for name in SCOPED_TABLES}
    token = ids["agents"].hex[:8]
    _sql(
        "INSERT INTO curie.agents (id, name) VALUES (:id, :name)",
        {"id": ids["agents"], "name": f"mig-0054-{token}"},
    )
    _sql(
        "INSERT INTO curie.agent_channels (id, agent_id, kind, address) "
        "VALUES (:id, :agent_id, 'slack', :address)",
        {"id": ids["agent_channels"], "agent_id": ids["agents"], "address": f"C-{token}"},
    )
    _sql(
        "INSERT INTO curie.agent_versions (id, agent_id, version_label, created_by) "
        "VALUES (:id, :agent_id, 'v1', 'test')",
        {"id": ids["agent_versions"], "agent_id": ids["agents"]},
    )
    _sql(
        "INSERT INTO curie.deployments (id, agent_id, version_id, environment, status) "
        "VALUES (:id, :agent_id, :version_id, 'prod', 'active')",
        {
            "id": ids["deployments"],
            "agent_id": ids["agents"],
            "version_id": ids["agent_versions"],
        },
    )
    return ids


def test_0054_revises_0053() -> None:
    script = ScriptDirectory.from_config(_config())
    revision = script.get_revision("0054")
    assert revision is not None
    assert revision.down_revision == "0053"


def test_0054_backfills_existing_rows_and_round_trips(isolated_migration_db: None) -> None:
    config = _config()
    command.upgrade(config, "head")
    try:
        command.downgrade(config, "0053")
        ids = _seed_pre_0054_rows()

        command.upgrade(config, "0054")

        for table in SCOPED_TABLES:
            (row,) = _sql(f"SELECT tenant_id FROM curie.{table} WHERE id = :id", {"id": ids[table]})
            assert row["tenant_id"] == DEFAULT_TENANT_UUID, table
            columns = _columns(table)
            assert NEW_COLUMNS[table] <= set(columns), table
            assert columns["tenant_id"] == "NO", table
        (agent,) = _sql(
            "SELECT status, owning_team_id FROM curie.agents WHERE id = :id",
            {"id": ids["agents"]},
        )
        assert agent["status"] == "active"
        assert agent["owning_team_id"] is None
        assert _columns("agents")["status"] == "NO"
        (channel,) = _sql(
            "SELECT provider_installation_id, topic_id FROM curie.agent_channels WHERE id = :id",
            {"id": ids["agent_channels"]},
        )
        assert channel == {"provider_installation_id": None, "topic_id": None}
        for table, names in NAMED_CONSTRAINTS.items():
            assert names <= _constraint_names(table), table

        # N-1 writers (API/worker pods still on 0053 code) insert without any
        # tenant column; the server default must keep those inserts working.
        n1 = _seed_pre_0054_rows()
        for table in SCOPED_TABLES:
            (row,) = _sql(f"SELECT tenant_id FROM curie.{table} WHERE id = :id", {"id": n1[table]})
            assert row["tenant_id"] == DEFAULT_TENANT_UUID, table
        (n1_agent,) = _sql("SELECT status FROM curie.agents WHERE id = :id", {"id": n1["agents"]})
        assert n1_agent["status"] == "active"

        command.downgrade(config, "0053")
        for table in SCOPED_TABLES:
            assert not NEW_COLUMNS[table] & set(_columns(table)), table
        assert "provider_installations_tenant_id_id_key" not in _constraint_names(
            "provider_installations"
        )
        # 0051-0053's tables are FK targets and must outlive the downgrade.
        for table in ("tenants", "teams", "provider_installations"):
            assert _regclass(table) is not None, table
    finally:
        # A failed assertion must not leave this private database below head.
        command.upgrade(config, "head")
