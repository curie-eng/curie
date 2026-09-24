"""Migration 0054 adds curie.identity_links (#2910, ADR 0155 step 5).

It also adds ``UNIQUE (tenant_id, id)`` on provider_installations as the target
of the link's composite tenant FK. The downgrade removes both and leaves the
0053 tables (provider_installations, principals) in place.
"""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

from alembic import command
from alembic.config import Config
from alembic.script import ScriptDirectory
from curie_api.config import get_settings
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

ALEMBIC_DIR = Path(__file__).resolve().parents[1] / "alembic"

# Named because a rename would change what a constraint violation reports.
NAMED_CONSTRAINTS = {
    "identity_links_pkey",
    "identity_links_subject_xor_ck",
}
PRINCIPAL_NATIVE_INDEX = "identity_links_principal_native_key"
INSTALLATION_TENANT_KEY = "provider_installations_tenant_id_id_key"


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


def _constraint_names(table: str) -> set[str]:
    rows = _sql(
        "SELECT conname FROM pg_constraint WHERE conrelid = CAST(:t AS regclass)",
        {"t": f"curie.{table}"},
    )
    return {row["conname"] for row in rows}


def _index(name: str) -> dict[str, Any] | None:
    rows = _sql(
        "SELECT indexdef FROM pg_indexes WHERE schemaname = 'curie' AND indexname = :n",
        {"n": name},
    )
    return rows[0] if rows else None


def test_0054_revises_0053() -> None:
    script = ScriptDirectory.from_config(_config())
    revision = script.get_revision("0054")
    assert revision is not None
    assert revision.down_revision == "0053"


def test_0054_round_trip_creates_and_drops_identity_links(
    isolated_migration_db: None,
) -> None:
    config = _config()
    command.upgrade(config, "head")
    assert _regclass("identity_links") is not None
    assert INSTALLATION_TENANT_KEY in _constraint_names("provider_installations")
    try:
        command.downgrade(config, "0053")
        assert _regclass("identity_links") is None
        assert INSTALLATION_TENANT_KEY not in _constraint_names("provider_installations")
        # The FK targets from 0052/0053 must outlive the downgrade.
        assert _regclass("provider_installations") is not None
        assert _regclass("principals") is not None
        assert _regclass("tenants") is not None
    finally:
        # A failed assertion must not leave this private database below head.
        command.upgrade(config, "head")
    assert _regclass("identity_links") is not None
    assert NAMED_CONSTRAINTS <= _constraint_names("identity_links")
    assert INSTALLATION_TENANT_KEY in _constraint_names("provider_installations")
    index = _index(PRINCIPAL_NATIVE_INDEX)
    assert index is not None
    definition = index["indexdef"]
    # Uniqueness is scoped to principal links only (bot links may share a native id).
    assert "UNIQUE" in definition
    assert "provider_installation_id" in definition and "provider_native_id" in definition
    assert "principal_id IS NOT NULL" in definition
