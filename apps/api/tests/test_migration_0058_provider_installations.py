"""Migration 0058 adds curie.provider_installations (#2909)."""

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

# Every constraint the table must carry under a stable name: the API classifies
# IntegrityError by constraint name, so a rename would silently turn a 409/422
# into a 500.
NAMED_CONSTRAINTS = {
    "provider_installations_pkey",
    "provider_installations_provider_ck",
    "provider_installations_status_ck",
    "provider_installations_disconnected_at_ck",
    "provider_installations_credential_ref_ck",
    "provider_installations_webhook_verification_ref_ck",
    "provider_installations_tenant_provider_external_key",
    "provider_installations_tenant_id_fkey",
    "provider_installations_installer_fkey",
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


def _constraint_names() -> set[str]:
    rows = _sql(
        "SELECT conname FROM pg_constraint "
        "WHERE conrelid = 'curie.provider_installations'::regclass"
    )
    return {row["conname"] for row in rows}


def test_0058_revises_0057() -> None:
    script = ScriptDirectory.from_config(_config())
    revision = script.get_revision("0058")
    assert revision is not None
    assert revision.down_revision == "0057"


def test_0058_round_trip_creates_and_drops_provider_installations(
    isolated_migration_db: None,
) -> None:
    config = _config()
    command.upgrade(config, "head")
    assert _regclass("provider_installations") is not None
    try:
        command.downgrade(config, "0057")
        assert _regclass("provider_installations") is None
        # The FK targets from 0051 and 0057 must outlive the downgrade.
        assert _regclass("tenants") is not None
        assert _regclass("principals") is not None
    finally:
        # A failed assertion must not leave this private database below head.
        command.upgrade(config, "head")
    assert _regclass("provider_installations") is not None
    assert NAMED_CONSTRAINTS <= _constraint_names()
