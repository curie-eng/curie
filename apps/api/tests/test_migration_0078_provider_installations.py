"""Migration 0083 adds curie.provider_installations and curie.channel_identities (#2909)."""

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

# Every constraint each table must carry under a stable name: the API
# classifies IntegrityError by constraint name, so a rename would silently
# turn a 409/422 into a 500.
NAMED_INSTALLATION_CONSTRAINTS = {
    "provider_installations_pkey",
    "provider_installations_provider_ck",
    "provider_installations_status_ck",
    "provider_installations_disconnected_at_ck",
    "provider_installations_tenant_provider_authority_account_key",
    "provider_installations_tenant_provider_id_key",
    "provider_installations_tenant_id_fkey",
    "provider_installations_installer_fkey",
}
NAMED_IDENTITY_CONSTRAINTS = {
    "channel_identities_pkey",
    "channel_identities_provider_ck",
    "channel_identities_status_ck",
    "channel_identities_credential_ref_ck",
    "channel_identities_webhook_verification_ref_ck",
    "channel_identities_tenant_provider_name_key",
    "channel_identities_tenant_id_fkey",
    "channel_identities_installation_fkey",
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


def _constraint_names(table: str) -> set[str]:
    rows = _sql(
        "SELECT conname FROM pg_constraint WHERE conrelid = to_regclass(:table)",
        {"table": f"curie.{table}"},
    )
    return {row["conname"] for row in rows}


def test_0083_revises_0082() -> None:
    script = ScriptDirectory.from_config(_config())
    revision = script.get_revision("0083")
    assert revision is not None
    assert revision.down_revision == "0082"


def test_0083_round_trip_creates_and_drops_both_tables(
    isolated_migration_db: None,
) -> None:
    config = _config()
    command.upgrade(config, "head")
    assert _regclass("provider_installations") is not None
    assert _regclass("channel_identities") is not None
    try:
        command.downgrade(config, "0082")
        assert _regclass("channel_identities") is None
        assert _regclass("provider_installations") is None
        # The FK targets from 0051 (tenants) and 0057 (principals) must outlive the downgrade.
        assert _regclass("tenants") is not None
        assert _regclass("principals") is not None
    finally:
        # A failed assertion must not leave this private database below head.
        command.upgrade(config, "head")
    assert _regclass("provider_installations") is not None
    assert _regclass("channel_identities") is not None
    assert NAMED_INSTALLATION_CONSTRAINTS <= _constraint_names("provider_installations")
    assert NAMED_IDENTITY_CONSTRAINTS <= _constraint_names("channel_identities")


def test_channel_identity_can_only_attach_to_its_own_tenant_and_provider(
    isolated_migration_db: None,
) -> None:
    """The composite FK, not just application code, refuses a cross-tenant or
    cross-provider attach -- a direct SQL insert proves the database itself
    enforces it."""

    config = _config()
    command.upgrade(config, "head")
    _sql(
        "INSERT INTO curie.tenants (id, deployment_id, status) VALUES "
        "('00000000-0000-0000-0000-00000000aaaa', 'adr0193-tenant-a', 'active'), "
        "('00000000-0000-0000-0000-00000000bbbb', 'adr0193-tenant-b', 'active')"
    )
    _sql(
        "INSERT INTO curie.provider_installations "
        "(id, tenant_id, provider, authority, external_account_id) VALUES "
        "('00000000-0000-0000-0000-00000000cccc', "
        "'00000000-0000-0000-0000-00000000aaaa', 'slack', '', 'T1')"
    )
    try:
        _sql(
            "INSERT INTO curie.channel_identities "
            "(id, tenant_id, provider, name, provider_installation_id) VALUES "
            "('00000000-0000-0000-0000-00000000dddd', "
            "'00000000-0000-0000-0000-00000000bbbb', 'slack', 'cross-tenant', "
            "'00000000-0000-0000-0000-00000000cccc')"
        )
    except Exception as exc:  # noqa: BLE001 -- asserting on the real driver error
        assert "channel_identities_installation_fkey" in str(exc)
    else:
        raise AssertionError("cross-tenant attach should have violated the composite FK")
