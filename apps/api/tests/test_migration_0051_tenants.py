"""Migration 0051 adds curie.tenants and auto-provisions the default tenant."""

from __future__ import annotations

from _migration_support import IsolatedMigrationDb, alembic_config, sql_dicts
from alembic import command

DEFAULT_TENANT_ID = "00000000-0000-0000-0000-000000000001"


def _tenants_regclass() -> str | None:
    rows = sql_dicts("SELECT to_regclass('curie.tenants') AS name")
    name = rows[0]["name"]
    return None if name is None else str(name)


def test_0051_creates_tenants_and_downgrade_drops_it(
    isolated_migration_db: IsolatedMigrationDb,
) -> None:
    config = alembic_config()
    isolated_migration_db.at("head")
    assert _tenants_regclass() is not None
    rows = sql_dicts("SELECT id, status FROM curie.tenants")
    assert len(rows) == 1
    assert str(rows[0]["id"]) == DEFAULT_TENANT_ID
    assert rows[0]["status"] == "active"
    try:
        command.downgrade(config, "0050")
        assert _tenants_regclass() is None
    finally:
        # A failed assertion must not leave this private database below head.
        command.upgrade(config, "head")
