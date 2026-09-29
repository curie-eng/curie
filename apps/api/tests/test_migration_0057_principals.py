"""Migration 0057 adds curie.principals, curie.teams and curie.principal_teams."""

from __future__ import annotations

from _migration_support import IsolatedMigrationDb, alembic_config, sql_dicts
from alembic import command

NEW_TABLES = ("principals", "teams", "principal_teams")


def _regclass(table: str) -> str | None:
    rows = sql_dicts("SELECT to_regclass(:name)::text AS name", {"name": f"curie.{table}"})
    name = rows[0]["name"]
    return None if name is None else str(name)


def _present(tables: tuple[str, ...]) -> dict[str, bool]:
    return {table: _regclass(table) is not None for table in tables}


def test_0057_round_trip_creates_and_drops_principal_tables(
    isolated_migration_db: IsolatedMigrationDb,
) -> None:
    config = alembic_config()
    isolated_migration_db.at("head")
    assert _present(NEW_TABLES) == {table: True for table in NEW_TABLES}
    try:
        command.downgrade(config, "0056")
        assert _present(NEW_TABLES) == {table: False for table in NEW_TABLES}
        # 0051's tenants table is the FK target and must outlive the downgrade.
        assert _regclass("tenants") is not None
    finally:
        # A failed assertion must not leave this private database below head.
        command.upgrade(config, "head")
    assert _present(NEW_TABLES) == {table: True for table in NEW_TABLES}
