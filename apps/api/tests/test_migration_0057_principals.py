"""Migration 0057 adds curie.principals, curie.teams and curie.principal_teams."""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

from alembic import command
from alembic.config import Config
from curie_api.config import get_settings
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

ALEMBIC_DIR = Path(__file__).resolve().parents[1] / "alembic"
NEW_TABLES = ("principals", "teams", "principal_teams")


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


def _present(tables: tuple[str, ...]) -> dict[str, bool]:
    return {table: _regclass(table) is not None for table in tables}


def test_0057_round_trip_creates_and_drops_principal_tables(
    isolated_migration_db: None,
) -> None:
    config = _config()
    command.upgrade(config, "head")
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
