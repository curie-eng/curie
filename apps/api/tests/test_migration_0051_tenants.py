"""Migration 0051 adds curie.tenants and auto-provisions the default tenant."""

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
DEFAULT_TENANT_ID = "00000000-0000-0000-0000-000000000001"


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


def _tenants_regclass() -> str | None:
    rows = _sql("SELECT to_regclass('curie.tenants') AS name")
    name = rows[0]["name"]
    return None if name is None else str(name)


def test_0051_creates_tenants_and_downgrade_drops_it(
    isolated_migration_db: None,
) -> None:
    config = _config()
    command.upgrade(config, "head")
    assert _tenants_regclass() is not None
    rows = _sql("SELECT id, status FROM curie.tenants")
    assert len(rows) == 1
    assert str(rows[0]["id"]) == DEFAULT_TENANT_ID
    assert rows[0]["status"] == "active"
    try:
        command.downgrade(config, "0050")
        assert _tenants_regclass() is None
    finally:
        # A failed assertion must not leave this private database below head.
        command.upgrade(config, "head")
