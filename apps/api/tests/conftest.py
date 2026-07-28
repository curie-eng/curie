"""Shared fixtures: a DISPOSABLE per-run database + a TestClient.

Every suite run provisions its own throwaway database on the compose Postgres
server (curie_test_<utc>_<rand>), migrates it, and drops it at teardown, so the
tests never touch the shared `curie` database. This is the deterministic fix
for the cross-lane failure where one lane's migration stamped the shared DB ahead
of main and reddened another lane's suite. Integration tests still run against a
real Postgres (and real Valkey/Langfuse); nothing here mocks them.
"""

import asyncio
import os
import secrets
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import asyncpg
import pytest
from alembic import command
from alembic.config import Config
from curie_api.config import get_settings
from curie_api.main import create_app
from fastapi.testclient import TestClient
from sqlalchemy import make_url
from sqlalchemy.engine import URL
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.sql import text

API_DIR = Path(__file__).resolve().parents[1]
ALEMBIC_DIR = API_DIR / "alembic"
DB_PREFIX = "curie_test_"
TS_FORMAT = "%Y%m%d%H%M%S"


async def _admin_connect(base: URL) -> asyncpg.Connection:
    """Connect to the `postgres` maintenance DB to run CREATE/DROP DATABASE."""

    return await asyncpg.connect(
        user=base.username,
        password=base.password,
        host=base.host,
        port=base.port,
        database="postgres",
    )


def _stale(datname: str, cutoff: datetime) -> bool:
    """A disposable DB is stale if its embedded timestamp is older than cutoff."""

    try:
        stamp = datname[len(DB_PREFIX) :].split("_", 1)[0]
        return datetime.strptime(stamp, TS_FORMAT).replace(tzinfo=UTC) < cutoff
    except ValueError:
        return False


async def _provision(base: URL, run_db: str) -> None:
    conn = await _admin_connect(base)
    try:
        role = await conn.fetchrow(
            "select rolcreatedb, rolsuper from pg_roles where rolname = current_user"
        )
        if not (role and (role["rolcreatedb"] or role["rolsuper"])):
            raise RuntimeError(
                f"Postgres role {base.username!r} lacks CREATEDB; grant it "
                "(ALTER ROLE ... CREATEDB) or run the suite as a superuser role."
            )
        # Self-heal: a suite that died mid-run leaves its database behind; drop
        # any disposable DB older than a day before creating this run's. The
        # LIKE underscores are escaped (they are wildcards otherwise) and a
        # literal startswith guard ensures we only ever drop our own databases.
        cutoff = datetime.now(UTC) - timedelta(days=1)
        like = DB_PREFIX.replace("_", r"\_") + "%"
        for row in await conn.fetch(
            r"select datname from pg_database where datname like $1 escape '\'",
            like,
        ):
            name: str = row["datname"]
            if name.startswith(DB_PREFIX) and _stale(name, cutoff):
                await conn.execute(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)')
        await conn.execute(f'CREATE DATABASE "{run_db}"')
    finally:
        await conn.close()


async def _drop(base: URL, run_db: str) -> None:
    conn = await _admin_connect(base)
    try:
        await conn.execute(f'DROP DATABASE IF EXISTS "{run_db}" WITH (FORCE)')
    finally:
        await conn.close()


@pytest.fixture(scope="session")
def _disposable_db() -> Any:
    # Not autouse: pure unit tests (no client/db fixtures) still run without a
    # database. Any db-touching fixture (client, migrated, clean_db) pulls this
    # in, which sets DATABASE_URL before the app engine or alembic is created.
    base = make_url(get_settings().database_url)
    run_db = (
        f"{DB_PREFIX}{datetime.now(UTC).strftime(TS_FORMAT)}_{secrets.token_hex(3)}"
    )
    asyncio.run(_provision(base, run_db))

    # Point the app + alembic at the disposable DB for the rest of the session.
    os.environ["DATABASE_URL"] = base.set(database=run_db).render_as_string(
        hide_password=False
    )
    get_settings.cache_clear()
    try:
        # Inside the try so a failed migration still drops the run's database.
        cfg = Config()
        cfg.set_main_option("script_location", str(ALEMBIC_DIR))
        command.upgrade(cfg, "head")
        yield run_db
    finally:
        asyncio.run(_drop(base, run_db))
        os.environ.pop("DATABASE_URL", None)
        get_settings.cache_clear()


@pytest.fixture(scope="session")
def migrated(_disposable_db: Any) -> None:
    """The disposable DB is created and migrated by _disposable_db."""


async def _truncate() -> None:
    engine = create_async_engine(get_settings().database_url)
    try:
        async with engine.begin() as conn:
            await conn.execute(
                text(
                    "TRUNCATE curie.approvals, curie.deployments, "
                    "curie.agent_versions, curie.agents, "
                    "curie.console_sessions CASCADE"
                )
            )
    finally:
        await engine.dispose()


@pytest.fixture
def clean_db(migrated: None) -> None:
    asyncio.run(_truncate())


@pytest.fixture
def client(_disposable_db: Any) -> Any:
    # Depends on _disposable_db so the app engine is built against the disposable
    # DB (the app lifespan already requires the compose stack: MinIO, Valkey).
    with TestClient(create_app()) as test_client:
        yield test_client


@pytest.fixture
def auth_headers() -> dict[str, str]:
    return {"X-API-Key": get_settings().api_key}
