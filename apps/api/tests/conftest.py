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
import sys
import uuid
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import asyncpg
import pytest
import redis
from alembic import command
from curie_api.config import get_settings
from curie_api.main import create_app
from curie_test_support.valkey import connect_or_skip
from fastapi.testclient import TestClient
from sqlalchemy import make_url
from sqlalchemy.engine import URL
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.sql import text

# Test modules import shared helpers from this directory; see _migration_support.
sys.path.insert(0, str(Path(__file__).resolve().parent))
# The shared forge fakes hold fixtures and asserts that lived in test modules;
# keep pytest's assertion detail for them.
pytest.register_assert_rewrite("forge_fakes")

from _migration_support import (  # noqa: E402
    IsolatedMigrationDb,
    MigrationTemplates,
    admin_execute,
    alembic_config,
    render_url,
)

# The object-store credential the suite talks to compose's RustFS with. The code
# default in `curie_api.config` is deliberately empty so that omitting these
# variables selects the AWS provider chain instead (the key-free BYO object-store
# path, #1559), which means the test environment has to name the dev credential
# itself rather than leaning on a production default. Declaring it here also makes
# the suite independent of whatever ambient AWS credentials the developer's machine
# happens to carry, which previously decided the result. `setdefault`, so a
# contributor pointing the suite at another store by exporting their own value wins.
os.environ.setdefault("S3_ACCESS_KEY", "rustfs")
os.environ.setdefault("S3_SECRET_KEY", "rustfssecret")
# Production enables the work-item reconciler. Suite create_app() must not:
# a 5s pass races TRUNCATE on the shared engine. Loop tests construct it.
os.environ.setdefault("CURIE_WORK_ITEM_RECONCILER_ENABLED", "false")
# The deploy notice outbox reconciler likewise: its 5s pass holds pooled
# connections from the TestClient's loop, which a test driving the same app
# from its own loop then receives (#1331). Its tests call reconcile_once.
os.environ.setdefault("CURIE_DEPLOY_NOTICE_RECONCILER_ENABLED", "false")
get_settings.cache_clear()
# A dedicated placeholder attester key for authenticated chat approval tests.
# It is intentionally distinct from the platform key: sharing those keys would
# let any platform-key holder forge the Slack identity/channel proof ADR-0106
# requires the dispatcher alone to attest.
os.environ.setdefault(
    "CURIE_APPROVAL_CHAT_ATTESTER_SECRET", "approval-chat-attester-test-secret"
)

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
        command.upgrade(alembic_config(), "head")
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
                    "TRUNCATE curie.execution_requests, curie.work_items, "
                    "curie.deploy_notice_outbox, "
                    "curie.approvals, curie.deployments, "
                    "curie.agent_versions, curie.agents, "
                    "curie.console_sessions, curie.github_review_deliveries, "
                    "curie.factory_poll_cursors CASCADE"
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
    # DB (the app lifespan already requires the compose stack: RustFS, Valkey).
    # Give each fixture instance its own client address so a session request in
    # one test cannot consume another test's Valkey rate limit budget.
    suffix = secrets.token_hex(8)
    address = "2001:db8::" + ":".join(
        suffix[index : index + 4] for index in range(0, 16, 4)
    )
    with TestClient(create_app(), client=(address, 5000)) as test_client:
        yield test_client


# --- hook ingress fixtures (test_hooks.py, test_hook_partition.py) -------------
#
# Shared here rather than imported across test modules: the suite runs under
# ``--import-mode=importlib`` with no ``__init__.py``, so one test module
# cannot reliably import another by name, but pytest discovers conftest
# fixtures regardless of import mode.


@pytest.fixture
def runs_stream() -> Iterator[str]:
    """A per-test runs stream, so enqueued turns never leak between tests."""

    name = f"test:curie:runs:{uuid.uuid4().hex}"
    os.environ["RUNS_STREAM"] = name
    get_settings.cache_clear()
    yield name
    os.environ.pop("RUNS_STREAM", None)
    get_settings.cache_clear()


@pytest.fixture
def valkey(runs_stream: str) -> Iterator[redis.Redis]:
    client = connect_or_skip(decode_responses=True)
    yield client
    client.delete(runs_stream)
    client.close()


@pytest.fixture
def hooks_client(_disposable_db: Any, runs_stream: str) -> Iterator[TestClient]:
    # Built AFTER runs_stream so the app reads the per-test stream name.
    with TestClient(create_app()) as test_client:
        yield test_client


@pytest.fixture
def auth_headers() -> dict[str, str]:
    return {"X-API-Key": get_settings().api_key}


@pytest.fixture(scope="session")
def _migration_templates() -> Iterator[MigrationTemplates]:
    templates = MigrationTemplates(make_url(get_settings().database_url))
    try:
        yield templates
    finally:
        templates.drop_all()


@pytest.fixture
def isolated_migration_db(
    _migration_templates: MigrationTemplates,
) -> Iterator[IsolatedMigrationDb]:
    """A throwaway database ALL to itself, for a test that downgrades/upgrades.

    The session ``_disposable_db`` is shared across every test in the run, so
    running ``alembic downgrade`` against it mid-suite disrupts siblings (the
    approval sweeper tests seed rows and count them). apps/api/CLAUDE.md is
    explicit: migrations are tested against a database of their own, never
    shared state. This provisions one, points DATABASE_URL + alembic at it for
    the test, and drops it after, restoring the session URL so nothing else is
    perturbed. It starts empty; ``.at(revision)`` swaps in a clone of the
    session template at that revision.
    """
    base = make_url(get_settings().database_url)
    run_db = f"curie_test_mig_{secrets.token_hex(4)}"
    saved_url = os.environ.get("DATABASE_URL")
    asyncio.run(admin_execute(base, f'CREATE DATABASE "{run_db}"'))
    try:
        os.environ["DATABASE_URL"] = render_url(base.set(database=run_db))
        get_settings.cache_clear()
        yield IsolatedMigrationDb(base, run_db, _migration_templates)
    finally:
        if saved_url is None:
            os.environ.pop("DATABASE_URL", None)
        else:
            os.environ["DATABASE_URL"] = saved_url
        get_settings.cache_clear()
        asyncio.run(admin_execute(base, f'DROP DATABASE IF EXISTS "{run_db}" WITH (FORCE)'))
