"""Fixtures for the shared connector resolver suites (ADR 0239).

The resolver is tested against the real API and a real Postgres 16: the API
(built with ``shared_app``) registers applications, members and bindings and
provisions each application's restricted role; the resolver connects with the
DSN the API sealed. Only the external provider is faked (``_fake_provider``).

``shared_database`` gives this module tree its own disposable database,
migrated to ``head``, and points ``DATABASE_URL`` at it for the module only
(plan R1-7). Module scope keeps it from overlapping the API suite's session
database when both run in one pytest process, in either order.

This file imports nothing from the resolver package, so a missing production
module fails the tests that need it rather than aborting collection here.
"""

from __future__ import annotations

import asyncio
import os
import secrets
import sys
from collections.abc import Iterator
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pytest

HERE = Path(__file__).resolve().parent
API_TESTS = HERE.parents[2] / "api" / "tests"
for entry in (str(API_TESTS), str(HERE)):
    if entry not in sys.path:
        sys.path.insert(0, entry)
pytest.register_assert_rewrite("_resolver_harness", "_fake_provider", "_refresh_source")

from _fake_provider import FakeProvider  # noqa: E402
from _migration_support import admin_execute, alembic_config, render_url, sql_dicts  # noqa: E402
from _shared_connector_support import (  # noqa: E402
    SessionPool,
    SharedCleanup,
    SharedStore,
    random_installation,
    shared_app,
)
from alembic import command  # noqa: E402
from curie_api.config import get_settings  # noqa: E402
from curie_test_support.valkey import connect_or_skip  # noqa: E402
from sqlalchemy.engine import make_url  # noqa: E402

if TYPE_CHECKING:
    from _resolver_harness import Fleet

# The same defaults the API suite's conftest sets before it builds an app; set
# for this module tree only and restored at teardown.
API_ENV_DEFAULTS = {
    "S3_ACCESS_KEY": "rustfs",
    "S3_SECRET_KEY": "rustfssecret",
    "CURIE_WORK_ITEM_RECONCILER_ENABLED": "false",
    "CURIE_DEPLOY_NOTICE_RECONCILER_ENABLED": "false",
    "CURIE_APPROVAL_CHAT_ATTESTER_SECRET": "approval-chat-attester-test-secret",
    "CURIE_REMEDIATION_AUTHORITY_KEY": "remediation-authority-test-secret",
}


@pytest.fixture(scope="module", autouse=True)
def _required_valkey() -> None:
    """The required store contract: Valkey must answer, as CI requires it to.

    The resolver keeps no Valkey state (refresh state lives in Postgres); the
    API the fixtures drive does.
    """

    client = connect_or_skip()
    client.close()


@pytest.fixture(scope="module")
def shared_database() -> Iterator[str]:
    """A disposable database migrated to head, for this module only (R1-7)."""

    base = make_url(get_settings().database_url)
    name = f"curie_test_crs_{secrets.token_hex(4)}"
    saved_url = os.environ.get("DATABASE_URL")
    saved_defaults = {key: os.environ.get(key) for key in API_ENV_DEFAULTS}
    asyncio.run(admin_execute(base, f'CREATE DATABASE "{name}"'))
    try:
        for key, value in API_ENV_DEFAULTS.items():
            os.environ.setdefault(key, value)
        os.environ["DATABASE_URL"] = render_url(base.set(database=name))
        get_settings.cache_clear()
        (row,) = sql_dicts("SELECT current_database() AS name")
        assert row["name"] == name, (row, name)
        command.upgrade(alembic_config(), "head")
        yield name
    finally:
        if saved_url is None:
            os.environ.pop("DATABASE_URL", None)
        else:
            os.environ["DATABASE_URL"] = saved_url
        for key, value in saved_defaults.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
        get_settings.cache_clear()
        asyncio.run(admin_execute(base, f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)'))


@pytest.fixture
def cleanup(shared_database: str) -> Iterator[SharedCleanup]:
    tracker = SharedCleanup()
    yield tracker
    tracker.run()


@pytest.fixture
def sessions(cleanup: SharedCleanup) -> Iterator[SessionPool]:
    pool = SessionPool()
    yield pool
    pool.close_all()


@pytest.fixture
def store(
    shared_database: str, cleanup: SharedCleanup, monkeypatch: pytest.MonkeyPatch
) -> Iterator[SharedStore]:
    installation = cleanup.installation(random_installation())
    with shared_app(monkeypatch, installation) as client:
        yield SharedStore(client, installation, cleanup)
    monkeypatch.undo()
    get_settings.cache_clear()


@pytest.fixture
def provider(tmp_path: Path) -> Iterator[FakeProvider]:
    fake = FakeProvider(tmp_path / "provider")
    try:
        yield fake
    finally:
        fake.close()


@pytest.fixture
def fleet(
    shared_database: str, store: SharedStore, sessions: SessionPool, cleanup: SharedCleanup
) -> Iterator[Fleet]:
    """Applications through the API and resolvers launched for them.

    Torn down before ``sessions`` and ``cleanup``: every resolver closes (and
    its socket is shown gone) before roles are dropped.
    """

    from _resolver_harness import Fleet

    built = Fleet(store, sessions, cleanup)
    try:
        yield built
    finally:
        built.close()


@pytest.fixture
def owner_flags(shared_database: str) -> dict[str, Any]:
    (row,) = sql_dicts(
        "SELECT rolsuper, rolcreaterole, rolcreatedb, rolreplication, rolbypassrls "
        "FROM pg_roles WHERE rolname = current_user"
    )
    return row
