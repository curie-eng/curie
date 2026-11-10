"""The shared install's reset write against the live API's own reads.

reset_case stops the worker but leaves the API running, and the API's work
item reconciler keeps reading the factory tables on every pass. The reset's
one TRUNCATE takes ACCESS EXCLUSIVE on every case table, one table at a time
in statement order: work_items, agents, then the rest by name. The GitHub
poll intake reads agent_channels and then work_items in one transaction
(curie_api.factory_poll_intake._poll_locked). A reset that arrives between
those two reads holds work_items and waits for agent_channels, and the poll
then waits for work_items: a lock cycle that Postgres breaks by aborting one
side after deadlock_timeout.

The test drives that interleaving deterministically on a database migrated to
head. The reset text is the harness's own (reset_sql inside transaction_sql,
the command text write_sql hands psql), sent as one simple query as psql
sends it. The poll side is the product's read helpers, in the order and the
single transaction the poll pass uses.
"""

from __future__ import annotations

import asyncio
import json
import os
import secrets
import sys
import time
from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import asyncpg
import pytest
from sqlalchemy import make_url
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parents[2] / "apps/api/tests"))
sys.path.insert(0, str(HERE))

from _migration_support import admin_execute, alembic_config, render_url  # noqa: E402
from alembic import command  # noqa: E402
from c4_shared_install import (  # noqa: E402
    TABLE_ROWS_QUERY,
    inventoried_table_rows,
    reset_sql,
    transaction_sql,
)
from curie_api.config import get_settings  # noqa: E402
from curie_api.factory_label_reconcile import (  # noqa: E402
    bound_repositories,
    open_work_item_repositories,
)
from curie_api.models import Agent, AgentChannel, Tenant  # noqa: E402
from curie_test_support.postgres import pg_connect_or_skip  # noqa: E402

REPOSITORY = "acme-corp/acme-bot"
# The reset blocks within milliseconds of starting; this only bounds a broken run.
BLOCK_DEADLINE_SECONDS = 30
RESET_WAITS_ON = (
    "SELECT EXISTS (SELECT 1 FROM pg_locks WHERE pid = $1 AND NOT granted "
    "AND locktype = 'relation' AND relation = $2::regclass)"
)


@pytest.fixture
def reset_database(monkeypatch: pytest.MonkeyPatch) -> Iterator[str]:
    """A database of this test's own on the ``DATABASE_URL`` server, migrated to head."""

    base = make_url(os.environ.get("DATABASE_URL") or get_settings().database_url)

    async def reachable() -> None:
        probe = create_async_engine(base)
        try:
            await pg_connect_or_skip(probe)
        finally:
            await probe.dispose()

    asyncio.run(reachable())
    name = f"curie_test_{datetime.now(UTC).strftime('%Y%m%d%H%M%S')}_{secrets.token_hex(3)}"
    asyncio.run(admin_execute(base, f'CREATE DATABASE "{name}"'))
    url = render_url(base.set(database=name))
    monkeypatch.setenv("DATABASE_URL", url)
    get_settings.cache_clear()
    try:
        command.upgrade(alembic_config(), "head")
        yield url
    finally:
        get_settings.cache_clear()
        asyncio.run(admin_execute(base, f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)'))


def connect_args(url: str) -> dict[str, Any]:
    parsed = make_url(url)
    return {
        "user": parsed.username,
        "password": parsed.password,
        "host": parsed.host,
        "port": parsed.port,
        "database": parsed.database,
    }


async def install_state(
    observer: asyncpg.Connection,
) -> tuple[tuple[tuple[str, int], ...], frozenset[str]]:
    rows = inventoried_table_rows(json.loads(await observer.fetchval(TABLE_ROWS_QUERY)))
    tenants = frozenset(
        str(row["id"]) for row in await observer.fetch("SELECT id FROM curie.tenants")
    )
    return rows, tenants


async def reset_waits_on(
    observer: asyncpg.Connection, pid: int, table: str, resetting: asyncio.Task[Any]
) -> None:
    deadline = time.monotonic() + BLOCK_DEADLINE_SECONDS
    while not await observer.fetchval(RESET_WAITS_ON, pid, table):
        if resetting.done():
            resetting.result()
            raise AssertionError(f"the reset finished without waiting on {table}")
        if time.monotonic() > deadline:
            raise AssertionError(f"the reset never waited on {table}")


async def reset_beside_poll(url: str) -> None:
    engine = create_async_engine(url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    observer = await asyncpg.connect(**connect_args(url))
    psql = await asyncpg.connect(**connect_args(url))
    try:
        baseline_rows, baseline_tenants = await install_state(observer)
        # A case's state: its own tenant and an agent bound to the repository.
        async with sessions() as session:
            tenant = Tenant(deployment_id="acme-case")
            session.add(tenant)
            await session.flush()
            agent = Agent(name="acme-bot", tenant_id=tenant.id)
            session.add(agent)
            await session.flush()
            session.add(
                AgentChannel(
                    agent_id=agent.id,
                    tenant_id=tenant.id,
                    kind="github",
                    address=REPOSITORY,
                    trigger_label="acme-label",
                )
            )
            await session.commit()
        pid = await psql.fetchval("SELECT pg_backend_pid()")
        async with sessions() as poll:
            assert await bound_repositories(poll) == [REPOSITORY]
            resetting = asyncio.create_task(
                psql.execute(transaction_sql(reset_sql(baseline_rows, baseline_tenants)))
            )
            await reset_waits_on(observer, pid, "curie.agent_channels", resetting)
            reset, read = await asyncio.gather(
                resetting, open_work_item_repositories(poll), return_exceptions=True
            )
        assert not isinstance(reset, BaseException), (
            f"the reset was aborted: {type(reset).__name__} "
            f"SQLSTATE {getattr(reset, 'sqlstate', None)}: {reset}"
        )
        # The cycle formed, and Postgres aborted the poll's read, which the
        # reconciler repeats on its next pass.
        assert isinstance(read, DBAPIError), f"the poll read was not aborted: {read!r}"
        assert getattr(read.orig, "sqlstate", None) == "40P01", read
        assert await install_state(observer) == (baseline_rows, baseline_tenants)
    finally:
        await psql.close()
        await observer.close()
        await engine.dispose()


def test_reset_completes_when_the_live_poll_intake_closes_a_lock_cycle(
    reset_database: str,
) -> None:
    asyncio.run(reset_beside_poll(reset_database))
