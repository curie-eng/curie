"""Bounded source gate acquisition across pool and lock waits, @spec PROTECTED-HOOK-SOURCE-2/10.

A supplied ``wait_seconds`` is one monotonic budget covering gate pool
checkout, connection and transaction setup and the advisory lock
acquisition. Every case runs against real Postgres: the administrative case
drives the real application lifespan and the authenticated source-policy
route over HTTP, and the layer cases use real pools, real advisory locks and
real cancellations. Nothing here mocks an engine, a pool, a lock, CRUD or a
router.
"""

from __future__ import annotations

import asyncio
import time
import uuid
from collections.abc import AsyncIterator, Iterator
from contextlib import asynccontextmanager
from typing import Any

import httpx
import pytest
from _migration_support import IsolatedMigrationDb, sql_dicts
from curie_api.config import get_settings
from curie_api.main import create_app
from curie_protected_hooks import source_policy_sql as source_sql
from sqlalchemy import event, text
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import NullPool

HOOK = "daily-summary"
LOCK_KEY = "hook-source:"


@pytest.fixture
def deadline_db(
    isolated_migration_db: IsolatedMigrationDb, monkeypatch: pytest.MonkeyPatch
) -> Iterator[None]:
    """A disposable migrated database, @spec PROTECTED-HOOK-SOURCE-2/10."""
    isolated_migration_db.at("head")
    for name, value in {
        "GITHUB_REVIEW_INGRESS_ENABLED": "false",
        "RESUME_RECONCILER_ENABLED": "false",
        "CURIE_WORK_ITEM_RECONCILER_ENABLED": "false",
        "APPROVAL_SWEEP_INTERVAL_S": "0",
        "DEAD_LETTER_WATCH_INTERVAL_S": "0",
        "COMMIT_POLL_INTERVAL_S": "0",
        "OTEL_SDK_DISABLED": "true",
        "OTEL_EXPORTER_OTLP_ENDPOINT": "",
    }.items():
        monkeypatch.setenv(name, value)
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


def source_state(agent: uuid.UUID) -> tuple[tuple[tuple[tuple[str, str], ...], ...], ...]:
    """The actual source table rows for one agent, @spec PROTECTED-HOOK-SOURCE-2/10."""

    def rows(statement: str) -> tuple[tuple[tuple[str, str], ...], ...]:
        return tuple(
            tuple(sorted((str(key), str(value)) for key, value in dict(row).items()))
            for row in sql_dicts(statement, {"id": agent})
        )

    return (
        rows("SELECT * FROM curie.agents WHERE id=:id"),
        rows("SELECT * FROM curie.hook_source_policies WHERE agent_id=:id"),
        rows("SELECT * FROM curie.hook_source_operations WHERE agent_id=:id"),
    )


@asynccontextmanager
async def conflicting_lock(url: str, agent: uuid.UUID) -> AsyncIterator[None]:
    """A real advisory transaction lock on one agent, held open, @spec PROTECTED-HOOK-SOURCE-2."""
    observer = create_async_engine(url, poolclass=NullPool)
    try:
        async with observer.connect() as connection:
            async with connection.begin():
                await connection.execute(
                    text("SELECT pg_advisory_xact_lock(hashtextextended(:lock_key, 0))"),
                    {"lock_key": LOCK_KEY + str(agent)},
                )
                yield
    finally:
        await observer.dispose()


async def advisory_locks(url: str) -> int:
    """Granted advisory locks on this database, @spec PROTECTED-HOOK-SOURCE-2."""
    observer = create_async_engine(url, poolclass=NullPool)
    try:
        async with observer.connect() as connection:
            return int(
                await connection.scalar(
                    text(
                        "SELECT COUNT(*) FROM pg_locks l "
                        "JOIN pg_database d ON d.oid=l.database "
                        "WHERE l.locktype='advisory' AND l.granted AND d.datname=current_database()"
                    )
                )
            )
    finally:
        await observer.dispose()


async def first_advisory_wait(url: str) -> float | None:
    """The monotonic instant a backend first waits on an advisory lock.

    Watches ``pg_stat_activity`` from a separate connection, so the observed
    wait is the real gate connection's server side advisory lock request.
    Each poll opens its own connection: PostgreSQL caches the activity
    snapshot for the reading transaction, so one transaction held across
    polls would keep returning its first, pre-wait snapshot,
    @spec PROTECTED-HOOK-SOURCE-2.
    """
    limit = time.monotonic() + 10
    observer = create_async_engine(url, poolclass=NullPool)
    try:
        while time.monotonic() < limit:
            async with observer.connect() as connection:
                waiting = await connection.scalar(
                    text(
                        "SELECT EXISTS (SELECT 1 FROM pg_stat_activity "
                        "WHERE datname = current_database() "
                        "AND pid <> pg_backend_pid() "
                        "AND wait_event_type = 'Lock' "
                        "AND wait_event = 'advisory')"
                    )
                )
            if waiting:
                return time.monotonic()
            await asyncio.sleep(0.01)
        return None
    finally:
        await observer.dispose()


def lock_timeout_ms(value: str) -> float:
    """Milliseconds in a PostgreSQL ``lock_timeout`` setting, @spec PROTECTED-HOOK-SOURCE-2."""
    if value.endswith("ms"):
        return float(value[:-2])
    if value.endswith("s"):
        return float(value[:-1]) * 1000
    return float(value) * 1000


def test_admin_pool_checkout_respects_gate_deadline(deadline_db: None) -> None:
    """A saturated production gate pool refuses inside the acquisition budget.

    The five second administrative budget starts at pool checkout, so an
    authenticated real request against four occupied gate connections answers
    503 ``source_state_unavailable`` within 5.75 seconds without ever
    entering guarded work: the work pool is never checked out and the actual
    table state is unchanged. Missing authentication refuses before the
    wait, and the unsaturated missing-agent control stays 404.
    @spec PROTECTED-HOOK-SOURCE-2/3/10.
    """
    agent = uuid.uuid4()
    sql_dicts(
        "INSERT INTO curie.agents (id, name) VALUES (:id, :name)",
        {"id": agent, "name": "gate-deadline-" + agent.hex},
    )
    before = source_state(agent)

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-2/3/10."""
        app = create_app()
        async with app.router.lifespan_context(app):
            gate = app.state.source_gate
            headers = {"X-API-Key": get_settings().api_key}
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app), base_url="http://test"
            ) as client:
                healthy = await client.get("/health")
                assert healthy.status_code == 200, healthy.text
                missing = await client.get(
                    f"/agents/{uuid.uuid4()}/hooks/{HOOK}/source-policy", headers=headers
                )
                assert missing.status_code == 404, missing.text
                assert missing.headers["cache-control"] == "no-store"
                assert missing.json()["detail"]["code"] == "source_agent_not_found"

                async with (
                    gate.engine.connect(),
                    gate.engine.connect(),
                    gate.engine.connect(),
                    gate.engine.connect(),
                ):
                    assert gate.engine.pool.checkedout() == 4
                    started = time.monotonic()
                    unauthenticated = await client.get(
                        f"/agents/{agent}/hooks/{HOOK}/source-policy"
                    )
                    assert unauthenticated.status_code == 401
                    assert time.monotonic() - started < 2
                    started = time.monotonic()
                    pending = asyncio.create_task(
                        client.get(f"/agents/{agent}/hooks/{HOOK}/source-policy", headers=headers)
                    )
                    await asyncio.sleep(2)
                    assert not pending.done()
                    assert app.state.engine.pool.checkedout() == 0
                    assert gate.engine.pool.checkedout() == 4
                    response = await pending
                    elapsed = time.monotonic() - started
                    assert response.status_code == 503, response.text
                    assert elapsed < 5.75
                    assert response.headers["cache-control"] == "no-store"
                    detail = response.json()["detail"]
                    assert detail["code"] == "source_state_unavailable"
                    assert detail["committed_generation"] is None
                    assert app.state.engine.pool.checkedout() == 0
                assert gate.engine.pool.checkedout() == 0
                assert await advisory_locks(get_settings().database_url) == 0

    asyncio.run(asyncio.wait_for(scenario(), 30))
    assert source_state(agent) == before


def test_cumulative_acquisition_pool_wait_is_bounded_by_budget(deadline_db: None) -> None:
    """A bounded acquisition refuses without waiting out the pool's own timeout.

    A 0.2 second budget against an exhausted pool whose timeout is three
    seconds refuses within 0.8 seconds, never enters the guarded body, and
    leaves the pool unchanged.
    @spec PROTECTED-HOOK-SOURCE-2.
    """
    agent = uuid.uuid4()

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-2."""
        url = get_settings().database_url
        gate = create_async_engine(url, pool_size=1, max_overflow=0, pool_timeout=3)
        source = source_sql.SourceGate(gate)
        entered = asyncio.Event()
        try:
            async with gate.connect():
                assert gate.pool.checkedout() == 1
                started = time.monotonic()
                with pytest.raises(source_sql.SourceSnapshotUnavailable):
                    async with source.hold(agent, wait_seconds=0.2):
                        entered.set()
                assert time.monotonic() - started < 0.8
                assert not entered.is_set()
                assert gate.pool.checkedout() == 1
            assert gate.pool.checkedout() == 0
            async with source.hold(agent, wait_seconds=5.0):
                assert gate.pool.checkedout() == 1
            assert gate.pool.checkedout() == 0
        finally:
            await gate.dispose()

    asyncio.run(asyncio.wait_for(scenario(), 15))


def test_cumulative_acquisition_lock_wait_uses_remaining_budget(deadline_db: None) -> None:
    """Pool delay consumes the budget; the lock waits only what remains.

    A one second budget spends about 0.8 seconds waiting for the only gate
    connection, which is held before the acquisition starts, so the real
    gate connection reaches its server side advisory lock request only after
    that pool wait, carries a server lock timeout of the budget still
    remaining then (about 0.2 seconds, observed on the acquiring connection
    by non-substitutive event instrumentation), and is cut at that remaining
    budget rather than a fresh full second counted from the lock request.
    The pre-change gate, which starts the server lock timeout only after
    checkout, hands the server the full second instead. The refusal enters
    no guarded body, releases the connection, and a same-agent retry
    succeeds once the conflict ends.
    @spec PROTECTED-HOOK-SOURCE-2.
    """
    agent = uuid.uuid4()

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-2."""
        url = get_settings().database_url
        gate = create_async_engine(url, pool_size=1, max_overflow=0, pool_timeout=3)
        source = source_sql.SourceGate(gate)
        entered = asyncio.Event()
        saturated = asyncio.Event()
        observed_lock_timeouts: list[str] = []

        def before_lock_request(
            connection: Any,
            cursor: Any,
            statement: str,
            parameters: Any,
            context: Any,
            executemany: bool,
        ) -> None:
            """Read the effective server lock timeout on the acquiring connection.

            Non-substitutive event instrumentation: SQLAlchemy wraps a
            handler without ``retval`` to return the statement and parameters
            unchanged. The read runs on the real gate connection inside its
            acquisition transaction, immediately before the advisory lock
            statement is sent, @spec PROTECTED-HOOK-SOURCE-2.
            """
            if "pg_advisory_xact_lock" in str(statement):
                observed_lock_timeouts.append(
                    str(
                        connection.exec_driver_sql(
                            "SELECT current_setting('lock_timeout')"
                        ).scalar()
                    )
                )

        event.listen(gate.sync_engine, "before_cursor_execute", before_lock_request)

        async def delayed_release() -> None:
            """Hold the only gate connection, then free it, @spec PROTECTED-HOOK-SOURCE-2."""
            async with gate.connect():
                saturated.set()
                await asyncio.sleep(0.8)

        releaser: Any = asyncio.create_task(delayed_release())
        watcher: Any = None
        try:
            await asyncio.wait_for(saturated.wait(), 5)
            async with conflicting_lock(url, agent):
                watcher = asyncio.create_task(first_advisory_wait(url))
                started = time.monotonic()
                with pytest.raises(source_sql.SourceSnapshotUnavailable):
                    async with source.hold(agent, wait_seconds=1.0):
                        entered.set()
                refused = time.monotonic()
                lock_wait_started = await watcher
                assert lock_wait_started is not None, "gate connection never reached the lock wait"
                # The pool wait happened inside this acquisition: the lock
                # request starts only once the connection is checked out.
                assert lock_wait_started - started >= 0.5, lock_wait_started - started
                # The lock wait is cut at the remaining budget, about 0.2s,
                # not at a fresh full second from the lock request.
                assert refused - lock_wait_started <= 0.5, refused - lock_wait_started
                assert refused - started < 1.5, refused - started
                # The server side lock timeout the acquiring connection
                # actually carries is the budget remaining after the pool
                # wait (about 200ms of the 1.0s budget), not a fresh full
                # second counted from checkout.
                assert observed_lock_timeouts, "no advisory lock request was observed"
                bound = lock_timeout_ms(observed_lock_timeouts[0])
                assert 10 <= bound <= 500, observed_lock_timeouts[0]
                assert not entered.is_set()
                assert gate.pool.checkedout() == 0
            assert await advisory_locks(url) == 0
            await releaser
            async with source.hold(agent, wait_seconds=5.0):
                assert gate.pool.checkedout() == 1
            assert gate.pool.checkedout() == 0
        finally:
            event.remove(gate.sync_engine, "before_cursor_execute", before_lock_request)
            for pending in (watcher, releaser):
                if pending is not None and not pending.done():
                    pending.cancel()
                    await asyncio.gather(pending, return_exceptions=True)
            await gate.dispose()

    asyncio.run(asyncio.wait_for(scenario(), 20))


def test_cumulative_acquisition_refusal_releases_and_permits_retry(deadline_db: None) -> None:
    """An expired acquisition releases its connection and lock, then retries.

    The advisory lock wait consumes a bounded acquisition, the refusal leaves
    no checked-out connection and no held lock, and a later same-agent
    acquisition succeeds.
    @spec PROTECTED-HOOK-SOURCE-2.
    """
    agent = uuid.uuid4()

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-2."""
        url = get_settings().database_url
        gate = create_async_engine(url, pool_size=1, max_overflow=0, pool_timeout=3)
        source = source_sql.SourceGate(gate)
        try:
            async with conflicting_lock(url, agent):
                with pytest.raises(source_sql.SourceSnapshotUnavailable):
                    async with source.hold(agent, wait_seconds=0.3):
                        pytest.fail("expired acquisition entered its guarded body")
                assert gate.pool.checkedout() == 0
            assert await advisory_locks(url) == 0
            async with source.hold(agent, wait_seconds=5.0):
                assert gate.pool.checkedout() == 1
            assert gate.pool.checkedout() == 0
        finally:
            await gate.dispose()

    asyncio.run(asyncio.wait_for(scenario(), 15))


@pytest.mark.parametrize("phase", ["pool", "lock"])
def test_cumulative_acquisition_cancellation_propagates_and_releases(
    deadline_db: None, phase: str
) -> None:
    """External cancellation during bounded acquisition propagates and releases.

    Cancelling a bounded acquisition while it waits on the pool, or while it
    holds a connection and waits on the advisory lock, raises CancelledError
    rather than the sanitized refusal, returns the connection to the pool,
    leaves no lock held, and lets a later same-agent acquisition succeed.
    @spec PROTECTED-HOOK-SOURCE-2.
    """
    agent = uuid.uuid4()

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-2."""
        url = get_settings().database_url
        gate = create_async_engine(url, pool_size=1, max_overflow=0, pool_timeout=3)
        source = source_sql.SourceGate(gate)

        async def waiting(budget: float) -> None:
            """A bounded acquisition that must never enter its body."""
            async with source.hold(agent, wait_seconds=budget):
                pytest.fail("cancelled acquisition entered its guarded body")

        waiter: Any = None
        try:
            if phase == "pool":
                async with gate.connect():
                    waiter = asyncio.create_task(waiting(0.5))
                    await asyncio.sleep(0.2)
                    waiter.cancel()
                    with pytest.raises(asyncio.CancelledError):
                        await waiter
                    assert gate.pool.checkedout() == 1
            else:
                async with conflicting_lock(url, agent):
                    waiter = asyncio.create_task(waiting(30.0))
                    await asyncio.sleep(0.2)
                    waiter.cancel()
                    with pytest.raises(asyncio.CancelledError):
                        await waiter
                    assert gate.pool.checkedout() == 0
                assert await advisory_locks(url) == 0
            async with source.hold(agent, wait_seconds=5.0):
                assert gate.pool.checkedout() == 1
            assert gate.pool.checkedout() == 0
        finally:
            if waiter is not None and not waiter.done():
                waiter.cancel()
                await asyncio.gather(waiter, return_exceptions=True)
            await gate.dispose()

    asyncio.run(asyncio.wait_for(scenario(), 20))


def test_cumulative_acquisition_release_before_expiry_permits_and_holder_outlives_budget(
    deadline_db: None,
) -> None:
    """Released capacity is acquired in budget, and the holder outlives it.

    The only gate connection frees 0.3 seconds into a two second budget
    (the connection is held before the acquisition starts), so the
    acquisition waits for it inside the budget and then holds the gate. The
    budget ends before the context is yielded: the guarded body crosses the
    recorded acquisition deadline, the held connection still serves SQL
    registration and commit with the server lock timeout cleared on it, and
    the transaction commits at exit without any residual lock.
    @spec PROTECTED-HOOK-SOURCE-2.
    """
    agent = uuid.uuid4()

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-2."""
        url = get_settings().database_url
        gate = create_async_engine(url, pool_size=1, max_overflow=0, pool_timeout=3)
        source = source_sql.SourceGate(gate)
        saturated = asyncio.Event()

        async def delayed_release() -> None:
            """Hold the only gate connection, then free it, @spec PROTECTED-HOOK-SOURCE-2."""
            async with gate.connect():
                saturated.set()
                await asyncio.sleep(0.3)

        releaser: Any = asyncio.create_task(delayed_release())
        try:
            await asyncio.wait_for(saturated.wait(), 5)
            acquired = time.monotonic()
            async with source.hold(agent, wait_seconds=2.0) as held:
                await releaser
                # The guarded body crosses the acquisition deadline: the
                # budget ended before the context was yielded.
                await asyncio.sleep(2.5)
                assert time.monotonic() - acquired > 2.0, time.monotonic() - acquired
                assert held.active
                await source_sql.ensure_source_gate_live(held)
                assert (
                    await held._connection.scalar(text("SELECT current_setting('lock_timeout')"))
                    == "0"
                )
            assert gate.pool.checkedout() == 0
            assert await advisory_locks(url) == 0
        finally:
            if not releaser.done():
                releaser.cancel()
                await asyncio.gather(releaser, return_exceptions=True)
            await gate.dispose()

    asyncio.run(asyncio.wait_for(scenario(), 20))


@pytest.mark.parametrize("invalid", [0, -0.5, 60.5, "1", True, False])
def test_cumulative_acquisition_invalid_wait_seconds_retained(
    deadline_db: None, invalid: Any
) -> None:
    """Invalid budgets are refused before any checkout, unchanged.

    @spec PROTECTED-HOOK-SOURCE-2.
    """
    agent = uuid.uuid4()

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-2."""
        gate = create_async_engine(
            get_settings().database_url, pool_size=1, max_overflow=0, pool_timeout=3
        )
        source = source_sql.SourceGate(gate)
        try:
            with pytest.raises(source_sql.SourceGateInvalid):
                async with source.hold(agent, wait_seconds=invalid):
                    pytest.fail("invalid wait_seconds acquired the gate")
            assert gate.pool.checkedout() == 0
        finally:
            await gate.dispose()

    asyncio.run(asyncio.wait_for(scenario(), 10))