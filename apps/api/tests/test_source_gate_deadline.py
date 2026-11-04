"""Bounded source gate acquisition across pool and lock waits, @spec PROTECTED-HOOK-SOURCE-2/10.

A supplied ``wait_seconds`` is one monotonic budget covering gate pool
checkout, connection and transaction setup and the advisory lock
acquisition. Every case runs against real Postgres: the administrative case
drives the real application lifespan and the authenticated source-policy
route over HTTP, and the layer cases use real pools, real advisory locks and
real cancellations. Nothing here mocks an engine, a pool, a lock, CRUD or a
router, and no case waits on a clock: pool delays are real acquisitions
whose own budgets release the connection, and progress is observed with
non-substitutive SQLAlchemy event instrumentation, bounded event waits and
single scheduling yields.

The negative control these regressions are red on: keep only this file on
the pre-change main the issue reproduces on
(``fe6812455fcae87ce44072b3e8157e931bc3eceb``), restore
``source_policy_sql.py`` alone to its pre-change state, start the documented
dev services (``curie local up --build`` from ``compose.dev.yaml``, then
``cd apps/api && uv run alembic upgrade head``), and run this module's
verification commands with ``CI_REQUIRE_POSTGRES_TESTS=1``: the
administrative case answers past its bound while the pool wait is
unbounded, and the lock-wait assertions observe a fresh full budget instead
of the remaining one, until the cumulative budget is in place.
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
    authenticated real request against four occupied gate connections waits
    out most of that budget on the pool and only then answers 503
    ``source_state_unavailable`` within 5.75 seconds, without ever entering
    guarded work: the work pool is never checked out and the actual table
    state is unchanged. Missing authentication refuses before the wait, and
    the unsaturated missing-agent control stays 404.
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
                    response = await client.get(
                        f"/agents/{agent}/hooks/{HOOK}/source-policy", headers=headers
                    )
                    elapsed = time.monotonic() - started
                    # With every gate connection held for the whole request,
                    # the only thing the acquisition can be doing is waiting on
                    # the pool: the request answers only after waiting out
                    # most of the five second budget, inside the budget's
                    # own bound, and never enters guarded work.
                    assert response.status_code == 503, response.text
                    assert 4.0 < elapsed < 5.75, elapsed
                    assert response.headers["cache-control"] == "no-store"
                    detail = response.json()["detail"]
                    assert detail["code"] == "source_state_unavailable"
                    assert detail["committed_generation"] is None
                    assert app.state.engine.pool.checkedout() == 0
                    assert gate.engine.pool.checkedout() == 4
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

    A capacity acquisition on the same conflicted agent holds the only gate
    connection for its own 0.8 second budget: the fixed code's own deadline
    cuts it while it waits on the real conflicting advisory lock, which is
    exactly when the connection returns to the pool. The one second
    acquisition under study therefore spends that hold waiting for the
    connection and only then reaches its server side advisory lock request,
    carrying a server lock timeout of the budget still remaining then
    (about 0.2 seconds, observed as both the bound the gate itself sent and
    the live setting on the acquiring connection, by non-substitutive event
    instrumentation), and is cut at that remaining budget rather than a
    fresh full second counted from the lock request. The pre-change gate,
    which starts the server lock timeout only after checkout, hands the
    server the full second instead. Neither acquisition enters its guarded
    body, the refusal releases the connection, and a same-agent retry
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
        capacity_entered = asyncio.Event()
        capacity_lock_requested = asyncio.Event()
        binds: list[str] = []
        requests: list[float] = []
        carried: list[str] = []

        def before_lock_request(
            connection: Any,
            cursor: Any,
            statement: str,
            parameters: Any,
            context: Any,
            executemany: bool,
        ) -> None:
            """Observe the lock timeout each real acquisition actually carries.

            Non-substitutive event instrumentation: SQLAlchemy wraps a
            handler without ``retval`` to return the statement and parameters
            unchanged. The observations run on the real acquiring connection
            inside its acquisition transaction: the bound the gate itself sent
            with ``set_config``, and, for the acquisition under study, the live
            server setting read on that connection immediately before its
            advisory lock statement is sent. The first advisory request
            belongs to the capacity holder; the studied acquisition is the
            later one.
            @spec PROTECTED-HOOK-SOURCE-2.
            """
            # The asyncpg dialect compiles ``:bound`` to a positional
            # parameter, so the DBAPI-level ``parameters`` arrive as a tuple;
            # the named values are the compiled parameters the execution
            # context carries, and raw ``exec_driver_sql`` statements carry
            # none.
            for named in list(getattr(context, "compiled_parameters", None) or []):
                if isinstance(named, dict) and named.get("bound") is not None:
                    binds.append(str(named["bound"]))
            if "pg_advisory_xact_lock" in str(statement):
                if capacity_lock_requested.is_set():
                    requests.append(time.monotonic())
                    carried.append(
                        str(
                            connection.exec_driver_sql(
                                "SELECT current_setting('lock_timeout')"
                            ).scalar()
                        )
                    )
                else:
                    capacity_lock_requested.set()

        event.listen(gate.sync_engine, "before_cursor_execute", before_lock_request)

        async def capacity() -> None:
            """Hold the only gate connection for a real bounded 0.8 seconds.

            One real acquisition on the conflicted agent: it checks out the
            only connection, asks for the advisory lock and is cut at its own
            budget, so the connection returns to the pool exactly when the
            fixed code's own deadline releases it, with no test-side clock
            wait, @spec PROTECTED-HOOK-SOURCE-2.
            """
            try:
                async with source.hold(agent, wait_seconds=0.8):
                    capacity_entered.set()
                    pytest.fail("the capacity holder entered its guarded body")
            except source_sql.SourceSnapshotUnavailable:
                pass

        capacity_task: Any = None
        try:
            async with conflicting_lock(url, agent):
                capacity_task = asyncio.create_task(capacity())
                await asyncio.wait_for(capacity_lock_requested.wait(), 5)
                started = time.monotonic()
                with pytest.raises(source_sql.SourceSnapshotUnavailable):
                    async with source.hold(agent, wait_seconds=1.0):
                        entered.set()
                refused = time.monotonic()
                await capacity_task
                assert not capacity_entered.is_set()
                assert not entered.is_set()
                # The server side lock timeout the acquiring connection
                # actually carries is the budget remaining after the pool
                # wait (about 0.2 seconds of the 1.0 second budget), not a
                # fresh full second counted from checkout.
                assert len(binds) >= 2, binds
                bound = lock_timeout_ms(binds[-1])
                assert 10 <= bound <= 500, binds
                assert carried, "the gate connection never reached the lock wait"
                assert lock_timeout_ms(carried[-1]) == bound, (carried, binds)
                # The lock request starts only once the connection is checked
                # out, after the capacity holder's bounded hold of it.
                lock_wait_started = requests[-1]
                assert lock_wait_started - started >= 0.5, lock_wait_started - started
                assert refused - lock_wait_started <= 0.5, refused - lock_wait_started
                assert refused - started < 1.5, refused - started
                assert gate.pool.checkedout() == 0
            assert await advisory_locks(url) == 0
            async with source.hold(agent, wait_seconds=5.0):
                assert gate.pool.checkedout() == 1
            assert gate.pool.checkedout() == 0
        finally:
            event.remove(gate.sync_engine, "before_cursor_execute", before_lock_request)
            if capacity_task is not None and not capacity_task.done():
                capacity_task.cancel()
                await asyncio.gather(capacity_task, return_exceptions=True)
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
    The pool-phase cancellation is issued after one scheduling yield, once
    the acquisition waits on the exhausted pool (nothing in the acquisition
    yields before checkout), and the lock-phase cancellation is issued after
    the advisory lock statement is observed on the gate connection, so each
    cancellation lands on the real wait it names.
    @spec PROTECTED-HOOK-SOURCE-2.
    """
    agent = uuid.uuid4()

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-2."""
        url = get_settings().database_url
        gate = create_async_engine(url, pool_size=1, max_overflow=0, pool_timeout=3)
        source = source_sql.SourceGate(gate)
        lock_requested = asyncio.Event()

        def before_lock_request(
            connection: Any,
            cursor: Any,
            statement: str,
            parameters: Any,
            context: Any,
            executemany: bool,
        ) -> None:
            """Observe the acquisition reaching its real advisory lock wait.

            Non-substitutive event instrumentation on the gate engine: the
            handler fires when the acquiring connection sends the advisory
            lock statement, so a cancellation issued after it lands on the
            actual server side lock wait, @spec PROTECTED-HOOK-SOURCE-2.
            """
            if "pg_advisory_xact_lock" in str(statement):
                lock_requested.set()

        event.listen(gate.sync_engine, "before_cursor_execute", before_lock_request)

        async def waiting(budget: float) -> None:
            """A bounded acquisition that must never enter its body."""
            async with source.hold(agent, wait_seconds=budget):
                pytest.fail("cancelled acquisition entered its guarded body")

        waiter: Any = None
        try:
            if phase == "pool":
                async with gate.connect():
                    waiter = asyncio.create_task(waiting(0.5))
                    # One scheduling yield runs the acquisition to its
                    # first suspension, which is the pool wait: the pool is
                    # exhausted and nothing before checkout yields.
                    await asyncio.sleep(0)
                    assert not lock_requested.is_set()
                    waiter.cancel()
                    with pytest.raises(asyncio.CancelledError):
                        await waiter
                    assert gate.pool.checkedout() == 1
            else:
                async with conflicting_lock(url, agent):
                    waiter = asyncio.create_task(waiting(30.0))
                    await asyncio.wait_for(lock_requested.wait(), 5)
                    waiter.cancel()
                    with pytest.raises(asyncio.CancelledError):
                        await waiter
                    assert gate.pool.checkedout() == 0
                assert await advisory_locks(url) == 0
            async with source.hold(agent, wait_seconds=5.0):
                assert gate.pool.checkedout() == 1
            assert gate.pool.checkedout() == 0
        finally:
            event.remove(gate.sync_engine, "before_cursor_execute", before_lock_request)
            if waiter is not None and not waiter.done():
                waiter.cancel()
                await asyncio.gather(waiter, return_exceptions=True)
            await gate.dispose()

    asyncio.run(asyncio.wait_for(scenario(), 20))


def test_cumulative_acquisition_release_before_expiry_permits_and_holder_outlives_budget(
    deadline_db: None,
) -> None:
    """Released capacity is acquired in budget, and the holder outlives it.

    A capacity acquisition on a second, conflicted agent holds the only gate
    connection for its own 0.4 second budget, so the one second acquisition
    under study spends that hold waiting for the connection and is then
    granted the gate inside its budget. The budget ends while the guarded
    body is still running: the body crosses the recorded acquisition
    deadline under its own real bounded work (one more acquisition that
    refuses on the exhausted pool after a whole further second), the held
    connection still serves SQL registration with the server lock timeout
    cleared on it, and the transaction commits at exit without any residual
    lock or checked-out connection.
    @spec PROTECTED-HOOK-SOURCE-2.
    """
    agent = uuid.uuid4()
    other = uuid.uuid4()
    probe = uuid.uuid4()

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-2."""
        url = get_settings().database_url
        gate = create_async_engine(url, pool_size=1, max_overflow=0, pool_timeout=3)
        source = source_sql.SourceGate(gate)
        capacity_lock_requested = asyncio.Event()

        def before_lock_request(
            connection: Any,
            cursor: Any,
            statement: str,
            parameters: Any,
            context: Any,
            executemany: bool,
        ) -> None:
            """Observe the capacity holder reaching its advisory lock wait.

            Non-substitutive event instrumentation on the gate engine: the
            handler fires when the capacity acquisition sends its advisory
            lock statement, which is exactly when it holds the only
            connection, @spec PROTECTED-HOOK-SOURCE-2.
            """
            if "pg_advisory_xact_lock" in str(statement):
                capacity_lock_requested.set()

        event.listen(gate.sync_engine, "before_cursor_execute", before_lock_request)

        async def capacity() -> None:
            """Hold the only gate connection for a real bounded 0.4 seconds.

            One real acquisition on the conflicted second agent: it checks
            out the only connection, asks for the advisory lock and is cut at
            its own budget, so the connection returns to the pool exactly
            when the fixed code's own deadline releases it, with no test-side
            clock wait, @spec PROTECTED-HOOK-SOURCE-2.
            """
            try:
                async with source.hold(other, wait_seconds=0.4):
                    pytest.fail("the conflicted capacity holder entered its guarded body")
            except source_sql.SourceSnapshotUnavailable:
                pass

        capacity_task: Any = None
        try:
            async with conflicting_lock(url, other):
                capacity_task = asyncio.create_task(capacity())
                await asyncio.wait_for(capacity_lock_requested.wait(), 5)
                acquired = time.monotonic()
                async with source.hold(agent, wait_seconds=1.0) as held:
                    await capacity_task
                    # The guarded body crosses the acquisition deadline
                    # under its own real bounded work: one more acquisition
                    # refuses on the exhausted pool after a whole further
                    # second, past the deadline recorded for this one.
                    with pytest.raises(source_sql.SourceSnapshotUnavailable):
                        async with source.hold(probe, wait_seconds=1.0):
                            pytest.fail("the crossed-deadline probe acquired the gate")
                    assert time.monotonic() - acquired > 1.0, time.monotonic() - acquired
                    assert held.active
                    await source_sql.ensure_source_gate_live(held)
                    assert (
                        await held._connection.scalar(
                            text("SELECT current_setting('lock_timeout')")
                        )
                        == "0"
                    )
                assert gate.pool.checkedout() == 0
            assert await advisory_locks(url) == 0
        finally:
            event.remove(gate.sync_engine, "before_cursor_execute", before_lock_request)
            if capacity_task is not None and not capacity_task.done():
                capacity_task.cancel()
                await asyncio.gather(capacity_task, return_exceptions=True)
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
