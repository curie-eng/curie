"""Real source gate contracts, @spec PROTECTED-HOOK-SOURCE-2/10."""

from __future__ import annotations

import asyncio
import hashlib
import importlib
import importlib.util
import json
import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from typing import Any

import pytest
from _migration_support import IsolatedMigrationDb, sql_dicts
from curie_api.config import get_settings
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine
from sqlalchemy.pool import NullPool


@pytest.fixture
def gate_db(isolated_migration_db: IsolatedMigrationDb) -> tuple[uuid.UUID, uuid.UUID]:
    """@spec PROTECTED-HOOK-SOURCE-2/10."""
    isolated_migration_db.at("head")
    ids = (uuid.uuid4(), uuid.uuid4())
    for agent_id in ids:
        sql_dicts(
            "INSERT INTO curie.agents (id, name) VALUES (:id, :name)",
            {"id": agent_id, "name": "gate-test-" + agent_id.hex},
        )
    return ids


def gate_module() -> Any:
    """Require a product assertion, @spec PROTECTED-HOOK-SOURCE-2/10."""
    assert importlib.util.find_spec("curie_protected_hooks.source_policy_sql") is not None, (
        "PROTECTED-HOOK-SOURCE-2/10: typed source SQL gate is not implemented"
    )
    return importlib.import_module("curie_protected_hooks.source_policy_sql")


@asynccontextmanager
async def engines() -> AsyncIterator[tuple[AsyncEngine, AsyncEngine, AsyncEngine]]:
    """Actual isolated pools, @spec PROTECTED-HOOK-SOURCE-2/10."""
    url = get_settings().database_url
    gate = create_async_engine(url, pool_size=2, max_overflow=0, pool_timeout=2)
    work = create_async_engine(url, pool_size=1, max_overflow=0, pool_timeout=2)
    observer = create_async_engine(url, poolclass=NullPool)
    try:
        yield gate, work, observer
    finally:
        await gate.dispose()
        await work.dispose()
        await observer.dispose()


async def waiting_advisory(observer: AsyncEngine) -> None:
    """Actual PG lock evidence, @spec PROTECTED-HOOK-SOURCE-2."""
    async with asyncio.timeout(5):
        while True:
            async with observer.connect() as conn:
                waiting = await conn.scalar(
                    text(
                        "SELECT EXISTS (SELECT 1 FROM pg_stat_activity "
                        "WHERE datname = current_database() AND wait_event = 'advisory')"
                    )
                )
            if waiting:
                return
            await asyncio.sleep(0.01)


def seed_attempt(agent_id: uuid.UUID, *, generation: int, status: str = "pending") -> uuid.UUID:
    """@spec PROTECTED-HOOK-SOURCE-10."""
    operation = uuid.uuid4()
    target = {
        "mode": "ordinary",
        "tool_access": None,
        "runtime_id": None,
        "qualification_id": None,
        "bundle_digest": None,
    }
    intent = hashlib.sha256(
        json.dumps(
            target,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
        ).encode("ascii")
    ).hexdigest()
    sql_dicts(
        "INSERT INTO curie.hook_source_operations "
        "(agent_id,hook,operation_id,intent_sha256,status,generation) "
        "VALUES (:agent,'daily-summary',:operation,:intent,:status,:generation)",
        {
            "agent": agent_id,
            "operation": operation,
            "intent": intent,
            "status": status,
            "generation": generation,
        },
    )
    return operation


def seed_policy(agent_id: uuid.UUID, operation: uuid.UUID, generation: int) -> None:
    """@spec PROTECTED-HOOK-SOURCE-2/10."""
    sql_dicts(
        "INSERT INTO curie.hook_source_policies "
        "(agent_id,hook,generation,operation_id,mode,legacy_generation,updated_at) "
        "VALUES (:agent,'daily-summary',:generation,:operation,'ordinary',0,:updated)",
        {
            "agent": agent_id,
            "operation": operation,
            "generation": generation,
            "updated": datetime.now(UTC),
        },
    )


def test_same_agent_absent_policy_waits_but_other_agent_proceeds(gate_db: Any) -> None:
    """@spec PROTECTED-HOOK-SOURCE-2."""
    module = gate_module()

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-2."""
        async with engines() as (gate, work, observer):
            source = module.SourceGate(gate)
            async with source.hold(gate_db[0]):

                async def wait_same() -> None:
                    """@spec PROTECTED-HOOK-SOURCE-2."""
                    async with source.hold(gate_db[0]):
                        pass

                task = asyncio.create_task(wait_same())
                try:
                    await waiting_advisory(observer)
                    assert not task.done()
                    # Same two-slot gate pool is occupied; another independent gate
                    # connection demonstrates the lock is agent-scoped.
                    async with module.SourceGate(observer).hold(gate_db[1]):
                        pass
                finally:
                    task.cancel()
                    with pytest.raises(asyncio.CancelledError):
                        await task
            async with source.hold(gate_db[0]) as held:
                async with work.begin() as conn:
                    assert (
                        await module.read_source_snapshot(held, conn, "daily-summary")
                    ).never_configured

    asyncio.run(asyncio.wait_for(scenario(), 12))


def test_separate_work_commit_does_not_release_gate_or_starve_work_pool(gate_db: Any) -> None:
    """@spec PROTECTED-HOOK-SOURCE-2."""
    module = gate_module()

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-2."""
        async with engines() as (gate, work, observer):
            source = module.SourceGate(gate)
            entered = asyncio.Event()
            observed_counters = []

            async def waiter() -> None:
                """@spec PROTECTED-HOOK-SOURCE-2."""
                async with source.hold(gate_db[0]) as held:
                    async with work.begin() as conn:
                        snapshot = await module.read_source_snapshot(held, conn, "daily-summary")
                        observed_counters.append(snapshot.legacy_generation)
                    entered.set()

            async with source.hold(gate_db[0]):
                task = asyncio.create_task(waiter())
                try:
                    await waiting_advisory(observer)
                    async with work.begin() as conn:
                        await conn.execute(
                            text("UPDATE curie.agents SET hook_generation=1 WHERE id=:id"),
                            {"id": gate_db[0]},
                        )
                    assert not entered.is_set()
                    async with work.begin() as conn:
                        assert (
                            await conn.scalar(
                                text("SELECT hook_generation FROM curie.agents WHERE id=:id"),
                                {"id": gate_db[0]},
                            )
                            == 1
                        )
                    assert not entered.is_set()
                except BaseException:
                    task.cancel()
                    await asyncio.gather(task, return_exceptions=True)
                    raise
            await asyncio.wait_for(task, 5)
            assert entered.is_set()
            assert observed_counters == [1]

    asyncio.run(asyncio.wait_for(scenario(), 12))


@pytest.mark.parametrize("ending", ["error", "cancel"])
def test_holder_error_or_cancellation_releases_transaction(gate_db: Any, ending: str) -> None:
    """@spec PROTECTED-HOOK-SOURCE-2."""
    module = gate_module()

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-2."""
        async with engines() as (gate, work, observer):
            source = module.SourceGate(gate)
            ready = asyncio.Event()
            tokens = []

            async def holder() -> None:
                """@spec PROTECTED-HOOK-SOURCE-2."""
                async with source.hold(gate_db[0]) as held:
                    tokens.append(held)
                    ready.set()
                    if ending == "error":
                        raise RuntimeError("expected test error")
                    await asyncio.Event().wait()

            task = asyncio.create_task(holder())
            await asyncio.wait_for(ready.wait(), 5)
            if ending == "cancel":
                task.cancel()
            with pytest.raises(asyncio.CancelledError if ending == "cancel" else RuntimeError):
                await task
            assert not tokens[0].active
            async with source.hold(gate_db[0]):
                pass
            async with work.begin() as conn:
                with pytest.raises(module.SourceGateInvalid):
                    await module.read_source_snapshot(tokens[0], conn, "daily-summary")

    asyncio.run(asyncio.wait_for(scenario(), 12))


def test_nested_same_agent_gate_and_same_engine_work_are_refused(gate_db: Any) -> None:
    """@spec PROTECTED-HOOK-SOURCE-2."""
    module = gate_module()

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-2."""
        async with engines() as (gate, _work, _observer):
            source = module.SourceGate(gate)
            async with source.hold(gate_db[0]) as held:
                with pytest.raises(module.SourceGateInvalid):
                    async with module.SourceGate(gate).hold(gate_db[0]):
                        pytest.fail("nested agent gate acquired")
                async with gate.begin() as conn:
                    with pytest.raises(module.SourceGateInvalid):
                        await module.read_source_snapshot(held, conn, "daily-summary")

    asyncio.run(asyncio.wait_for(scenario(), 12))


def test_context_cannot_be_borrowed_by_another_task(gate_db: Any) -> None:
    """@spec PROTECTED-HOOK-SOURCE-2."""
    module = gate_module()

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-2."""
        async with engines() as (gate, work, _observer):
            async with module.SourceGate(gate).hold(gate_db[0]) as held:

                async def borrower() -> None:
                    """@spec PROTECTED-HOOK-SOURCE-2."""
                    async with work.begin() as conn:
                        with pytest.raises(module.SourceGateInvalid):
                            await module.read_source_snapshot(held, conn, "daily-summary")

                await asyncio.wait_for(asyncio.create_task(borrower()), 5)

    asyncio.run(asyncio.wait_for(scenario(), 12))


@pytest.mark.parametrize("history", [False, True])
def test_absent_policy_is_ordinary_only_without_history(gate_db: Any, history: bool) -> None:
    """@spec PROTECTED-HOOK-SOURCE-2/10."""
    module = gate_module()
    if history:
        seed_attempt(gate_db[0], generation=2**53 + 17)

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-2/10."""
        async with engines() as (gate, work, _observer):
            async with module.SourceGate(gate).hold(gate_db[0]) as held:
                async with work.begin() as conn:
                    result = await module.read_source_snapshot(held, conn, "daily-summary")
                assert result.policy is None
                assert result.attempt_history_present is history
                assert result.never_configured is (not history)
                assert result.refusal_reason == ("pending_history" if history else None)
                assert result.attempt_generation_highwater == (2**53 + 17 if history else 0)
                with pytest.raises((AttributeError, TypeError)):
                    result.legacy_generation = 99

    asyncio.run(asyncio.wait_for(scenario(), 12))


def test_current_policy_and_highwater_include_retained_pending_attempt(gate_db: Any) -> None:
    """@spec PROTECTED-HOOK-SOURCE-2/10."""
    module = gate_module()
    operation = seed_attempt(gate_db[0], generation=5, status="committed")
    seed_policy(gate_db[0], operation, 5)
    seed_attempt(gate_db[0], generation=2**53 + 17)

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-2/10."""
        async with engines() as (gate, work, _observer):
            async with module.SourceGate(gate).hold(gate_db[0]) as held:
                async with work.begin() as conn:
                    result = await module.read_source_snapshot(held, conn, "daily-summary")
                assert not result.never_configured and result.refusal_reason is None
                assert result.policy.operation_id == operation
                assert result.policy.generation == 5
                assert result.attempt_generation_highwater == 2**53 + 17
                with pytest.raises((AttributeError, TypeError)):
                    result.policy.generation = 6

    asyncio.run(asyncio.wait_for(scenario(), 12))


@pytest.mark.parametrize("inconsistent", ["missing", "pending", "generation", "intent"])
def test_policy_without_exact_committed_ledger_fails_closed(
    gate_db: Any, inconsistent: str
) -> None:
    """@spec PROTECTED-HOOK-SOURCE-2/10."""
    module = gate_module()
    operation = uuid.uuid4()
    if inconsistent != "missing":
        operation = seed_attempt(
            gate_db[0],
            generation=6 if inconsistent == "generation" else 5,
            status="pending" if inconsistent == "pending" else "committed",
        )
    seed_policy(gate_db[0], operation, 5)
    if inconsistent == "intent":
        # SQL-valid protected target, not the ordinary intent retained by ledger.
        sql_dicts(
            "UPDATE curie.hook_source_policies SET mode='protected', tool_access='read-only', "
            "runtime_id=:runtime, qualification_id=:qualification,bundle_digest=:bundle",
            {"runtime": str(uuid.uuid4()), "qualification": str(uuid.uuid4()), "bundle": "a" * 64},
        )

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-2/10."""
        async with engines() as (gate, work, _observer):
            async with module.SourceGate(gate).hold(gate_db[0]) as held:
                async with work.begin() as conn:
                    with pytest.raises(module.SourceSnapshotUnavailable):
                        await module.read_source_snapshot(held, conn, "daily-summary")

    asyncio.run(asyncio.wait_for(scenario(), 12))


def test_missing_table_is_unavailable_not_empty_policy(gate_db: Any) -> None:
    """@spec PROTECTED-HOOK-SOURCE-2/10."""
    module = gate_module()
    sql_dicts("DROP TABLE curie.hook_source_operations")

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-2/10."""
        async with engines() as (gate, work, _observer):
            async with module.SourceGate(gate).hold(gate_db[0]) as held:
                async with work.begin() as conn:
                    with pytest.raises(module.SourceSnapshotUnavailable) as error:
                        await module.read_source_snapshot(held, conn, "daily-summary")
                    assert "postgresql" not in str(error.value).lower()
                    assert get_settings().database_url not in str(error.value)

    asyncio.run(asyncio.wait_for(scenario(), 12))


def test_missing_agent_is_not_an_ordinary_source(gate_db: Any) -> None:
    """@spec PROTECTED-HOOK-SOURCE-2."""
    module = gate_module()

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-2."""
        async with engines() as (gate, work, _observer):
            async with module.SourceGate(gate).hold(uuid.uuid4()) as held:
                async with work.begin() as conn:
                    with pytest.raises(module.SourceAgentNotFound):
                        await module.read_source_snapshot(held, conn, "daily-summary")

    asyncio.run(asyncio.wait_for(scenario(), 12))


def test_repeatable_read_work_transaction_is_refused(gate_db: Any) -> None:
    """@spec PROTECTED-HOOK-SOURCE-2."""
    module = gate_module()

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-2."""
        async with engines() as (gate, work, _observer):
            async with work.connect() as conn:
                await conn.execution_options(isolation_level="REPEATABLE READ")
                async with conn.begin():
                    await conn.execute(text("SELECT 1"))
                    async with module.SourceGate(gate).hold(gate_db[0]) as held:
                        with pytest.raises(module.SourceGateInvalid):
                            await module.read_source_snapshot(held, conn, "daily-summary")

    asyncio.run(asyncio.wait_for(scenario(), 12))


def test_disconnected_holder_cannot_authorize_an_ordinary_snapshot(gate_db: Any) -> None:
    """@spec PROTECTED-HOOK-SOURCE-2."""
    module = gate_module()

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-2."""
        async with engines() as (gate, work, observer):
            source = module.SourceGate(gate)
            with pytest.raises((module.SourceGateInvalid, module.SourceSnapshotUnavailable)):
                async with source.hold(gate_db[0]) as held:
                    async with observer.begin() as conn:
                        pid = await conn.scalar(
                            text(
                                "SELECT l.pid FROM pg_locks l JOIN pg_database d "
                                "ON d.oid=l.database WHERE d.datname=current_database() "
                                "AND l.locktype='advisory' AND l.granted"
                            )
                        )
                        assert pid is not None
                        assert await conn.scalar(
                            text("SELECT pg_terminate_backend(:pid)"), {"pid": pid}
                        )
                    async with work.begin() as conn:
                        await module.read_source_snapshot(held, conn, "daily-summary")
                    pytest.fail("disconnected gate returned an ordinary snapshot")
            async with source.hold(gate_db[0]) as held:
                async with work.begin() as conn:
                    result = await module.read_source_snapshot(held, conn, "daily-summary")
                    assert result.never_configured

    asyncio.run(asyncio.wait_for(scenario(), 12))


def test_autocommit_gate_retains_actual_advisory_exclusion(gate_db: Any) -> None:
    """@spec PROTECTED-HOOK-SOURCE-2."""
    module = gate_module()

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-2."""
        async with engines() as (gate, _work, observer):
            autocommit = gate.execution_options(isolation_level="AUTOCOMMIT")
            entered = asyncio.Event()

            async def waiter() -> None:
                """@spec PROTECTED-HOOK-SOURCE-2."""
                async with module.SourceGate(observer).hold(gate_db[0]):
                    entered.set()

            async with module.SourceGate(autocommit).hold(gate_db[0]):
                task = asyncio.create_task(waiter())
                try:
                    await waiting_advisory(observer)
                    assert not entered.is_set()
                finally:
                    task.cancel()
                    await asyncio.gather(task, return_exceptions=True)
            async with module.SourceGate(observer).hold(gate_db[0]):
                pass

    asyncio.run(asyncio.wait_for(scenario(), 12))


def test_same_pool_alias_nested_gate_is_refused_immediately(gate_db: Any) -> None:
    """@spec PROTECTED-HOOK-SOURCE-2."""
    module = gate_module()

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-2."""
        async with engines() as (gate, _work, _observer):
            alias = gate.execution_options(isolation_level="READ COMMITTED")
            assert alias is not gate and alias.pool is gate.pool
            async with module.SourceGate(gate).hold(gate_db[0]):
                async with asyncio.timeout(2):
                    with pytest.raises(module.SourceGateInvalid):
                        async with module.SourceGate(alias).hold(gate_db[0]):
                            pytest.fail("same pool nested gate acquired")

    asyncio.run(asyncio.wait_for(scenario(), 12))


def test_same_pool_alias_work_connection_is_refused(gate_db: Any) -> None:
    """@spec PROTECTED-HOOK-SOURCE-2."""
    module = gate_module()

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-2."""
        async with engines() as (gate, _work, _observer):
            alias = gate.execution_options(isolation_level="READ COMMITTED")
            assert alias is not gate and alias.pool is gate.pool
            async with module.SourceGate(gate).hold(gate_db[0]) as held:
                async with alias.begin() as conn:
                    with pytest.raises(module.SourceGateInvalid):
                        await module.read_source_snapshot(held, conn, "daily-summary")

    asyncio.run(asyncio.wait_for(scenario(), 12))


def test_same_pool_alias_other_task_waits_then_proceeds(gate_db: Any) -> None:
    """@spec PROTECTED-HOOK-SOURCE-2."""
    module = gate_module()

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-2."""
        async with engines() as (gate, _work, observer):
            alias = gate.execution_options(isolation_level="READ COMMITTED")
            entered = asyncio.Event()

            async def waiter() -> None:
                """@spec PROTECTED-HOOK-SOURCE-2."""
                async with module.SourceGate(alias).hold(gate_db[0]):
                    entered.set()

            async with module.SourceGate(gate).hold(gate_db[0]):
                task = asyncio.create_task(waiter())
                try:
                    await waiting_advisory(observer)
                    assert not entered.is_set()
                except BaseException:
                    task.cancel()
                    await asyncio.gather(task, return_exceptions=True)
                    raise
            await asyncio.wait_for(task, 5)
            assert entered.is_set()

    asyncio.run(asyncio.wait_for(scenario(), 12))
