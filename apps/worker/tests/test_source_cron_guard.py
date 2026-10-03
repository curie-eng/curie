"""Real SQL cron source guard, @spec PROTECTED-HOOK-SOURCE-2/10."""

from __future__ import annotations

import asyncio
import importlib
import importlib.util
import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
from curie_protected_hooks.source_policy_records import target_intent_sha256
from curie_protected_hooks.source_policy_sql import (
    SourceAgentNotFound,
    SourceGate,
    SourceGateInvalid,
    SourceSnapshotUnavailable,
)
from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import create_async_engine


def startup_support() -> Any:
    """Public fixtures only, @spec PROTECTED-HOOK-SOURCE-2/10."""
    path = Path(__file__).with_name("test_source_worker_startup.py")
    spec = importlib.util.spec_from_file_location("_source_cron_guard_setup", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


startup = startup_support()
worker_db = startup.worker_db
worker_templates = startup.worker_templates


@pytest.fixture
def guard_db(worker_db: Any) -> tuple[Any, str, uuid.UUID]:
    """@spec PROTECTED-HOOK-SOURCE-2/10."""
    support, _, url = worker_db
    agent = uuid.uuid4()
    support.sql_dicts(
        "INSERT INTO curie.agents(id,name) VALUES(:id,:name)",
        {"id": agent, "name": "guard-" + agent.hex},
    )
    return support, url, agent


def module() -> Any:
    """@spec PROTECTED-HOOK-SOURCE-2/10."""
    name = "curie_worker.hook_source_guard"
    assert importlib.util.find_spec(name) is not None, (
        "SOURCE-2 worker cron source guard is missing"
    )
    return importlib.import_module(name)


@asynccontextmanager
async def pools(url: str) -> AsyncIterator[Any]:
    """@spec PROTECTED-HOOK-SOURCE-2."""
    gate = create_async_engine(url, pool_size=2, max_overflow=0, pool_timeout=2)
    work = create_async_engine(url, pool_size=1, max_overflow=0, pool_timeout=2)
    observer = create_async_engine(url, pool_size=1, max_overflow=0)
    try:
        yield gate, work, observer
    finally:
        await gate.dispose()
        await work.dispose()
        await observer.dispose()


def seed(support: Any, agent: uuid.UUID, hook: str, *, mode: str | None) -> None:
    """SQL fixture grants no runtime authority, @spec PROTECTED-HOOK-SOURCE-2/10."""
    operation = uuid.uuid4()
    desired = dict(
        mode="ordinary",
        tool_access=None,
        runtime_id=None,
        qualification_id=None,
        bundle_digest=None,
    )
    if mode == "protected":
        desired.update(
            mode="protected",
            tool_access="read-only",
            runtime_id=str(uuid.uuid4()),
            qualification_id=str(uuid.uuid4()),
            bundle_digest="a" * 64,
        )
    intent = target_intent_sha256(desired)
    support.sql_dicts(
        "INSERT INTO curie.hook_source_operations "
        "(agent_id,hook,operation_id,generation,intent_sha256,status) "
        "VALUES(:agent,:hook,:operation,1,:intent,:status)",
        dict(
            agent=agent,
            hook=hook,
            operation=operation,
            intent=intent,
            status="pending" if mode is None else "committed",
        ),
    )
    if mode is not None:
        support.sql_dicts(
            "INSERT INTO curie.hook_source_policies "
            "(agent_id,hook,generation,operation_id,mode,tool_access,runtime_id,"
            "qualification_id,bundle_digest,legacy_generation,updated_at) "
            "VALUES(:agent,:hook,1,:operation,:mode,:tool_access,:runtime_id,:qualification_id,:bundle_digest,0,:updated)",
            dict(agent=agent, hook=hook, operation=operation, updated=datetime.now(UTC), **desired),
        )


@pytest.mark.parametrize("hook", ["daily-summary", "Daily Summary/legacy"])
def test_exact_absent_source_positive_keeps_work_pool_free_in_scope(
    guard_db: Any, hook: str
) -> None:
    """@spec PROTECTED-HOOK-SOURCE-2/10."""
    _, url, agent = guard_db
    product = module()

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-2/10."""
        async with pools(url) as (gate, work, _):
            guard = product.CronHookSourceGuard(SourceGate(gate), work)
            async with guard.locked_snapshot(agent, hook) as context:
                guard.validate_context(context, agent, hook)
                assert await context.ensure_before_effect(agent, hook) is None
                assert work.pool.checkedout() == 0
                async with work.connect() as connection:
                    assert await connection.scalar(text("SELECT 1")) == 1

    asyncio.run(scenario())


@pytest.mark.parametrize("mode", ["ordinary", "protected", None])
@pytest.mark.parametrize("hook", ["daily-summary", "Daily Summary/legacy"])
def test_any_exact_policy_or_history_refuses_without_mutation(
    guard_db: Any, mode: str | None, hook: str
) -> None:
    """@spec PROTECTED-HOOK-SOURCE-2/10."""
    support, url, agent = guard_db
    seed(support, agent, hook, mode=mode)
    before = (
        support.sql_dicts("SELECT * FROM curie.hook_source_operations ORDER BY generation"),
        support.sql_dicts("SELECT * FROM curie.hook_source_policies ORDER BY hook"),
        support.sql_dicts("SELECT hook_generation FROM curie.agents WHERE id=:id", {"id": agent}),
    )
    product = module()

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-2/10."""
        async with pools(url) as (gate, work, _):
            guard = product.CronHookSourceGuard(SourceGate(gate), work)
            with pytest.raises(
                SourceSnapshotUnavailable,
                match="pending_history" if mode is None else "authority_unavailable",
            ):
                async with guard.locked_snapshot(agent, hook):
                    pytest.fail("configured source admitted ordinary cron")

    asyncio.run(scenario())
    assert (
        support.sql_dicts("SELECT * FROM curie.hook_source_operations ORDER BY generation"),
        support.sql_dicts("SELECT * FROM curie.hook_source_policies ORDER BY hook"),
        support.sql_dicts("SELECT hook_generation FROM curie.agents WHERE id=:id", {"id": agent}),
    ) == before


def test_legacy_exact_name_does_not_alias_configured_canonical_neighbor(guard_db: Any) -> None:
    """@spec PROTECTED-HOOK-SOURCE-2/10."""
    support, url, agent = guard_db
    seed(support, agent, "daily-summary", mode="ordinary")
    product = module()

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-2/10."""
        async with pools(url) as (gate, work, _):
            guard = product.CronHookSourceGuard(SourceGate(gate), work)
            async with guard.locked_snapshot(agent, "Daily-Summary") as context:
                assert await context.ensure_before_effect(agent, "Daily-Summary") is None

    asyncio.run(scenario())


@pytest.mark.parametrize("hook", ["daily-summary", "Daily Summary/legacy"])
def test_missing_source_table_never_becomes_ordinary(guard_db: Any, hook: str) -> None:
    """@spec PROTECTED-HOOK-SOURCE-2/10."""
    support, url, agent = guard_db
    support.sql_dicts("DROP TABLE curie.hook_source_operations")
    product = module()

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-2/10."""
        async with pools(url) as (gate, work, _):
            guard = product.CronHookSourceGuard(SourceGate(gate), work)
            with pytest.raises(SourceSnapshotUnavailable):
                async with guard.locked_snapshot(agent, hook):
                    pytest.fail("unreadable source table admitted cron")

    asyncio.run(scenario())


def test_real_context_exact_owner_agent_name_task_and_scope(guard_db: Any) -> None:
    """@spec PROTECTED-HOOK-SOURCE-2."""
    _, url, agent = guard_db
    product = module()

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-2."""
        async with pools(url) as (gate, work, _):
            guard = product.CronHookSourceGuard(SourceGate(gate), work)
            other_guard = product.CronHookSourceGuard(SourceGate(gate), work)
            async with guard.locked_snapshot(agent, "daily-summary") as context:
                with pytest.raises(SourceGateInvalid):
                    other_guard.validate_context(context, agent, "daily-summary")
                for wrong_agent, wrong_name in [
                    (uuid.uuid4(), "daily-summary"),
                    (agent, "Daily-Summary"),
                ]:
                    with pytest.raises(SourceGateInvalid):
                        guard.validate_context(context, wrong_agent, wrong_name)
                    with pytest.raises(SourceGateInvalid):
                        await context.ensure_before_effect(wrong_agent, wrong_name)

                async def transferred() -> None:
                    """@spec PROTECTED-HOOK-SOURCE-2."""
                    with pytest.raises(SourceGateInvalid):
                        guard.validate_context(context, agent, "daily-summary")
                    with pytest.raises(SourceGateInvalid):
                        await context.ensure_before_effect(agent, "daily-summary")

                await asyncio.create_task(transferred())
            with pytest.raises(SourceGateInvalid):
                guard.validate_context(context, agent, "daily-summary")
            with pytest.raises(SourceGateInvalid):
                await context.ensure_before_effect(agent, "daily-summary")

    asyncio.run(scenario())


def test_constructor_rejects_same_pool_alias_and_different_database(guard_db: Any) -> None:
    """@spec PROTECTED-HOOK-SOURCE-2."""
    _, url, _ = guard_db
    product = module()

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-2."""
        async with pools(url) as (gate, _, _):
            with pytest.raises(SourceGateInvalid):
                product.CronHookSourceGuard(SourceGate(gate), gate.execution_options())
            different = create_async_engine(
                make_url(url).set(database="missing_guard_" + uuid.uuid4().hex)
            )
            try:
                with pytest.raises(SourceGateInvalid):
                    product.CronHookSourceGuard(SourceGate(gate), different)
                assert gate.pool.checkedout() == 0
            finally:
                await different.dispose()

    asyncio.run(scenario())


def test_real_advisory_wait_precedes_work_checkout_and_fresh_resolution(guard_db: Any) -> None:
    """@spec PROTECTED-HOOK-SOURCE-2/10."""
    _, url, agent = guard_db
    product = module()

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-2/10."""
        async with pools(url) as (gate, work, observer):
            source = SourceGate(gate)
            guard = product.CronHookSourceGuard(source, work)

            async def enter() -> None:
                """@spec PROTECTED-HOOK-SOURCE-2/10."""
                async with guard.locked_snapshot(agent, "daily-summary") as context:
                    await context.ensure_before_effect(agent, "daily-summary")

            async with source.hold(agent):
                task = asyncio.create_task(enter())
                try:
                    async with asyncio.timeout(4):
                        while True:
                            async with observer.connect() as connection:
                                waiting = await connection.scalar(
                                    text(
                                        "SELECT EXISTS(SELECT 1 FROM pg_stat_activity "
                                        "WHERE datname=current_database() "
                                        "AND wait_event='advisory')"
                                    )
                                )
                            if waiting:
                                break
                            assert not task.done()
                            await asyncio.sleep(0.01)
                    assert work.pool.checkedout() == 0
                    async with work.connect() as connection:
                        assert await connection.scalar(text("SELECT 1")) == 1
                        await connection.execute(
                            text(
                                "INSERT INTO curie.hook_source_operations "
                                "(agent_id,hook,operation_id,intent_sha256,status,generation) "
                                "VALUES(:agent,'daily-summary',:op,:intent,'pending',1)"
                            ),
                            dict(agent=agent, op=uuid.uuid4(), intent="a" * 64),
                        )
                        await connection.commit()
                finally:
                    if task.done():
                        await task
            with pytest.raises(SourceSnapshotUnavailable, match="pending_history"):
                await task

    asyncio.run(asyncio.wait_for(scenario(), 10))


def test_actual_gate_backend_loss_refuses_next_effect(guard_db: Any) -> None:
    """@spec PROTECTED-HOOK-SOURCE-2."""
    _, url, agent = guard_db
    product = module()

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-2."""
        async with pools(url) as (gate, work, observer):
            guard = product.CronHookSourceGuard(SourceGate(gate), work)
            with pytest.raises((SourceSnapshotUnavailable, SourceGateInvalid)):
                async with guard.locked_snapshot(agent, "daily-summary") as context:
                    async with observer.connect() as connection:
                        pid = await connection.scalar(
                            text(
                                "SELECT l.pid FROM pg_locks l "
                                "JOIN pg_stat_activity a ON a.pid=l.pid "
                                "WHERE a.datname=current_database() AND l.locktype='advisory' "
                                "AND l.granted"
                            )
                        )
                        assert pid is not None
                        assert await connection.scalar(
                            text("SELECT pg_terminate_backend(:pid)"), {"pid": pid}
                        )
                    await context.ensure_before_effect(agent, "daily-summary")
                    pytest.fail("dead gate authorized an effect")

    asyncio.run(scenario())


@pytest.mark.parametrize("kind", ["unknown_agent", "inconsistent_current"])
def test_unknown_or_inconsistent_canonical_source_refuses(guard_db: Any, kind: str) -> None:
    """@spec PROTECTED-HOOK-SOURCE-2/10."""
    support, url, agent = guard_db
    if kind == "unknown_agent":
        agent = uuid.uuid4()
    else:
        seed(support, agent, "daily-summary", mode="ordinary")
        support.sql_dicts(
            "UPDATE curie.hook_source_policies SET operation_id=:op WHERE agent_id=:agent",
            {"op": uuid.uuid4(), "agent": agent},
        )
    product = module()

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-2/10."""
        async with pools(url) as (gate, work, _):
            guard = product.CronHookSourceGuard(SourceGate(gate), work)
            refusal = SourceAgentNotFound if kind == "unknown_agent" else SourceSnapshotUnavailable
            with pytest.raises(refusal):
                async with guard.locked_snapshot(agent, "daily-summary"):
                    pytest.fail("missing or inconsistent source admitted ordinary cron")

    asyncio.run(scenario())
