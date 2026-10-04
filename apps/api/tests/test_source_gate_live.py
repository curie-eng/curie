"""Held-connection liveness, @spec PROTECTED-HOOK-SOURCE-2."""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager, suppress
from dataclasses import replace
from typing import Any

import pytest
from _migration_support import IsolatedMigrationDb
from curie_api.config import get_settings
from curie_protected_hooks import source_policy_sql as source_sql
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import NullPool


@pytest.fixture
def live_db(isolated_migration_db: IsolatedMigrationDb) -> None:
    """@spec PROTECTED-HOOK-SOURCE-2."""
    isolated_migration_db.at("head")


def live_probe() -> Any:
    """@spec PROTECTED-HOOK-SOURCE-2."""
    probe = getattr(source_sql, "ensure_source_gate_live", None)
    assert callable(probe), "PROTECTED-HOOK-SOURCE-2: held source gate liveness probe missing"
    return probe


@asynccontextmanager
async def pools() -> AsyncIterator[Any]:
    """@spec PROTECTED-HOOK-SOURCE-2."""
    url = get_settings().database_url
    gate = create_async_engine(url, pool_size=2, max_overflow=0, pool_timeout=2)
    work = create_async_engine(url, pool_size=1, max_overflow=0, pool_timeout=2)
    observer = create_async_engine(url, poolclass=NullPool)
    try:
        yield source_sql.SourceGate(gate), gate, work, observer
    finally:
        await gate.dispose()
        await work.dispose()
        await observer.dispose()


def test_live_probe_uses_held_connection_without_work_checkout(live_db: None) -> None:
    """@spec PROTECTED-HOOK-SOURCE-2."""
    probe = live_probe()

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-2."""
        async with pools() as (source, gate, work, _observer):
            assert gate.pool is not work.pool
            async with work.connect(), source.hold(uuid.uuid4()) as held:
                assert work.pool.checkedout() == 1
                async with asyncio.timeout(1):
                    assert await probe(held) is None
                assert gate.pool.checkedout() == 1
                assert work.pool.checkedout() == 1
                assert held.active

    asyncio.run(asyncio.wait_for(scenario(), 10))


@pytest.mark.parametrize("invalid", [None, object()])
def test_live_probe_refuses_invalid_context_type(live_db: None, invalid: Any) -> None:
    """@spec PROTECTED-HOOK-SOURCE-2."""
    probe = live_probe()

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-2."""
        with pytest.raises(source_sql.SourceGateInvalid):
            await probe(invalid)

    asyncio.run(asyncio.wait_for(scenario(), 10))


def test_live_probe_refuses_unregistered_context_copy(live_db: None) -> None:
    """@spec PROTECTED-HOOK-SOURCE-2."""
    probe = live_probe()

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-2."""
        async with pools() as (source, _gate, _work, _observer):
            async with source.hold(uuid.uuid4()) as held:
                with pytest.raises(source_sql.SourceGateInvalid):
                    await probe(replace(held))
                assert await probe(held) is None

    asyncio.run(asyncio.wait_for(scenario(), 10))


def test_live_probe_refuses_other_task_even_when_context_inherited(live_db: None) -> None:
    """@spec PROTECTED-HOOK-SOURCE-2."""
    probe = live_probe()

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-2."""
        async with pools() as (source, _gate, _work, _observer):
            async with source.hold(uuid.uuid4()) as held:

                async def borrower() -> None:
                    """@spec PROTECTED-HOOK-SOURCE-2."""
                    with pytest.raises(source_sql.SourceGateInvalid):
                        await probe(held)

                await asyncio.create_task(borrower())
                assert await probe(held) is None

    asyncio.run(asyncio.wait_for(scenario(), 10))


def test_live_probe_refuses_context_after_scope_closes(live_db: None) -> None:
    """@spec PROTECTED-HOOK-SOURCE-2."""
    probe = live_probe()

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-2."""
        async with pools() as (source, _gate, _work, _observer):
            async with source.hold(uuid.uuid4()) as held:
                assert await probe(held) is None
            with pytest.raises(source_sql.SourceGateInvalid):
                await probe(held)

    asyncio.run(asyncio.wait_for(scenario(), 10))


def test_live_probe_detects_actual_backend_termination_before_effect(live_db: None) -> None:
    """@spec PROTECTED-HOOK-SOURCE-2."""
    probe = live_probe()

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-2."""
        async with pools() as (source, _gate, _work, observer):
            errors = []
            with suppress(source_sql.SourceSnapshotUnavailable):
                async with source.hold(uuid.uuid4()) as held:
                    assert await probe(held) is None
                    async with observer.connect() as conn:
                        pid = await conn.scalar(
                            text(
                                "SELECT l.pid FROM pg_locks l "
                                "JOIN pg_database d ON d.oid=l.database "
                                "WHERE l.locktype='advisory' AND l.granted "
                                "AND d.datname=current_database()"
                            )
                        )
                        assert pid is not None
                        assert await conn.scalar(
                            text("SELECT pg_terminate_backend(:pid)"), {"pid": pid}
                        )
                    with pytest.raises(
                        (source_sql.SourceGateInvalid, source_sql.SourceSnapshotUnavailable)
                    ) as caught:
                        await probe(held)
                    errors.append(caught.value)
            assert len(errors) == 1
            assert "postgres" not in str(errors[0]).lower()
            async with source.hold(uuid.uuid4()) as successor:
                assert await probe(successor) is None

    asyncio.run(asyncio.wait_for(scenario(), 10))
