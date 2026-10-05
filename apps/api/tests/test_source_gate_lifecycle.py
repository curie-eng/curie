"""API gate composition ownership, @spec PROTECTED-HOOK-SOURCE-2/10."""

from __future__ import annotations

import asyncio
import uuid
from typing import Any

import pytest
from _migration_support import IsolatedMigrationDb
from botocore.exceptions import ParamValidationError
from curie_api import db
from curie_api.config import get_settings
from curie_api.main import create_app
from curie_protected_hooks.source_policy_sql import SourceGate
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import NullPool


@pytest.fixture
def lifecycle_db(
    isolated_migration_db: IsolatedMigrationDb, monkeypatch: pytest.MonkeyPatch
) -> None:
    """@spec PROTECTED-HOOK-SOURCE-2/10."""
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


def require_factory() -> Any:
    """@spec PROTECTED-HOOK-SOURCE-2."""
    factory = getattr(db, "create_source_gate_engine", None)
    assert callable(factory), "PROTECTED-HOOK-SOURCE-2: production source gate factory missing"
    return factory


async def assert_backend_closed(pid: int) -> None:
    """Actual PostgreSQL cleanup, @spec PROTECTED-HOOK-SOURCE-2."""
    observer = create_async_engine(get_settings().database_url, poolclass=NullPool)
    try:
        async with observer.connect() as conn:
            assert not await conn.scalar(
                text("SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE pid=:pid)"), {"pid": pid}
            )
    finally:
        await observer.dispose()


def test_factory_has_independent_bounded_production_pool(lifecycle_db: None) -> None:
    """@spec PROTECTED-HOOK-SOURCE-2."""
    factory = require_factory()

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-2."""
        gate, work = factory(), db.create_engine()
        try:
            assert gate.pool is not work.pool
            assert gate.pool.size() == 4
            assert gate.pool._max_overflow == 0
            assert gate.pool.timeout() == 30
            assert gate.pool._pre_ping
            async with gate.connect() as conn:
                assert await conn.scalar(text("SELECT 1")) == 1
        finally:
            await gate.dispose()
            await work.dispose()

    asyncio.run(asyncio.wait_for(scenario(), 15))


def test_lifespan_composes_gate_and_disposes_opened_backend(lifecycle_db: None) -> None:
    """@spec PROTECTED-HOOK-SOURCE-2."""
    require_factory()

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-2."""
        app = create_app()
        async with app.router.lifespan_context(app):
            source = app.state.source_gate
            assert isinstance(source, SourceGate)
            assert source.engine.pool is not app.state.engine.pool
            assert source.engine.pool.size() == 4
            async with source.engine.connect() as conn:
                pid = await conn.scalar(text("SELECT pg_backend_pid()"))
            old_pool = source.engine.pool
            assert old_pool.checkedin() == 1
        assert old_pool.checkedin() == 0
        await assert_backend_closed(pid)

    asyncio.run(asyncio.wait_for(scenario(), 20))


def test_four_production_holders_bound_capacity_and_preserve_work_pool(lifecycle_db: None) -> None:
    """@spec PROTECTED-HOOK-SOURCE-2."""
    require_factory()

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-2."""
        app = create_app()
        async with app.router.lifespan_context(app):
            source = app.state.source_gate
            releases = [asyncio.Event() for _ in range(4)]
            ready = [asyncio.Event() for _ in range(4)]

            async def holder(index: int) -> None:
                """@spec PROTECTED-HOOK-SOURCE-2."""
                async with source.hold(uuid.uuid4()):
                    ready[index].set()
                    await releases[index].wait()

            tasks = [asyncio.create_task(holder(index)) for index in range(4)]
            fifth_started, fifth_entered = asyncio.Event(), asyncio.Event()

            async def fifth() -> None:
                """@spec PROTECTED-HOOK-SOURCE-2."""
                fifth_started.set()
                async with source.hold(uuid.uuid4()):
                    fifth_entered.set()

            waiter = None
            try:
                await asyncio.wait_for(asyncio.gather(*(event.wait() for event in ready)), 5)
                waiter = asyncio.create_task(fifth())
                await fifth_started.wait()
                async with app.state.engine.connect() as conn:
                    assert await conn.scalar(text("SELECT 1")) == 1
                assert source.engine.pool.checkedout() == 4
                assert source.engine.pool.overflow() == 0
                assert not fifth_entered.is_set()
                waiter.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await waiter
                releases[0].set()
                await asyncio.wait_for(tasks[0], 5)
                async with source.hold(uuid.uuid4()):
                    pass
            finally:
                for task in tasks + ([waiter] if waiter is not None else []):
                    if not task.done():
                        task.cancel()
                await asyncio.gather(
                    *tasks, *([waiter] if waiter is not None else []), return_exceptions=True
                )

    asyncio.run(asyncio.wait_for(scenario(), 20))


def test_canceled_production_holder_releases_for_successor(lifecycle_db: None) -> None:
    """@spec PROTECTED-HOOK-SOURCE-2."""
    require_factory()

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-2."""
        app = create_app()
        async with app.router.lifespan_context(app):
            source, agent, ready = app.state.source_gate, uuid.uuid4(), asyncio.Event()

            async def holder() -> None:
                """@spec PROTECTED-HOOK-SOURCE-2."""
                async with source.hold(agent):
                    ready.set()
                    await asyncio.Event().wait()

            task = asyncio.create_task(holder())
            try:
                await asyncio.wait_for(ready.wait(), 5)
            finally:
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
            async with source.hold(agent):
                assert source.engine.pool.checkedout() == 1
            assert source.engine.pool.checkedout() == 0

    asyncio.run(asyncio.wait_for(scenario(), 20))


def test_actual_store_startup_failure_has_no_owned_gate_pool_leak(
    lifecycle_db: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    """@spec PROTECTED-HOOK-SOURCE-2."""
    require_factory()
    # Real object-store adapter refuses this invalid bucket, without a stub.
    monkeypatch.setenv("BUNDLE_BUCKET", "invalid/bucket")
    get_settings.cache_clear()

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-2."""
        app = create_app()
        with pytest.raises(ParamValidationError):
            async with app.router.lifespan_context(app):
                pytest.fail("startup accepted an invalid object-store bucket")
        source = getattr(app.state, "source_gate", None)
        if source is not None:
            assert source.engine.pool.checkedout() == 0
            assert source.engine.pool.checkedin() == 0
        # Existing pre-yield resource cleanup is outside this new-pool contract.
        if hasattr(app.state, "http_client"):
            await app.state.http_client.aclose()
        if hasattr(app.state, "engine"):
            await app.state.engine.dispose()

    asyncio.run(asyncio.wait_for(scenario(), 20))


def test_gate_backend_closes_despite_external_http_cleanup_failure(
    lifecycle_db: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    """@spec PROTECTED-HOOK-SOURCE-2."""
    require_factory()

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-2."""
        app = create_app()
        original_close = None
        try:
            with pytest.raises(RuntimeError, match="external cleanup test failure"):
                async with app.router.lifespan_context(app):
                    source = app.state.source_gate
                    async with source.engine.connect() as conn:
                        pid = await conn.scalar(text("SELECT pg_backend_pid()"))
                    old_pool = source.engine.pool
                    original_close = app.state.http_client.aclose

                    async def external_close_failure() -> None:
                        """External HTTP failure only, @spec PROTECTED-HOOK-SOURCE-2."""
                        raise RuntimeError("external cleanup test failure")

                    monkeypatch.setattr(app.state.http_client, "aclose", external_close_failure)
            assert old_pool.checkedin() == 0
            await assert_backend_closed(pid)
        finally:
            if original_close is not None:
                await original_close()
            if hasattr(app.state, "engine"):
                await app.state.engine.dispose()

    asyncio.run(asyncio.wait_for(scenario(), 20))


def test_production_advisory_waiter_does_not_occupy_work_pool(lifecycle_db: None) -> None:
    """@spec PROTECTED-HOOK-SOURCE-2."""
    require_factory()

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-2."""
        app = create_app()
        observer = create_async_engine(get_settings().database_url, poolclass=NullPool)
        try:
            async with app.router.lifespan_context(app):
                source, agent = app.state.source_gate, uuid.uuid4()
                entered = asyncio.Event()

                async def waiter() -> None:
                    """@spec PROTECTED-HOOK-SOURCE-2."""
                    async with source.hold(agent):
                        entered.set()

                async with source.hold(agent):
                    task = asyncio.create_task(waiter())
                    try:
                        async with asyncio.timeout(5):
                            while True:
                                async with observer.connect() as conn:
                                    waiting = await conn.scalar(
                                        text(
                                            "SELECT EXISTS (SELECT 1 FROM pg_stat_activity "
                                            "WHERE datname=current_database() "
                                            "AND wait_event='advisory')"
                                        )
                                    )
                                if waiting:
                                    break
                                await asyncio.sleep(0.01)
                        async with app.state.engine.connect() as conn:
                            assert await conn.scalar(text("SELECT 1")) == 1
                        assert not entered.is_set()
                    except BaseException:
                        task.cancel()
                        await asyncio.gather(task, return_exceptions=True)
                        raise
                await asyncio.wait_for(task, 5)
                assert entered.is_set()
        finally:
            await observer.dispose()

    asyncio.run(asyncio.wait_for(scenario(), 20))
