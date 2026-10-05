"""Actual startup deadline/cancellation, @spec PROTECTED-HOOK-SOURCE-2."""

from __future__ import annotations

import asyncio
import time
from typing import Any

import pytest
from _migration_support import IsolatedMigrationDb
from curie_api.config import get_settings
from curie_api.schema_compat import assert_servable
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine


@pytest.fixture
def lifecycle_db(isolated_migration_db: IsolatedMigrationDb) -> None:
    """@spec PROTECTED-HOOK-SOURCE-2."""
    isolated_migration_db.at("head")


async def waiting_backend(observer: Any, task: asyncio.Task[Any]) -> int:
    """@spec PROTECTED-HOOK-SOURCE-2."""
    async with asyncio.timeout(4):
        while True:
            assert not task.done(), "SOURCE-2 startup failed to probe required locked source table"
            async with observer.connect() as connection:
                pid = await connection.scalar(
                    text(
                        "SELECT l.pid FROM pg_locks l JOIN pg_stat_activity a ON a.pid=l.pid "
                        "WHERE a.datname=current_database() AND l.locktype='relation' "
                        "AND l.relation='curie.hook_source_operations'::regclass "
                        "AND NOT l.granted AND a.wait_event='relation'"
                    )
                )
            if pid is not None:
                return int(pid)
            await asyncio.sleep(0.01)
    raise AssertionError("unreachable")


async def require_backend_gone(observer: Any, pid: int) -> None:
    """@spec PROTECTED-HOOK-SOURCE-2."""
    async with asyncio.timeout(5):
        while True:
            async with observer.connect() as connection:
                present = await connection.scalar(
                    text("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE pid=:pid)"),
                    {"pid": pid},
                )
            if not present:
                return
            await asyncio.sleep(0.01)


@pytest.mark.parametrize("outcome", ["deadline", "cancel"])
def test_real_required_table_wait_obeys_deadline_or_cancellation_and_disposes_backend(
    lifecycle_db: None, outcome: str
) -> None:
    """@spec PROTECTED-HOOK-SOURCE-2."""

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-2."""
        blocker = create_async_engine(get_settings().database_url, pool_size=1, max_overflow=0)
        observer = create_async_engine(get_settings().database_url, pool_size=1, max_overflow=0)
        task = None
        try:
            async with blocker.connect() as connection:
                async with connection.begin():
                    await connection.execute(
                        text("LOCK TABLE curie.hook_source_operations IN ACCESS EXCLUSIVE MODE")
                    )
                    started = time.monotonic()
                    task = asyncio.create_task(assert_servable())
                    pid = await waiting_backend(observer, task)
                    if outcome == "cancel":
                        task.cancel()
                        with pytest.raises(asyncio.CancelledError):
                            async with asyncio.timeout(6):
                                await task
                    else:
                        with pytest.raises(RuntimeError, match="schema_probe_timeout"):
                            async with asyncio.timeout(36):
                                await task
                        assert 29 <= time.monotonic() - started <= 36
                    await require_backend_gone(observer, pid)
                async with observer.connect() as observed:
                    assert (
                        await observed.scalar(text("SELECT version_num FROM curie.alembic_version"))
                        == "0079"
                    )
                    assert (
                        await observed.scalar(
                            text("SELECT count(*) FROM curie.hook_source_operations")
                        )
                        == 0
                    )
        finally:
            if task is not None:
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
            await blocker.dispose()
            await observer.dispose()

    asyncio.run(asyncio.wait_for(scenario(), 45))
