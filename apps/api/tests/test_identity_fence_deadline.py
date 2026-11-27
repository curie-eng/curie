"""The identity fence cannot hold its lock or the pool forever (#5995).

Real Postgres advisory locks and a real Valkey client throughout. A Valkey
that never answers is a real loopback port that accepts and stays silent. Either the
fence's own deadline or the client's socket timeout, whichever fires first,
must surface as identity_writer_unavailable; the stuck writer's client has no
socket bound, so only the database lock timeout bounds its readers.
"""

from __future__ import annotations

import asyncio
import contextlib
import time
from collections.abc import Iterator
from typing import Any
from urllib.parse import quote

import pytest
from _tcp_blackhole import Blackhole, silent_listener, within
from curie_api import db, identity_fence
from curie_api.config import get_settings
from curie_internal.valkey import AsyncRedis
from identity_writer_lifetime_support import run
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import NullPool

FENCE_LOCK = 2250220


@pytest.fixture
def fence_env(migrated: None, monkeypatch: pytest.MonkeyPatch) -> Iterator[Any]:
    def apply(**values: object) -> None:
        for name, value in values.items():
            monkeypatch.setenv(name, str(value))
        get_settings.cache_clear()

    yield apply
    monkeypatch.undo()
    get_settings.cache_clear()


@pytest.mark.parametrize(
    ("fence_deadline", "socket_timeout"),
    [
        # The fence deadline fires before the client's socket timeout.
        (1, 5),
        # The client's socket timeout fires before the fence deadline.
        (30, 0.5),
    ],
    ids=["deadline_first", "socket_timeout_first"],
)
def test_unanswered_fence_acknowledgement_is_unavailable_and_releases(
    fence_env: Any, fence_deadline: float, socket_timeout: float
) -> None:
    fence_env(IDENTITY_FENCE_ACQUIRE_TIMEOUT_SECONDS=fence_deadline)

    async def check() -> None:
        url = get_settings().database_url
        engine = create_async_engine(url, pool_size=5, max_overflow=0)
        factory = async_sessionmaker(engine)
        observer = create_async_engine(url, poolclass=NullPool)
        async with silent_listener() as listener:
            client = AsyncRedis.from_url(
                f"redis://127.0.0.1:{listener.port}/0", socket_timeout=socket_timeout
            )
            try:
                started = time.monotonic()
                async with within(10, "a fence acknowledgement Valkey never sends"):
                    with pytest.raises(identity_fence.IdentityWriterUnavailable):
                        async with identity_fence.mutation(factory, client):
                            pytest.fail("the fence was acquired without an acknowledgement")
                assert time.monotonic() - started < 4
                assert listener.accepted.is_set()
                async with observer.connect() as probe:
                    taken = await probe.scalar(
                        text("SELECT pg_try_advisory_lock(:key)"), {"key": FENCE_LOCK}
                    )
                    assert taken is True
                    await probe.execute(
                        text("SELECT pg_advisory_unlock(:key)"), {"key": FENCE_LOCK}
                    )
                assert engine.pool.checkedout() == 0  # type: ignore[attr-defined]
            finally:
                await client.aclose()
                await engine.dispose()
                await observer.dispose()

    run(check)


def test_fence_lock_wait_past_the_deadline_is_unavailable(fence_env: Any) -> None:
    """The deadline also covers the advisory lock wait, with no lock timeout set."""
    fence_env(IDENTITY_FENCE_ACQUIRE_TIMEOUT_SECONDS=1)

    async def check() -> None:
        settings = get_settings()
        engine = create_async_engine(settings.database_url, pool_size=5, max_overflow=0)
        factory = async_sessionmaker(engine)
        holder = create_async_engine(settings.database_url, poolclass=NullPool)
        client = AsyncRedis.from_url(settings.valkey_dsn())
        try:
            async with holder.connect() as held:
                await held.execute(text("SELECT pg_advisory_lock(:key)"), {"key": FENCE_LOCK})
                started = time.monotonic()
                async with within(10, "a fence lock wait"):
                    with pytest.raises(identity_fence.IdentityWriterUnavailable):
                        async with identity_fence.mutation(factory, client):
                            pytest.fail("the fence was acquired while another session held it")
                assert time.monotonic() - started < 4
                assert engine.pool.checkedout() == 0  # type: ignore[attr-defined]
                await held.execute(text("SELECT pg_advisory_unlock(:key)"), {"key": FENCE_LOCK})
            async with identity_fence.mutation(factory, client):
                pass
        finally:
            await client.delete(identity_fence.FENCE_KEY)
            await client.aclose()
            await engine.dispose()
            await holder.dispose()

    run(check)


def test_fence_lock_timeout_before_the_deadline_is_unavailable(fence_env: Any) -> None:
    """The server's lock timeout ending the fence lock wait is the same outcome."""
    fence_env(DB_LOCK_TIMEOUT_SECONDS=1, IDENTITY_FENCE_ACQUIRE_TIMEOUT_SECONDS=30)

    async def check() -> None:
        settings = get_settings()
        engine = db.create_engine()
        factory = async_sessionmaker(engine)
        holder = create_async_engine(settings.database_url, poolclass=NullPool)
        client = AsyncRedis.from_url(settings.valkey_dsn(), **settings.valkey_client_options())
        try:
            async with holder.connect() as held:
                await held.execute(text("SELECT pg_advisory_lock(:key)"), {"key": FENCE_LOCK})
                started = time.monotonic()
                async with within(10, "a fence lock wait under a lock timeout"):
                    with pytest.raises(identity_fence.IdentityWriterUnavailable):
                        async with identity_fence.mutation(factory, client):
                            pytest.fail("the fence was acquired while another session held it")
                assert time.monotonic() - started < 4
                assert engine.pool.checkedout() == 0  # type: ignore[attr-defined]
                # The holder still owns the lock it took.
                released = await held.scalar(
                    text("SELECT pg_advisory_unlock(:key)"), {"key": FENCE_LOCK}
                )
                assert released is True
        finally:
            await client.aclose()
            await engine.dispose()
            await holder.dispose()

    run(check)


def test_lost_fence_acknowledgement_is_unavailable_and_the_next_mutation_heals(
    fence_env: Any,
) -> None:
    """Valkey applies the pending write but its reply is lost on the way back."""
    fence_env(IDENTITY_FENCE_ACQUIRE_TIMEOUT_SECONDS=30)

    async def check() -> None:
        settings = get_settings()
        engine = create_async_engine(settings.database_url, pool_size=5, max_overflow=0)
        factory = async_sessionmaker(engine)
        observer = create_async_engine(settings.database_url, poolclass=NullPool)
        healthy = AsyncRedis.from_url(settings.valkey_dsn(), **settings.valkey_client_options())
        key = identity_fence.FENCE_KEY
        try:
            async with Blackhole(settings.valkey_host, settings.valkey_port) as relay:
                password = quote(settings.valkey_password, safe="")
                lossy = AsyncRedis.from_url(
                    f"redis://:{password}@127.0.0.1:{relay.port}/0", socket_timeout=1
                )
                try:
                    # One open connection through the relay, then lose its replies.
                    assert await lossy.ping() is True
                    await healthy.set(key, "stable:before")
                    relay.freeze_replies()
                    started = time.monotonic()
                    async with within(10, "a fence acknowledgement lost in transit"):
                        with pytest.raises(identity_fence.IdentityWriterUnavailable):
                            async with identity_fence.mutation(factory, lossy):
                                pytest.fail("the fence was acquired without an acknowledgement")
                    assert time.monotonic() - started < 4
                finally:
                    await lossy.aclose()
            assert engine.pool.checkedout() == 0  # type: ignore[attr-defined]
            async with observer.connect() as probe:
                taken = await probe.scalar(
                    text("SELECT pg_try_advisory_lock(:key)"), {"key": FENCE_LOCK}
                )
                assert taken is True
                await probe.execute(text("SELECT pg_advisory_unlock(:key)"), {"key": FENCE_LOCK})
            # The write landed although its reply did not.
            assert (await healthy.get(key)).startswith(b"pending:")
            async with identity_fence.mutation(factory, healthy):
                pass
            assert (await healthy.get(key)).startswith(b"stable:")
        finally:
            await healthy.delete(key)
            await healthy.aclose()
            await engine.dispose()
            await observer.dispose()

    run(check)


def test_readers_fail_within_the_bound_while_the_writer_is_stuck(fence_env: Any) -> None:
    """AC2: "With a transaction holding the identity advisory lock and blocked on
    a Valkey call that never answers, identity readers fail within the
    configured bound instead of holding connections forever, and the pool
    recovers."
    """
    fence_env(
        DB_LOCK_TIMEOUT_SECONDS=1,
        DB_POOL_TIMEOUT_SECONDS=10,
        IDENTITY_FENCE_ACQUIRE_TIMEOUT_SECONDS=60,
    )
    # More readers than the 15 connection main pool can serve at once.
    readers = 20

    async def check() -> None:
        url = get_settings().database_url
        writer_engine = create_async_engine(url, pool_size=1, max_overflow=0)
        writer_factory = async_sessionmaker(writer_engine)
        engine = db.create_engine()
        factory = async_sessionmaker(engine, expire_on_commit=False)
        async with silent_listener() as listener:
            client = AsyncRedis.from_url(f"redis://127.0.0.1:{listener.port}/0")

            async def stuck_writer() -> None:
                async with identity_fence.mutation(writer_factory, client):
                    pytest.fail("the stuck writer was acknowledged")

            writer = asyncio.create_task(stuck_writer())

            async def reader() -> tuple[str | None, float]:
                started = time.monotonic()
                async with factory() as session:
                    try:
                        await identity_fence.lock_identity_reader(session)
                    except DBAPIError as error:
                        return getattr(error.orig, "sqlstate", None), time.monotonic() - started
                    return "acquired", time.monotonic() - started

            try:
                # The writer contacts Valkey only after it holds the lock.
                await asyncio.wait_for(listener.accepted.wait(), 10)
                async with within(15, "identity readers behind a stuck writer"):
                    results = await asyncio.gather(*(reader() for _ in range(readers)))
                assert [state for state, _ in results] == ["55P03"] * readers
                assert max(elapsed for _, elapsed in results) < 6
                assert not writer.done()
                assert engine.pool.checkedout() == 0  # type: ignore[attr-defined]
            finally:
                writer.cancel()
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await writer
                await client.aclose()
        try:
            async with within(10, "a reader after the writer is gone"), factory() as session:
                await identity_fence.lock_identity_reader(session)
                assert await session.scalar(text("SELECT 1")) == 1
            assert engine.pool.checkedout() == 0  # type: ignore[attr-defined]
        finally:
            await engine.dispose()
            await writer_engine.dispose()

    run(check)


def test_bounded_mutation_still_rotates_the_fence(fence_env: Any) -> None:
    fence_env(IDENTITY_FENCE_ACQUIRE_TIMEOUT_SECONDS=5)

    async def check() -> None:
        settings = get_settings()
        engine = create_async_engine(settings.database_url)
        client = AsyncRedis.from_url(settings.valkey_dsn(), **settings.valkey_client_options())
        try:
            factory = async_sessionmaker(engine)
            key = identity_fence.FENCE_KEY
            await client.set(key, "stable:before")
            async with identity_fence.mutation(factory, client) as session:
                assert isinstance(session, AsyncSession)
                assert (await client.get(key)).startswith(b"pending:")
            after = await client.get(key)
            assert after.startswith(b"stable:") and after != b"stable:before"
        finally:
            await client.delete(identity_fence.FENCE_KEY)
            await client.aclose()
            await engine.dispose()

    run(check)
