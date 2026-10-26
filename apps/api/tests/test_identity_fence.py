"""Real identity fence lifetime. @spec IDENTITY-WRITER-LIFETIME-V1-1 to -10."""

import asyncio
import uuid

import pytest
import redis.asyncio as redis
from curie_api import identity_fence
from curie_api.config import get_settings
from curie_api.models import Tenant
from identity_writer_lifetime_support import (
    AckClient,
    AckLost,
    ConnectionObserver,
    api,
    blocking,
    run,
    wait_locks,
)
from sqlalchemy import delete
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from .identity_fence_support import identity_fence_backend  # noqa: F401


def test_pending_fence_brackets_mutation_and_rotates_on_error(migrated):
    """@spec IDENTITY-WRITER-LIFETIME-V1-2/5. Known healthy settlement rotates even on refusal."""
    service = api()

    async def check():
        """@spec IDENTITY-WRITER-LIFETIME-V1-2/5."""
        engine = create_async_engine(get_settings().database_url)
        client = redis.from_url(get_settings().valkey_dsn())
        try:
            factory = async_sessionmaker(engine)
            key = identity_fence.FENCE_KEY
            await client.set(key, "stable:before")
            async with service.mutation(factory, client) as session:
                assert isinstance(session, AsyncSession)
                assert (await client.get(key)).startswith(b"pending:")
            after = await client.get(key)
            assert after.startswith(b"stable:") and after != b"stable:before"
            with pytest.raises(RuntimeError, match="intentional"):
                async with service.mutation(factory, client) as session:
                    assert isinstance(session, AsyncSession)
                    assert (await client.get(key)).startswith(b"pending:")
                    raise RuntimeError("intentional")
            assert (await client.get(key)).startswith(b"stable:")
            assert await client.get(key) != after
        finally:
            await client.delete(identity_fence.FENCE_KEY)
            await client.aclose()
            await engine.dispose()

    run(check)


def test_failed_publication_keeps_committed_state_and_pending_fence(migrated):
    """@spec IDENTITY-WRITER-LIFETIME-V1-5/6. Real bound SQL survives a pre-send ACK failure."""
    service = api()

    async def check():
        """@spec IDENTITY-WRITER-LIFETIME-V1-5/6. No ACL/server administration is needed."""
        engine = create_async_engine(get_settings().database_url)
        factory = async_sessionmaker(engine)
        client = redis.from_url(get_settings().valkey_dsn())
        tenant_id = uuid.uuid4()
        try:
            await client.set(identity_fence.FENCE_KEY, "stable:before")
            with pytest.raises(AckLost, match="stable_ack_lost"):
                async with service.mutation(factory, AckClient(client, "stable_before")) as session:
                    assert isinstance(session, AsyncSession)
                    session.add(Tenant(id=tenant_id, deployment_id=f"synthetic-fence-{tenant_id}"))
                    await session.commit()
            assert (await client.get(identity_fence.FENCE_KEY)).startswith(b"pending:")
            async with factory() as observer:
                await identity_fence.lock_identity_reader(observer)
                assert await observer.get(Tenant, tenant_id) is not None
        finally:
            async with factory.begin() as cleanup:
                await cleanup.execute(delete(Tenant).where(Tenant.id == tenant_id))
            await client.delete(identity_fence.FENCE_KEY)
            await client.aclose()
            await engine.dispose()

    run(check)


def test_concurrent_identity_writers_are_serialized(migrated, monkeypatch):
    """@spec IDENTITY-WRITER-LIFETIME-V1-3. Observe a real blocking edge before releasing W1."""
    service = api()

    async def check():
        """@spec IDENTITY-WRITER-LIFETIME-V1-3."""
        engine = create_async_engine(get_settings().database_url)
        client = redis.from_url(get_settings().valkey_dsn())
        entered, release = asyncio.Event(), asyncio.Event()
        order = []
        try:
            factory = async_sessionmaker(engine)

            async def first():
                """@spec IDENTITY-WRITER-LIFETIME-V1-3."""
                async with service.mutation(factory, client):
                    order.append("first")
                    entered.set()
                    await release.wait()

            async def second():
                """@spec IDENTITY-WRITER-LIFETIME-V1-3."""
                await entered.wait()
                async with service.mutation(factory, client):
                    order.append("second")

            with monkeypatch.context() as observed:
                with ConnectionObserver(engine, observed) as connections:
                    one, two = asyncio.create_task(first()), asyncio.create_task(second())
                    try:
                        await entered.wait()
                        await wait_locks(connections, 2)
                        holder = connections.lock_connections[
                            0
                        ].connection.driver_connection.get_server_pid()
                        waiter = connections.lock_connections[
                            1
                        ].connection.driver_connection.get_server_pid()
                        async with factory() as observer:
                            await blocking(observer, waiter, holder)
                        assert order == ["first"]
                        release.set()
                        await asyncio.wait_for(asyncio.gather(one, two), timeout=5)
                        assert order == ["first", "second"]
                    finally:
                        release.set()
                        for task in (one, two):
                            if not task.done():
                                task.cancel()
                                await asyncio.gather(task, return_exceptions=True)
        finally:
            await client.delete(identity_fence.FENCE_KEY)
            await client.aclose()
            await engine.dispose()

    run(check)


def test_bundle_upload_keeps_its_own_size_and_time_contract():
    """@spec IDENTITY-WRITER-LIFETIME-V1-1. Existing large-upload exemptions are unchanged."""
    assert not identity_fence.protects("PUT", "/agents/example/versions/example/bundle")
    assert not identity_fence.protects("POST", "/agents/example/versions")
    assert identity_fence.protects("PUT", "/agents/example/access-policy")
    assert identity_fence.protects("PUT", "/agents/example/channels/callers")
