"""A committed identity update can never retain an old stable cache generation."""

import asyncio
import uuid
from urllib.parse import urlsplit, urlunsplit

import pytest
import redis.asyncio as redis
from curie_api import identity_fence
from curie_api.config import get_settings
from curie_api.models import Tenant
from redis.exceptions import NoPermissionError
from sqlalchemy import delete
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from .identity_fence_support import identity_fence_backend  # noqa: F401


def test_pending_fence_brackets_mutation_and_rotates_on_error(migrated):
    async def check():
        engine = create_async_engine(get_settings().database_url)
        client = redis.from_url(get_settings().valkey_dsn())
        try:
            factory = async_sessionmaker(engine)
            key = identity_fence.FENCE_KEY
            await client.set(key, "stable:before")
            async with identity_fence.mutation(factory, client):
                assert (await client.get(key)).startswith(b"pending:")
            after = await client.get(key)
            assert after.startswith(b"stable:") and after != b"stable:before"
            with pytest.raises(RuntimeError, match="intentional"):
                async with identity_fence.mutation(factory, client):
                    assert (await client.get(key)).startswith(b"pending:")
                    raise RuntimeError("intentional")
            assert (await client.get(key)).startswith(b"stable:")
            assert await client.get(key) != after
        finally:
            await client.delete(identity_fence.FENCE_KEY)
            await client.aclose()
            await engine.dispose()

    asyncio.run(check())


def test_failed_publication_keeps_committed_state_and_pending_fence(migrated):
    async def check():
        engine = create_async_engine(get_settings().database_url)
        factory = async_sessionmaker(engine)
        owner = redis.from_url(get_settings().valkey_dsn())
        username, password = f"j6-fence-{uuid.uuid4().hex}", uuid.uuid4().hex
        parts = urlsplit(get_settings().valkey_dsn())
        endpoint = urlunsplit(
            parts._replace(netloc=f"{username}:{password}@{parts.netloc.rsplit('@', 1)[-1]}")
        )
        restricted = redis.from_url(endpoint)
        tenant_id = uuid.uuid4()
        created = False
        try:
            assert await owner.execute_command("ACL", "GETUSER", username) is None
            # Real server-side command denial, using the documented ACL rules:
            # https://valkey.io/commands/acl-setuser/. SET succeeds; EVAL cannot.
            await owner.execute_command(
                "ACL",
                "SETUSER",
                username,
                "reset",
                "on",
                f">{password}",
                f"~{identity_fence.FENCE_KEY}",
                "+select",
                "+get",
                "+set",
                "+ping",
                "+hello",
                "+client|setinfo",
            )
            created = True
            await restricted.ping()
            await owner.set(identity_fence.FENCE_KEY, "stable:before")
            with pytest.raises(NoPermissionError, match="eval"):
                async with identity_fence.mutation(factory, restricted):
                    async with factory.begin() as session:
                        session.add(Tenant(id=tenant_id, deployment_id="acme-fence-publication"))
            assert (await owner.get(identity_fence.FENCE_KEY)).startswith(b"pending:")
            async with factory() as session:
                await identity_fence.lock_identity_reader(session)
                assert await session.get(Tenant, tenant_id) is not None
        finally:
            await restricted.aclose()
            if created:
                await owner.execute_command("ACL", "DELUSER", username)
            async with factory.begin() as session:
                await session.execute(delete(Tenant).where(Tenant.id == tenant_id))
            await owner.delete(identity_fence.FENCE_KEY)
            await owner.aclose()
            await engine.dispose()

    asyncio.run(check())


def test_concurrent_identity_writers_are_serialized(migrated):
    async def check():
        engine = create_async_engine(get_settings().database_url)
        client = redis.from_url(get_settings().valkey_dsn())
        entered, release = asyncio.Event(), asyncio.Event()
        order = []
        try:
            factory = async_sessionmaker(engine)

            async def first():
                async with identity_fence.mutation(factory, client):
                    order.append("first")
                    entered.set()
                    await release.wait()

            async def second():
                await entered.wait()
                async with identity_fence.mutation(factory, client):
                    order.append("second")

            one, two = asyncio.create_task(first()), asyncio.create_task(second())
            await entered.wait()
            await asyncio.sleep(0.03)
            assert order == ["first"]
            release.set()
            await asyncio.wait_for(asyncio.gather(one, two), timeout=5)
            assert order == ["first", "second"]
        finally:
            release.set()
            await client.delete(identity_fence.FENCE_KEY)
            await client.aclose()
            await engine.dispose()

    asyncio.run(check())


def test_bundle_upload_keeps_its_own_size_and_time_contract():
    assert not identity_fence.protects("PUT", "/agents/example/versions/example/bundle")
    assert not identity_fence.protects("POST", "/agents/example/versions")
    assert identity_fence.protects("PUT", "/agents/example/access-policy")
    assert identity_fence.protects("PUT", "/agents/example/channels/callers")
