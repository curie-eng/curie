"""Per-thread lock steal from a dead owner (#2500).

Against real Valkey, never a mock. A force-killed worker leaves ``SET NX PX``
held until TTL; the replacement must CAS-steal once the named owner's consumer
heartbeat is gone, and must not steal from a live owner or an opaque legacy
value (#849).
"""

from __future__ import annotations

import asyncio
import contextlib
import time
import uuid
from collections.abc import AsyncIterator

import pytest
from curie_test_support.valkey import (
    VALKEY_HOST as _VALKEY_HOST,
)
from curie_test_support.valkey import (
    VALKEY_PORT as _VALKEY_PORT,
)
from curie_test_support.valkey import (
    VALKEY_PW as _VALKEY_PW,
)
from curie_worker.consumer_liveness import ConsumerLivenessStore
from curie_worker.threadlock import LockAcquireTimeout, ThreadLock
from redis.asyncio import Redis as AsyncRedis

_TTL_MS = 60_000
_ACQUIRE_S = 0.4
_POLL_S = 0.02
_PROOF_S = 0.05


class _OwnerLiveness:
    """Adapter matching ThreadLock's owner-liveness protocol on real Valkey."""

    def __init__(self, store: ConsumerLivenessStore, *, stream: str, group: str) -> None:
        self._store = store
        self._stream = stream
        self._group = group

    async def is_alive(self, owner: str) -> bool:
        return await self._store.is_alive(stream=self._stream, group=self._group, consumer=owner)

    async def is_capable(self, owner: str) -> bool:
        return await self._store.is_capable(stream=self._stream, group=self._group, consumer=owner)


@contextlib.asynccontextmanager
async def _client() -> AsyncIterator[AsyncRedis]:
    redis: AsyncRedis = AsyncRedis(
        host=_VALKEY_HOST,
        port=_VALKEY_PORT,
        password=_VALKEY_PW or None,
        decode_responses=True,
    )
    try:
        yield redis
    finally:
        with contextlib.suppress(Exception):
            await redis.aclose()


def _lock(
    redis: AsyncRedis,
    *,
    owner: str,
    liveness: _OwnerLiveness | None,
    acquire_s: float = _ACQUIRE_S,
    ttl_ms: int = _TTL_MS,
    proof_s: float = _PROOF_S,
) -> ThreadLock:
    return ThreadLock(
        redis,
        ttl_ms=ttl_ms,
        acquire_timeout_s=acquire_s,
        poll_interval_s=_POLL_S,
        owner=owner,
        owner_liveness=liveness,
        dead_owner_proof_s=proof_s,
    )


async def _expire_alive(
    redis: AsyncRedis, store: ConsumerLivenessStore, *, stream: str, group: str, owner: str
) -> None:
    await store.publish(
        stream=stream,
        group=group,
        consumer=owner,
        heartbeat_ttl_ms=1,
        capability_ttl_ms=5_000,
    )
    key = f"{stream}:consumer-heartbeat:{group}:{owner}"
    deadline = time.monotonic() + 2.0
    while await redis.exists(key):
        if time.monotonic() > deadline:
            raise AssertionError(f"alive lease for {owner} never expired")
        await asyncio.sleep(0.005)


def test_dead_capable_owner_lock_is_stolen_without_waiting_ttl(names: dict[str, str]) -> None:
    """#2500 positive: replacement acquires a crashed owner's lock in << TTL."""

    async def go() -> None:
        async with _client() as redis:
            key = f"{names['prefix']}:lock:slack:C0LOCALDEV:2500-dead"
            stream, group = names["stream"], names["group"]
            store = ConsumerLivenessStore(redis)
            liveness = _OwnerLiveness(store, stream=stream, group=group)
            dead = _lock(redis, owner="dead-pod", liveness=None)
            await dead.acquire(key)
            await _expire_alive(redis, store, stream=stream, group=group, owner="dead-pod")

            replacement = _lock(redis, owner="replacement-pod", liveness=liveness)
            started = time.monotonic()
            token = await replacement.acquire(key)
            elapsed = time.monotonic() - started
            assert elapsed < 2.0, (
                f"replacement waited {elapsed:.3f}s for a dead owner's lock; "
                "steal did not happen and the TTL is still the recovery path"
            )
            assert elapsed < (_TTL_MS / 1000) / 10
            held = await redis.get(key)
            assert held == token
            assert str(held).startswith("replacement-pod")
            await replacement.release(key, token)

    asyncio.run(go())


def test_live_capable_owner_lock_is_not_stolen(names: dict[str, str]) -> None:
    """#2500 negative: a live worker's lock still serializes."""

    async def go() -> None:
        async with _client() as redis:
            key = f"{names['prefix']}:lock:slack:C0LOCALDEV:2500-live"
            stream, group = names["stream"], names["group"]
            store = ConsumerLivenessStore(redis)
            liveness = _OwnerLiveness(store, stream=stream, group=group)
            await store.publish(
                stream=stream,
                group=group,
                consumer="live-pod",
                heartbeat_ttl_ms=5_000,
                capability_ttl_ms=5_000,
            )
            live = _lock(redis, owner="live-pod", liveness=None)
            live_token = await live.acquire(key)

            waiter = _lock(redis, owner="replacement-pod", liveness=liveness, acquire_s=0.35)
            with pytest.raises(LockAcquireTimeout):
                await waiter.acquire(key)
            assert await redis.get(key) == live_token
            await live.release(key, live_token)

    asyncio.run(go())


def test_opaque_legacy_lock_value_is_not_stolen(names: dict[str, str]) -> None:
    """#849 sibling: UUID-only / opaque values stay on TTL, never guessed dead."""

    async def go() -> None:
        async with _client() as redis:
            key = f"{names['prefix']}:lock:slack:C0LOCALDEV:2500-opaque"
            stream, group = names["stream"], names["group"]
            store = ConsumerLivenessStore(redis)
            liveness = _OwnerLiveness(store, stream=stream, group=group)
            assert await redis.set(key, "another-worker", nx=True, px=_TTL_MS)
            waiter = _lock(redis, owner="replacement-pod", liveness=liveness, acquire_s=0.35)
            with pytest.raises(LockAcquireTimeout):
                await waiter.acquire(key)
            assert await redis.get(key) == "another-worker"

    asyncio.run(go())


def test_uuid_hex_production_token_is_not_stolen(names: dict[str, str]) -> None:
    """Mixed-version: today's uuid.hex tokens have no owner separator."""

    async def go() -> None:
        async with _client() as redis:
            key = f"{names['prefix']}:lock:slack:C0LOCALDEV:2500-uuid"
            stream, group = names["stream"], names["group"]
            store = ConsumerLivenessStore(redis)
            liveness = _OwnerLiveness(store, stream=stream, group=group)
            legacy = uuid.uuid4().hex
            assert await redis.set(key, legacy, nx=True, px=_TTL_MS)
            waiter = _lock(redis, owner="replacement-pod", liveness=liveness, acquire_s=0.35)
            with pytest.raises(LockAcquireTimeout):
                await waiter.acquire(key)
            assert await redis.get(key) == legacy

    asyncio.run(go())


def test_same_name_leftover_is_stolen_while_replacement_heartbeat_is_live(
    names: dict[str, str],
) -> None:
    """#2500 replicas:1 kubelet restart: hostname-pid is unchanged, heartbeat is already up."""

    async def go() -> None:
        async with _client() as redis:
            key = f"{names['prefix']}:lock:slack:C0LOCALDEV:2500-same"
            stream, group = names["stream"], names["group"]
            store = ConsumerLivenessStore(redis)
            liveness = _OwnerLiveness(store, stream=stream, group=group)
            crashed = _lock(redis, owner="stable-pod", liveness=None)
            await crashed.acquire(key)
            await store.publish(
                stream=stream,
                group=group,
                consumer="stable-pod",
                heartbeat_ttl_ms=5_000,
                capability_ttl_ms=5_000,
            )
            replacement = _lock(redis, owner="stable-pod", liveness=liveness)
            started = time.monotonic()
            token = await replacement.acquire(key)
            elapsed = time.monotonic() - started
            assert elapsed < 2.0, (
                f"same-name leftover waited {elapsed:.3f}s; hold-set steal did not fire"
            )
            assert str(await redis.get(key)) == token
            await replacement.release(key, token)

    asyncio.run(go())


def test_same_instance_sibling_task_does_not_steal_a_live_hold(names: dict[str, str]) -> None:
    """release_thread / reap vs claim: two tasks, one ThreadLock, must still serialize."""

    async def go() -> None:
        async with _client() as redis:
            key = f"{names['prefix']}:lock:slack:C0LOCALDEV:2500-sibling"
            stream, group = names["stream"], names["group"]
            store = ConsumerLivenessStore(redis)
            liveness = _OwnerLiveness(store, stream=stream, group=group)
            await store.publish(
                stream=stream,
                group=group,
                consumer="same-pod",
                heartbeat_ttl_ms=5_000,
                capability_ttl_ms=5_000,
            )
            lock = _lock(redis, owner="same-pod", liveness=liveness, acquire_s=0.4)
            first = await lock.acquire(key)

            async def waiter() -> str:
                return await lock.acquire(key)

            waiting = asyncio.create_task(waiter())
            await asyncio.sleep(0.25)
            assert not waiting.done(), "sibling task stole a live same-instance hold"
            assert await redis.get(key) == first
            waiting.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await waiting
            await lock.release(key, first)

    asyncio.run(go())
