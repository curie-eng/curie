"""Renewable Valkey leases for stream-consumer process liveness.

Stream consumer idle time is not process liveness: it grows while a worker is
draining a turn or waiting for local capacity.  This module deliberately keeps
the independent liveness signal outside :class:`StreamBroker`.  The broker port
remains stream-only; the one Redis implementation used by the worker gets this
small string-key adapter alongside it.
"""

from __future__ import annotations

import asyncio
import uuid

from redis.asyncio import Redis


def consumer_heartbeat_key(stream: str, group: str, consumer: str) -> str:
    """The renewable alive lease for one stream consumer."""

    return f"{stream}:consumer-heartbeat:{group}:{consumer}"


def consumer_heartbeat_capable_key(stream: str, group: str, consumer: str) -> str:
    """The compatibility marker proving a consumer publishes alive leases."""

    return f"{stream}:consumer-heartbeat-capable:{group}:{consumer}"


def consumer_reclaim_lock_key(stream: str, group: str, consumer: str) -> str:
    """The short arbitration lease for transferring one dead peer's PEL."""

    return f"{stream}:consumer-reclaim-lock:{group}:{consumer}"


class ThreadLockOwnerLiveness:
    """Runs-group heartbeat adapter for :class:`~curie_worker.threadlock.ThreadLock`.

    The kernel's per-thread lock is process-wide. Stamping it with the runs
    consumer name and checking that consumer's alive lease is enough: eval
    and runs share a process, so a crash drops both heartbeats together.
    """

    def __init__(self, store: ConsumerLivenessStore, *, stream: str, group: str) -> None:
        self._store = store
        self._stream = stream
        self._group = group

    async def is_alive(self, owner: str) -> bool:
        return await self._store.is_alive(stream=self._stream, group=self._group, consumer=owner)

    async def is_capable(self, owner: str) -> bool:
        return await self._store.is_capable(stream=self._stream, group=self._group, consumer=owner)


class ConsumerLivenessStore:
    """The narrow Redis string-key surface used by consumer liveness.

    Publication and renewal are ordered transactions: the alive lease is
    written before the capability marker.  Therefore observing capability can
    never race ahead of the first alive lease.  The capability marker is not
    deleted at shutdown; its longer TTL lets a replacement distinguish a dead
    capable process from a pre-marker worker during a rolling upgrade.
    """

    def __init__(self, redis: Redis) -> None:
        self._redis = redis

    async def publish(
        self,
        *,
        stream: str,
        group: str,
        consumer: str,
        heartbeat_ttl_ms: int,
        capability_ttl_ms: int,
    ) -> None:
        """Publish alive first, then capability, in one ordered transaction."""

        async with self._redis.pipeline(transaction=True) as pipe:
            pipe.set(
                consumer_heartbeat_key(stream, group, consumer),
                "alive",
                px=heartbeat_ttl_ms,
            )
            pipe.set(
                consumer_heartbeat_capable_key(stream, group, consumer),
                "1",
                px=capability_ttl_ms,
            )
            await pipe.execute()

    async def renew(
        self,
        *,
        stream: str,
        group: str,
        consumer: str,
        heartbeat_ttl_ms: int,
        capability_ttl_ms: int,
    ) -> None:
        """Renew both markers together, preserving their publication order."""

        await self.publish(
            stream=stream,
            group=group,
            consumer=consumer,
            heartbeat_ttl_ms=heartbeat_ttl_ms,
            capability_ttl_ms=capability_ttl_ms,
        )

    async def is_alive(self, *, stream: str, group: str, consumer: str) -> bool:
        return bool(
            await self._redis.exists(consumer_heartbeat_key(stream, group, consumer))
        )

    async def is_capable(self, *, stream: str, group: str, consumer: str) -> bool:
        return bool(
            await self._redis.exists(
                consumer_heartbeat_capable_key(stream, group, consumer)
            )
        )

    async def cleanup_alive(
        self,
        *,
        stream: str,
        group: str,
        consumer: str,
        timeout_s: float,
    ) -> None:
        """Best-effort bounded removal of the short alive lease only."""

        async with asyncio.timeout(timeout_s):
            await self._redis.delete(consumer_heartbeat_key(stream, group, consumer))

    async def try_acquire_reclaim(
        self,
        *,
        stream: str,
        group: str,
        consumer: str,
        ttl_ms: int,
    ) -> str | None:
        """Acquire one dead peer's prompt-reclaim lease, returning its token."""

        token = uuid.uuid4().hex
        acquired = await self._redis.set(
            consumer_reclaim_lock_key(stream, group, consumer),
            token,
            nx=True,
            px=ttl_ms,
        )
        return token if acquired else None

    async def release_reclaim(
        self,
        *,
        stream: str,
        group: str,
        consumer: str,
        token: str,
    ) -> None:
        """Release an arbitration lease only when this caller still owns it."""

        await self._redis.eval(
            """
            if redis.call('GET', KEYS[1]) == ARGV[1] then
              return redis.call('DEL', KEYS[1])
            end
            return 0
            """,
            1,
            consumer_reclaim_lock_key(stream, group, consumer),
            token,
        )
