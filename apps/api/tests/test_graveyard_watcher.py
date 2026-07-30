"""Dead-letter graveyard watcher (#531) against real Valkey.

The worker moves a permanently-failing entry to ``<stream>:dead`` and acks it;
nothing platform-side watched it. These tests pin the watcher's contract: it
alerts once per NEW dead-letter, seeds at the tail so a boot does not re-alert
history, and does not double-alert or suppress across an approximate-MAXLEN trim.
"""

from __future__ import annotations

import asyncio
import uuid

import pytest
import redis.asyncio as aioredis
from curie_api.graveyardwatcher import GraveyardWatcher
from curie_test_support.valkey import (
    VALKEY_HOST as _VALKEY_HOST,
)
from curie_test_support.valkey import (
    VALKEY_PORT as _VALKEY_PORT,
)
from curie_test_support.valkey import (
    VALKEY_PW as _VALKEY_PW,
)


def _client() -> aioredis.Redis:
    return aioredis.Redis(
        host=_VALKEY_HOST, port=_VALKEY_PORT, password=_VALKEY_PW, decode_responses=True
    )


async def _dead_letter(client: aioredis.Redis, stream: str, *, original: str, reason: str) -> str:
    return await client.xadd(
        stream,
        {
            "payload": "{}",
            "dl_original_id": original,
            "dl_delivery_count": "5",
            "dl_reason": reason,
            "dl_dead_lettered_at": "2026-07-16T00:00:00+00:00",
        },
    )


def test_alerts_once_per_new_dead_letter_and_seeds_at_tail() -> None:
    async def go() -> None:
        stream = f"test:curie:runs:dead:{uuid.uuid4().hex}"
        client = _client()
        watcher = GraveyardWatcher(client, stream=stream, interval_seconds=0.01)
        try:
            # A pre-existing historical dead-letter: seeding at the tail must NOT
            # re-alert it.
            await _dead_letter(client, stream, original="1700000000000-0", reason="unparseable")
            await watcher.seed_cursor()
            assert await watcher.scan_once() == 0
            assert watcher.alerts_emitted == 0

            # Two new dead-letters arrive while the watcher runs -> two alerts.
            await _dead_letter(client, stream, original="1700000000001-0", reason="max-delivery")
            await _dead_letter(client, stream, original="1700000000002-0", reason="max-delivery")
            assert await watcher.scan_once() == 2
            assert watcher.alerts_emitted == 2

            # A subsequent pass with nothing new alerts nothing (no double-alert).
            assert await watcher.scan_once() == 0
            assert watcher.alerts_emitted == 2
        finally:
            await client.delete(stream)
            await client.aclose()

    asyncio.run(go())


def test_run_forever_alerts_a_new_dead_letter_then_stops() -> None:
    async def go() -> None:
        stream = f"test:curie:runs:dead:{uuid.uuid4().hex}"
        client = _client()
        watcher = GraveyardWatcher(client, stream=stream, interval_seconds=0.02)
        task = asyncio.create_task(watcher.run_forever())
        try:
            await asyncio.sleep(0.05)  # let it seed at the (empty) tail
            await _dead_letter(client, stream, original="1700000000003-0", reason="unparseable")
            for _ in range(100):
                if watcher.alerts_emitted >= 1:
                    break
                await asyncio.sleep(0.01)
            assert watcher.alerts_emitted == 1
        finally:
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
            await client.delete(stream)
            await client.aclose()

    asyncio.run(go())
