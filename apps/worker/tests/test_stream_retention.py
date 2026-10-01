"""Settled-entry stream retention (ADR 0184 decisions 1 to 3, #1523).

Against real Valkey, never a mock. The property under test is Valkey
semantics: what ``XINFO GROUPS`` and ``XPENDING`` report, and what
``XTRIM MINID`` removes. A mocked script would only assert the Lua we wrote.

Each test builds its own stream and groups under a per-test unique name
(``names`` in ``tests/conftest.py``), so this file shares no fixed key with any
other test file.

The first test is also the mutation guard the ADR names: a floor taken from
``last-delivered-id`` instead of the oldest pending id trims a pending entry, and
the test fails on the pending entry's missing body.
"""

from __future__ import annotations

import asyncio
import contextlib
import time
from collections.abc import AsyncIterator

import pytest
import redis
from curie_test_support.valkey import VALKEY_HOST, VALKEY_PORT, VALKEY_PW
from curie_worker.config import WorkerConfig
from curie_worker.stream_retention import (
    StreamRetention,
    build_stream_retention,
    trim_settled,
)
from pydantic import ValidationError
from redis.asyncio import Redis as AsyncRedis


@contextlib.asynccontextmanager
async def _client(*, decode_responses: bool = True) -> AsyncIterator[AsyncRedis]:
    client = AsyncRedis(
        host=VALKEY_HOST,
        port=VALKEY_PORT,
        password=VALKEY_PW or None,
        decode_responses=decode_responses,
    )
    try:
        yield client
    finally:
        with contextlib.suppress(Exception):
            await client.aclose()


def _fill(sync_redis: redis.Redis, stream: str, count: int) -> list[str]:
    return [str(sync_redis.xadd(stream, {"payload": f"turn-{i}"})) for i in range(count)]


def _deliver(sync_redis: redis.Redis, stream: str, group: str, count: int) -> list[str]:
    rows = sync_redis.xreadgroup(group, "c1", {stream: ">"}, count=count)
    return [entry_id for _stream, entries in rows for entry_id, _fields in entries]


def _body(sync_redis: redis.Redis, stream: str, entry_id: str) -> dict[str, str] | None:
    rows = sync_redis.xrange(stream, min=entry_id, max=entry_id)
    return dict(rows[0][1]) if rows else None


def _trim(stream: str, *, min_age_s: int = 0, decode: bool = True) -> int:
    async def go() -> int:
        async with _client(decode_responses=decode) as client:
            return await trim_settled(client, stream, min_age_s=min_age_s)

    return asyncio.run(go())


def test_trims_settled_entries_and_keeps_every_pending_one(
    sync_redis: redis.Redis, names: dict[str, str]
) -> None:
    stream, group = names["stream"], names["group"]
    sync_redis.xgroup_create(stream, group, id="0", mkstream=True)
    ids = _fill(sync_redis, stream, 10)
    assert _deliver(sync_redis, stream, group, 10) == ids
    sync_redis.xack(stream, group, *ids[:8])

    trimmed = _trim(stream)

    assert trimmed == 8
    assert sync_redis.xlen(stream) == 2
    # Both pending entries keep their bodies, so a reclaim can still run them.
    assert _body(sync_redis, stream, ids[8]) == {"payload": "turn-8"}
    assert _body(sync_redis, stream, ids[9]) == {"payload": "turn-9"}
    pending = sync_redis.xpending_range(stream, group, min="-", max="+", count=10)
    assert [row["message_id"] for row in pending] == ids[8:]


def test_keeps_entries_no_group_has_read(
    sync_redis: redis.Redis, names: dict[str, str]
) -> None:
    stream, group = names["stream"], names["group"]
    sync_redis.xgroup_create(stream, group, id="0", mkstream=True)
    ids = _fill(sync_redis, stream, 10)
    delivered = _deliver(sync_redis, stream, group, 6)
    sync_redis.xack(stream, group, *delivered)

    assert _trim(stream) == 6

    assert sync_redis.xlen(stream) == 4
    assert _deliver(sync_redis, stream, group, 10) == ids[6:]


def test_the_slowest_group_sets_the_floor(
    sync_redis: redis.Redis, names: dict[str, str]
) -> None:
    stream = names["stream"]
    fast, slow = f"{names['group']}-fast", f"{names['group']}-slow"
    sync_redis.xgroup_create(stream, fast, id="0", mkstream=True)
    sync_redis.xgroup_create(stream, slow, id="0")
    ids = _fill(sync_redis, stream, 10)
    sync_redis.xack(stream, fast, *_deliver(sync_redis, stream, fast, 10))
    sync_redis.xack(stream, slow, *_deliver(sync_redis, stream, slow, 3))

    assert _trim(stream) == 3

    assert sync_redis.xlen(stream) == 7
    assert _deliver(sync_redis, stream, slow, 10) == ids[3:]


def test_entries_younger_than_min_age_survive(
    sync_redis: redis.Redis, names: dict[str, str]
) -> None:
    stream, group = names["stream"], names["group"]
    sync_redis.xgroup_create(stream, group, id="0", mkstream=True)
    two_hours_ago_ms = int(time.time() * 1000) - 2 * 3600 * 1000
    old = [
        str(sync_redis.xadd(stream, {"payload": f"old-{i}"}, id=f"{two_hours_ago_ms}-{i}"))
        for i in range(4)
    ]
    recent = _fill(sync_redis, stream, 4)
    sync_redis.xack(stream, group, *_deliver(sync_redis, stream, group, 8))

    # Every entry is settled, and only the two-hour-old ones are past the window.
    assert _trim(stream, min_age_s=3600) == len(old)

    assert [entry_id for entry_id, _ in sync_redis.xrange(stream)] == recent
    # A window longer than every entry's age trims nothing more.
    assert _trim(stream, min_age_s=86400) == 0
    assert sync_redis.xlen(stream) == 4


def test_a_stream_without_a_group_is_left_alone(
    sync_redis: redis.Redis, names: dict[str, str]
) -> None:
    stream = names["stream"]
    _fill(sync_redis, stream, 5)

    assert _trim(stream) == 0
    assert sync_redis.xlen(stream) == 5


def test_a_missing_stream_is_a_no_op(names: dict[str, str]) -> None:
    assert _trim(names["stream"]) == 0


def test_a_bytes_client_trims_the_same(
    sync_redis: redis.Redis, names: dict[str, str]
) -> None:
    # The worker's production clients are built without ``decode_responses``.
    stream, group = names["stream"], names["group"]
    sync_redis.xgroup_create(stream, group, id="0", mkstream=True)
    ids = _fill(sync_redis, stream, 3)
    _deliver(sync_redis, stream, group, 3)
    sync_redis.xack(stream, group, *ids[:2])

    assert _trim(stream, decode=False) == 2
    assert sync_redis.xlen(stream) == 1


def test_one_failing_stream_does_not_stop_the_others(
    sync_redis: redis.Redis, names: dict[str, str]
) -> None:
    stream, group = names["stream"], names["group"]
    broken = f"{names['prefix']}:not-a-stream"
    sync_redis.set(broken, "x")
    sync_redis.xgroup_create(stream, group, id="0", mkstream=True)
    ids = _fill(sync_redis, stream, 4)
    _deliver(sync_redis, stream, group, 4)
    sync_redis.xack(stream, group, *ids)

    async def go() -> dict[str, int]:
        async with _client() as client:
            retention = StreamRetention(client, [broken, stream], min_age_s=0, interval_s=60)
            return await retention.trim_once()

    assert asyncio.run(go()) == {stream: 4}

    assert sync_redis.xlen(stream) == 0


def test_run_forever_trims_until_shutdown(
    sync_redis: redis.Redis, names: dict[str, str]
) -> None:
    stream, group = names["stream"], names["group"]
    sync_redis.xgroup_create(stream, group, id="0", mkstream=True)
    ids = _fill(sync_redis, stream, 3)
    _deliver(sync_redis, stream, group, 3)
    sync_redis.xack(stream, group, *ids)

    async def go() -> None:
        shutdown = asyncio.Event()
        async with _client() as client:
            retention = StreamRetention(client, [stream], min_age_s=0, interval_s=0.05)
            task = asyncio.create_task(retention.run_forever(shutdown))
            deadline = time.monotonic() + 5
            while sync_redis.xlen(stream) and time.monotonic() < deadline:
                await asyncio.sleep(0.05)
            shutdown.set()
            await asyncio.wait_for(task, timeout=5)

    asyncio.run(go())

    assert sync_redis.xlen(stream) == 0


def test_the_worker_trims_the_runs_and_eval_streams() -> None:
    config = WorkerConfig(stream="acme:runs", eval_stream="acme:evals")
    retention = build_stream_retention(config, AsyncRedis())

    assert retention.streams == ("acme:runs", "acme:evals")
    assert retention.min_age_s == 86400


def test_min_age_is_read_from_env_and_bounded(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("CURIE_STREAM_RETENTION_MIN_AGE_S", "7200")
    assert WorkerConfig().stream_retention_min_age_s == 7200
    for bad in ("3599", "31536001", "0"):
        monkeypatch.setenv("CURIE_STREAM_RETENTION_MIN_AGE_S", bad)
        with pytest.raises(ValidationError):
            WorkerConfig()
