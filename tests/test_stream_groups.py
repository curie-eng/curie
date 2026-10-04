"""All runs stream consumers share a real Valkey group creation path."""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import Awaitable, Callable

import pytest
from curie_internal.streams import ensure_group
from curie_test_support.valkey import NO_RETRY, VALKEY_HOST, VALKEY_PORT, VALKEY_PW
from redis.asyncio import Redis
from redis.exceptions import ResponseError


def _with_valkey(exercise: Callable[[Redis, str, str], Awaitable[None]]) -> None:
    async def run() -> None:
        client = Redis(
            host=VALKEY_HOST,
            port=VALKEY_PORT,
            password=VALKEY_PW or None,
            decode_responses=True,
            retry=NO_RETRY,
        )
        token = uuid.uuid4().hex
        stream = f"test:3833:stream:{token}"
        group = f"test-group-{token}"
        try:
            await client.ping()
            await exercise(client, stream, group)
        finally:
            try:
                await client.delete(stream)
                assert await client.exists(stream) == 0
            finally:
                await client.aclose()

    asyncio.run(run())


def test_shared_group_creation_initializes_a_missing_stream() -> None:
    async def exercise(client: Redis, stream: str, group: str) -> None:
        assert await client.exists(stream) == 0

        await ensure_group(client, stream, group, start_id="0")

        assert await client.type(stream) == "stream"
        groups = await client.xinfo_groups(stream)
        assert len(groups) == 1
        assert groups[0]["name"] == group
        assert groups[0]["pending"] == 0

    _with_valkey(exercise)


def test_shared_group_creation_accepts_busygroup_without_resetting_delivery() -> None:
    async def exercise(client: Redis, stream: str, group: str) -> None:
        entry_id = await client.xadd(stream, {"payload": "existing-entry"})
        await ensure_group(client, stream, group, start_id="0")
        delivered = await client.xreadgroup(group, "consumer", {stream: ">"}, count=1)
        assert delivered[0][1][0][0] == entry_id

        # The existing group makes XGROUP CREATE return BUSYGROUP on real Valkey.
        # Reusing it must retain its cursor and pending entry.
        await ensure_group(client, stream, group, start_id="0")

        assert await client.xreadgroup(group, "consumer", {stream: ">"}, count=1) == []
        groups = await client.xinfo_groups(stream)
        assert len(groups) == 1
        assert groups[0]["pending"] == 1
        assert groups[0]["last-delivered-id"] == entry_id

    _with_valkey(exercise)


def test_shared_group_creation_propagates_other_valkey_errors() -> None:
    async def exercise(client: Redis, stream: str, group: str) -> None:
        await client.set(stream, "occupied-by-another-type")

        with pytest.raises(ResponseError, match="WRONGTYPE"):
            await ensure_group(client, stream, group, start_id="0")

        assert await client.get(stream) == "occupied-by-another-type"

    _with_valkey(exercise)
