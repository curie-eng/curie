"""How long the API can wait on one Valkey reply (#5995).

The tests build clients with the production builder (`valkey_client.create_valkey`)
against the real test Valkey, through a TCP relay whose open flows can be cut.
"""

from __future__ import annotations

import asyncio
import time
import uuid
from collections.abc import Iterator
from typing import Any

import pytest
import redis
from _connection_bounds_support import OUTER_DEADLINE_S
from curie_api.config import get_settings
from curie_api.valkey_client import create_valkey

pytestmark = pytest.mark.anyio


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


@pytest.fixture
def valkey_through(
    valkey: redis.Redis, tcp_blackhole: Any, monkeypatch: pytest.MonkeyPatch
) -> Iterator[Any]:
    """Route the API's Valkey settings through a relay to the real test Valkey."""

    settings = get_settings()
    relay = tcp_blackhole(settings.valkey_host, settings.valkey_port)
    monkeypatch.setenv("VALKEY_HOST", "127.0.0.1")
    monkeypatch.setenv("VALKEY_PORT", str(relay.port))
    monkeypatch.delenv("VALKEY_URL", raising=False)
    key = f"test:curie:hold-bounds:{uuid.uuid4().hex}"
    yield relay, key
    valkey.delete(key)


async def test_a_valkey_reply_that_never_arrives_fails_at_socket_timeout(
    connection_bounds: Any, valkey_through: Any
) -> None:
    relay, key = valkey_through
    connection_bounds(CURIE_API_VALKEY_SOCKET_TIMEOUT_S="0.5")
    client = create_valkey(get_settings())
    try:
        assert await client.set(key, "held") is True
        assert relay.cut() == 1

        async def read_on_the_dead_flow() -> None:
            with pytest.raises(redis.exceptions.TimeoutError):
                await client.get(key)

        started = time.monotonic()
        await asyncio.wait_for(read_on_the_dead_flow(), OUTER_DEADLINE_S)
        # Well under redis-py's own 5 s default: the configured deadline governs.
        assert time.monotonic() - started < 2.5
        # The timed out connection was dropped; the next command reconnects.
        assert await asyncio.wait_for(client.get(key), OUTER_DEADLINE_S) == b"held"
        assert len(relay.sent) == 2
    finally:
        await client.aclose()


async def test_a_valkey_connection_idle_past_the_interval_is_pinged_before_reuse(
    connection_bounds: Any, valkey_through: Any
) -> None:
    relay, key = valkey_through
    # Far above any stall inside the connect handshake, whose pipelined replies
    # a check falling due mid-handshake would consume, and short enough that the
    # test waits half a second for one.
    connection_bounds(CURIE_API_VALKEY_HEALTH_CHECK_INTERVAL_S="0.25")
    client = create_valkey(get_settings())
    try:
        assert await client.set(key, "held") is True
        sent_before = len(relay.sent[0])
        # The pooled connection sits idle past the interval: one timer, 0.5 s.
        idle = asyncio.Event()
        asyncio.get_running_loop().call_later(0.5, idle.set)
        await idle.wait()
        assert await client.get(key) == b"held"
        assert len(relay.sent) == 1
        between = bytes(relay.sent[0][sent_before:])
        assert b"PING" in between
        assert between.index(b"PING") < between.index(b"GET")
    finally:
        await client.aclose()
