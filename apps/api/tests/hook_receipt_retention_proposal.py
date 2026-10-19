"""Explicit failing reproduction for Draft ADR 0237; not a default suite test."""

from __future__ import annotations

import asyncio

import redis
import redis.asyncio as async_redis
from curie_api.config import get_settings
from curie_worker.stream_retention import trim_settled
from fastapi.testclient import TestClient
from test_hook_tool_access import _claim, _post
from test_hook_tool_access import hook_agent as _hook_agent

hook_agent = _hook_agent


def test_faithful_retry_after_settled_retention_retains_receipt(
    hooks_client: TestClient,
    hook_agent: str,
    valkey: redis.Redis,
    runs_stream: str,
) -> None:
    """@spec Draft ADR 0237, acceptance test 1."""
    first = _post(hooks_client, hook_agent, body=b'{"event":"synthetic-example"}')
    assert first.status_code == 200, first.text
    stream_id = first.json()["stream_id"]
    claim = _claim(hook_agent)
    assert valkey.get(claim) == stream_id
    assert valkey.ttl(claim) == -1
    valkey.xgroup_create(runs_stream, "synthetic-consumer", id="0-0")
    rows = valkey.xreadgroup("synthetic-consumer", "worker", {runs_stream: ">"}, count=1)
    assert rows[0][1][0][0] == stream_id
    assert valkey.xack(runs_stream, "synthetic-consumer", stream_id) == 1

    async def retain() -> int:
        async with async_redis.from_url(get_settings().valkey_dsn()) as client:
            # Production retention algorithm; zero lag makes this local probe
            # deterministic without waiting for the operator's age window.
            return await trim_settled(client, runs_stream, min_age_s=0)

    assert asyncio.run(retain()) == 1
    assert valkey.xlen(runs_stream) == 0
    retry = _post(hooks_client, hook_agent, body=b'{"event":"synthetic-example"}')
    assert valkey.xlen(runs_stream) == 0
    assert valkey.get(claim) == stream_id
    assert valkey.ttl(claim) == -1
    assert retry.status_code == 200, retry.text
    assert retry.json() == {**first.json(), "duplicate": True}
