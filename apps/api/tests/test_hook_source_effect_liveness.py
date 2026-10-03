"""Genuine ingress effect boundary loss, @spec PROTECTED-HOOK-SOURCE-2."""

from __future__ import annotations

import asyncio
import importlib.util
import uuid
from pathlib import Path
from typing import Any

import pytest
from _source_valkey_reply_relay import GenuineReplyRelay
from curie_api.config import get_settings
from redis.asyncio import Redis
from redis.exceptions import ResponseError
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import NullPool


def ingress_support() -> Any:
    """@spec PROTECTED-HOOK-SOURCE-2."""
    path = Path(__file__).with_name("test_hook_source_ingress.py")
    spec = importlib.util.spec_from_file_location("_source_effect_ingress_setup", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


core = ingress_support()
ingress_db = core.ingress_db


async def terminate_exact_gate(observer: Any, agent: str) -> None:
    """Exact owned DB/hash PID, @spec PROTECTED-HOOK-SOURCE-2."""
    async with observer.connect() as connection:
        pid = await connection.scalar(
            text(
                "SELECT l.pid FROM pg_locks l "
                "CROSS JOIN (SELECT hashtextextended(:key,0) AS h) k "
                "WHERE l.locktype='advisory' AND l.granted "
                "AND l.database=(SELECT oid FROM pg_database WHERE datname=current_database()) "
                "AND l.classid::bigint=((k.h >> 32) & 4294967295) "
                "AND l.objid::bigint=(k.h & 4294967295)"
            ),
            dict(key="hook-source:" + agent),
        )
        assert pid is not None
        assert await connection.scalar(text("SELECT pg_terminate_backend(:pid)"), dict(pid=pid))


async def owned_state(redis: Any, observer: Any, agent: str) -> tuple[Any, Any]:
    """@spec PROTECTED-HOOK-SOURCE-2."""
    keys = sorted([k async for k in redis.scan_iter(match=f"curie:hook:*{agent}*")])
    values = {key: await redis.get(key) for key in keys}
    async with observer.connect() as connection:
        workspaces = (
            await connection.execute(
                text("SELECT * FROM curie.thread_workspaces WHERE agent_id=:agent"),
                dict(agent=uuid.UUID(agent)),
            )
        ).all()
    return values, workspaces


@pytest.mark.parametrize("phase", ["live_claim", "claim", "quota", "enqueue_error"])
def test_genuine_completed_effect_gate_loss_refuses_next_effect(
    ingress_db: None, monkeypatch: pytest.MonkeyPatch, phase: str
) -> None:
    """Prior claim/quota are honest durable effects, @spec PROTECTED-HOOK-SOURCE-2."""
    monkeypatch.setenv("GITHUB_REPO_ALLOWLIST", '["example-org/*"]')
    get_settings.cache_clear()
    upstream = get_settings()
    upstream_dsn = upstream.valkey_dsn()

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-2."""
        relay = GenuineReplyRelay(
            upstream.valkey_host,
            upstream.valkey_port,
            "claim" if phase == "live_claim" else phase,
        )
        port = await relay.start()
        monkeypatch.setenv("VALKEY_HOST", "127.0.0.1")
        monkeypatch.setenv("VALKEY_PORT", str(port))
        get_settings.cache_clear()
        observer = create_async_engine(get_settings().database_url, poolclass=NullPool)
        direct = Redis.from_url(upstream_dsn, decode_responses=True)
        request = None
        try:
            async with core.ingress() as (app, client, agent):
                if phase in {"quota", "live_claim"}:
                    configured = await client.patch(
                        f"/agents/{agent}",
                        headers={"X-API-Key": get_settings().api_key},
                        json={
                            "source_bindings": {
                                core.HOOK: {
                                    "workload_pointer": "/message",
                                    "map": {
                                        "test": {
                                            "repository": "example-org/example-repo",
                                            "revision": "0123456789abcdef0123456789abcdef01234567",
                                        }
                                    },
                                }
                            }
                        },
                    )
                    assert configured.status_code == 200, configured.text
                if phase == "enqueue_error":
                    await direct.set(get_settings().runs_stream, "owned-wrongtype")
                request = asyncio.create_task(
                    client.post(
                        f"/hooks/{agent}/{core.HOOK}",
                        content=core.BODY,
                        headers=core.signed_headers(core.secret(agent), delivery="effect-boundary"),
                    )
                )
                await asyncio.wait_for(relay.seen.wait(), 5)
                before, workspaces = await owned_state(direct, observer, agent)
                claims = [k for k in before if ":delivery:" in k]
                quotas = [k for k in before if ":backlog:" in k]
                assert len(claims) == 1 and before[claims[0]].startswith("pending:")
                assert workspaces == []
                if phase in {"quota", "enqueue_error"}:
                    assert len(quotas) == 2, "real quota counter and token completed"
                else:
                    assert quotas == []
                if phase != "live_claim":
                    await terminate_exact_gate(observer, agent)
                relay.release.set()
                if phase == "enqueue_error":
                    with pytest.raises(ResponseError, match="WRONGTYPE"):
                        await asyncio.wait_for(request, 5)
                    assert await direct.get(get_settings().runs_stream) == "owned-wrongtype"
                else:
                    response = await asyncio.wait_for(request, 5)
                    if phase == "live_claim":
                        assert response.status_code == 200, response.text
                        assert await direct.xlen(get_settings().runs_stream) == 1
                        _, selected = await owned_state(direct, observer, agent)
                        assert len(selected) == 1, "live real mapping selects actual workspace"
                        return
                    assert response.status_code == 503, response.text
                    assert response.json()["detail"] == "authority_unavailable"
                    assert await direct.xlen(get_settings().runs_stream) == 0
                after, workspaces = await owned_state(direct, observer, agent)
                assert after == before, "detected dead gate cannot reserve/refund/delete next state"
                assert workspaces == []
        finally:
            relay.release.set()
            if request is not None and not request.done():
                request.cancel()
                await asyncio.gather(request, return_exceptions=True)
            await direct.aclose()
            await observer.dispose()
            await relay.close()
            get_settings.cache_clear()

    asyncio.run(asyncio.wait_for(scenario(), 20))
