"""Fresh legacy credential reads, @spec PROTECTED-HOOK-SOURCE-2/4/5."""

from __future__ import annotations

import asyncio
import base64
import hashlib
import hmac
import json
import time
import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

import httpx
import pytest
from _migration_support import IsolatedMigrationDb
from curie_api.config import get_settings
from curie_api.main import create_app
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import NullPool


@pytest.fixture
def secret_db(isolated_migration_db: IsolatedMigrationDb, monkeypatch: pytest.MonkeyPatch) -> None:
    """@spec PROTECTED-HOOK-SOURCE-2/5."""
    isolated_migration_db.at("head")
    for name, value in {
        "GITHUB_REVIEW_INGRESS_ENABLED": "false",
        "RESUME_RECONCILER_ENABLED": "false",
        "CURIE_WORK_ITEM_RECONCILER_ENABLED": "false",
        "APPROVAL_SWEEP_INTERVAL_S": "0",
        "DEAD_LETTER_WATCH_INTERVAL_S": "0",
        "COMMIT_POLL_INTERVAL_S": "0",
        "OTEL_SDK_DISABLED": "true",
        "OTEL_EXPORTER_OTLP_ENDPOINT": "",
    }.items():
        monkeypatch.setenv(name, value)
    monkeypatch.setenv("RUNS_STREAM", "test:curie:legacy-secret:" + uuid.uuid4().hex)
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


@asynccontextmanager
async def secret_app() -> AsyncIterator[Any]:
    """@spec PROTECTED-HOOK-SOURCE-2/5."""
    app = create_app()
    stream = get_settings().runs_stream
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as client:
            headers = {"X-API-Key": get_settings().api_key}
            response = await client.post(
                "/agents",
                headers=headers,
                json={
                    "name": "secret-" + uuid.uuid4().hex,
                    "channel": {
                        "kind": "email",
                        "address": "secret@example.test",
                        "endpoint": "http://adapter.example.test",
                        "adapter": "mail",
                    },
                },
            )
            assert response.status_code == 201
            agent = response.json()["id"]
            try:
                yield app, client, headers, agent
            finally:
                keys = [
                    key async for key in app.state.valkey.scan_iter(match=f"curie:hook:*{agent}*")
                ]
                if keys:
                    await app.state.valkey.delete(*keys)
                await app.state.valkey.delete(stream)


def expected_key(agent: str, generation: int) -> str:
    """Independent legacy derivation, @spec PROTECTED-HOOK-SOURCE-4/5."""
    digest = hmac.new(
        get_settings().api_key.encode(),
        f"curie.hook.v1:{agent}:{generation}".encode(),
        hashlib.sha256,
    ).digest()
    return base64.urlsafe_b64encode(digest).rstrip(b"=").decode()


async def counter(app: Any, agent: str, generation: int) -> None:
    """Adversarial actual SQL fixture, @spec PROTECTED-HOOK-SOURCE-2/5."""
    async with app.state.engine.begin() as conn:
        await conn.execute(
            text("UPDATE curie.agents SET hook_generation=:generation WHERE id=:agent"),
            {"generation": generation, "agent": uuid.UUID(agent)},
        )


async def wait_gate(observer: Any) -> None:
    """Actual PG lock evidence, @spec PROTECTED-HOOK-SOURCE-2."""
    async with asyncio.timeout(5):
        while True:
            async with observer.connect() as conn:
                waiting = await conn.scalar(
                    text(
                        "SELECT EXISTS (SELECT 1 FROM pg_stat_activity "
                        "WHERE datname=current_database() AND wait_event='advisory')"
                    )
                )
            if waiting:
                return
            await asyncio.sleep(0.01)


@pytest.mark.parametrize("generation", [0, 2147483647])
def test_current_legacy_key_shape_and_no_store_are_preserved(
    secret_db: None, generation: int
) -> None:
    """@spec PROTECTED-HOOK-SOURCE-2/4/5."""

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-2/4/5."""
        async with secret_app() as (app, client, headers, agent):
            await counter(app, agent, generation)
            response = await client.get(f"/agents/{agent}/hook-secret", headers=headers)
            assert response.status_code == 200
            assert response.headers["Cache-Control"] == "no-store"
            assert set(response.json()) == {"secret"}
            assert hmac.compare_digest(response.json()["secret"], expected_key(agent, generation))

    asyncio.run(asyncio.wait_for(scenario(), 20))


def test_negative_counter_refuses_without_returning_any_secret(secret_db: None) -> None:
    """@spec PROTECTED-HOOK-SOURCE-2/5."""

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-2/5."""
        async with secret_app() as (app, client, headers, agent):
            await counter(app, agent, -1)
            response = await client.get(f"/agents/{agent}/hook-secret", headers=headers)
            assert response.status_code == 503
            assert response.json() == {"detail": "authority_unavailable"}

    asyncio.run(asyncio.wait_for(scenario(), 20))


@pytest.mark.parametrize("bad_headers", [{}, {"X-API-Key": "wrong-key"}])
def test_platform_authentication_refuses_before_agent_gate(
    secret_db: None, bad_headers: dict[str, str]
) -> None:
    """@spec PROTECTED-HOOK-SOURCE-2/5."""

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-2/5."""
        async with secret_app() as (app, client, _headers, agent):
            async with app.state.source_gate.hold(uuid.UUID(agent)):
                async with asyncio.timeout(2):
                    response = await client.get(f"/agents/{agent}/hook-secret", headers=bad_headers)
                assert response.status_code == 401
                assert app.state.source_gate.engine.pool.checkedout() == 1

    asyncio.run(asyncio.wait_for(scenario(), 20))


def test_unknown_agent_retains_exact_not_found_response(secret_db: None) -> None:
    """@spec PROTECTED-HOOK-SOURCE-2/5."""

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-2/5."""
        async with secret_app() as (_app, client, headers, _agent):
            response = await client.get(f"/agents/{uuid.uuid4()}/hook-secret", headers=headers)
            assert response.status_code == 404
            assert response.json() == {"detail": "agent not found"}

    asyncio.run(asyncio.wait_for(scenario(), 20))


def test_waiting_secret_read_releases_work_and_uses_new_counter(secret_db: None) -> None:
    """@spec PROTECTED-HOOK-SOURCE-2/5."""

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-2/5."""
        async with secret_app() as (app, client, headers, agent):
            observer = create_async_engine(get_settings().database_url, poolclass=NullPool)
            task = None
            try:
                async with app.state.source_gate.hold(uuid.UUID(agent)):
                    task = asyncio.create_task(
                        client.get(f"/agents/{agent}/hook-secret", headers=headers)
                    )
                    await wait_gate(observer)
                    assert app.state.engine.pool.checkedout() == 0
                    async with app.state.engine.connect() as conn:
                        assert await conn.scalar(text("SELECT 1")) == 1
                    await counter(app, agent, 7)
                response = await asyncio.wait_for(task, 5)
                assert response.status_code == 200
                value = response.json()["secret"]
                assert hmac.compare_digest(value, expected_key(agent, 7))
                assert not hmac.compare_digest(value, expected_key(agent, 0))
                assert response.headers["Cache-Control"] == "no-store"
            finally:
                if task is not None and not task.done():
                    task.cancel()
                    await asyncio.gather(task, return_exceptions=True)
                await observer.dispose()

    asyncio.run(asyncio.wait_for(scenario(), 20))


def test_cancelled_secret_waiter_frees_resources_for_successor(secret_db: None) -> None:
    """@spec PROTECTED-HOOK-SOURCE-2/5."""

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-2/5."""
        async with secret_app() as (app, client, headers, agent):
            observer = create_async_engine(get_settings().database_url, poolclass=NullPool)
            task = None
            try:
                async with app.state.source_gate.hold(uuid.UUID(agent)):
                    task = asyncio.create_task(
                        client.get(f"/agents/{agent}/hook-secret", headers=headers)
                    )
                    await wait_gate(observer)
                    task.cancel()
                    with pytest.raises(asyncio.CancelledError):
                        await task
                    assert app.state.source_gate.engine.pool.checkedout() == 1
                    assert app.state.engine.pool.checkedout() == 0
                response = await client.get(f"/agents/{agent}/hook-secret", headers=headers)
                assert response.status_code == 200
                assert app.state.source_gate.engine.pool.checkedout() == 0
            finally:
                if task is not None and not task.done():
                    task.cancel()
                    await asyncio.gather(task, return_exceptions=True)
                await observer.dispose()

    asyncio.run(asyncio.wait_for(scenario(), 20))


def test_protected_source_get_returns_only_legacy_family_rejected_by_ingress(
    secret_db: None,
) -> None:
    """No qualified authority, @spec PROTECTED-HOOK-SOURCE-2/4/5."""

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-2/4/5."""
        async with secret_app() as (app, client, headers, agent):
            hook = "daily-summary"
            target = dict(
                mode="protected",
                tool_access="read-only",
                runtime_id=str(uuid.uuid4()),
                qualification_id=str(uuid.uuid4()),
                bundle_digest="a" * 64,
            )
            intent = hashlib.sha256(
                json.dumps(
                    target, sort_keys=True, separators=(",", ":"), ensure_ascii=True
                ).encode()
            ).hexdigest()
            operation = uuid.uuid4()
            async with app.state.engine.begin() as conn:
                await conn.execute(
                    text(
                        "INSERT INTO curie.hook_source_operations "
                        "(agent_id,hook,operation_id,intent_sha256,status,generation) "
                        "VALUES (:agent,:hook,:op,:intent,'committed',9)"
                    ),
                    dict(agent=uuid.UUID(agent), hook=hook, op=operation, intent=intent),
                )
                await conn.execute(
                    text(
                        "INSERT INTO curie.hook_source_policies "
                        "(agent_id,hook,generation,operation_id,mode,tool_access,runtime_id,"
                        "qualification_id,bundle_digest,legacy_generation) "
                        "VALUES (:agent,:hook,9,:op,:mode,:tool_access,:runtime_id,"
                        ":qualification_id,:bundle_digest,0)"
                    ),
                    dict(agent=uuid.UUID(agent), hook=hook, op=operation, **target),
                )
            response = await client.get(f"/agents/{agent}/hook-secret", headers=headers)
            assert response.status_code == 200 and set(response.json()) == {"secret"}
            assert hmac.compare_digest(response.json()["secret"], expected_key(agent, 0))
            stamp, delivery, body = str(int(time.time())), "legacy-delivery", b'{"test":true}'
            context = json.dumps([hook, None], separators=(",", ":")).encode()
            material = (
                b"curie.hook.delivery.v2\n"
                + f"{stamp}.{delivery}.{len(context)}:".encode()
                + context
                + body
            )
            signature = (
                "sha256="
                + hmac.new(response.json()["secret"].encode(), material, hashlib.sha256).hexdigest()
            )
            denied = await client.post(
                f"/hooks/{agent}/{hook}",
                content=body,
                headers={
                    "X-Curie-Timestamp": stamp,
                    "X-Curie-Delivery-Id": delivery,
                    "X-Curie-Signature-256": signature,
                },
            )
            assert denied.status_code == 401
            assert denied.json() == {"detail": "missing or invalid signature"}
            assert await app.state.valkey.xlen(get_settings().runs_stream) == 0
            assert not [
                key async for key in app.state.valkey.scan_iter(match=f"curie:hook:*{agent}*")
            ]

    asyncio.run(asyncio.wait_for(scenario(), 20))
