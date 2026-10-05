"""Closed signed source ingress, @spec PROTECTED-HOOK-SOURCE-2/4/10."""

from __future__ import annotations

import asyncio
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
from _migration_support import IsolatedMigrationDb, sql_dicts
from curie_api import hook_signing, hook_source_signing
from curie_api.config import get_settings
from curie_api.main import create_app
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import NullPool

HOOK = "daily-summary"
BODY = b'{"message":"test"}'


@pytest.fixture
def ingress_db(isolated_migration_db: IsolatedMigrationDb, monkeypatch: pytest.MonkeyPatch) -> None:
    """@spec PROTECTED-HOOK-SOURCE-2/10."""
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
    monkeypatch.setenv("RUNS_STREAM", "test:curie:source-ingress:" + uuid.uuid4().hex)
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


@asynccontextmanager
async def ingress() -> AsyncIterator[Any]:
    """@spec PROTECTED-HOOK-SOURCE-2."""
    owned_stream = get_settings().runs_stream
    app = create_app()
    agent = None
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as client:
            response = await client.post(
                "/agents",
                headers={"X-API-Key": get_settings().api_key},
                json={
                    "name": "ingress-" + uuid.uuid4().hex,
                    "channel": {
                        "kind": "email",
                        "address": "ingress@example.test",
                        "endpoint": "http://adapter.example.test",
                        "adapter": "mail",
                    },
                },
            )
            assert response.status_code == 201, response.text
            agent = response.json()["id"]
            try:
                yield app, client, agent
            finally:
                keys = [
                    key async for key in app.state.valkey.scan_iter(match=f"curie:hook:*{agent}*")
                ]
                if keys:
                    await app.state.valkey.delete(*keys)
                await app.state.valkey.delete(owned_stream)


def secret(agent: str, *, protected: bool = False, generation: int = 0) -> str:
    """@spec PROTECTED-HOOK-SOURCE-4."""
    if protected:
        return hook_source_signing.derive(
            get_settings().api_key, agent_id=agent, hook=HOOK, generation=generation
        )
    return hook_signing.derive(get_settings().api_key, agent_id=agent, generation=generation)


def signed_headers(
    key: str, *, requested: str | None = None, delivery: str = "test-delivery"
) -> dict[str, str]:
    """Independent delivery framing, @spec PROTECTED-HOOK-SOURCE-2/4."""
    stamp = str(int(time.time()))
    context = json.dumps([HOOK, requested], ensure_ascii=True, separators=(",", ":")).encode(
        "ascii"
    )
    material = (
        b"curie.hook.delivery.v2\n"
        + f"{stamp}.{delivery}.{len(context)}:".encode()
        + context
        + BODY
    )
    return {
        "X-Curie-Timestamp": stamp,
        "X-Curie-Delivery-Id": delivery,
        "X-Curie-Signature-256": "sha256="
        + hmac.new(key.encode(), material, hashlib.sha256).hexdigest(),
    }


def seed(agent: str, kind: str) -> None:
    """SQL records prove no runtime authority, @spec PROTECTED-HOOK-SOURCE-2/10."""
    protected = kind == "protected"
    target = dict(
        mode="protected" if protected else "ordinary",
        tool_access="read-only" if protected else None,
        runtime_id=str(uuid.uuid4()) if protected else None,
        qualification_id=str(uuid.uuid4()) if protected else None,
        bundle_digest="a" * 64 if protected else None,
    )
    operation = str(uuid.uuid4())
    intent = hashlib.sha256(
        json.dumps(target, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode()
    ).hexdigest()
    sql_dicts(
        "INSERT INTO curie.hook_source_operations "
        "(agent_id,hook,operation_id,intent_sha256,status,generation) "
        "VALUES (:agent,:hook,:operation,:intent,:status,9)",
        dict(
            agent=agent,
            hook=HOOK,
            operation=operation,
            intent=intent,
            status="pending" if kind == "pending" else "committed",
        ),
    )
    if kind in ("protected", "ordinary"):
        sql_dicts(
            "INSERT INTO curie.hook_source_policies "
            "(agent_id,hook,operation_id,generation,mode,tool_access,runtime_id,"
            "qualification_id,bundle_digest,legacy_generation) "
            "VALUES (:agent,:hook,:operation,9,:mode,:tool_access,:runtime_id,"
            ":qualification_id,:bundle_digest,0)",
            dict(agent=agent, hook=HOOK, operation=operation, **target),
        )


async def effects(app: Any, agent: str) -> Any:
    """@spec PROTECTED-HOOK-SOURCE-2/10."""
    keys = sorted([key async for key in app.state.valkey.scan_iter(match=f"curie:hook:*{agent}*")])
    return (
        keys,
        await app.state.valkey.xlen(get_settings().runs_stream),
        await asyncio.to_thread(
            sql_dicts,
            "SELECT * FROM curie.thread_workspaces WHERE agent_id=:agent",
            {"agent": agent},
        ),
    )


async def wait_for_advisory(observer: Any) -> None:
    """Measured PG lock wait, @spec PROTECTED-HOOK-SOURCE-2."""
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


@pytest.mark.parametrize("requested", [None, "read-only"])
def test_never_configured_ordinary_preserves_policy_and_durable_duplicate(
    ingress_db: None, requested: str | None
) -> None:
    """@spec PROTECTED-HOOK-SOURCE-2."""

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-2."""
        async with ingress() as (app, client, agent):
            params = {"tool_access": requested} if requested else {}
            headers = signed_headers(secret(agent), requested=requested)
            first = await client.post(
                f"/hooks/{agent}/{HOOK}", content=BODY, headers=headers, params=params
            )
            assert first.status_code == 200, first.text
            assert first.json()["tool_access"] == requested and not first.json()["duplicate"]
            second = await client.post(
                f"/hooks/{agent}/{HOOK}", content=BODY, headers=headers, params=params
            )
            assert second.status_code == 200, second.text
            assert (
                second.json()["duplicate"]
                and second.json()["stream_id"] == first.json()["stream_id"]
            )
            assert await app.state.valkey.xlen(get_settings().runs_stream) == 1

    asyncio.run(asyncio.wait_for(scenario(), 20))


@pytest.mark.parametrize(
    "kind,requested",
    [
        ("pending", None),
        ("historical", None),
        ("ordinary", None),
        ("protected", None),
        ("protected", "read-only"),
    ],
)
def test_configured_or_history_source_is_closed_before_every_effect(
    ingress_db: None, kind: str, requested: str | None
) -> None:
    """@spec PROTECTED-HOOK-SOURCE-2/4/10."""

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-2/4/10."""
        async with ingress() as (app, client, agent):
            await asyncio.to_thread(seed, agent, kind)
            before = await effects(app, agent)
            key = secret(
                agent, protected=kind == "protected", generation=9 if kind == "protected" else 0
            )
            response = await client.post(
                f"/hooks/{agent}/{HOOK}",
                content=BODY,
                headers=signed_headers(key, requested=requested),
                params={"tool_access": requested} if requested else {},
            )
            assert response.status_code == 503, response.text
            assert await effects(app, agent) == before

    asyncio.run(asyncio.wait_for(scenario(), 20))


def test_legacy_key_cannot_authenticate_protected_source(ingress_db: None) -> None:
    """@spec PROTECTED-HOOK-SOURCE-2/4."""

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-2/4."""
        async with ingress() as (app, client, agent):
            await asyncio.to_thread(seed, agent, "protected")
            before = await effects(app, agent)
            response = await client.post(
                f"/hooks/{agent}/{HOOK}", content=BODY, headers=signed_headers(secret(agent))
            )
            assert response.status_code == 401
            assert response.json()["detail"] == "missing or invalid signature"
            assert await effects(app, agent) == before

    asyncio.run(asyncio.wait_for(scenario(), 20))


@pytest.mark.parametrize("unknown", [False, True])
def test_bad_or_unknown_authentication_finishes_without_gate(
    ingress_db: None, unknown: bool
) -> None:
    """@spec PROTECTED-HOOK-SOURCE-2."""

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-2."""
        async with ingress() as (app, client, agent):
            target = str(uuid.uuid4()) if unknown else agent
            async with app.state.source_gate.hold(uuid.UUID(target)):
                async with asyncio.timeout(2):
                    response = await client.post(
                        f"/hooks/{target}/{HOOK}",
                        content=BODY,
                        headers=signed_headers("bad-secret"),
                    )
                assert response.status_code == 401
                assert response.json()["detail"] == "missing or invalid signature"
                assert app.state.source_gate.engine.pool.checkedout() == 1

    asyncio.run(asyncio.wait_for(scenario(), 20))


@pytest.mark.parametrize("protected", [False, True])
def test_waiting_request_reauthenticates_before_fresh_consistency(
    ingress_db: None, protected: bool
) -> None:
    """@spec PROTECTED-HOOK-SOURCE-2/4/10."""

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-2/4/10."""
        async with ingress() as (app, client, agent):
            if protected:
                await asyncio.to_thread(seed, agent, "protected")
            before = await effects(app, agent)
            headers = signed_headers(
                secret(agent, protected=protected, generation=9 if protected else 0)
            )
            observer = create_async_engine(get_settings().database_url, poolclass=NullPool)
            task = None
            try:
                async with app.state.source_gate.hold(uuid.UUID(agent)):
                    task = asyncio.create_task(
                        client.post(f"/hooks/{agent}/{HOOK}", content=BODY, headers=headers)
                    )
                    await wait_for_advisory(observer)
                    assert app.state.engine.pool.checkedout() == 0
                    async with app.state.engine.connect() as conn:
                        assert await conn.scalar(text("SELECT 1")) == 1
                    async with observer.begin() as conn:
                        if protected:
                            await conn.execute(
                                text(
                                    "UPDATE curie.hook_source_policies SET generation=10,"
                                    "operation_id=:op,runtime_id='invalid-ref' "
                                    "WHERE agent_id=:agent"
                                ),
                                {"op": uuid.uuid4(), "agent": uuid.UUID(agent)},
                            )
                        else:
                            await conn.execute(
                                text("UPDATE curie.agents SET hook_generation=1 WHERE id=:agent"),
                                {"agent": uuid.UUID(agent)},
                            )
                            await asyncio.to_thread(seed, agent, "pending")
                response = await asyncio.wait_for(task, 5)
                assert response.status_code == 401, response.text
                assert response.json()["detail"] == "missing or invalid signature"
                assert await effects(app, agent) == before
                fresh = signed_headers(
                    secret(agent, protected=protected, generation=10 if protected else 1),
                    delivery="fresh-delivery",
                )
                response = await client.post(f"/hooks/{agent}/{HOOK}", content=BODY, headers=fresh)
                assert response.status_code == 503, response.text
                assert response.json()["detail"] == (
                    "authority_unavailable" if protected else "pending_history"
                )
                assert await effects(app, agent) == before
            finally:
                if task is not None and not task.done():
                    task.cancel()
                    await asyncio.gather(task, return_exceptions=True)
                await observer.dispose()

    asyncio.run(asyncio.wait_for(scenario(), 20))


def test_canceled_signed_waiter_releases_resources_then_successor_enqueues(
    ingress_db: None,
) -> None:
    """@spec PROTECTED-HOOK-SOURCE-2."""

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-2."""
        async with ingress() as (app, client, agent):
            before = await effects(app, agent)
            observer = create_async_engine(get_settings().database_url, poolclass=NullPool)
            task = None
            try:
                async with app.state.source_gate.hold(uuid.UUID(agent)):
                    task = asyncio.create_task(
                        client.post(
                            f"/hooks/{agent}/{HOOK}",
                            content=BODY,
                            headers=signed_headers(secret(agent), delivery="canceled-delivery"),
                        )
                    )
                    await wait_for_advisory(observer)
                    task.cancel()
                    with pytest.raises(asyncio.CancelledError):
                        await task
                    assert app.state.source_gate.engine.pool.checkedout() == 1
                    assert app.state.engine.pool.checkedout() == 0
                    assert await effects(app, agent) == before
                response = await client.post(
                    f"/hooks/{agent}/{HOOK}",
                    content=BODY,
                    headers=signed_headers(secret(agent), delivery="successor-delivery"),
                )
                assert response.status_code == 200, response.text
                assert not response.json()["duplicate"]
                assert await app.state.valkey.xlen(get_settings().runs_stream) == 1
                assert app.state.source_gate.engine.pool.checkedout() == 0
            finally:
                if task is not None and not task.done():
                    task.cancel()
                    await asyncio.gather(task, return_exceptions=True)
                await observer.dispose()

    asyncio.run(asyncio.wait_for(scenario(), 20))


def test_actual_gate_loss_during_routing_refuses_before_delivery_claim(ingress_db: None) -> None:
    """Pre-effect proof only, @spec PROTECTED-HOOK-SOURCE-2."""

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-2."""
        async with ingress() as (app, client, agent):
            before = await effects(app, agent)
            observer = create_async_engine(get_settings().database_url, poolclass=NullPool)
            task = None
            try:
                async with observer.connect() as blocker:
                    transaction = await blocker.begin()
                    try:
                        async with app.state.source_gate.hold(uuid.UUID(agent)):
                            task = asyncio.create_task(
                                client.post(
                                    f"/hooks/{agent}/{HOOK}",
                                    content=BODY,
                                    headers=signed_headers(
                                        secret(agent), delivery="terminated-delivery"
                                    ),
                                )
                            )
                            await wait_for_advisory(observer)
                            # Preauth has released its read transaction. The table
                            # lock now stops a later real post-auth routing SELECT.
                            await blocker.execute(
                                text("LOCK TABLE curie.agent_channels IN ACCESS EXCLUSIVE MODE")
                            )
                        async with asyncio.timeout(5):
                            while True:
                                async with observer.connect() as conn:
                                    relation_wait = await conn.scalar(
                                        text(
                                            "SELECT EXISTS (SELECT 1 FROM pg_stat_activity "
                                            "WHERE datname=current_database() "
                                            "AND wait_event='relation')"
                                        )
                                    )
                                if relation_wait:
                                    break
                                await asyncio.sleep(0.01)
                        assert not task.done()
                        async with observer.connect() as conn:
                            gate_pid = await conn.scalar(
                                text(
                                    "SELECT l.pid FROM pg_locks l "
                                    "JOIN pg_database d ON d.oid=l.database "
                                    "WHERE l.locktype='advisory' AND l.granted "
                                    "AND d.datname=current_database()"
                                )
                            )
                            assert gate_pid is not None
                            assert await conn.scalar(
                                text("SELECT pg_terminate_backend(:pid)"), {"pid": gate_pid}
                            )
                        await transaction.commit()
                        response = await asyncio.wait_for(task, 5)
                        assert response.status_code == 503, response.text
                        assert response.json()["detail"] == "authority_unavailable"
                        assert await effects(app, agent) == before
                    finally:
                        if transaction.is_active:
                            await transaction.rollback()
            finally:
                if task is not None and not task.done():
                    task.cancel()
                    await asyncio.gather(task, return_exceptions=True)
                await observer.dispose()

    asyncio.run(asyncio.wait_for(scenario(), 20))
