"""Once the key is the triple, one non-Slack pair can hold two routes (ADR-0168 decision 3).

A caller that names no adapter is then ambiguous. Each caller answers that
explicitly: the mint, a platform-key ingress and the hook reply surface with a
409 that says to name one, a token-bearing ingress through the row its claim
names, an adapter principal with the same 403 an unserved route gets, and an
agent-scoped lookup by narrowing to the agent.
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import time
import uuid
from typing import Any

import redis
from curie_api import adapter_principal, crud, hook_signing
from curie_api.config import get_settings
from fastapi.testclient import TestClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

ADDRESS = "ops@example.com"
ADAPTER_HEADER = "X-Curie-Adapter-Principal"


def _route(adapter: str) -> dict[str, str]:
    return {
        "kind": "email",
        "address": ADDRESS,
        "endpoint": f"https://{adapter}.example.test/",
        "adapter": adapter,
    }


def _mail(client: TestClient, headers: dict[str, str], name: str, adapter: str) -> str:
    resp = client.post("/agents", json={"name": name, "channel": _route(adapter)}, headers=headers)
    assert resp.status_code == 201, resp.text
    return str(resp.json()["id"])


def _two_agents_on_one_pair(client: TestClient, headers: dict[str, str]) -> tuple[str, str]:
    return (
        _mail(client, headers, "inbox-a", "inbox-a"),
        _mail(client, headers, "inbox-b", "inbox-b"),
    )


def _turn() -> dict[str, Any]:
    return {
        "kind": "email",
        "address": ADDRESS,
        "delivery_id": f"msg-{uuid.uuid4().hex}",
        "conversation_id": f"thread-{uuid.uuid4().hex[:8]}",
        "author": "someone@example.com",
        "text": "hello",
        "reply_ref": f"ref-{uuid.uuid4().hex[:8]}",
    }


def _binding_id(agent_id: str) -> str:
    async def go() -> str:
        engine = create_async_engine(get_settings().database_url)
        try:
            async with engine.connect() as conn:
                result = await conn.execute(
                    text("SELECT id FROM curie.agent_channels WHERE agent_id = :a"),
                    {"a": agent_id},
                )
                return str(result.scalar_one())
        finally:
            await engine.dispose()

    return asyncio.run(go())


def test_minting_for_an_ambiguous_pair_asks_for_the_adapter(
    hooks_client: TestClient, auth_headers: dict[str, str], clean_db: None
) -> None:
    _two_agents_on_one_pair(hooks_client, auth_headers)

    ambiguous = hooks_client.post(
        "/channels/token", json={"kind": "email", "address": ADDRESS}, headers=auth_headers
    )
    assert ambiguous.status_code == 409, ambiguous.text
    assert "adapter" in ambiguous.json()["detail"]

    named = hooks_client.post(
        "/channels/token",
        json={"kind": "email", "address": ADDRESS, "adapter": "inbox-b"},
        headers=auth_headers,
    )
    assert named.status_code == 200, named.text


def test_an_adapter_principal_serving_neither_route_learns_nothing_of_the_count(
    hooks_client: TestClient, auth_headers: dict[str, str], clean_db: None
) -> None:
    """Unserved and unknown read identically for an adapter principal, so an
    ambiguous pair it does not serve must not answer the 409 that names how
    many routes the pair holds."""

    _two_agents_on_one_pair(hooks_client, auth_headers)
    elsewhere = hooks_client.post(
        "/agents",
        json={
            "name": "elsewhere",
            "channel": {**_route("inbox-c"), "address": "desk@example.com"},
        },
        headers=auth_headers,
    )
    assert elsewhere.status_code == 201, elsewhere.text
    token = adapter_principal.mint(
        get_settings().api_key,
        subject="mail-adapter-test",
        bindings=[_binding_id(str(elsewhere.json()["id"]))],
        exp=int(time.time()) + 600,
    )

    refused = hooks_client.post(
        "/channels/token",
        json={"kind": "email", "address": ADDRESS, "ttl_s": 60},
        headers={ADAPTER_HEADER: token},
    )

    assert refused.status_code == 403, refused.text
    assert refused.json()["detail"] == "adapter principal does not serve this binding"


def test_a_token_bearing_turn_reaches_the_row_its_claim_names(
    hooks_client: TestClient,
    auth_headers: dict[str, str],
    clean_db: None,
    valkey: redis.Redis,
) -> None:
    _two_agents_on_one_pair(hooks_client, auth_headers)
    token = hooks_client.post(
        "/channels/token",
        json={"kind": "email", "address": ADDRESS, "adapter": "inbox-b"},
        headers=auth_headers,
    ).json()["token"]

    turn = hooks_client.post("/channels/turns", json=_turn(), headers={"X-API-Key": token})
    assert turn.status_code == 200, turn.text

    other_pair = {**_turn(), "address": "desk@example.com"}
    mismatched = hooks_client.post("/channels/turns", json=other_pair, headers={"X-API-Key": token})
    assert mismatched.status_code == 401, mismatched.text


def test_a_platform_key_turn_on_an_ambiguous_pair_is_a_conflict(
    hooks_client: TestClient,
    auth_headers: dict[str, str],
    clean_db: None,
    valkey: redis.Redis,
) -> None:
    _two_agents_on_one_pair(hooks_client, auth_headers)

    turn = hooks_client.post("/channels/turns", json=_turn(), headers=auth_headers)

    assert turn.status_code == 409, turn.text
    assert "adapter" in turn.json()["detail"]


def test_a_hook_reply_surface_with_two_routes_on_the_pair_is_a_conflict(
    hooks_client: TestClient, auth_headers: dict[str, str], clean_db: None
) -> None:
    agent_id = _mail(hooks_client, auth_headers, "two-inboxes", "inbox-a")
    added = hooks_client.post(
        f"/agents/{agent_id}/channels", json=_route("inbox-b"), headers=auth_headers
    )
    assert added.status_code == 201, added.text
    body = b"{}"
    secret = hook_signing.derive(get_settings().api_key, agent_id=agent_id, generation=0)
    signature = "sha256=" + hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()

    resp = hooks_client.post(
        f"/hooks/{agent_id}/issues?kind=email&address={ADDRESS}",
        content=body,
        headers={"X-Curie-Signature-256": signature, "X-Curie-Delivery-Id": "two-routes-1"},
    )

    assert resp.status_code == 409, resp.text
    assert "adapter" in resp.json()["detail"]


def test_an_agent_scoped_lookup_is_not_ambiguous(
    hooks_client: TestClient, auth_headers: dict[str, str], clean_db: None
) -> None:
    first, _ = _two_agents_on_one_pair(hooks_client, auth_headers)

    async def go() -> str | None:
        engine = create_async_engine(get_settings().database_url)
        try:
            async with async_sessionmaker(engine)() as session:
                row = await crud.binding_for_route(
                    session, "email", None, ADDRESS, agent_id=uuid.UUID(first)
                )
                return None if row is None else row.adapter
        finally:
            await engine.dispose()

    assert asyncio.run(go()) == "inbox-a"
