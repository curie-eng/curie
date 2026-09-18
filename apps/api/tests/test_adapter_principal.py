"""Adapter principal: one scoped credential for a trusted ingress adapter (#2806, ADR-0154).

The adapter credential (`adp.` token, header `X-Curie-Adapter-Principal`) is
issued by the platform key, carries exactly three scopes over a set of binding
rows, and can: mint `chn` tokens for bindings it serves, list approvals routed
to those bindings, and resolve them on behalf of a sender it authenticated
(`X-Curie-Approval-Actor`). Every other route keeps refusing it.

Everything drives the real HTTP surface against real Postgres and Valkey.
"""

from __future__ import annotations

import asyncio
import time
import uuid
from collections.abc import Iterator
from typing import Any

import pytest
import redis
from curie_api import adapter_principal
from curie_api.config import get_settings
from curie_api.main import create_app
from fastapi.testclient import TestClient
from sqlalchemy import text as sql_text
from sqlalchemy.ext.asyncio import create_async_engine

ADAPTER_HEADER = "X-Curie-Adapter-Principal"
ACTOR_HEADER = "X-Curie-Approval-Actor"
ADAPTER_SUBJECT = "mail-adapter-test"
SENDER = "U0EXAMPLE1"
OTHER = "U0EXAMPLE2"
EMAIL_ENDPOINT = "http://curie-mail-adapter:8080/"
EMAIL_ADAPTER = "agentmail-sandbox"
NOT_FOUND = "approval not found"


@pytest.fixture
def adapter_client(_disposable_db: Any, runs_stream: str) -> Iterator[TestClient]:
    with TestClient(create_app()) as test_client:
        yield test_client


# --- helpers ------------------------------------------------------------------


def _uid() -> str:
    return uuid.uuid4().hex[:8]


def _binding_id(agent_id: str) -> str:
    async def run() -> str:
        engine = create_async_engine(get_settings().database_url)
        try:
            async with engine.connect() as conn:
                result = await conn.execute(
                    sql_text("SELECT id FROM curie.agent_channels WHERE agent_id = :aid"),
                    {"aid": agent_id},
                )
                return str(result.scalar_one())
        finally:
            await engine.dispose()

    return asyncio.run(run())


def _email_agent(client: TestClient, auth: dict[str, str], address: str) -> tuple[str, str]:
    created = client.post(
        "/agents",
        json={
            "name": f"adapter-mail-{_uid()}",
            "channel": {
                "kind": "email",
                "address": address,
                "endpoint": EMAIL_ENDPOINT,
                "adapter": EMAIL_ADAPTER,
            },
        },
        headers=auth,
    )
    assert created.status_code == 201, created.text
    agent_id = str(created.json()["id"])
    return agent_id, _binding_id(agent_id)


def _routed_agent(
    client: TestClient,
    auth: dict[str, str],
    *,
    approvers: dict[str, Any] | None,
) -> dict[str, str]:
    """An agent whose channel binding IS its approval route's resolution target."""

    channel = f"C0ADP{_uid().upper()}"
    route = f"route-{_uid()}"
    route_body: dict[str, Any] = {"resolution": {"kind": "slack", "address": channel}}
    if approvers is not None:
        route_body["approvers"] = approvers
    created = client.post(
        "/agents",
        json={
            "name": f"adapter-route-{_uid()}",
            "channel": {"kind": "slack", "address": channel},
            "approval_routes": {route: route_body},
        },
        headers=auth,
    )
    assert created.status_code == 201, created.text
    agent_id = str(created.json()["id"])
    return {
        "agent_id": agent_id,
        "binding_id": _binding_id(agent_id),
        "route": route,
        "channel": channel,
    }


def _approval(client: TestClient, auth: dict[str, str], routed: dict[str, str]) -> dict[str, Any]:
    response = client.post(
        "/approvals",
        json={
            "conversation_id": f"th-{_uid()}",
            "author": OTHER,
            "summary": "Confirm the requested action",
            "reply_kind": "slack",
            "reply_channel": routed["channel"],
            "reply_placeholder": "p-1",
            "dedupe_key": uuid.uuid4().hex,
            "agent_id": routed["agent_id"],
            "route": routed["route"],
            "card_channel": routed["channel"],
            "gate_kind": "policy",
        },
        headers=auth,
    )
    assert response.status_code == 201, response.text
    return response.json()


def _adapter_token(bindings: list[str], *, exp: int | None = None) -> str:
    return adapter_principal.mint(
        get_settings().api_key,
        subject=ADAPTER_SUBJECT,
        bindings=bindings,
        exp=exp if exp is not None else int(time.time()) + 600,
    )


def _adp(token: str, actor: str | None = None) -> dict[str, str]:
    headers = {ADAPTER_HEADER: token}
    if actor is not None:
        headers[ACTOR_HEADER] = actor
    return headers


def _turn(address: str) -> dict[str, Any]:
    return {
        "kind": "email",
        "address": address,
        "delivery_id": f"msg-{uuid.uuid4().hex}",
        "conversation_id": f"thread-{_uid()}",
        "author": "correspondent@example.test",
        "text": "status please",
        "reply_ref": f"ref-{_uid()}",
    }


def _status(client: TestClient, auth: dict[str, str], approval_id: str) -> str:
    return str(client.get(f"/approvals/{approval_id}", headers=auth).json()["status"])


def _audit(client: TestClient, auth: dict[str, str], approval_id: str) -> list[dict[str, Any]]:
    response = client.get(f"/approvals/{approval_id}/audit", headers=auth)
    assert response.status_code == 200, response.text
    return list(response.json())


# --- credential module --------------------------------------------------------


def test_verify_round_trips_and_refuses_expired_and_foreign_keys() -> None:
    key = "unit-signing-key"
    binding = str(uuid.uuid4())
    now = int(time.time())
    token = adapter_principal.mint(key, subject="a", bindings=[binding], exp=now + 60)
    assert token.startswith("adp.")
    for scope in adapter_principal.SCOPES:
        claims = adapter_principal.verify(token, key, scope=scope, now=now)
        assert claims is not None
        assert claims.subject == "a"
        assert claims.bindings == frozenset({uuid.UUID(binding)})
    assert adapter_principal.verify(token, key, scope="state", now=now) is None
    assert adapter_principal.verify(token, "other-key", scope="approvals:read", now=now) is None
    assert adapter_principal.verify(token, key, scope="approvals:read", now=now + 60) is None
    with pytest.raises(ValueError):
        adapter_principal.mint(key, subject=" ", bindings=[binding], exp=now + 60)
    with pytest.raises(ValueError):
        adapter_principal.mint(key, subject="a", bindings=[], exp=now + 60)


# --- 1. channel token mint ----------------------------------------------------


def test_adapter_mints_chn_for_served_binding_and_generation_bump_kills_old_token(
    adapter_client: TestClient,
    auth_headers: dict[str, str],
    clean_db: None,
    valkey: redis.Redis,
    runs_stream: str,
) -> None:
    address = f"served-{_uid()}@example.test"
    _agent, binding = _email_agent(adapter_client, auth_headers, address)
    headers = _adp(_adapter_token([binding]))
    body = {"kind": "email", "address": address, "ttl_s": 3600}

    first = adapter_client.post("/channels/token", json=body, headers=headers)
    assert first.status_code == 200, first.text
    old = first.json()["token"]
    assert old.startswith("chn.")
    second = adapter_client.post("/channels/token", json=body, headers=headers)
    assert second.status_code == 200, second.text
    new = second.json()["token"]

    accepted = adapter_client.post(
        "/channels/turns", json=_turn(address), headers={"X-API-Key": new}
    )
    assert accepted.status_code == 200, accepted.text
    assert len(valkey.xrange(runs_stream)) == 1
    refused = adapter_client.post(
        "/channels/turns", json=_turn(address), headers={"X-API-Key": old}
    )
    assert refused.status_code == 401, refused.text
    assert len(valkey.xrange(runs_stream)) == 1


def test_adapter_mint_for_unserved_binding_is_403(
    adapter_client: TestClient, auth_headers: dict[str, str], clean_db: None
) -> None:
    served_address = f"served-{_uid()}@example.test"
    other_address = f"other-{_uid()}@example.test"
    _a, served = _email_agent(adapter_client, auth_headers, served_address)
    _email_agent(adapter_client, auth_headers, other_address)
    headers = _adp(_adapter_token([served]))

    for address in (other_address, f"unknown-{_uid()}@example.test"):
        refused = adapter_client.post(
            "/channels/token",
            json={"kind": "email", "address": address, "ttl_s": 60},
            headers=headers,
        )
        assert refused.status_code == 403, refused.text
        assert refused.json()["detail"] == "adapter principal does not serve this binding"


def test_channel_mint_without_credential_is_401_and_platform_key_still_works(
    adapter_client: TestClient, auth_headers: dict[str, str], clean_db: None
) -> None:
    address = f"platform-{_uid()}@example.test"
    _a, binding = _email_agent(adapter_client, auth_headers, address)
    body = {"kind": "email", "address": address, "ttl_s": 60}

    assert adapter_client.post("/channels/token", json=body).status_code == 401
    platform = adapter_client.post("/channels/token", json=body, headers=auth_headers)
    assert platform.status_code == 200, platform.text
    assert platform.json()["token"].startswith("chn.")
    both = adapter_client.post(
        "/channels/token",
        json=body,
        headers={**auth_headers, **_adp(_adapter_token([binding]))},
    )
    assert both.status_code == 401, both.text


# --- 2. served filter ---------------------------------------------------------


def test_list_returns_only_served_approvals_and_unserved_resolve_is_404(
    adapter_client: TestClient, auth_headers: dict[str, str], clean_db: None
) -> None:
    served = _routed_agent(adapter_client, auth_headers, approvers={"users": [SENDER]})
    unserved = _routed_agent(adapter_client, auth_headers, approvers={"users": [SENDER]})
    mine = _approval(adapter_client, auth_headers, served)
    theirs = _approval(adapter_client, auth_headers, unserved)
    token = _adapter_token([served["binding_id"]])

    listed = adapter_client.get("/approvals", headers=_adp(token))
    assert listed.status_code == 200, listed.text
    assert {row["id"] for row in listed.json()} == {mine["id"]}

    # The platform key still sees both.
    everything = adapter_client.get("/approvals", headers=auth_headers)
    assert {mine["id"], theirs["id"]} <= {row["id"] for row in everything.json()}

    refused = adapter_client.post(
        f"/approvals/{theirs['id']}/resolve",
        json={"decision": "approved"},
        headers=_adp(token, SENDER),
    )
    assert refused.status_code == 404, refused.text
    assert refused.json()["detail"] == NOT_FOUND
    assert _status(adapter_client, auth_headers, theirs["id"]) == "pending"
    assert _audit(adapter_client, auth_headers, theirs["id"]) == []


# --- 3. expiry ----------------------------------------------------------------


def test_expired_adapter_credential_is_401_everywhere(
    adapter_client: TestClient, auth_headers: dict[str, str], clean_db: None
) -> None:
    served = _routed_agent(adapter_client, auth_headers, approvers={"users": [SENDER]})
    approval = _approval(adapter_client, auth_headers, served)
    expired = _adapter_token([served["binding_id"]], exp=int(time.time()) - 1)

    resolve = adapter_client.post(
        f"/approvals/{approval['id']}/resolve",
        json={"decision": "approved"},
        headers=_adp(expired, SENDER),
    )
    assert resolve.status_code == 401, resolve.text
    assert adapter_client.get("/approvals", headers=_adp(expired)).status_code == 401
    mint = adapter_client.post(
        "/channels/token",
        json={"kind": "slack", "address": served["channel"], "ttl_s": 60},
        headers=_adp(expired),
    )
    assert mint.status_code == 401, mint.text
    assert _status(adapter_client, auth_headers, approval["id"]) == "pending"


# --- 4. rotation --------------------------------------------------------------


def test_rotation_before_expiry_issues_a_working_token(
    adapter_client: TestClient, auth_headers: dict[str, str], clean_db: None
) -> None:
    served = _routed_agent(adapter_client, auth_headers, approvers={"users": [SENDER]})
    approval = _approval(adapter_client, auth_headers, served)
    token = _adapter_token([served["binding_id"]])

    rotated = adapter_client.post(
        "/approvals/principals/adapter/rotate", json={"ttl_s": 3600}, headers=_adp(token)
    )
    assert rotated.status_code == 201, rotated.text
    out = rotated.json()
    assert out["subject"] == ADAPTER_SUBJECT
    assert out["kind"] == "adapter"
    assert [str(b) for b in out["binding_ids"]] == [served["binding_id"]]
    assert out["token"].startswith("adp.")

    listed = adapter_client.get("/approvals", headers=_adp(out["token"]))
    assert listed.status_code == 200, listed.text
    assert [row["id"] for row in listed.json()] == [approval["id"]]


def test_rotation_refuses_expired_token_and_platform_key(
    adapter_client: TestClient, auth_headers: dict[str, str], clean_db: None
) -> None:
    served = _routed_agent(adapter_client, auth_headers, approvers={"users": [SENDER]})
    expired = _adapter_token([served["binding_id"]], exp=int(time.time()) - 1)

    stale = adapter_client.post(
        "/approvals/principals/adapter/rotate", json={}, headers=_adp(expired)
    )
    assert stale.status_code == 401, stale.text
    platform = adapter_client.post(
        "/approvals/principals/adapter/rotate", json={}, headers=auth_headers
    )
    assert platform.status_code == 401, platform.text


# --- 5. resolution and audit --------------------------------------------------


def test_adapter_resolves_for_listed_sender_and_audits_adapter_subject(
    adapter_client: TestClient,
    auth_headers: dict[str, str],
    clean_db: None,
    valkey: redis.Redis,
) -> None:
    served = _routed_agent(adapter_client, auth_headers, approvers={"users": [SENDER]})
    approval = _approval(adapter_client, auth_headers, served)
    token = _adapter_token([served["binding_id"]])

    accepted = adapter_client.post(
        f"/approvals/{approval['id']}/resolve",
        json={"decision": "approved"},
        headers=_adp(token, SENDER),
    )
    assert accepted.status_code == 200, accepted.text
    assert accepted.json()["resolved_by"] == SENDER

    audit = _audit(adapter_client, auth_headers, approval["id"])
    assert len(audit) == 1
    assert audit[0]["principal_kind"] == "adapter"
    assert audit[0]["actor"] == SENDER
    assert audit[0]["principal_subject"] == ADAPTER_SUBJECT
    assert audit[0]["actor_channel"] is None
    assert audit[0]["authenticated"] is True


def test_adapter_sender_not_in_explicit_list_is_403_with_denied_audit(
    adapter_client: TestClient, auth_headers: dict[str, str], clean_db: None
) -> None:
    served = _routed_agent(adapter_client, auth_headers, approvers={"users": [SENDER]})
    approval = _approval(adapter_client, auth_headers, served)
    token = _adapter_token([served["binding_id"]])

    denied = adapter_client.post(
        f"/approvals/{approval['id']}/resolve",
        json={"decision": "approved"},
        headers=_adp(token, OTHER),
    )
    assert denied.status_code == 403, denied.text
    assert _status(adapter_client, auth_headers, approval["id"]) == "pending"
    audit = _audit(adapter_client, auth_headers, approval["id"])
    assert len(audit) == 1
    assert audit[0]["action"] == "denied"
    assert audit[0]["authorized"] is False
    assert audit[0]["principal_kind"] == "adapter"
    assert audit[0]["actor"] == OTHER
    assert audit[0]["principal_subject"] == ADAPTER_SUBJECT


def test_adapter_on_channel_membership_route_is_403_eligibility(
    adapter_client: TestClient, auth_headers: dict[str, str], clean_db: None
) -> None:
    served = _routed_agent(adapter_client, auth_headers, approvers=None)
    approval = _approval(adapter_client, auth_headers, served)
    token = _adapter_token([served["binding_id"]])

    denied = adapter_client.post(
        f"/approvals/{approval['id']}/resolve",
        json={"decision": "approved"},
        headers=_adp(token, SENDER),
    )
    assert denied.status_code == 403, denied.text
    assert "explicit" in denied.json()["detail"].lower()
    assert _status(adapter_client, auth_headers, approval["id"]) == "pending"


def test_adapter_resolve_without_actor_header_is_401(
    adapter_client: TestClient, auth_headers: dict[str, str], clean_db: None
) -> None:
    served = _routed_agent(adapter_client, auth_headers, approvers={"users": [SENDER]})
    approval = _approval(adapter_client, auth_headers, served)
    token = _adapter_token([served["binding_id"]])

    for headers in (_adp(token), _adp(token, "  ")):
        response = adapter_client.post(
            f"/approvals/{approval['id']}/resolve",
            json={"decision": "approved"},
            headers=headers,
        )
        assert response.status_code == 401, response.text
    assert _status(adapter_client, auth_headers, approval["id"]) == "pending"
    assert _audit(adapter_client, auth_headers, approval["id"]) == []


# --- 6. other routes stay closed ----------------------------------------------


def test_adapter_token_is_refused_on_other_routes(
    adapter_client: TestClient, auth_headers: dict[str, str], clean_db: None
) -> None:
    served = _routed_agent(adapter_client, auth_headers, approvers={"users": [SENDER]})
    approval = _approval(adapter_client, auth_headers, served)
    address = f"closed-{_uid()}@example.test"
    _a, mail_binding = _email_agent(adapter_client, auth_headers, address)
    token = _adapter_token([served["binding_id"], mail_binding])

    one = adapter_client.get(f"/approvals/{approval['id']}", headers=_adp(token))
    assert one.status_code == 401, one.text
    for headers in (_adp(token), {"X-API-Key": token}):
        turn = adapter_client.post("/channels/turns", json=_turn(address), headers=headers)
        assert turn.status_code == 401, turn.text


# --- 7. issuance --------------------------------------------------------------


def test_issuance_requires_platform_key_and_validates_body(
    adapter_client: TestClient, auth_headers: dict[str, str], clean_db: None
) -> None:
    served = _routed_agent(adapter_client, auth_headers, approvers={"users": [SENDER]})
    body = {"subject": ADAPTER_SUBJECT, "binding_ids": [served["binding_id"]]}

    assert adapter_client.post("/approvals/principals/adapter", json=body).status_code == 401

    issued = adapter_client.post("/approvals/principals/adapter", json=body, headers=auth_headers)
    assert issued.status_code == 201, issued.text
    assert issued.headers.get("cache-control") == "no-store"
    out = issued.json()
    assert out["kind"] == "adapter"
    assert out["subject"] == ADAPTER_SUBJECT
    assert sorted(out["scopes"]) == ["approvals:read", "approvals:resolve", "channels:token"]
    listed = adapter_client.get("/approvals", headers=_adp(out["token"]))
    assert listed.status_code == 200, listed.text

    unknown = adapter_client.post(
        "/approvals/principals/adapter",
        json={"subject": ADAPTER_SUBJECT, "binding_ids": [str(uuid.uuid4())]},
        headers=auth_headers,
    )
    assert unknown.status_code == 422, unknown.text

    too_long = adapter_client.post(
        "/approvals/principals/adapter", json={**body, "ttl_s": 604801}, headers=auth_headers
    )
    assert too_long.status_code == 422, too_long.text
