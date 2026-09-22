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
import json
import time
import uuid
from collections.abc import Iterator
from typing import Any

import pytest
import redis
from curie_api import adapter_principal, approval_principal, channel_token, sandbox_token
from curie_api.adapter_principal import _b64url, _signature
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


def _channel_bindings(agent_id: str) -> list[dict[str, str]]:
    async def run() -> list[dict[str, str]]:
        engine = create_async_engine(get_settings().database_url)
        try:
            async with engine.connect() as conn:
                result = await conn.execute(
                    sql_text("SELECT id, address FROM curie.agent_channels WHERE agent_id = :aid"),
                    {"aid": agent_id},
                )
                return [{"id": str(row.id), "address": row.address} for row in result]
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


# --- 8. one agent, two routes: the served pair conjunct ------------------------


def test_one_agent_two_routes_adapter_serves_only_its_own_binding(
    adapter_client: TestClient, auth_headers: dict[str, str], clean_db: None
) -> None:
    """Grok finding 1: the served predicate is a (kind, address) match on the
    SAME agent, not merely a shared agent id. One agent, two channel bindings,
    two routes each resolving to a different binding; a token naming only
    binding A must not see or resolve the approval routed through binding B."""

    channel_a = f"C0ADPA{_uid().upper()}"
    channel_b = f"C0ADPB{_uid().upper()}"
    route_a, route_b = f"route-a-{_uid()}", f"route-b-{_uid()}"
    created = adapter_client.post(
        "/agents",
        json={
            "name": f"adapter-two-route-{_uid()}",
            "channel": {"kind": "slack", "address": channel_a},
            "approval_routes": {
                route_a: {
                    "resolution": {"kind": "slack", "address": channel_a},
                    "approvers": {"users": [SENDER]},
                },
                route_b: {
                    "resolution": {"kind": "slack", "address": channel_b},
                    "approvers": {"users": [SENDER]},
                },
            },
        },
        headers=auth_headers,
    )
    assert created.status_code == 201, created.text
    agent_id = str(created.json()["id"])
    added = adapter_client.post(
        f"/agents/{agent_id}/channels",
        json={"kind": "slack", "address": channel_b},
        headers=auth_headers,
    )
    assert added.status_code == 201, added.text

    rows = _channel_bindings(agent_id)
    binding_by_address = {row["address"]: row["id"] for row in rows}
    binding_a = binding_by_address[channel_a]

    routed_a = {
        "agent_id": agent_id,
        "binding_id": binding_a,
        "route": route_a,
        "channel": channel_a,
    }
    routed_b = {
        "agent_id": agent_id,
        "binding_id": binding_by_address[channel_b],
        "route": route_b,
        "channel": channel_b,
    }
    approval_a = _approval(adapter_client, auth_headers, routed_a)
    approval_b = _approval(adapter_client, auth_headers, routed_b)
    token = _adapter_token([binding_a])

    listed = adapter_client.get("/approvals", headers=_adp(token))
    assert listed.status_code == 200, listed.text
    assert {row["id"] for row in listed.json()} == {approval_a["id"]}

    refused = adapter_client.post(
        f"/approvals/{approval_b['id']}/resolve",
        json={"decision": "approved"},
        headers=_adp(token, SENDER),
    )
    assert refused.status_code == 404, refused.text
    assert refused.json()["detail"] == NOT_FOUND
    assert _status(adapter_client, auth_headers, approval_b["id"]) == "pending"
    assert _audit(adapter_client, auth_headers, approval_b["id"]) == []

    allowed = adapter_client.post(
        f"/approvals/{approval_a['id']}/resolve",
        json={"decision": "approved"},
        headers=_adp(token, SENDER),
    )
    assert allowed.status_code == 200, allowed.text


# --- 9. rotate with an ambiguous credential ------------------------------------


def test_rotate_refuses_adapter_token_plus_platform_key(
    adapter_client: TestClient, auth_headers: dict[str, str], clean_db: None
) -> None:
    """Grok finding 4: rotate is documented as the adapter credential ALONE;
    presenting the platform key alongside a live adp token must fail closed,
    the same as every other resolver-credential pair, not silently succeed."""

    served = _routed_agent(adapter_client, auth_headers, approvers={"users": [SENDER]})
    token = _adapter_token([served["binding_id"]])

    both = adapter_client.post(
        "/approvals/principals/adapter/rotate",
        json={},
        headers={**auth_headers, **_adp(token)},
    )
    assert both.status_code == 401, both.text


# --- 10. actor header normalization --------------------------------------------


def test_resolve_strips_whitespace_around_a_listed_actor(
    adapter_client: TestClient, auth_headers: dict[str, str], clean_db: None
) -> None:
    """Grok finding 5: the actor header is compared to the explicit user list
    after stripping, and the audit row records the stripped value, not the
    raw header with its surrounding whitespace."""

    served = _routed_agent(adapter_client, auth_headers, approvers={"users": [SENDER]})
    approval = _approval(adapter_client, auth_headers, served)
    token = _adapter_token([served["binding_id"]])

    accepted = adapter_client.post(
        f"/approvals/{approval['id']}/resolve",
        json={"decision": "approved"},
        headers=_adp(token, f"  {SENDER}  "),
    )
    assert accepted.status_code == 200, accepted.text
    assert accepted.json()["resolved_by"] == SENDER

    audit = _audit(adapter_client, auth_headers, approval["id"])
    assert audit[-1]["actor"] == SENDER


# --- 11. hand-signed cross-prefix / malformed claim rejection ------------------


def test_verify_refuses_other_prefixes_and_malformed_claims() -> None:
    """Grok finding 7: cross-verify the four prefixes both directions, plus
    extra-key and unsorted-bindings shapes that a loosened check might admit."""

    key = "unit-signing-key"
    binding = str(uuid.uuid4())
    binding2 = str(uuid.uuid4())
    now = int(time.time())

    # A chn-shaped and an sbx-shaped token never verify as adapter.
    chn_token = channel_token.mint(
        key,
        channel_id=binding,
        generation=1,
        scope=channel_token.CHANNEL_ENQUEUE_SCOPE,
        exp=now + 60,
    )
    assert adapter_principal.verify(chn_token, key, scope="approvals:read", now=now) is None
    sbx_token = sandbox_token.mint(key, agent="a", scope="state", exp=now + 60)
    assert adapter_principal.verify(sbx_token, key, scope="approvals:read", now=now) is None

    # An hmac-approval-principal ("apr") shaped token never verifies as adapter.
    apr_payload = json.dumps(
        {"sub": "op", "kind": "operator", "exp": now + 60}, separators=(",", ":"), sort_keys=True
    ).encode()
    apr_signing_input = f"apr.{_b64url(apr_payload)}"
    apr_token = f"{apr_signing_input}.{_signature(key, apr_signing_input)}"
    assert adapter_principal.verify(apr_token, key, scope="approvals:read", now=now) is None

    # An adp token is never accepted where an approval-principal is expected.
    adp_token = adapter_principal.mint(key, subject="a", bindings=[binding], exp=now + 60)
    assert (
        approval_principal.verify_claims(adp_token, key, scope=approval_principal.APPROVE_SCOPE)
        is None
    )

    # Extra claim key.
    with_extra = json.dumps(
        {
            "sub": "a",
            "kind": "adapter",
            "bindings": [binding],
            "scopes": list(adapter_principal.SCOPES),
            "exp": now + 60,
            "extra": "nope",
        },
        separators=(",", ":"),
        sort_keys=True,
    ).encode()
    signing_input = f"adp.{_b64url(with_extra)}"
    extra_token = f"{signing_input}.{_signature(key, signing_input)}"
    assert adapter_principal.verify(extra_token, key, scope="approvals:read", now=now) is None

    # Unsorted bindings list.
    unsorted = sorted([binding, binding2], reverse=True)
    assert unsorted[0] != sorted(unsorted)[0] or unsorted != sorted(unsorted)
    with_unsorted = json.dumps(
        {
            "sub": "a",
            "kind": "adapter",
            "bindings": unsorted,
            "scopes": list(adapter_principal.SCOPES),
            "exp": now + 60,
        },
        separators=(",", ":"),
        sort_keys=True,
    ).encode()
    signing_input = f"adp.{_b64url(with_unsorted)}"
    unsorted_token = f"{signing_input}.{_signature(key, signing_input)}"
    assert adapter_principal.verify(unsorted_token, key, scope="approvals:read", now=now) is None


def test_adapter_token_on_approval_principal_header_is_401(
    adapter_client: TestClient, auth_headers: dict[str, str], clean_db: None
) -> None:
    """Grok finding 7: an adp token presented as `X-Curie-Approval-Principal`
    (the wrong header) must never verify; it should be refused as an
    unauthenticated/invalid principal, not silently treated as some kind."""

    served = _routed_agent(adapter_client, auth_headers, approvers={"users": [SENDER]})
    approval = _approval(adapter_client, auth_headers, served)
    token = _adapter_token([served["binding_id"]])

    response = adapter_client.post(
        f"/approvals/{approval['id']}/resolve",
        json={"decision": "approved"},
        headers={"X-Curie-Approval-Principal": token},
    )
    assert response.status_code == 401, response.text
    assert _status(adapter_client, auth_headers, approval["id"]) == "pending"


# --- 12. HTTP-issued and rotated tokens drive the real behaviors --------------


def test_http_issued_and_rotated_tokens_drive_mint_resolve_and_audit(
    adapter_client: TestClient, auth_headers: dict[str, str], clean_db: None
) -> None:
    """Grok finding 6: chn-mint, resolve, and audit driven by the token that
    came back from POST /approvals/principals/adapter and from rotate, not an
    in-process `mint()` call."""

    address = f"httpissued-{_uid()}@example.test"
    _agent, binding = _email_agent(adapter_client, auth_headers, address)
    issued = adapter_client.post(
        "/approvals/principals/adapter",
        json={"subject": ADAPTER_SUBJECT, "binding_ids": [binding]},
        headers=auth_headers,
    )
    assert issued.status_code == 201, issued.text
    issued_token = issued.json()["token"]

    mint = adapter_client.post(
        "/channels/token",
        json={"kind": "email", "address": address, "ttl_s": 3600},
        headers=_adp(issued_token),
    )
    assert mint.status_code == 200, mint.text
    assert mint.json()["token"].startswith("chn.")

    served = _routed_agent(adapter_client, auth_headers, approvers={"users": [SENDER]})
    issued2 = adapter_client.post(
        "/approvals/principals/adapter",
        json={"subject": ADAPTER_SUBJECT, "binding_ids": [served["binding_id"]]},
        headers=auth_headers,
    )
    assert issued2.status_code == 201, issued2.text
    approval = _approval(adapter_client, auth_headers, served)

    rotated = adapter_client.post(
        "/approvals/principals/adapter/rotate",
        json={"ttl_s": 3600},
        headers=_adp(issued2.json()["token"]),
    )
    assert rotated.status_code == 201, rotated.text
    rotated_token = rotated.json()["token"]

    resolved = adapter_client.post(
        f"/approvals/{approval['id']}/resolve",
        json={"decision": "approved"},
        headers=_adp(rotated_token, SENDER),
    )
    assert resolved.status_code == 200, resolved.text

    audit = _audit(adapter_client, auth_headers, approval["id"])
    assert audit[-1]["principal_kind"] == "adapter"
    assert audit[-1]["actor"] == SENDER
    assert audit[-1]["principal_subject"] == ADAPTER_SUBJECT


# --- 13. adapter + other resolver credential pairs -----------------------------


def test_resolve_refuses_adapter_plus_operator_and_adapter_plus_console(
    adapter_client: TestClient, auth_headers: dict[str, str], clean_db: None
) -> None:
    """Grok finding 9: adp plus an `apr` operator header, and adp plus a
    console session cookie, must both fail closed as ambiguous, with no
    state change -- not just the operator+cookie pair already covered."""

    served = _routed_agent(adapter_client, auth_headers, approvers={"users": [SENDER]})
    approval = _approval(adapter_client, auth_headers, served)
    token = _adapter_token([served["binding_id"]])

    operator_issue = adapter_client.post(
        "/approvals/principals/operator",
        json={"subject": SENDER},
        headers=auth_headers,
    )
    assert operator_issue.status_code == 201, operator_issue.text
    operator_token = operator_issue.json()["token"]

    with_operator = adapter_client.post(
        f"/approvals/{approval['id']}/resolve",
        json={"decision": "approved"},
        headers={
            **_adp(token, SENDER),
            "X-Curie-Approval-Principal": operator_token,
        },
    )
    assert with_operator.status_code == 401, with_operator.text
    assert "ambiguous" in with_operator.json()["detail"].lower()

    with_cookie = adapter_client.post(
        f"/approvals/{approval['id']}/resolve",
        json={"decision": "approved"},
        headers=_adp(token, SENDER),
        cookies={"curie_console_session": "whatever-session-token"},
    )
    assert with_cookie.status_code == 401, with_cookie.text
    assert "ambiguous" in with_cookie.json()["detail"].lower()

    assert _status(adapter_client, auth_headers, approval["id"]) == "pending"
    assert _audit(adapter_client, auth_headers, approval["id"]) == []
