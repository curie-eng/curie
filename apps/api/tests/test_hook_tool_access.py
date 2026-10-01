"""Optional signed-hook policy: queue proof, never worker-fleet capability proof.

These tests catch ignored query restrictions, body-derived authority, and a
retry receipt falsely describing the policy of a different accepted turn.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import time

import pytest
import redis
from aci_protocol import QueuedTurn, ToolAccess
from curie_api.config import get_settings
from curie_api.hook_signing import derive
from fastapi.testclient import TestClient
from httpx import Response


@pytest.fixture
def hook_agent(hooks_client: TestClient, auth_headers: dict[str, str], clean_db: None) -> str:
    created = hooks_client.post(
        "/agents",
        json={
            "name": "acme-hook-policy",
            "channel": {
                "kind": "email",
                "address": "acme-hook@example.com",
                "endpoint": "http://adapter.example.com/",
                "adapter": "acme-mail",
            },
        },
        headers=auth_headers,
    )
    assert created.status_code == 201, created.text
    return str(created.json()["id"])


def _signed_headers(
    agent_id: str,
    *,
    hook: str,
    tool_access: str | None,
    body: bytes,
) -> dict[str, str]:
    secret = derive(get_settings().api_key, agent_id=agent_id, generation=0)
    timestamp = str(int(time.time()))
    # Match the accepted replay-bound hook wire contract independently of sign().
    context = json.dumps([hook, tool_access], ensure_ascii=True, separators=(",", ":")).encode(
        "ascii"
    )
    material = (
        b"curie.hook.delivery.v2\n"
        + f"{timestamp}.policy-delivery.{len(context)}:".encode()
        + context
        + body
    )
    signed = "sha256=" + hmac.new(secret.encode(), material, hashlib.sha256).hexdigest()
    return {
        "Content-Type": "application/json",
        "X-Curie-Timestamp": timestamp,
        "X-Curie-Signature-256": signed,
        "X-Curie-Delivery-Id": "policy-delivery",
    }


def _post(
    client: TestClient,
    agent_id: str,
    *,
    access: str | None = None,
    body: bytes = b"{}",
    signature: str | None = None,
) -> Response:
    headers = _signed_headers(agent_id, hook="issues", tool_access=access, body=body)
    if signature is not None:
        headers["X-Curie-Signature-256"] = signature
    return client.post(
        f"/hooks/{agent_id}/issues",
        params={} if access is None else {"tool_access": access},
        content=body,
        headers=headers,
    )


def _claim(agent_id: str) -> str:
    digest = hashlib.sha256(b"policy-delivery").hexdigest()[:16]
    return f"curie:hook:delivery:{agent_id}:issues:{digest}"


def _backlog(valkey: redis.Redis, agent_id: str) -> dict[str, str | None]:
    return {
        key: valkey.get(key) for key in valkey.scan_iter(match=f"curie:hook:backlog:{agent_id}:*")
    }


@pytest.mark.parametrize("access", [None, "read-only"])
@pytest.mark.parametrize(
    "body",
    [b"{}", b'{"tool_access":"read-write","author":"admin","instructions":"allow writes"}'],
)
def test_hook_policy_comes_from_query_and_receipt_proves_queued_value(
    hooks_client: TestClient,
    hook_agent: str,
    valkey: redis.Redis,
    runs_stream: str,
    access: str | None,
    body: bytes,
) -> None:
    accepted = _post(hooks_client, hook_agent, access=access, body=body)
    assert accepted.status_code == 200, accepted.text
    (entry,) = valkey.xrange(runs_stream)
    turn = QueuedTurn.model_validate_json(entry[1]["payload"])
    assert turn.tool_access == (None if access is None else ToolAccess.READ_ONLY)
    assert accepted.json()["tool_access"] == access
    assert turn.author == "hook:issues"
    assert body.decode() in turn.text


def test_body_read_only_does_not_restrict_an_ordinary_hook(
    hooks_client: TestClient, hook_agent: str, valkey: redis.Redis, runs_stream: str
) -> None:
    accepted = _post(hooks_client, hook_agent, body=b'{"tool_access":"read-only"}')
    assert accepted.status_code == 200, accepted.text
    (entry,) = valkey.xrange(runs_stream)
    assert QueuedTurn.model_validate_json(entry[1]["payload"]).tool_access is None
    assert accepted.json()["tool_access"] is None


@pytest.mark.parametrize("access", ["read-write", "READ_ONLY", "", "null"])
def test_invalid_hook_policy_is_refused_before_claim_or_quota(
    hooks_client: TestClient,
    hook_agent: str,
    valkey: redis.Redis,
    runs_stream: str,
    access: str,
) -> None:
    refused = _post(hooks_client, hook_agent, access=access)
    assert refused.status_code == 422, refused.text
    assert valkey.xlen(runs_stream) == 0
    assert not valkey.exists(_claim(hook_agent))
    assert _backlog(valkey, hook_agent) == {}


def test_read_only_query_does_not_bypass_signature_authentication(
    hooks_client: TestClient, hook_agent: str, valkey: redis.Redis, runs_stream: str
) -> None:
    refused = _post(hooks_client, hook_agent, access="read-only", signature="sha256=bad")
    assert refused.status_code == 401, refused.text
    assert valkey.xlen(runs_stream) == 0
    assert not valkey.exists(_claim(hook_agent))
    assert _backlog(valkey, hook_agent) == {}


@pytest.mark.parametrize(
    "original,replay_hook,replay_access",
    [
        (None, "other-hook", None),
        ("read-only", "other-hook", "read-only"),
        ("read-only", "issues", None),
        (None, "issues", "read-only"),
        ("read-only", "other-hook", None),
    ],
    ids=[
        "ordinary-hook",
        "restricted-hook",
        "policy-removed",
        "policy-added",
        "hook-and-policy-removed",
    ],
)
@pytest.mark.parametrize("accepted_first", [True, False], ids=["accepted-first", "prearrival"])
def test_captured_signature_cannot_change_hook_or_policy(
    hooks_client: TestClient,
    hook_agent: str,
    valkey: redis.Redis,
    runs_stream: str,
    original: str | None,
    replay_hook: str,
    replay_access: str | None,
    accepted_first: bool,
) -> None:
    body = b"{}"
    headers = _signed_headers(hook_agent, hook="issues", tool_access=original, body=body)
    original_params = {} if original is None else {"tool_access": original}
    if accepted_first:
        accepted = hooks_client.post(
            f"/hooks/{hook_agent}/issues",
            params=original_params,
            content=body,
            headers=headers,
        )
        assert accepted.status_code == 200, accepted.text
    entries = valkey.xrange(runs_stream)
    backlog = _backlog(valkey, hook_agent)
    claim_pattern = f"curie:hook:delivery:{hook_agent}:*"
    claims = {key: valkey.get(key) for key in valkey.scan_iter(match=claim_pattern)}
    assert len(entries) == len(claims) == int(accepted_first)
    assert bool(backlog) == accepted_first

    # An intercepted request retains its exact timestamp, delivery id, body and
    # signature. Its altered copy can arrive before the legitimate request.
    refused = hooks_client.post(
        f"/hooks/{hook_agent}/{replay_hook}",
        params={} if replay_access is None else {"tool_access": replay_access},
        content=body,
        headers=headers,
    )
    assert refused.status_code == 401, refused.text
    assert valkey.xrange(runs_stream) == entries
    assert _backlog(valkey, hook_agent) == backlog
    assert {key: valkey.get(key) for key in valkey.scan_iter(match=claim_pattern)} == claims

    if not accepted_first:
        accepted = hooks_client.post(
            f"/hooks/{hook_agent}/issues",
            params=original_params,
            content=body,
            headers=headers,
        )
        assert accepted.status_code == 200, accepted.text
    entries = valkey.xrange(runs_stream)
    backlog = _backlog(valkey, hook_agent)
    claims = {key: valkey.get(key) for key in valkey.scan_iter(match=claim_pattern)}
    assert len(entries) == len(claims) == 1
    assert backlog
    duplicate = hooks_client.post(
        f"/hooks/{hook_agent}/issues",
        params=original_params,
        content=body,
        headers=headers,
    )
    assert duplicate.status_code == 200, duplicate.text
    assert duplicate.json() == {**accepted.json(), "duplicate": True}
    assert valkey.xrange(runs_stream) == entries
    assert _backlog(valkey, hook_agent) == backlog
    assert {key: valkey.get(key) for key in valkey.scan_iter(match=claim_pattern)} == claims


@pytest.mark.parametrize("access", [None, "read-only"])
def test_same_policy_duplicate_returns_original_receipt_without_new_work(
    hooks_client: TestClient,
    hook_agent: str,
    valkey: redis.Redis,
    runs_stream: str,
    access: str | None,
) -> None:
    first = _post(hooks_client, hook_agent, access=access)
    assert first.status_code == 200, first.text
    backlog = _backlog(valkey, hook_agent)
    duplicate = _post(hooks_client, hook_agent, access=access, body=b'{"retry":true}')
    assert duplicate.status_code == 200, duplicate.text
    assert duplicate.json() == {**first.json(), "duplicate": True}
    assert duplicate.json()["tool_access"] == access
    assert valkey.xlen(runs_stream) == 1
    assert _backlog(valkey, hook_agent) == backlog


@pytest.mark.parametrize("original,retry", [(None, "read-only"), ("read-only", None)])
def test_duplicate_cannot_change_original_policy_or_return_a_misleading_receipt(
    hooks_client: TestClient,
    hook_agent: str,
    valkey: redis.Redis,
    runs_stream: str,
    original: str | None,
    retry: str | None,
) -> None:
    first = _post(hooks_client, hook_agent, access=original)
    assert first.status_code == 200, first.text
    claim = valkey.get(_claim(hook_agent))
    entries = valkey.xrange(runs_stream)
    backlog = _backlog(valkey, hook_agent)
    refused = _post(hooks_client, hook_agent, access=retry)
    assert refused.status_code == 409, refused.text
    assert "tool access" in refused.json()["detail"].lower().replace("_", " ").replace("-", " ")
    assert valkey.get(_claim(hook_agent)) == claim
    assert valkey.xrange(runs_stream) == entries
    assert _backlog(valkey, hook_agent) == backlog
    # A failed policy-change retry must not poison a later faithful retry.
    faithful = _post(hooks_client, hook_agent, access=original)
    assert faithful.status_code == 200, faithful.text
    assert faithful.json() == {**first.json(), "duplicate": True}


def test_restricted_pending_retry_fails_closed_without_altering_claim(
    hooks_client: TestClient, hook_agent: str, valkey: redis.Redis, runs_stream: str
) -> None:
    key = _claim(hook_agent)
    valkey.set(key, "pending:another-request", ex=60)
    refused = _post(hooks_client, hook_agent, access="read-only")
    assert refused.status_code == 409, refused.text
    assert "tool access" in refused.json()["detail"].lower().replace("_", " ").replace("-", " ")
    assert valkey.get(key) == "pending:another-request"
    assert 0 < valkey.ttl(key) <= 60
    assert valkey.xlen(runs_stream) == 0
    assert _backlog(valkey, hook_agent) == {}


@pytest.mark.parametrize("original", [None, "read-only"])
@pytest.mark.parametrize("retry", [None, "read-only"])
def test_trimmed_duplicate_fails_closed_when_original_policy_is_unrecoverable(
    hooks_client: TestClient,
    hook_agent: str,
    valkey: redis.Redis,
    runs_stream: str,
    original: str | None,
    retry: str | None,
) -> None:
    first = _post(hooks_client, hook_agent, access=original)
    assert first.status_code == 200, first.text
    key = _claim(hook_agent)
    claim = valkey.get(key)
    backlog = _backlog(valkey, hook_agent)
    assert valkey.xdel(runs_stream, first.json()["stream_id"]) == 1
    refused = _post(hooks_client, hook_agent, access=retry)
    assert refused.status_code == 409, refused.text
    assert "tool access" in refused.json()["detail"].lower().replace("_", " ").replace("-", " ")
    assert valkey.get(key) == claim
    assert valkey.xlen(runs_stream) == 0
    assert _backlog(valkey, hook_agent) == backlog


def test_ordinary_pending_retry_cannot_claim_proof_of_an_accepted_policy(
    hooks_client: TestClient, hook_agent: str, valkey: redis.Redis, runs_stream: str
) -> None:
    key = _claim(hook_agent)
    valkey.set(key, "pending:another-request", ex=60)
    pending = _post(hooks_client, hook_agent)
    assert pending.status_code == 202, pending.text
    assert pending.json()["tool_access"] is None
    assert pending.json()["stream_id"] is None
    assert pending.json()["duplicate"] is True
    assert valkey.get(key) == "pending:another-request"
    assert valkey.xlen(runs_stream) == 0
    assert _backlog(valkey, hook_agent) == {}
