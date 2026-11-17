"""Current-check requests use a real queued invocation and fresh owned TLS facts.

@spec HTTP-INVOCATION-EXECUTION-8 HTTP-INVOCATION-EXECUTION-16.
"""

from __future__ import annotations

import hashlib
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from types import SimpleNamespace
from typing import Any

import pytest
import redis
from curie_api.config import get_settings
from curie_api.identity.http_transport import HttpIdentityClient
from curie_api.main import create_app
from curie_internal import identity_context as codec
from fastapi.testclient import TestClient

from apps.api.tests import test_http_invocation_membership as membership
from apps.api.tests.test_http_invocation_membership import synthetic_provider as synthetic_provider

PATH = "/v1/internal/identity/turn-authority/check"


@contextmanager
def queued_invocation(
    provider: SimpleNamespace, valkey: redis.Redis, stream: str, *, grid: bool = False,
    trusted: tuple[Any, ...] = (), slack: Any = None,
) -> Iterator[SimpleNamespace]:
    """@spec HTTP-INVOCATION-EXECUTION-8/16/21/24: use ingress, never a test sealing oracle."""
    scope = membership._scope(provider.url, grid=grid)
    scope["trusted"] = trusted
    module = membership._runtime_api()
    identity_client = HttpIdentityClient(tls_context=provider.tls)
    try:
        with TestClient(create_app(
            http_identity_runtime=membership._runtime(module, scope),
            http_identity_client=identity_client,
        )) as client:
            credential = membership._credential(client, scope)
            if slack is not None:
                submission = membership._submission(scope, slack=slack)
            elif grid:
                submission = membership._submission(scope, slack=membership._grid_evidence())
            else:
                submission = membership._submission(scope)
            accepted = membership._post(client, credential, *submission)
            assert accepted.status_code == 200, accepted.text
            entries = valkey.xrange(stream)
            assert len(entries) == 1
            fields = entries[0][1]
            capsule, payload = fields["curie_identity_context_v1"], fields["payload"]
            ring = codec.build_identity_keyring(
                "context", (("synthetic-context", 1, scope["context_key"]),),
            )
            context = codec.open_invocation_context(capsule, ring)
            assert isinstance(context, codec.InvocationContextV1)
            assert context.payload_sha256 == hashlib.sha256(payload.encode()).hexdigest()
            assert len(provider.requests) == 1
            yield SimpleNamespace(
                client=client, scope=scope, capsule=capsule, context=context,
                payload=payload, entries=entries,
            )
        assert identity_client.close_status().state == "closed"
    finally:
        keys = membership._keys(valkey, scope["binding"])
        if keys:
            valkey.delete(*keys)


def request_for(invocation: SimpleNamespace, *, stage: str = "prepare") -> dict[str, Any]:
    """@spec HTTP-INVOCATION-EXECUTION-8/16: equality expectations are not authority."""
    return {
        "schema_version": 1,
        "request_id": str(uuid.uuid4()),
        "stage": stage,
        "context": invocation.capsule,
        "payload_sha256": hashlib.sha256(invocation.payload.encode()).hexdigest(),
        "event_id": invocation.context.event_id,
        "scope": {
            "tenant_id": invocation.context.scope.tenant_id,
            "agent_id": invocation.context.scope.agent_id,
            "binding_id": invocation.context.scope.binding_id,
            "binding_generation": invocation.context.scope.binding_generation,
        },
        "approval_id": None,
    }


def delivery_state(client: redis.Redis, stream: str, binding: str) -> tuple[Any, Any]:
    """@spec HTTP-INVOCATION-EXECUTION-8/16: observe all binding claims and quota state."""
    return (
        {key: client.dump(key) for key in membership._keys(client, binding)},
        client.xrange(stream),
    )


def post_check(client: TestClient, request: dict[str, Any]) -> Any:
    """@spec HTTP-INVOCATION-EXECUTION-16: exercise the existing worker credential."""
    return client.post(
        PATH, json=request,
        headers={"X-Curie-Worker-Token": get_settings().internal_worker_token},
    )


def assert_response(
    response: Any, request: dict[str, Any], *, decision: str, reason: str,
) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-8/16: no capsule, payload or signing oracle in output."""
    assert response.status_code == 200, response.text
    assert response.headers["cache-control"] == "no-store"
    assert response.json() == {
        "schema_version": 1,
        "request_id": request["request_id"],
        "stage": request["stage"],
        "event_id": request["event_id"],
        "context_sha256": hashlib.sha256(request["context"].encode("ascii")).hexdigest(),
        "scope": request["scope"],
        "decision": decision,
        "reason": reason,
    }


def test_initial_current_check_refuses_unsupported_approval_resume_before_lookup(
    clean_db: None, valkey: redis.Redis, runs_stream: str,
    synthetic_provider: SimpleNamespace,
) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-8/16: worker auth cannot manufacture approval authority."""
    provider = synthetic_provider
    with queued_invocation(provider, valkey, runs_stream) as invocation:
        request = request_for(invocation, stage="claim_or_resume")
        request["approval_id"] = str(uuid.uuid4())
        before = delivery_state(valkey, runs_stream, invocation.scope["binding"])
        assert_response(
            post_check(invocation.client, request), request,
            decision="refuse", reason="unsupported_resume",
        )
        assert len(provider.requests) == 1
        assert delivery_state(valkey, runs_stream, invocation.scope["binding"]) == before


_NESTED = b"[" * 4096 + b"0" + b"]" * 4096


def _assert_fixed_refusal_like_malformed_json(path: str, detail: str) -> None:
    """A deeply nested body gets the same fixed 422 as ordinary malformed JSON."""
    headers = {"Content-Type": "application/json"}
    with TestClient(create_app(), raise_server_exceptions=False) as client:
        malformed = client.post(path, content=b"{", headers=headers)
        deep = client.post(path, content=_NESTED, headers=headers)
    assert (malformed.status_code, malformed.json()) == (422, {"detail": detail})
    assert (deep.status_code, deep.json()) == (422, {"detail": detail})
    assert deep.headers["cache-control"] == malformed.headers["cache-control"] == "no-store"


def test_deeply_nested_unauthenticated_turn_authority_body_gets_the_fixed_refusal() -> None:
    """@spec HTTP-INVOCATION-EXECUTION-16: depth must not become a 500 before authentication."""
    _assert_fixed_refusal_like_malformed_json(PATH, "invalid_turn_authority_request")


@pytest.mark.parametrize(
    ("path", "detail"),
    [
        ("/v1/internal/identity/approval-grant/check", "invalid_approval_grant_request"),
        (
            "/v1/internal/identity/binding-classification/check",
            "invalid_binding_classification_request",
        ),
    ],
)
def test_deeply_nested_sibling_route_bodies_keep_their_fixed_refusals(
    path: str, detail: str,
) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-16: the shared wrapper keeps each route's refusal."""
    _assert_fixed_refusal_like_malformed_json(path, detail)
