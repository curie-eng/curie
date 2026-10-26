"""Current-check requests use a real queued invocation and fresh owned TLS facts.

@spec HTTP-INVOCATION-EXECUTION-8 HTTP-INVOCATION-EXECUTION-16.
"""

from __future__ import annotations

import copy
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
    provider: SimpleNamespace, valkey: redis.Redis, stream: str,
) -> Iterator[SimpleNamespace]:
    """@spec HTTP-INVOCATION-EXECUTION-8/16: use ingress, never a test sealing oracle."""
    scope = membership._scope(provider.url)
    module = membership._runtime_api()
    identity_client = HttpIdentityClient(tls_context=provider.tls)
    try:
        with TestClient(create_app(
            http_identity_runtime=membership._runtime(module, scope),
            http_identity_client=identity_client,
        )) as client:
            credential = membership._credential(client, scope)
            accepted = membership._post(client, credential, *membership._submission(scope))
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


def test_current_check_repeats_fresh_group_authority_without_reenqueuing(
    clean_db: None, valkey: redis.Redis, runs_stream: str,
    synthetic_provider: SimpleNamespace,
) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-8/16: prior continue cannot cache a group grant."""
    provider = synthetic_provider
    with queued_invocation(provider, valkey, runs_stream) as invocation:
        before = delivery_state(valkey, runs_stream, invocation.scope["binding"])
        first = request_for(invocation)
        assert_response(
            post_check(invocation.client, first), first,
            decision="continue", reason="current_authority",
        )
        assert len(provider.requests) == 2
        assert provider.requests[1]["request_id"] != provider.requests[0]["request_id"]
        assert delivery_state(valkey, runs_stream, invocation.scope["binding"]) == before

        provider.member = False
        second = request_for(invocation, stage="start")
        assert second["request_id"] != first["request_id"]
        assert_response(
            post_check(invocation.client, second), second,
            decision="refuse", reason="caller_not_allowed",
        )
        assert len(provider.requests) == 3
        assert provider.requests[2]["request_id"] != provider.requests[1]["request_id"]
        assert provider.responses[2]["membership"]["complete"] is True
        assert provider.responses[2]["membership"]["groups"][0]["id"] != membership.GROUP
        assert delivery_state(valkey, runs_stream, invocation.scope["binding"]) == before


@pytest.mark.parametrize("mismatch", ["capsule", "payload", "scope", "event"])
def test_current_check_rejects_unbound_context_inputs_before_provider_or_delivery(
    mismatch: str, clean_db: None, valkey: redis.Redis, runs_stream: str,
    synthetic_provider: SimpleNamespace,
) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-8/16: valid outer request cannot replace signed scope."""
    provider = synthetic_provider
    with queued_invocation(provider, valkey, runs_stream) as invocation:
        request = copy.deepcopy(request_for(invocation))
        if mismatch == "capsule":
            request["context"] = "synthetic-invalid-context"
        elif mismatch == "payload":
            request["payload_sha256"] = "0" * 64
            assert request["payload_sha256"] != invocation.context.payload_sha256
        elif mismatch == "scope":
            request["scope"]["binding_id"] = str(uuid.uuid4())
        else:
            request["event_id"] = "synthetic-unrelated-event"
        before = delivery_state(valkey, runs_stream, invocation.scope["binding"])
        assert_response(
            post_check(invocation.client, request), request,
            decision="refuse", reason="invalid_context",
        )
        assert len(provider.requests) == 1
        assert delivery_state(valkey, runs_stream, invocation.scope["binding"]) == before


@pytest.mark.parametrize("credential", ["missing", "wrong", "platform-only"])
def test_current_check_requires_the_existing_worker_credential(
    credential: str, clean_db: None, valkey: redis.Redis, runs_stream: str,
    synthetic_provider: SimpleNamespace,
) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-8/16: a platform credential cannot ask as a worker."""
    provider = synthetic_provider
    with queued_invocation(provider, valkey, runs_stream) as invocation:
        headers = {}
        if credential == "wrong":
            headers["X-Curie-Worker-Token"] = "synthetic-wrong-worker-token"
        elif credential == "platform-only":
            headers["X-API-Key"] = get_settings().api_key
        before = delivery_state(valkey, runs_stream, invocation.scope["binding"])
        response = invocation.client.post(PATH, json=request_for(invocation), headers=headers)
        assert response.status_code == 401, response.text
        assert response.headers["cache-control"] == "no-store"
        assert len(provider.requests) == 1
        assert delivery_state(valkey, runs_stream, invocation.scope["binding"]) == before


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
