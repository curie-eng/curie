"""Real ASGI per-identity HTTP classification in one adopted tenant.

@spec HTTP-INVOCATION-EXECUTION-15 HTTP-INVOCATION-EXECUTION-22.
Synthetic provider and Slack identifiers only; no real directory is qualified.
"""

from __future__ import annotations

import importlib
import json
import uuid
from types import ModuleType, SimpleNamespace
from typing import Any

import pytest
import redis
from aci_protocol import QueuedTurn
from curie_api import agent_role_policy
from curie_api.config import get_settings
from curie_api.identity.http_transport import HttpIdentityClient
from curie_api.main import create_app
from curie_api.models import Agent, AgentChannel, ChannelIdentity
from fastapi.testclient import TestClient
from principal_adoption_support import ISSUER
from role_policy_support import change, run
from sqlalchemy import update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from test_http_invocation_membership import (
    _credential,
    _keys,
    _post,
    _runtime,
    _runtime_api,
    _scope,
    _submission,
    synthetic_provider,  # noqa: F401 - pytest fixture reused from the vertical suite.
)

CHECK = "/v1/internal/identity/binding-classification/check"


def _classification() -> ModuleType:
    """@spec HTTP-INVOCATION-EXECUTION-22: missing features fail in the test body."""
    module = importlib.import_module("curie_internal.identity_authority")
    for symbol in ("BindingClassificationRequestV1", "BindingClassificationResponseV1"):
        assert hasattr(module, symbol), f"binding classification missing {symbol}"
    return module


def _sibling(scope: dict[str, Any], *, kind: str = "slack") -> dict[str, Any]:
    """@spec HTTP-INVOCATION-EXECUTION-22: a second agent on an unregistered identity."""
    sibling: dict[str, Any] = {"address": "C" + uuid.uuid4().hex[:12].upper()}

    async def persist(factory: async_sessionmaker[AsyncSession]) -> None:
        """@spec HTTP-INVOCATION-EXECUTION-22: same tenant, no role migration."""
        tenant = uuid.UUID(scope["tenant"])
        async with factory() as session, session.begin():
            agent = Agent(tenant_id=tenant, name="synthetic-legacy-" + uuid.uuid4().hex)
            session.add(agent)
            identity = ChannelIdentity(
                tenant_id=tenant,
                provider="slack",
                name="synthetic-legacy-" + uuid.uuid4().hex,
                provider_installation_id=uuid.UUID(scope["installation"]),
                status="active",
                attributes={},
            )
            session.add(identity)
            await session.flush()
            binding = AgentChannel(
                tenant_id=tenant,
                agent_id=agent.id,
                kind=kind,
                address=sibling["address"],
                adapter=identity.name if kind == "slack" else "synthetic-mail",
                endpoint=None if kind == "slack" else "https://mail.example.test/reply",
                allowed_callers=None,
            )
            session.add(binding)
            await session.flush()
            sibling.update(
                tenant=scope["tenant"],
                agent=str(agent.id),
                binding=str(binding.id),
                adapter=binding.adapter,
                kind=kind,
                generation=binding.generation,
            )

    run(persist)
    return sibling


def _legacy_body(scope: dict[str, Any]) -> bytes:
    """@spec HTTP-INVOCATION-EXECUTION-22: an ordinary context-free channel turn."""
    return json.dumps(
        {
            "kind": scope.get("kind", "slack"),
            "address": scope["address"],
            "adapter": scope["adapter"],
            "delivery_id": "synthetic-legacy-" + uuid.uuid4().hex,
            "conversation_id": scope["address"] + ":synthetic-thread",
            "author": "U0EXAMPLE2",
            "text": "Synthetic legacy invocation",
            "reply_ref": "",
            "attachments": [],
        },
        separators=(",", ":"),
    ).encode()


def _legacy_post(client: TestClient, token: str, scope: dict[str, Any]) -> Any:
    """@spec HTTP-INVOCATION-EXECUTION-22: the channel credential, no receiver proof."""
    return client.post(
        "/channels/turns",
        content=_legacy_body(scope),
        headers={"X-API-Key": token, "Content-Type": "application/json"},
    )


def _cleanup(valkey: redis.Redis, *bindings: str) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-12: remove only this test's delivery state."""
    for binding in bindings:
        keys = _keys(valkey, binding)
        if keys:
            valkey.delete(*keys)


def test_unregistered_identity_in_adopted_tenant_keeps_the_legacy_path(
    clean_db: None,
    valkey: redis.Redis,
    runs_stream: str,
    synthetic_provider: SimpleNamespace,  # noqa: F811 - the reused fixture.
) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-15/22: one registered identity, one legacy sibling."""
    scope = _scope(synthetic_provider.url)
    sibling = _sibling(scope)
    module = _runtime_api()
    try:
        with TestClient(
            create_app(
                http_identity_runtime=_runtime(module, scope),
                http_identity_client=HttpIdentityClient(tls_context=synthetic_provider.tls),
            )
        ) as client:
            registered_token = _credential(client, scope)
            refused = _legacy_post(client, registered_token, scope)
            assert refused.status_code == 401
            assert refused.json() == {"detail": "invalid_http_receiver"}
            assert valkey.xlen(runs_stream) == 0

            legacy_token = _credential(client, sibling)
            accepted = _legacy_post(client, legacy_token, sibling)
            assert accepted.status_code == 200, accepted.text
            entries = valkey.xrange(runs_stream)
            assert len(entries) == 1
            assert set(entries[0][1]) == {"payload"}
            turn = QueuedTurn.model_validate_json(entries[0][1]["payload"])
            assert turn.reply_handle.adapter == sibling["adapter"]
            assert synthetic_provider.requests == []

            # The registered identity still queues its own HTTP turn.
            member = _post(client, registered_token, *_submission(scope))
            assert member.status_code == 200, member.text
            assert "curie_identity_context_v1" in valkey.xrange(runs_stream)[-1][1]
    finally:
        _cleanup(valkey, scope["binding"], sibling["binding"])


def test_unlisted_binding_on_a_registered_identity_refuses_instead_of_legacy(
    clean_db: None,
    valkey: redis.Redis,
    runs_stream: str,
    synthetic_provider: SimpleNamespace,  # noqa: F811 - the reused fixture.
) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-22: the identity, not binding_ids, selects HTTP."""
    scope = _scope(synthetic_provider.url)
    unlisted = _sibling(scope)

    async def share_identity(factory: async_sessionmaker[AsyncSession]) -> None:
        """@spec HTTP-INVOCATION-EXECUTION-22: rebind the sibling onto the registered name."""
        async with factory() as session, session.begin():
            await session.execute(
                update(AgentChannel)
                .where(AgentChannel.id == uuid.UUID(unlisted["binding"]))
                .values(adapter=scope["adapter"])
            )

    run(share_identity)
    unlisted["adapter"] = scope["adapter"]
    module = _runtime_api()
    try:
        with TestClient(
            create_app(
                http_identity_runtime=_runtime(module, scope),
                http_identity_client=HttpIdentityClient(tls_context=synthetic_provider.tls),
            )
        ) as client:
            token = _credential(client, unlisted)
            refused = _legacy_post(client, token, unlisted)
            assert refused.status_code == 401
            assert refused.json() == {"detail": "invalid_http_receiver"}
            assert valkey.xlen(runs_stream) == 0 and synthetic_provider.requests == []
    finally:
        _cleanup(valkey, scope["binding"], unlisted["binding"])


def test_adopted_tenant_without_registrations_keeps_context_free_turns_legacy(
    clean_db: None,
    valkey: redis.Redis,
    runs_stream: str,
    hooks_client: TestClient,
    synthetic_provider: SimpleNamespace,  # noqa: F811 - the reused fixture.
) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-15/22: no runtime registers no identity."""
    scope = _scope(synthetic_provider.url)
    sibling = _sibling(scope)
    try:
        token = _credential(hooks_client, sibling)
        accepted = _legacy_post(hooks_client, token, sibling)
        assert accepted.status_code == 200, accepted.text
        assert valkey.xlen(runs_stream) == 1 and synthetic_provider.requests == []
    finally:
        _cleanup(valkey, scope["binding"], sibling["binding"])


def _check(client: TestClient, scope: dict[str, Any], **changes: Any) -> Any:
    """@spec HTTP-INVOCATION-EXECUTION-22: the worker's classification question."""
    body = {
        "schema_version": 1,
        "request_id": str(uuid.uuid4()),
        "scope": {
            "tenant_id": scope["tenant"],
            "agent_id": scope["agent"],
            "binding_id": scope["binding"],
            "binding_generation": scope["generation"],
            **changes,
        },
    }
    response = client.post(
        CHECK, json=body,
        headers={"X-Curie-Worker-Token": get_settings().internal_worker_token},
    )
    if response.status_code == 200:
        assert response.json()["request_id"] == body["request_id"]
        assert response.json()["scope"] == body["scope"]
        assert response.headers["cache-control"] == "no-store"
    return response


def test_internal_route_classifies_by_registered_identity(
    clean_db: None,
    synthetic_provider: SimpleNamespace,  # noqa: F811 - the reused fixture.
) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-22: registered, unregistered, non-Slack and changed."""
    _classification()
    scope = _scope(synthetic_provider.url)
    sibling = _sibling(scope)
    mail = _sibling(scope, kind="email")
    module = _runtime_api()
    with TestClient(
        create_app(
            http_identity_runtime=_runtime(module, scope),
            http_identity_client=HttpIdentityClient(tls_context=synthetic_provider.tls),
        )
    ) as client:
        scope["generation"] = run_generation(scope)
        registered = _check(client, scope)
        assert registered.status_code == 200, registered.text
        assert registered.json()["http_identity_required"] is True
        for other in (sibling, mail):
            answer = _check(client, other)
            assert answer.status_code == 200, answer.text
            assert answer.json()["http_identity_required"] is False
        changed = _check(client, scope, binding_generation=scope["generation"] + 1)
        assert changed.status_code == 409
        assert changed.json() == {"detail": "binding_scope_changed"}
        foreign = _check(client, scope, agent_id=sibling["agent"])
        assert foreign.status_code == 409
        absent = _check(client, scope, binding_id=str(uuid.uuid4()))
        assert absent.status_code == 409
        wrong = client.post(
            CHECK, json={}, headers={"X-Curie-Worker-Token": "synthetic-wrong-worker-token"},
        )
        assert wrong.status_code in {401, 403}
        assert synthetic_provider.requests == []


def test_internal_route_without_runtime_classifies_every_binding_legacy(
    clean_db: None,
    hooks_client: TestClient,
    synthetic_provider: SimpleNamespace,  # noqa: F811 - the reused fixture.
) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-22: an uninitialized API registers nothing.

    The scope's own agent has a migrated filter, so only the unfiltered sibling
    is legacy.
    """
    scope = _scope(synthetic_provider.url)
    sibling = _sibling(scope)
    scope["generation"] = run_generation(scope)
    assert _check(hooks_client, scope).json()["http_identity_required"] is True
    answer = _check(hooks_client, sibling)
    assert answer.status_code == 200, answer.text
    assert answer.json()["http_identity_required"] is False


@pytest.mark.parametrize("field", ["schema_version", "request_id", "scope"])
def test_internal_route_rejects_incomplete_requests(
    clean_db: None, hooks_client: TestClient, field: str,
) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-22: strict request shape, fixed refusal."""
    body: dict[str, Any] = {
        "schema_version": 1,
        "request_id": str(uuid.uuid4()),
        "scope": {
            "tenant_id": str(uuid.uuid4()),
            "agent_id": str(uuid.uuid4()),
            "binding_id": str(uuid.uuid4()),
            "binding_generation": 0,
        },
    }
    del body[field]
    response = hooks_client.post(
        CHECK, json=body,
        headers={"X-Curie-Worker-Token": get_settings().internal_worker_token},
    )
    assert response.status_code == 422
    assert response.json() == {"detail": "invalid_binding_classification_request"}


def run_generation(scope: dict[str, Any]) -> int:
    """@spec HTTP-INVOCATION-EXECUTION-22: the stored channel generation."""
    result: dict[str, int] = {}

    async def read(factory: async_sessionmaker[AsyncSession]) -> None:
        """@spec HTTP-INVOCATION-EXECUTION-22."""
        async with factory() as session:
            row = await session.get(AgentChannel, uuid.UUID(scope["binding"]))
            assert row is not None
            result["generation"] = row.generation

    run(read)
    return result["generation"]


GROUP_FILTER = {
    "entries": [{"kind": "issuer_group", "group": {"issuer": ISSUER, "id": "SyntheticOtherGroup"}}],
}


def _restrict(sibling: dict[str, Any], operation: str) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-22: give the sibling agent an HTTP-only filter."""

    async def apply(factory: async_sessionmaker[AsyncSession]) -> None:
        """@spec HTTP-INVOCATION-EXECUTION-22: through the existing role-policy owner."""
        async with factory() as session, session.begin():
            await agent_role_policy.apply_role_change(
                session,
                change=change(
                    uuid.UUID(sibling["tenant"]), uuid.UUID(sibling["agent"]),
                    operation=operation, filter=GROUP_FILTER,
                ),
            )

    run(apply)


@pytest.mark.parametrize("operation", ["migrate_role", "set_filter"])
@pytest.mark.parametrize("initialized", [True, False])
def test_unregistered_binding_of_a_filtered_agent_refuses_instead_of_legacy(
    clean_db: None,
    valkey: redis.Redis,
    runs_stream: str,
    synthetic_provider: SimpleNamespace,  # noqa: F811 - the reused fixture.
    operation: str,
    initialized: bool,
) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-22: the legacy path never evaluates role filters."""
    scope = _scope(synthetic_provider.url)
    sibling = _sibling(scope)
    _restrict(sibling, operation)
    module = _runtime_api()
    app = (
        create_app(
            http_identity_runtime=_runtime(module, scope),
            http_identity_client=HttpIdentityClient(tls_context=synthetic_provider.tls),
        )
        if initialized else create_app()
    )
    try:
        with TestClient(app) as client:
            token = _credential(client, sibling)
            refused = _legacy_post(client, token, sibling)
            assert refused.status_code == 401, refused.text
            assert refused.json() == {"detail": "invalid_http_receiver"}
            assert valkey.xlen(runs_stream) == 0 and synthetic_provider.requests == []
            assert _keys(valkey, sibling["binding"]) == set()
            answer = _check(client, sibling)
            assert answer.status_code == 200, answer.text
            assert answer.json()["http_identity_required"] is True
    finally:
        _cleanup(valkey, scope["binding"], sibling["binding"])
