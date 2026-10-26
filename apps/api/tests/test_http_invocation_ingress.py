"""Actual adopted-HTTP channel ingress must not borrow legacy open admission.

@spec HTTP-INVOCATION-EXECUTION-2 HTTP-INVOCATION-EXECUTION-3
@spec HTTP-INVOCATION-EXECUTION-4 HTTP-INVOCATION-EXECUTION-5.
"""

from __future__ import annotations

import uuid
from typing import Any

import pytest
import redis
from curie_api import agent_role_policy, channel_token, identity_fence, principal_authority
from curie_api.config import get_settings
from curie_api.identity.http_transport import HttpIdentityClient
from curie_api.models import (
    AgentChannel,
    ChannelIdentity,
    IdentityLink,
    IdentityNamespace,
    Principal,
    ProviderInstallation,
)
from curie_dispatcher import admission as dispatcher_admission
from curie_internal.keyspace import CHANNEL_KEY_PREFIX
from fastapi.testclient import TestClient
from principal_adoption_support import ISSUER, adopt
from role_policy_support import change, run, seed, state
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker


def test_http_group_binding_refuses_channel_token_without_independent_receiver_proof(
    clean_db: None, hooks_client: TestClient, valkey: redis.Redis, runs_stream: str,
    identity_fence_key: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-2 HTTP-INVOCATION-EXECUTION-4/5."""
    assert identity_fence.FENCE_KEY == dispatcher_admission.IDENTITY_FENCE_KEY == identity_fence_key
    fixture: dict[str, Any] = {}
    address = "C" + uuid.uuid4().hex[:12].upper()
    native_sender = "U0EXAMPLE1"

    async def persisted_scope(factory: async_sessionmaker[AsyncSession]) -> None:
        """@spec HTTP-INVOCATION-EXECUTION-2 HTTP-INVOCATION-EXECUTION-5."""
        tenant, agent = await seed(factory)
        await adopt(factory, tenant)
        async with factory() as session, session.begin():
            person = Principal(
                tenant_id=tenant, idp_issuer=ISSUER, idp_subject="SyntheticHumanSubject",
                type="human", status="active", authorization_version=1,
            )
            installation = ProviderInstallation(
                tenant_id=tenant, provider="slack", authority="",
                external_account_id="T0EXAMPLE1", status="connected",
            )
            namespace = IdentityNamespace(
                tenant_id=tenant, provider="slack", authority="",
                kind="slack_workspace", key="T0EXAMPLE1", status="active",
            )
            session.add_all([person, installation, namespace])
            await session.flush()
            # Stored mapping and installation reports do not attest this request.
            identity = ChannelIdentity(
                tenant_id=tenant, provider="slack", name="http-receiver-" + uuid.uuid4().hex,
                provider_installation_id=installation.id, status="active",
                attributes={"slack_auth_test": {
                    "team_id": "T0EXAMPLE1", "is_enterprise_install": False,
                }},
            )
            link = IdentityLink(
                tenant_id=tenant, identity_namespace_id=namespace.id,
                provider_native_id=native_sender, principal_id=person.id,
                verification_source="idp_directory", created_by_directory="synthetic-fixture",
            )
            binding = AgentChannel(
                tenant_id=tenant, agent_id=agent, kind="slack", address=address,
                adapter=identity.name, allowed_callers=None,
            )
            session.add_all([identity, link, binding])
            await session.flush()
            fixture.update(binding=str(binding.id), adapter=identity.name)

        group_filter = {"entries": [{
            "kind": "issuer_group", "group": {"issuer": ISSUER, "id": "RequiredGroup"},
        }]}
        async with factory() as session, session.begin():
            await agent_role_policy.apply_role_change(
                session,
                change=change(tenant, agent, operation="migrate_role", filter=group_filter),
            )
        async with factory() as session, session.begin():
            authority = await principal_authority.read_authority(session, tenant_id=tenant)
            assert authority is not None and authority.selection.mode == "http"
            stored = await state(session, agent)
            assert stored is not None
            assert stored["invocation_team_id"] is None
            invocation = stored["role_configuration"]["invocation"]
            assert invocation["legacy_state"] == "migrated"
            assert invocation["filter"] == group_filter

    run(persisted_scope)
    token_response = hooks_client.post(
        "/channels/token", headers={"X-API-Key": get_settings().api_key},
        json={"kind": "slack", "address": address, "adapter": fixture["adapter"], "ttl_s": 60},
    )
    assert token_response.status_code == 200, token_response.text
    credential = token_response.json()["token"]
    claims = channel_token.verify_claims(
        credential, get_settings().api_key, scope=channel_token.CHANNEL_ENQUEUE_SCOPE,
    )
    assert claims is not None and claims.channel_id == fixture["binding"]

    provider_calls = []

    async def forbidden_lookup(*_args: Any, **_kwargs: Any) -> Any:
        """@spec HTTP-INVOCATION-EXECUTION-2: no external call before attestation."""
        provider_calls.append(True)
        raise AssertionError("provider lookup happened before independent receiver proof")

    monkeypatch.setattr(HttpIdentityClient, "lookup", forbidden_lookup)
    claim_prefix = f"{CHANNEL_KEY_PREFIX}:delivery:{fixture['binding']}:"
    backlog_prefix = f"{CHANNEL_KEY_PREFIX}:backlog:{fixture['binding']}:"

    def delivery_keys() -> list[Any]:
        """@spec HTTP-INVOCATION-EXECUTION-2."""
        return [*valkey.scan_iter(match=claim_prefix + "*"),
                *valkey.scan_iter(match=backlog_prefix + "*")]

    assert delivery_keys() == [] and valkey.xlen(runs_stream) == 0
    try:
        response = hooks_client.post(
            "/channels/turns", headers={"X-API-Key": credential},
            json={
                "kind": "slack", "address": address, "delivery_id": "synthetic-native-event",
                "conversation_id": address + ":synthetic-thread", "author": native_sender,
                "text": "Synthetic group-restricted invocation", "reply_ref": "",
            },
        )
        entries, keys = valkey.xlen(runs_stream), delivery_keys()
        assert response.status_code in {401, 403, 503}, (
            "adopted HTTP group policy accepted an unattested channel-token invocation",
            response.status_code, response.json(), {"entries": entries, "delivery_keys": len(keys)},
        )
        assert provider_calls == []
        assert entries == 0 and keys == []
    finally:
        keys = delivery_keys()
        if keys:
            valkey.delete(*keys)
