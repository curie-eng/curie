"""One-to-one direct messages through the actual ASGI channel route.

Real ASGI routes, owned synthetic TLS directory, PostgreSQL and Valkey; synthetic
workspace, organisation, user and channel ids only. No real direct message
delivery is qualified here: its team fields remain unmeasured (ADR 0263).
@spec HTTP-INVOCATION-EXECUTION-26.
"""

from __future__ import annotations

import hashlib
import json
import uuid
from types import SimpleNamespace
from typing import Any

import redis
from aci_protocol import QueuedTurn
from curie_api.config import get_settings
from curie_api.models import Agent, AgentChannel
from curie_api.schemas.http_identity import NamespaceV1
from curie_internal import identity_context as codec
from role_policy_support import run
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from apps.api.tests import test_http_invocation_membership as membership
from apps.api.tests.test_http_invocation_membership import (
    ACCOUNT,
    GRID_ORG,
    GRID_WORKSPACE,
    SENDER,
)
from apps.api.tests.test_http_invocation_membership import synthetic_provider as synthetic_provider
from apps.api.tests.test_http_turn_authority import delivery_state, queued_invocation

PARTNER = "T0EXAMPLEPARTNER"
STRANGER = "T0EXAMPLESTRANGER"


def direct(**changes: Any) -> codec.ReceiverSlackEvidenceV1:
    """@spec HTTP-INVOCATION-EXECUTION-26: a plain DM on an ordinary registration."""
    return codec.ReceiverSlackEvidenceV1.model_validate({
        "delivery": "message", "user_id": SENDER, "event_user_team": ACCOUNT,
        "event_team": ACCOUNT, "interaction_user_team_id": None,
        "is_ext_shared_channel": False, "enterprise_ids": (), "channel_type": "im", **changes,
    })


def grid_direct(**changes: Any) -> codec.ReceiverSlackEvidenceV1:
    """@spec HTTP-INVOCATION-EXECUTION-26: a plain DM on a Grid workspace registration."""
    return direct(**{
        "event_user_team": "T0EXAMPLESIBLING", "event_team": GRID_WORKSPACE,
        "enterprise_ids": (GRID_ORG,), **changes,
    })


def refused(invocation: SimpleNamespace, provider: SimpleNamespace, *cases: Any) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-26: no provider call or delivery write for any case."""
    scope = invocation.scope
    before = delivery_state_for(invocation)
    calls = len(provider.requests)
    token = membership._credential(invocation.client, scope)
    for evidence in cases:
        response = membership._post(
            invocation.client, token, *membership._submission(scope, slack=evidence),
        )
        assert response.status_code == 401, evidence
        assert response.json() == {"detail": "invalid_http_receiver"}
    assert len(provider.requests) == calls
    assert delivery_state_for(invocation) == before


def delivery_state_for(invocation: SimpleNamespace) -> Any:
    """@spec HTTP-INVOCATION-EXECUTION-26."""
    return delivery_state(invocation.valkey, invocation.stream, invocation.scope["binding"])


def test_ordinary_direct_message_queues_one_context_in_the_own_namespace(
    clean_db: None, valkey: redis.Redis, runs_stream: str, synthetic_provider: SimpleNamespace,
) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-26: positive statements resolve; everything else refuses."""
    provider = synthetic_provider
    with queued_invocation(provider, valkey, runs_stream, slack=direct()) as invocation:
        invocation.valkey, invocation.stream = valkey, runs_stream
        namespace = invocation.scope["namespace"].model_dump()
        assert invocation.context.scope.namespace.model_dump() == namespace
        assert invocation.context.scope.native_id == SENDER
        (observed,) = provider.requests
        assert observed["scope"]["sender"]["namespace"] == namespace
        assert observed["scope"]["sender"]["native_id"] == SENDER
        assert QueuedTurn.model_validate_json(invocation.payload).author == SENDER
        refused(
            invocation, provider,
            direct(channel_type="mpim"),
            direct(channel_type=None),
            direct(is_ext_shared_channel=True),
            direct(is_ext_shared_channel=None),
            direct(event_user_team=None, event_team=None),
            direct(event_user_team=STRANGER),
            direct(event_source_team=STRANGER),
            direct(enterprise_ids=(GRID_ORG,)),
            direct(user_id=None),
            # A mention never carries a conversation type.
            direct(delivery="app_mention", event_user_team=None),
            direct(delivery="message_changed"),
        )


def test_grid_direct_message_queues_one_organisation_context(
    clean_db: None, valkey: redis.Redis, runs_stream: str, synthetic_provider: SimpleNamespace,
) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-21/26."""
    provider = synthetic_provider
    with queued_invocation(
        provider, valkey, runs_stream, grid=True, slack=grid_direct(),
    ) as invocation:
        invocation.valkey, invocation.stream = valkey, runs_stream
        organisation = {
            "id": invocation.scope["namespace"].id, "authority": "", "kind": "slack_enterprise",
            "key": GRID_ORG,
        }
        assert invocation.context.scope.namespace.model_dump() == organisation
        (observed,) = provider.requests
        assert observed["scope"]["sender"]["namespace"] == organisation
        refused(
            invocation, provider,
            grid_direct(enterprise_ids=()),
            grid_direct(enterprise_ids=("E0EXAMPLEPARTNERORG",)),
            grid_direct(is_ext_shared_channel=True),
            grid_direct(channel_type="mpim"),
        )


def test_trusted_partner_namespace_never_widens_a_direct_message(
    clean_db: None, valkey: redis.Redis, runs_stream: str, synthetic_provider: SimpleNamespace,
) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-24/26."""
    provider = synthetic_provider
    trusted = NamespaceV1(id=str(uuid.uuid4()), authority="", kind="slack_workspace", key=PARTNER)
    with queued_invocation(
        provider, valkey, runs_stream, trusted=(trusted,), slack=direct(),
    ) as invocation:
        invocation.valkey, invocation.stream = valkey, runs_stream
        assert invocation.context.scope.namespace.model_dump() == (
            invocation.scope["namespace"].model_dump()
        )
        refused(
            invocation, provider,
            direct(event_user_team=PARTNER, event_source_team=PARTNER),
            direct(event_user_team=PARTNER, event_team=None),
        )


def other_agent_conversation(scope: dict[str, Any]) -> str:
    """@spec HTTP-INVOCATION-EXECUTION-26: a DM conversation bound to a second agent."""
    address = "D" + uuid.uuid4().hex[:12].upper()

    async def persist(factory: async_sessionmaker[AsyncSession]) -> None:
        async with factory() as session, session.begin():
            agent = Agent(
                tenant_id=uuid.UUID(scope["tenant"]), name="synthetic-other-" + uuid.uuid4().hex,
                status="active",
            )
            session.add(agent)
            await session.flush()
            session.add(AgentChannel(
                tenant_id=agent.tenant_id, agent_id=agent.id, kind="slack", address=address,
                adapter=scope["adapter"], allowed_callers=None,
            ))

    run(persist)
    return address


def submission_for(scope: dict[str, Any], address: str) -> tuple[bytes, str]:
    """@spec HTTP-INVOCATION-EXECUTION-26: a genuine receiver proof naming another conversation."""
    body = json.dumps({
        "kind": "slack", "address": address, "adapter": scope["adapter"],
        "delivery_id": "synthetic-event-" + uuid.uuid4().hex, "conversation_id": "1700.0004",
        "author": SENDER, "text": "Synthetic direct message", "reply_ref": "",
        "attachments": [],
    }, separators=(",", ":")).encode()
    parsed = json.loads(body)
    proof = codec.ReceiverProofV1(
        schema_version=1, receiver_id=scope["receiver_id"], receiver_generation=1,
        channel_identity_id=scope["identity"], provider_installation_id=scope["installation"],
        binding_id=scope["binding"], binding_generation=scope["generation"],
        delivery_id=parsed["delivery_id"], native_channel_id=address,
        conversation_id=parsed["conversation_id"], body_sha256=hashlib.sha256(body).hexdigest(),
        slack=direct(),
    )
    ring = codec.build_identity_keyring(
        "receiver", ((scope["receiver_id"], 1, scope["receiver_key"]),),
    )
    return body, codec.sign_receiver_proof(proof, ring)


def test_direct_message_conversation_bound_to_another_agent_is_refused(
    clean_db: None, valkey: redis.Redis, runs_stream: str, synthetic_provider: SimpleNamespace,
) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-26: routing stays the registered binding's address."""
    provider = synthetic_provider
    with queued_invocation(provider, valkey, runs_stream, slack=direct()) as invocation:
        invocation.valkey, invocation.stream = valkey, runs_stream
        scope = invocation.scope
        address = other_agent_conversation(scope)
        before = delivery_state_for(invocation)
        token = membership._credential(invocation.client, scope)
        response = membership._post(invocation.client, token, *submission_for(scope, address))
        # The channel credential names this agent's binding, not that conversation.
        assert response.status_code == 401, response.text
        assert response.json() == {"detail": "missing or invalid credential"}
        # The other agent's own channel credential cannot carry this receiver's DM either.
        other = invocation.client.post(
            "/channels/token",
            headers={"X-API-Key": get_settings().api_key},
            json={"kind": "slack", "address": address, "adapter": scope["adapter"], "ttl_s": 60},
        )
        assert other.status_code == 200, other.text
        response = membership._post(
            invocation.client, other.json()["token"], *submission_for(scope, address),
        )
        assert response.status_code == 401, response.text
        assert response.json() == {"detail": "invalid_http_receiver"}
        assert len(provider.requests) == 1
        assert delivery_state_for(invocation) == before
