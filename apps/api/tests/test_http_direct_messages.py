"""One-to-one direct messages through the actual ASGI channel route.

Real ASGI routes, owned synthetic TLS directory, PostgreSQL and Valkey; synthetic
workspace, organisation, user and channel ids only. No real direct message
delivery is qualified here: its team fields remain unmeasured (ADR 0263).
@spec HTTP-INVOCATION-EXECUTION-26.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import redis
from curie_internal import identity_context as codec

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
