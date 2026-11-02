"""Slack Connect partners through the actual ASGI routes with operator-trusted namespaces.

Real ASGI routes, owned synthetic TLS directory, PostgreSQL and Valkey; synthetic
workspace, organisation and user ids only. No real Slack Connect delivery or
external-workspace interaction is qualified here.
@spec HTTP-INVOCATION-EXECUTION-24.
"""

from __future__ import annotations

import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from types import SimpleNamespace
from typing import Any

import pytest
import redis
from aci_protocol import QueuedTurn
from curie_api.identity.http_transport import HttpIdentityClient
from curie_api.main import create_app
from curie_api.resumequeue import resume_event_id
from curie_api.schemas.http_identity import NamespaceV1
from curie_internal import identity_context as codec
from fastapi.testclient import TestClient

from apps.api.tests import test_http_invocation_membership as membership
from apps.api.tests.test_http_approval_answers import (
    answer,
    approver_role,
    resolve,
    sealed_actor,
    slack_evidence,
    unchanged,
)
from apps.api.tests.test_http_approval_creation import request_for as creation_request
from apps.api.tests.test_http_approval_legacy_writers import created
from apps.api.tests.test_http_invocation_membership import ACCOUNT, GRID_ORG, SENDER
from apps.api.tests.test_http_invocation_membership import synthetic_provider as synthetic_provider
from apps.api.tests.test_http_turn_authority import (
    assert_response,
    delivery_state,
    post_check,
    queued_invocation,
    request_for,
)

PARTNER = "T0EXAMPLEPARTNER"
STRANGER = "T0EXAMPLESTRANGER"


def partner_entry() -> NamespaceV1:
    """@spec HTTP-INVOCATION-EXECUTION-24: an operator-declared partner workspace."""
    return NamespaceV1(
        id=str(uuid.uuid4()), authority="", kind="slack_workspace", key=PARTNER,
    )


def connect_mention(**changes: Any) -> codec.ReceiverSlackEvidenceV1:
    """@spec HTTP-INVOCATION-EXECUTION-24: a partner member's mention in a Connect channel."""
    return codec.ReceiverSlackEvidenceV1.model_validate({
        "delivery": "app_mention", "user_id": SENDER, "event_user_team": PARTNER,
        "event_team": ACCOUNT, "interaction_user_team_id": None,
        "is_ext_shared_channel": True, "enterprise_ids": (), "event_source_team": PARTNER,
        **changes,
    })


def opened(invocation: SimpleNamespace) -> codec.InvocationContextV1:
    """@spec HTTP-INVOCATION-EXECUTION-24."""
    return invocation.context


def test_trusted_partner_queues_in_its_namespace_and_untrusted_partners_refuse(
    clean_db: None, valkey: redis.Redis, runs_stream: str, synthetic_provider: SimpleNamespace,
) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-24: partner namespace to the provider; outsiders never."""
    provider = synthetic_provider
    trusted = partner_entry()
    with queued_invocation(
        provider, valkey, runs_stream, trusted=(trusted,), slack=connect_mention(),
    ) as invocation:
        context = opened(invocation)
        assert context.scope.namespace.model_dump() == trusted.model_dump()
        assert context.scope.native_id == SENDER
        (observed,) = provider.requests
        assert observed["scope"]["sender"]["namespace"] == trusted.model_dump()
        assert observed["scope"]["sender"]["native_id"] == SENDER
        assert QueuedTurn.model_validate_json(invocation.payload).author == SENDER

        scope = invocation.scope
        before = delivery_state(valkey, runs_stream, scope["binding"])
        token = membership._credential(invocation.client, scope)
        for refused in (
            {"event_user_team": STRANGER, "event_source_team": STRANGER},
            {"event_source_team": STRANGER},
            {"event_user_team": None},
            {"event_user_team": None, "event_source_team": None, "event_team": PARTNER},
            {"is_ext_shared_channel": None},
            {"enterprise_ids": ("E0EXAMPLEPARTNER",)},
        ):
            submission = membership._submission(scope, slack=connect_mention(**refused))
            response = membership._post(invocation.client, token, *submission)
            assert response.status_code == 401, refused
            assert response.json() == {"detail": "invalid_http_receiver"}
        assert len(provider.requests) == 1
        assert delivery_state(valkey, runs_stream, scope["binding"]) == before


def test_own_member_in_a_connect_channel_resolves_in_the_own_namespace(
    clean_db: None, valkey: redis.Redis, runs_stream: str, synthetic_provider: SimpleNamespace,
) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-24: ADR 0261 decision 2 without any trust entry."""
    own = connect_mention(event_user_team=None, event_source_team=None)
    with queued_invocation(synthetic_provider, valkey, runs_stream, slack=own) as invocation:
        namespace = invocation.scope["namespace"]
        assert opened(invocation).scope.namespace.model_dump() == namespace.model_dump()
        assert synthetic_provider.requests[0]["scope"]["sender"]["namespace"] == (
            namespace.model_dump()
        )


def test_removing_a_trusted_entry_refuses_its_members_later_worker_check(
    clean_db: None, valkey: redis.Redis, runs_stream: str, synthetic_provider: SimpleNamespace,
) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-24: the next process replacement drops the partner."""
    provider = synthetic_provider
    with queued_invocation(
        provider, valkey, runs_stream, trusted=(partner_entry(),), slack=connect_mention(),
    ) as invocation:
        before = delivery_state(valkey, runs_stream, invocation.scope["binding"])
        first = request_for(invocation)
        assert_response(
            post_check(invocation.client, first), first,
            decision="continue", reason="current_authority",
        )
        assert len(provider.requests) == 2
        scope = {**invocation.scope, "trusted": ()}
        replaced = membership._runtime(membership._runtime_api(), scope)
        with TestClient(create_app(
            http_identity_runtime=replaced,
            http_identity_client=HttpIdentityClient(tls_context=provider.tls),
        )) as client:
            second = request_for(invocation, stage="start")
            assert_response(
                post_check(client, second), second,
                decision="refuse", reason="invalid_context",
            )
        assert len(provider.requests) == 2
        assert delivery_state(valkey, runs_stream, invocation.scope["binding"]) == before


@contextmanager
def approval_case(
    provider: SimpleNamespace, valkey: redis.Redis, stream: str, **invocation: Any,
) -> Iterator[SimpleNamespace]:
    """@spec HTTP-APPROVAL-CONTINUITY-7 HTTP-INVOCATION-EXECUTION-24: a real selected row."""
    with queued_invocation(provider, valkey, stream, **invocation) as case:
        case.provider = provider
        case.request = creation_request(case)
        before = delivery_state(valkey, stream, case.scope["binding"])
        case.row = created(case)
        approver_role(case)
        case.stream_before = valkey.xrange(stream)
        yield case
        for entry_id, fields in valkey.xrange(stream)[len(case.stream_before):]:
            assert QueuedTurn.model_validate_json(fields["payload"]).event_id == resume_event_id(
                case.row["id"],
            )
            valkey.xdel(stream, entry_id)
        assert delivery_state(valkey, stream, case.scope["binding"]) == before


def answered(case: SimpleNamespace, **changes: Any) -> tuple[bytes, dict[str, str]]:
    """@spec HTTP-APPROVAL-CONTINUITY-7: the same evidence in the attestation and the proof."""
    evidence = slack_evidence(**changes)
    return answer(
        case, apr_evidence=evidence,
        slack={**evidence, "enterprise_ids": tuple(evidence["enterprise_ids"])},
    )


def test_trusted_partner_answer_seals_the_partner_namespace(
    clean_db: None, valkey: redis.Redis, runs_stream: str, synthetic_provider: SimpleNamespace,
) -> None:
    """@spec HTTP-APPROVAL-CONTINUITY-7 HTTP-INVOCATION-EXECUTION-24."""
    trusted = partner_entry()
    with approval_case(synthetic_provider, valkey, runs_stream, trusted=(trusted,)) as case:
        before = len(case.provider.requests)
        refused = resolve(case, *answered(case, interaction_user_team_id=STRANGER))
        assert refused.status_code == 401, refused.text
        unchanged(case, before)
        response = resolve(case, *answered(case, interaction_user_team_id=PARTNER))
        assert response.status_code == 200, response.text
        _, capsule = sealed_actor(case, "approved")
        assert capsule.actor_scope is not None
        assert capsule.actor_scope.namespace.model_dump() == trusted.model_dump()
        approvals = [
            item for item in case.provider.requests[before:] if item["purpose"] == "approval"
        ]
        assert approvals and all(
            item["scope"]["sender"]["namespace"] == trusted.model_dump() for item in approvals
        )


def test_partner_invoker_original_association_survives_an_own_answer(
    clean_db: None, valkey: redis.Redis, runs_stream: str, synthetic_provider: SimpleNamespace,
) -> None:
    """@spec HTTP-APPROVAL-CONTINUITY-8 HTTP-INVOCATION-EXECUTION-24: the recorded partner."""
    trusted = partner_entry()
    with approval_case(
        synthetic_provider, valkey, runs_stream, trusted=(trusted,), slack=connect_mention(),
    ) as case:
        assert case.context.scope.namespace.model_dump() == trusted.model_dump()
        response = resolve(case, *answered(case))
        assert response.status_code == 200, response.text
        _, capsule = sealed_actor(case, "approved")
        assert capsule.actor_scope is not None
        assert capsule.actor_scope.namespace.model_dump() == case.scope["namespace"].model_dump()


@pytest.mark.parametrize(("changes", "status"), [
    ({"delivery": "view_submission", "enterprise_ids": []}, 200),
    ({"delivery": "block_actions", "enterprise_ids": [GRID_ORG]}, 200),
    ({"delivery": "block_actions", "enterprise_ids": []}, 401),
    ({"delivery": "view_submission", "enterprise_ids": [], "interaction_user_team_id": STRANGER},
     401),
])
def test_grid_partner_answers_follow_the_trusted_set(
    changes: dict[str, Any], status: int,
    clean_db: None, valkey: redis.Redis, runs_stream: str, synthetic_provider: SimpleNamespace,
) -> None:
    """@spec HTTP-APPROVAL-CONTINUITY-7 HTTP-INVOCATION-EXECUTION-24: ADR 0261 decision 4."""
    trusted = partner_entry()
    with approval_case(
        synthetic_provider, valkey, runs_stream, grid=True, trusted=(trusted,),
    ) as case:
        before = len(case.provider.requests)
        response = resolve(case, *answered(case, **{
            "interaction_user_team_id": PARTNER, **changes,
        }))
        assert response.status_code == status, response.text
        if status != 200:
            unchanged(case, before)
            return
        _, capsule = sealed_actor(case, "approved")
        assert capsule.actor_scope is not None
        assert capsule.actor_scope.namespace.model_dump() == trusted.model_dump()
