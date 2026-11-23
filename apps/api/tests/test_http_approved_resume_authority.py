"""Approved resumes recheck the preserved answering actor at every worker boundary.

A real selected row is resolved through the actual answer route, so the sealed
actor is the one the approval commit owner recorded. Real ASGI routes, owned
synthetic TLS directory, PostgreSQL and Valkey; synthetic inputs only.
@spec HTTP-APPROVAL-CONTINUITY-8/12.
"""

from __future__ import annotations

import ssl
from types import SimpleNamespace

import redis
from curie_api.identity.http_transport import HttpIdentityClient
from curie_internal import identity_approval as wire

from apps.api.tests.test_http_approval_answers import (
    OTHER_APPROVER,
    answer,
    approver_role,
    resolve,
    restrict_route,
)
from apps.api.tests.test_http_approval_answers import answer_case as answer_case
from apps.api.tests.test_http_approval_answers import routed_case as routed_case
from apps.api.tests.test_http_approval_creation import creation_case as creation_case
from apps.api.tests.test_http_approval_grants import (
    assert_grant,
    grant_request,
    post_grant,
    resume_check,
    resume_token,
    ring,
    stored,
)
from apps.api.tests.test_http_invocation_membership import GROUP, SENDER
from apps.api.tests.test_http_invocation_membership import synthetic_provider as synthetic_provider
from apps.api.tests.test_http_turn_authority import assert_response, post_check


def approved(case: SimpleNamespace) -> tuple[str, str]:
    """@spec HTTP-APPROVAL-CONTINUITY-8/12: resolve through the actual answer owner."""
    response = resolve(case, *answer(case))
    assert response.status_code == 200, response.text
    case.approval_id = str(case.row["id"])
    saved = wire.open_approval_context(stored(case.approval_id)["identity_context"], ring(case))
    assert isinstance(saved, wire.ApprovalContextV1) and saved.status == "approved"
    return resume_token(case, saved)


def purposes(case: SimpleNamespace, before: int) -> list[str]:
    """@spec HTTP-APPROVAL-CONTINUITY-8: lookups made since ``before``, by provider purpose."""
    return sorted(item["purpose"] for item in case.provider.requests[before:])


def test_approved_resume_and_grant_distinguish_current_role_route_and_provider(
    routed_case: SimpleNamespace, valkey: redis.Redis, runs_stream: str,
) -> None:
    case = routed_case
    token, payload = approved(case)
    stream = valkey.xrange(runs_stream)
    row = stored(case.approval_id)
    grant = grant_request(case, token, payload)
    state = case.client.app.state
    trusted = state.http_identity_client

    for stage in ("prepare", "claim_or_resume", "start", "steer"):
        before = len(case.provider.requests)
        check = resume_check(case, token, payload, stage=stage)
        assert_response(post_check(case.client, check), check,
                        decision="continue", reason="current_authority")
        assert purposes(case, before) == ["approval", "invocation"]

    for cause in ("route", "role", "provider"):
        if cause == "route":
            restrict_route(case, [OTHER_APPROVER])
        elif cause == "role":
            approver_role(case, group="SyntheticApproverOnlyGroup")
        else:
            state.http_identity_client = HttpIdentityClient(
                tls_context=ssl.create_default_context(),
            )
        decision, reason = ("unavailable", "authority_unavailable") if cause == "provider" else (
            "refuse", "caller_not_allowed",
        )
        check = resume_check(case, token, payload)
        assert_response(post_check(case.client, check), check, decision=decision, reason=reason)
        assert_grant(post_grant(case.client, grant), grant, decision=decision, reason=reason)
        assert stored(case.approval_id) == row
        assert valkey.xrange(runs_stream) == stream

        if cause == "route":
            restrict_route(case, [SENDER])
        elif cause == "role":
            approver_role(case, group=GROUP)
        else:
            state.http_identity_client = trusted
        assert_grant(post_grant(case.client, grant), grant,
                     decision="continue", reason="current_authority")

    committed = []

    def revoke_during_lookup() -> None:
        if not committed:
            approver_role(case, group="SyntheticApproverOnlyGroup")
            committed.append(case.scope["role_version"])

    case.provider.during_lookup = revoke_during_lookup
    try:
        check = resume_check(case, token, payload)
        assert_response(post_check(case.client, check), check,
                        decision="unavailable", reason="authority_unavailable")
    finally:
        case.provider.during_lookup = None
    assert len(committed) == 1
    assert stored(case.approval_id) == row
    assert valkey.xrange(runs_stream) == stream
    assert_grant(post_grant(case.client, grant), grant,
                 decision="refuse", reason="caller_not_allowed")
