"""Approved resumes recheck the preserved answering actor at every worker boundary.

A real selected row is resolved through the actual answer route, so the sealed
actor is the one the approval commit owner recorded. Real ASGI routes, owned
synthetic TLS directory, PostgreSQL and Valkey; synthetic inputs only.
@spec HTTP-APPROVAL-CONTINUITY-8/12.
"""

from __future__ import annotations

import uuid
from types import SimpleNamespace
from typing import Any

import pytest
from curie_api import agent_role_policy
from curie_internal import identity_approval as wire
from principal_adoption_support import ISSUER
from role_policy_support import change, run
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from apps.api.tests.test_http_approval_answers import answer, resolve
from apps.api.tests.test_http_approval_answers import answer_case as answer_case
from apps.api.tests.test_http_approval_creation import creation_case as creation_case
from apps.api.tests.test_http_approval_grants import (
    assert_grant,
    grant_for,
    post_grant,
    resume_check,
    resume_token,
    ring,
    stored,
)
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


def narrow_approver(case: SimpleNamespace) -> None:
    """@spec HTTP-APPROVAL-CONTINUITY-8: only the approver role's stored filter changes."""

    async def narrow(factory: async_sessionmaker[AsyncSession]) -> None:
        """@spec HTTP-APPROVAL-CONTINUITY-8."""
        async with factory() as session, session.begin():
            result = await agent_role_policy.apply_role_change(session, change=change(
                uuid.UUID(case.scope["tenant"]), uuid.UUID(case.scope["agent"]),
                version=case.scope["role_version"], target="approver",
                filter={"entries": [
                    {"kind": "issuer_group",
                     "group": {"issuer": ISSUER, "id": "SyntheticApproverOnlyGroup"}},
                ]},
            ))
            case.scope["role_version"] = result.version

    run(narrow)


def purposes(case: SimpleNamespace, before: int) -> list[str]:
    """@spec HTTP-APPROVAL-CONTINUITY-8: lookups made since ``before``, by provider purpose."""
    return sorted(item["purpose"] for item in case.provider.requests[before:])


@pytest.mark.parametrize("stage", ["prepare", "claim_or_resume", "start", "steer"])
def test_approved_resume_continues_on_fresh_invoker_and_approver_authority(
    answer_case: SimpleNamespace, stage: str,
) -> None:
    """@spec HTTP-APPROVAL-CONTINUITY-8 HTTP-APPROVAL-CONTINUITY-12: both subjects, each time."""
    case = answer_case
    token, payload = approved(case)
    before = len(case.provider.requests)
    request = resume_check(case, token, payload, stage=stage)
    assert_response(
        post_check(case.client, request), request, decision="continue",
        reason="current_authority",
    )
    assert purposes(case, before) == ["approval", "invocation"]


def test_exact_grant_continues_only_with_fresh_approver_authority(
    answer_case: SimpleNamespace,
) -> None:
    """@spec HTTP-APPROVAL-CONTINUITY-12: the exact submitted snapshot reaches continue."""
    case = answer_case
    token, payload = approved(case)
    body: dict[str, Any] = {
        "schema_version": 1, "check": resume_check(case, token, payload, stage="prepare"),
        "grant": grant_for(case),
    }
    assert_grant(
        post_grant(case.client, body), body, decision="continue", reason="current_authority",
    )


def test_approver_removed_after_resolution_refuses_the_resume_and_the_grant(
    answer_case: SimpleNamespace,
) -> None:
    """@spec HTTP-APPROVAL-CONTINUITY-8: invoker authority never stands in for the approver."""
    case = answer_case
    token, payload = approved(case)
    narrow_approver(case)
    request = resume_check(case, token, payload, stage="claim_or_resume")
    assert_response(
        post_check(case.client, request), request, decision="refuse",
        reason="caller_not_allowed",
    )
    body: dict[str, Any] = {
        "schema_version": 1, "check": resume_check(case, token, payload, stage="prepare"),
        "grant": grant_for(case),
    }
    assert_grant(
        post_grant(case.client, body), body, decision="refuse", reason="caller_not_allowed",
    )

