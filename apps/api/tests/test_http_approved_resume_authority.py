"""Approved resumes recheck the preserved answering actor at every worker boundary.

A real selected row is resolved through the actual answer route, so the sealed
actor is the one the approval commit owner recorded. Real ASGI routes, owned
synthetic TLS directory, PostgreSQL and Valkey; synthetic inputs only.
@spec HTTP-APPROVAL-CONTINUITY-8/12.
"""

from __future__ import annotations

from types import SimpleNamespace

from curie_internal import identity_approval as wire

from apps.api.tests.test_http_approval_answers import (
    answer,
    resolve,
)
from apps.api.tests.test_http_approval_answers import answer_case as answer_case
from apps.api.tests.test_http_approval_answers import routed_case as routed_case
from apps.api.tests.test_http_approval_creation import creation_case as creation_case
from apps.api.tests.test_http_approval_grants import (
    resume_token,
    ring,
    stored,
)
from apps.api.tests.test_http_invocation_membership import synthetic_provider as synthetic_provider


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
