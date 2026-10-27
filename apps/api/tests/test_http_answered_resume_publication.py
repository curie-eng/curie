"""A committed selected answer publishes its resume through the core-owned path.

The actual resolve and recover routes, the production approved-resume authority,
ResumeQueue, PostgreSQL, Valkey and the synthetic TLS identity provider;
synthetic inputs only. @spec HTTP-APPROVAL-CONTINUITY-8/10/11.
"""

from __future__ import annotations

import os
import uuid
from types import SimpleNamespace
from typing import Any

import pytest
import redis
from aci_protocol import QueuedTurn
from curie_api import agent_role_policy
from curie_api.config import get_settings
from curie_api.db import SCHEMA
from curie_api.resumequeue import resume_event_id
from principal_adoption_support import ISSUER
from role_policy_support import change, run
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from apps.api.tests.test_http_approval_answers import (
    SENDER,
    answer,
    resolve,
    sealed_actor,
)
from apps.api.tests.test_http_approval_answers import answer_case as answer_case
from apps.api.tests.test_http_approval_creation import creation_case as creation_case
from apps.api.tests.test_http_approval_creation import rows
from apps.api.tests.test_http_approval_legacy_writers import RECOVERY_ENV
from apps.api.tests.test_http_approval_resume import (
    CONTEXT_FIELD,
    resume_context,
    resume_module,
    with_stores,
)
from apps.api.tests.test_http_invocation_membership import synthetic_provider as synthetic_provider


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


@pytest.fixture
def published_case(answer_case: SimpleNamespace) -> SimpleNamespace:
    """@spec HTTP-APPROVAL-CONTINUITY-11: a real selected row and a migrated approver role."""
    return answer_case


def resumes(case: SimpleNamespace, valkey: redis.Redis, stream: str) -> list[dict[str, Any]]:
    """@spec HTTP-APPROVAL-CONTINUITY-11: entries appended since the fixture began."""
    return [fields for _, fields in valkey.xrange(stream)[len(case.stream_before):]]


def owe(approval_id: Any) -> None:
    """@spec HTTP-APPROVAL-CONTINUITY-11: reopen the owed wake, as a dead-letter reopen does."""

    async def clear(factory: async_sessionmaker[AsyncSession]) -> None:
        """@spec HTTP-APPROVAL-CONTINUITY-11."""
        async with factory() as session, session.begin():
            await session.execute(
                text(f'UPDATE "{SCHEMA}".approvals SET resumed_at = NULL WHERE id = :id'),
                {"id": approval_id},
            )

    run(clear)


def test_an_approved_answer_publishes_one_sealed_resume_after_its_commit(
    published_case: SimpleNamespace, valkey: redis.Redis, runs_stream: str,
) -> None:
    """@spec HTTP-APPROVAL-CONTINUITY-11: payload and context together, then marked resumed."""
    case = published_case
    before = len(case.provider.requests)
    response = resolve(case, *answer(case))
    assert response.status_code == 200, response.text
    row, capsule = sealed_actor(case, "approved")
    assert row["resumed_at"] is not None
    entries = resumes(case, valkey, runs_stream)
    assert len(entries) == 1
    assert set(entries[0]) - {"traceparent"} == {"payload", CONTEXT_FIELD}
    turn = QueuedTurn.model_validate_json(entries[0]["payload"])
    assert turn.event_id == resume_event_id(row["id"])
    context = resume_context(case, entries[0][CONTEXT_FIELD])
    assert context.status == "approved" and context.actor_scope == capsule.actor_scope
    assert context.scope == case.context.scope
    # Answer (invoker+approver), then publication (invoker+approver), each fresh.
    purposes = sorted(item["purpose"] for item in case.provider.requests[before:])
    assert purposes == ["approval", "approval", "invocation", "invocation"]


def test_a_recovered_answer_publishes_its_rejected_notification(
    published_case: SimpleNamespace, valkey: redis.Redis, runs_stream: str,
) -> None:
    """@spec HTTP-APPROVAL-CONTINUITY-10/11: recovery converges on the same publisher."""
    case = published_case
    body, headers = answer(case, operation="recover")
    headers["X-API-Key"] = get_settings().api_key
    os.environ[RECOVERY_ENV] = "true"
    get_settings.cache_clear()
    try:
        response = case.client.post(
            f"/approvals/{case.row['id']}/recover", content=body, headers=headers,
        )
    finally:
        os.environ.pop(RECOVERY_ENV, None)
        get_settings.cache_clear()
    assert response.status_code == 200, response.text
    assert response.json()["actor"] == SENDER
    row, _ = sealed_actor(case, "rejected")
    assert row["resumed_at"] is not None
    entries = resumes(case, valkey, runs_stream)
    assert len(entries) == 1
    assert resume_context(case, entries[0][CONTEXT_FIELD]).status == "rejected"


def test_an_approver_removed_before_publication_leaves_the_wake_owed(
    published_case: SimpleNamespace, valkey: redis.Redis, runs_stream: str,
) -> None:
    """@spec HTTP-APPROVAL-CONTINUITY-8/11: invoker authority never publishes for the approver."""
    case = published_case
    assert resolve(case, *answer(case)).status_code == 200
    owe(case.row["id"])
    narrow_approver(case)

    async def body(stores: SimpleNamespace) -> None:
        """@spec HTTP-APPROVAL-CONTINUITY-11."""
        from curie_api.identity.http_approval_answer import approved_resume_authority

        stores.state.approver_sets = case.client.app.state.approver_sets
        owner = resume_module().http_resume_owner(
            stores.state, approved=approved_resume_authority(stores.state),
        )
        queue = type(stores.queue)(stores.valkey, stream=stores.stream, approvals=owner)
        with pytest.raises(resume_module().ApprovalResumeRefused) as refused:
            await queue.enqueue_approval(case.row["id"])
        assert refused.value.code == "caller_not_allowed"

    assert with_stores(case, body) == []
    assert rows(case.row["dedupe_key"])[0]["resumed_at"] is None


def test_a_refused_publication_still_returns_the_committed_answer(
    published_case: SimpleNamespace, valkey: redis.Redis, runs_stream: str,
) -> None:
    """@spec HTTP-APPROVAL-CONTINUITY-10/11: no append, not marked, resolution stands."""
    case = published_case
    queue = case.client.app.state.resume_queue
    real = queue._approvals
    # An owner without the approver observation refuses every approved publication.
    queue._approvals = resume_module().http_resume_owner(case.client.app.state)
    try:
        response = resolve(case, *answer(case))
    finally:
        queue._approvals = real
    assert response.status_code == 200, response.text
    assert response.json()["status"] == "approved"
    row, _ = sealed_actor(case, "approved")
    assert row["resumed_at"] is None
    assert resumes(case, valkey, runs_stream) == []
