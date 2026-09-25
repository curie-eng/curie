"""Review feedback on a factory-owned pull request asks for one more revision (#2798).

A WorkItem owns its pull request through ``work_items.publication_lineage_id``.
Signature verification stays on the webhook route. This arm reuses the review
parser, the signed delivery audit and the provider truth verifier, then admits
the next ExecutionRequest on the owning WorkItem. Refusals carry codes only,
never webhook contents.
"""

from __future__ import annotations

import json
import logging
import uuid
from typing import Any

import httpx
from fastapi import HTTPException
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from . import workitem_dispatch
from .config import Settings
from .factory_reply_target import feedback_url
from .github_factory import (
    _IGNORED,
    _admission_result,
    _delivery_uuid,
    _Facts,
    _ignored,
    lock_issue,
)
from .github_factory_events import mentions_login
from .github_review_audit import claim_review_delivery, settle_review_delivery
from .github_review_events import (
    FeedbackIgnored,
    FeedbackUnavailable,
    UnverifiedFeedback,
    parse_feedback,
)
from .github_review_store import feedback_provenance
from .github_review_truth import BoundReviewLineage, verify_feedback_truth
from .models import ThreadPublicationLineage, WorkItem
from .schemas import WebhookResult
from .workspace_policy import repository_is_allowed

logger = logging.getLogger(__name__)

_MAX_OBJECTIVE = 65536
# Payload-shape refusals the review ingress also treats as ignorable.
_REVIEW_IGNORED = _IGNORED | {
    "unsupported_action",
    "non_actionable_review",
    "empty_feedback",
    "edited_feedback",
    "not_pull_request",
}



def _payload_pull_request(event: str, payload: Any) -> tuple[int, int] | None:
    """Repository id and PR number claimed by the payload, for routing only."""

    if not isinstance(payload, dict):
        return None
    repository = payload.get("repository")
    holder = payload.get("issue") if event == "issue_comment" else payload.get("pull_request")
    if not isinstance(repository, dict) or not isinstance(holder, dict):
        return None
    if event == "issue_comment" and not isinstance(holder.get("pull_request"), dict):
        return None
    repository_id, number = repository.get("id"), holder.get("number")
    if type(repository_id) is not int or type(number) is not int:
        return None
    return repository_id, number


async def _owner(
    session: AsyncSession, repository_id: int, pr_number: int
) -> tuple[WorkItem, ThreadPublicationLineage] | None:
    row = (
        await session.execute(
            select(WorkItem, ThreadPublicationLineage)
            .join(
                ThreadPublicationLineage,
                ThreadPublicationLineage.id == WorkItem.publication_lineage_id,
            )
            .where(
                ThreadPublicationLineage.github_repository_id == repository_id,
                ThreadPublicationLineage.pr_number == pr_number,
                WorkItem.github_repository_id == repository_id,
            )
            .order_by(ThreadPublicationLineage.created_at.desc())
            .limit(1)
        )
    ).first()
    if row is None:
        return None
    return row[0], row[1]


async def factory_owns(session: AsyncSession, event: str, payload: Any) -> bool:
    """True when a WorkItem owns the lineage for the payload's pull request."""

    claimed = _payload_pull_request(event, payload)
    if claimed is None:
        return False
    return await _owner(session, *claimed) is not None


def is_actionable_feedback(event: str, payload: Any, delivery_id: str) -> bool:
    """True when the payload parses as human PR feedback (routing only)."""

    try:
        parse_feedback(event, payload, delivery_id)
    except FeedbackIgnored:
        return False
    return True


def _objective(feedback: UnverifiedFeedback, settings: Settings, repo_full_name: str) -> str:
    fragment = feedback.url.split("#", 1)[1]
    url = feedback_url(settings.github_clone_base, repo_full_name, feedback.pr_number, fragment)
    provenance = feedback_provenance(feedback)
    return (
        f"{url}\n\n"
        "Human GitHub review feedback on this work item's pull request follows as JSON. "
        "Use its body as the reviewer's requested changes and revise the same pull request.\n"
        + json.dumps(provenance, ensure_ascii=False)
    )


def _bound(lineage: ThreadPublicationLineage) -> BoundReviewLineage:
    if (
        lineage.pr_number is None
        or lineage.head_sha is None
        or lineage.github_repository_id is None
        or lineage.github_installation_id is None
        or lineage.github_pr_node_id is None
        or lineage.base_ref is None
    ):
        raise FeedbackIgnored("lineage_authority_unproved")
    return BoundReviewLineage(
        lineage.repo_full_name,
        lineage.pr_number,
        lineage.branch,
        lineage.head_sha,
        lineage.github_repository_id,
        lineage.github_installation_id,
        lineage.github_pr_node_id,
        lineage.base_ref,
    )


async def _admit(
    session: AsyncSession,
    feedback: UnverifiedFeedback,
    *,
    settings: Settings,
    client: httpx.AsyncClient,
) -> WebhookResult:
    if not mentions_login(feedback.body, settings.github_factory_mention):
        raise FeedbackIgnored("ordinary_comment")
    if not repository_is_allowed(feedback.repo_full_name, settings.github_repo_allowlist):
        raise FeedbackIgnored("repository_not_allowed")
    owner = await _owner(session, feedback.repository_id, feedback.pr_number)
    if owner is None:
        raise FeedbackIgnored("lineage_unbound")
    work_item = owner[0]
    # Serialize with issue cancellation, then re-read under the lock.
    await lock_issue(session, work_item.github_repository_id, work_item.github_issue_number)
    await session.refresh(work_item)
    lineage = await session.get(ThreadPublicationLineage, owner[1].id, populate_existing=True)
    if lineage is None or work_item.publication_lineage_id != lineage.id:
        raise FeedbackIgnored("lineage_unbound")
    if lineage.status != "open":
        raise FeedbackIgnored("lineage_closed")
    if work_item.cancelled_at is not None:
        raise FeedbackIgnored("work_item_cancelled")
    bound = _bound(lineage)
    if (
        feedback.installation_id != work_item.github_installation_id
        or feedback.installation_id != bound.installation_id
    ):
        raise FeedbackIgnored("installation_mismatch")
    await verify_feedback_truth(feedback, bound, settings=settings, client=client)
    objective = _objective(feedback, settings, work_item.repo_full_name)
    if len(objective) > _MAX_OBJECTIVE:
        raise FeedbackIgnored("feedback_too_large")
    facts = _Facts(
        agent_id=work_item.agent_id,
        kind="github",
        address=work_item.repo_full_name,
        reply_conversation_id=f"issue-{work_item.github_issue_number}",
        repo_full_name=work_item.repo_full_name,
        github_repository_id=work_item.github_repository_id,
        github_issue_number=work_item.github_issue_number,
        github_installation_id=work_item.github_installation_id,
        objective=objective,
        requester=f"github:{feedback.sender_id}:{feedback.sender_login}",
        request_id=uuid.uuid5(uuid.NAMESPACE_URL, feedback.event_id),
    )
    result = await workitem_dispatch.admit(session, facts)
    return _admission_result(result, facts.request_id)


async def handle_factory_review_delivery(
    session: AsyncSession,
    *,
    settings: Settings,
    client: httpx.AsyncClient,
    event: str,
    delivery_id: str,
    body: bytes,
    payload: Any,
) -> WebhookResult:
    """Admit, ignore, or refuse one signed review delivery on a factory PR."""

    parsed_delivery = _delivery_uuid(delivery_id)
    audit, conflict = await claim_review_delivery(
        session, delivery_id=parsed_delivery, event=event, body=body, payload=payload
    )
    if conflict:
        await session.commit()
        return _ignored("delivery_identity_conflict")
    if audit.status == "accepted":
        await session.commit()
        return WebhookResult(status="factory_duplicate")
    if audit.status in {"ignored", "rejected"}:
        assert audit.reason is not None
        await session.commit()
        return _ignored(audit.reason)
    try:
        feedback = parse_feedback(event, payload, delivery_id)
        outcome = await _admit(session, feedback, settings=settings, client=client)
    except FeedbackUnavailable as exc:
        settle_review_delivery(audit, "retryable", exc.code)
        await session.commit()
        raise HTTPException(503, {"code": exc.code}, headers={"Retry-After": "10"}) from None
    except FeedbackIgnored as exc:
        if exc.code == "invalid_delivery":
            raise HTTPException(400, {"code": exc.code}) from None
        disposition = "ignored" if exc.code in _REVIEW_IGNORED else "rejected"
        settle_review_delivery(audit, disposition, exc.code)
        await session.commit()
        logger.info("factory review delivery ignored: %s", exc.code)
        return _ignored(exc.code)
    if outcome.status == "factory_ignored":
        code = outcome.errors[0]["code"] if outcome.errors else "ignored"
        settle_review_delivery(audit, "ignored" if code in _REVIEW_IGNORED else "rejected", code)
    else:
        settle_review_delivery(audit, "accepted")
    await session.commit()
    return outcome
