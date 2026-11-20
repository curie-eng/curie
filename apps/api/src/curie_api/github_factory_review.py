"""Review feedback on a factory-owned pull request asks for one more revision (#2798).

A WorkItem owns its pull request through ``work_items.publication_lineage_id``.
Signature verification stays on the webhook route. This arm reuses the review
parser, the signed delivery audit and the provider truth verifier, then admits
the next ExecutionRequest on the owning WorkItem. Refusals carry codes only,
never webhook contents.

Feedback needs no factory mention (ADR 0234, #5794). Review feedback admits one
request per submitted review, whichever of its pieces arrives first, and each
pull request admits at most `PLAIN_FEEDBACK_CAP` revisions from feedback that
does not mention the factory; past that a mention is needed, and one notice on
the pull request says so.
"""

from __future__ import annotations

import json
import logging
import uuid
from collections.abc import Iterable
from typing import Any

import httpx
from fastapi import HTTPException
from sqlalchemy import func, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from curie_api.factory_admission import ignored
from curie_api.forges.github.identity import delivery_uuid
from curie_api.github_factory import IGNORED, admission_result
from curie_api.schemas.deployments import WebhookResult

from . import workitem_dispatch
from .config import Settings
from .factory_follow_up import with_pull_request_follow_up_marker
from .factory_reply_target import feedback_url
from .factory_review_context import pull_request_body_context
from .forges.errors import ForgeError
from .forges.hosts import factory_app_slug, factory_code_host_entry, factory_tracker, github_host
from .forges.registry import ForgeRegistry, forges_registry
from .forges.trackers import code_host_for_repository, reply_route
from .forges.types import GITHUB, PullRequestRef, ReplyTarget
from .github_factory import lock_issue
from .github_factory_events import mentions_factory
from .github_review_audit import claim_review_delivery, settle_review_delivery
from .github_review_events import (
    FeedbackIgnored,
    FeedbackUnavailable,
    UnverifiedFeedback,
    parse_feedback,
)
from .github_review_snapshot import (
    ReviewComment,
    SubmittedReview,
    event_request_id,
    read_review_comment,
    read_review_comments,
    read_submitted_review,
    review_request_id,
)
from .github_review_store import feedback_provenance
from .github_review_truth import (
    BoundReviewLineage,
    VerifiedRepository,
    check_feedback_lineage,
    verify_feedback_in_repository,
    verify_repository,
)
from .models import ExecutionRequest, ThreadPublicationLineage, WorkItem
from .repo_full_name import repo_url_path
from .workitem_dispatch import Admission
from .workitems.lifecycle import WorkItemOutcome
from .workspace_policy import repository_is_allowed

logger = logging.getLogger(__name__)

_MAX_OBJECTIVE = 65536
# Payload-shape refusals the review ingress also treats as ignorable.
_REVIEW_IGNORED = IGNORED | {
    "unsupported_action",
    "non_actionable_review",
    "empty_feedback",
    "edited_feedback",
    "not_pull_request",
    "review_not_submitted",
    "plain_feedback_cap",
}
# Revisions feedback without a factory mention may start on one pull request.
PLAIN_FEEDBACK_CAP = 10
MENTION_NOTICE_MARKER = "curie-factory-mention-needed"


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
    session: AsyncSession, settings: Settings, repository_id: int, pr_number: int
) -> tuple[WorkItem, ThreadPublicationLineage] | None:
    host, project_id = github_host(settings), str(repository_id)
    row = (
        await session.execute(
            select(WorkItem, ThreadPublicationLineage)
            .join(
                ThreadPublicationLineage,
                ThreadPublicationLineage.id == WorkItem.publication_lineage_id,
            )
            .where(
                ThreadPublicationLineage.code_host_kind == GITHUB,
                ThreadPublicationLineage.code_host_host == host,
                ThreadPublicationLineage.repository_project_id == project_id,
                ThreadPublicationLineage.pr_number == pr_number,
                WorkItem.code_host_kind == GITHUB,
                WorkItem.code_host_host == host,
                WorkItem.repository_project_id == project_id,
            )
            .order_by(ThreadPublicationLineage.created_at.desc())
            .limit(1)
        )
    ).first()
    if row is None:
        return None
    return row[0], row[1]


async def factory_owns(session: AsyncSession, settings: Settings, event: str, payload: Any) -> bool:
    """True when a WorkItem owns the lineage for the payload's pull request."""

    claimed = _payload_pull_request(event, payload)
    if claimed is None:
        return False
    return await _owner(session, settings, *claimed) is not None


def is_actionable_feedback(
    event: str, payload: Any, delivery_id: str, *, github_html_base: str
) -> bool:
    """True when the payload parses as human PR feedback (routing only)."""

    try:
        parse_feedback(event, payload, delivery_id, github_html_base=github_html_base)
    except FeedbackIgnored:
        return False
    return True


def _feedback_url(feedback: UnverifiedFeedback, settings: Settings, repo_full_name: str) -> str:
    fragment = feedback.url.split("#", 1)[1]
    return feedback_url(settings.github_html_base, repo_full_name, feedback.pr_number, fragment)


def _reply_target(feedback: UnverifiedFeedback, work_item: WorkItem) -> ReplyTarget:
    """An inline review comment is answered in its thread; a PR comment or a
    submitted review in the pull request conversation."""

    pull_request = PullRequestRef(work_item.repository, str(feedback.pr_number))
    if feedback.event == "pull_request_review_comment":
        return ReplyTarget.on_thread(pull_request, str(feedback.feedback_id))
    return ReplyTarget.on_pull_request(pull_request)


def _objective(
    provenance: dict[str, Any], url: str, publication_context: dict[str, str | bool]
) -> str:
    # The feedback URL opens the objective for the agent's benefit only; the
    # reply target is stored typed on the request.
    provenance = {**provenance, **publication_context}
    return with_pull_request_follow_up_marker(
        f"{url}\n\n"
        "Human GitHub review feedback on this work item's pull request follows as JSON. "
        "Answer from the pull request body's Decisions section and the current diff first. "
        "Start the revision pipeline only when the comment asks for a change. "
        "The feedback may ask a question or request a change. Answer questions directly and "
        "revise the same pull request when a change is needed. The pull_request_body, when "
        "present, records the last successfully published description.\n"
        + json.dumps(provenance, ensure_ascii=False)
    )


def _review_provenance(
    feedback: UnverifiedFeedback,
    url: str,
    review: SubmittedReview,
    comments: tuple[ReviewComment, ...],
) -> dict[str, Any]:
    """The whole submitted review as the model sees it."""

    return {
        "event": "pull_request_review",
        "url": url,
        "sender": feedback.sender_login,
        "review_id": review.id,
        "state": review.state,
        "body": review.body,
        "comments": [
            {"path": comment.path, "line": comment.line, "body": comment.body}
            for comment in comments
        ],
    }


def mention_notice_text(mention: str) -> str:
    return (
        f"This pull request has had {PLAIN_FEEDBACK_CAP} revisions from plain comments. "
        f"To ask for another change, mention @{mention} in your comment."
    )


async def _post_mention_notice(
    registry: ForgeRegistry,
    client: httpx.AsyncClient,
    work_item: WorkItem,
    *,
    lineage_id: uuid.UUID,
    pr_number: int,
    installation_id: int,
    mention: str,
) -> bool:
    """Keep the one mention-needed comment on the pull request conversation.

    False when the provider refused or failed the write. A failed notice never
    blocks later feedback; the next plain comment past the cap posts it again.
    """

    repository = work_item.repository
    try:
        code_host = code_host_for_repository(
            registry, client, repository, installations={repository.path: installation_id}
        )
        await code_host.marked_comments.upsert_marked(
            ReplyTarget.on_pull_request(PullRequestRef(repository, str(pr_number))),
            f"{MENTION_NOTICE_MARKER}:{lineage_id}",
            mention_notice_text(mention),
        )
    except ForgeError:
        logger.info("factory mention notice deferred: mention_notice_unavailable")
        return False
    return True


async def _review_replay(
    session: AsyncSession, work_item: WorkItem, request_ids: Iterable[uuid.UUID]
) -> WebhookResult | None:
    """The replay answer when the review was already admitted under one of
    ``request_ids`` on this WorkItem, or None when none of them exists."""

    owners = set(
        await session.scalars(
            select(ExecutionRequest.work_item_id).where(ExecutionRequest.id.in_(set(request_ids)))
        )
    )
    if not owners:
        return None
    if owners != {work_item.id}:
        return ignored("identity_mismatch")
    return WebhookResult(status="factory_duplicate")


def _bound(lineage: ThreadPublicationLineage) -> BoundReviewLineage:
    if (
        lineage.pr_number is None
        or lineage.head_sha is None
        or lineage.code_host_kind != GITHUB
        or lineage.repository_project_id is None
        or lineage.code_host_installation_id is None
        or lineage.code_host_pr_id is None
        or lineage.base_ref is None
    ):
        raise FeedbackIgnored("lineage_authority_unproved")
    return BoundReviewLineage(
        lineage.repo_full_name,
        lineage.pr_number,
        lineage.branch,
        lineage.head_sha,
        int(lineage.repository_project_id),
        lineage.code_host_installation_id,
        lineage.code_host_pr_id,
        lineage.base_ref,
    )


async def admit_parsed_feedback(
    session: AsyncSession,
    feedback: UnverifiedFeedback,
    *,
    settings: Settings,
    client: httpx.AsyncClient,
    verified: VerifiedRepository | None,
) -> WebhookResult:
    """Admit one piece of review feedback.

    ``verified`` is the poll pass's repository read with its freshly verified
    installation token, reused for every piece of feedback in that pass; a
    webhook delivery passes None and verifies its own.

    A piece that names a review (a submitted review, or an inline comment with
    its ``pull_request_review_id``) admits the whole submitted review under one
    request id. Only feedback that mentions the factory in an approved or
    dismissed review is admitted on its own, as before #5794.
    """

    registry = forges_registry(settings)
    mention = factory_tracker(registry).config.mention
    app_slug = await factory_app_slug(registry)
    if not repository_is_allowed(feedback.repo_full_name, settings.github_repo_allowlist):
        raise FeedbackIgnored("repository_not_allowed")
    owner = await _owner(session, settings, feedback.repository_id, feedback.pr_number)
    if owner is None:
        raise FeedbackIgnored("lineage_unbound")
    work_item = owner[0]
    # Serialize with issue cancellation, then re-read under the lock.
    await lock_issue(session, work_item.tracker_issue)
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
        feedback.installation_id != work_item.code_host_installation_id
        or feedback.installation_id != bound.installation_id
    ):
        raise FeedbackIgnored("installation_mismatch")
    review_key = (
        None
        if feedback.review_id is None
        else review_request_id(feedback.repository_id, feedback.pr_number, feedback.review_id)
    )
    if review_key is not None:
        assert feedback.review_id is not None
        # Another piece of this review already admitted it: replay without
        # reading the provider again, as admission's own replay answers. A
        # review answered before #5794 was admitted under the per-event id of
        # the review or of a mentioning piece.
        replay = await _review_replay(
            session,
            work_item,
            (
                review_key,
                event_request_id(feedback.repository_id, "pull_request_review", feedback.review_id),
                event_request_id(feedback.repository_id, feedback.event, feedback.feedback_id),
            ),
        )
        if replay is not None:
            return replay
    check_feedback_lineage(feedback, bound)
    if verified is None:
        code_host = factory_code_host_entry(registry)
        verified = await verify_repository(
            bound,
            client=client,
            credentials=code_host.credentials.credentials,
            api_url=code_host.config.api_url,
        )
    repo_path = f"/repos/{repo_url_path(bound.repo_full_name)}"
    review: SubmittedReview | None = None
    if feedback.review_id is not None:
        review = await read_submitted_review(
            client,
            verified,
            repo_path=repo_path,
            pr_number=bound.pr_number,
            review_id=feedback.review_id,
            author_id=feedback.sender_id,
        )
        if not review.actionable:
            # Approving or dismissing with a comment never spends a revision;
            # a mention in it is admitted on its own, as before.
            if not mentions_factory(feedback.body, mention, app_slug):
                raise FeedbackIgnored("non_actionable_review")
            if feedback.event == "pull_request_review_comment":
                # The fresh read below matches an edit that added the mention;
                # only the comment's updated_at against submission shows it.
                await read_review_comment(
                    client,
                    verified,
                    repo_path=repo_path,
                    pr_number=bound.pr_number,
                    review=review,
                    comment_id=feedback.feedback_id,
                    head_sha=bound.head_sha,
                )
            review = None
    # The fresh read of this piece is its edit check. A review body edited
    # before the review is first admitted is caught only when the review's own
    # event decides first; an inline comment's edit is caught on every path by
    # the snapshot's updated_at check.
    await verify_feedback_in_repository(feedback, bound, verified, settings=settings, client=client)
    if review is not None:
        assert review_key is not None
        if review.commit_id != bound.head_sha:
            raise FeedbackIgnored("stale_feedback_head")
        comments = await read_review_comments(
            client,
            verified,
            repo_path=repo_path,
            pr_number=bound.pr_number,
            review=review,
            head_sha=bound.head_sha,
        )
        if feedback.event == "pull_request_review_comment" and feedback.feedback_id not in {
            comment.id for comment in comments
        }:
            raise FeedbackUnavailable("review_comments_unavailable")
        if not review.body.strip() and not comments:
            raise FeedbackIgnored("empty_feedback")
        # A review whose mentioning inline comment was admitted on its own
        # before #5794 was answered then.
        replay = await _review_replay(
            session,
            work_item,
            (
                event_request_id(feedback.repository_id, "pull_request_review_comment", comment.id)
                for comment in comments
            ),
        )
        if replay is not None:
            return replay
        request_id = review_key
        url = feedback_url(
            settings.github_html_base,
            work_item.repository_path,
            feedback.pr_number,
            f"pullrequestreview-{review.id}",
        )
        provenance = _review_provenance(feedback, url, review, comments)
        reply_target = ReplyTarget.on_pull_request(
            PullRequestRef(work_item.repository, str(feedback.pr_number))
        )
        texts = [review.body, *(comment.body for comment in comments)]
    else:
        request_id = uuid.uuid5(uuid.NAMESPACE_URL, feedback.event_id)
        url = _feedback_url(feedback, settings, work_item.repository_path)
        provenance = feedback_provenance(feedback)
        reply_target = _reply_target(feedback, work_item)
        texts = [feedback.body]
    plain = not any(mentions_factory(text, mention, app_slug) for text in texts)
    # Provider verification can overlap another WorkItem or lineage writer.
    # Keep both current rows locked through admission and reject changed proof.
    current_item = await session.scalar(
        select(WorkItem)
        .where(WorkItem.id == work_item.id)
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    if current_item is None or current_item.publication_lineage_id != lineage.id:
        raise FeedbackUnavailable("lineage_changed")
    current = (
        select(ThreadPublicationLineage)
        .where(ThreadPublicationLineage.id == lineage.id)
        .execution_options(populate_existing=True)
    )
    # Plain feedback writes the lineage's count (FOR NO KEY UPDATE), never its
    # key or version; feedback that mentions the factory only reads it.
    current_lineage = await session.scalar(
        current.with_for_update(key_share=True) if plain else current.with_for_update(read=True)
    )
    if current_lineage is None:
        raise FeedbackUnavailable("lineage_changed")
    if current_lineage.status != "open":
        raise FeedbackIgnored("lineage_closed")
    try:
        current_bound = _bound(current_lineage)
    except FeedbackIgnored:
        raise FeedbackUnavailable("lineage_changed") from None
    if current_bound != bound:
        raise FeedbackUnavailable("lineage_changed")
    if current_item.cancelled_at is not None:
        raise FeedbackIgnored("work_item_cancelled")
    work_item = current_item
    objective = _objective(provenance, url, await pull_request_body_context(session, work_item))
    if len(objective) > _MAX_OBJECTIVE:
        raise FeedbackIgnored("feedback_too_large")
    if plain and current_lineage.plain_feedback_revisions >= PLAIN_FEEDBACK_CAP:
        replay = await session.scalar(
            select(ExecutionRequest.id).where(ExecutionRequest.id == request_id)
        )
        if replay is None:
            # Post first: a failed post leaves the cap unrecorded, so a later
            # plain comment posts it again; the marker keeps it one.
            if current_lineage.plain_feedback_capped_at is None and await _post_mention_notice(
                registry,
                client,
                work_item,
                lineage_id=current_lineage.id,
                pr_number=feedback.pr_number,
                installation_id=feedback.installation_id,
                mention=mention,
            ):
                await session.execute(
                    update(ThreadPublicationLineage)
                    .where(ThreadPublicationLineage.id == current_lineage.id)
                    # The lineage's updated_at is its merge time to readers.
                    .values(
                        plain_feedback_capped_at=func.now(),
                        updated_at=ThreadPublicationLineage.updated_at,
                    )
                )
            return ignored("plain_feedback_cap")
    kind, address, conversation = reply_route(
        registry, work_item.tracker_issue, work_item.repository_path
    )
    facts = Admission(
        agent_id=work_item.agent_id,
        kind=kind,
        address=address,
        reply_conversation_id=conversation,
        issue=work_item.tracker_issue,
        repository=work_item.repository,
        code_host_installation_id=work_item.code_host_installation_id,
        objective=objective,
        requester=f"github:{feedback.sender_id}:{feedback.sender_login}",
        request_id=request_id,
        reply_target=reply_target,
        reply_url=url,
        # The installation's mode and never an owner, as tracker ingress does: a
        # personal installation refuses a service WorkItem, and a personal
        # WorkItem's revision copies its locked tuple; a reviewer is not an
        # owner (#5378).
        model_auth_mode=settings.factory_model_auth_mode,
    )
    # Keep dispatch commits inside savepoints so the caller's issue lock remains
    # held until admission and delivery settlement commit together.
    async with AsyncSession(
        bind=await session.connection(),
        join_transaction_mode="create_savepoint",
        expire_on_commit=False,
    ) as admission:
        result = await workitem_dispatch.admit_revision(admission, facts)
    if (
        plain
        and isinstance(result, WorkItemOutcome)
        and not result.replayed
        and result.request is not None
        and result.request.id == request_id
    ):
        # Counted in the caller's transaction, with the new request.
        await session.execute(
            update(ThreadPublicationLineage)
            .where(ThreadPublicationLineage.id == current_lineage.id)
            # The lineage's updated_at is its merge time to readers; keep it.
            .values(
                plain_feedback_revisions=ThreadPublicationLineage.plain_feedback_revisions + 1,
                updated_at=ThreadPublicationLineage.updated_at,
            )
        )
    return admission_result(result, facts.request_id)


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

    parsed_delivery = delivery_uuid(delivery_id)
    audit, conflict = await claim_review_delivery(
        session, delivery_id=parsed_delivery, event=event, body=body, payload=payload
    )
    if conflict:
        await session.commit()
        return ignored("delivery_identity_conflict")
    if audit.status == "accepted":
        await session.commit()
        return WebhookResult(status="factory_duplicate")
    if audit.status in {"ignored", "rejected"}:
        assert audit.reason is not None
        await session.commit()
        return ignored(audit.reason)
    try:
        feedback = parse_feedback(
            event,
            payload,
            delivery_id,
            github_html_base=settings.github_html_base,
            allow_empty_review=True,
        )
        outcome = await admit_parsed_feedback(
            session, feedback, settings=settings, client=client, verified=None
        )
    except FeedbackUnavailable as exc:
        settle_review_delivery(audit, "retryable", exc.code)
        await session.commit()
        raise HTTPException(503, {"code": exc.code}, headers={"Retry-After": "10"}) from None
    except FeedbackIgnored as exc:
        if exc.code == "invalid_delivery":
            raise HTTPException(400, {"code": exc.code}) from None
        if exc.code == "review_not_submitted":
            # The review is still pending. Leave the receipt open, so a
            # redelivery of this delivery is decided again after submission.
            settle_review_delivery(audit, "retryable", exc.code)
            await session.commit()
            return ignored(exc.code)
        disposition = "ignored" if exc.code in _REVIEW_IGNORED else "rejected"
        settle_review_delivery(audit, disposition, exc.code)
        await session.commit()
        logger.info("factory review delivery ignored: %s", exc.code)
        return ignored(exc.code)
    if outcome.status == "factory_ignored":
        code = outcome.errors[0]["code"] if outcome.errors else "ignored"
        settle_review_delivery(audit, "ignored" if code in _REVIEW_IGNORED else "rejected", code)
    else:
        settle_review_delivery(audit, "accepted")
    await session.commit()
    return outcome
