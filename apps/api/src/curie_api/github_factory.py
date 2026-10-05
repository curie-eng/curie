"""Signed GitHub issue intake for one canonical WorkItem.

Signature verification stays on the webhook route. This module checks the
installation, repository allowlist, and sender permission with the review
verifier, then calls the generic WorkItem service. It does not read issue
bodies into the platform and it does not bind Slack.
"""

import logging
import uuid
from dataclasses import dataclass, replace
from typing import Any

import httpx
from fastapi import HTTPException
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession
from starlette.concurrency import run_in_threadpool

from curie_api.schemas.deployments import WebhookResult
from curie_api.workitems.lifecycle import WorkItemConflict, WorkItemOutcome

from . import factory_base, workitem_dispatch
from .config import Settings
from .factory_base import BaseRefusal
from .factory_notices import mark_status_comment_stale
from .forges.github.binding import _binding, github_reply_route
from .forges.github.identity import _issue_lock_keys, delivery_uuid
from .forges.github.transport import get_github_json, repository_identity_matches
from .github_app import GitHubAppError, GitHubInstallationRefused, credentials_for
from .github_factory_events import (
    FactoryNotice,
    FactoryRefused,
    mentions_login,
    parse_factory_event,
)
from .github_review_audit import claim_review_delivery, settle_review_delivery
from .github_review_events import FeedbackIgnored, FeedbackUnavailable
from .github_review_truth import verify_sender_write_permission
from .models import (
    AgentChannel,
    ThreadPublicationLineage,
    WorkItem,
)
from .repo_full_name import repo_url_path
from .workitem_dispatch import DispatchConflict
from .workspace_policy import repository_is_allowed

logger = logging.getLogger(__name__)

IGNORED = {
    "unsupported_action",
    "unsupported_event",
    "ordinary_comment",
    "unrelated_label",
    "app_authored",
    "non_human_sender",
    "pull_request_issue",
    "label_absent",
    "label_still_present",
    "issue_still_open",
    "issue_not_open",
    "comment_changed",
    "comment_target_mismatch",
    "invalid_comment",
    "invalid_label",
    "invalid_issue",
    "invalid_payload",
    "sender_mismatch",
    "not_admitted",
    "work_item_absent",
    "active_request",
    "work_item_cancelled",
    "comment_too_large",
    "base_conflict",
    "base_not_allowed",
    "base_missing",
    "base_label_recorded",
}


@dataclass(frozen=True)
class Facts:
    agent_id: uuid.UUID
    kind: str
    address: str
    reply_conversation_id: str
    repo_full_name: str
    github_repository_id: int
    github_issue_number: int
    github_installation_id: int
    objective: str
    requester: str
    request_id: uuid.UUID
    # The base a fresh admission resolved (ADR 0186), written on a new WorkItem.
    base: factory_base.ResolvedBase | None = None


@dataclass(frozen=True)
class VerifiedIssue:
    """What GitHub confirmed about the issue, reused for base resolution."""

    labels: set[str]
    default_branch: str | None
    token: str
    repo_path: str


def ignored(code: str) -> WebhookResult:
    return WebhookResult(status="factory_ignored", errors=[{"code": code}])


def _label_names(issue: dict[str, Any]) -> set[str]:
    labels = issue.get("labels")
    if not isinstance(labels, list):
        raise FactoryRefused("invalid_issue")
    names: set[str] = set()
    for label in labels:
        if not isinstance(label, dict) or not isinstance(label.get("name"), str):
            raise FactoryRefused("invalid_issue")
        names.add(label["name"])
    return names


async def verify_current(
    notice: FactoryNotice,
    *,
    settings: Settings,
    client: httpx.AsyncClient,
) -> VerifiedIssue:
    try:
        token = await run_in_threadpool(
            credentials_for(settings).token_for_verified_installation,
            notice.repo_full_name,
            notice.installation_id,
        )
    except (GitHubInstallationRefused, ValueError):
        raise FactoryRefused("installation_unverified") from None
    except GitHubAppError:
        raise FeedbackUnavailable("installation_unavailable") from None

    api = settings.github_api_url.rstrip("/")
    repo_path = f"/repos/{repo_url_path(notice.repo_full_name)}"
    repository = await get_github_json(
        client,
        api=api,
        token=token,
        path=repo_path,
        refusal="repository_unavailable",
    )
    if not repository_identity_matches(
        repository,
        repository_id=notice.repository_id,
        repo_full_name=notice.repo_full_name,
    ):
        raise FactoryRefused("repository_mismatch")
    issue = await get_github_json(
        client,
        api=api,
        token=token,
        path=f"{repo_path}/issues/{notice.issue_number}",
        refusal="issue_unavailable",
    )
    if type(issue.get("number")) is not int or issue["number"] != notice.issue_number:
        raise FactoryRefused("issue_mismatch")
    if "pull_request" in issue:
        raise FactoryRefused("pull_request_issue")
    names = _label_names(issue)
    # A ``base:`` label change is only recorded against an existing WorkItem;
    # the labels are what matter, so none of the per-disposition checks apply.
    if notice.disposition != "base_label":
        if notice.disposition == "admit":
            if issue.get("state") != "open" or notice.label not in names:
                raise FactoryRefused(
                    "issue_not_open" if issue.get("state") != "open" else "label_absent"
                )
        elif notice.action == "closed":
            if issue.get("state") != "closed":
                raise FactoryRefused("issue_still_open")
        elif notice.action == "unlabeled":
            if notice.label in names:
                raise FactoryRefused("label_still_present")
        else:
            if issue.get("state") != "open":
                raise FactoryRefused("issue_not_open")
            comment = await get_github_json(
                client,
                api=api,
                token=token,
                path=f"{repo_path}/issues/comments/{notice.comment_id}",
                refusal="comment_unavailable",
            )
            if comment.get("issue_url") != f"{api}{repo_path}/issues/{notice.issue_number}":
                raise FactoryRefused("comment_target_mismatch")
            if comment.get("performed_via_github_app") is not None:
                raise FactoryRefused("app_authored")
            if comment.get("body") != notice.comment_body:
                raise FactoryRefused("comment_changed")
            user = comment.get("user")
            if (
                not isinstance(user, dict)
                or type(user.get("id")) is not int
                or user["id"] != notice.sender_id
            ):
                raise FactoryRefused("sender_mismatch")
            body = comment.get("body")
            if not isinstance(body, str) or not mentions_login(
                body, settings.github_factory_mention
            ):
                raise FactoryRefused("ordinary_comment")
    await verify_sender_write_permission(
        client,
        api=api,
        token=token,
        repo_path=repo_path,
        sender_id=notice.sender_id,
        sender_login=notice.sender_login,
    )
    default_branch = repository.get("default_branch")
    return VerifiedIssue(
        labels=names,
        default_branch=default_branch if isinstance(default_branch, str) else None,
        token=token,
        repo_path=repo_path,
    )


async def _lock_issue(session: AsyncSession, notice: FactoryNotice) -> None:
    await lock_issue(session, notice.repository_id, notice.issue_number)


async def lock_issue(session: AsyncSession, repository_id: int, issue_number: int) -> None:
    """Hold one issue until this transaction commits.

    Admission and cancellation both re-read GitHub before they touch the
    WorkItem. Without this lock a closure can be stored as absent, and the
    admission that was already in flight then creates work the closure will
    not see again.
    """

    classid, objid = _issue_lock_keys(repository_id, issue_number)
    await session.execute(
        text("SELECT pg_advisory_xact_lock(CAST(:classid AS integer), CAST(:objid AS integer))"),
        {"classid": classid, "objid": objid},
    )


def _facts(notice: FactoryNotice, binding: AgentChannel, settings: Settings) -> Facts:
    base = settings.github_clone_base.rstrip("/")
    objective = f"{base}/{notice.repo_full_name}/issues/{notice.issue_number}"
    if notice.disposition == "mention":
        objective = f"{objective}#issuecomment-{notice.comment_id}"
    kind, address, reply_conversation_id = github_reply_route(
        notice.repo_full_name, notice.issue_number
    )
    return Facts(
        agent_id=binding.agent_id,
        kind=kind,
        address=address,
        reply_conversation_id=reply_conversation_id,
        repo_full_name=notice.repo_full_name,
        github_repository_id=notice.repository_id,
        github_issue_number=notice.issue_number,
        github_installation_id=notice.installation_id,
        objective=objective,
        requester=f"github:{notice.sender_id}:{notice.sender_login}",
        request_id=notice.request_id,
    )


async def work_item_for(
    session: AsyncSession, repository_id: int, issue_number: int
) -> WorkItem | None:
    found: WorkItem | None = await session.scalar(
        select(WorkItem).where(
            WorkItem.github_repository_id == repository_id,
            WorkItem.github_issue_number == issue_number,
        )
    )
    return found


def admission_result(
    result: WorkItemOutcome | WorkItemConflict | DispatchConflict,
    request_id: uuid.UUID,
) -> WebhookResult:
    if isinstance(result, WorkItemOutcome):
        if result.replayed:
            return WebhookResult(status="factory_duplicate")
        if result.request is not None and result.request.status == "queued":
            return WebhookResult(status="factory_queued")
        if result.request is not None and result.request.id != request_id:
            return WebhookResult(status="factory_readmit_pending")
        return WebhookResult(status="factory_admitted")
    code = result.code
    if code in IGNORED:
        return ignored(code)
    return ignored(code)


async def _has_open_pr(session: AsyncSession, item: WorkItem) -> bool:
    if item.publication_lineage_id is None:
        return False
    status = await session.scalar(
        select(ThreadPublicationLineage.status).where(
            ThreadPublicationLineage.id == item.publication_lineage_id
        )
    )
    return status == "open"


async def _record_label(
    session: AsyncSession, item: WorkItem, verified: VerifiedIssue, settings: Settings
) -> None:
    """Note a ``base:`` label that disagrees with the frozen base. Never move it."""

    if item.base_branch is None:
        # A legacy WorkItem keeps the repository default branch and says nothing.
        return
    ignored = factory_base.label_disagreement(
        verified.labels,
        factory_base.bases_for(settings, item.repo_full_name),
        verified.default_branch,
        item.base_branch,
    )
    if ignored == item.base_label_ignored:
        return
    item.base_label_ignored = ignored
    await mark_status_comment_stale(session, item.id)


async def _fresh_base(
    notice: FactoryNotice,
    verified: VerifiedIssue,
    settings: Settings,
    client: httpx.AsyncClient,
) -> factory_base.ResolvedBase:
    """Resolve the base for a fresh admission, or comment and refuse."""

    resolved = await factory_base.resolve_base(
        client,
        settings=settings,
        token=verified.token,
        repo_full_name=notice.repo_full_name,
        repo_path=verified.repo_path,
        labels=verified.labels,
        default_branch=verified.default_branch,
    )
    if isinstance(resolved, BaseRefusal):
        await factory_base.comment_refusal(
            client,
            settings=settings,
            token=verified.token,
            repo_path=verified.repo_path,
            issue_number=notice.issue_number,
            refusal=resolved,
        )
        raise FactoryRefused(resolved.code)
    return resolved


async def admit_notice(
    session: AsyncSession,
    notice: FactoryNotice,
    settings: Settings,
    verified: VerifiedIssue,
    client: httpx.AsyncClient,
) -> WebhookResult:
    binding = await _binding(session, notice)
    # Under the issue lock: the WorkItem decides whether the base is resolved
    # again (a fresh admission) or kept (ADR 0186 decision 5).
    existing = await work_item_for(session, notice.repository_id, notice.issue_number)
    if notice.disposition == "mention" and existing is None:
        raise FactoryRefused("not_admitted")
    facts = _facts(notice, binding, settings)
    if existing is not None and (
        notice.disposition == "mention" or await _has_open_pr(session, existing)
    ):
        # A revision, or work with an open PR, keeps its recorded base; no
        # branch is read.
        await _record_label(session, existing, verified, settings)
    else:
        # The fresh base travels with the admission facts. Dispatch validates
        # ownership first, then writes it with the request that runs on it: at
        # once for a fresh admission, or when a stopping run's replacement is
        # admitted (ADR 0186 decision 5).
        facts = replace(facts, base=await _fresh_base(notice, verified, settings, client))
    # Dispatch helpers commit between WorkItem creation and request creation.
    # Keep those commits inside savepoints so the caller's issue lock remains
    # held until admission and delivery settlement commit together.
    async with AsyncSession(
        bind=await session.connection(),
        join_transaction_mode="create_savepoint",
        expire_on_commit=False,
    ) as admission:
        if notice.disposition == "mention":
            result = await workitem_dispatch.admit_revision(admission, facts)
        else:
            result = await workitem_dispatch.readmit(admission, facts)
        return admission_result(result, facts.request_id)


async def record_base_label_notice(
    session: AsyncSession, notice: FactoryNotice, verified: VerifiedIssue, settings: Settings
) -> WebhookResult:
    """A ``base:`` label changed: record whether it now disagrees (ADR 0186)."""

    item = await work_item_for(session, notice.repository_id, notice.issue_number)
    if item is None:
        raise FactoryRefused("work_item_absent")
    if (
        item.github_installation_id != notice.installation_id
        or item.repo_full_name != notice.repo_full_name
    ):
        raise FactoryRefused("identity_mismatch")
    await _record_label(session, item, verified, settings)
    return ignored("base_label_recorded")


async def cancel_notice(session: AsyncSession, notice: FactoryNotice) -> WebhookResult:
    """Cancel one verified close or unlabel. Callers commit the session."""

    return await _cancel(session, notice)


async def _cancel(session: AsyncSession, notice: FactoryNotice) -> WebhookResult:
    item = await work_item_for(session, notice.repository_id, notice.issue_number)
    if item is None:
        raise FactoryRefused("work_item_absent")
    if (
        item.github_installation_id != notice.installation_id
        or item.repo_full_name != notice.repo_full_name
    ):
        raise FactoryRefused("identity_mismatch")
    result = await workitem_dispatch.cancel(
        session, work_item_id=item.id, expected_version=item.version
    )
    if isinstance(result, WorkItemConflict) and result.code == "stale_version":
        if result.work_item_version is None:
            return ignored("stale_version")
        result = await workitem_dispatch.cancel(
            session,
            work_item_id=item.id,
            expected_version=result.work_item_version,
        )
    if isinstance(result, WorkItemConflict):
        return ignored(result.code)
    if result.replayed:
        return WebhookResult(status="factory_duplicate")
    status = None if result.request is None else result.request.status
    if status == "cancellation_requested":
        return WebhookResult(status="factory_cancellation_requested")
    return WebhookResult(status="factory_cancelled")


async def _with_label_event(
    notice: FactoryNotice,
    *,
    settings: Settings,
    client: httpx.AsyncClient,
) -> FactoryNotice:
    """Attach the newest human labeled event id. The delivery id stays put.

    The events list is the same read the missed-label backstop uses. When
    GitHub cannot answer, the route retries instead of admitting under the
    webhook header.
    """

    from curie_api.factory_label_reconcile import last_label_event
    from curie_api.forges.github.transport import Unavailable, get_all

    try:
        token = await run_in_threadpool(
            credentials_for(settings).token_for_verified_installation,
            notice.repo_full_name,
            notice.installation_id,
        )
    except (GitHubInstallationRefused, ValueError):
        raise FactoryRefused("installation_unverified") from None
    except GitHubAppError:
        raise FeedbackUnavailable("installation_unavailable") from None
    api = settings.github_api_url.rstrip("/")
    repo_path = f"/repos/{repo_url_path(notice.repo_full_name)}"
    label = notice.label or settings.github_factory_label
    try:
        events = await get_all(
            client,
            api=api,
            token=token,
            path=f"{repo_path}/issues/{notice.issue_number}/events",
            params={},
        )
    except Unavailable:
        raise FeedbackUnavailable("label_events_unavailable") from None
    event = last_label_event(events, label)
    if event is None or type(event.get("id")) is not int:
        raise FeedbackUnavailable("label_events_unavailable")
    if event.get("performed_via_github_app") is not None:
        raise FactoryRefused("app_authored")
    actor = event.get("actor")
    if not isinstance(actor, dict) or actor.get("type") == "Bot":
        raise FactoryRefused("app_authored")
    return replace(notice, label_event_id=event["id"])


async def handle_factory_delivery(
    session: AsyncSession,
    *,
    settings: Settings,
    client: httpx.AsyncClient,
    event: str,
    delivery_id: str,
    body: bytes,
    payload: Any,
) -> WebhookResult:
    """Admit, ignore, or cancel one signed issue delivery. One issue stays one WorkItem."""

    parsed_delivery = delivery_uuid(delivery_id)
    audit, conflict = await claim_review_delivery(
        session,
        delivery_id=parsed_delivery,
        event=event,
        body=body,
        payload=payload,
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
        notice = parse_factory_event(
            event,
            payload,
            delivery_id,
            label=settings.github_factory_label,
            mention=settings.github_factory_mention,
        )
        if not repository_is_allowed(notice.repo_full_name, settings.github_repo_allowlist):
            raise FactoryRefused("repository_not_allowed")
        await _lock_issue(session, notice)
        verified = await verify_current(notice, settings=settings, client=client)
        if notice.disposition == "admit":
            notice = await _with_label_event(notice, settings=settings, client=client)
        if notice.disposition == "cancel":
            outcome = await cancel_notice(session, notice)
        elif notice.disposition == "base_label":
            outcome = await record_base_label_notice(session, notice, verified, settings)
        else:
            outcome = await admit_notice(session, notice, settings, verified, client)
    except FeedbackUnavailable as exc:
        settle_review_delivery(audit, "retryable", exc.code)
        await session.commit()
        raise HTTPException(503, {"code": exc.code}, headers={"Retry-After": "10"}) from None
    except FeedbackIgnored as exc:
        if exc.code == "invalid_delivery":
            raise HTTPException(400, {"code": exc.code}) from None
        disposition = "ignored" if exc.code in IGNORED else "rejected"
        settle_review_delivery(audit, disposition, exc.code)
        await session.commit()
        logger.info("factory delivery ignored: %s", exc.code)
        return ignored(exc.code)
    if outcome.status == "factory_ignored":
        code = "ignored"
        if outcome.errors:
            code = outcome.errors[0]["code"]
        disposition = "ignored" if code in IGNORED else "rejected"
        settle_review_delivery(audit, disposition, code)
    else:
        settle_review_delivery(audit, "accepted")
    await session.commit()
    return outcome
