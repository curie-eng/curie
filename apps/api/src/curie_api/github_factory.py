"""Signed GitHub issue intake for one canonical WorkItem.

Signature verification stays on the webhook route. This module checks the
installation and repository allowlist, has the GitHub tracker adapter confirm
the issue and the sender's permission, then calls the generic WorkItem
service. It does not read issue bodies into the platform and it does not bind
Slack.
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
from curie_api.workitems.lifecycle import (
    WorkItemConflict,
    WorkItemOutcome,
    for_tracker_issue,
    same_repository,
)

from . import factory_base, workitem_dispatch
from .config import Settings
from .factory_base import BaseRefusal
from .factory_notices import mark_status_comment_stale
from .forges.errors import ForgeError, Unavailable
from .forges.github.binding import (
    agent_binding,
    bound_labels,
    github_reply_route,
    resolve_binding,
)
from .forges.github.comments import static_token
from .forges.github.identity import delivery_uuid
from .forges.github.tracker import GitHubTracker, last_label_event
from .forges.hosts import (
    factory_code_host,
    factory_code_host_entry,
    factory_credentials,
    factory_repository,
    factory_tracker,
)
from .forges.identity import issue_lock_keys
from .forges.ports import CodeHost
from .forges.registry import ForgeRegistry, forges_registry
from .forges.surfaces import effective_trigger_label
from .forges.types import GITHUB, ReplyTarget, RepositoryRef, TrackerIssueRef
from .github_app import GitHubAppError, GitHubInstallationRefused
from .github_factory_events import (
    FactoryNotice,
    FactoryRefused,
    event_repository,
    parse_factory_event,
)
from .github_review_audit import claim_review_delivery, settle_review_delivery
from .github_review_events import FeedbackIgnored, FeedbackUnavailable
from .models import (
    AgentChannel,
    ThreadPublicationLineage,
    WorkItem,
)
from .workitem_dispatch import Admission, DispatchConflict
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
    "label_not_admitting",
    "work_item_other_agent",
    "label_conflict",
}

# The one label refusal comment per issue (#4361 decision 9), edited, never
# duplicated, like the `base:` refusal.
LABEL_REFUSAL_MARKER = "curie-factory-label-refusal"


@dataclass(frozen=True)
class VerifiedIssue:
    """What GitHub confirmed about the issue, the tracker that confirmed it, and
    the code host of its repository.

    Base resolution reads the branch through the code host and comments a
    refusal through the tracker.
    """

    labels: set[str]
    default_branch: str | None
    tracker: GitHubTracker
    code_host: CodeHost
    repository: RepositoryRef


def ignored(code: str) -> WebhookResult:
    return WebhookResult(status="factory_ignored", errors=[{"code": code}])


async def _tracker(
    notice: FactoryNotice, *, registry: ForgeRegistry, client: httpx.AsyncClient
) -> GitHubTracker:
    """The notice's repository tracker, under the token for its installation."""

    try:
        token = await run_in_threadpool(
            factory_credentials(registry).token_for_verified_installation,
            notice.repo_full_name,
            notice.installation_id,
        )
    except (GitHubInstallationRefused, ValueError):
        raise FactoryRefused("installation_unverified") from None
    except GitHubAppError:
        raise FeedbackUnavailable("installation_unavailable") from None
    return factory_tracker(registry).for_repository(
        client,
        repo_full_name=notice.repo_full_name,
        repository_id=notice.repository_id,
        token=static_token(token),
    )


async def verify_current(
    notice: FactoryNotice,
    *,
    settings: Settings,
    client: httpx.AsyncClient,
) -> VerifiedIssue:
    registry = forges_registry(settings)
    tracker = await _tracker(notice, registry=registry, client=client)
    facts = await tracker.verify_notice(notice)
    return VerifiedIssue(
        labels=facts.labels,
        default_branch=facts.default_branch,
        tracker=tracker,
        code_host=factory_code_host(registry, client),
        repository=notice_repository(notice, registry),
    )


def notice_issue(notice: FactoryNotice, registry: ForgeRegistry) -> TrackerIssueRef:
    """The tracker issue a GitHub notice names, on the GitHub tracker's host."""

    host = factory_tracker(registry).config.host
    return TrackerIssueRef(GITHUB, host, str(notice.repository_id), str(notice.issue_number))


def notice_repository(notice: FactoryNotice, registry: ForgeRegistry) -> RepositoryRef:
    """The repository a GitHub notice's issue lives on: its own (ADR 0197 rule 3)."""

    return factory_repository(
        registry, path=notice.repo_full_name, project_id=notice.repository_id
    )


async def lock_issue(session: AsyncSession, issue: TrackerIssueRef) -> None:
    """Hold one issue until this transaction commits.

    Admission and cancellation both re-read GitHub before they touch the
    WorkItem. Without this lock a closure can be stored as absent, and the
    admission that was already in flight then creates work the closure will
    not see again.
    """

    classid, objid = issue_lock_keys(issue)
    await session.execute(
        text("SELECT pg_advisory_xact_lock(CAST(:classid AS integer), CAST(:objid AS integer))"),
        {"classid": classid, "objid": objid},
    )


def _facts(notice: FactoryNotice, binding: AgentChannel, registry: ForgeRegistry) -> Admission:
    """The admission facts of a verified notice on its resolved surface.

    A label admission carries the surface's effective trigger label, which the
    WorkItem stores (#4361 decision 10); a mention starts nothing by label.
    """

    base = factory_code_host_entry(registry).html_base.rstrip("/")
    objective = f"{base}/{notice.repo_full_name}/issues/{notice.issue_number}"
    if notice.disposition == "mention":
        objective = f"{objective}#issuecomment-{notice.comment_id}"
    kind, address, reply_conversation_id = github_reply_route(
        notice.repo_full_name, notice.issue_number
    )
    return Admission(
        agent_id=binding.agent_id,
        kind=kind,
        address=address,
        reply_conversation_id=reply_conversation_id,
        issue=notice_issue(notice, registry),
        repository=notice_repository(notice, registry),
        code_host_installation_id=notice.installation_id,
        objective=objective,
        requester=f"github:{notice.sender_id}:{notice.sender_login}",
        request_id=notice.request_id,
        trigger_label=(
            effective_trigger_label(binding, registry) if notice.disposition == "admit" else None
        ),
    )


async def work_item_for(session: AsyncSession, issue: TrackerIssueRef) -> WorkItem | None:
    found: WorkItem | None = await session.scalar(
        select(WorkItem).where(*for_tracker_issue(issue))
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
        factory_base.bases_for(forges_registry(settings), item.repository_path),
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
) -> factory_base.ResolvedBase:
    """Resolve the base for a fresh admission, or comment and refuse."""

    tracker = verified.tracker
    resolved = await factory_base.resolve_base(
        registry=forges_registry(settings),
        repo_full_name=notice.repo_full_name,
        labels=verified.labels,
        default_branch=verified.default_branch,
        code_host=verified.code_host,
        repository=verified.repository,
    )
    if isinstance(resolved, BaseRefusal):
        await factory_base.comment_refusal(
            tracker.marked_comments, tracker.issue(notice.issue_number), resolved
        )
        raise FactoryRefused(resolved.code)
    return resolved


async def admitting_label(
    session: AsyncSession, registry: ForgeRegistry, item: WorkItem
) -> str | None:
    """The label whose removal cancels ``item`` (#4361 decision 10).

    The label stored when it was admitted or last readmitted. A WorkItem
    admitted before that was stored compares with its agent's GitHub surface on
    the repository when it still holds one, else with the configured GitHub
    tracker label. Never resolves a binding by label (ADR 0161).
    """

    if item.trigger_label is not None:
        return item.trigger_label
    binding = await agent_binding(session, item.agent_id, item.repository_path)
    if binding is not None:
        return effective_trigger_label(binding, registry)
    tracker = registry.github_tracker()
    return None if tracker is None else tracker.config.label


def _label_span(label: str | None) -> str:
    return f"`{label}`" if label is not None else "its original label"


async def _comment_label_refusal(tracker: GitHubTracker, number: int, body: str) -> None:
    try:
        await tracker.marked_comments.upsert_marked(
            ReplyTarget.on_issue(tracker.issue(number)), LABEL_REFUSAL_MARKER, body
        )
    except ForgeError:
        raise FeedbackUnavailable("label_refusal_unavailable") from None


async def _refuse_label_admission(
    session: AsyncSession,
    registry: ForgeRegistry,
    notice: FactoryNotice,
    verified: VerifiedIssue,
    existing: WorkItem | None,
    binding: AgentChannel,
) -> frozenset[str]:
    """Refuse a label admission another label owns, with a marked comment
    (#4361 decision 9). Deterministic: it reads the labels present now.
    Returns the labels the repository's surfaces serve.

    1. ``work_item_other_agent``: the issue's WorkItem, cancelled or not,
       belongs to another agent that still serves the repository; its
       conversation and PR lineage stay there. When that agent no longer
       serves it (its surface moved), dispatch refuses ``identity_mismatch``.
    2. ``label_conflict``: the issue carries two or more labels bound on the
       repository. A run already admitted is not touched.
    """

    if (
        existing is not None
        and existing.agent_id != binding.agent_id
        and await agent_binding(session, existing.agent_id, notice.repo_full_name) is not None
    ):
        owner = await admitting_label(session, registry, existing)
        await _comment_label_refusal(
            verified.tracker,
            notice.issue_number,
            f"Curie did not start this issue under {_label_span(notice.label)}: it belongs to the "
            f"run the {_label_span(owner)} label started, which is another agent's. Open a new "
            f"issue to run it under {_label_span(notice.label)}.",
        )
        raise FactoryRefused("work_item_other_agent")
    bound = await bound_labels(session, registry, notice.repo_full_name)
    present = sorted(label for label in bound if label in verified.labels)
    if len(present) > 1:
        await _comment_label_refusal(
            verified.tracker,
            notice.issue_number,
            "Curie did not start this issue: it carries "
            f"{', '.join(_label_span(label) for label in present)}, which start different "
            "agents on this repository. Keep exactly one of them; Curie will pick the issue "
            "up on its next pass.",
        )
        raise FactoryRefused("label_conflict")
    return frozenset(bound)


async def _settle_label_refusal(
    tracker: GitHubTracker, number: int, label: str | None, served: frozenset[str]
) -> None:
    """Edit a standing label refusal once the issue is admitted, so it does not
    read as current. Best effort: the admission already stands.

    A refusal can stand only if another served label was ever put on the
    issue, so the issue's label timeline is read first and the comments only
    when it shows one.
    """

    others = served - {label}
    if not others:
        return
    target = ReplyTarget.on_issue(tracker.issue(number))
    try:
        events = await tracker.issue_events(number)
        if not any(
            isinstance(event, dict)
            and event.get("event") == "labeled"
            and isinstance(event.get("label"), dict)
            and event["label"].get("name") in others
            for event in events
        ):
            return
        if await tracker.marked_comments.find_marked(target, LABEL_REFUSAL_MARKER) is None:
            return
        await tracker.marked_comments.upsert_marked(
            target,
            LABEL_REFUSAL_MARKER,
            f"Curie started this issue under {_label_span(label)}; the earlier label refusal no "
            "longer applies.",
        )
    except ForgeError:
        logger.info("factory label refusal on issue %d left unedited", number)


async def admit_notice(
    session: AsyncSession,
    notice: FactoryNotice,
    settings: Settings,
    verified: VerifiedIssue,
) -> WebhookResult:
    registry = forges_registry(settings)
    # Under the issue lock: the WorkItem decides whether the base is resolved
    # again (a fresh admission) or kept (ADR 0186 decision 5).
    existing = await work_item_for(session, notice_issue(notice, registry))
    served: frozenset[str] = frozenset()
    if notice.disposition == "mention":
        if existing is None:
            raise FactoryRefused("not_admitted")
        # A mention routes to the WorkItem's agent's surface (#4361 decision 11).
        found = await agent_binding(session, existing.agent_id, notice.repo_full_name)
        if found is None:
            raise FactoryRefused("binding_missing")
        binding = found
    else:
        binding = await resolve_binding(session, registry, notice)
        served = await _refuse_label_admission(
            session, registry, notice, verified, existing, binding
        )
    facts = _facts(notice, binding, registry)
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
        facts = replace(facts, base=await _fresh_base(notice, verified, settings))
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
        outcome = admission_result(result, facts.request_id)
    # A label refusal can stand only where several labels are served, so a
    # single-label repository pays no read for it.
    if existing is None and len(served) > 1 and outcome.status == "factory_admitted":
        await _settle_label_refusal(
            verified.tracker, notice.issue_number, facts.trigger_label, served
        )
    return outcome


async def record_base_label_notice(
    session: AsyncSession, notice: FactoryNotice, verified: VerifiedIssue, settings: Settings
) -> WebhookResult:
    """A ``base:`` label changed: record whether it now disagrees (ADR 0186)."""

    registry = forges_registry(settings)
    item = await work_item_for(session, notice_issue(notice, registry))
    if item is None:
        raise FactoryRefused("work_item_absent")
    if not _same_notice_identity(item, notice, registry):
        raise FactoryRefused("identity_mismatch")
    await _record_label(session, item, verified, settings)
    return ignored("base_label_recorded")


def _same_notice_identity(item: WorkItem, notice: FactoryNotice, registry: ForgeRegistry) -> bool:
    return item.code_host_installation_id == notice.installation_id and same_repository(
        item.repository, notice_repository(notice, registry)
    )


async def cancel_notice(
    session: AsyncSession, notice: FactoryNotice, settings: Settings
) -> WebhookResult:
    """Cancel one verified close or unlabel. Callers commit the session."""

    registry = forges_registry(settings)
    item = await work_item_for(session, notice_issue(notice, registry))
    if item is None:
        raise FactoryRefused("work_item_absent")
    if not _same_notice_identity(item, notice, registry):
        raise FactoryRefused("identity_mismatch")
    # Closing cancels whatever label admitted it; removing a label cancels
    # only when it is the label that admitted it (#4361 decision 10).
    if notice.action == "unlabeled" and notice.label != await admitting_label(
        session, registry, item
    ):
        return ignored("label_not_admitting")
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

    tracker = await _tracker(notice, registry=forges_registry(settings), client=client)
    label = notice.label or tracker.label
    try:
        events = await tracker.issue_events(notice.issue_number)
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
        registry = forges_registry(settings)
        intake = factory_tracker(registry).config
        # A label event is judged against the labels bound on its repository
        # (#4361 decision 15), so a repository outside the allowlist is
        # refused before its surfaces are read.
        repository = event_repository(payload)
        if repository is not None and not repository_is_allowed(
            repository, settings.github_repo_allowlist
        ):
            raise FactoryRefused("repository_not_allowed")
        labels = (
            frozenset()
            if repository is None
            else frozenset(await bound_labels(session, registry, repository))
        )
        notice = parse_factory_event(
            event,
            payload,
            delivery_id,
            labels=labels,
            mention=intake.mention,
        )
        await lock_issue(session, notice_issue(notice, registry))
        verified = await verify_current(notice, settings=settings, client=client)
        if notice.disposition == "admit":
            notice = await _with_label_event(notice, settings=settings, client=client)
        if notice.disposition == "cancel":
            outcome = await cancel_notice(session, notice, settings)
        elif notice.disposition == "base_label":
            outcome = await record_base_label_notice(session, notice, verified, settings)
        else:
            outcome = await admit_notice(session, notice, settings, verified)
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
