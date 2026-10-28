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

from . import factory_rediscovery, workitem_dispatch
from .config import Settings
from .factory_admission import (
    LABEL_REFUSAL_MARKER,
    cancel_work_item,
    ignored,
    label_refusal,
    label_span,
)
from .forges.errors import ForgeError, Unavailable
from .forges.github.binding import GITHUB_CHANNEL_KIND, resolve_binding
from .forges.github.comments import static_token
from .forges.github.identity import delivery_uuid
from .forges.github.tracker import GitHubTracker, IssueFacts, last_label_event
from .forges.github.transport import parse_time
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
from .forges.surfaces import agent_binding, bound_labels, effective_trigger_label
from .forges.trackers import reply_route
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
    "publication_exists",
}


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
    return _verified(notice, registry, client, tracker, facts)


async def verify_polled(
    notice: FactoryNotice,
    *,
    settings: Settings,
    client: httpx.AsyncClient,
    tracker: GitHubTracker,
    repository: Any,
) -> VerifiedIssue:
    """`verify_current` for a notice a poll pass built.

    The pass already minted its repository's installation token and read the
    repository once (#4361 decision 14), so the notice is confirmed with that
    ``tracker`` and against that ``repository`` object; only the issue is
    re-read, for the labels admission and cancellation judge.
    """

    registry = forges_registry(settings)
    facts = await tracker.verify_issue(notice, repository)
    return _verified(notice, registry, client, tracker, facts)


def _verified(
    notice: FactoryNotice,
    registry: ForgeRegistry,
    client: httpx.AsyncClient,
    tracker: GitHubTracker,
    facts: IssueFacts,
) -> VerifiedIssue:
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

    return factory_repository(registry, path=notice.repo_full_name, project_id=notice.repository_id)


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
    kind, address, reply_conversation_id = reply_route(
        registry, notice_issue(notice, registry), notice.repo_full_name
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
    found: WorkItem | None = await session.scalar(select(WorkItem).where(*for_tracker_issue(issue)))
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


def admitting_label(registry: ForgeRegistry, item: WorkItem) -> str | None:
    """The label whose removal cancels ``item`` (#4361 decision 10).

    The label stored when it was admitted or last readmitted. A WorkItem with
    no stored label predates the column, when the configured GitHub tracker
    label was the only install label, so it compares with that alone. Never
    consults a binding: a removed or moved surface cannot change what cancels
    the run (ADR 0161).
    """

    if item.trigger_label is not None:
        return item.trigger_label
    tracker = registry.github_tracker()
    return None if tracker is None else tracker.config.label


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
       belongs to another agent, whether or not that agent still serves the
       repository; its conversation and PR lineage stay there.
    2. ``label_conflict``: the issue carries two or more labels bound on the
       repository. A run already admitted is not touched.
    """

    bound = await bound_labels(session, registry, GITHUB_CHANNEL_KIND, notice.repo_full_name)
    refusal = label_refusal(
        label=notice.label,
        owner_label=None if existing is None else admitting_label(registry, existing),
        other_agent=existing is not None and existing.agent_id != binding.agent_id,
        present=sorted(label for label in bound if label in verified.labels),
    )
    if refusal is not None:
        code, body = refusal
        await _comment_label_refusal(verified.tracker, notice.issue_number, body)
        raise FactoryRefused(code)
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
            f"Curie started this issue under {label_span(label)}; the earlier label refusal no "
            "longer applies.",
        )
    except ForgeError:
        logger.info("factory label refusal on issue %d left unedited", number)


async def _refuse_rediscovered(
    notice: FactoryNotice,
    settings: Settings,
    client: httpx.AsyncClient,
    verified: VerifiedIssue,
) -> None:
    """Refuse a label admission with no local WorkItem when the code host
    already has this issue's factory publication (#4629).

    Raises `FactoryRefused` ``publication_exists`` (an open, ambiguous or
    unconfirmed one is named on the issue; a completed one whose trigger this
    replays is logged), or `FeedbackUnavailable` when the evidence cannot be
    read completely, so the caller defers instead of admitting.
    """

    issue = notice_issue(notice, forges_registry(settings))
    code_host = factory_rediscovery.publishing_code_host(settings, client, verified.repository)
    try:
        found = await factory_rediscovery.rediscover(
            verified.tracker,
            code_host,
            issue,
            verified.repository,
            event_at=notice.label_event_at,
        )
    except ForgeError:
        raise FeedbackUnavailable("publication_listing_unavailable") from None
    if found.verdict == "clear":
        return
    try:
        await factory_rediscovery.report(verified.tracker, issue, found)
    except ForgeError:
        raise FeedbackUnavailable("publication_report_unavailable") from None
    raise FactoryRefused("publication_exists")


async def admit_notice(
    session: AsyncSession,
    notice: FactoryNotice,
    settings: Settings,
    verified: VerifiedIssue,
    *,
    client: httpx.AsyncClient,
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
        found = await agent_binding(
            session, existing.agent_id, GITHUB_CHANNEL_KIND, notice.repo_full_name
        )
        if found is None:
            raise FactoryRefused("binding_missing")
        binding = found
    else:
        binding = await resolve_binding(session, registry, notice)
        served = await _refuse_label_admission(
            session, registry, notice, verified, existing, binding
        )
    if existing is None and notice.disposition == "admit":
        # No local history: the code host's own publications decide (#4629).
        await _refuse_rediscovered(notice, settings, client, verified)
    facts = _facts(notice, binding, registry)
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
    session: AsyncSession,
    notice: FactoryNotice,
    verified: VerifiedIssue,
    settings: Settings,
    *,
    client: httpx.AsyncClient,
) -> WebhookResult:
    """Reconcile a verified change to the current base selection."""

    registry = forges_registry(settings)
    item = await work_item_for(session, notice_issue(notice, registry))
    if item is None:
        raise FactoryRefused("work_item_absent")
    if not _same_notice_identity(item, notice, registry):
        raise FactoryRefused("identity_mismatch")
    from .factory_base_reconcile import observe_base_labels

    await observe_base_labels(
        session,
        item=item,
        labels=verified.labels,
        default_branch=verified.default_branch,
        event_identity=str(notice.request_id),
        settings=settings,
        client=client,
    )
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
    if notice.action == "unlabeled" and notice.label != admitting_label(registry, item):
        return ignored("label_not_admitting")
    return await cancel_work_item(session, item)


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
    return replace(
        notice,
        label_event_id=event["id"],
        label_event_at=parse_time(event.get("created_at")),
    )


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
            else frozenset(await bound_labels(session, registry, GITHUB_CHANNEL_KIND, repository))
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
            outcome = await record_base_label_notice(
                session, notice, verified, settings, client=client
            )
        else:
            outcome = await admit_notice(session, notice, settings, verified, client=client)
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
