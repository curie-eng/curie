"""Signed GitHub issue intake for one canonical WorkItem.

Signature verification stays on the webhook route. This module checks the
installation and repository allowlist, has the GitHub tracker adapter confirm
the issue and the sender's permission, then calls the generic WorkItem
service. It does not read issue bodies into the platform and it does not bind
Slack.
"""

import logging
import uuid
from collections.abc import Awaitable, Callable, Collection, Mapping
from dataclasses import dataclass, replace
from datetime import datetime
from typing import Any

import httpx
from fastapi import HTTPException
from sqlalchemy import cast, func, select, text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from starlette.concurrency import run_in_threadpool

from curie_api.schemas.deployments import WebhookResult
from curie_api.workitems.lifecycle import (
    WorkItemConflict,
    WorkItemOutcome,
    for_tracker_issue,
    same_repository,
)

from . import factory_intake_notices, factory_questions, factory_rediscovery, workitem_dispatch
from .config import Settings, get_settings
from .factory_admission import (
    LABEL_REFUSAL_MARKER,
    cancel_work_item,
    ignored,
    label_refusal,
    label_span,
)
from .factory_facts_schema import InvalidFactsRequest, parse_request
from .forges.errors import ForgeError, NotFound, Unavailable
from .forges.github.binding import GITHUB_CHANNEL_KIND, resolve_binding
from .forges.github.comments import static_token
from .forges.github.identity import delivery_uuid
from .forges.github.review_polling import trailing_id
from .forges.github.tracker import GitHubTracker, IssueFacts, last_label_event
from .forges.github.transport import parse_time
from .forges.hosts import (
    factory_app_slug,
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
from .github_review_audit import claim_review_delivery, comment_refusal, settle_review_delivery
from .github_review_events import FeedbackIgnored, FeedbackUnavailable, label_actor
from .models import (
    AgentChannel,
    ExecutionRequest,
    FactoryAnswerReceipt,
    FactoryPollCursor,
    FactoryQuestionRound,
    FactorySlackThread,
    PersonalClaudeEnrollment,
    WorkItem,
)
from .personal_execution import ModelAuthMode, OwnerContext, existing_work_item_refusal
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
    # Answer intake (#5787): a comment while a question is open that is not
    # admitted as an answer, or that the answer rule refuses.
    "answer_from_bot",
    "answer_not_authorized",
    "ticket_closed",
    "trigger_label_absent",
    "comment_unavailable",
    "binding_missing",
    "installation_mismatch",
    "not_outstanding",
    "not_after_question",
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


@dataclass(frozen=True)
class Selection:
    """One trusted selection of one issue for admission (#5588 decision 7).

    A verified human label event or mention (`selection_of`). ``requester`` is the
    ``github:<id>:<login>`` attribution the request records;
    ``label_event_at`` is when the selecting label was applied, when known.
    """

    disposition: str
    repo_full_name: str
    repository_id: int
    issue_number: int
    installation_id: int
    label: str | None
    requester: str
    request_id: uuid.UUID
    label_event_at: datetime | None = None
    comment_id: int | None = None
    principal: uuid.UUID | None = None
    owner: OwnerContext | None = None


def selection_of(notice: FactoryNotice) -> Selection:
    """The selection a verified notice makes."""

    return Selection(
        disposition=notice.disposition,
        repo_full_name=notice.repo_full_name,
        repository_id=notice.repository_id,
        issue_number=notice.issue_number,
        installation_id=notice.installation_id,
        label=notice.label,
        requester=f"github:{notice.sender_id}:{notice.sender_login}",
        request_id=notice.request_id,
        label_event_at=notice.label_event_at,
        comment_id=notice.comment_id,
    )


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


def notice_issue(notice: FactoryNotice | Selection, registry: ForgeRegistry) -> TrackerIssueRef:
    """The tracker issue a GitHub notice names, on the GitHub tracker's host."""

    host = factory_tracker(registry).config.host
    return TrackerIssueRef(GITHUB, host, str(notice.repository_id), str(notice.issue_number))


def notice_repository(notice: FactoryNotice | Selection, registry: ForgeRegistry) -> RepositoryRef:
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


def _facts(selection: Selection, binding: AgentChannel, registry: ForgeRegistry) -> Admission:
    """The admission facts of a trusted selection on its resolved surface.

    A label admission carries the surface's effective trigger label, which the
    WorkItem stores (#4361 decision 10); a mention starts nothing by label.
    """

    base = factory_code_host_entry(registry).html_base.rstrip("/")
    objective = f"{base}/{selection.repo_full_name}/issues/{selection.issue_number}"
    if selection.disposition == "mention":
        objective = f"{objective}#issuecomment-{selection.comment_id}"
    kind, address, reply_conversation_id = reply_route(
        registry, notice_issue(selection, registry), selection.repo_full_name
    )
    return Admission(
        agent_id=binding.agent_id,
        kind=kind,
        address=address,
        reply_conversation_id=reply_conversation_id,
        issue=notice_issue(selection, registry),
        repository=notice_repository(selection, registry),
        code_host_installation_id=selection.installation_id,
        objective=objective,
        requester=selection.requester,
        request_id=selection.request_id,
        trigger_label=(
            effective_trigger_label(binding, registry) if selection.disposition == "admit" else None
        ),
        model_auth_mode=get_settings().factory_model_auth_mode,
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


async def kickoff_work_items(
    session: AsyncSession, item_ids: Collection[uuid.UUID]
) -> frozenset[uuid.UUID]:
    """The WorkItems among ``item_ids`` a Slack kickoff started (#5380).

    No label admitted them (no stored trigger label) and their factory Slack
    thread's origin is ``kickoff``. A WorkItem a label later readmitted stores
    that label, so it is not one of them.
    """

    if not item_ids:
        return frozenset()
    found = await session.scalars(
        select(WorkItem.id)
        .join(FactorySlackThread, FactorySlackThread.work_item_id == WorkItem.id)
        .where(
            WorkItem.id.in_(list(item_ids)),
            WorkItem.trigger_label.is_(None),
            FactorySlackThread.origin == "kickoff",
        )
    )
    return frozenset(found)


def admitting_label(
    registry: ForgeRegistry, item: WorkItem, *, kickoff_items: Collection[uuid.UUID]
) -> str | None:
    """The label whose removal cancels ``item`` (#4361 decision 10), or None.

    The label stored when it was admitted or last readmitted. A WorkItem a
    Slack kickoff started (one of ``kickoff_items``, from
    `kickoff_work_items`) has none: no label removal, earlier or later,
    cancels it, while closing its issue still does (#5380). Any other WorkItem
    with no stored label predates the column, when the configured GitHub
    tracker label was the only install label, so it compares with that alone.
    Never consults a binding: a removed or moved surface cannot change what
    cancels the run (ADR 0161).
    """

    if item.trigger_label is not None:
        return item.trigger_label
    if item.id in kickoff_items:
        return None
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
    selection: Selection,
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

    bound = await bound_labels(session, registry, GITHUB_CHANNEL_KIND, selection.repo_full_name)
    refusal = label_refusal(
        label=selection.label,
        owner_label=(
            None
            if existing is None
            else admitting_label(
                registry,
                existing,
                kickoff_items=await kickoff_work_items(session, [existing.id]),
            )
        ),
        other_agent=existing is not None and existing.agent_id != binding.agent_id,
        present=sorted(label for label in bound if label in verified.labels),
    )
    if refusal is not None:
        code, body = refusal
        await _comment_label_refusal(verified.tracker, selection.issue_number, body)
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
    agent_id: uuid.UUID,
    selection: Selection,
    settings: Settings,
    client: httpx.AsyncClient,
    verified: VerifiedIssue,
    publications: factory_rediscovery.PassPublications | None = None,
) -> str | None:
    """Refuse a label admission with no local WorkItem when the code host
    already has this issue's factory publication (#4629).

    Open and ambiguous publications keep their existing notice. A replay of
    completed work answers ``publication_completed`` with the publication's
    link, which the human path owes as an intake notice. An incomplete read is
    retried.
    """

    issue = notice_issue(selection, forges_registry(settings))
    code_host = factory_rediscovery.publishing_code_host(settings, client, verified.repository)
    try:
        found = await factory_rediscovery.rediscover(
            verified.tracker,
            code_host,
            issue,
            verified.repository,
            event_at=selection.label_event_at,
            publications=publications,
        )
    except ForgeError:
        raise FeedbackUnavailable("publication_listing_unavailable") from None
    if found.verdict == "clear":
        return None
    if found.verdict == "completed":
        return found.publications[0].url
    try:
        await factory_rediscovery.report(verified.tracker, issue, found)
    except ForgeError:
        raise FeedbackUnavailable("publication_report_unavailable") from None
    raise FactoryRefused("publication_exists")


@dataclass(frozen=True)
class Selected:
    """What the shared admission service decided for one trusted selection.

    ``notice_reason`` names the intake notice a human selection owes
    (``publication_url`` with ``publication_completed``); an internal selection
    owes none and answers on its own source. ``result`` is the dispatch result
    when admission ran.
    """

    outcome: WebhookResult
    notice_reason: str | None = None
    publication_url: str | None = None
    result: WorkItemOutcome | WorkItemConflict | DispatchConflict | None = None


async def admit_selection(
    session: AsyncSession,
    selection: Selection,
    settings: Settings,
    verified: VerifiedIssue,
    existing: WorkItem | None,
    binding: AgentChannel,
    *,
    client: httpx.AsyncClient,
    reconcile_only: bool = False,
    publications: factory_rediscovery.PassPublications | None = None,
) -> Selected:
    """The trusted-selection half of admission, shared by every path (#5588 decision 7).

    The caller has verified who selected the issue (a human label event or a
    mention) and resolved the binding. This applies
    the ownership, label conflict, rediscovery and binding checks of ordinary
    admission, then readmits through `workitem_dispatch` under the caller's
    issue lock. It records no intake notice: it names the one owed instead.
    """

    registry = forges_registry(settings)
    served: frozenset[str] = frozenset()
    if selection.disposition == "admit":
        served = await _refuse_label_admission(
            session, registry, selection, verified, existing, binding
        )
    if existing is None and selection.disposition == "admit":
        # No local history: the code host's own publications decide (#4629).
        completed = await _refuse_rediscovered(
            binding.agent_id, selection, settings, client, verified, publications
        )
        if completed is not None:
            return Selected(
                ignored("publication_completed"),
                notice_reason="publication_completed",
                publication_url=completed,
            )
    facts = _facts(selection, binding, registry)
    if reconcile_only and existing is not None:
        if (
            existing.agent_id == binding.agent_id
            and existing.tracker_issue == facts.issue
            and same_repository(existing.repository, facts.repository)
            and not workitem_dispatch.work_item_matches(existing, facts, binding.adapter)
        ):
            return Selected(ignored("installation_changed"), notice_reason="installation_changed")
        return Selected(ignored("work_item_present"))
    if existing is None and selection.disposition == "admit":
        facts = replace(facts, requester_principal_id=selection.principal, owner=selection.owner)
    # Dispatch helpers commit between WorkItem creation and request creation.
    # Keep those commits inside savepoints so the caller's issue lock remains
    # held until admission and delivery settlement commit together.
    async with AsyncSession(
        bind=await session.connection(),
        join_transaction_mode="create_savepoint",
        expire_on_commit=False,
    ) as admission:
        if selection.disposition == "mention":
            result = await workitem_dispatch.admit_revision(admission, facts)
        else:
            result = await workitem_dispatch.readmit(admission, facts)
        if existing is None and isinstance(result, WorkItemOutcome):
            from .factory_base_reconcile import initialize_base_selection

            await initialize_base_selection(
                admission,
                item_id=result.work_item.id,
                labels=verified.labels,
                default_branch=verified.default_branch,
                settings=settings,
            )
        outcome = admission_result(result, facts.request_id)
    if selection.disposition == "admit" and isinstance(result, DispatchConflict):
        reason: str | None = None
        if result.code == "personal_owner_required":
            reason = result.code
        elif (
            result.code == "identity_mismatch"
            and existing is not None
            and (
                existing.agent_id == binding.agent_id
                and existing.tracker_issue == facts.issue
                and same_repository(existing.repository, facts.repository)
                and not workitem_dispatch.work_item_matches(existing, facts, binding.adapter)
            )
        ):
            reason = "installation_changed"
        if reason is not None:
            return Selected(ignored(reason), notice_reason=reason, result=result)
    # A label refusal can stand only where several labels are served, so a
    # single-label repository pays no read for it.
    if existing is None and len(served) > 1 and outcome.status == "factory_admitted":
        await _settle_label_refusal(
            verified.tracker, selection.issue_number, facts.trigger_label, served
        )
    return Selected(outcome, result=result)


async def admit_notice(
    session: AsyncSession,
    notice: FactoryNotice,
    settings: Settings,
    verified: VerifiedIssue,
    *,
    client: httpx.AsyncClient,
    reconcile_only: bool = False,
    publications: factory_rediscovery.PassPublications | None = None,
) -> WebhookResult:
    registry = forges_registry(settings)
    # Under the issue lock: the WorkItem decides whether the base is resolved
    # again (a fresh admission) or kept (ADR 0186 decision 5).
    existing = await work_item_for(session, notice_issue(notice, registry))
    principal: uuid.UUID | None = None
    owner: OwnerContext | None = None
    if notice.disposition == "admit":
        issue = notice_issue(notice, registry)
        assert notice.label_event_id is not None
        if await factory_intake_notices.answered(session, issue, notice.label_event_id):
            return ignored("label_event_answered")
        admitted = await session.get(ExecutionRequest, notice.request_id)
        if admitted is not None or notice.previously_admitted:
            if (
                notice.actor_refusal is None
                and await verified.tracker.label_permission(notice) is None
            ):
                await record_base_label_notice(session, notice, verified, settings, client=client)
            return WebhookResult(status="factory_duplicate")
        grouped = await bound_labels(session, registry, GITHUB_CHANNEL_KIND, notice.repo_full_name)
        matches = grouped.get(notice.label or "", [])
        if not matches:
            raise FactoryRefused("binding_missing")
        agent_id = matches[0].agent_id if len(matches) == 1 else None
        reason = notice.actor_refusal
        if reason is None:
            reason = await verified.tracker.label_permission(notice)
        if reason is None and existing is None and agent_id is not None:
            from .factory_slack import resolve_github_requester

            principal = await resolve_github_requester(
                session,
                agent_id=agent_id,
                tracker_host=issue.host,
                account_id=notice.sender_id,
            )
            owner = None
            if principal is not None and settings.factory_model_auth_mode == "personal":
                enrollment = await session.scalar(
                    select(PersonalClaudeEnrollment).where(
                        PersonalClaudeEnrollment.principal_id == principal,
                        PersonalClaudeEnrollment.active.is_(True),
                    )
                )
                if enrollment is not None:
                    owner = OwnerContext(enrollment.tenant_id, principal, enrollment.generation)
        if (
            reason is None
            and settings.factory_model_auth_mode == "personal"
            and (existing is None or existing.model_auth_mode != "personal")
            and owner is None
        ):
            # A resolved principal with no active enrollment can connect a plan.
            # An unresolved actor still needs a link.
            reason = "enrollment_inactive" if principal is not None else "personal_owner_required"
        if reason is None and len(matches) > 1:
            reason = "binding_ambiguous"
        if reason is not None:
            await factory_intake_notices.record_notice(
                session,
                notice,
                issue,
                settings=settings,
                reason=reason,
                agent_id=agent_id,
            )
            code = "sender_permission_refused" if reason == "actor_no_permission" else reason
            return ignored(code)
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
    selected = await admit_selection(
        session,
        replace(selection_of(notice), principal=principal, owner=owner),
        settings,
        verified,
        existing,
        binding,
        client=client,
        reconcile_only=reconcile_only,
        publications=publications,
    )
    if selected.notice_reason is not None:
        await factory_intake_notices.record_notice(
            session,
            notice,
            notice_issue(notice, registry),
            settings=settings,
            reason=selected.notice_reason,
            agent_id=binding.agent_id,
            publication_url=selected.publication_url,
        )
    return selected.outcome


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
    # only when it is the label that admitted it (#4361 decision 10). A Slack
    # kickoff's WorkItem has no admitting label, so no removal cancels it.
    if notice.action == "unlabeled":
        admitting = admitting_label(
            registry, item, kickoff_items=await kickoff_work_items(session, [item.id])
        )
        if admitting is None or notice.label != admitting:
            return ignored("label_not_admitting")
    return await cancel_work_item(session, item)


# Answers to the factory's questions (#5787) -----------------------------------

ANSWER_FROM_BOT = "answer_from_bot"
ANSWER_NOT_AUTHORIZED = "answer_not_authorized"
TICKET_CLOSED = "ticket_closed"
TRIGGER_LABEL_ABSENT = "trigger_label_absent"
COMMENT_UNAVAILABLE = "comment_unavailable"
# The poll cursor key prefix of an answer comment to read again (#5787): the
# poll pass, or in webhook mode the work item reconciler, drains it.
ANSWER_RETRY = "answer-retry:"
# Rows a comment can answer, or will once the round's tracker write lands.
_ANSWERABLE = frozenset(
    {factory_questions.POSTING, factory_questions.OUTSTANDING, factory_questions.ANSWERED}
)
_ANSWER_RESULTS = {
    factory_questions.ADMITTED: "factory_admitted",
    factory_questions.QUEUED_FOLLOW_UP: "factory_queued",
    factory_questions.COALESCED: "factory_coalesced",
}


def _refuse_answer(
    code: str, item: WorkItem, notice: FactoryNotice, actor: int | None = None
) -> WebhookResult:
    # Identifiers and the reason only: never the comment's text or a token.
    logger.info(
        "factory answer refused reason=%s work_item=%s comment=%s actor=%s",
        code,
        item.id,
        notice.comment_id,
        actor,
    )
    return ignored(code)


def _facts_request(body: object) -> bool:
    if not isinstance(body, str):
        return False
    try:
        parse_request(body)
    except InvalidFactsRequest:
        return False
    return True


async def add_answer_retry(
    session: AsyncSession,
    *,
    kind: str,
    host: str,
    scope_id: str,
    scope_path: str,
    comment_id: int,
) -> None:
    """Keep ``answer-retry:<comment id>`` in the scope's poll cursor, in the
    caller's transaction (#5787).

    The key is merged into the row as it is now (jsonb ``||``, the row created
    with no since values when absent), never written from a cursor snapshot,
    so a concurrent writer of another key is kept. The poll pass's cursor save
    never writes or drops these keys; only this and the retry drain's jsonb
    ``-`` change them.
    """

    added = cast({f"{ANSWER_RETRY}{comment_id}": "pending"}, JSONB)
    await session.execute(
        pg_insert(FactoryPollCursor)
        .values(
            tracker_kind=kind,
            tracker_host=host,
            tracker_scope_id=scope_id,
            scope_path=scope_path,
            etags=added,
        )
        .on_conflict_do_update(
            index_elements=[
                FactoryPollCursor.tracker_kind,
                FactoryPollCursor.tracker_host,
                FactoryPollCursor.tracker_scope_id,
            ],
            set_={
                "etags": FactoryPollCursor.etags.op("||")(added),
                "updated_at": func.now(),
            },
        )
    )


async def record_answer_retry(
    session: AsyncSession, notice: FactoryNotice, registry: ForgeRegistry
) -> None:
    """Keep a deferred webhook answer for a later read (#5787).

    A webhook answer that cannot be decided yet is answered 503, which GitHub
    never redelivers by itself; the key is what reads it again, through the
    poll pass or, in webhook mode, the work item reconciler's answer drain.
    """

    if notice.comment_id is None:
        return
    issue = notice_issue(notice, registry)
    await add_answer_retry(
        session,
        kind=issue.kind,
        host=issue.host,
        scope_id=issue.scope_id,
        scope_path=notice.repo_full_name,
        comment_id=notice.comment_id,
    )


@dataclass(frozen=True)
class _AnswerOwner:
    """An answer's admission facts as the owner guard reads them: the
    install's mode and no named owner, exactly as a mention's (#5787)."""

    model_auth_mode: ModelAuthMode
    owner: None = None


# The answer refusals no later state can lift: they judge who wrote the
# comment, or the ticket as it stood when the comment was decided.
_STANDING_REFUSALS = frozenset(
    {ANSWER_FROM_BOT, ANSWER_NOT_AUTHORIZED, TICKET_CLOSED, TRIGGER_LABEL_ABSENT}
)


async def _closed_round_answer(
    session: AsyncSession, item: WorkItem, notice: FactoryNotice
) -> WebhookResult | None:
    """A comment on an issue whose round has ended: refused again when a
    webhook delivery of it was refused as an answer, for who wrote it or for
    a closed or unlabelled ticket, else None.

    That comment was an answer when it was made, so it never becomes a
    mention revision (#5787 decision 4) when the poll pass first lists it
    after another answer ended the round, however many rounds later, even
    if its author has since gained permission or the ticket was reopened or
    labelled again. The webhook's own recorded verdict decides, not a
    comparison of GitHub's whole-second creation time with the round's end.
    """

    if notice.comment_id is None:
        return None
    reason = await comment_refusal(session, notice.comment_id, _STANDING_REFUSALS)
    if reason is None:
        return None
    return _refuse_answer(reason, item, notice)


async def admit_answer_notice(
    session: AsyncSession,
    settings: Settings,
    tracker: GitHubTracker | Callable[[], Awaitable[GitHubTracker]],
    notice: FactoryNotice,
    comment: Mapping[str, Any] | None,
) -> WebhookResult | None:
    """Recognise one created issue comment as an answer and admit it (#5787).

    The one GitHub recognition rule, shared by the webhook and the poll pass
    (ADR 0161: both doors admit identically). None when the comment is not an
    answer: the issue has no WorkItem, or its question row is absent,
    ``cleared`` or ``exhausted``; the caller then applies its mention or
    ordinary comment rules unchanged. Otherwise the comment is never also a
    mention (decision 4), and the result is the answer's.

    ``comment`` is the factory's own authenticated read of the comment (the
    poll listing); None reads it now through the App, since a webhook payload
    is not trusted for who wrote it or through which App (decision 2). The
    actor rule is ADR 0279's for labels: a ``User`` with a valid id, not acting
    through this factory's own App; a person acting through another App
    passes. Permission, open state and the trigger label are read fresh
    (decision 3). A Slack kickoff's WorkItem has no trigger label, so only the
    open check applies to it. A ``posting`` round is passed to the answer core,
    which refuses it before writing anything (`factory_questions.QUESTION_POSTING`);
    that is `FeedbackUnavailable`, retried until the round is ``outstanding``.

    A comment with an answer receipt was already decided as an answer, so it
    replays (``factory_duplicate``) whatever the round is now and with no
    GitHub read: a mention that answered stays an answer after the round
    clears. A comment whose authenticated body is a facts request (ADR 0280)
    is never an answer. Before any GitHub read the WorkItem's agent must still
    have a GitHub surface on the repository (``binding_missing``) and the
    delivery must come from the WorkItem's installation
    (``installation_mismatch``), and the install's model auth mode must fit
    the WorkItem (`personal_execution.existing_work_item_refusal`, so a
    retained service WorkItem under personal mode is ``personal_owner_required``),
    each as for a mention.

    The caller holds the issue lock (`lock_issue`) and commits; the answer
    core runs in a savepoint of the caller's transaction. ``tracker`` may be a
    factory, resolved only once the comment needs a GitHub read.
    """

    registry = forges_registry(settings)
    issue = notice_issue(notice, registry)
    item = await work_item_for(session, issue)
    if item is None:
        return None
    if notice.comment_id is None:
        raise FactoryRefused("invalid_comment")
    receipt = await session.scalar(
        select(FactoryAnswerReceipt.outcome).where(
            FactoryAnswerReceipt.work_item_id == item.id,
            FactoryAnswerReceipt.comment_key == str(notice.comment_id),
        )
    )
    if receipt is not None:
        logger.info(
            "factory answer replayed %s work_item=%s comment=%s",
            receipt,
            item.id,
            notice.comment_id,
        )
        return WebhookResult(status="factory_duplicate")
    row: FactoryQuestionRound | None = await session.scalar(
        select(FactoryQuestionRound)
        .where(FactoryQuestionRound.work_item_id == item.id)
        .execution_options(populate_existing=True)
    )
    if row is None:
        return None
    if row.state not in _ANSWERABLE:
        return await _closed_round_answer(session, item, notice)
    if item.cancelled_at is not None:
        return _refuse_answer(factory_questions.WORK_ITEM_CANCELLED, item, notice)
    if (
        await agent_binding(session, item.agent_id, GITHUB_CHANNEL_KIND, notice.repo_full_name)
        is None
    ):
        return _refuse_answer("binding_missing", item, notice)
    if (
        item.code_host_installation_id is not None
        and item.code_host_installation_id != notice.installation_id
    ):
        return _refuse_answer("installation_mismatch", item, notice)
    mode_refusal = await existing_work_item_refusal(
        session, item, _AnswerOwner(settings.factory_model_auth_mode)
    )
    if mode_refusal is not None:
        return _refuse_answer(mode_refusal.code, item, notice)
    github = tracker if isinstance(tracker, GitHubTracker) else await tracker()
    if comment is None:
        try:
            comment = await github.issue_comment(notice.comment_id)
        except NotFound:
            return _refuse_answer(COMMENT_UNAVAILABLE, item, notice)
        except ForgeError:
            raise FeedbackUnavailable(COMMENT_UNAVAILABLE) from None
    if comment.get("id") != notice.comment_id or (
        trailing_id(comment.get("issue_url")) != notice.issue_number
    ):
        return _refuse_answer("comment_target_mismatch", item, notice)
    if _facts_request(comment.get("body")):
        # The facts exchange keeps precedence (decision 6), judged on the
        # body GitHub returns now, not the one a payload or listing claimed.
        return None
    try:
        actor_id, actor_login = label_actor(
            {
                "actor": comment.get("user"),
                "performed_via_github_app": comment.get("performed_via_github_app"),
            },
            app_id=github.app_id,
        )
    except FeedbackIgnored as exc:
        code = (
            ANSWER_FROM_BOT if exc.code in {"actor_bot", "actor_own_app"} else ANSWER_NOT_AUTHORIZED
        )
        return _refuse_answer(code, item, notice)
    author = replace(notice, sender_id=actor_id, sender_login=actor_login)
    if await github.label_permission(author) is not None:
        return _refuse_answer(ANSWER_NOT_AUTHORIZED, item, notice, actor_id)
    try:
        state = await github.issue_state(notice.issue_number)
    except NotFound:
        return _refuse_answer(TICKET_CLOSED, item, notice, actor_id)
    except ForgeError:
        raise FeedbackUnavailable("issue_unavailable") from None
    if state.state != "open" or state.pull_request:
        return _refuse_answer(TICKET_CLOSED, item, notice, actor_id)
    label = admitting_label(
        registry, item, kickoff_items=await kickoff_work_items(session, [item.id])
    )
    if label is not None and label not in state.labels:
        return _refuse_answer(TRIGGER_LABEL_ABSENT, item, notice, actor_id)
    facts = factory_questions.AnswerFacts(
        work_item_id=item.id,
        issue=issue,
        comment_id=str(notice.comment_id),
        created_at=parse_time(comment.get("created_at")),
        requester=f"github:{actor_id}:{actor_login}",
    )
    # The answer core commits and rolls back its own session; in a savepoint
    # the issue lock and the webhook's delivery audit stay in the caller's
    # transaction, as admission's dispatch calls do.
    async with AsyncSession(
        bind=await session.connection(),
        join_transaction_mode="create_savepoint",
        expire_on_commit=False,
    ) as answering:
        try:
            outcome = await factory_questions.admit_answer(answering, facts)
        except factory_questions.AnswerNotAdmitted as exc:
            if exc.code == factory_questions.QUESTION_POSTING:
                raise FeedbackUnavailable(exc.code) from None
            return _refuse_answer(exc.code, item, notice, actor_id)
    if outcome.replayed:
        return WebhookResult(status="factory_duplicate")
    status = _ANSWER_RESULTS.get(outcome.outcome)
    if status is None:
        return ignored(outcome.outcome)
    return WebhookResult(status=status)


async def _with_label_event(
    notice: FactoryNotice,
    *,
    settings: Settings,
    client: httpx.AsyncClient,
) -> FactoryNotice:
    """Attach the newest label event and its actor. The delivery id stays put.

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
    if event is None:
        raise FeedbackUnavailable("label_events_unavailable")
    return factory_intake_notices.observe_label(
        replace(notice, label_event_at=parse_time(event.get("created_at"))),
        event,
        app_id=tracker.app_id,
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
            app_slug=await factory_app_slug(registry),
        )
        await lock_issue(session, notice_issue(notice, registry))
        answered: WebhookResult | None = None
        if notice.event == "issue_comment":
            # A comment is an answer first (#5787 decision 4); only a comment
            # that is not one goes on to today's mention or comment rules.
            commented = notice
            try:
                answered = await admit_answer_notice(
                    session,
                    settings,
                    lambda: _tracker(commented, registry=registry, client=client),
                    notice,
                    None,
                )
            except FeedbackUnavailable:
                # Settled with the retryable audit below, in this transaction.
                await record_answer_retry(session, commented, registry)
                raise
            if answered is None and notice.disposition == "comment":
                raise FactoryRefused(notice.comment_refusal or "ordinary_comment")
        if answered is not None:
            outcome = answered
        else:
            if notice.disposition == "admit":
                notice = await _with_label_event(notice, settings=settings, client=client)
            verified = await verify_current(notice, settings=settings, client=client)
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
    try:
        await factory_intake_notices.sync_intake_notices(
            async_sessionmaker(session.bind, expire_on_commit=False),
            settings,
            owner="webhook-intake",
            client=client,
            limit=1,
        )
    except Exception:
        logger.exception("factory intake notice delivery deferred")
    return outcome
