"""Resolve request bases and restart work only before publication authority exists."""

from __future__ import annotations

import asyncio
import hashlib
import uuid
from datetime import datetime, timedelta

import httpx
from sqlalchemy import func, or_, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from . import factory_base
from .config import Settings
from .factory_notices import mark_status_comment_stale
from .forges.errors import ForgeError, Unavailable
from .forges.registry import forges_registry
from .forges.trackers import (
    code_host_for_repository,
    reply_route,
    tracker_for_work_item,
    work_item_policy,
)
from .github_review_events import FeedbackUnavailable
from .models import (
    ExecutionRequest,
    Publication,
    PublicationReviewReservation,
    ThreadPublicationLineage,
    WorkItem,
)
from .workitems import lifecycle

_READ_SECONDS = 8.0


def _lineage_empty(lineage: ThreadPublicationLineage) -> bool:
    return (
        lineage.status == "open"
        and lineage.head_sha is None
        and lineage.pr_number is None
        and lineage.pr_url is None
        and lineage.code_host_pr_id is None
    )


async def _record_requested_base(
    session: AsyncSession,
    item: WorkItem,
    choice: factory_base.BaseChoice | factory_base.ResolvedBase,
) -> None:
    if (item.base_requested_branch, item.base_requested_source) == (choice.branch, choice.source):
        return
    item.base_requested_branch, item.base_requested_source = choice.branch, choice.source
    item.version += 1
    await mark_status_comment_stale(session, item.id)


async def _defer_selection(
    session: AsyncSession,
    request: ExecutionRequest,
    reason: str,
    *,
    settle_until: datetime | None = None,
) -> bool:
    await session.execute(
        update(ExecutionRequest)
        .where(
            ExecutionRequest.id == request.id,
            ExecutionRequest.status == "waiting",
            ExecutionRequest.version == request.version,
        )
        .values(
            dispatch_generation=ExecutionRequest.dispatch_generation + 1,
            dispatch_not_before=func.clock_timestamp() + timedelta(seconds=5),
            acquired_generation=None,
            acquire_owner=None,
            acquire_expires_at=None,
            last_deferral_reason=reason,
            updated_at=func.clock_timestamp(),
            **({"base_settle_until": settle_until} if settle_until is not None else {}),
        )
    )
    await session.commit()
    return False


async def resolve_for_acquire(
    session: AsyncSession,
    *,
    request_id: uuid.UUID,
    owner: str,
    generation: int,
    settings: Settings,
    client: httpx.AsyncClient,
) -> bool:
    session.info.pop("factory_base_pin", None)
    request = await session.get(ExecutionRequest, request_id)
    item = await session.get(WorkItem, request.work_item_id) if request else None
    if (
        request is None
        or item is None
        or request.status != "waiting"
        or item.cancelled_at is not None
        or request.dispatch_generation != generation
    ):
        await session.commit()
        return True
    now = await lifecycle.database_now(session)
    if request.wait_deadline is None or request.wait_deadline <= now:
        await session.commit()
        return True
    held = (
        request.acquire_owner is not None
        and request.acquire_owner != owner
        and request.acquire_expires_at is not None
        and request.acquire_expires_at > now
    )
    if held:
        await session.commit()
        return True
    if item.base_resolved_request_id == request.id or item.trigger_label is None:
        return True
    settle_until = request.base_settle_until
    if settle_until is None:
        prior_clock = await session.scalar(
            select(ExecutionRequest.base_settle_until)
            .where(
                ExecutionRequest.work_item_id == item.id,
                ExecutionRequest.sequence < request.sequence,
                ExecutionRequest.base_settle_until.is_not(None),
            )
            .order_by(ExecutionRequest.sequence.desc())
            .limit(1)
        )
        first_created_at = await session.scalar(
            select(func.min(ExecutionRequest.created_at)).where(
                ExecutionRequest.work_item_id == item.id,
                ExecutionRequest.sequence <= request.sequence,
            )
        )
        settle_until = prior_clock or (first_created_at or request.created_at) + timedelta(
            seconds=settings.factory_base_label_settle_s
        )
    if settle_until > now:
        return await _defer_selection(
            session, request, "base_label_settling", settle_until=settle_until
        )
    registry = forges_registry(settings)
    issue, repository, installation = (
        item.tracker_issue,
        item.repository,
        item.code_host_installation_id,
    )
    policy = work_item_policy(registry, issue, repository)
    item_id, item_version, request_version = item.id, item.version, request.version
    trigger_label, lineage_id = item.trigger_label, item.publication_lineage_id
    lineage = await session.get(ThreadPublicationLineage, lineage_id) if lineage_id else None
    lineage_version = lineage.version if lineage is not None else None
    lineage_base = lineage.base_ref if lineage is not None else None
    lineage_branch = lineage.branch if lineage is not None and _lineage_empty(lineage) else None
    await session.commit()
    try:
        async with asyncio.timeout(_READ_SECONDS):
            tracker = tracker_for_work_item(
                registry, client, issue, repository, installation, timeout_s=_READ_SECONDS
            )
            ticket = await tracker.ticket_labels(issue)
            host = code_host_for_repository(
                registry,
                client,
                repository,
                installations={repository.path: installation} if installation is not None else {},
            )
            current_repository = await host.resolve_repository(repository.project_id)
            if current_repository != repository or current_repository.path != repository.path:
                raise Unavailable("repository_identity_changed")
            if trigger_label not in ticket.labels:
                raise Unavailable("factory_label_absent")
            resolved = await factory_base.resolve_base(
                policy=policy,
                labels=set(ticket.labels),
                default_branch=current_repository.default_branch,
                code_host=host,
                repository=repository,
            )
            remote_lineage_head = (
                await host.branch_head(repository, lineage_branch)
                if (
                    lineage_branch is not None
                    and not isinstance(resolved, factory_base.BaseRefusal)
                    and lineage_base != resolved.branch
                )
                else None
            )
    except (ForgeError, FeedbackUnavailable, TimeoutError):
        await lifecycle.lock_work_item(session, item_id)
        request = await lifecycle.lock_request(session, work_item_id=item_id, request_id=request_id)
        if request is not None and request.dispatch_generation == generation:
            return await _defer_selection(session, request, "base_unavailable")
        await session.commit()
        return False
    item = await lifecycle.lock_work_item(session, item_id)
    request = await lifecycle.lock_request(session, work_item_id=item_id, request_id=request_id)
    now = await lifecycle.database_now(session)
    if (
        item is None
        or request is None
        or item.version != item_version
        or request.version != request_version
        or request.dispatch_generation != generation
        or request.status != "waiting"
        or item.cancelled_at is not None
        or item.publication_lineage_id != lineage_id
        or request.wait_deadline is None
        or request.wait_deadline <= now
        or (
            request.acquire_owner is not None
            and request.acquire_owner != owner
            and request.acquire_expires_at is not None
            and request.acquire_expires_at > now
        )
        or work_item_policy(forges_registry(settings), item.tracker_issue, item.repository)
        != policy
    ):
        await session.commit()
        return False
    if isinstance(resolved, factory_base.BaseRefusal):
        await session.commit()
        try:
            await factory_base.comment_refusal(tracker.marked_comments, issue, resolved)
        except (ForgeError, FeedbackUnavailable):
            pass
        await lifecycle.lock_work_item(session, item_id)
        request = await lifecycle.lock_request(session, work_item_id=item_id, request_id=request_id)
        return await _defer_selection(session, request, resolved.code) if request else False
    lineage = (
        await session.scalar(
            select(ThreadPublicationLineage)
            .where(ThreadPublicationLineage.id == lineage_id)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
        if lineage_id is not None
        else None
    )
    if lineage_id is not None and (lineage is None or lineage.version != lineage_version):
        await _record_requested_base(session, item, resolved)
        return await _defer_selection(session, request, "base_publication_pending")
    empty_lineage_id = None
    base_commit = resolved.commit
    if lineage is not None:
        if lineage.base_ref == resolved.branch:
            if not lineage.base_sha:
                return await _defer_selection(session, request, "base_publication_pending")
            base_commit = lineage.base_sha
        elif (
            not _lineage_empty(lineage)
            or lineage_branch != lineage.branch
            or remote_lineage_head is not None
            or await _lineage_busy(session, lineage.id)
        ):
            await _record_requested_base(session, item, resolved)
            return await _defer_selection(session, request, "base_publication_pending")
        else:
            empty_lineage_id = lineage.id
    session.info["factory_base_pin"] = (
        item.id,
        request.id,
        resolved,
        settle_until,
        empty_lineage_id,
        base_commit,
    )
    return True


async def observe_base_labels(
    session: AsyncSession,
    *,
    item: WorkItem,
    labels: set[str],
    default_branch: str | None,
    event_identity: str | None,
    settings: Settings,
    client: httpx.AsyncClient,
) -> None:
    from .workitem_dispatch import Admission, readmit

    expected_version = item.version
    locked_item = await lifecycle.lock_work_item(session, item.id)
    if locked_item is None or locked_item.version != expected_version:
        return
    item = locked_item
    registry = forges_registry(settings)
    policy = work_item_policy(registry, item.tracker_issue, item.repository)
    choice = factory_base.choose_base(labels, factory_base.bases_for(policy), default_branch)
    if (
        item.base_branch is None
        and item.cancelled_at is None
        and (item.trigger_label is None or item.trigger_label in labels)
    ):
        intent = (
            f"refusal\0{choice.code}\0{choice.reason}"
            if isinstance(choice, factory_base.BaseRefusal)
            else f"{choice.branch}\0{choice.source}"
        )
        fingerprint = hashlib.sha256(intent.encode()).hexdigest()
        if item.base_selection_fingerprint != fingerprint:
            item.base_selection_fingerprint = fingerprint
            item.version += 1
            await mark_status_comment_stale(session, item.id)
    if isinstance(choice, factory_base.BaseRefusal):
        issue, repository, installation = (
            item.tracker_issue,
            item.repository,
            item.code_host_installation_id,
        )
        await session.commit()
        tracker = tracker_for_work_item(registry, client, issue, repository, installation)
        await factory_base.comment_refusal(tracker.marked_comments, issue, choice)
        return
    if item.cancelled_at is not None or (
        item.trigger_label is not None and item.trigger_label not in labels
    ):
        return
    if choice.branch == item.base_branch:
        changed = (
            item.base_source != choice.source
            or item.base_requested_branch is not None
            or item.base_requested_source is not None
            or item.base_label_ignored is not None
        )
        item.base_requested_branch, item.base_requested_source = None, None
        item.base_selection_fingerprint = None
        item.base_source = choice.source
        item.base_label_ignored = None
        if changed:
            item.version += 1
            await mark_status_comment_stale(session, item.id)
        return
    if item.base_branch is None:
        return
    fingerprint = hashlib.sha256(f"{choice.branch}\0{choice.source}".encode()).hexdigest()
    if item.base_selection_fingerprint == fingerprint:
        return
    previous = await session.scalar(
        select(ExecutionRequest)
        .where(ExecutionRequest.work_item_id == item.id)
        .order_by(ExecutionRequest.sequence.desc())
        .limit(1)
    )
    if previous is None:
        return
    item_id, item_version = item.id, item.version
    previous_id, request_version, generation = (
        previous.id,
        previous.version,
        previous.dispatch_generation,
    )
    lineage_id = item.publication_lineage_id
    lineage = await session.get(ThreadPublicationLineage, lineage_id) if lineage_id else None
    lineage_version = lineage.version if lineage is not None else None
    lineage_branch = lineage.branch if lineage is not None and _lineage_empty(lineage) else None
    repository, installation = item.repository, item.code_host_installation_id
    await session.commit()
    remote_absent = lineage_id is None
    if lineage_branch is not None:
        try:
            async with asyncio.timeout(_READ_SECONDS):
                host = code_host_for_repository(
                    registry,
                    client,
                    repository,
                    installations=(
                        {repository.path: installation} if installation is not None else {}
                    ),
                )
                remote_absent = await host.branch_head(repository, lineage_branch) is None
        except (ForgeError, FeedbackUnavailable, TimeoutError):
            remote_absent = False
    locked_item = await lifecycle.lock_work_item(session, item_id)
    previous = await lifecycle.lock_request(session, work_item_id=item_id, request_id=previous_id)
    now = await lifecycle.database_now(session)
    if (
        locked_item is None
        or previous is None
        or locked_item.version != item_version
        or locked_item.cancelled_at is not None
        or locked_item.publication_lineage_id != lineage_id
        or previous.version != request_version
        or previous.dispatch_generation != generation
        or (
            previous.status == "waiting"
            and (previous.wait_deadline is None or previous.wait_deadline <= now)
        )
        or (
            previous.status == "running"
            and (previous.execution_deadline is None or previous.execution_deadline <= now)
        )
        or work_item_policy(
            forges_registry(settings), locked_item.tracker_issue, locked_item.repository
        )
        != policy
    ):
        return
    item = locked_item
    lineage = (
        await session.scalar(
            select(ThreadPublicationLineage)
            .where(ThreadPublicationLineage.id == lineage_id)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
        if lineage_id is not None
        else None
    )
    safe = lineage_id is None or (
        lineage is not None
        and lineage.version == lineage_version
        and _lineage_empty(lineage)
        and lineage.branch == lineage_branch
        and remote_absent
        and not await _lineage_busy(session, lineage.id)
    )
    await _record_requested_base(session, item, choice)
    if not safe:
        item.base_selection_fingerprint = None
        return
    request_id = uuid.uuid5(item.id, f"base:{event_identity or fingerprint}")
    item.base_selection_fingerprint = fingerprint
    await session.flush()
    kind, address, conversation = reply_route(registry, item.tracker_issue, item.repository_path)
    facts = Admission(
        agent_id=item.agent_id,
        kind=kind,
        address=address,
        reply_conversation_id=conversation,
        issue=item.tracker_issue,
        repository=item.repository,
        code_host_installation_id=item.code_host_installation_id,
        objective=(
            "Verify the repository after its base changed. "
            "Run fresh validation and continue the existing task."
        ),
        requester=previous.requester or "factory",
        request_id=request_id,
        trigger_label=item.trigger_label,
    )
    settle_until = previous.base_settle_until
    async with AsyncSession(
        bind=await session.connection(),
        join_transaction_mode="create_savepoint",
        expire_on_commit=False,
    ) as dispatch:
        await readmit(dispatch, facts)
        replacement = await dispatch.get(ExecutionRequest, request_id)
        if replacement is not None:
            replacement.base_settle_until = settle_until
        current_item = await dispatch.get(WorkItem, item.id)
        if current_item is not None and current_item.readmit_request_id == request_id:
            current_item.readmit_base_settle_until = settle_until
        await dispatch.commit()
    await mark_status_comment_stale(session, item.id)


async def _lineage_busy(session: AsyncSession, lineage_id: uuid.UUID) -> bool:
    publication = await session.scalar(
        select(Publication.id)
        .where(
            Publication.lineage_id == lineage_id,
            or_(
                Publication.status.in_(lifecycle.IN_FLIGHT_PUBLICATION),
                Publication.outcome_history_ready_at.is_(None),
            ),
        )
        .limit(1)
    )
    review = await session.scalar(
        select(PublicationReviewReservation.id)
        .where(
            PublicationReviewReservation.lineage_id == lineage_id,
            PublicationReviewReservation.status == "reserved",
        )
        .limit(1)
    )
    return publication is not None or review is not None
