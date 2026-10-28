"""Resolve current factory selection and reconcile durable branch moves."""

from __future__ import annotations

import asyncio
import hashlib
import uuid
from datetime import timedelta
from typing import Any

import httpx
from sqlalchemy import and_, exists, func, or_, select, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from . import factory_base, transcripts
from .config import Settings
from .factory_notices import mark_status_comment_stale
from .forges.errors import ForgeError, NotFound, Unavailable
from .forges.registry import forges_registry
from .forges.trackers import (
    code_host_for_repository,
    reply_route,
    tracker_for_work_item,
    work_item_policy,
)
from .forges.types import CredentialScope, PullRequestRef, PullRequestState, ReplyTarget
from .github_review_events import FeedbackUnavailable
from .models import (
    ExecutionRequest,
    FactoryBaseTransition,
    Publication,
    PublicationReviewReservation,
    ThreadPublicationLineage,
    ThreadTranscript,
    WorkItem,
)
from .repository_access import issue_stored_credential
from .workitems import lifecycle

_UNFINISHED = ("pending", "prepared", "pushed", "retargeted", "ci_prepared", "ci_pushed")
_READ_SECONDS = 8.0
_LEASE_SECONDS = 90


def transition_blocks() -> Any:
    return or_(
        FactoryBaseTransition.phase.in_(_UNFINISHED),
        and_(
            FactoryBaseTransition.phase == "applied",
            FactoryBaseTransition.outcome_ready_at.is_(None),
        ),
    )


async def _defer_selection(
    session: AsyncSession, request: ExecutionRequest, reason: str, *, settle_until: Any = None
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
    blocked = await session.scalar(
        select(FactoryBaseTransition.id)
        .where(
            FactoryBaseTransition.request_id == request.id,
            or_(transition_blocks(), FactoryBaseTransition.phase.in_(("conflict", "refused"))),
        )
        .limit(1)
    )
    if blocked is not None:
        return await _defer_selection(session, request, "base_transition_pending")
    if item.base_resolved_request_id == request.id:
        return True
    if item.trigger_label is None:
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
    trigger_label, publication_lineage_id = item.trigger_label, item.publication_lineage_id
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
            if trigger_label is not None and trigger_label not in ticket.labels:
                raise Unavailable("factory_label_absent")
            resolved = await factory_base.resolve_base(
                policy=policy,
                labels=set(ticket.labels),
                default_branch=current_repository.default_branch,
                code_host=host,
                repository=repository,
            )
            remote_lineage_head = None
            lineage = (
                await session.get(ThreadPublicationLineage, publication_lineage_id)
                if publication_lineage_id
                else None
            )
            lineage_branch = (
                lineage.branch if lineage is not None and lineage.head_sha is None else None
            )
            if (
                lineage is not None
                and not isinstance(resolved, factory_base.BaseRefusal)
                and lineage.base_ref != resolved.branch
            ):
                await _supersede_pending_authority(session, lineage.id)
            if lineage_branch is not None:
                await session.commit()
                remote_lineage_head = await host.branch_head(repository, lineage_branch)
    except (ForgeError, FeedbackUnavailable, TimeoutError):
        item = await lifecycle.lock_work_item(session, item_id)
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
        or request.wait_deadline is None
        or request.wait_deadline <= now
        or work_item_policy(forges_registry(settings), item.tracker_issue, item.repository)
        != policy
    ):
        await session.commit()
        return False
    if isinstance(resolved, factory_base.BaseRefusal):
        # The marked refusal uses a separate transaction and no locked database rows.
        await session.commit()
        try:
            await factory_base.comment_refusal(tracker.marked_comments, issue, resolved)
        except (ForgeError, FeedbackUnavailable):
            pass
        request = await lifecycle.lock_request(session, work_item_id=item_id, request_id=request_id)
        return await _defer_selection(session, request, resolved.code) if request else False
    lineage = (
        await session.scalar(
            select(ThreadPublicationLineage)
            .where(ThreadPublicationLineage.id == item.publication_lineage_id)
            .with_for_update()
        )
        if item.publication_lineage_id
        else None
    )
    if lineage is not None and lineage.base_ref != resolved.branch:
        busy = await _lineage_busy(session, lineage.id)
        if busy or (lineage.head_sha is None and remote_lineage_head is not None):
            return await _defer_selection(session, request, "base_publication_pending")
        if lineage.head_sha is not None or lineage.pr_number is not None:
            if lineage.head_sha is None or lineage.pr_number is None or not lineage.base_sha:
                return await _defer_selection(session, request, "base_publication_pending")
            session.add(
                FactoryBaseTransition(
                    work_item_id=item.id,
                    request_id=request.id,
                    lineage_id=lineage.id,
                    old_base_ref=lineage.base_ref or item.base_branch or "",
                    old_base_sha=lineage.base_sha,
                    old_head_sha=lineage.head_sha,
                    target_base_ref=resolved.branch,
                    target_base_sha=resolved.commit,
                    base_source=resolved.source,
                    branch=lineage.branch,
                    pull_request_number=str(lineage.pr_number),
                    committer_name="Curie",
                    committer_email="factory@example.com",
                    committer_time=now,
                    next_attempt_at=now,
                    repository_snapshot={
                        "kind": item.code_host_kind,
                        "host": item.code_host_host,
                        "project_id": item.repository_project_id,
                        "path": item.repository_path,
                        "installation_id": item.code_host_installation_id,
                        "pull_request_id": lineage.code_host_pr_id,
                    },
                )
            )
            return await _defer_selection(session, request, "base_transition_pending")
    session.info["factory_base_pin"] = (
        item.id,
        request.id,
        resolved,
        settle_until,
        lineage.id
        if lineage is not None and lineage.head_sha is None and lineage.base_ref != resolved.branch
        else None,
        lineage.base_sha
        if lineage is not None and lineage.base_ref == resolved.branch and lineage.base_sha
        else resolved.commit,
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
    if isinstance(choice, factory_base.BaseRefusal):
        issue, repository, installation = (
            item.tracker_issue,
            item.repository,
            item.code_host_installation_id,
        )
        await session.commit()
        tracker = tracker_for_work_item(
            registry, client, issue, repository, installation
        )
        await factory_base.comment_refusal(tracker.marked_comments, issue, choice)
        return
    if item.cancelled_at is not None or (
        item.trigger_label is not None and item.trigger_label not in labels
    ):
        return
    item.base_requested_branch, item.base_requested_source = choice.branch, choice.source
    hold = await session.scalar(
        select(FactoryBaseTransition.id)
        .where(
            FactoryBaseTransition.work_item_id == item.id,
            FactoryBaseTransition.request_id.in_(
                select(ExecutionRequest.id).where(
                    ExecutionRequest.work_item_id == item.id,
                    ExecutionRequest.status.in_(("waiting", "running", "cancellation_requested")),
                )
            ),
            FactoryBaseTransition.phase.in_(("conflict", "refused")),
        )
        .limit(1)
    )
    if hold is not None:
        if choice.branch == item.base_branch and item.base_source != choice.source:
            item.base_source = choice.source
            item.version += 1
        await mark_status_comment_stale(session, item.id)
        return
    if item.base_branch is None:
        return
    moving = await session.scalar(
        select(FactoryBaseTransition)
        .where(
            FactoryBaseTransition.work_item_id == item.id,
            transition_blocks(),
        )
        .with_for_update()
        .limit(1)
    )
    if choice.branch == item.base_branch and moving is None:
        item.base_requested_branch, item.base_requested_source = None, None
        if item.base_source != choice.source:
            item.base_source = choice.source
            item.version += 1
            await mark_status_comment_stale(session, item.id)
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
    request_id = uuid.uuid5(item.id, f"base:{event_identity or fingerprint}")
    item.base_selection_fingerprint = fingerprint
    if moving is not None and moving.phase not in ("pending", "prepared"):
        item.readmit_request_id = request_id
        item.readmit_requester = previous.requester or "factory"
        item.readmit_objective = (
            "Verify the repository after its base changed. "
            "Run fresh validation and continue the existing task."
        )
        item.readmit_base_settle_until = previous.base_settle_until
        item.readmit_base_branch, item.readmit_base_source, item.readmit_base_commit = (
            choice.branch,
            choice.source,
            None,
        )
        item.version += 1
        await mark_status_comment_stale(session, item.id)
        return
    if moving is not None:
        moving.phase, moving.error_code = "refused", "selection_superseded"
        moving.owner, moving.lease_expires_at = None, None
        moving.version += 1
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
    await mark_status_comment_stale(session, item.id)
    async with AsyncSession(
        bind=await session.connection(),
        join_transaction_mode="create_savepoint",
        expire_on_commit=False,
    ) as dispatch:
        await readmit(dispatch, facts)
        # A base edit preserves the last factory-marking clock.
        replacement = await dispatch.get(ExecutionRequest, request_id)
        if replacement is not None:
            replacement.base_settle_until = settle_until
        current_item = await dispatch.get(WorkItem, item.id)
        if current_item is not None and current_item.readmit_request_id == request_id:
            current_item.readmit_base_settle_until = settle_until
        await dispatch.commit()
    await mark_status_comment_stale(session, item.id)


async def _supersede_pending_authority(session: AsyncSession, lineage_id: uuid.UUID) -> None:
    from .crud.approvals import claim_approval_resolution
    from .crud.errors import PublicationLineageConflict
    from .crud.lineages import cancel_review_revision
    from .models import ApprovalStatus

    approvals = list(
        await session.scalars(
            select(Publication.approval_id).where(
                Publication.lineage_id == lineage_id,
                Publication.status == "pending",
            )
        )
    )
    reviews = list(
        await session.scalars(
            select(PublicationReviewReservation).where(
                PublicationReviewReservation.lineage_id == lineage_id,
                PublicationReviewReservation.status == "reserved",
            )
        )
    )
    review_ids = [(row.id, row.origin_key, row.version) for row in reviews]
    await session.commit()
    for approval_id in approvals:
        await claim_approval_resolution(
            session,
            approval_id,
            decision=ApprovalStatus.rejected,
            resolved_by="factory-base-change",
            note="The selected factory base changed; fresh verification and approval are required.",
        )
    for reservation_id, origin_key, version in review_ids:
        try:
            await cancel_review_revision(
                session, reservation_id, origin_key=origin_key, expected_version=version
            )
            await session.commit()
        except (LookupError, PublicationLineageConflict):
            await session.rollback()


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


async def _owned(
    sessionmaker: async_sessionmaker[AsyncSession], operation: FactoryBaseTransition
) -> bool:
    async with sessionmaker() as session:
        valid = await session.scalar(
            select(FactoryBaseTransition.id)
            .join(WorkItem, WorkItem.id == FactoryBaseTransition.work_item_id)
            .where(
                FactoryBaseTransition.id == operation.id,
                FactoryBaseTransition.owner == operation.owner,
                FactoryBaseTransition.version == operation.version,
                FactoryBaseTransition.lease_expires_at > func.clock_timestamp(),
                or_(
                    FactoryBaseTransition.phase.not_in(("pending", "prepared")),
                    and_(
                        WorkItem.cancelled_at.is_(None),
                        exists().where(
                            ExecutionRequest.id == operation.request_id,
                            ExecutionRequest.status == "waiting",
                            ExecutionRequest.wait_deadline > func.clock_timestamp(),
                        ),
                    ),
                ),
            )
        )
        return valid is not None


async def _record_phase(
    sessionmaker: async_sessionmaker[AsyncSession],
    operation: FactoryBaseTransition,
    *,
    phase: str | None = None,
    **values: Any,
) -> bool:
    async with sessionmaker() as session:
        item = await lifecycle.lock_work_item(session, operation.work_item_id)
        if item is None or (
            item.cancelled_at is not None
            and operation.phase in ("pending", "prepared")
            and phase != "refused"
        ):
            return False
        changed = await session.scalar(
            update(FactoryBaseTransition)
            .where(
                FactoryBaseTransition.id == operation.id,
                FactoryBaseTransition.owner == operation.owner,
                FactoryBaseTransition.version == operation.version,
                FactoryBaseTransition.lease_expires_at > func.clock_timestamp(),
            )
            .values(
                **({"phase": phase} if phase else {}),
                **values,
                version=FactoryBaseTransition.version + 1,
                owner=None,
                lease_expires_at=None,
                next_attempt_at=func.clock_timestamp() + timedelta(seconds=5),
            )
            .returning(FactoryBaseTransition.id)
        )
        if changed is not None:
            await mark_status_comment_stale(session, item.id)
        await session.commit()
        return changed is not None


async def _read_pull(
    host: Any,
    repository: Any,
    operation: FactoryBaseTransition,
    *,
    heads: tuple[str, ...],
    bases: tuple[str, ...],
) -> Any:
    pull = await host.read_pull_request(PullRequestRef(repository, operation.pull_request_number))
    if (
        pull.ref.repository != repository
        or pull.ref.number != operation.pull_request_number
        or pull.state is not PullRequestState.OPEN
    ):
        raise _Refused("pull_request_not_open")
    if pull.head_ref != operation.branch or pull.head_sha not in heads:
        raise _Refused("head_changed")
    if pull.base_ref not in bases:
        raise _Refused("base_changed")
    return pull


class _Refused(Exception):
    pass


async def _apply_transition(
    sessionmaker: async_sessionmaker[AsyncSession], operation: FactoryBaseTransition
) -> None:
    async with sessionmaker() as session:
        item = await lifecycle.lock_work_item(session, operation.work_item_id)
        request = await lifecycle.lock_request(
            session, work_item_id=operation.work_item_id, request_id=operation.request_id
        )
        current = await session.scalar(
            select(FactoryBaseTransition)
            .where(
                FactoryBaseTransition.id == operation.id,
                FactoryBaseTransition.version == operation.version,
                FactoryBaseTransition.owner == operation.owner,
                FactoryBaseTransition.lease_expires_at > func.clock_timestamp(),
            )
            .with_for_update()
        )
        if (
            item is None
            or request is None
            or current is None
            or item.publication_lineage_id != operation.lineage_id
        ):
            return
        lineage = await session.scalar(
            select(ThreadPublicationLineage)
            .where(ThreadPublicationLineage.id == operation.lineage_id)
            .with_for_update()
        )
        if (
            lineage is None
            or lineage.branch != operation.branch
            or str(lineage.pr_number) != operation.pull_request_number
            or lineage.head_sha != operation.old_head_sha
            or lineage.base_ref != operation.old_base_ref
            or lineage.base_sha != operation.old_base_sha
        ):
            await session.rollback()
            await _record_phase(
                sessionmaker, operation, phase="refused", error_code="lineage_changed"
            )
            return
        if await _lineage_busy(session, lineage.id):
            await session.rollback()
            await _record_phase(sessionmaker, operation, error_code="publication_pending")
            return
        now = await lifecycle.database_now(session)
        item.base_requested_branch, item.base_requested_source = None, None
        item.base_branch, item.base_commit, item.base_source = (
            operation.target_base_ref,
            operation.target_base_sha,
            operation.base_source,
        )
        item.base_resolved_request_id = request.id
        item.base_selection_fingerprint = None
        item.version += 1
        lineage.base_ref, lineage.base_sha, lineage.head_sha = (
            operation.target_base_ref,
            operation.target_base_sha,
            operation.verification_head_sha,
        )
        lineage.version += 1
        current.phase, current.applied_at = "applied", now
        current.owner, current.lease_expires_at, current.error_code = None, None, None
        current.version += 1
        request.dispatch_not_before = now
        await mark_status_comment_stale(session, item.id)
        await session.commit()


async def _append_outcome(
    sessionmaker: async_sessionmaker[AsyncSession], operation: FactoryBaseTransition
) -> None:
    async with sessionmaker() as session:
        item = await lifecycle.lock_work_item(session, operation.work_item_id)
        current = await session.scalar(
            select(FactoryBaseTransition)
            .where(
                FactoryBaseTransition.id == operation.id,
                FactoryBaseTransition.version == operation.version,
                FactoryBaseTransition.owner == operation.owner,
                FactoryBaseTransition.lease_expires_at > func.clock_timestamp(),
            )
            .with_for_update()
        )
        if item is None or current is None:
            return
        marker = f"factory-base-transition:{current.id}"
        row = await session.scalar(
            select(ThreadTranscript)
            .where(
                ThreadTranscript.agent_id == item.agent_id,
                ThreadTranscript.binding_scope.is_(None),
                ThreadTranscript.thread_key == item.conversation_id,
                or_(
                    ThreadTranscript.expires_at.is_(None), ThreadTranscript.expires_at > func.now()
                ),
            )
            .with_for_update()
        )
        history = row.value if row is not None and isinstance(row.value, list) else []
        if not any(isinstance(entry, dict) and entry.get("id") == marker for entry in history):
            async with AsyncSession(
                bind=await session.connection(),
                join_transaction_mode="create_savepoint",
                expire_on_commit=False,
            ) as history_session:
                await transcripts.append(
                    history_session,
                    item.agent_id,
                    None,
                    item.conversation_id,
                    {
                        "id": marker,
                        "role": "system",
                        "content": (
                            "The platform changed the pull request base to "
                            f"{current.target_base_ref} at {current.target_base_sha}; "
                            f"current head is {current.verification_head_sha}. "
                            "Run fresh repository verification before publishing or "
                            "completing this request. Earlier approvals and verification "
                            "describe the previous tree."
                        ),
                    },
                    None,
                )
        current.outcome_ready_at = await lifecycle.database_now(session)
        current.owner, current.lease_expires_at = None, None
        current.version += 1
        if item.readmit_request_id is None:
            await session.execute(
                update(ExecutionRequest)
                .where(
                    ExecutionRequest.id == current.request_id, ExecutionRequest.status == "waiting"
                )
                .values(dispatch_not_before=func.clock_timestamp())
            )
        await session.commit()


async def _advance_transition(
    sessionmaker: async_sessionmaker[AsyncSession],
    operation: FactoryBaseTransition,
    *,
    settings: Settings,
    client: httpx.AsyncClient,
) -> None:
    from . import factory_rebase

    async with sessionmaker() as session:
        item = await session.get(WorkItem, operation.work_item_id)
        lineage = await session.get(ThreadPublicationLineage, operation.lineage_id)
        if item is None or lineage is None:
            return
        request = await session.get(ExecutionRequest, operation.request_id)
        stopped = (
            item.cancelled_at is not None
            or request is None
            or request.status != "waiting"
            or request.wait_deadline is None
            or request.wait_deadline <= await lifecycle.database_now(session)
        )
        repository, issue = item.repository, item.tracker_issue
        installation = item.code_host_installation_id
        busy = await _lineage_busy(session, lineage.id)
        stopping = await session.scalar(
            select(ExecutionRequest.id)
            .where(
                ExecutionRequest.work_item_id == item.id,
                ExecutionRequest.id != operation.request_id,
                or_(
                    ExecutionRequest.status.in_(("running", "cancellation_requested")),
                    ExecutionRequest.teardown_unconfirmed_at.is_not(None),
                ),
            )
            .limit(1)
        )
    if stopped and operation.phase in ("pending", "prepared"):
        await _record_phase(
            sessionmaker, operation, phase="refused", error_code="request_not_waiting"
        )
        return
    identity = {
        "kind": repository.kind,
        "host": repository.host,
        "project_id": repository.project_id,
        "path": repository.path,
        "installation_id": installation,
        "pull_request_id": lineage.code_host_pr_id,
    }
    if operation.repository_snapshot is None:
        await _record_phase(sessionmaker, operation, repository_snapshot=identity)
        return
    if operation.repository_snapshot != identity:
        await _record_phase(
            sessionmaker, operation, phase="refused", error_code="repository_identity_changed"
        )
        return
    if busy or stopping is not None:
        await _record_phase(
            sessionmaker,
            operation,
            error_code="publication_pending" if busy else "termination_pending",
        )
        return
    registry = forges_registry(settings)
    host = code_host_for_repository(
        registry,
        client,
        repository,
        installations={repository.path: installation} if installation is not None else {},
    )
    snapshot = factory_rebase.RebaseSnapshot(
        transition_id=operation.id,
        repository=repository,
        branch=operation.branch,
        old_base_sha=operation.old_base_sha,
        old_head_sha=operation.old_head_sha,
        target_ref=operation.target_base_ref,
        target_sha=operation.target_base_sha,
        committer_name=operation.committer_name,
        committer_email=operation.committer_email,
        committer_time=operation.committer_time,
        max_seconds=60,
        max_bytes=1024 * 1024 * 1024,
    )
    try:
        if operation.phase == "conflict":
            tracker = tracker_for_work_item(registry, client, issue, repository, installation)
            await tracker.marked_comments.upsert_marked(
                ReplyTarget.on_issue(issue),
                f"curie-base-transition-{operation.id}",
                f"Curie refused the base change to `{operation.target_base_ref}` because "
                "the replay conflicts. The branch and pull request target are unchanged. "
                "Apply the factory label again to retry after resolving the conflict.",
            )
            await _record_phase(sessionmaker, operation, refusal_noticed_at=func.clock_timestamp())
            return
        if operation.phase == "applied":
            await _append_outcome(sessionmaker, operation)
            return
        if operation.phase == "pending":
            await _read_pull(
                host,
                repository,
                operation,
                heads=(operation.old_head_sha,),
                bases=(operation.old_base_ref,),
            )
            if not await _owned(sessionmaker, operation):
                return
            credential = await issue_stored_credential(
                settings, client, repository=repository, scope=CredentialScope.PUSH
            )
            prepared = await factory_rebase.prepare_rebase(snapshot, credential)
            await _record_phase(
                sessionmaker,
                operation,
                phase="prepared",
                rebased_head_sha=prepared.head_sha,
                prepared_at=func.clock_timestamp(),
                error_code=None,
            )
        elif operation.phase == "prepared":
            candidate = operation.rebased_head_sha
            if candidate is None:
                raise _Refused("candidate_missing")
            await _read_pull(
                host,
                repository,
                operation,
                heads=(operation.old_head_sha, candidate),
                bases=(operation.old_base_ref,),
            )
            remote = await host.branch_head(repository, operation.branch)
            if remote == operation.old_head_sha:
                credential = await issue_stored_credential(
                    settings, client, repository=repository, scope=CredentialScope.PUSH
                )
                prepared = await factory_rebase.prepare_rebase(snapshot, credential)
                if prepared.head_sha != candidate:
                    raise _Refused("candidate_changed")
                if not await _owned(sessionmaker, operation):
                    return
                await factory_rebase.push_rebase(snapshot, credential, prepared)
            elif remote != candidate:
                raise _Refused("head_changed")
            if await host.branch_head(repository, operation.branch) != candidate:
                raise _Refused("head_changed")
            await _record_phase(
                sessionmaker,
                operation,
                phase="pushed",
                pushed_at=func.clock_timestamp(),
                error_code=None,
            )
        elif operation.phase == "pushed":
            candidate = operation.rebased_head_sha
            if candidate is None:
                raise _Refused("candidate_missing")
            pull = await _read_pull(
                host,
                repository,
                operation,
                heads=(candidate,),
                bases=(operation.old_base_ref, operation.target_base_ref),
            )
            if pull.base_ref != operation.target_base_ref:
                if not await _owned(sessionmaker, operation):
                    return
                await host.retarget_pull_request(
                    pull.ref, base_ref=operation.target_base_ref, expected_head_sha=candidate
                )
            await _read_pull(
                host, repository, operation, heads=(candidate,), bases=(operation.target_base_ref,)
            )
            await _record_phase(
                sessionmaker,
                operation,
                phase="retargeted",
                retargeted_at=func.clock_timestamp(),
                error_code=None,
            )
        elif operation.phase == "retargeted":
            candidate = operation.rebased_head_sha
            if candidate is None:
                raise _Refused("candidate_missing")
            await _read_pull(
                host, repository, operation, heads=(candidate,), bases=(operation.target_base_ref,)
            )
            credential = await issue_stored_credential(
                settings, client, repository=repository, scope=CredentialScope.PUSH
            )
            marker = await factory_rebase.prepare_ci_marker(snapshot, credential, candidate)
            await _record_phase(
                sessionmaker,
                operation,
                phase="ci_prepared",
                verification_head_sha=marker.head_sha,
                error_code=None,
            )
        elif operation.phase == "ci_prepared":
            candidate, verification = operation.rebased_head_sha, operation.verification_head_sha
            if candidate is None or verification is None:
                raise _Refused("candidate_missing")
            await _read_pull(
                host,
                repository,
                operation,
                heads=(candidate, verification),
                bases=(operation.target_base_ref,),
            )
            remote = await host.branch_head(repository, operation.branch)
            if remote == candidate:
                credential = await issue_stored_credential(
                    settings, client, repository=repository, scope=CredentialScope.PUSH
                )
                marker = await factory_rebase.prepare_ci_marker(snapshot, credential, candidate)
                if marker.head_sha != verification:
                    raise _Refused("candidate_changed")
                if not await _owned(sessionmaker, operation):
                    return
                await factory_rebase.push_ci_marker(snapshot, credential, marker, candidate)
            elif remote != verification:
                raise _Refused("head_changed")
            await _read_pull(
                host,
                repository,
                operation,
                heads=(verification,),
                bases=(operation.target_base_ref,),
            )
            credential = await issue_stored_credential(
                settings, client, repository=repository, scope=CredentialScope.PUSH
            )
            paths = await factory_rebase.describe_changed_paths(snapshot, credential, verification)
            await _record_phase(
                sessionmaker,
                operation,
                phase="ci_pushed",
                ci_pushed_at=func.clock_timestamp(),
                changed_paths=list(paths),
                error_code=None,
            )
        elif operation.phase == "ci_pushed":
            if operation.verification_head_sha is None:
                raise _Refused("candidate_missing")
            await _read_pull(
                host,
                repository,
                operation,
                heads=(operation.verification_head_sha,),
                bases=(operation.target_base_ref,),
            )
            await _apply_transition(sessionmaker, operation)
    except NotFound:
        await _record_phase(
            sessionmaker, operation, phase="refused", error_code="pull_request_missing"
        )
    except _Refused as error:
        await _record_phase(sessionmaker, operation, phase="refused", error_code=str(error))
    except (ForgeError, FeedbackUnavailable, TimeoutError):
        exhausted = operation.attempts >= 9 and operation.phase != "conflict"
        await _record_phase(
            sessionmaker,
            operation,
            phase="refused" if exhausted else None,
            error_code="retry_limit"
            if exhausted
            else operation.error_code
            if operation.phase == "conflict"
            else "base_move_unavailable",
            attempts=operation.attempts + 1,
        )
    except factory_rebase.RebaseRefused as error:
        code = error.code
        await _record_phase(
            sessionmaker,
            operation,
            phase="conflict" if code == "conflict" else "refused",
            error_code=code,
        )


async def reconcile_base_transitions(
    sessionmaker: async_sessionmaker[AsyncSession],
    *,
    settings: Settings,
    client: httpx.AsyncClient,
) -> None:
    handled: set[uuid.UUID] = set()
    for _ in range(20):
        async with sessionmaker() as session:
            operation = await session.scalar(
                select(FactoryBaseTransition)
                .where(
                    or_(
                        FactoryBaseTransition.phase.in_(_UNFINISHED),
                        and_(
                            FactoryBaseTransition.phase == "applied",
                            FactoryBaseTransition.outcome_ready_at.is_(None),
                        ),
                        and_(
                            FactoryBaseTransition.phase == "conflict",
                            FactoryBaseTransition.refusal_noticed_at.is_(None),
                        ),
                    ),
                    FactoryBaseTransition.next_attempt_at <= func.clock_timestamp(),
                    or_(
                        FactoryBaseTransition.lease_expires_at.is_(None),
                        FactoryBaseTransition.lease_expires_at <= func.clock_timestamp(),
                    ),
                    *((FactoryBaseTransition.id.not_in(handled),) if handled else ()),
                )
                .order_by(FactoryBaseTransition.next_attempt_at)
                .with_for_update(skip_locked=True)
                .limit(1)
            )
            if operation is None:
                return
            if operation.repository_snapshot is None:
                item = await session.get(WorkItem, operation.work_item_id)
                lineage = await session.get(ThreadPublicationLineage, operation.lineage_id)
                if item is None or lineage is None:
                    continue
                operation.repository_snapshot = {
                    "kind": item.code_host_kind,
                    "host": item.code_host_host,
                    "project_id": item.repository_project_id,
                    "path": item.repository_path,
                    "installation_id": item.code_host_installation_id,
                    "pull_request_id": lineage.code_host_pr_id,
                }
            operation.owner = f"base-reconcile:{uuid.uuid4()}"
            operation.lease_expires_at = await lifecycle.database_now(session) + timedelta(
                seconds=_LEASE_SECONDS
            )
            operation.version += 1
            await session.commit()
            session.expunge(operation)
        handled.add(operation.id)
        await _advance_transition(sessionmaker, operation, settings=settings, client=client)
