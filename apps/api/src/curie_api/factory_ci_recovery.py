"""Read-only, finite CI recovery for an existing factory pull request (#5430)."""

from __future__ import annotations

import asyncio
import uuid
from dataclasses import dataclass
from datetime import timedelta

import httpx
import redis.asyncio as redis
from sqlalchemy import exists, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from sqlalchemy.orm import aliased

from . import factory_ci, factory_notices
from .config import Settings
from .forges.errors import ForgeError
from .forges.registry import forges_registry
from .forges.trackers import (
    code_host_for_repository,
    stored_repository,
    tracker_for_work_item,
)
from .forges.types import Actor, Disposition, MarkedNotice, PullRequestRef, PullRequestState
from .models import (
    ExecutionRequest,
    FactoryBaseTransition,
    FactoryCiObservation,
    Publication,
    ThreadPublicationLineage,
    WorkItem,
)
from .workitems import lifecycle

WINDOW_SECONDS = 86400
RECOVERABLE = ("ci_timeout", "ci_unverified")


async def _facts(
    session: AsyncSession,
    item: WorkItem,
    request: ExecutionRequest,
    lineage: ThreadPublicationLineage,
) -> factory_ci.CiFacts | None:
    if not lineage.head_sha:
        return None
    publication = await session.scalar(
        select(Publication)
        .where(Publication.execution_request_id == request.id, Publication.status == "succeeded")
        .order_by(Publication.revision_number.desc())
        .limit(1)
    )
    subject: lifecycle.CISubject
    if publication is not None:
        subject = lifecycle.PublicationCISubject(
            publication.id, publication.version, lineage.head_sha, lineage.base_ref, (), None
        )
    else:
        transition = await session.scalar(
            select(FactoryBaseTransition)
            .where(
                FactoryBaseTransition.work_item_id == item.id,
                FactoryBaseTransition.ci_request_id == request.id,
                FactoryBaseTransition.lineage_id == lineage.id,
                FactoryBaseTransition.phase == "applied",
            )
            .order_by(FactoryBaseTransition.applied_at.desc())
            .limit(1)
        )
        if transition is None:
            return None
        subject = lifecycle.BaseTransitionCISubject(
            transition.id, transition.version, lineage.head_sha, lineage.base_ref, (), None
        )
    return await factory_ci.load_ci_facts(
        session,
        lifecycle.PublicationSettlement(
            item.id,
            request.id,
            item.version,
            request.version,
            request.terminal_cause or "",
            subject,
        ),
        recovery=True,
    )


async def _latest(session: AsyncSession, request: ExecutionRequest) -> bool:
    return not await session.scalar(
        select(
            exists().where(
                ExecutionRequest.work_item_id == request.work_item_id,
                ExecutionRequest.sequence > request.sequence,
            )
        )
    )


async def _seed(session: AsyncSession, limit: int) -> None:
    newer = aliased(ExecutionRequest)
    items = (
        await session.execute(
            select(WorkItem, ExecutionRequest, ThreadPublicationLineage)
            .join(ExecutionRequest, ExecutionRequest.work_item_id == WorkItem.id)
            .join(
                ThreadPublicationLineage,
                ThreadPublicationLineage.id == WorkItem.publication_lineage_id,
            )
            .where(
                WorkItem.cancelled_at.is_(None),
                ExecutionRequest.status == "failed",
                ExecutionRequest.terminal_cause.in_(RECOVERABLE),
                ThreadPublicationLineage.status == "open",
                ThreadPublicationLineage.pr_number.is_not(None),
                ThreadPublicationLineage.head_sha.is_not(None),
                ~exists().where(
                    newer.work_item_id == WorkItem.id, newer.sequence > ExecutionRequest.sequence
                ),
                ~exists().where(FactoryCiObservation.request_id == ExecutionRequest.id),
            )
            .order_by(ExecutionRequest.terminal_at)
            .limit(limit)
            .with_for_update(skip_locked=True, of=WorkItem)
        )
    ).all()
    for item, request, lineage in items:
        if await _facts(session, item, request, lineage) is None:
            continue
        now = await lifecycle.database_now(session)
        inserted = await session.scalar(
            insert(FactoryCiObservation)
            .values(
                request_id=request.id,
                work_item_id=item.id,
                lineage_id=lineage.id,
                head_sha=lineage.head_sha,
                status="observing",
                created_at=now,
                expires_at=now + timedelta(seconds=WINDOW_SECONDS),
                next_due_at=now,
                reason=request.terminal_cause,
            )
            .on_conflict_do_nothing(index_elements=[FactoryCiObservation.request_id])
            .returning(FactoryCiObservation.request_id)
        )
        if inserted is not None:
            await factory_notices.mark_status_comment_stale(session, item.id)
    await session.commit()


@dataclass(frozen=True)
class _Claim:
    request_id: uuid.UUID
    work_item_id: uuid.UUID
    lineage_id: uuid.UUID
    version: int
    head_sha: str
    work_item_version: int
    request_version: int
    lineage_version: int
    facts: factory_ci.CiFacts


async def _claim(session: AsyncSession) -> _Claim | None:
    now = await lifecycle.database_now(session)
    row = await session.scalar(
        select(FactoryCiObservation)
        .where(
            FactoryCiObservation.status == "observing",
            FactoryCiObservation.next_due_at <= now,
        )
        .order_by(FactoryCiObservation.next_due_at)
        .limit(1)
        .with_for_update(skip_locked=True)
    )
    if row is None:
        return None
    item = await session.get(WorkItem, row.work_item_id)
    request = await session.get(ExecutionRequest, row.request_id)
    lineage = await session.get(ThreadPublicationLineage, row.lineage_id)
    reason = None
    if now >= row.expires_at:
        row.status, reason = "expired", "observation_window_expired"
    elif (
        item is None
        or request is None
        or lineage is None
        or item.cancelled_at is not None
        or not await _latest(session, request)
        or item.publication_lineage_id != row.lineage_id
        or lineage.status != "open"
        or request.status != "failed"
        or request.terminal_cause not in RECOVERABLE
    ):
        row.status, reason = "invalidated", "observation_no_longer_current"
    if reason is not None:
        row.reason = reason
        row.version += 1
        await factory_notices.mark_status_comment_stale(session, row.work_item_id)
        await session.commit()
        return None
    assert item is not None and request is not None and lineage is not None
    facts = await _facts(session, item, request, lineage)
    if facts is None:
        row.reason = "publication_evidence_unavailable"
        row.next_due_at = now + timedelta(seconds=factory_ci.CI_POLL_SECONDS)
        row.version += 1
        await session.commit()
        return None
    # Only the durable publication lineage authorizes a replacement head.
    # The original window survives the replacement and all process restarts.
    if row.head_sha != facts.subject.head_sha:
        row.head_sha = facts.subject.head_sha
        row.reason = "authorized_head_requires_proof"
        await factory_notices.mark_status_comment_stale(session, item.id)
    row.next_due_at = now + timedelta(seconds=factory_ci.CI_CLAIM_SECONDS)
    row.version += 1
    claim = _Claim(
        row.request_id,
        item.id,
        lineage.id,
        row.version,
        row.head_sha,
        item.version,
        request.version,
        lineage.version,
        facts,
    )
    await session.flush()
    session.expunge_all()
    await session.commit()
    return claim


async def _read(
    sessionmaker: async_sessionmaker[AsyncSession],
    valkey: redis.Redis,
    settings: Settings,
    client: httpx.AsyncClient,
    claim: _Claim,
) -> tuple[str, str | None]:
    facts = claim.facts
    item, lineage = facts.work_item, facts.lineage
    registry = forges_registry(settings)
    repository = stored_repository(
        settings,
        kind=lineage.code_host_kind or item.code_host_kind,
        host=lineage.code_host_host or item.code_host_host,
        path=lineage.repo_full_name or item.repository_path,
        project_id=lineage.repository_project_id or item.repository_project_id,
    )
    try:
        tracker = tracker_for_work_item(
            registry,
            client,
            item.tracker_issue,
            item.repository,
            item.code_host_installation_id,
            label=item.trigger_label,
        )
        cancellation = MarkedNotice(
            item.tracker_issue,
            item.trigger_label or "",
            Actor("observation", "observation"),
            "ci-observation",
            Disposition.CANCEL,
            "",
            None,
        )
        if await tracker.verify_current(cancellation):
            return "invalidated", "tracker_cancelled"
        code_host = code_host_for_repository(
            registry,
            client,
            repository,
            installations=factory_ci.stored_installation(lineage, item),
        )
        # A separate exact-head PR read is mandatory even when the CI read fails.
        pull = await code_host.read_pull_request(PullRequestRef(repository, str(lineage.pr_number)))
        if pull.state is not PullRequestState.OPEN:
            return "invalidated", "pr_closed"
        if pull.head_sha != claim.head_sha:
            return "observing", "foreign_pull_request_head"
        async with sessionmaker() as session:
            now = await lifecycle.database_now(session)
        verdict, view, _fresh, _metadata, _now = await factory_ci.inspect_ci(
            sessionmaker,
            valkey,
            settings,
            code_host,
            repository,
            facts,
            now,
            may_observe=lambda: True,
        )
        assert verdict is not None
        if (
            view is not None
            and view.pull_head_sha == claim.head_sha
            and view.closed_unmerged is False
            and view.merged is False
            and verdict.kind == "green"
        ):
            return "completed", None
        return "observing", verdict.reason or verdict.kind
    except ForgeError as exc:
        return "observing", str(exc) or "ci_unavailable"


async def _record(
    session: AsyncSession,
    claim: _Claim,
    status: str,
    reason: str | None,
) -> None:
    item = await lifecycle.lock_work_item(session, claim.work_item_id)
    request = await lifecycle.lock_request(
        session, work_item_id=claim.work_item_id, request_id=claim.request_id
    )
    lineage = await session.scalar(
        select(ThreadPublicationLineage)
        .where(ThreadPublicationLineage.id == claim.lineage_id)
        .with_for_update()
    )
    row = await session.scalar(
        select(FactoryCiObservation)
        .where(FactoryCiObservation.request_id == claim.request_id)
        .with_for_update()
    )
    if row is None or row.status != "observing" or row.version != claim.version:
        return
    now = await lifecycle.database_now(session)
    if now >= row.expires_at:
        status, reason = "expired", "observation_window_expired"
    elif (
        item is None
        or request is None
        or lineage is None
        or item.cancelled_at is not None
        or not await _latest(session, request)
        or item.publication_lineage_id != claim.lineage_id
        or lineage.status != "open"
        or request.status != "failed"
        or request.terminal_cause not in RECOVERABLE
    ):
        status, reason = "invalidated", "observation_no_longer_current"
    elif (
        item.version != claim.work_item_version
        or request.version != claim.request_version
        or lineage.version != claim.lineage_version
        or lineage.head_sha != claim.head_sha
    ):
        status, reason = "observing", "observation_snapshot_changed"
    changed = row.status != status
    row.status, row.reason = status, reason
    row.next_due_at = now + timedelta(seconds=factory_ci.CI_POLL_SECONDS)
    row.observed_at = now if status == "completed" else None
    row.version += 1
    if changed:
        await factory_notices.mark_status_comment_stale(session, row.work_item_id)
    await session.commit()


async def reconcile(
    sessionmaker: async_sessionmaker[AsyncSession],
    valkey: redis.Redis,
    settings: Settings,
    client: httpx.AsyncClient,
) -> None:
    async with sessionmaker() as session:
        await _seed(session, settings.work_item_batch_limit)
    for _ in range(factory_ci.CI_OBSERVATIONS_PER_PASS):
        async with sessionmaker() as session:
            claim = await _claim(session)
        if claim is None:
            break
        try:
            async with asyncio.timeout(factory_ci.CI_CLAIM_SECONDS):
                status, reason = await _read(sessionmaker, valkey, settings, client, claim)
        except TimeoutError:
            status, reason = "observing", "timeout"
        async with sessionmaker() as session:
            await _record(session, claim, status, reason)
