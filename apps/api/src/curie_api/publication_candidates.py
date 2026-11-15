"""Private publication candidate ledger (#5428); object transport runs in the worker.

A candidate is a private, non-authorizing copy of one finished capture: the
WorkItem's retained base archive (one per WorkItem, shared by its candidates)
and one bounded patch. It grants no right to publish, never touches Approval or
Publication rows and shares nothing with ADR 0251 checkpoints.

Locks are always taken WorkItem, request, installation row, group, then
candidates. Collection takes the installation row then groups with SKIP LOCKED
and never locks a WorkItem. Every time decision uses the database clock.

Retention: ``expires_at`` stays NULL while the owning request has no
``terminal_at``. The first ledger operation that sees the owner terminal fixes
it to exactly ``terminal_at + 86400 s`` (``_fix_expiry``); since ``terminal_at``
is immutable the result does not depend on when it is materialized, and no read
ever moves it. ``transfer_ownership`` hands a candidate to a pending successor
and clears the expiry so it is fixed again from the successor's disposition.
"""

from __future__ import annotations

import hashlib
import json
import uuid
from datetime import datetime, timedelta
from typing import Any, cast

from sqlalchemy import delete, func, or_, select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm.attributes import set_committed_value

from .config import Settings
from .models import ExecutionRequest, WorkItem
from .models import PublicationCandidate as Candidate
from .models import PublicationCandidateGroup as Group
from .models import PublicationCandidateInstallation as Installation
from .models import PublicationCandidateObject as CandidateObject
from .models import PublicationCandidateOutcome as Outcome
from .models import PublicationCandidateReader as ReaderLease
from .schemas import publication_candidates as wire
from .workitems.lifecycle import database_now, lock_request, lock_work_item

STAGING_EXPIRY_SECONDS = 600
RETENTION_SECONDS = 86400
READER_LEASE_SECONDS = 300
CLAIM_SECONDS = 300
MAX_CANDIDATES_PER_WORK_ITEM = 3
MAX_READERS_PER_WORK_ITEM = 8
MAX_RETAINED_WORK_ITEMS = 10000
OBJECT_PREFIX = "publication-candidates"


class CandidateRefusal(Exception):
    def __init__(self, code: str, status: int = 409, *, recorded: bool = False) -> None:
        self.code = f"publication_candidates.{code}"
        self.status = status
        self.recorded = recorded
        super().__init__(self.code)


def _enabled(settings: Settings) -> None:
    if not settings.publication_candidates_enabled:
        raise CandidateRefusal("disabled", 503)


def _live(item: WorkItem, request: ExecutionRequest, now: datetime, *, live: bool) -> None:
    if item.cancelled_at is not None:
        raise CandidateRefusal("source_identity")
    if live and (
        request.status != "running"
        or request.terminal_at is not None
        or request.runtime_heartbeat_expires_at is None
        or request.runtime_heartbeat_expires_at <= now
        or request.execution_deadline is None
        or request.execution_deadline <= now
    ):
        raise CandidateRefusal("expired_source")


async def _superseded(session: AsyncSession, request: ExecutionRequest) -> bool:
    """Whether a later admitted request exists; a queued mention does not supersede."""

    return (
        await session.scalar(
            select(ExecutionRequest.id)
            .where(
                ExecutionRequest.work_item_id == request.work_item_id,
                ExecutionRequest.sequence > request.sequence,
                ExecutionRequest.status != "queued",
            )
            .limit(1)
        )
        is not None
    )


async def _identity(
    session: AsyncSession, body: wire.CandidateCaptureAuthority
) -> tuple[WorkItem, ExecutionRequest]:
    """Lock the WorkItem then the request; only the capture's identity must match.

    No liveness, sequence or status requirement: a terminal or superseded
    request still answers for its own capture.
    """

    item = await lock_work_item(session, body.work_item_id)
    if item is None:
        raise CandidateRefusal("source_identity")
    request = await lock_request(session, work_item_id=item.id, request_id=body.request_id)
    if (
        request is None
        or request.runtime_epoch != body.runtime_epoch
        or request.dispatch_generation != body.dispatch_generation
    ):
        raise CandidateRefusal("source_identity")
    return item, request


async def _authority(
    session: AsyncSession, body: wire.CandidateCaptureAuthority, *, live: bool
) -> tuple[WorkItem, ExecutionRequest]:
    item, request = await _identity(session, body)
    if item.base_commit is None or item.base_branch is None or await _superseded(session, request):
        raise CandidateRefusal("source_identity")
    _live(item, request, await database_now(session), live=live)
    return item, request


async def _installation(session: AsyncSession) -> None:
    row = await session.scalar(select(Installation).where(Installation.id == 1).with_for_update())
    if row is None:
        raise CandidateRefusal("installation_unavailable", 503)


async def _group(session: AsyncSession, item_id: uuid.UUID) -> Group | None:
    group: Group | None = await session.scalar(
        select(Group)
        .where(Group.work_item_id == item_id)
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    return group


async def _candidate(
    session: AsyncSession, item_id: uuid.UUID, candidate_id: uuid.UUID
) -> Candidate:
    row = await session.scalar(
        select(Candidate)
        .where(Candidate.id == candidate_id, Candidate.work_item_id == item_id)
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    if row is None:
        raise CandidateRefusal("candidate_identity")
    return row


def _group_identity(group: Group) -> tuple[str, ...]:
    return (
        group.repository_provider,
        group.repository_host,
        group.repository_project_id,
        group.repository_path,
        group.base_commit,
        group.target_branch,
    )


def _item_identity(item: WorkItem) -> tuple[str | None, ...]:
    return (
        item.code_host_kind,
        item.code_host_host,
        item.repository_project_id,
        item.repository_path,
        item.base_commit,
        item.base_branch,
    )


def _capture_identity(candidate: Candidate, body: wire.CandidateCaptureAuthority) -> None:
    if (
        candidate.origin_request_id != body.request_id
        or candidate.runtime_epoch != body.runtime_epoch
        or candidate.dispatch_generation != body.dispatch_generation
    ):
        raise CandidateRefusal("candidate_source_identity")


async def _write_outcome(
    session: AsyncSession, item_id: uuid.UUID, code: str, at: datetime
) -> bool:
    """Upsert the WorkItem's capture outcome; a later recorded outcome is kept."""

    insert = pg_insert(Outcome).values(work_item_id=item_id, code=code, recorded_at=at)
    upsert = insert.on_conflict_do_update(
        index_elements=[Outcome.work_item_id],
        set_={"code": insert.excluded.code, "recorded_at": insert.excluded.recorded_at},
        where=Outcome.recorded_at <= insert.excluded.recorded_at,
    ).returning(Outcome.work_item_id)
    return await session.scalar(upsert) is not None


async def _record_refusal(
    session: AsyncSession, item: WorkItem, code: str, status: int = 409
) -> None:
    await _write_outcome(session, item.id, code, await database_now(session))
    raise CandidateRefusal(code, status, recorded=True)


async def _objects(session: AsyncSession, item_id: uuid.UUID) -> list[CandidateObject]:
    return list(
        await session.scalars(
            select(CandidateObject)
            .where(CandidateObject.work_item_id == item_id)
            .order_by(CandidateObject.kind, CandidateObject.id)
        )
    )


def _receipt(obj: CandidateObject) -> wire.CandidateObjectReceipt:
    return wire.CandidateObjectReceipt.model_validate(
        {"kind": obj.kind, "key": obj.key, "size_bytes": obj.size_bytes, "sha256": obj.sha256}
    )


async def _reservation(
    session: AsyncSession, candidate: Candidate, *, replayed: bool
) -> wire.CandidateReservation:
    objects = [
        obj
        for obj in await _objects(session, candidate.work_item_id)
        if obj.candidate_id == candidate.id or (candidate.includes_base and obj.kind == "base")
    ]
    if len(objects) != (2 if candidate.includes_base else 1):
        raise CandidateRefusal("candidate_collecting")
    return wire.CandidateReservation(
        candidate_id=candidate.id,
        objects=[_receipt(obj) for obj in objects],
        capture_deadline=candidate.capture_deadline,
        staging_expires_at=candidate.staging_expires_at,
        replayed=replayed,
    )


async def _fix_expiry(session: AsyncSession, candidate: Candidate) -> None:
    """Fix a complete candidate's expiry from its owner's final disposition, once.

    One conditional UPDATE: it applies only while the row is still complete,
    still has no expiry and is still owned by the owner this caller observed,
    and it reads that owner's ``terminal_at`` in the same statement, so an
    expiry is never fixed from a request that no longer owns the candidate.
    An owner that is not yet terminal leaves the row untouched; a later call
    fixes it. The ORM row is refreshed from the database afterwards.
    """

    if candidate.status != "complete" or candidate.expires_at is not None:
        return
    observed_owner = candidate.owner_request_id
    owner_expiry = (
        select(ExecutionRequest.terminal_at + timedelta(seconds=RETENTION_SECONDS))
        .where(ExecutionRequest.id == observed_owner)
        .scalar_subquery()
    )
    await session.execute(
        update(Candidate)
        .where(
            Candidate.id == candidate.id,
            Candidate.expires_at.is_(None),
            Candidate.owner_request_id == observed_owner,
            Candidate.status == "complete",
            owner_expiry.is_not(None),
        )
        .values(expires_at=owner_expiry, version=Candidate.version + 1)
        .execution_options(synchronize_session=False)
    )
    await session.refresh(candidate)


def _unexpired(candidate: Candidate, now: datetime) -> bool:
    return candidate.expires_at is None or candidate.expires_at > now


async def context(
    session: AsyncSession, settings: Settings, body: wire.CandidateCaptureAuthority
) -> wire.CandidateCaptureContext:
    """The WorkItem's repository identity and trusted base, for a capture about to start."""

    _enabled(settings)
    item, _ = await _authority(session, body, live=True)
    assert item.base_commit is not None and item.base_branch is not None
    return wire.CandidateCaptureContext(
        repository=wire.CandidateRepository(
            provider=item.code_host_kind,
            host=item.code_host_host,
            project_id=item.repository_project_id,
            path=item.repository_path,
        ),
        base_commit=item.base_commit,
        target_branch=item.base_branch,
    )


async def outcome(
    session: AsyncSession, settings: Settings, body: wire.CandidateOutcome
) -> wire.CandidateOutcomeRecorded:
    """Record a capture that ended before any reservation; a later outcome is kept.

    Identity-only authority, like ``fail``: a capture that outlived its
    heartbeat, its deadline or its request still records why it ended.
    """

    _enabled(settings)
    item, _ = await _identity(session, body)
    recorded = await _write_outcome(session, item.id, body.reason, await database_now(session))
    return wire.CandidateOutcomeRecorded(recorded=recorded)


def _content_digest(body: wire.CandidateReserve) -> str:
    """The replay digest of one capture's content.

    A ``base`` declaration is excluded: the base is covered by ``base_commit``
    and the group's base digest, and a writer declares it only after a
    ``base_required`` answer, so a replay without it is the same capture.
    """

    contents = body.model_dump(mode="json", exclude={"capture_budget_seconds"})
    contents["objects"] = sorted(
        (value for value in contents["objects"] if value["kind"] != "base"),
        key=lambda value: value["kind"],
    )
    return hashlib.sha256(json.dumps(contents, sort_keys=True).encode()).hexdigest()


async def reserve(
    session: AsyncSession, settings: Settings, body: wire.CandidateReserve
) -> wire.CandidateReservation:
    _enabled(settings)
    item, request = await _authority(session, body, live=True)
    identity = (
        body.repository.provider,
        body.repository.host,
        body.repository.project_id,
        body.repository.path,
        body.base_commit,
        body.target_branch,
    )
    if identity != _item_identity(item) or (
        body.base_archive_limit_bytes != settings.workspace_max_archive_bytes
    ):
        raise CandidateRefusal("workspace_identity")
    await _installation(session)
    group = await _group(session, item.id)
    now = await database_now(session)
    _live(item, request, now, live=True)
    digest = _content_digest(body)
    replay = await session.scalar(
        select(Candidate)
        .where(
            Candidate.origin_request_id == request.id,
            Candidate.idempotency_key == body.dedupe_key,
        )
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    if replay is not None:
        if replay.content_sha256 != digest:
            raise CandidateRefusal("idempotency_conflict")
        if replay.status == "tombstoned":
            raise CandidateRefusal("candidate_state")
        return await _reservation(session, replay, replayed=True)
    patch = next(obj for obj in body.objects if obj.kind == "patch")
    base = next((obj for obj in body.objects if obj.kind == "base"), None)
    if patch.size_bytes > min(wire.PATCH_CEILING_BYTES, body.patch_limit_bytes):
        await _record_refusal(session, item, "patch_too_large", 413)
    if base is not None and base.size_bytes > settings.workspace_max_archive_bytes:
        await _record_refusal(session, item, "base_too_large", 413)
    if group is not None:
        if _group_identity(group) != identity or (
            base is not None and base.sha256 != group.base_sha256
        ):
            await _record_refusal(session, item, "retained_base_conflict")
        retained = next(
            (obj for obj in await _objects(session, item.id) if obj.kind == "base"), None
        )
        if retained is None or retained.status != "complete":
            await _record_refusal(session, item, "retained_base_unavailable")
        if base is not None:
            raise CandidateRefusal("retained_base_declaration")
    elif base is None:
        raise CandidateRefusal("base_required")
    candidates = list(
        await session.scalars(select(Candidate).where(Candidate.work_item_id == item.id))
    )
    if len(candidates) >= MAX_CANDIDATES_PER_WORK_ITEM or any(
        candidate.status == "staging" for candidate in candidates
    ):
        await _record_refusal(session, item, "work_item_capacity", 429)
    if (
        group is None
        and (await session.scalar(select(func.count()).select_from(Group)) or 0)
        >= MAX_RETAINED_WORK_ITEMS
    ):
        await _record_refusal(session, item, "retained_work_item_capacity", 429)
    used = await session.scalar(select(func.coalesce(func.sum(CandidateObject.size_bytes), 0)))
    declared = sum(obj.size_bytes for obj in body.objects)
    if int(used or 0) + declared > settings.publication_candidate_max_bytes:
        await _record_refusal(session, item, "installation_capacity", 429)
    if group is None:
        assert base is not None
        group = Group(
            work_item_id=item.id,
            repository_provider=identity[0],
            repository_host=identity[1],
            repository_project_id=identity[2],
            repository_path=identity[3],
            base_commit=identity[4],
            target_branch=identity[5],
            base_sha256=base.sha256,
        )
        session.add(group)
        await session.flush()
    candidate = Candidate(
        id=uuid.uuid4(),
        work_item_id=item.id,
        origin_request_id=request.id,
        owner_request_id=request.id,
        runtime_epoch=body.runtime_epoch,
        dispatch_generation=body.dispatch_generation,
        idempotency_key=body.dedupe_key,
        content_sha256=digest,
        changed_paths=list(body.changed_paths),
        contains_workflow_files=body.contains_workflow_files,
        title=body.title,
        body=body.body,
        includes_base=base is not None,
        capture_deadline=now + timedelta(seconds=body.capture_budget_seconds),
        staging_expires_at=now + timedelta(seconds=STAGING_EXPIRY_SECONDS),
        created_at=now,
    )
    session.add(candidate)
    await session.flush()
    for obj in body.objects:
        object_id = uuid.uuid4()
        key = (
            f"{OBJECT_PREFIX}/retained/{item.id}/{object_id}/base"
            if obj.kind == "base"
            else f"{OBJECT_PREFIX}/candidates/{candidate.id}/patch"
        )
        session.add(
            CandidateObject(
                id=object_id,
                work_item_id=item.id,
                candidate_id=None if obj.kind == "base" else candidate.id,
                kind=obj.kind,
                key=key,
                size_bytes=obj.size_bytes,
                sha256=obj.sha256,
                staging_expires_at=candidate.staging_expires_at,
            )
        )
    await session.flush()
    await _write_outcome(session, item.id, "reserved", now)
    return await _reservation(session, candidate, replayed=False)


async def _transition(
    session: AsyncSession,
    candidate: Candidate,
    *,
    new_status: str,
    completed_at: datetime | None = None,
) -> None:
    values: dict[str, Any] = {"status": new_status, "version": candidate.version + 1}
    if completed_at is not None:
        values["completed_at"] = completed_at
    changed = await session.scalar(
        update(Candidate)
        .where(
            Candidate.id == candidate.id,
            Candidate.version == candidate.version,
            Candidate.status == candidate.status,
            *(
                [Candidate.capture_deadline > func.clock_timestamp()]
                if new_status == "complete"
                else []
            ),
        )
        .values(**values)
        .returning(Candidate.version)
    )
    if changed is None:
        raise CandidateRefusal("candidate_version_conflict")
    set_committed_value(candidate, "status", new_status)
    set_committed_value(candidate, "version", changed)
    if completed_at is not None:
        set_committed_value(candidate, "completed_at", completed_at)


async def _tombstone(session: AsyncSession, candidate: Candidate) -> None:
    if candidate.status == "tombstoned":
        return
    await _transition(session, candidate, new_status="tombstoned")
    await session.execute(
        update(CandidateObject)
        .where(
            CandidateObject.candidate_id == candidate.id,
            CandidateObject.status.in_(("staging", "complete")),
        )
        .values(status="tombstoned")
    )


def _completion(candidate: Candidate) -> wire.CandidateCompletion:
    assert candidate.completed_at is not None
    return wire.CandidateCompletion(candidate_id=candidate.id, completed_at=candidate.completed_at)


async def complete(session: AsyncSession, body: wire.CandidateComplete) -> wire.CandidateCompletion:
    """Acknowledge an uploaded candidate. Bounded by its capture deadline, not liveness."""

    item, request = await _authority(session, body, live=False)
    await _installation(session)
    group = await _group(session, item.id)
    if group is None or _group_identity(group) != _item_identity(item):
        raise CandidateRefusal("retained_identity")
    candidate = await _candidate(session, item.id, body.candidate_id)
    now = await database_now(session)
    _live(item, request, now, live=False)
    _capture_identity(candidate, body)
    reservation = await _reservation(session, candidate, replayed=True)
    expected = sorted(obj.model_dump_json() for obj in reservation.objects)
    actual = sorted(obj.model_dump_json() for obj in body.receipts)
    if expected != actual:
        raise CandidateRefusal("receipt_mismatch")
    if candidate.status == "complete":
        return _completion(candidate)
    if candidate.status != "staging" or candidate.capture_deadline <= now:
        raise CandidateRefusal("capture_deadline_or_state")
    objects = await _objects(session, item.id)
    base = next((obj for obj in objects if obj.kind == "base"), None)
    if base is None or base.status != ("staging" if candidate.includes_base else "complete"):
        raise CandidateRefusal("base_incomplete")
    if any(obj.status != "staging" for obj in objects if obj.candidate_id == candidate.id):
        raise CandidateRefusal("candidate_incomplete")
    await _transition(session, candidate, new_status="complete", completed_at=now)
    for obj in objects:
        if obj.candidate_id == candidate.id or (candidate.includes_base and obj.kind == "base"):
            obj.status = "complete"
    await session.flush()
    await _write_outcome(session, item.id, "complete", now)
    return _completion(candidate)


async def fail(session: AsyncSession, body: wire.CandidateFail) -> wire.CandidateCaptureFailure:
    """Withdraw a staging candidate. Needs the capture's identity, not its liveness.

    A complete candidate is never withdrawn by its writer: once acknowledged it
    is retained until its expiry.
    """

    item, _ = await _identity(session, body)
    await _installation(session)
    if await _group(session, item.id) is None:
        raise CandidateRefusal("candidate_identity")
    candidate = await _candidate(session, item.id, body.candidate_id)
    now = await database_now(session)
    _capture_identity(candidate, body)
    if candidate.status == "tombstoned":
        return wire.CandidateCaptureFailure(candidate_id=candidate.id)
    if candidate.status != "staging":
        raise CandidateRefusal("candidate_state")
    await _tombstone(session, candidate)
    await session.execute(
        update(CandidateObject)
        .where(
            CandidateObject.work_item_id == item.id,
            CandidateObject.kind == "base",
            CandidateObject.status == "staging",
            ~select(Candidate.id)
            .where(Candidate.work_item_id == item.id, Candidate.status == "complete")
            .exists(),
        )
        .values(status="tombstoned")
    )
    await _write_outcome(session, item.id, body.reason, now)
    return wire.CandidateCaptureFailure(candidate_id=candidate.id)


async def _expire_readers(session: AsyncSession, item_id: uuid.UUID, now: datetime) -> None:
    await session.execute(
        delete(ReaderLease).where(
            ReaderLease.work_item_id == item_id, ReaderLease.expires_at <= now
        )
    )


async def acquire_reader(
    session: AsyncSession, settings: Settings, body: wire.CandidateAcquireReader
) -> wire.CandidateReader:
    """Lease a complete, unexpired candidate for 300 s. Never renews or transfers it."""

    _enabled(settings)
    await _installation(session)
    group = await _group(session, body.work_item_id)
    if group is None:
        raise CandidateRefusal("candidate_identity")
    candidate = await _candidate(session, group.work_item_id, body.candidate_id)
    await _fix_expiry(session, candidate)
    now = await database_now(session)
    if candidate.status != "complete" or not _unexpired(candidate, now):
        raise CandidateRefusal("candidate_unavailable")
    objects = [
        obj
        for obj in await _objects(session, group.work_item_id)
        if obj.kind == "base" or obj.candidate_id == candidate.id
    ]
    if sorted(obj.kind for obj in objects) != ["base", "patch"] or any(
        obj.status != "complete" for obj in objects
    ):
        raise CandidateRefusal("candidate_incomplete")
    await _expire_readers(session, group.work_item_id, now)
    readers = list(
        await session.scalars(
            select(ReaderLease).where(ReaderLease.work_item_id == group.work_item_id)
        )
    )
    lease = next(
        (
            reader
            for reader in readers
            if reader.candidate_id == candidate.id and reader.holder_id == body.holder_id
        ),
        None,
    )
    if lease is None:
        if len(readers) >= MAX_READERS_PER_WORK_ITEM:
            raise CandidateRefusal("reader_capacity", 429)
        changed = await session.scalar(
            update(Candidate)
            .where(
                Candidate.id == candidate.id,
                Candidate.version == candidate.version,
                Candidate.status == "complete",
                or_(
                    Candidate.expires_at.is_(None),
                    Candidate.expires_at > func.clock_timestamp(),
                ),
            )
            .values(version=candidate.version + 1)
            .returning(Candidate.version)
        )
        if changed is None:
            raise CandidateRefusal("candidate_unavailable")
        set_committed_value(candidate, "version", changed)
        lease = ReaderLease(
            id=uuid.uuid4(),
            work_item_id=group.work_item_id,
            candidate_id=candidate.id,
            holder_id=body.holder_id,
            expires_at=now + timedelta(seconds=READER_LEASE_SECONDS),
        )
        session.add(lease)
    return wire.CandidateReader(
        lease_id=lease.id,
        candidate_id=candidate.id,
        holder_id=lease.holder_id,
        expires_at=lease.expires_at,
        objects=[_receipt(obj) for obj in objects],
    )


async def release_reader(
    session: AsyncSession, body: wire.CandidateReleaseReader
) -> wire.CandidateAcknowledged:
    await _installation(session)
    lease = await session.get(ReaderLease, body.lease_id)
    if lease is None:
        return wire.CandidateAcknowledged()
    await _group(session, lease.work_item_id)
    if lease.holder_id != body.holder_id:
        raise CandidateRefusal("reader_holder_identity")
    await session.execute(
        delete(ReaderLease).where(
            ReaderLease.id == body.lease_id, ReaderLease.holder_id == body.holder_id
        )
    )
    return wire.CandidateAcknowledged()


async def claim_collection(
    session: AsyncSession, body: wire.CandidateClaimCollection
) -> wire.CandidateCollectionClaims:
    """Claim unpinned tombstoned patches, then a group's base once it holds nothing.

    Fixes expiries first and tombstones expired complete candidates and staging
    candidates past ``staging_expires_at``. A claim whose ``claimed_until`` has
    passed is claimed again, so a collector that died between deleting the
    object and finalizing is finished by the next one.
    """

    await _installation(session)
    groups = list(
        await session.scalars(
            select(Group).order_by(Group.work_item_id).with_for_update(skip_locked=True)
        )
    )
    claims: list[wire.CandidateCollectionClaim] = []
    for group in groups:
        if len(claims) >= body.limit:
            break
        candidates = list(
            await session.scalars(
                select(Candidate)
                .where(Candidate.work_item_id == group.work_item_id)
                .order_by(Candidate.id)
                .with_for_update()
                .execution_options(populate_existing=True)
            )
        )
        for candidate in candidates:
            await _fix_expiry(session, candidate)
        now = await database_now(session)
        await _expire_readers(session, group.work_item_id, now)
        for candidate in candidates:
            if (candidate.status == "staging" and candidate.staging_expires_at <= now) or (
                candidate.status == "complete" and not _unexpired(candidate, now)
            ):
                await _tombstone(session, candidate)
        readers = list(
            await session.scalars(
                select(ReaderLease).where(ReaderLease.work_item_id == group.work_item_id)
            )
        )
        pinned = {reader.candidate_id for reader in readers}
        eligible = {
            candidate.id
            for candidate in candidates
            if candidate.status == "tombstoned" and candidate.id not in pinned
        }
        for obj in await _objects(session, group.work_item_id):
            if len(claims) >= body.limit:
                break
            if obj.kind == "base":
                if candidates or readers:
                    continue
            elif obj.candidate_id not in eligible:
                continue
            if (
                obj.status == "claimed"
                and obj.claimed_until is not None
                and obj.claimed_until > now
            ):
                if obj.collector_id != body.collector_id:
                    continue
            else:
                claim_id, version = uuid.uuid4(), obj.version + 1
                claimed_until = now + timedelta(seconds=CLAIM_SECONDS)
                changed = await session.scalar(
                    update(CandidateObject)
                    .where(
                        CandidateObject.id == obj.id,
                        CandidateObject.version == obj.version,
                        or_(
                            CandidateObject.status != "claimed",
                            CandidateObject.claimed_until <= func.clock_timestamp(),
                        ),
                    )
                    .values(
                        status="claimed",
                        claim_id=claim_id,
                        version=version,
                        collector_id=body.collector_id,
                        claimed_until=claimed_until,
                    )
                    .returning(CandidateObject.id)
                )
                if changed is None:
                    continue
                for name, value in (
                    ("status", "claimed"),
                    ("claim_id", claim_id),
                    ("version", version),
                    ("collector_id", body.collector_id),
                    ("claimed_until", claimed_until),
                ):
                    set_committed_value(obj, name, value)
            assert obj.claim_id is not None and obj.claimed_until is not None
            claims.append(
                wire.CandidateCollectionClaim(
                    object_id=obj.id,
                    key=obj.key,
                    claim_id=obj.claim_id,
                    version=obj.version,
                    claimed_until=obj.claimed_until,
                )
            )
    return wire.CandidateCollectionClaims(claims=claims)


async def finalize_collection(
    session: AsyncSession, body: wire.CandidateFinalizeCollection
) -> wire.CandidateFinalized:
    """Delete a claimed object row, then its empty candidate, then the empty group."""

    await _installation(session)
    obj = await session.get(CandidateObject, body.object_id)
    if obj is None:
        return wire.CandidateFinalized()
    group = await _group(session, obj.work_item_id)
    if group is None:
        raise CandidateRefusal("retained_identity")
    candidate = (
        await _candidate(session, obj.work_item_id, obj.candidate_id)
        if obj.candidate_id is not None
        else None
    )
    now = await database_now(session)
    if (
        obj.status != "claimed"
        or obj.claim_id != body.claim_id
        or obj.collector_id != body.collector_id
        or obj.version != body.version
        or obj.claimed_until is None
        or obj.claimed_until <= now
    ):
        raise CandidateRefusal("collection_claim_conflict")
    removed = await session.scalar(
        delete(CandidateObject)
        .where(
            CandidateObject.id == obj.id,
            CandidateObject.claim_id == body.claim_id,
            CandidateObject.collector_id == body.collector_id,
            CandidateObject.version == body.version,
            CandidateObject.claimed_until > func.clock_timestamp(),
        )
        .returning(CandidateObject.id)
    )
    if removed is None:
        raise CandidateRefusal("collection_claim_conflict")
    if candidate is not None and not await session.scalar(
        select(CandidateObject.id).where(CandidateObject.candidate_id == candidate.id).limit(1)
    ):
        await session.execute(
            delete(ReaderLease).where(
                ReaderLease.candidate_id == candidate.id, ReaderLease.expires_at <= now
            )
        )
        await session.execute(
            delete(Candidate).where(
                Candidate.id == candidate.id,
                Candidate.version == candidate.version,
                Candidate.status == "tombstoned",
            )
        )
    if not await session.scalar(
        select(CandidateObject.id)
        .where(CandidateObject.work_item_id == group.work_item_id)
        .limit(1)
    ) and not await session.scalar(
        select(Candidate.id).where(Candidate.work_item_id == group.work_item_id).limit(1)
    ):
        await session.execute(delete(Group).where(Group.work_item_id == group.work_item_id))
    return wire.CandidateFinalized()


async def transfer_ownership(
    session: AsyncSession,
    *,
    work_item_id: uuid.UUID,
    candidate_id: uuid.UUID,
    to_request_id: uuid.UUID,
) -> Candidate:
    """Hand a complete, unexpired candidate to a pending successor request.

    Under the WorkItem lock, the target must be a non-terminal request of the
    same WorkItem with a greater sequence than the current owner. The origin
    request never changes; the previous owner and the transfer time are
    recorded, and the expiry is cleared so it is fixed again, exactly, from the
    successor's final disposition. A read never reaches this path. The caller
    commits; a refusal leaves nothing written.

    Locks follow the module order: WorkItem, target request, installation row,
    group, candidate. Collection takes the installation row first too, so a
    transfer and a collection pass never interleave.
    """

    item = await lock_work_item(session, work_item_id)
    if item is None:
        raise CandidateRefusal("candidate_identity")
    target = await lock_request(session, work_item_id=item.id, request_id=to_request_id)
    if target is None:
        raise CandidateRefusal("transfer_target_identity")
    await _installation(session)
    if await _group(session, item.id) is None:
        raise CandidateRefusal("candidate_identity")
    candidate = await _candidate(session, item.id, candidate_id)
    await _fix_expiry(session, candidate)
    now = await database_now(session)
    if candidate.status != "complete" or not _unexpired(candidate, now):
        raise CandidateRefusal("candidate_unavailable")
    if target.terminal_at is not None:
        raise CandidateRefusal("transfer_target_terminal")
    owner_sequence = await session.scalar(
        select(ExecutionRequest.sequence).where(ExecutionRequest.id == candidate.owner_request_id)
    )
    if owner_sequence is None or target.sequence <= owner_sequence:
        raise CandidateRefusal("transfer_target_sequence")
    changed = await session.scalar(
        update(Candidate)
        .where(
            Candidate.id == candidate.id,
            Candidate.version == candidate.version,
            Candidate.status == "complete",
            Candidate.owner_request_id == candidate.owner_request_id,
            or_(Candidate.expires_at.is_(None), Candidate.expires_at > func.clock_timestamp()),
        )
        .values(
            owner_request_id=target.id,
            previous_owner_request_id=candidate.owner_request_id,
            owner_transferred_at=now,
            expires_at=None,
            version=candidate.version + 1,
        )
        .returning(Candidate.id)
    )
    if changed is None:
        raise CandidateRefusal("candidate_unavailable")
    await session.refresh(candidate)
    return candidate


async def list_metadata(session: AsyncSession, work_item_id: uuid.UUID) -> wire.CandidatePublicList:
    """Operator metadata: ids, statuses, timestamps, counts, sizes and digests only."""

    item = await session.get(WorkItem, work_item_id)
    if item is None:
        raise CandidateRefusal("work_item_not_found", 404)
    await _installation(session)
    group = await _group(session, work_item_id)
    candidates: list[Candidate] = []
    objects: list[CandidateObject] = []
    if group is not None:
        candidates = list(
            await session.scalars(
                select(Candidate)
                .where(Candidate.work_item_id == work_item_id)
                .order_by(Candidate.created_at, Candidate.id)
                .with_for_update()
                .execution_options(populate_existing=True)
            )
        )
        for candidate in candidates:
            await _fix_expiry(session, candidate)
        objects = await _objects(session, work_item_id)
    now = await database_now(session)
    recorded = await session.scalar(
        select(Outcome)
        .where(Outcome.work_item_id == work_item_id)
        .execution_options(populate_existing=True)
    )
    owners: dict[uuid.UUID, datetime | None] = {}
    if candidates:
        rows = await session.execute(
            select(ExecutionRequest.id, ExecutionRequest.terminal_at).where(
                ExecutionRequest.id.in_([row.owner_request_id for row in candidates])
            )
        )
        owners = {row.id: row.terminal_at for row in rows}
    base = next((obj for obj in objects if obj.kind == "base"), None)
    public = []
    for candidate in candidates:
        patch = next((obj for obj in objects if obj.candidate_id == candidate.id), None)
        available = (
            candidate.status == "complete"
            and _unexpired(candidate, now)
            and patch is not None
            and patch.status == "complete"
            and base is not None
            and base.status == "complete"
        )
        public.append(
            wire.CandidatePublic(
                candidate_id=candidate.id,
                origin_request_id=candidate.origin_request_id,
                owner_request_id=candidate.owner_request_id,
                previous_owner_request_id=candidate.previous_owner_request_id,
                owner_transferred_at=candidate.owner_transferred_at,
                runtime_epoch=candidate.runtime_epoch,
                status=cast(wire.CandidateStatus, candidate.status),
                created_at=candidate.created_at,
                completed_at=candidate.completed_at,
                final_disposition_at=owners.get(candidate.owner_request_id),
                expires_at=candidate.expires_at,
                available=available,
                changed_path_count=len(candidate.changed_paths),
                patch=(
                    wire.CandidatePublicPatch(size_bytes=patch.size_bytes, sha256=patch.sha256)
                    if patch is not None
                    else None
                ),
                base=(
                    wire.CandidatePublicBase(
                        size_bytes=base.size_bytes,
                        sha256=base.sha256,
                        base_commit=group.base_commit,
                    )
                    if base is not None and group is not None
                    else None
                ),
            )
        )
    return wire.CandidatePublicList(
        work_item_id=item.id,
        capture_outcome=recorded.code if recorded is not None else None,
        capture_outcome_at=recorded.recorded_at if recorded is not None else None,
        candidates=public,
    )
