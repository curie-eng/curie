"""Recovery metadata transactions; object transport runs outside this module."""

from __future__ import annotations

import hashlib
import json
import uuid
from datetime import datetime, timedelta
from typing import Any

from sqlalchemy import delete, func, or_, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from .config import Settings
from .models import (
    ExecutionRequest,
    WorkItem,
)
from .models import (
    WorkspaceRecoveryCheckpoint as Checkpoint,
)
from .models import (
    WorkspaceRecoveryGroup as Group,
)
from .models import (
    WorkspaceRecoveryInstallation as Installation,
)
from .models import (
    WorkspaceRecoveryObject as RecoveryObject,
)
from .models import (
    WorkspaceRecoveryReader as ReaderLease,
)
from .schemas import workspace_recovery as wire
from .workitems.lifecycle import database_now, lock_request, lock_work_item

STAGING_EXPIRY_SECONDS = 600


class RecoveryRefusal(Exception):
    def __init__(self, code: str, status: int = 409, *, recorded: bool = False) -> None:
        self.code = f"workspace_recovery.{code}"
        self.status = status
        self.recorded = recorded
        super().__init__(self.code)


def _enabled(settings: Settings) -> None:
    if not settings.workspace_recovery_enabled:
        raise RecoveryRefusal("disabled", 503)


def _live_authority(
    item: WorkItem, source: ExecutionRequest, now: datetime, *, starting: bool
) -> None:
    if (
        item.cancelled_at is not None
        or source.status != "failed"
        or source.started_at is None
        or source.terminal_at is None
        or source.termination_observation is not None
        or source.terminal_cause
        in {
            None,
            "owner_lost",
            "start_failed",
            "execution_deadline",
            "issue_cancelled",
            "lineage_closed",
        }
    ):
        raise RecoveryRefusal("ineligible_source")
    if starting and (
        source.runtime_heartbeat_expires_at is None
        or source.runtime_heartbeat_expires_at <= now
        or source.execution_deadline is None
        or source.execution_deadline <= now
    ):
        raise RecoveryRefusal("expired_source")


async def _authority(
    session: AsyncSession, body: wire.RecoveryCaptureAuthority, *, starting: bool
) -> tuple[WorkItem, ExecutionRequest]:
    item = await lock_work_item(session, body.work_item_id)
    if item is None:
        raise RecoveryRefusal("source_identity")
    source = await lock_request(session, work_item_id=item.id, request_id=body.source_request_id)
    if (
        source is None
        or source.runtime_epoch != body.runtime_epoch
        or source.dispatch_generation != body.dispatch_generation
        or source.sequence
        != await session.scalar(
            select(func.max(ExecutionRequest.sequence)).where(
                ExecutionRequest.work_item_id == item.id
            )
        )
        or item.base_commit is None
        or item.base_branch is None
    ):
        raise RecoveryRefusal("source_identity")
    _live_authority(item, source, await database_now(session), starting=starting)
    return item, source


async def _installation(session: AsyncSession) -> None:
    row = await session.scalar(select(Installation).where(Installation.id == 1).with_for_update())
    if row is None:
        raise RecoveryRefusal("installation_unavailable", 503)


async def _group(session: AsyncSession, item_id: uuid.UUID) -> Group | None:
    group: Group | None = await session.scalar(
        select(Group)
        .where(Group.work_item_id == item_id)
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    return group


async def _checkpoint(
    session: AsyncSession, item_id: uuid.UUID, checkpoint_id: uuid.UUID
) -> Checkpoint:
    row = await session.scalar(
        select(Checkpoint)
        .where(Checkpoint.id == checkpoint_id, Checkpoint.work_item_id == item_id)
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    if row is None:
        raise RecoveryRefusal("checkpoint_identity")
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


def _capture_identity(checkpoint: Checkpoint, body: wire.RecoveryCaptureAuthority) -> None:
    if (
        checkpoint.source_request_id != body.source_request_id
        or checkpoint.runtime_epoch != body.runtime_epoch
        or checkpoint.dispatch_generation != body.dispatch_generation
    ):
        raise RecoveryRefusal("checkpoint_source_identity")


def _outcome(item: WorkItem, code: str, at: datetime) -> None:
    item.workspace_recovery_outcome = code
    item.workspace_recovery_outcome_at = at


def _outcome_if_not_later(item: WorkItem, code: str, at: datetime, bound: datetime) -> bool:
    """Write ``code`` unless the WorkItem holds an outcome written after ``bound``.

    ``bound`` is the latest moment the caller's own capture wrote anything, so an
    outcome a newer capture recorded is never displaced by an older one.
    """

    current = item.workspace_recovery_outcome_at
    if current is not None and current > bound:
        return False
    _outcome(item, code, at)
    return True


async def _record_refusal(
    session: AsyncSession, item: WorkItem, code: str, status: int = 409
) -> None:
    _outcome(item, code, await database_now(session))
    raise RecoveryRefusal(code, status, recorded=True)


async def _objects(session: AsyncSession, item_id: uuid.UUID) -> list[RecoveryObject]:
    return list(
        await session.scalars(
            select(RecoveryObject)
            .where(RecoveryObject.work_item_id == item_id)
            .order_by(RecoveryObject.kind, RecoveryObject.id)
        )
    )


def _receipt(obj: RecoveryObject) -> wire.RecoveryObjectReceipt:
    return wire.RecoveryObjectReceipt.model_validate(
        {
            "kind": obj.kind,
            "key": obj.key,
            "size_bytes": obj.size_bytes,
            "sha256": obj.sha256,
        }
    )


async def _reservation(session: AsyncSession, cp: Checkpoint) -> wire.RecoveryReservation:
    objects = [
        obj
        for obj in await _objects(session, cp.work_item_id)
        if obj.checkpoint_id == cp.id or (cp.includes_base and obj.kind == "base")
    ]
    if len(objects) != (3 if cp.includes_base else 2):
        raise RecoveryRefusal("checkpoint_collecting")
    return wire.RecoveryReservation(
        checkpoint_id=cp.id,
        objects=[_receipt(obj) for obj in objects],
        capture_deadline=cp.capture_deadline,
        staging_expires_at=cp.staging_expires_at,
    )


async def context(
    session: AsyncSession, settings: Settings, body: wire.RecoveryCaptureAuthority
) -> wire.RecoveryCaptureContext:
    """The WorkItem's repository identity and trusted base, for a capture about to start."""

    _enabled(settings)
    item, _ = await _authority(session, body, starting=True)
    assert item.base_commit is not None and item.base_branch is not None
    return wire.RecoveryCaptureContext(
        repository=wire.RecoveryRepository(
            provider=item.code_host_kind,
            host=item.code_host_host,
            project_id=item.repository_project_id,
            path=item.repository_path,
        ),
        base_commit=item.base_commit,
        target_branch=item.base_branch,
    )


async def outcome(
    session: AsyncSession, settings: Settings, body: wire.RecoveryOutcome
) -> wire.RecoveryOutcomeRecorded:
    """Record a capture that ended before any reservation (ADR 0251 section 3).

    The source must be the latest failed request, but an expired heartbeat or
    deadline does not block it. An outcome written after the source's terminus
    (its own reservation, or a newer capture) is kept.
    """

    _enabled(settings)
    item, source = await _authority(session, body, starting=False)
    assert source.terminal_at is not None
    recorded = _outcome_if_not_later(
        item, body.reason, await database_now(session), source.terminal_at
    )
    await session.flush()
    return wire.RecoveryOutcomeRecorded(recorded=recorded)


async def reserve(
    session: AsyncSession, settings: Settings, body: wire.RecoveryReserve
) -> wire.RecoveryReservation:
    _enabled(settings)
    item, source = await _authority(session, body, starting=True)
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
        raise RecoveryRefusal("workspace_identity")
    await _installation(session)
    group = await _group(session, item.id)
    now = await database_now(session)
    _live_authority(item, source, now, starting=True)
    contents = body.model_dump(mode="json", exclude={"capture_budget_seconds"})
    contents["objects"] = sorted(contents["objects"], key=lambda value: value["kind"])
    digest = hashlib.sha256(json.dumps(contents, sort_keys=True).encode()).hexdigest()
    replay = await session.scalar(
        select(Checkpoint)
        .where(
            Checkpoint.source_request_id == source.id,
            Checkpoint.idempotency_key == body.idempotency_key,
        )
        .with_for_update()
    )
    if replay is not None:
        if replay.content_sha256 != digest:
            raise RecoveryRefusal("idempotency_conflict")
        return await _reservation(session, replay)
    caps = {"base": settings.workspace_max_archive_bytes, "patch": 900000, "manifest": 1048576}
    for obj in body.objects:
        if obj.size_bytes > caps[obj.kind]:
            await _record_refusal(session, item, f"{obj.kind}_too_large", 413)
    if group is not None and _group_identity(group) != identity:
        await _record_refusal(session, item, "retained_base_conflict")
    existing = await _objects(session, item.id) if group is not None else []
    base = next((obj for obj in existing if obj.kind == "base"), None)
    includes_base = any(obj.kind == "base" for obj in body.objects)
    if group is not None:
        if base is None or base.status != "complete":
            await _record_refusal(session, item, "retained_base_unavailable")
        if includes_base:
            raise RecoveryRefusal("retained_base_declaration")
    elif not includes_base:
        raise RecoveryRefusal("base_required")
    checkpoints = list(
        await session.scalars(select(Checkpoint).where(Checkpoint.work_item_id == item.id))
    )
    if (
        len(checkpoints) >= 3
        or len(existing) + len(body.objects) > 7
        or any(cp.status == "staging" for cp in checkpoints)
    ):
        await _record_refusal(session, item, "work_item_capacity", 429)
    if (
        group is None
        and (await session.scalar(select(func.count()).select_from(Group)) or 0) >= 10000
    ):
        await _record_refusal(session, item, "retained_work_item_capacity", 429)
    used = await session.scalar(select(func.coalesce(func.sum(RecoveryObject.size_bytes), 0)))
    cap = settings.workspace_recovery_max_bytes
    if cap is None or int(used or 0) + sum(obj.size_bytes for obj in body.objects) > cap:
        await _record_refusal(session, item, "installation_capacity", 429)
    if group is None:
        group = Group(
            work_item_id=item.id,
            repository_provider=identity[0],
            repository_host=identity[1],
            repository_project_id=identity[2],
            repository_path=identity[3],
            base_commit=identity[4],
            target_branch=identity[5],
        )
        session.add(group)
        await session.flush()
    cp = Checkpoint(
        id=uuid.uuid4(),
        work_item_id=item.id,
        source_request_id=source.id,
        runtime_epoch=body.runtime_epoch,
        dispatch_generation=body.dispatch_generation,
        idempotency_key=body.idempotency_key,
        content_sha256=digest,
        includes_base=includes_base,
        capture_deadline=now + timedelta(seconds=body.capture_budget_seconds),
        staging_expires_at=now + timedelta(seconds=STAGING_EXPIRY_SECONDS),
    )
    session.add(cp)
    await session.flush()
    for obj in body.objects:
        object_id = uuid.uuid4()
        key = (
            f"workspace-recovery/retained/{item.id}/{object_id}/base"
            if obj.kind == "base"
            else f"workspace-recovery/checkpoints/{cp.id}/{obj.kind}"
        )
        session.add(
            RecoveryObject(
                id=object_id,
                work_item_id=item.id,
                checkpoint_id=None if obj.kind == "base" else cp.id,
                kind=obj.kind,
                key=key,
                size_bytes=obj.size_bytes,
                sha256=obj.sha256,
                staging_expires_at=cp.staging_expires_at,
            )
        )
    _outcome(item, "reserved", now)
    await session.flush()
    return await _reservation(session, cp)


async def _transition(
    session: AsyncSession,
    cp: Checkpoint,
    *,
    new_status: str,
    completed_at: datetime | None = None,
    expires_at: datetime | None = None,
) -> None:
    values: dict[str, Any] = {"status": new_status, "version": cp.version + 1}
    if completed_at is not None:
        values.update(completed_at=completed_at, expires_at=expires_at)
    changed = await session.scalar(
        update(Checkpoint)
        .where(
            Checkpoint.id == cp.id,
            Checkpoint.version == cp.version,
            Checkpoint.status == cp.status,
            *(
                [Checkpoint.capture_deadline > func.clock_timestamp()]
                if new_status == "complete"
                else []
            ),
        )
        .values(**values)
        .returning(Checkpoint.id)
    )
    if changed is None:
        raise RecoveryRefusal("checkpoint_version_conflict")


async def _tombstone(session: AsyncSession, group: Group, cp: Checkpoint) -> None:
    if cp.status == "tombstoned":
        return
    await _transition(session, cp, new_status="tombstoned")
    if group.last_good_checkpoint_id == cp.id:
        group.last_good_checkpoint_id = None
    await session.execute(
        update(RecoveryObject)
        .where(
            RecoveryObject.checkpoint_id == cp.id,
            RecoveryObject.status.in_(("staging", "complete")),
        )
        .values(status="tombstoned")
    )


def _completion(cp: Checkpoint) -> wire.RecoveryCompletion:
    assert cp.completed_at is not None and cp.expires_at is not None
    return wire.RecoveryCompletion(
        checkpoint_id=cp.id, completed_at=cp.completed_at, expires_at=cp.expires_at
    )


async def _capture_context(
    session: AsyncSession, body: wire.RecoveryComplete
) -> tuple[WorkItem, Group, Checkpoint, datetime]:
    item, source = await _authority(session, body, starting=False)
    await _installation(session)
    group = await _group(session, item.id)
    if group is None or _group_identity(group) != _item_identity(item):
        raise RecoveryRefusal("retained_identity")
    cp = await _checkpoint(session, item.id, body.checkpoint_id)
    now = await database_now(session)
    _live_authority(item, source, now, starting=False)
    _capture_identity(cp, body)
    return item, group, cp, now


async def complete(session: AsyncSession, body: wire.RecoveryComplete) -> wire.RecoveryCompletion:
    item, group, cp, now = await _capture_context(session, body)
    reservation = await _reservation(session, cp)
    expected = sorted(obj.model_dump_json() for obj in reservation.objects)
    actual = sorted(obj.model_dump_json() for obj in body.receipts)
    if expected != actual:
        raise RecoveryRefusal("receipt_mismatch")
    if cp.status == "complete":
        return _completion(cp)
    if cp.status != "staging" or cp.capture_deadline <= now:
        raise RecoveryRefusal("capture_deadline_or_state")
    objects = await _objects(session, item.id)
    base = next((obj for obj in objects if obj.kind == "base"), None)
    if base is None or base.status != ("staging" if cp.includes_base else "complete"):
        raise RecoveryRefusal("base_incomplete")
    if any(obj.status != "staging" for obj in objects if obj.checkpoint_id == cp.id):
        raise RecoveryRefusal("checkpoint_incomplete")
    await _transition(
        session,
        cp,
        new_status="complete",
        completed_at=now,
        expires_at=now + timedelta(hours=24),
    )
    for obj in objects:
        if obj.checkpoint_id == cp.id or (cp.includes_base and obj.kind == "base"):
            obj.status = "complete"
    completed = list(
        await session.scalars(
            select(Checkpoint)
            .where(Checkpoint.work_item_id == item.id, Checkpoint.status == "complete")
            .order_by(Checkpoint.completed_at.desc(), Checkpoint.id.desc())
            .with_for_update()
        )
    )
    for old in completed[2:]:
        await _tombstone(session, group, old)
    group.last_good_checkpoint_id = cp.id
    _outcome(item, "complete", now)
    await session.flush()
    return _completion(cp)


async def _cleanup_context(
    session: AsyncSession, body: wire.RecoveryFail
) -> tuple[WorkItem, Group, Checkpoint, datetime]:
    """Authority for cleaning up a capture's own checkpoint.

    Unlike ``_capture_context`` the source need not be the WorkItem's latest
    request, nor unexpired: a predecessor whose lease was lost after a successor
    was admitted still tombstones its own staging. The exact source (request,
    epoch, dispatch generation) and its eligible failure are still required.
    """

    item = await lock_work_item(session, body.work_item_id)
    if item is None:
        raise RecoveryRefusal("source_identity")
    source = await lock_request(session, work_item_id=item.id, request_id=body.source_request_id)
    if (
        source is None
        or source.runtime_epoch != body.runtime_epoch
        or source.dispatch_generation != body.dispatch_generation
        or item.base_commit is None
        or item.base_branch is None
    ):
        raise RecoveryRefusal("source_identity")
    await _installation(session)
    group = await _group(session, item.id)
    if group is None or _group_identity(group) != _item_identity(item):
        raise RecoveryRefusal("retained_identity")
    cp = await _checkpoint(session, item.id, body.checkpoint_id)
    now = await database_now(session)
    _live_authority(item, source, now, starting=False)
    _capture_identity(cp, body)
    return item, group, cp, now


async def _retract(
    session: AsyncSession, item: WorkItem, group: Group, cp: Checkpoint, now: datetime
) -> wire.RecoveryCaptureFailure:
    """Withdraw a just-completed checkpoint whose capture lost its lease after commit.

    Its objects are left for the collector, whose reader-pinning rule keeps a
    reader that already leased it working until its lease expires.
    """

    if group.last_good_checkpoint_id != cp.id:
        raise RecoveryRefusal("checkpoint_state")
    assert cp.completed_at is not None
    await _tombstone(session, group, cp)
    group.last_good_checkpoint_id = await session.scalar(
        select(Checkpoint.id)
        .where(
            Checkpoint.work_item_id == item.id,
            Checkpoint.status == "complete",
            Checkpoint.id != cp.id,
        )
        .order_by(Checkpoint.completed_at.desc(), Checkpoint.id.desc())
        .limit(1)
    )
    _outcome_if_not_later(item, "lease_lost", now, cp.completed_at)
    await session.flush()
    return wire.RecoveryCaptureFailure(checkpoint_id=cp.id, status="retracted")


async def fail(session: AsyncSession, body: wire.RecoveryFail) -> wire.RecoveryCaptureFailure:
    item, group, cp, now = await _cleanup_context(session, body)
    if cp.status == "tombstoned":
        return wire.RecoveryCaptureFailure(checkpoint_id=cp.id)
    if cp.status == "complete" and body.reason == "lease_lost":
        return await _retract(session, item, group, cp, now)
    if cp.status != "staging":
        raise RecoveryRefusal("checkpoint_state")
    await _tombstone(session, group, cp)
    await session.execute(
        update(RecoveryObject)
        .where(
            RecoveryObject.work_item_id == item.id,
            RecoveryObject.kind == "base",
            RecoveryObject.status == "staging",
            ~select(Checkpoint.id)
            .where(Checkpoint.work_item_id == item.id, Checkpoint.status == "complete")
            .exists(),
        )
        .values(status="tombstoned")
    )
    reserved_at = cp.staging_expires_at - timedelta(seconds=STAGING_EXPIRY_SECONDS)
    _outcome_if_not_later(item, body.reason, now, reserved_at)
    return wire.RecoveryCaptureFailure(checkpoint_id=cp.id)


async def _expire_readers(session: AsyncSession, item_id: uuid.UUID, now: datetime) -> None:
    await session.execute(
        delete(ReaderLease).where(
            ReaderLease.work_item_id == item_id, ReaderLease.expires_at <= now
        )
    )


async def acquire_reader(
    session: AsyncSession, settings: Settings, body: wire.RecoveryAcquireReader
) -> wire.RecoveryReader:
    _enabled(settings)
    await _installation(session)
    group = await _group(session, body.work_item_id)
    if group is None:
        raise RecoveryRefusal("checkpoint_identity")
    cp = await _checkpoint(session, group.work_item_id, body.checkpoint_id)
    now = await database_now(session)
    if cp.status != "complete" or cp.expires_at is None or cp.expires_at <= now:
        raise RecoveryRefusal("checkpoint_unavailable")
    objects = [
        obj
        for obj in await _objects(session, group.work_item_id)
        if obj.kind == "base" or obj.checkpoint_id == cp.id
    ]
    if {obj.kind for obj in objects} != {"base", "patch", "manifest"} or any(
        obj.status != "complete" for obj in objects
    ):
        raise RecoveryRefusal("checkpoint_incomplete")
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
            if reader.checkpoint_id == cp.id and reader.holder_id == body.holder_id
        ),
        None,
    )
    if lease is None:
        if len(readers) >= 8:
            raise RecoveryRefusal("reader_capacity", 429)
        changed = await session.scalar(
            update(Checkpoint)
            .where(
                Checkpoint.id == cp.id,
                Checkpoint.version == cp.version,
                Checkpoint.status == "complete",
                Checkpoint.expires_at > func.clock_timestamp(),
            )
            .values(version=cp.version + 1)
            .returning(Checkpoint.id)
        )
        if changed is None:
            raise RecoveryRefusal("checkpoint_unavailable")
        lease = ReaderLease(
            id=uuid.uuid4(),
            work_item_id=group.work_item_id,
            checkpoint_id=cp.id,
            holder_id=body.holder_id,
            expires_at=now + timedelta(seconds=300),
        )
        session.add(lease)
    return wire.RecoveryReader(
        lease_id=lease.id,
        checkpoint_id=cp.id,
        holder_id=lease.holder_id,
        expires_at=lease.expires_at,
        objects=[_receipt(obj) for obj in objects],
    )


async def release_reader(
    session: AsyncSession, body: wire.RecoveryReleaseReader
) -> wire.RecoveryAcknowledged:
    await _installation(session)
    lease = await session.get(ReaderLease, body.lease_id)
    if lease is None:
        return wire.RecoveryAcknowledged()
    await _group(session, lease.work_item_id)
    if lease.holder_id != body.holder_id:
        raise RecoveryRefusal("reader_holder_identity")
    await session.execute(
        delete(ReaderLease).where(
            ReaderLease.id == body.lease_id, ReaderLease.holder_id == body.holder_id
        )
    )
    return wire.RecoveryAcknowledged()


async def claim_collection(
    session: AsyncSession, body: wire.RecoveryClaimCollection
) -> wire.RecoveryCollectionClaims:
    await _installation(session)
    groups = list(
        await session.scalars(
            select(Group).order_by(Group.work_item_id).with_for_update(skip_locked=True)
        )
    )
    claims: list[wire.RecoveryCollectionClaim] = []
    for group in groups:
        if len(claims) >= body.limit:
            break
        now = await database_now(session)
        await _expire_readers(session, group.work_item_id, now)
        checkpoints = list(
            await session.scalars(
                select(Checkpoint)
                .where(Checkpoint.work_item_id == group.work_item_id)
                .order_by(Checkpoint.id)
                .with_for_update()
            )
        )
        for cp in checkpoints:
            if (cp.status == "staging" and cp.staging_expires_at <= now) or (
                cp.status == "complete" and cp.expires_at is not None and cp.expires_at <= now
            ):
                await _tombstone(session, group, cp)
        readers = list(
            await session.scalars(
                select(ReaderLease).where(ReaderLease.work_item_id == group.work_item_id)
            )
        )
        pinned = {reader.checkpoint_id for reader in readers}
        eligible = {
            cp.id for cp in checkpoints if cp.status == "tombstoned" and cp.id not in pinned
        }
        objects = await _objects(session, group.work_item_id)
        for obj in objects:
            if len(claims) >= body.limit:
                break
            if obj.kind == "base":
                if checkpoints or readers:
                    continue
            elif obj.checkpoint_id not in eligible:
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
                changed = await session.scalar(
                    update(RecoveryObject)
                    .where(
                        RecoveryObject.id == obj.id,
                        RecoveryObject.version == obj.version,
                        or_(
                            RecoveryObject.status != "claimed",
                            RecoveryObject.claimed_until <= func.clock_timestamp(),
                        ),
                    )
                    .values(
                        status="claimed",
                        claim_id=claim_id,
                        version=version,
                        collector_id=body.collector_id,
                        claimed_until=now + timedelta(seconds=300),
                    )
                    .returning(RecoveryObject.id)
                )
                if changed is None:
                    continue
            assert obj.claim_id is not None and obj.claimed_until is not None
            claims.append(
                wire.RecoveryCollectionClaim(
                    object_id=obj.id,
                    key=obj.key,
                    claim_id=obj.claim_id,
                    version=obj.version,
                    claimed_until=obj.claimed_until,
                )
            )
    return wire.RecoveryCollectionClaims(claims=claims)


async def finalize_collection(
    session: AsyncSession, body: wire.RecoveryFinalizeCollection
) -> wire.RecoveryFinalized:
    await _installation(session)
    obj = await session.get(RecoveryObject, body.object_id)
    if obj is None:
        return wire.RecoveryFinalized()
    group = await _group(session, obj.work_item_id)
    if group is None:
        raise RecoveryRefusal("retained_identity")
    cp = (
        await _checkpoint(session, obj.work_item_id, obj.checkpoint_id)
        if obj.checkpoint_id is not None
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
        raise RecoveryRefusal("collection_claim_conflict")
    removed = await session.scalar(
        delete(RecoveryObject)
        .where(
            RecoveryObject.id == obj.id,
            RecoveryObject.claim_id == body.claim_id,
            RecoveryObject.collector_id == body.collector_id,
            RecoveryObject.version == body.version,
            RecoveryObject.claimed_until > func.clock_timestamp(),
        )
        .returning(RecoveryObject.id)
    )
    if removed is None:
        raise RecoveryRefusal("collection_claim_conflict")
    if cp is not None and not await session.scalar(
        select(RecoveryObject.id).where(RecoveryObject.checkpoint_id == cp.id).limit(1)
    ):
        if group.last_good_checkpoint_id == cp.id:
            group.last_good_checkpoint_id = None
            await session.flush()
        await session.execute(
            delete(Checkpoint).where(
                Checkpoint.id == cp.id,
                Checkpoint.version == cp.version,
                Checkpoint.status == "tombstoned",
            )
        )
    if not await session.scalar(
        select(RecoveryObject.id).where(RecoveryObject.work_item_id == group.work_item_id).limit(1)
    ) and not await session.scalar(
        select(Checkpoint.id).where(Checkpoint.work_item_id == group.work_item_id).limit(1)
    ):
        await session.execute(delete(Group).where(Group.work_item_id == group.work_item_id))
    return wire.RecoveryFinalized()
