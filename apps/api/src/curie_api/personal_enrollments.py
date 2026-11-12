"""Owner enrollment and the personal grant custody protocol (#5377, ADR 0268 sections 3-4).

An owner's enrollment is one ``personal_claude_enrollments`` header (#5376).
Each generation's grant lives in an immutable Kubernetes Secret
(``custody_store``) that a token-free ``personal_claude_custody`` row names.
Nothing here is a cross-store transaction; the protocol keeps the database
authoritative and every Secret it creates owned by exactly one row:

1. Tx1 stage: lock the header, check the intent and expected version, reclaim
   this owner's expired staging, and insert a ``staged`` row. The partial
   unique index on staged rows lets one writer per owner stage at a time.
2. Create the Secret, outside any transaction. A failed create leaves the
   previous generation active. A same-name orphan whose row is abandoned,
   retired or missing is deleted by its UID and the create retried once.
3. Record the UID, guarded on the row still being staged. Zero rows means a
   reclaim won the row: the loser path removes the Secret by that UID.
4. Tx2 publish: lock in ``personal_execution.revalidate``'s order (the owner's
   personal WorkItems, their requests, the header) and then the custody rows,
   under a lock timeout. The row must still be staged with our UID and the
   header must still satisfy the intent. Advance the header, mark the previous
   published row ``retiring``, publish ours, and fence the owner's requests
   (``lifecycle.fence_enrollment_generation``) in the same transaction.
5. The loser path first wins the row in the database (``abandoned``) and only
   then deletes the Secret, with its UID precondition.
6. Retire inline after publish or disconnect, best effort; the reconciler
   (``reconcile_custody``) retries.

Every destructive step wins its row first: an ``abandoned`` or ``retired`` row
never becomes published again, so a Secret deleted for such a row was never
the winner's. ``secret_absent_at`` is bookkeeping; the orphan sweep lists the
namespace on every pass regardless of it.

The grant is a function argument handed to the store and nowhere else. Store
failures surface as ``CustodyStoreError`` carrying a status code only; nothing
here logs a store message, a Secret name or a request body.
"""

from __future__ import annotations

import asyncio
import logging
import uuid
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import timedelta
from typing import Any, Literal

from sqlalchemy import and_, case, func, or_, select, text, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from .custody_store import (
    CUSTODY_LABEL,
    CUSTODY_SECRET_KEY,
    CustodySecretStore,
    CustodyStoreError,
    DeleteOutcome,
    SecretExists,
    StoredSecret,
    custody_annotations,
    custody_labels,
    custody_secret_name,
)
from .models import ExecutionRequest, PersonalClaudeCustody, PersonalClaudeEnrollment, WorkItem
from .workitems import lifecycle

logger = logging.getLogger(__name__)

Intent = Literal["enroll", "replace", "reconnect"]
EnrollmentState = Literal["absent", "active", "inactive"]
RefusalCode = Literal[
    "enrollment_absent",
    "stale_version",
    "enrollment_exists",
    "enrollment_active",
    "enrollment_inactive",
    "staging_conflict",
    "custody_unavailable",
    "enrollment_busy",
]

#: Bound on every lock the staging, publishing and disconnect transactions wait for.
LOCK_TIMEOUT = "5s"
# lock_not_available and deadlock_detected: the owner's rows are contended.
_BUSY_SQLSTATES = frozenset({"55P03", "40P01"})
_SWEEP_BATCH = 100

C = PersonalClaudeCustody
E = PersonalClaudeEnrollment


@dataclass(frozen=True)
class Custody:
    """What the protocol needs: the database, the Secret store and the grant namespace."""

    sessionmaker: async_sessionmaker[AsyncSession]
    store: CustodySecretStore
    namespace: str
    stage_lease_seconds: int


@dataclass(frozen=True)
class EnrollmentStatus:
    state: EnrollmentState
    generation: int | None
    version: int | None
    pending_runtimes: int
    pending_custody_cleanup: int


@dataclass(frozen=True)
class EnrollmentRefusal:
    code: RefusalCode


@dataclass
class CustodyPass:
    """What one reconciler pass did."""

    reclaimed: int = 0
    cleared: int = 0
    swept: int = 0


@dataclass(frozen=True)
class _Header:
    id: uuid.UUID
    generation: int
    version: int
    active: bool


@dataclass(frozen=True)
class _Staged:
    id: uuid.UUID
    enrollment_id: uuid.UUID
    generation: int
    namespace: str
    name: str


# --- reads -------------------------------------------------------------------------------


def _owned(model: Any, tenant_id: uuid.UUID, principal_id: uuid.UUID) -> tuple[Any, ...]:
    return (model.tenant_id == tenant_id, model.principal_id == principal_id)


async def read_status(
    session: AsyncSession, *, tenant_id: uuid.UUID, principal_id: uuid.UUID
) -> EnrollmentStatus:
    """The owner's state, generation, version and outstanding teardown. Token free."""

    header = (
        await session.execute(
            select(E.generation, E.version, E.active).where(*_owned(E, tenant_id, principal_id))
        )
    ).one_or_none()
    cleanup = await session.scalar(
        select(func.count())
        .select_from(C)
        .where(
            *_owned(C, tenant_id, principal_id),
            or_(
                C.state.in_(("staged", "retiring")),
                and_(C.state.in_(("abandoned", "retired")), C.secret_absent_at.is_(None)),
            ),
        )
    )
    if header is None:
        return EnrollmentStatus("absent", None, None, 0, int(cleanup or 0))
    runtimes = await session.scalar(
        select(func.count())
        .select_from(ExecutionRequest)
        .where(
            ExecutionRequest.tenant_id == tenant_id,
            ExecutionRequest.owner_principal_id == principal_id,
            ExecutionRequest.model_auth_mode == "personal",
            ExecutionRequest.enrollment_generation < header.generation,
            or_(
                ExecutionRequest.status.in_(("running", "cancellation_requested")),
                ExecutionRequest.teardown_unconfirmed_at.is_not(None),
                and_(
                    ExecutionRequest.started_at.is_not(None),
                    or_(
                        ExecutionRequest.termination_observation.is_(None),
                        func.btrim(ExecutionRequest.termination_observation) == "",
                    ),
                ),
            ),
        )
    )
    return EnrollmentStatus(
        "active" if header.active else "inactive",
        int(header.generation),
        int(header.version),
        int(runtimes or 0),
        int(cleanup or 0),
    )


async def status(
    sessionmaker: async_sessionmaker[AsyncSession],
    *,
    tenant_id: uuid.UUID,
    principal_id: uuid.UUID,
) -> EnrollmentStatus:
    async with sessionmaker() as session:
        return await read_status(session, tenant_id=tenant_id, principal_id=principal_id)


def _intent_refusal(
    header: _Header | None, intent: Intent, expected_version: int | None
) -> RefusalCode | None:
    """Enroll needs no header; replace needs it active and reconnect inactive, at the version."""

    if intent == "enroll":
        return None if header is None else "enrollment_exists"
    if header is None:
        return "enrollment_absent"
    if header.version != expected_version:
        return "stale_version"
    if intent == "replace" and not header.active:
        return "enrollment_inactive"
    if intent == "reconnect" and header.active:
        return "enrollment_active"
    return None


def _busy(exc: DBAPIError) -> bool:
    return getattr(exc.orig, "sqlstate", None) in _BUSY_SQLSTATES


async def _set_lock_timeout(session: AsyncSession) -> None:
    await session.execute(text(f"SET LOCAL lock_timeout = '{LOCK_TIMEOUT}'"))


async def _lock_header(
    session: AsyncSession, tenant_id: uuid.UUID, principal_id: uuid.UUID
) -> _Header | None:
    row = (
        await session.execute(
            select(E.id, E.generation, E.version, E.active)
            .where(*_owned(E, tenant_id, principal_id))
            .with_for_update()
        )
    ).one_or_none()
    if row is None:
        return None
    return _Header(row.id, int(row.generation), int(row.version), bool(row.active))


async def _lock_owner(
    session: AsyncSession, tenant_id: uuid.UUID, principal_id: uuid.UUID
) -> _Header | None:
    """Lock in ``personal_execution.revalidate``'s order: WorkItems, requests, header."""

    work_items = list(
        await session.scalars(
            select(WorkItem.id)
            .where(
                WorkItem.tenant_id == tenant_id,
                WorkItem.owner_principal_id == principal_id,
                WorkItem.model_auth_mode == "personal",
            )
            .order_by(WorkItem.id)
            .with_for_update()
        )
    )
    if work_items:
        await session.execute(
            select(ExecutionRequest.id)
            .where(ExecutionRequest.work_item_id.in_(work_items))
            .order_by(ExecutionRequest.work_item_id, ExecutionRequest.sequence)
            .with_for_update()
        )
    return await _lock_header(session, tenant_id, principal_id)


# --- the store, off the event loop -------------------------------------------------------


async def _store_call[T](call: Callable[..., T], *args: Any, **kwargs: Any) -> T:
    """Run one store call in a thread; any failure becomes a bare ``CustodyStoreError``.

    The original exception is dropped before raising, so neither its message
    nor its context reaches a log or a traceback.
    """

    exists = False
    status_code: int | None = None
    try:
        return await asyncio.to_thread(call, *args, **kwargs)
    except SecretExists:
        exists = True
    except CustodyStoreError as exc:
        status_code = exc.status
    except Exception:  # noqa: BLE001 - a store failure of any kind; text never read
        status_code = None
    if exists:
        raise SecretExists()
    raise CustodyStoreError(status_code)


async def _read(custody: Custody, namespace: str, name: str) -> StoredSecret | None:
    return await _store_call(custody.store.read, namespace, name)


async def _delete(custody: Custody, namespace: str, name: str, uid: str) -> DeleteOutcome:
    return await _store_call(custody.store.delete, namespace, name, uid=uid)


# --- custody row transitions -------------------------------------------------------------


async def _row(custody: Custody, row_id: uuid.UUID) -> Any:
    async with custody.sessionmaker() as session:
        return (
            await session.execute(
                select(
                    C.id,
                    C.state,
                    C.secret_namespace,
                    C.secret_name,
                    C.secret_uid,
                    C.secret_absent_at,
                ).where(C.id == row_id)
            )
        ).one_or_none()


async def _abandon(custody: Custody, row_id: uuid.UUID, *, uid: str | None = None) -> None:
    """Win a staged row for cleanup: ``abandoned``, recording the Secret UID if we know it."""

    async with custody.sessionmaker() as session:
        await session.execute(
            update(C)
            .where(C.id == row_id, C.state == "staged")
            .values(state="abandoned", version=C.version + 1)
        )
        if uid is not None:
            await session.execute(
                update(C)
                .where(C.id == row_id, C.state == "abandoned", C.secret_uid.is_(None))
                .values(secret_uid=uid, version=C.version + 1)
            )
        await session.commit()


async def _mark_absent(custody: Custody, row_id: uuid.UUID, *, unrecorded: bool = False) -> bool:
    """Record a verified absence: abandoned rows keep their state, retiring ones retire.

    ``unrecorded`` limits it to an abandoned row with no UID (R10 pass 3).
    """

    guards = [C.id == row_id, C.secret_absent_at.is_(None)]
    if unrecorded:
        guards += [C.state == "abandoned", C.secret_uid.is_(None)]
    else:
        guards.append(C.state.in_(("abandoned", "retiring")))
    retiring = C.state == "retiring"
    async with custody.sessionmaker() as session:
        marked = await session.scalar(
            update(C)
            .where(*guards)
            .values(
                secret_absent_at=func.clock_timestamp(),
                state=case((retiring, "retired"), else_=C.state),
                retired_at=case((retiring, func.clock_timestamp()), else_=C.retired_at),
                version=C.version + 1,
            )
            .returning(C.id)
        )
        await session.commit()
    return marked is not None


async def _clean_row(custody: Custody, row_id: uuid.UUID) -> bool:
    """Remove an abandoned or retiring row's Secret by its recorded UID (R10 pass 2).

    A 404, or a name that now holds another Secret, is absence; a UID mismatch
    never deletes. Failures leave the row for the reconciler. True when this
    call recorded the absence.
    """

    row = await _row(custody, row_id)
    if (
        row is None
        or row.state not in ("abandoned", "retiring")
        or row.secret_absent_at is not None
        or row.secret_uid is None
    ):
        return False
    return await _remove(custody, row_id, row.secret_namespace, row.secret_name, row.secret_uid)


async def _remove(custody: Custody, row_id: uuid.UUID, namespace: str, name: str, uid: str) -> bool:
    """Delete the Secret with ``uid`` for a row already won in the database, verify it
    is gone and record the absence. True when this call recorded it."""

    try:
        outcome = await _delete(custody, namespace, name, uid)
        if outcome == "deleted":
            again = await _read(custody, namespace, name)
            if again is not None and again.uid == uid:
                return False
    except CustodyStoreError as exc:
        logger.warning("personal custody %s: secret removal deferred (HTTP %s)", row_id, exc.status)
        return False
    return await _mark_absent(custody, row_id)


# --- the owner mutations -----------------------------------------------------------------


async def _stage(
    custody: Custody,
    *,
    tenant_id: uuid.UUID,
    principal_id: uuid.UUID,
    intent: Intent,
    expected_version: int | None,
) -> tuple[_Staged | EnrollmentRefusal, list[uuid.UUID]]:
    """Tx1. Returns the staged row (or the refusal) and the rows it reclaimed."""

    async with custody.sessionmaker() as session:
        try:
            await _set_lock_timeout(session)
            header = await _lock_header(session, tenant_id, principal_id)
            refused = _intent_refusal(header, intent, expected_version)
            if refused is not None:
                await session.rollback()
                return EnrollmentRefusal(refused), []
            reclaimed = list(
                await session.scalars(
                    update(C)
                    .where(
                        *_owned(C, tenant_id, principal_id),
                        C.state == "staged",
                        C.lease_expires_at < func.clock_timestamp(),
                    )
                    .values(state="abandoned", version=C.version + 1)
                    .returning(C.id)
                )
            )
            enrollment_id = header.id if header is not None else uuid.uuid4()
            generation = header.generation + 1 if header is not None else 1
            staged = _Staged(
                id=uuid.uuid4(),
                enrollment_id=enrollment_id,
                generation=generation,
                namespace=custody.namespace,
                name=custody_secret_name(enrollment_id, generation),
            )
            inserted = await session.scalar(
                insert(C)
                .values(
                    id=staged.id,
                    tenant_id=tenant_id,
                    principal_id=principal_id,
                    enrollment_id=enrollment_id,
                    generation=generation,
                    secret_namespace=staged.namespace,
                    secret_name=staged.name,
                    secret_key=CUSTODY_SECRET_KEY,
                    state="staged",
                    lease_expires_at=func.clock_timestamp()
                    + timedelta(seconds=custody.stage_lease_seconds),
                )
                .on_conflict_do_nothing()
                .returning(C.id)
            )
            await session.commit()
        except DBAPIError as exc:
            await session.rollback()
            if _busy(exc):
                return EnrollmentRefusal("enrollment_busy"), []
            raise
    if inserted is None:
        return EnrollmentRefusal("staging_conflict"), reclaimed
    return staged, reclaimed


async def _clear_orphan(custody: Custody, staged: _Staged) -> bool:
    """Delete a same-name Secret only when its annotation names an abandoned, retired
    or missing row. True when the name is free for one more create."""

    try:
        existing = await _read(custody, staged.namespace, staged.name)
        if existing is None:
            return True
        owner = existing.custody_id
        if owner is None or existing.labels.get(CUSTODY_LABEL) != "true" or owner == staged.id:
            return False
        row = await _row(custody, owner)
        if row is not None and row.state not in ("abandoned", "retired"):
            return False
        outcome = await _delete(custody, staged.namespace, staged.name, existing.uid)
        if outcome == "uid_mismatch":
            return False
        again = await _read(custody, staged.namespace, staged.name)
    except CustodyStoreError:
        return False
    if row is not None and row.secret_uid == existing.uid:
        await _mark_absent(custody, owner)
    return again is None


async def _create(custody: Custody, staged: _Staged, token: str) -> str | EnrollmentRefusal:
    """Step 2: the Secret's UID, or the refusal after leaving no Secret of ours behind."""

    labels = custody_labels(staged.enrollment_id, staged.generation)
    annotations = custody_annotations(staged.id)
    for attempt in (1, 2):
        try:
            return await _store_call(
                custody.store.create,
                staged.namespace,
                staged.name,
                token=token,
                labels=labels,
                annotations=annotations,
            )
        except SecretExists:
            if attempt == 1 and await _clear_orphan(custody, staged):
                continue
            # The name holds a Secret we do not own; ours was never created.
            await _abandon(custody, staged.id)
            await _mark_absent(custody, staged.id)
            return EnrollmentRefusal("custody_unavailable")
        except CustodyStoreError:
            return await _after_failed_create(custody, staged)
    raise AssertionError("unreachable")


async def _after_failed_create(custody: Custody, staged: _Staged) -> EnrollmentRefusal:
    """A create failed: learn whether it landed and leave nothing of ours behind."""

    try:
        existing = await _read(custody, staged.namespace, staged.name)
    except CustodyStoreError:
        await _abandon(custody, staged.id)
        return EnrollmentRefusal("custody_unavailable")
    if existing is not None and existing.custody_id == staged.id:
        return await _lose(custody, staged, existing.uid, "custody_unavailable")
    await _abandon(custody, staged.id)
    await _mark_absent(custody, staged.id)
    return EnrollmentRefusal("custody_unavailable")


async def _record_uid(custody: Custody, row_id: uuid.UUID, uid: str) -> bool:
    """Step 3. Exactly one row, or a reclaim won it."""

    async with custody.sessionmaker() as session:
        recorded = await session.scalar(
            update(C)
            .where(C.id == row_id, C.state == "staged", C.secret_uid.is_(None))
            .values(secret_uid=uid, version=C.version + 1)
            .returning(C.id)
        )
        await session.commit()
    return recorded is not None


async def _lose(
    custody: Custody, staged: _Staged, uid: str, code: RefusalCode
) -> EnrollmentRefusal:
    """Step 5: win our row as abandoned, then delete only our own Secret by its UID.

    ``uid`` is what our create returned, so the precondition can match no other
    Secret. That holds even when a reclaim abandoned the row first and a sweep
    already recorded it absent: the create landed after that listing.
    """

    await _abandon(custody, staged.id, uid=uid)
    row = await _row(custody, staged.id)
    if row is not None and row.state == "abandoned":
        await _remove(custody, staged.id, staged.namespace, staged.name, uid)
    return EnrollmentRefusal(code)


async def _lock_custody(
    session: AsyncSession, *, enrollment_id: uuid.UUID, row_id: uuid.UUID | None
) -> list[Any]:
    clauses = [and_(C.enrollment_id == enrollment_id, C.state == "published")]
    if row_id is not None:
        clauses.append(C.id == row_id)
    return list(
        (
            await session.execute(
                select(C.id, C.state, C.secret_uid)
                .where(or_(*clauses))
                .order_by(C.id)
                .with_for_update()
            )
        ).all()
    )


async def _retire_published(session: AsyncSession, enrollment_id: uuid.UUID) -> list[uuid.UUID]:
    return list(
        await session.scalars(
            update(C)
            .where(C.enrollment_id == enrollment_id, C.state == "published")
            .values(state="retiring", version=C.version + 1)
            .returning(C.id)
        )
    )


async def _publish(
    custody: Custody,
    staged: _Staged,
    uid: str,
    *,
    tenant_id: uuid.UUID,
    principal_id: uuid.UUID,
    intent: Intent,
    expected_version: int | None,
) -> list[uuid.UUID] | RefusalCode:
    """Tx2. The rows it marked retiring, or why it lost (nothing written)."""

    async with custody.sessionmaker() as session:
        try:
            await _set_lock_timeout(session)
            header = await _lock_owner(session, tenant_id, principal_id)
            locked = await _lock_custody(
                session, enrollment_id=staged.enrollment_id, row_id=staged.id
            )
            ours = next((row for row in locked if row.id == staged.id), None)
            if ours is None or ours.state != "staged" or ours.secret_uid != uid:
                await session.rollback()
                return "staging_conflict"
            refused = _intent_refusal(header, intent, expected_version)
            if refused is not None:
                await session.rollback()
                return refused
            if header is None:
                created = await session.scalar(
                    insert(E)
                    .values(
                        id=staged.enrollment_id,
                        tenant_id=tenant_id,
                        principal_id=principal_id,
                        generation=staged.generation,
                        active=True,
                        version=1,
                    )
                    .on_conflict_do_nothing()
                    .returning(E.id)
                )
                if created is None:
                    await session.rollback()
                    return "enrollment_exists"
            else:
                if (header.id, header.generation + 1) != (staged.enrollment_id, staged.generation):
                    await session.rollback()
                    return "stale_version"
                await session.execute(
                    update(E)
                    .where(E.id == header.id, E.version == header.version)
                    .values(
                        generation=staged.generation,
                        version=E.version + 1,
                        active=True,
                        updated_at=func.clock_timestamp(),
                    )
                )
            retiring = await _retire_published(session, staged.enrollment_id)
            await session.execute(
                update(C)
                .where(C.id == staged.id, C.state == "staged", C.secret_uid == uid)
                .values(
                    state="published", published_at=func.clock_timestamp(), version=C.version + 1
                )
            )
            await lifecycle.fence_enrollment_generation(
                session, tenant_id=tenant_id, owner_principal_id=principal_id
            )
            await session.commit()
        except DBAPIError as exc:
            await session.rollback()
            if _busy(exc):
                return "enrollment_busy"
            raise
    return retiring


async def write(
    custody: Custody,
    *,
    tenant_id: uuid.UUID,
    principal_id: uuid.UUID,
    intent: Intent,
    expected_version: int | None,
    token: str,
) -> EnrollmentStatus | EnrollmentRefusal:
    """Enroll, replace or reconnect: stage, create, record, publish, retire."""

    staged, reclaimed = await _stage(
        custody,
        tenant_id=tenant_id,
        principal_id=principal_id,
        intent=intent,
        expected_version=expected_version,
    )
    for row_id in reclaimed:
        await _clean_row(custody, row_id)
    if isinstance(staged, EnrollmentRefusal):
        return staged
    created = await _create(custody, staged, token)
    if isinstance(created, EnrollmentRefusal):
        return created
    if not await _record_uid(custody, staged.id, created):
        return await _lose(custody, staged, created, "staging_conflict")
    published = await _publish(
        custody,
        staged,
        created,
        tenant_id=tenant_id,
        principal_id=principal_id,
        intent=intent,
        expected_version=expected_version,
    )
    if isinstance(published, str):
        return await _lose(custody, staged, created, published)
    for row_id in published:
        await _clean_row(custody, row_id)
    return await status(custody.sessionmaker, tenant_id=tenant_id, principal_id=principal_id)


async def disconnect(
    custody: Custody,
    *,
    tenant_id: uuid.UUID,
    principal_id: uuid.UUID,
    expected_version: int,
) -> EnrollmentStatus | EnrollmentRefusal:
    """Set the enrollment inactive at a new generation, fence, then retire its Secret.

    Inactive at the expected version is the idempotent answer: nothing changes.
    """

    retiring: list[uuid.UUID] = []
    async with custody.sessionmaker() as session:
        try:
            await _set_lock_timeout(session)
            header = await _lock_owner(session, tenant_id, principal_id)
            if header is None:
                await session.rollback()
                return EnrollmentRefusal("enrollment_absent")
            if header.version != expected_version:
                await session.rollback()
                return EnrollmentRefusal("stale_version")
            if header.active:
                await _lock_custody(session, enrollment_id=header.id, row_id=None)
                await session.execute(
                    update(E)
                    .where(E.id == header.id, E.version == header.version)
                    .values(
                        generation=E.generation + 1,
                        version=E.version + 1,
                        active=False,
                        updated_at=func.clock_timestamp(),
                    )
                )
                retiring = await _retire_published(session, header.id)
                await lifecycle.fence_enrollment_generation(
                    session, tenant_id=tenant_id, owner_principal_id=principal_id
                )
            await session.commit()
        except DBAPIError as exc:
            await session.rollback()
            if _busy(exc):
                return EnrollmentRefusal("enrollment_busy")
            raise
    for row_id in retiring:
        await _clean_row(custody, row_id)
    return await status(custody.sessionmaker, tenant_id=tenant_id, principal_id=principal_id)


# --- the reconciler's passes (R10) -------------------------------------------------------


def _orphaned(row: Any, secret: StoredSecret) -> bool:
    """Whether pass 4 may delete ``secret``, given the row its annotation names."""

    if row is None:
        return True
    if row.state in ("abandoned", "retired"):
        return True
    if row.state in ("published", "retiring"):
        return bool(row.secret_uid != secret.uid)
    # A staged row belongs to its writer, or to pass 1 once its lease lapses.
    return False


async def _ids(custody: Custody, statement: Any) -> list[uuid.UUID]:
    async with custody.sessionmaker() as session:
        found = list(await session.scalars(statement))
        await session.commit()
    return found


async def _sweep(custody: Custody, unrecorded: Sequence[uuid.UUID], result: CustodyPass) -> None:
    """Pass 4, then pass 3's bookkeeping from the same listing."""

    try:
        listed = await _store_call(custody.store.list, custody.namespace)
    except CustodyStoreError as exc:
        logger.warning("personal custody sweep: listing failed (HTTP %s)", exc.status)
        return
    annotated = [
        (secret, secret.custody_id)
        for secret in listed
        if secret.labels.get(CUSTODY_LABEL) == "true" and secret.custody_id is not None
    ]
    rows: dict[uuid.UUID, Any] = {}
    if annotated:
        async with custody.sessionmaker() as session:
            for row in (
                await session.execute(
                    select(C.id, C.state, C.secret_uid).where(
                        C.id.in_([custody_id for _, custody_id in annotated])
                    )
                )
            ).all():
                rows[row.id] = row
    remaining: set[uuid.UUID] = set()
    for secret, custody_id in annotated:
        assert custody_id is not None
        if not _orphaned(rows.get(custody_id), secret):
            remaining.add(custody_id)
            continue
        try:
            outcome = await _delete(custody, custody.namespace, secret.name, secret.uid)
        except CustodyStoreError as exc:
            logger.warning(
                "personal custody %s: orphan removal deferred (HTTP %s)", custody_id, exc.status
            )
            remaining.add(custody_id)
            continue
        if outcome == "uid_mismatch":
            remaining.add(custody_id)
        elif outcome == "deleted":
            result.swept += 1
    for row_id in unrecorded:
        if row_id not in remaining and await _mark_absent(custody, row_id, unrecorded=True):
            result.cleared += 1


async def reconcile_custody(custody: Custody) -> CustodyPass:
    """One pass of the four ownership-checked cleanup passes (R10)."""

    result = CustodyPass()
    # 1. Reclaim stale staging: the database decides first, then pass 2 deletes.
    result.reclaimed = len(
        await _ids(
            custody,
            update(C)
            .where(C.state == "staged", C.lease_expires_at < func.clock_timestamp())
            .values(state="abandoned", version=C.version + 1)
            .returning(C.id),
        )
    )
    # 2. Abandoned or retiring rows with a recorded UID and no recorded absence.
    for row_id in await _ids(
        custody,
        select(C.id)
        .where(
            C.state.in_(("abandoned", "retiring")),
            C.secret_absent_at.is_(None),
            C.secret_uid.is_not(None),
        )
        .order_by(C.staged_at)
        .limit(_SWEEP_BATCH),
    ):
        if await _clean_row(custody, row_id):
            result.cleared += 1
    # 3. Abandoned rows without a UID are never deleted by name; their candidates
    # are read before the listing so a later abandon waits for the next pass.
    unrecorded = await _ids(
        custody,
        select(C.id).where(
            C.state == "abandoned",
            C.secret_uid.is_(None),
            C.secret_absent_at.is_(None),
            C.secret_namespace == custody.namespace,
        ),
    )
    # 4. The orphan sweep lists the namespace on every pass.
    await _sweep(custody, unrecorded, result)
    return result
