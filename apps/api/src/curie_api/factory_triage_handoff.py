"""Hand a published triage ticket to implementation, once, on the requester's authority (#5588).

ADR 0277 decision 5: one triage label authorizes one investigation and, when
it publishes a ticket, one implementation run on that ticket, started with no
second human action after a fresh permission check.

Delegation (`record_delegation`). Admitting a tracker-result request whose
policy allows a ticket stores one `FactoryTriageDelegation` in the admission
transaction: the verified labeller by immutable id, the selecting label event,
the source issue, request, repository and installation, the source
fingerprint, the result policy digest and the target. The target is the one
implementation binding the declared label resolves to on the source
repository, frozen (binding id, generation, agent); a label that resolves to
none, several, or a tracker-result binding stores the delegation without one,
and its handoff is refused ``target_unavailable``. Authority is never rebuilt
from issue text or a requester string.

Internal selection event (`open_handoff`). The transaction that records a
verified ticket receipt writes one `FactoryTriageHandoff` whose id
(`selection_event_id`) derives from the source request and the output issue
identity. No label event is fabricated. A source admitted before this change
has no delegation, so its ticket renders as published, as before.

Consumption (`hand_off`, run by the WorkItem reconciler). Every check below is
read again on every attempt, and all of them pass before any label is
written (`github_factory.check_delegated`):

1. The source WorkItem and request are not cancelled: ``source_cancelled``.
2. The repository is allowlisted and the installation still covers it:
   ``scope_lost``.
3. The output issue is open and its body digest equals the delivered one:
   ``output_changed``.
4. The labeller, by immutable id, may still start work (`Tracker.may_start`):
   ``authority_revoked``. A read that fails is retried until the handoff
   deadline (the receipt time plus the tracker result delivery deadline) and
   then refused ``permission_unreadable``; it is never approval.
5. The frozen target binding still exists with the same identity and
   generation, still serves the label, and has the implementation contract:
   ``target_unavailable``.
6. Ordinary admission's ownership, label conflict and rediscovery rules:
   their own codes.

Writes, in order. (1) ``associated_at`` commits the output association, which
the label intake reads (`delegated_output_label`) so the factory's own label
on its output is never answered with an ``actor_bot`` notice. (2) The
implementation label is added through the tracker port and read back, then
``labelled_at`` commits; the output keeps its ``base:`` labels and never gets
the triage label. (3) Under the output issue lock, the shared admission
service admits a separate WorkItem with the request id `implementation_request_id`
derives from the selection event, and the delegation's consumption and the
outcome commit in that same transaction. A crash anywhere replays into the
same request. An admission refused after (2) removes the label again and
records the refusal.

Outcomes. ``admitted`` with the implementation request id, or ``refused``
with the reason. Either re-renders the source's status comment (endings 1 and
2 of `curie_internal.factory_triage`); the Slack triage line waits for it.
"""

from __future__ import annotations

import logging
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, replace
from datetime import datetime, timedelta

import httpx
from sqlalchemy import ColumnElement, func, or_, select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from .config import Settings
from .factory_tracker_result_runtime import (
    CONTRACT_TRACKER_RESULT,
    CompletionContract,
    select_completion_contract,
    work_item_tracker,
)
from .factory_triage_status import on_tracker_result_settled
from .forges.errors import ForgeError
from .forges.registry import ForgeRegistry
from .forges.types import Actor, TrackerIssueRef
from .models import (
    ExecutionRequest,
    FactoryTriageDelegation,
    FactoryTriageHandoff,
    WorkItem,
)
from .tracker_result_declaration import ResultKind
from .workitems.lifecycle import WorkItemOutcome

logger = logging.getLogger(__name__)

# The identity namespace of internal triage selections. Fixed forever: the
# selection event and implementation request ids derive from it.
SELECTION_NAMESPACE = uuid.UUID("6a1f3c52-9d4e-5b0a-8c71-55880a1d0f0e")
# How far the database clock may run ahead of the tracker's event clock when
# the intake compares a label event with the recorded association.
CLOCK_SKEW = timedelta(minutes=5)
LEASE_SECONDS = 120

SOURCE_CANCELLED = "source_cancelled"
TARGET_UNAVAILABLE = "target_unavailable"
SCOPE_LOST = "scope_lost"
PERMISSION_UNREADABLE = "permission_unreadable"


class HandoffRefused(Exception):
    """A check failed: the handoff ends refused with ``code``."""

    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(f"triage handoff refused: {code}")


class HandoffDeferred(Exception):
    """A read failed: the handoff is retried until its deadline."""

    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(f"triage handoff deferred: {code}")


@dataclass(frozen=True)
class ImplementationTarget:
    """The one implementation binding a delegation freezes."""

    binding_id: uuid.UUID
    generation: int
    agent_id: uuid.UUID


@dataclass(frozen=True)
class DelegatedOutput:
    """What one handoff attempt admits: the output, the target and the authority."""

    repo_full_name: str
    repository_id: int
    issue_number: int
    installation_id: int
    label: str
    binding_id: uuid.UUID
    binding_generation: int
    agent_id: uuid.UUID
    actor: Actor
    requester: str
    request_id: uuid.UUID
    body_sha256: str
    labelled_at: datetime | None


def selection_event_id(source_request_id: uuid.UUID, output: TrackerIssueRef) -> uuid.UUID:
    """The internal selection event's identity: the source request and the output issue."""

    identity = (
        f"triage-selection:v1:{source_request_id}:"
        f"{output.kind}:{output.host}:{output.scope_id}:{output.issue_id}"
    )
    return uuid.uuid5(SELECTION_NAMESPACE, identity)


def implementation_request_id(handoff_id: uuid.UUID) -> uuid.UUID:
    """The implementation request a selection event admits; replay finds the same one."""

    return uuid.uuid5(SELECTION_NAMESPACE, f"triage-implementation:v1:{handoff_id}")


# Delegation -----------------------------------------------------------------


async def record_delegation(
    session: AsyncSession,
    *,
    work_item_id: uuid.UUID,
    request_id: uuid.UUID,
    actor_id: str,
    actor_login: str,
    selection_event: str,
    installation_id: int | None,
    registry: ForgeRegistry,
    target: Callable[[str], Awaitable[ImplementationTarget | None]],
) -> bool:
    """Store the delegation of one admitted triage selection; True when one exists.

    Nothing for a request whose contract is implementation or whose policy
    allows findings only. Idempotent per source request. Runs in the caller's
    admission transaction; the request row may not exist yet when the
    selection readmits a WorkItem whose earlier run is still stopping.
    """

    from .factory_tracker_result_delivery import source_fingerprint

    work_item = await session.get(WorkItem, work_item_id)
    if work_item is None:
        return False
    request = await session.get(ExecutionRequest, request_id)
    if request is not None and request.completion_contract != CONTRACT_TRACKER_RESULT:
        return False
    contract = await select_completion_contract(
        session,
        agent_id=work_item.agent_id,
        tracker_kind=work_item.tracker_kind,
        tracker_host=work_item.tracker_host,
        registry=registry,
    )
    if (
        not isinstance(contract, CompletionContract)
        or not contract.is_tracker_result
        or ResultKind.TICKET.value not in contract.result_kinds
        or contract.implementation_label is None
        or contract.result_policy_digest is None
    ):
        return False
    resolved = await target(contract.implementation_label)
    source = work_item.tracker_issue
    await session.execute(
        insert(FactoryTriageDelegation)
        .values(
            id=uuid.uuid4(),
            work_item_id=work_item.id,
            source_execution_request_id=request_id,
            actor_id=actor_id,
            actor_login=actor_login,
            selection_event=selection_event,
            tracker_kind=source.kind,
            tracker_host=source.host,
            tracker_scope_id=source.scope_id,
            tracker_issue_id=source.issue_id,
            repository_id=work_item.repository_project_id,
            installation_id=installation_id,
            source_fingerprint=(
                source_fingerprint(work_item, request) if request is not None else None
            ),
            result_policy_digest=(
                request.result_policy_digest
                if request is not None and request.result_policy_digest is not None
                else contract.result_policy_digest
            ),
            target_label=contract.implementation_label,
            target_binding_id=resolved.binding_id if resolved else None,
            target_binding_generation=resolved.generation if resolved else None,
            target_agent_id=resolved.agent_id if resolved else None,
        )
        .on_conflict_do_nothing(index_elements=["source_execution_request_id"])
    )
    logger.info(
        "triage delegation recorded request=%s target=%s",
        request_id,
        "resolved" if resolved else "unavailable",
    )
    return True


# Internal selection event ---------------------------------------------------


async def open_handoff(
    session: AsyncSession,
    *,
    execution_request_id: uuid.UUID,
    output: TrackerIssueRef,
    output_url: str,
    output_body_sha256: str,
    settings: Settings,
) -> uuid.UUID | None:
    """Write the internal selection event of a verified ticket receipt, in its transaction.

    None when the request holds no unconsumed delegation (a source admitted
    before #5588, or an implementation request). Replaying it writes nothing.
    """

    delegation_id = await session.scalar(
        select(FactoryTriageDelegation.id).where(
            FactoryTriageDelegation.source_execution_request_id == execution_request_id,
            FactoryTriageDelegation.consumed_at.is_(None),
        )
    )
    if delegation_id is None:
        return None
    handoff_id = selection_event_id(execution_request_id, output)
    now = func.clock_timestamp()
    await session.execute(
        insert(FactoryTriageHandoff)
        .values(
            id=handoff_id,
            delegation_id=delegation_id,
            source_execution_request_id=execution_request_id,
            output_tracker_kind=output.kind,
            output_tracker_host=output.host,
            output_scope_id=output.scope_id,
            output_issue_id=output.issue_id,
            output_url=output_url,
            output_body_sha256=output_body_sha256,
            next_attempt_at=now,
            deadline_at=now
            + timedelta(seconds=settings.factory_tracker_result_delivery_deadline_seconds),
        )
        .on_conflict_do_nothing()
    )
    return handoff_id


async def delegated_output_label(
    session: AsyncSession, issue: TrackerIssueRef, *, label: str, event_at: datetime | None
) -> bool:
    """Whether ``issue`` is a recorded delegated output and ``label`` is the one its
    handoff applies, applied no earlier than the recorded association."""

    row = (
        await session.execute(
            select(FactoryTriageHandoff.associated_at, FactoryTriageDelegation.target_label)
            .join(
                FactoryTriageDelegation,
                FactoryTriageDelegation.id == FactoryTriageHandoff.delegation_id,
            )
            .where(
                FactoryTriageHandoff.output_tracker_kind == issue.kind,
                FactoryTriageHandoff.output_tracker_host == issue.host,
                FactoryTriageHandoff.output_scope_id == issue.scope_id,
                FactoryTriageHandoff.output_issue_id == issue.issue_id,
                FactoryTriageHandoff.associated_at.is_not(None),
            )
        )
    ).one_or_none()
    if row is None:
        return False
    associated_at, target_label = row
    if target_label != label:
        return False
    return event_at is None or event_at >= associated_at - CLOCK_SKEW


# Consumption ----------------------------------------------------------------


@dataclass(frozen=True)
class _Loaded:
    handoff: FactoryTriageHandoff
    delegation: FactoryTriageDelegation
    work_item: WorkItem
    request: ExecutionRequest | None


async def _load(session: AsyncSession, handoff_id: uuid.UUID) -> _Loaded | None:
    row = (
        await session.execute(
            select(FactoryTriageHandoff, FactoryTriageDelegation, WorkItem)
            .join(
                FactoryTriageDelegation,
                FactoryTriageDelegation.id == FactoryTriageHandoff.delegation_id,
            )
            .join(WorkItem, WorkItem.id == FactoryTriageDelegation.work_item_id)
            .where(FactoryTriageHandoff.id == handoff_id)
            .execution_options(populate_existing=True)
        )
    ).one_or_none()
    if row is None:
        return None
    handoff, delegation, work_item = row
    request = await session.get(
        ExecutionRequest, handoff.source_execution_request_id, populate_existing=True
    )
    return _Loaded(handoff, delegation, work_item, request)


def _source_cancelled(loaded: _Loaded) -> bool:
    request = loaded.request
    return (
        loaded.delegation.consumed_at is not None
        or loaded.work_item.cancelled_at is not None
        or request is None
        or request.status == "cancelled"
    )


def _output(loaded: _Loaded) -> DelegatedOutput:
    delegation, handoff = loaded.delegation, loaded.handoff
    if (
        delegation.target_binding_id is None
        or delegation.target_binding_generation is None
        or delegation.target_agent_id is None
    ):
        raise HandoffRefused(TARGET_UNAVAILABLE)
    if delegation.installation_id is None:
        raise HandoffRefused(SCOPE_LOST)
    return DelegatedOutput(
        repo_full_name=loaded.work_item.repository_path,
        repository_id=int(delegation.repository_id),
        issue_number=int(handoff.output_issue_id),
        installation_id=delegation.installation_id,
        label=delegation.target_label,
        binding_id=delegation.target_binding_id,
        binding_generation=delegation.target_binding_generation,
        agent_id=delegation.target_agent_id,
        actor=Actor(delegation.actor_id, delegation.actor_login),
        requester=f"github:{delegation.actor_id}:{delegation.actor_login}",
        request_id=implementation_request_id(handoff.id),
        body_sha256=handoff.output_body_sha256,
        labelled_at=handoff.labelled_at,
    )


async def _claim(
    sessionmaker: async_sessionmaker[AsyncSession], handoff_id: uuid.UUID, owner: str
) -> bool:
    async with sessionmaker() as session:
        claimed = await session.scalar(
            update(FactoryTriageHandoff)
            .where(
                FactoryTriageHandoff.id == handoff_id,
                FactoryTriageHandoff.outcome.is_(None),
                FactoryTriageHandoff.next_attempt_at <= func.clock_timestamp(),
                or_(
                    FactoryTriageHandoff.lease_owner.is_(None),
                    FactoryTriageHandoff.lease_expires_at <= func.clock_timestamp(),
                ),
            )
            .values(
                lease_owner=owner,
                lease_expires_at=func.clock_timestamp() + timedelta(seconds=LEASE_SECONDS),
                attempts=FactoryTriageHandoff.attempts + 1,
            )
            .returning(FactoryTriageHandoff.id)
        )
        await session.commit()
    return claimed is not None


def _held(handoff_id: uuid.UUID, owner: str) -> tuple[ColumnElement[bool], ...]:
    return (
        FactoryTriageHandoff.id == handoff_id,
        FactoryTriageHandoff.outcome.is_(None),
        FactoryTriageHandoff.lease_owner == owner,
    )


async def _mark(
    sessionmaker: async_sessionmaker[AsyncSession],
    handoff_id: uuid.UUID,
    owner: str,
    column: str,
) -> datetime | None:
    """Stamp ``associated_at`` or ``labelled_at`` once, under the lease; its value."""

    target = getattr(FactoryTriageHandoff, column)
    async with sessionmaker() as session:
        await session.execute(
            update(FactoryTriageHandoff)
            .where(*_held(handoff_id, owner), target.is_(None))
            .values({column: func.clock_timestamp()})
        )
        stamped: datetime | None = await session.scalar(
            select(target).where(FactoryTriageHandoff.id == handoff_id)
        )
        await session.commit()
    return stamped


async def _settle(
    session: AsyncSession,
    loaded: _Loaded,
    owner: str,
    *,
    outcome: str,
    reason: str | None = None,
    request_id: uuid.UUID | None = None,
) -> bool:
    """Consume the delegation and record the outcome in ``session``; the caller commits.

    False when another attempt already settled it or the lease was lost.
    """

    handoff_id = loaded.handoff.id
    settled = await session.scalar(
        update(FactoryTriageHandoff)
        .where(*_held(handoff_id, owner))
        .values(
            outcome=outcome,
            reason=reason,
            implementation_request_id=request_id,
            settled_at=func.clock_timestamp(),
            lease_owner=None,
            lease_expires_at=None,
            last_error=None,
        )
        .returning(FactoryTriageHandoff.id)
    )
    if settled is None:
        return False
    await session.execute(
        update(FactoryTriageDelegation)
        .where(
            FactoryTriageDelegation.id == loaded.delegation.id,
            FactoryTriageDelegation.consumed_at.is_(None),
        )
        .values(consumed_at=func.clock_timestamp())
    )
    # The source status comment renders the handoff's ending on its next sync.
    await on_tracker_result_settled(session, loaded.handoff.source_execution_request_id)
    return True


async def _refuse(
    sessionmaker: async_sessionmaker[AsyncSession], loaded: _Loaded, owner: str, reason: str
) -> str | None:
    async with sessionmaker() as session:
        settled = await _settle(session, loaded, owner, outcome="refused", reason=reason)
        await session.commit()
    if not settled:
        return None
    logger.info("triage handoff refused handoff=%s reason=%s", loaded.handoff.id, reason)
    return "refused"


async def _defer(
    sessionmaker: async_sessionmaker[AsyncSession],
    handoff_id: uuid.UUID,
    owner: str,
    code: str,
    settings: Settings,
    attempts: int,
) -> None:
    delay = min(
        settings.work_item_backoff_base_seconds * int(2 ** max(attempts - 1, 0)),
        settings.work_item_backoff_max_seconds,
    )
    async with sessionmaker() as session:
        await session.execute(
            update(FactoryTriageHandoff)
            .where(*_held(handoff_id, owner))
            .values(
                last_error=code,
                next_attempt_at=func.clock_timestamp() + timedelta(seconds=delay),
                lease_owner=None,
                lease_expires_at=None,
            )
        )
        await session.commit()
    logger.info("triage handoff deferred handoff=%s code=%s", handoff_id, code)


async def _remove_label(
    settings: Settings, client: httpx.AsyncClient, loaded: _Loaded, label: str
) -> None:
    """Take the handoff's label off the output again; `HandoffDeferred` when it stays."""

    try:
        tracker = work_item_tracker(settings, client, loaded.work_item)
        output = loaded.handoff.output_issue
        await tracker.set_state_label(output, add=None, remove=(label,))
        if label in (await tracker.ticket_labels(output)).labels:
            raise HandoffDeferred("label_removal_unconfirmed")
    except ForgeError:
        raise HandoffDeferred("label_removal_unavailable") from None


async def _add_label(
    settings: Settings, client: httpx.AsyncClient, loaded: _Loaded, label: str
) -> None:
    """Add the implementation label and read it back; `HandoffDeferred` when unconfirmed."""

    try:
        tracker = work_item_tracker(settings, client, loaded.work_item)
        output = loaded.handoff.output_issue
        await tracker.set_state_label(output, add=label, remove=())
        if label not in (await tracker.ticket_labels(output)).labels:
            raise HandoffDeferred("label_write_unconfirmed")
    except ForgeError:
        raise HandoffDeferred("label_write_unavailable") from None


async def _admit(
    sessionmaker: async_sessionmaker[AsyncSession],
    loaded: _Loaded,
    output: DelegatedOutput,
    owner: str,
    *,
    settings: Settings,
    client: httpx.AsyncClient,
) -> str | None:
    """Admission, delegation consumption and the outcome, in one transaction."""

    from . import github_factory

    async with sessionmaker() as session:
        try:
            selected = await github_factory.admit_delegated(session, settings, client, output)
            current = await _load(session, loaded.handoff.id)
            if current is None or _source_cancelled(current):
                raise HandoffRefused(SOURCE_CANCELLED)
        except BaseException:
            await session.rollback()
            raise
        if not isinstance(selected.result, WorkItemOutcome):
            await session.rollback()
            code = (
                selected.result.code
                if selected.result is not None
                else (selected.outcome.errors or [{"code": "not_admitted"}])[0]["code"]
            )
            raise HandoffRefused(str(code))
        settled = await _settle(
            session, current, owner, outcome="admitted", request_id=output.request_id
        )
        if not settled:
            await session.rollback()
            return None
        await session.commit()
    logger.info(
        "triage handoff admitted handoff=%s request=%s", loaded.handoff.id, output.request_id
    )
    return "admitted"


async def hand_off(
    sessionmaker: async_sessionmaker[AsyncSession],
    handoff_id: uuid.UUID,
    *,
    settings: Settings,
    client: httpx.AsyncClient,
    owner: str,
) -> str | None:
    """One attempt at one handoff: ``admitted``, ``refused``, or None (deferred, held
    by another attempt, or already settled)."""

    from . import github_factory

    if not await _claim(sessionmaker, handoff_id, owner):
        return None
    async with sessionmaker() as session:
        loaded = await _load(session, handoff_id)
        now = await session.scalar(select(func.clock_timestamp()))
        # Detach the rows first: a rollback expires every attached instance,
        # and this attempt reads them after the read transaction ends.
        session.expunge_all()
        await session.rollback()
    if loaded is None:
        return None
    label = loaded.delegation.target_label
    labelled = loaded.handoff.labelled_at is not None
    # Once associated, the label may have been written even if ``labelled_at``
    # was not: every refusal from then on takes it off again.
    associated = loaded.handoff.associated_at is not None
    try:
        if _source_cancelled(loaded):
            raise HandoffRefused(SOURCE_CANCELLED)
        output = _output(loaded)
        if not labelled:
            async with sessionmaker() as session:
                try:
                    await github_factory.check_delegated(
                        session, settings, client, output, labelled=False
                    )
                finally:
                    await session.rollback()
            if await _mark(sessionmaker, handoff_id, owner, "associated_at") is None:
                return None
            associated = True
            await _add_label(settings, client, loaded, label)
            labelled_at = await _mark(sessionmaker, handoff_id, owner, "labelled_at")
            if labelled_at is None:
                return None
            labelled = True
            output = replace(output, labelled_at=labelled_at)
        return await _admit(sessionmaker, loaded, output, owner, settings=settings, client=client)
    except HandoffRefused as refused:
        if associated:
            try:
                await _remove_label(settings, client, loaded, label)
            except HandoffDeferred as deferred:
                await _defer(
                    sessionmaker,
                    handoff_id,
                    owner,
                    deferred.code,
                    settings,
                    loaded.handoff.attempts + 1,
                )
                return None
        return await _refuse(sessionmaker, loaded, owner, refused.code)
    except HandoffDeferred as deferred:
        if now is not None and now >= loaded.handoff.deadline_at:
            if associated:
                try:
                    await _remove_label(settings, client, loaded, label)
                except HandoffDeferred:
                    pass
            return await _refuse(sessionmaker, loaded, owner, deferred.code)
        await _defer(
            sessionmaker, handoff_id, owner, deferred.code, settings, loaded.handoff.attempts + 1
        )
        return None


async def due_handoffs(session: AsyncSession, *, limit: int = 50) -> list[uuid.UUID]:
    """Unsettled handoffs whose backoff has passed and whose lease is free or lapsed."""

    ids = await session.scalars(
        select(FactoryTriageHandoff.id)
        .where(
            FactoryTriageHandoff.outcome.is_(None),
            FactoryTriageHandoff.next_attempt_at <= func.clock_timestamp(),
            or_(
                FactoryTriageHandoff.lease_owner.is_(None),
                FactoryTriageHandoff.lease_expires_at <= func.clock_timestamp(),
            ),
        )
        .order_by(FactoryTriageHandoff.next_attempt_at)
        .limit(limit)
    )
    return list(ids)


__all__ = [
    "DelegatedOutput",
    "HandoffDeferred",
    "HandoffRefused",
    "ImplementationTarget",
    "delegated_output_label",
    "due_handoffs",
    "hand_off",
    "implementation_request_id",
    "open_handoff",
    "record_delegation",
    "selection_event_id",
]
