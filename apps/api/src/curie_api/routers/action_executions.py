"""The capability probe route and the worker's fenced execution routes.

@spec ACTION-EXECUTOR-1: the probe route is the third and last producer of
executions, and it can only produce a ``tools/list``: its body is exactly
``{agent_id, connector, digest}`` and a probe can never commit ``dispatched``.
@spec ACTION-EXECUTOR-18: the worker claims and reports through
``POST /action-executions/claim``, ``.../{id}/observation``,
``.../{id}/dispatch``, ``.../{id}/outcome`` and ``GET /action-executions/{id}``.
Each transition presents the fence the claim returned, is idempotent for the
same fence and payload, and answers ``409`` for a conflicting one.

@spec ACTION-EXECUTOR-15 and the ACTION-EXECUTOR-20 amendment: the worker posts
the version ``observe_version`` reported and the API, as ledger owner, compares
it with the recorded ``post_version``. A difference, or an absent or malformed
version, ends the execution ``refused`` with ``version_conflict`` and writes the
``refused_conflict`` audit row naming both versions, in the same transaction,
so no ``dispatched`` commit (and so no write call) can follow it.

Every route uses the platform key the worker already holds
(``require_platform_key``), never a console session. No response, audit row or
refusal body carries an envelope, a state, an argument or a result.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Response, status
from sqlalchemy import func, or_, select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from ..action_execution_codes import (
    EXHAUSTED_CLAIM_CODE,
    EXPIRED_DISPATCH_CODE,
    CodeRejected,
    outcome_code,
)
from ..auth import require_internal_worker_token, require_platform_key
from ..config import get_settings
from ..deps import SessionDep
from ..models import (
    ActionAuditEntry,
    ActionExecution,
    Agent,
    AgentAction,
    ConnectorCapability,
    ExecutionKind,
    ExecutionState,
)
from ..schemas.action_executions import (
    ExecutionClaim,
    ExecutionCreated,
    ExecutionFence,
    ExecutionObservation,
    ExecutionOut,
    ExecutionOutcome,
    ProbeCreate,
)

# Route decisions: the writing routes take the internal worker token, never
# the platform or operator key, so a key holder cannot forge a confirmed
# restore or a ``restore_capable`` row. The receipt stays on the platform key
# for the operator CLI (ACTION-EXECUTOR-23).
probe_router = APIRouter(
    prefix="/connector-capabilities",
    tags=["action-executions"],
    dependencies=[Depends(require_internal_worker_token)],
)
router = APIRouter(
    prefix="/action-executions",
    tags=["action-executions"],
    dependencies=[Depends(require_internal_worker_token)],
)
receipt_router = APIRouter(
    prefix="/action-executions",
    tags=["action-executions"],
    dependencies=[Depends(require_platform_key)],
)

# @spec ACTION-EXECUTOR-17: lease expiry in ``claimed`` returns the row for
# another attempt "at most three times", then refuses it.
MAX_ATTEMPTS = 3

# @spec ACTION-EXECUTOR-13 @spec ACTION-EXECUTOR-8: the verb pair a probe must
# observe for its digest to be restore capable. A lone ``restore`` is not.
RESTORE_PAIR = frozenset({"restore", "observe_version"})

# A sealed version is "a non-empty string of at most 256 characters"
# (ACTION-EXECUTOR-9); anything else observed is malformed, so a conflict.
_MAX_VERSION = 256

_TERMINAL = frozenset(
    {
        ExecutionState.confirmed,
        ExecutionState.failed,
        ExecutionState.indeterminate,
        ExecutionState.refused,
    }
)

# The audit actor and authorizer of a row the executor writes, when the
# execution names no requester.
_EXECUTOR = "action-executor"


def _out(execution: ActionExecution) -> ExecutionOut:
    return ExecutionOut.model_validate(execution)


async def _now(session: AsyncSession) -> datetime:
    """The database clock, so leases compare against one clock only."""

    value = await session.scalar(select(func.clock_timestamp()))
    assert isinstance(value, datetime)
    return value


async def _locked(session: AsyncSession, execution_id: uuid.UUID) -> ActionExecution:
    execution = await session.scalar(
        select(ActionExecution)
        .where(ActionExecution.id == execution_id)
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    if execution is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "action execution not found")
    return execution


def _conflict(reason: str) -> HTTPException:
    return HTTPException(status.HTTP_409_CONFLICT, reason)


def _check_fence(execution: ActionExecution, fence: ExecutionFence, now: datetime) -> None:
    """@spec ACTION-EXECUTOR-17: a stale owner, attempt or lease moves nothing.

    A terminal row keeps its last fence so the holder's replay of the same
    report is answered; only a live row also needs a live lease.
    """

    if execution.lease_owner != fence.lease_owner or execution.attempt != fence.attempt:
        raise _conflict("the fence does not hold this execution")
    if execution.state not in _TERMINAL and (
        execution.lease_expires_at is None or execution.lease_expires_at <= now
    ):
        raise _conflict("the lease on this execution has expired")


def _audit(
    execution: ActionExecution,
    *,
    kind: str,
    authorized: bool,
    reason: str,
    evidence: dict[str, Any],
) -> ActionAuditEntry:
    """An audit row on a restore's subject action. Codes and versions only."""

    assert execution.subject_action_id is not None
    return ActionAuditEntry(
        action_id=execution.subject_action_id,
        action=kind,
        actor=execution.requested_by or _EXECUTOR,
        actor_channel=None,
        authorizer=_EXECUTOR,
        authorized=authorized,
        reason=reason,
        evidence={"execution_id": str(execution.id), **evidence},
        created_at=func.clock_timestamp(),
    )


_CLOSING_REASONS = {
    ExecutionState.confirmed: "the connector confirmed the restore",
    ExecutionState.failed: "the restore failed",
    ExecutionState.indeterminate: "the restore may or may not have been applied",
    ExecutionState.refused: "the restore was refused before any write",
}


def _finish(
    session: AsyncSession,
    execution: ActionExecution,
    state: ExecutionState,
    code: str | None,
    now: datetime,
    *,
    versions: dict[str, str | None] | None = None,
) -> None:
    """End ``execution`` in ``state`` and write its one closing audit row.

    @spec ACTION-EXECUTOR-18 and the route decisions: every terminal restore
    outcome writes one closing row on its action naming its state and code,
    with versions only (the conflict refusal names both). A confirmed restore's
    row is ``confirmed``; a version conflict's is ``refused_conflict``.
    ``undone_at`` is written by ``_confirm_restore`` only, never here.
    """

    execution.state = state
    execution.finished_at = now
    if state == ExecutionState.refused:
        execution.refusal_code = code
    elif code is not None:
        execution.failure_code = code
    if execution.kind != ExecutionKind.restore:
        return
    kind = state.value
    reason = _CLOSING_REASONS[state]
    if state == ExecutionState.refused and code == "version_conflict":
        # Naming both versions is the point: the operator has to see that
        # their own change is what stopped the restore.
        kind = "refused_conflict"
        reason = "the target changed after this action; refusing to restore over it"
    session.add(
        _audit(
            execution,
            kind=kind,
            authorized=state == ExecutionState.confirmed,
            reason=reason if code is None else f"{reason} ({code})",
            evidence={"state": state.value, "code": code, **(versions or {})},
        )
    )


async def _confirm_restore(
    session: AsyncSession, execution: ActionExecution, now: datetime
) -> None:
    """@spec ACTION-EXECUTOR-18: set ``undone_at`` and ``undone_by`` from the requester."""

    await session.execute(
        update(AgentAction)
        .where(
            AgentAction.id == execution.subject_action_id,
            AgentAction.undone_at.is_(None),
        )
        # ``agent_actions`` stores naive UTC timestamps.
        .values(
            undone_at=now.astimezone(UTC).replace(tzinfo=None),
            undone_by=execution.requested_by,
        )
        .execution_options(synchronize_session=False)
    )


# --------------------------------------------------------------------------- #
# The probe producer (ACTION-EXECUTOR-1, -13)
# --------------------------------------------------------------------------- #


# Route decisions: a probe in one of these states is adopted by a later
# request for the same agent, connector and digest. A ``refused`` or
# ``failed`` probe is not: it recorded no capability, so blocking a new probe
# would leave the digest not restore capable for good.
_ADOPTED_PROBE_STATES = frozenset(
    {
        ExecutionState.requested,
        ExecutionState.claimed,
        ExecutionState.dispatched,
        ExecutionState.confirmed,
    }
)


@probe_router.post("/probes", response_model=ExecutionCreated, status_code=status.HTTP_201_CREATED)
async def create_probe(
    data: ProbeCreate, session: SessionDep, response: Response
) -> ExecutionCreated:
    """Request a capability probe of one connector image for one agent.

    @spec ACTION-EXECUTOR-1 @spec ACTION-EXECUTOR-13 and the route decisions.
    The first probe is keyed ``probe:<agent>:<connector>:<digest>``; a request
    while the latest probe is pending or confirmed adopts it (``200``). After a
    probe that ended ``refused`` or ``failed``, the next one is a new execution
    whose key carries the next probe attempt number (``...:<digest>:2`` and
    so on). A probe's ``authority_ref`` is its key: the body is exactly three
    keys, so no reconcile pass id can reach it.
    """

    if await session.get(Agent, data.agent_id) is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "agent not found")
    base = f"probe:{data.agent_id}:{data.connector}:{data.digest}"
    same_triple = (
        ActionExecution.agent_id == data.agent_id,
        ActionExecution.kind == ExecutionKind.probe,
        or_(
            ActionExecution.idempotency_key == base,
            ActionExecution.idempotency_key.startswith(f"{base}:", autoescape=True),
        ),
    )
    while True:
        earlier = (
            await session.scalars(
                select(ActionExecution)
                .where(*same_triple)
                .order_by(ActionExecution.created_at.desc(), ActionExecution.id.desc())
            )
        ).all()
        if earlier and earlier[0].state in _ADOPTED_PROBE_STATES:
            await session.commit()
            response.status_code = status.HTTP_200_OK
            return ExecutionCreated(execution_id=earlier[0].id, state=earlier[0].state)
        key = base if not earlier else f"{base}:{len(earlier) + 1}"
        created = await session.scalar(
            insert(ActionExecution)
            .values(
                id=uuid.uuid4(),
                kind=ExecutionKind.probe.value,
                agent_id=data.agent_id,
                connector=data.connector,
                tool=None,
                connector_digest=data.digest,
                authority_kind="capability_probe",
                authority_ref=key,
                idempotency_key=key,
                state=ExecutionState.requested.value,
                attempt=0,
            )
            .on_conflict_do_nothing(constraint="uq_action_executions_agent_idempotency_key")
            .returning(ActionExecution.id)
        )
        await session.commit()
        if created is not None:
            return ExecutionCreated(execution_id=created, state=ExecutionState.requested.value)
        # A concurrent request took this key first: read again and adopt it.


# --------------------------------------------------------------------------- #
# Claim (ACTION-EXECUTOR-17)
# --------------------------------------------------------------------------- #


async def _expire_dispatched(session: AsyncSession, now: datetime) -> None:
    """@spec ACTION-EXECUTOR-17: a ``dispatched`` lease that expired is ``indeterminate``.

    The call may have reached the connector, so it is never repeated.
    """

    expired = (
        await session.scalars(
            select(ActionExecution)
            .where(
                ActionExecution.state == ExecutionState.dispatched,
                ActionExecution.lease_expires_at <= now,
            )
            .with_for_update(skip_locked=True)
        )
    ).all()
    for execution in expired:
        _finish(session, execution, ExecutionState.indeterminate, EXPIRED_DISPATCH_CODE, now)


@router.post(
    "/claim",
    response_model=ExecutionOut,
    responses={204: {"description": "Nothing is claimable."}},
)
async def claim_execution(data: ExecutionClaim, session: SessionDep) -> Any:
    """Claim the oldest claimable execution under a lease, or ``204``.

    @spec ACTION-EXECUTOR-17 and the ACTION-EXECUTOR-20 amendment: a
    ``requested`` row is claimable, and so is a ``claimed`` row whose lease
    expired, which is reclaimed with the next attempt so the earlier holder's
    fence is stale. A reclaim forgets the earlier attempt's observation, so the
    new holder must observe again. After ``MAX_ATTEMPTS`` the row is refused.
    @spec ACTION-EXECUTOR-1: with the executor off, nothing is handed out.
    """

    if not get_settings().action_executor_enabled:
        return Response(status_code=status.HTTP_204_NO_CONTENT)
    now = await _now(session)
    await _expire_dispatched(session, now)
    while True:
        execution = await session.scalar(
            select(ActionExecution)
            .where(
                or_(
                    ActionExecution.state == ExecutionState.requested,
                    (ActionExecution.state == ExecutionState.claimed)
                    & (ActionExecution.lease_expires_at <= now),
                )
            )
            .order_by(ActionExecution.created_at, ActionExecution.id)
            .limit(1)
            .with_for_update(skip_locked=True)
        )
        if execution is None:
            await session.commit()
            return Response(status_code=status.HTTP_204_NO_CONTENT)
        if execution.state == ExecutionState.claimed and execution.attempt >= MAX_ATTEMPTS:
            _finish(session, execution, ExecutionState.refused, EXHAUSTED_CLAIM_CODE, now)
            await session.flush()
            continue
        execution.state = ExecutionState.claimed
        execution.attempt = execution.attempt + 1
        execution.lease_owner = data.lease_owner
        execution.lease_expires_at = now + timedelta(seconds=data.lease_seconds)
        execution.outcome = None
        await session.commit()
        await session.refresh(execution)
        return _out(execution)


@receipt_router.get("/{execution_id}", response_model=ExecutionOut)
async def get_execution(execution_id: uuid.UUID, session: SessionDep) -> ExecutionOut:
    """@spec ACTION-EXECUTOR-18: the execution's receipt."""

    execution = await session.get(ActionExecution, execution_id)
    if execution is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "action execution not found")
    return _out(execution)


# --------------------------------------------------------------------------- #
# Observation (ACTION-EXECUTOR-15)
# --------------------------------------------------------------------------- #


def _observed(execution: ActionExecution) -> tuple[bool, str | None]:
    outcome = execution.outcome or {}
    return "observed_version" in outcome, outcome.get("observed_version")


@router.post("/{execution_id}/observation", response_model=ExecutionOut)
async def record_observation(
    execution_id: uuid.UUID, data: ExecutionObservation, session: SessionDep
) -> ExecutionOut:
    """Compare the version observed now with the version the action left.

    @spec ACTION-EXECUTOR-15. Equal: recorded, the execution stays ``claimed``
    and may dispatch. Any difference, or an absent or malformed version: the
    execution ends ``refused`` with ``version_conflict`` and the action is
    released, with the ``refused_conflict`` audit row naming both versions.
    """

    execution = await _locked(session, execution_id)
    now = await _now(session)
    _check_fence(execution, data, now)
    if execution.kind != ExecutionKind.restore:
        raise _conflict("only a restore observes a version")
    seen, version = _observed(execution)
    # Security review F6: a malformed version is stored and audited at most at
    # the sealed version bound, never at whatever length the connector sent.
    observed = data.version[:_MAX_VERSION] if data.version is not None else None
    if seen:
        # A replay of the same observation answers the row unchanged, whether
        # it was equal or a conflict; another version is a conflicting report.
        if version != observed:
            raise _conflict("a different version was already observed")
        return _out(execution)
    if execution.state != ExecutionState.claimed:
        raise _conflict(f"an execution in state {execution.state} observes nothing")

    action = await session.get(AgentAction, execution.subject_action_id)
    recorded = action.post_version if action is not None else None
    valid = bool(data.version) and len(data.version or "") <= _MAX_VERSION
    if valid and recorded and observed == recorded:
        execution.outcome = {"observed_version": observed}
    else:
        versions = {"recorded_version": recorded, "observed_version": observed}
        execution.outcome = dict(versions)
        _finish(
            session, execution, ExecutionState.refused, "version_conflict", now, versions=versions
        )
    await session.commit()
    await session.refresh(execution)
    return _out(execution)


# --------------------------------------------------------------------------- #
# Dispatch and outcome (ACTION-EXECUTOR-17, -18, -20)
# --------------------------------------------------------------------------- #


@router.post("/{execution_id}/dispatch", response_model=ExecutionOut)
async def dispatch_execution(
    execution_id: uuid.UUID, data: ExecutionFence, session: SessionDep
) -> ExecutionOut:
    """Commit ``dispatched``, the last step before a write call.

    @spec ACTION-EXECUTOR-17 @spec ACTION-EXECUTOR-15: only a ``claimed``
    restore whose observed version equalled the recorded one dispatches. A
    probe never does (ACTION-EXECUTOR-1). A forward execution dispatches only
    once the API creates its ledger row at dispatch (ACTION-EXECUTOR-19), which
    is not built yet, so it is refused here too.
    """

    execution = await _locked(session, execution_id)
    now = await _now(session)
    _check_fence(execution, data, now)
    # Route decisions: the setting stops a lease already held, not only new
    # claims, so nothing enters ``dispatched`` while the executor is off.
    if not get_settings().action_executor_enabled:
        raise HTTPException(
            status.HTTP_503_SERVICE_UNAVAILABLE, "the action executor is not enabled"
        )
    if execution.state == ExecutionState.dispatched:
        return _out(execution)
    if execution.state != ExecutionState.claimed:
        raise _conflict(f"an execution in state {execution.state} cannot dispatch")
    if execution.kind != ExecutionKind.restore:
        raise _conflict(f"a {execution.kind} execution cannot dispatch")
    seen, _ = _observed(execution)
    if not seen:
        raise _conflict("a restore dispatches only after an unchanged version is observed")
    execution.state = ExecutionState.dispatched
    execution.dispatched_at = now
    await session.commit()
    await session.refresh(execution)
    return _out(execution)


def _reported(execution: ActionExecution) -> tuple[str, str | None, bool | None]:
    code = execution.refusal_code or execution.failure_code
    capable = (execution.outcome or {}).get("restore_capable")
    return execution.state, code, capable


@router.post("/{execution_id}/outcome", response_model=ExecutionOut)
async def report_outcome(
    execution_id: uuid.UUID, data: ExecutionOutcome, session: SessionDep
) -> ExecutionOut:
    """Record how an execution ended.

    @spec ACTION-EXECUTOR-17 @spec ACTION-EXECUTOR-18 @spec ACTION-EXECUTOR-20.
    ``refused`` lands only on a ``claimed`` row, with a pre-dispatch code:
    past ``dispatched`` the call may have reached the connector. ``confirmed``,
    ``failed`` and ``indeterminate`` land only on a ``dispatched`` row, except
    a probe, which never dispatches and is ``confirmed`` from ``claimed`` with
    the verbs it observed. An unknown post-dispatch code is normalized by stage.
    A replay of the stored outcome returns the row unchanged; a different one
    is refused and the first stands.
    """

    try:
        code = outcome_code(data.state, data.code)
    except CodeRejected as exc:
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_ENTITY, str(exc)) from exc
    execution = await _locked(session, execution_id)
    probe_confirmed = execution.kind == ExecutionKind.probe and data.state == "confirmed"
    if probe_confirmed and data.advertised is None:
        raise HTTPException(
            status.HTTP_422_UNPROCESSABLE_ENTITY, "a finished probe reports what it observed"
        )
    if not probe_confirmed and data.advertised is not None:
        raise HTTPException(
            status.HTTP_422_UNPROCESSABLE_ENTITY, "only a finished probe reports verbs"
        )
    capable = RESTORE_PAIR <= set(data.advertised or ()) if probe_confirmed else None
    now = await _now(session)
    _check_fence(execution, data, now)

    if execution.state in _TERMINAL:
        if _reported(execution) != (data.state, code, capable):
            raise _conflict("this execution already ended with another outcome")
        return _out(execution)

    state = ExecutionState(data.state)
    allowed_from: ExecutionState | None
    if state == ExecutionState.refused:
        allowed_from = ExecutionState.claimed
    elif execution.kind == ExecutionKind.probe:
        allowed_from = ExecutionState.claimed if state == ExecutionState.confirmed else None
    else:
        allowed_from = ExecutionState.dispatched
    if execution.state != allowed_from:
        raise _conflict(
            f"a {execution.kind} execution in state {execution.state} cannot end {state.value}"
        )

    _finish(session, execution, state, code, now)
    if probe_confirmed:
        # @spec ACTION-EXECUTOR-13: one row per agent, connector and digest,
        # capable only for the restore and observe_version pair.
        execution.outcome = {"restore_capable": capable}
        await session.execute(
            insert(ConnectorCapability)
            .values(
                agent_id=execution.agent_id,
                connector=execution.connector,
                digest=execution.connector_digest,
                restore_capable=bool(capable),
                observed_at=now,
            )
            .on_conflict_do_update(
                index_elements=["agent_id", "connector", "digest"],
                set_={"restore_capable": bool(capable), "observed_at": now},
            )
        )
    elif state == ExecutionState.confirmed and execution.kind == ExecutionKind.restore:
        await _confirm_restore(session, execution, now)
    await session.commit()
    await session.refresh(execution)
    return _out(execution)
