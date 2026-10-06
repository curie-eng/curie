"""The action ledger: record what an agent did, and read it back (ADR-0117).

The worker has no database of its own -- it persists an approval by POSTing to
this API, and it records an action the same way. So the two ACI frames of one
side-effecting call arrive here as two requests: a create when the call was made,
and a completion when its result came back.

Both are idempotent, because the worker redelivers at least once (ADR-0013). A
replayed create adopts the record it already wrote; a replayed completion is
returned unchanged rather than overwriting the first account of the call. That
second one matters more than it looks: the prior state on a record is what a
restore replays, so a completion allowed to rewrite it moves the target of an
undo that has already been offered to a human.

Ruling on an undo is here, and executing one is the action executor's
(ACTION-EXECUTOR-3). A granted ruling writes a ``requested`` restore execution
and its ``authorized`` audit row in one transaction and returns the
execution's id; it never hands back the ``target`` or the sealed snapshot, and
never marks the action undone. The executor observes the live version through
the pinned connector and reports through ``routers/action_executions.py``,
which writes ``undone_at`` only when the restore is confirmed.
"""

import logging
import uuid
from collections.abc import Sequence
from typing import Annotated, NoReturn

from fastapi import APIRouter, Depends, HTTPException, Response, status
from sqlalchemy import func
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from curie_api.crud import actions as crud_actions
from curie_api.crud import approvals as crud_approvals
from curie_api.schemas.actions import (
    ActionAuditOut,
    ActionComplete,
    ActionOut,
    ActionRecord,
    ActionUndo,
    ActionUndoOut,
)

from ..action_undoable import undo_refusal, undoable_action_ids
from ..auth import require_api_key
from ..config import get_settings
from ..deps import ApproverSetSelectorDep, SessionDep, StoreDep, get_store
from ..models import (
    ActionAuditEntry,
    ActionExecution,
    ActionStatus,
    AgentAction,
    Approval,
    ExecutionKind,
    ExecutionState,
)
from ..storage import BundleStore, ObjectStore

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/actions", tags=["actions"], dependencies=[Depends(require_api_key)])


def _out(action: AgentAction, *, undoable: bool) -> ActionOut:
    fields = {name: getattr(action, name) for name in ActionOut.model_fields if name != "undoable"}
    return ActionOut.model_validate({**fields, "undoable": undoable})


async def _outs(
    session: SessionDep, store: StoreDep, actions: Sequence[AgentAction]
) -> list[ActionOut]:
    """Render actions with ``undoable`` derived at read time.

    @spec ACTION-EXECUTOR-11: every read of an action, single or listed, takes
    ``undoable`` from the one derivation, so the two can never disagree.
    """

    undoable = await undoable_action_ids(session, store, actions)
    return [_out(action, undoable=action.id in undoable) for action in actions]


@router.post("", response_model=ActionOut, status_code=status.HTTP_201_CREATED)
async def record_action(
    data: ActionRecord, session: SessionDep, store: StoreDep, response: Response
) -> ActionOut:
    """Record a side-effecting call; idempotent on ``dedupe_key``.

    Created ``pending``: the call was made and nothing has come back to say what
    it did, which is also the state a turn that dies mid-call leaves behind.
    """

    try:
        action = await crud_actions.create_action(session, data)
    except IntegrityError as exc:
        await session.rollback()
        existing = await crud_actions.get_action_by_dedupe_key(session, data.dedupe_key)
        if existing is None:  # raced with a delete; surface the conflict as-is
            raise HTTPException(
                status.HTTP_409_CONFLICT, "action violates a uniqueness constraint"
            ) from exc
        response.status_code = status.HTTP_200_OK
        return (await _outs(session, store, [existing]))[0]
    return (await _outs(session, store, [action]))[0]


@router.get("", response_model=list[ActionOut])
async def list_actions(
    session: SessionDep,
    store: StoreDep,
    conversation_id: str | None = None,
    agent_id: uuid.UUID | None = None,
    limit: int = 50,
) -> list[ActionOut]:
    """A conversation's actions, oldest first -- the order a receipt lists them."""

    actions = await crud_actions.list_actions(
        session,
        conversation_id=conversation_id,
        agent_id=agent_id,
        limit=min(max(limit, 1), 200),
    )
    return await _outs(session, store, actions)


@router.get("/{action_id}", response_model=ActionOut)
async def get_action(action_id: uuid.UUID, session: SessionDep, store: StoreDep) -> ActionOut:
    action = await crud_actions.get_action(session, action_id)
    if action is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "action not found")
    return (await _outs(session, store, [action]))[0]


@router.post("/{action_id}/complete", response_model=ActionOut)
async def complete_action(
    action_id: uuid.UUID, data: ActionComplete, session: SessionDep, store: StoreDep
) -> ActionOut:
    """Record what the tool answered.

    ``prior_state`` and ``target`` are what a restore replays. A completion that
    carries neither produces a record that is not undoable, which is the honest
    answer for a connector that replied in prose -- nothing has to declare it.
    """

    action = await crud_actions.get_action(session, action_id)
    if action is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "action not found")
    completed = await crud_actions.complete_action(session, action, data)
    return (await _outs(session, store, [completed]))[0]


@router.get("/{action_id}/audit", response_model=list[ActionAuditOut])
async def get_action_audit(action_id: uuid.UUID, session: SessionDep) -> list[ActionAuditOut]:
    """The action's audit trail, oldest first.

    A refused undo leaves no trace anywhere else -- the world did not change and
    the record did not move -- so this is the only place the reason survives.
    """

    action = await crud_actions.get_action(session, action_id)
    if action is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "action not found")
    entries = await crud_actions.list_action_audit(session, action_id)
    return [ActionAuditOut.model_validate(e) for e in entries]


# The request's bundle store, or None when the ruling is called outside a
# request (an in-process caller); the ruling then opens the configured store.
_RulingStoreDep = Annotated[ObjectStore | None, Depends(get_store)]

# @spec ACTION-EXECUTOR-11: the stated reason for each missing ingredient. Each
# names what is missing and never a state, an envelope or a version value.
_INGREDIENT_REASONS = {
    "refused_no_agent": "this action ran under no agent, so no binding can restore it",
    "refused_unsealed": "no sealed snapshot was recorded, so this cannot be undone",
    "refused_unversioned": (
        "this call never reported the version it left, so whether the world has "
        "moved since cannot be determined"
    ),
    "refused_irreversible": "no target was recorded, so there is nowhere to restore to",
    "refused_no_digest": "the connector image this call ran under was not recorded",
    "refused_restore_in_flight": "a restore of this action already exists",
    "refused_not_restore_capable": "this connector image is not known to restore",
    "refused_key_custody": (
        "the agent's in-force version does not hold the sealing key for this connector"
    ),
}

# The authorizer name recorded when nothing gated the forward call. Not "none":
# the audit row is read by a human asking who permitted a write into their
# infrastructure, and "ungated" answers that question where a blank does not.
UNGATED = "ungated"


async def _authorize_undo(
    session: SessionDep,
    action: AgentAction,
    data: ActionUndo,
    approver_sets: ApproverSetSelectorDep,
) -> tuple[str, bool, str]:
    """Decide whether ``data.actor`` may undo ``action`` (ADR-0117 decision 3).

    Symmetry, in both directions. A call nobody had to approve is not gated on
    the way back: the state being restored is one the cluster was already in, and
    it got there without anyone approving it. A call that WAS gated needs an
    authorizer of that same route, resolved against membership the way ADR-0034
    resolves an approver -- someone who could have permitted the change.

    ADR-0106's authenticated-principal contract applies to approval resolution,
    not this ADR-0117 action-undo seam. This path preserves ADR-0117's existing
    approver-set check over ``data.actor`` and ``data.actor_channel`` and adds no
    distinct-requester rule; adding one would demand MORE authorization than the
    forward action needed, which decision 3 rules out in the same sentence that
    requires the route.
    """

    if action.gate_approval_id is None:
        return UNGATED, True, ""

    approval = await session.get(Approval, action.gate_approval_id)
    if approval is None:
        # The gate is unreadable, not absent. Treating it as absent would let a
        # deleted approval turn a gated action into a freely undoable one.
        return UNGATED, False, "the approval that gated this action can no longer be read"

    binding = await crud_approvals.get_approval_route_binding(session, approval)
    approver_set = approver_sets(approval, binding)
    verdict = await approver_set.contains(data.actor, data.actor_channel)
    if verdict.undetermined:
        # `member` is meaningless here. Failing open would let an outage at the
        # membership provider authorize a write into a customer's cluster.
        return (
            approver_set.audit_name,
            False,
            verdict.reason or "could not establish whether the actor may undo this",
        )
    return approver_set.audit_name, verdict.member, verdict.reason


async def _refuse(
    session: AsyncSession,
    action_id: uuid.UUID,
    data: ActionUndo,
    *,
    kind: str,
    reason: str,
    code: int,
    authorizer: str = "conflict-check",
    evidence: dict[str, object] | None = None,
) -> NoReturn:
    """Record the refusal, then raise it.

    Committed before the exception, so the reason outlives the HTTP response
    that carried it. An operator who pressed undo and saw a red message has
    somewhere to look; an operator who never saw the message still does.
    """

    session.add(
        ActionAuditEntry(
            action_id=action_id,
            action=kind,
            actor=data.actor,
            actor_channel=data.actor_channel,
            authorizer=authorizer,
            authorized=False,
            reason=reason,
            evidence=evidence,
            # A stale contender may start its transaction before the winner
            # but insert its refusal afterward. now() uses transaction start.
            created_at=func.clock_timestamp(),
        )
    )
    await session.commit()
    raise HTTPException(code, reason)


_EXECUTOR_DISABLED_REASON = "the action executor is not enabled on this installation"


@router.post(
    "/{action_id}/undo", response_model=ActionUndoOut, status_code=status.HTTP_202_ACCEPTED
)
async def undo_action(
    action_id: uuid.UUID,
    data: ActionUndo,
    session: SessionDep,
    approver_sets: ApproverSetSelectorDep,
    store: _RulingStoreDep = None,
) -> ActionUndoOut:
    """Rule on putting back what this action changed, and request the restore.

    @spec ACTION-EXECUTOR-3. A 202 means a ``requested`` restore execution and
    its ``authorized`` audit row were written together; every other outcome is
    a refusal that wrote one audit row and no execution. The refusals are
    ordered from the record's own state outward, so the most specific true
    reason is the one the operator is told.

    Authorization runs first, before any of the record's own state is examined:
    whether an actor may undo at all precedes whether this particular undo is
    possible, and it keeps a refused actor from learning anything about the
    record, its versions included.

    The caller no longer supplies the live state: the executor observes the
    version through the pinned connector and the API compares it before any
    write (ACTION-EXECUTOR-15).
    """

    action = await crud_actions.get_action(session, action_id)
    if action is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "action not found")

    authorizer, allowed, reason = await _authorize_undo(session, action, data, approver_sets)
    if not allowed:
        await _refuse(
            session,
            action.id,
            data,
            kind="refused_unauthorized",
            reason=reason or "not authorized to undo this action",
            code=status.HTTP_403_FORBIDDEN,
            authorizer=authorizer,
        )

    if action.undone_at is not None:
        await _refuse(
            session,
            action.id,
            data,
            kind="refused_already_undone",
            reason="this action was already undone",
            code=status.HTTP_409_CONFLICT,
        )
    if action.status != ActionStatus.succeeded:
        await _refuse(
            session,
            action.id,
            data,
            kind="refused_unsuccessful",
            reason=(
                f"the call did not succeed (status {action.status}), so there is nothing "
                "known to reverse"
            ),
            code=status.HTTP_409_CONFLICT,
        )
    # @spec ACTION-EXECUTOR-11: the ruling follows the derived ``undoable``. A
    # record missing any ingredient, or holding a live restore, is refused with
    # that code before any granted-undo audit row could be written.
    code = await undo_refusal(session, store or BundleStore(get_settings()), action)
    if code is not None:
        reason = _INGREDIENT_REASONS.get(code, "this action cannot be undone")
        if code == "refused_unsealed" and action.detail:
            # The connector's own words when it had them: the receipt and the
            # refusal state the same sentence.
            reason = action.detail
        await _refuse(
            session, action.id, data, kind=code, reason=reason, code=status.HTTP_409_CONFLICT
        )
    # @spec ACTION-EXECUTOR-1 @spec ACTION-EXECUTOR-20: the last ruling check.
    # Off, an undo that would authorize a restore is refused with 503, one
    # audit row and no execution.
    if not get_settings().action_executor_enabled:
        await _refuse(
            session,
            action.id,
            data,
            kind="executor_disabled",
            reason=_EXECUTOR_DISABLED_REASON,
            code=status.HTTP_503_SERVICE_UNAVAILABLE,
        )

    # Narrowed by ``undo_refusal``: a record with every ingredient has an
    # agent, a sealed envelope, a version, a connector and its digest.
    assert action.agent_id is not None and action.prior_state is not None
    assert action.connector is not None and action.connector_digest is not None
    subject_id = action.id
    audit_id = uuid.uuid4()
    execution_id = uuid.uuid4()
    execution = ActionExecution(
        id=execution_id,
        kind=ExecutionKind.restore.value,
        agent_id=action.agent_id,
        connector=action.connector,
        tool="restore",
        subject_action_id=subject_id,
        connector_digest=action.connector_digest,
        authority_kind="undo_ruling",
        authority_ref=str(audit_id),
        requested_by=data.actor,
        # @spec ACTION-EXECUTOR-2: keyed by the authorizing audit row.
        idempotency_key=f"restore:{subject_id}:{audit_id}",
        state=ExecutionState.requested.value,
        attempt=0,
    )
    session.add(execution)
    session.add(
        ActionAuditEntry(
            id=audit_id,
            action_id=subject_id,
            action="authorized",
            actor=data.actor,
            actor_channel=data.actor_channel,
            authorizer=authorizer,
            authorized=True,
            # @spec ACTION-EXECUTOR-3: the execution id, the key identifier and
            # the recorded version only; never the envelope or a state.
            evidence={
                "execution_id": str(execution_id),
                "kid": action.prior_state.get("kid"),
                "version": action.post_version,
            },
            created_at=func.clock_timestamp(),
        )
    )
    try:
        await session.commit()
    except IntegrityError:
        # A concurrent ruling won the partial unique index on live restores
        # (ACTION-EXECUTOR-2): turn the loss into an audited refusal.
        await session.rollback()
        await _refuse(
            session,
            subject_id,
            data,
            kind="refused_restore_in_flight",
            reason=_INGREDIENT_REASONS["refused_restore_in_flight"],
            code=status.HTTP_409_CONFLICT,
        )
    return ActionUndoOut(execution_id=execution_id, state=ExecutionState.requested.value)
