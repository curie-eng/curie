"""The action ledger: record what an agent did, and refuse or permit putting it back.

The worker posts one record per side-effecting call. Reading is per action and
per conversation, because a receipt is rendered from a turn's rows.

This router owns the two rules that make an undo safe, and owns them here rather
than in the sandbox: a restore needs a prior state that was actually captured,
and it is refused when the world no longer looks like what the action left. The
decision point is the server that holds the record, which is the same placement
approvals use and for the same reason.

What this router does NOT do is call the connector. It records the decision and
returns the call to make; the executor lives where a connector is reachable.
Keeping the ruling and the execution apart means a refusal cannot be bypassed by
whoever happens to hold the connector's address.
"""

import logging
import uuid
from datetime import UTC, datetime
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError

from ..auth import require_api_key
from ..deps import SessionDep
from ..models import ActionAuditEntry, AgentAction, UndoStatus
from ..schemas import ActionAuditOut, ActionOut, ActionRecordIn, ActionUndo

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/actions", tags=["actions"], dependencies=[Depends(require_api_key)])


def _out(action: AgentAction) -> ActionOut:
    payload = ActionOut.model_validate(action, from_attributes=True)
    return payload


@router.post("", response_model=ActionOut, status_code=status.HTTP_201_CREATED)
async def record_action(body: ActionRecordIn, session: SessionDep) -> ActionOut:
    """Record one side-effecting call.

    Idempotent on ``dedupe_key``: the worker's delivery is at-least-once, so a
    redelivered turn that replays the same call must adopt the existing row
    rather than fork a second record of one real-world action.
    """

    action = AgentAction(**body.model_dump())
    session.add(action)
    try:
        await session.commit()
    except IntegrityError:
        await session.rollback()
        existing = await session.scalar(
            select(AgentAction).where(AgentAction.dedupe_key == body.dedupe_key)
        )
        if existing is None:
            raise
        return _out(existing)
    await session.refresh(action)
    return _out(action)


@router.get("", response_model=list[ActionOut])
async def list_actions(
    session: SessionDep, conversation_id: str | None = None, turn_id: str | None = None
) -> list[ActionOut]:
    stmt = select(AgentAction).order_by(AgentAction.created_at)
    if conversation_id is not None:
        stmt = stmt.where(AgentAction.conversation_id == conversation_id)
    if turn_id is not None:
        stmt = stmt.where(AgentAction.turn_id == turn_id)
    rows = (await session.scalars(stmt)).all()
    return [_out(row) for row in rows]


@router.get("/{action_id}", response_model=ActionOut)
async def get_action(action_id: uuid.UUID, session: SessionDep) -> ActionOut:
    action = await session.get(AgentAction, action_id)
    if action is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "no such action")
    return _out(action)


@router.get("/{action_id}/audit", response_model=list[ActionAuditOut])
async def get_action_audit(action_id: uuid.UUID, session: SessionDep) -> list[ActionAuditOut]:
    rows = (
        await session.scalars(
            select(ActionAuditEntry)
            .where(ActionAuditEntry.action_id == action_id)
            .order_by(ActionAuditEntry.created_at)
        )
    ).all()
    return [ActionAuditOut.model_validate(row, from_attributes=True) for row in rows]


async def _refuse(
    session: SessionDep,
    action: AgentAction,
    body: ActionUndo,
    reason: str,
    code: int,
    evidence: dict[str, Any] | None = None,
) -> None:
    """Record the refusal, then raise it. A refusal nobody can see is a bug
    report the operator never gets."""

    session.add(
        ActionAuditEntry(
            action_id=action.id,
            action="refused",
            actor=body.actor,
            actor_channel=body.actor_channel,
            reason=reason,
            evidence=evidence,
        )
    )
    await session.commit()
    raise HTTPException(code, reason)


@router.post("/{action_id}/undo", response_model=ActionOut)
async def undo_action(action_id: uuid.UUID, body: ActionUndo, session: SessionDep) -> ActionOut:
    """Rule on an undo, and mark it claimed for the executor.

    Every refusal path writes an audit entry before it raises, so the reason a
    restore did not happen survives the HTTP response that carried it.
    """

    action = await session.get(AgentAction, action_id)
    if action is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "no such action")

    if action.undo_status == UndoStatus.undone:
        await _refuse(
            session, action, body, "this action was already undone", status.HTTP_409_CONFLICT
        )
    if action.outcome != "succeeded":
        await _refuse(
            session,
            action,
            body,
            f"the action did not succeed (outcome {action.outcome}), so there is "
            "nothing known to reverse",
            status.HTTP_409_CONFLICT,
        )
    if action.snapshot is None or action.target is None:
        await _refuse(
            session,
            action,
            body,
            action.irreversible_reason
            or "no recorded prior state, so this action cannot be undone",
            status.HTTP_409_CONFLICT,
        )

    # The rule the feature lives on. A blind restore silently reverts whatever a
    # human did by hand after the agent acted, which turns an undo control into a
    # way for the platform to fight the operator.
    if body.observed_state is None:
        await _refuse(
            session,
            action,
            body,
            "refusing to restore without the live state to compare against",
            status.HTTP_428_PRECONDITION_REQUIRED,
        )
    if action.post_state is not None and body.observed_state != action.post_state:
        await _refuse(
            session,
            action,
            body,
            "the target changed since this action; refusing to restore over it",
            status.HTTP_409_CONFLICT,
            evidence={"expected": action.post_state, "observed": body.observed_state},
        )

    action.undo_status = UndoStatus.undone
    action.undone_at = datetime.now(UTC).replace(tzinfo=None)
    action.undone_by = body.actor
    session.add(
        ActionAuditEntry(
            action_id=action.id,
            action="undone",
            actor=body.actor,
            actor_channel=body.actor_channel,
            evidence={"restored": action.snapshot},
        )
    )
    await session.commit()
    await session.refresh(action)
    return _out(action)
