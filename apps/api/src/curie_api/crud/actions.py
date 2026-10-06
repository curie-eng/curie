"""Database access for actions."""

import uuid
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import null, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from curie_api.schemas.actions import ActionComplete, ActionRecord

from ..models import ActionAuditEntry, ActionStatus, AgentAction


async def create_action(session: AsyncSession, data: ActionRecord) -> AgentAction:
    """Insert a pending action record.

    Raises IntegrityError on a ``dedupe_key`` replay; the router maps that to the
    existing record, so a redelivered turn adopts what it already wrote.
    """

    action = AgentAction(
        agent_id=data.agent_id,
        conversation_id=data.conversation_id,
        call_id=data.call_id,
        tool=data.tool,
        arguments=data.arguments,
        detail=data.detail,
        gate_approval_id=data.gate_approval_id,
        dedupe_key=data.dedupe_key,
        status=ActionStatus.pending,
    )
    session.add(action)
    await session.commit()
    await session.refresh(action)
    return action


async def get_action(session: AsyncSession, action_id: uuid.UUID) -> AgentAction | None:
    return await session.get(AgentAction, action_id)


async def get_action_by_dedupe_key(session: AsyncSession, key: str) -> AgentAction | None:
    result = await session.execute(select(AgentAction).where(AgentAction.dedupe_key == key))
    return result.scalar_one_or_none()


async def list_actions(
    session: AsyncSession,
    *,
    conversation_id: str | None = None,
    agent_id: uuid.UUID | None = None,
    limit: int = 50,
) -> list[AgentAction]:
    """A conversation's actions, oldest first -- the order a receipt lists them."""

    query = select(AgentAction)
    if conversation_id is not None:
        query = query.where(AgentAction.conversation_id == conversation_id)
    if agent_id is not None:
        query = query.where(AgentAction.agent_id == agent_id)
    query = query.order_by(AgentAction.created_at, AgentAction.call_id).limit(limit)
    result = await session.execute(query)
    return list(result.scalars().all())


async def complete_action(
    session: AsyncSession, action: AgentAction, data: ActionComplete
) -> AgentAction:
    """Record what came back, once.

    A completion that arrives for an already-completed record is a redelivery,
    not a correction: the first account of a call is the one that was true when
    it happened, and overwriting it with a second would silently move a prior
    state a restore is about to replay. Returned unchanged.
    """

    # A Core UPDATE would bind Python None into these JSONB columns as the JSON
    # value ``null``; an unreported field must stay SQL NULL, as it was when
    # the record opened, so a SQL ``IS NULL`` test agrees with ``undoable``.
    values: dict[str, Any] = {
        "status": ActionStatus.failed if data.failed else ActionStatus.succeeded,
        "result": null() if data.result is None else data.result,
        "prior_state": null() if data.prior_state is None else data.prior_state,
        "post_state": null() if data.post_state is None else data.post_state,
        "target": null() if data.target is None else data.target,
        "completed_at": datetime.now(UTC).replace(tzinfo=None),
    }
    if data.detail is not None:
        values["detail"] = data.detail
    await session.execute(
        update(AgentAction)
        .where(AgentAction.id == action.id, AgentAction.status == ActionStatus.pending)
        .values(**values)
        .execution_options(synchronize_session=False)
    )
    # The row's current state wins even when this session loaded ``pending``
    # before another completion committed. The SQL predicate, not the stale ORM
    # object, decides which completion is first.
    await session.commit()
    await session.refresh(action)
    return action


async def list_action_audit(session: AsyncSession, action_id: uuid.UUID) -> list[ActionAuditEntry]:
    result = await session.execute(
        select(ActionAuditEntry)
        .where(ActionAuditEntry.action_id == action_id)
        .order_by(ActionAuditEntry.created_at)
    )
    return list(result.scalars().all())
