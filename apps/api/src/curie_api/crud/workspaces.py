"""Database access for workspaces."""

import uuid

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from ..models import ThreadWorkspace


async def get_thread_workspace(
    session: AsyncSession, *, agent_id: uuid.UUID, conversation_id: str
) -> ThreadWorkspace | None:
    selected: ThreadWorkspace | None = await session.scalar(
        select(ThreadWorkspace).where(
            ThreadWorkspace.agent_id == agent_id,
            ThreadWorkspace.conversation_id == conversation_id,
        )
    )
    return selected


async def select_thread_workspace(
    session: AsyncSession,
    *,
    agent_id: uuid.UUID,
    deployment_id: uuid.UUID | None,
    conversation_id: str,
    repo_full_name: str,
    selected_by: str,
    revision: str | None = None,
) -> tuple[ThreadWorkspace, bool]:
    """Insert the first selection or atomically adopt the concurrent winner."""

    candidate_id = uuid.uuid4()
    inserted = await session.scalar(
        insert(ThreadWorkspace)
        .values(
            id=candidate_id,
            agent_id=agent_id,
            selected_by_deployment_id=deployment_id,
            conversation_id=conversation_id,
            repo_full_name=repo_full_name,
            revision=revision,
            selected_by=selected_by,
        )
        .on_conflict_do_nothing(constraint="thread_workspaces_agent_conversation_key")
        .returning(ThreadWorkspace.id)
    )
    await session.commit()
    selected = await get_thread_workspace(
        session, agent_id=agent_id, conversation_id=conversation_id
    )
    assert selected is not None
    return selected, inserted == candidate_id
