"""Database access for learned skills (ADR 0237).

A skill is a head row plus insert-only revisions. Every change goes through
``record``, which appends the next revision and moves the head to it inside
the caller's transaction. Callers hold the agent row lock (``lock_toggle``)
for the whole mutation, so the head's revision compare, the active-skill count
and the active-slug check cannot race another change to the same agent.
"""

import hashlib
import uuid
from collections.abc import Sequence
from datetime import UTC, datetime

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from ..models import Agent, LearnedSkill, LearnedSkillRevision


async def toggle(session: AsyncSession, agent_id: uuid.UUID) -> bool | None:
    """The agent's ``learned_skills`` switch; None when there is no such agent."""

    enabled: bool | None = await session.scalar(
        select(Agent.learned_skills).where(Agent.id == agent_id)
    )
    return enabled


async def lock_toggle(session: AsyncSession, agent_id: uuid.UUID) -> bool | None:
    """``toggle``, taking the agent row lock that serializes this agent's changes."""

    enabled: bool | None = await session.scalar(
        select(Agent.learned_skills).where(Agent.id == agent_id).with_for_update()
    )
    return enabled


async def find(
    session: AsyncSession, agent_id: uuid.UUID, ref: str, *, include_deleted: bool
) -> LearnedSkill | None:
    """Resolve ``ref``: the skill id first, then the active skill holding that
    slug, then (``include_deleted``) the most recently deleted one."""

    try:
        skill_id: uuid.UUID | None = uuid.UUID(ref)
    except ValueError:
        skill_id = None
    if skill_id is not None:
        skill = await session.scalar(
            select(LearnedSkill).where(
                LearnedSkill.id == skill_id, LearnedSkill.agent_id == agent_id
            )
        )
        if skill is not None and (include_deleted or skill.deleted_at is None):
            return skill
    query = select(LearnedSkill).where(LearnedSkill.agent_id == agent_id, LearnedSkill.slug == ref)
    if not include_deleted:
        query = query.where(LearnedSkill.deleted_at.is_(None))
    query = query.order_by(
        LearnedSkill.deleted_at.desc().nulls_first(), LearnedSkill.updated_at.desc()
    )
    found: LearnedSkill | None = await session.scalar(query.limit(1))
    return found


async def active_with_heads(
    session: AsyncSession, agent_id: uuid.UUID
) -> Sequence[tuple[LearnedSkill, LearnedSkillRevision]]:
    """The agent's active skills, ordered by slug, each with its head revision."""

    rows = await session.execute(
        select(LearnedSkill, LearnedSkillRevision)
        .join(
            LearnedSkillRevision,
            (LearnedSkillRevision.skill_id == LearnedSkill.id)
            & (LearnedSkillRevision.revision == LearnedSkill.revision),
        )
        .where(LearnedSkill.agent_id == agent_id, LearnedSkill.deleted_at.is_(None))
        .order_by(LearnedSkill.slug)
    )
    return [(skill, head) for skill, head in rows.tuples()]


async def revision(
    session: AsyncSession, skill_id: uuid.UUID, number: int
) -> LearnedSkillRevision | None:
    row: LearnedSkillRevision | None = await session.scalar(
        select(LearnedSkillRevision).where(
            LearnedSkillRevision.skill_id == skill_id, LearnedSkillRevision.revision == number
        )
    )
    return row


async def revisions(session: AsyncSession, skill_id: uuid.UUID) -> Sequence[LearnedSkillRevision]:
    rows = await session.scalars(
        select(LearnedSkillRevision)
        .where(LearnedSkillRevision.skill_id == skill_id)
        .order_by(LearnedSkillRevision.revision)
    )
    return rows.all()


async def active_count(session: AsyncSession, agent_id: uuid.UUID) -> int:
    count = await session.scalar(
        select(func.count())
        .select_from(LearnedSkill)
        .where(LearnedSkill.agent_id == agent_id, LearnedSkill.deleted_at.is_(None))
    )
    return count or 0


def digest(slug: str, description: str, body: str) -> str:
    return hashlib.sha256(f"{slug}\0{description}\0{body}".encode()).hexdigest()


async def record(
    session: AsyncSession,
    skill: LearnedSkill,
    *,
    action: str,
    description: str,
    body: str,
    deleted: bool,
    author: str,
    source: str,
    conversation: str | None,
    turn: str | None,
    reverted_to: int | None = None,
) -> LearnedSkillRevision:
    """Append the next revision of ``skill`` and move its head to it. A new
    skill arrives with ``revision`` 0 and not yet added to the session."""

    now = datetime.now(UTC)
    if skill.revision == 0:
        skill.created_at = now
        session.add(skill)
    skill.revision += 1
    skill.description = description
    skill.body = body
    skill.deleted_at = (skill.deleted_at or now) if deleted else None
    skill.updated_at = now
    await session.flush()
    head = LearnedSkillRevision(
        skill_id=skill.id,
        revision=skill.revision,
        action=action,
        slug=skill.slug,
        description=description,
        body=body,
        digest=digest(skill.slug, description, body),
        deleted=deleted,
        reverted_to=reverted_to,
        author=author,
        conversation=conversation,
        turn=turn,
        source=source,
        created_at=now,
    )
    session.add(head)
    await session.flush()
    return head
