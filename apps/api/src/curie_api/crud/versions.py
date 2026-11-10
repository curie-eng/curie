"""Database access for versions."""

import uuid

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from curie_api.schemas.versions import VersionCreate

from ..models import Agent, AgentVersion


async def get_version(session: AsyncSession, version_id: uuid.UUID) -> AgentVersion | None:
    return await session.get(AgentVersion, version_id)


async def attach_bundle(
    session: AsyncSession,
    version: AgentVersion,
    bundle_ref: str,
    bundle_sha256: str,
) -> AgentVersion:
    version.bundle_ref = bundle_ref
    version.bundle_sha256 = bundle_sha256
    await session.commit()
    await session.refresh(version)
    return version


async def create_version_row(
    session: AsyncSession,
    agent_id: uuid.UUID,
    version_label: str,
    created_by: str,
    commit_sha: str | None = None,
    bundle_ref: str | None = None,
) -> AgentVersion:
    version = AgentVersion(
        agent_id=agent_id,
        # The agent's tenant, read inside the INSERT so there is no
        # read-then-write window (ADR 0166 decision 3).
        tenant_id=select(Agent.tenant_id).where(Agent.id == agent_id).scalar_subquery(),
        version_label=version_label,
        created_by=created_by,
        commit_sha=commit_sha,
        bundle_ref=bundle_ref,
    )
    session.add(version)
    await session.commit()
    await session.refresh(version)
    return version


async def create_version(
    session: AsyncSession, agent_id: uuid.UUID, data: VersionCreate
) -> AgentVersion:
    return await create_version_row(
        session,
        agent_id,
        version_label=data.version_label,
        created_by=data.created_by,
        commit_sha=data.commit_sha,
        bundle_ref=data.bundle_ref,
    )


async def get_version_by_commit(
    session: AsyncSession, agent_id: uuid.UUID, commit_sha: str, created_by: str
) -> AgentVersion | None:
    version: AgentVersion | None = await session.scalar(
        select(AgentVersion).where(
            AgentVersion.agent_id == agent_id,
            AgentVersion.commit_sha == commit_sha,
            AgentVersion.created_by == created_by,
        )
    )
    return version


async def list_versions(session: AsyncSession, agent_id: uuid.UUID) -> list[AgentVersion]:
    result = await session.scalars(
        select(AgentVersion)
        .where(AgentVersion.agent_id == agent_id)
        .order_by(AgentVersion.created_at)
    )
    return list(result)
