"""Database access for deployments."""

import uuid

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from curie_api.schemas.deployments import DeploymentCreate

from ..models import Agent, AgentVersion, Deployment, Environment

_WORKSPACE_UNSET = object()


async def agent_has_active_deployment(session: AsyncSession, agent_id: uuid.UUID) -> bool:
    result = await session.scalar(
        select(Deployment.id)
        .where(Deployment.agent_id == agent_id, Deployment.status == "active")
        .limit(1)
    )
    return result is not None


async def version_has_active_deployment(session: AsyncSession, version_id: uuid.UUID) -> bool:
    """Whether an active deployment row ALREADY points at this version (#2436).

    The version-scoped sibling of ``agent_has_active_deployment``, and the
    condition that makes the approval-route gate on a bundle attachment
    conditional. An ordinary pre-deployment upload stays unrestricted, because
    the CLI's ``prepare_deploy`` uploads before ``curie <tier> approvals`` binds;
    an attachment onto a version the worker's resolve query can already boot
    (``curie_worker.binding`` joins ``deployments.status = 'active'`` to
    ``agent_versions.bundle_ref``) is the moment the bundle goes live, so it is
    gated like a deployment.
    """

    result = await session.scalar(
        select(Deployment.id)
        .where(Deployment.version_id == version_id, Deployment.status == "active")
        .limit(1)
    )
    return result is not None


async def create_deployment_row(
    session: AsyncSession,
    agent_id: uuid.UUID,
    version_id: uuid.UUID,
    environment: Environment,
    commit_sha: str | None = None,
    status: str = "active",
    workspace_enabled: bool | object = _WORKSPACE_UNSET,
) -> Deployment:
    resolved_workspace_enabled: bool
    if workspace_enabled is _WORKSPACE_UNSET:
        current = await get_active_deployment(session, agent_id, environment)
        resolved_workspace_enabled = current.workspace_enabled if current is not None else False
    else:
        assert isinstance(workspace_enabled, bool)
        resolved_workspace_enabled = workspace_enabled
    deployment = Deployment(
        agent_id=agent_id,
        # The agent's tenant, read inside the INSERT so there is no
        # read-then-write window (ADR 0166 decision 3).
        tenant_id=select(Agent.tenant_id).where(Agent.id == agent_id).scalar_subquery(),
        version_id=version_id,
        environment=environment,
        commit_sha=commit_sha,
        workspace_enabled=resolved_workspace_enabled,
        status=status,
    )
    session.add(deployment)
    await session.commit()
    await session.refresh(deployment)
    return deployment


async def create_deployment(session: AsyncSession, data: DeploymentCreate) -> Deployment:
    return await create_deployment_row(
        session,
        agent_id=data.agent_id,
        version_id=data.version_id,
        environment=data.environment,
        commit_sha=data.commit_sha,
        status=data.status,
        workspace_enabled=(
            data.workspace_enabled
            if "workspace_enabled" in data.model_fields_set
            else _WORKSPACE_UNSET
        ),
    )


async def get_active_deployment(
    session: AsyncSession, agent_id: uuid.UUID, environment: Environment
) -> Deployment | None:
    """The agent's current active deployment in an environment (most recent).

    Git-flow appends a new active Deployment row per push without superseding
    older ones, so "current" is the latest active row for the environment.
    """

    result: Deployment | None = await session.scalar(
        select(Deployment)
        .where(
            Deployment.agent_id == agent_id,
            Deployment.environment == environment,
            Deployment.status == "active",
        )
        .order_by(Deployment.deployed_at.desc())
        .limit(1)
    )
    return result


async def list_active_deployment_versions(
    session: AsyncSession, agent_id: uuid.UUID
) -> list[AgentVersion]:
    """Every DISTINCT version the agent has an active deployment of, one query.

    Deliberately NOT built on ``get_active_deployment`` (#2436). That helper
    returns only the NEWEST active row per environment, which is the right answer
    to "what is current" and the wrong set for "what can this write strand":
    git-flow appends a new active row per push without superseding older ones,
    ``end_deployment`` marks exactly one row stopped, and the worker's resolve
    query orders over ALL active rows
    (``apps/worker/src/curie_worker/binding.py``). So several active rows
    routinely coexist in one environment, and a check built on "newest per
    environment" would let an operator end the newest deployment and immediately
    unbind a route an older, still-active, still-bootable row declares.

    Rows sharing a version share one bundle object, so the join collapses them
    here rather than leaving the caller to de-duplicate a row list: each version
    comes back once, ordered by the earliest active row pointing at it.

    ``get_active_deployment`` is left unchanged: ``create_deployment_row``'s
    ``workspace_enabled`` inheritance depends on its "newest" semantics.
    """

    result = await session.scalars(
        select(AgentVersion)
        .join(Deployment, Deployment.version_id == AgentVersion.id)
        .where(Deployment.agent_id == agent_id, Deployment.status == "active")
        .group_by(AgentVersion.id)
        .order_by(func.min(Deployment.deployed_at))
    )
    return list(result)


async def list_deployments(
    session: AsyncSession, agent_id: uuid.UUID | None = None
) -> list[Deployment]:
    stmt = select(Deployment).order_by(Deployment.deployed_at)
    if agent_id is not None:
        stmt = stmt.where(Deployment.agent_id == agent_id)
    result = await session.scalars(stmt)
    return list(result)


async def get_deployment(session: AsyncSession, deployment_id: uuid.UUID) -> Deployment | None:
    return await session.get(Deployment, deployment_id)


async def end_deployment(session: AsyncSession, deployment: Deployment) -> None:
    deployment.status = "stopped"
    await session.commit()
