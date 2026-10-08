"""Database access for agents."""

import uuid
from typing import Any

from sqlalchemy import (
    delete,
    func,
    select,
    update,
)
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from curie_api.schemas.agents import AgentCreate, HookPartitionConfig, SourceBindingConfig

from ..models import (
    Agent,
    AgentChannel,
    AgentVersion,
    Deployment,
    WorkItem,
)
from ..publication_policy import PublicationPolicyConflict


async def refresh_with_channels(session: AsyncSession, agent: Agent) -> Agent:
    """Commit, then refresh the agent and name the `channels` relationship explicitly.

    The response model reads `agent.channels` after this session is done with,
    and an unloaded relationship RAISES under asyncio rather than lazy-loading,
    so the endpoint 500s where a crud-level test (holding a live session) passes.
    Naming the collection also re-reads it from the database, so a binding
    inserted or deleted around the relationship is reflected rather than served
    from the stale loaded collection (`expire_on_commit=False`).
    """
    await session.commit()
    await session.refresh(agent)
    await session.refresh(agent, ["channels"])
    return agent


async def create_agent(session: AsyncSession, data: AgentCreate) -> Agent:
    agent = Agent(
        name=data.name,
        # Attached through the relationship rather than inserted separately, so
        # the agent row and its binding are one transaction: a unique-constraint
        # collision on either rolls BOTH back, and no agent is ever left behind
        # bound to nothing (#38's silent-shadow state).
        # `endpoint`/`adapter` are the server-controlled reply route (ADR-0096
        # phase 2): for `slack` the identity alone (ADR-0168 decision 3), for
        # any other kind both NULL until configured or both set together. The
        # write schema has already refused any other shape, and
        # `agent_channels_route_ck` refuses one from an out-of-band writer.
        # A create binds exactly ONE channel (ADR-0118 keeps the create
        # singular); the rest arrive through `add_channel_binding`.
        channels=[
            AgentChannel(
                kind=data.channel.kind,
                address=data.channel.address,
                endpoint=data.channel.endpoint,
                adapter=data.channel.adapter,
            )
        ],
        repo_full_name=data.repo_full_name,
        deploy_notifications=data.deploy_notifications,
        model=data.model,
        reviewer_model=data.reviewer_model,
        thinking=data.thinking,
        behavior_packs=(
            data.behavior_packs.model_dump() if data.behavior_packs is not None else None
        ),
        approval_required_tools=data.approval_required_tools,
        approval_routes=(
            {name: b.model_dump() for name, b in data.approval_routes.items()}
            if data.approval_routes is not None
            else None
        ),
        hook_partitions=_stored_hook_partitions(data.hook_partitions),
        source_bindings=_stored_source_bindings(data.source_bindings),
        secrets=data.secrets,
        memory=data.memory,
        publication_policy=data.publication_policy,
        publication_draft=data.publication_draft,
        publication_branch_prefix=data.publication_branch_prefix,
    )
    session.add(agent)
    return await refresh_with_channels(session, agent)


async def list_agents(session: AsyncSession) -> list[Agent]:
    # `selectinload` explicitly, even though the relationship is already
    # lazy="selectin": the list path is the one where the per-row alternative is
    # correct AND unboundedly slow, so a hundred-agent install would pay a
    # hundred round trips to render one page.
    result = await session.scalars(
        select(Agent).options(selectinload(Agent.channels)).order_by(Agent.created_at)
    )
    return list(result)


async def get_agent(session: AsyncSession, agent_id: uuid.UUID) -> Agent | None:
    return await session.get(Agent, agent_id)


async def delete_agent(session: AsyncSession, agent_id: uuid.UUID) -> None:
    # Remove child rows first, then the agent. Bulk deletes bypass the ORM
    # relationship cascade (which would emit an async lazy-load during flush) and
    # match the FK ondelete=CASCADE already declared on every child table. Bundle
    # objects in RustFS are intentionally left in place (out of scope).
    await session.execute(delete(AgentChannel).where(AgentChannel.agent_id == agent_id))
    await session.execute(delete(WorkItem).where(WorkItem.agent_id == agent_id))
    await session.execute(delete(Deployment).where(Deployment.agent_id == agent_id))
    await session.execute(delete(AgentVersion).where(AgentVersion.agent_id == agent_id))
    await session.execute(delete(Agent).where(Agent.id == agent_id))
    await session.commit()


async def update_agent_model(session: AsyncSession, agent: Agent, model: str | None) -> Agent:
    agent.model = model
    await session.commit()
    await session.refresh(agent)
    return agent


async def update_agent_thinking(session: AsyncSession, agent: Agent, thinking: str | None) -> Agent:
    agent.thinking = thinking
    await session.commit()
    await session.refresh(agent)
    return agent


async def update_agent_execution_deadline(
    session: AsyncSession, agent: Agent, seconds: int | None
) -> Agent:
    agent.execution_deadline_seconds = seconds
    await session.commit()
    await session.refresh(agent)
    return agent


async def update_agent_max_turns(
    session: AsyncSession, agent: Agent, max_turns: int | None
) -> Agent:
    agent.max_turns = max_turns
    await session.commit()
    await session.refresh(agent)
    return agent


async def update_agent_runner_resources(
    session: AsyncSession, agent: Agent, resources: dict[str, Any] | None
) -> Agent:
    agent.runner_resources = resources
    await session.commit()
    await session.refresh(agent)
    return agent


async def update_agent_publication_policy(
    session: AsyncSession,
    agent: Agent,
    *,
    policy: str | None,
    draft: bool | None,
    branch_prefix: str | None,
    prefix_sent: bool,
) -> Agent:
    """Apply one operator publication-policy write and bump the version once.

    A request that names a field but does not change its stored value does not
    bump the version, so a repeated PATCH cannot revoke an in-flight approval.
    """

    values: dict[str, Any] = {}
    if policy is not None and policy != agent.publication_policy:
        values["publication_policy"] = policy
    if draft is not None and draft != agent.publication_draft:
        values["publication_draft"] = draft
    if prefix_sent and branch_prefix != agent.publication_branch_prefix:
        values["publication_branch_prefix"] = branch_prefix
    if not values:
        return agent
    expected_version = agent.publication_policy_version
    values["publication_policy_version"] = expected_version + 1
    # The version predicate is the compare-and-set. Two writers that read the
    # same version cannot both commit, so a publication created under the
    # winning version is revoked when the loser retries and bumps again.
    updated_version = await session.scalar(
        update(Agent)
        .where(
            Agent.id == agent.id,
            Agent.publication_policy_version == expected_version,
        )
        .values(**values)
        .returning(Agent.publication_policy_version)
    )
    if updated_version is None:
        await session.rollback()
        raise PublicationPolicyConflict("publication policy version changed; retry the read")
    await session.commit()
    await session.refresh(agent)
    return agent


async def update_agent_memory(session: AsyncSession, agent: Agent, memory: bool) -> Agent:
    """Set whether this agent's bindings share one workflow-state namespace
    (#1525 follow-up). Flipping it changes nothing already stored -- a row
    written under one scope is simply not the row a later request under the
    other scope reads; it is a routing decision for FUTURE state calls, not a
    migration of past ones."""

    agent.memory = memory
    await session.commit()
    await session.refresh(agent)
    return agent


async def update_agent_memory_writes(
    session: AsyncSession, agent: Agent, memory_writes: bool
) -> Agent:
    """Set whether the runner mounts its memory tools for this agent (#1461).
    Takes effect at the next sandbox boot; stored facts are untouched."""

    agent.memory_writes = memory_writes
    await session.commit()
    await session.refresh(agent)
    return agent


async def update_agent_approval_tools(
    session: AsyncSession, agent: Agent, tools: list[str]
) -> Agent:
    """Set the agent's permission gates (#245). An empty list clears them
    (stored as NULL, the no-gates posture)."""

    agent.approval_required_tools = tools or None
    await session.commit()
    await session.refresh(agent)
    return agent


async def update_agent_approval_routes(
    session: AsyncSession, agent: Agent, routes: dict[str, Any]
) -> Agent:
    """Set the agent's approval route bindings (#247). An empty dict clears
    them (stored as NULL: unbound routes escalate rather than inventing a
    resolution surface)."""

    agent.approval_routes = routes or None
    await session.commit()
    await session.refresh(agent)
    return agent


def _stored_hook_partitions(
    partitions: dict[str, HookPartitionConfig] | None,
) -> dict[str, Any] | None:
    """The column value for a hook-partition map (ADR-0134).

    One definition for both write paths, because create and PATCH must not
    disagree about what "no configuration" looks like in the column: an empty
    map is stored as NULL, the same "every hook returns to one thread per hook"
    posture as an omitted map, which is what an operator turning the feature
    off is asking for.
    """

    if not partitions:
        return None
    return {name: c.model_dump() for name, c in partitions.items()}


def _stored_source_bindings(
    bindings: dict[str, SourceBindingConfig] | None,
) -> dict[str, Any] | None:
    if not bindings:
        return None
    return {name: c.model_dump() for name, c in bindings.items()}


async def update_agent_source_bindings(
    session: AsyncSession, agent: Agent, bindings: dict[str, SourceBindingConfig]
) -> Agent:
    """Set the agent's workload-to-repository map (#2572). An empty dict clears it."""

    agent.source_bindings = _stored_source_bindings(bindings)
    await session.commit()
    await session.refresh(agent)
    return agent


async def update_agent_hook_partitions(
    session: AsyncSession, agent: Agent, partitions: dict[str, HookPartitionConfig]
) -> Agent:
    """Set which of the agent's hooks fan out (ADR-0134). An empty dict clears
    them (stored as NULL)."""

    agent.hook_partitions = _stored_hook_partitions(partitions)
    await session.commit()
    await session.refresh(agent)
    return agent


async def update_budget(
    session: AsyncSession,
    agent: Agent,
    max_usd_per_day: float | None,
    max_output_tokens_per_run: int | None,
) -> Agent:
    agent.max_usd_per_day = max_usd_per_day
    agent.max_output_tokens_per_run = max_output_tokens_per_run
    await session.commit()
    await session.refresh(agent)
    return agent


async def update_behavior_packs(
    session: AsyncSession, agent: Agent, behavior_packs: dict[str, Any] | None
) -> Agent:
    agent.behavior_packs = behavior_packs
    await session.commit()
    await session.refresh(agent)
    return agent


async def update_agent_secrets(
    session: AsyncSession, agent: Agent, secrets: dict[str, str] | None
) -> Agent:
    """Set the per-agent connector secrets (#429). An empty dict clears them."""
    agent.secrets = secrets
    await session.commit()
    await session.refresh(agent)
    return agent


async def get_agent_by_repo(session: AsyncSession, repo_full_name: str) -> Agent | None:
    agent: Agent | None = await session.scalar(
        select(Agent).where(Agent.repo_full_name == repo_full_name)
    )
    return agent


async def update_agent_repo(session: AsyncSession, agent: Agent, repo_full_name: str) -> Agent:
    agent.repo_full_name = repo_full_name
    await session.commit()
    await session.refresh(agent)
    return agent


async def update_agent_deploy_notifications(
    session: AsyncSession, agent: Agent, enabled: bool
) -> Agent:
    agent.deploy_notifications = enabled
    await session.commit()
    await session.refresh(agent)
    return agent


async def get_agents_by_repo(session: AsyncSession, repo_full_name: str) -> list[Agent]:
    """Every agent built from this repository (ADR-0091).

    One repository legitimately binds several agents -- a dev bot and a prod
    bot are the same bundle on two channels. Ordered by name so a caller that
    must pick one without a target (a bundle predating ``deploy.yaml``) picks
    the same one every time rather than whatever the planner returned.
    """

    result = await session.scalars(
        select(Agent).where(Agent.repo_full_name == repo_full_name).order_by(Agent.name)
    )
    return list(result)


async def get_agents_by_repo_casefold(session: AsyncSession, repo_full_name: str) -> list[Agent]:
    """Find a binding whose only difference from GitHub's name is ASCII casing."""

    result = await session.scalars(
        select(Agent)
        .where(func.lower(Agent.repo_full_name) == repo_full_name.lower())
        .order_by(Agent.name)
    )
    return list(result)


async def get_agent_by_name(session: AsyncSession, name: str) -> Agent | None:
    agent: Agent | None = await session.scalar(select(Agent).where(Agent.name == name))
    return agent


async def update_agent_reviewer_model(
    session: AsyncSession, agent: Agent, reviewer_model: str | None
) -> Agent:
    agent.reviewer_model = reviewer_model
    await session.commit()
    await session.refresh(agent)
    return agent
