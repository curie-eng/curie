"""Read and replace an agent's access policy (#4585, ADR 0220 section 8).

Storage and administration only: nothing consults the policy for an
authorization decision yet. A role may name only an IdP group team of the
agent's own tenant (ADR 0220 section 1). A write names the version it last
read and is refused when another write landed first; the agent row is locked
for the duration, so the first write for an agent cannot race another.
"""

import uuid

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from .models import Agent, AgentAccessPolicy, Team
from .schemas.agent_access import AgentAccessPolicyOut, AgentAccessPolicyWrite


class PolicyConflict(Exception):
    """The expected version is not the stored one."""


class PolicyInvalid(ValueError):
    """A named team cannot serve this agent's policy."""


def _out(agent: Agent, policy: AgentAccessPolicy | None) -> AgentAccessPolicyOut:
    if policy is None:
        return AgentAccessPolicyOut(
            agent_id=agent.id,
            tenant_id=agent.tenant_id,
            invocation_team_id=None,
            approver_team_id=None,
            version=0,
            updated_at=None,
        )
    return AgentAccessPolicyOut(
        agent_id=policy.agent_id,
        tenant_id=policy.tenant_id,
        invocation_team_id=policy.invocation_team_id,
        approver_team_id=policy.approver_team_id,
        version=policy.version,
        updated_at=policy.updated_at,
    )


async def read_policy(session: AsyncSession, agent: Agent) -> AgentAccessPolicyOut:
    return _out(agent, await session.get(AgentAccessPolicy, agent.id))


async def _require_idp_team(
    session: AsyncSession, tenant_id: uuid.UUID, team_id: uuid.UUID | None, role: str
) -> None:
    if team_id is None:
        return
    team = await session.scalar(
        select(Team).where(Team.id == team_id, Team.tenant_id == tenant_id)
    )
    if team is None or team.source != "idp_group":
        raise PolicyInvalid(f"{role} must be an IdP group team of the agent's tenant")


async def replace_policy(
    session: AsyncSession, agent_id: uuid.UUID, data: AgentAccessPolicyWrite
) -> AgentAccessPolicyOut | None:
    """Replace the policy, or return None when the agent does not exist.

    Commits on success; raises PolicyConflict or PolicyInvalid with nothing
    written.
    """

    agent = await session.scalar(select(Agent).where(Agent.id == agent_id).with_for_update())
    if agent is None:
        return None
    policy = await session.get(AgentAccessPolicy, agent_id, with_for_update=True)
    stored = policy.version if policy is not None else 0
    if data.expected_version != stored:
        raise PolicyConflict(
            f"version mismatch: expected {data.expected_version}, stored {stored}"
        )
    await _require_idp_team(session, agent.tenant_id, data.invocation_team_id, "invocation team")
    await _require_idp_team(session, agent.tenant_id, data.approver_team_id, "approver team")
    if policy is None:
        policy = AgentAccessPolicy(agent_id=agent.id, tenant_id=agent.tenant_id, version=1)
        session.add(policy)
    else:
        policy.version = stored + 1
        policy.updated_at = func.now()  # type: ignore[assignment]
    policy.invocation_team_id = data.invocation_team_id
    policy.approver_team_id = data.approver_team_id
    await session.flush()
    await session.refresh(policy)
    await session.commit()
    return _out(agent, policy)
