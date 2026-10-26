"""Read and replace an agent's access policy (#4585, ADR 0220 section 8).

@spec AGENT-ROLE-PERSISTENCE-V1-1/4/6. Existing team consumers live in
team_authorization, admission and approval_teams; new role filters have no
runtime consumer in this slice. Outer callers preserve the declared lock order.
"""

import uuid

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from .agent_role_policy import RolePolicyRefusal, lock_policy_rows, stored_snapshot
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
    team = await session.scalar(select(Team).where(Team.id == team_id, Team.tenant_id == tenant_id))
    if team is None or team.source != "idp_group":
        raise PolicyInvalid(f"{role} must be an IdP group team of the agent's tenant")


async def replace_policy(
    session: AsyncSession, agent_id: uuid.UUID, data: AgentAccessPolicyWrite
) -> AgentAccessPolicyOut | None:
    """@spec AGENT-ROLE-PERSISTENCE-V1-4/6."""

    # @spec AGENT-ROLE-PERSISTENCE-V1-4/6: caller work precedes the operation savepoint.
    await session.flush()
    async with session.begin_nested():
        with session.no_autoflush:
            tenant_id = await session.scalar(select(Agent.tenant_id).where(Agent.id == agent_id))
        if tenant_id is None:
            return None
        try:
            agent, policy, sql_null = await lock_policy_rows(
                session,
                tenant_id=tenant_id,
                agent_id=agent_id,
                require_active=False,
            )
            if agent is None:
                return None
            snapshot = stored_snapshot(policy, sql_null, tenant_id=tenant_id, agent_id=agent_id)
        except RolePolicyRefusal:
            raise PolicyConflict("invalid_role_policy_state") from None
        if snapshot is not None and any(
            role is not None and role.legacy_state == "migrated"
            for role in (snapshot.roles.invocation, snapshot.roles.approver)
        ):
            raise PolicyConflict("agent_role_policy_migrated") from None
        stored = policy.version if policy is not None else 0
        if data.expected_version != stored:
            raise PolicyConflict(
                f"version mismatch: expected {data.expected_version}, stored {stored}"
            )
        if stored == 2_147_483_647:
            raise PolicyConflict("role_policy_version_exhausted") from None
        await _require_idp_team(
            session, agent.tenant_id, data.invocation_team_id, "invocation team"
        )
        await _require_idp_team(session, agent.tenant_id, data.approver_team_id, "approver team")
        if policy is None:
            policy = AgentAccessPolicy(agent_id=agent.id, tenant_id=agent.tenant_id, version=1)
            session.add(policy)
        else:
            policy.version = stored + 1
            policy.updated_at = func.now()
        policy.invocation_team_id = data.invocation_team_id
        policy.approver_team_id = data.approver_team_id
        await session.flush()
        await session.refresh(policy)
        result = _out(agent, policy)
    await session.commit()
    return result
