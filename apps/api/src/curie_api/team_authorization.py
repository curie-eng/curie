"""Agent-wide caller and approver team authorization (Accepted ADR 0220)."""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from typing import Literal

from sqlalchemy import exists, select
from sqlalchemy.ext.asyncio import AsyncSession

from .identity.service import resolve_principal
from .identity.slack import SlackEvidence
from .models import (
    Agent,
    AgentAccessPolicy,
    AgentChannel,
    IdentityLink,
    IdentityNamespace,
    Principal,
    PrincipalTeam,
    Team,
    Tenant,
    TenantIdentityProvider,
)


@dataclass(frozen=True)
class TeamDecision:
    allowed: bool
    restricted: bool
    reason: str
    principal_id: uuid.UUID | None = None
    team_id: uuid.UUID | None = None
    epoch: int | None = None
    digest: str | None = None
    unlinked: bool = False


async def resolve_email(
    session: AsyncSession,
    *,
    tenant_id: uuid.UUID,
    email: str,
) -> uuid.UUID | None:
    """Resolve an exact mailbox only after the channel authenticated its sender.

    This function proves an IdP-owned mailbox link, not mail authentication.
    Its only intake callers already verified their channel capability and
    trust the adapter to authenticate the sender. Profile email is never used.
    """
    config = await session.get(TenantIdentityProvider, tenant_id)
    if (config is None or config.governed_domains is None
            or email != email.strip().lower() or email.count("@") != 1):
        return None
    domain = email.rsplit("@", 1)[1]
    if domain not in config.governed_domains:
        return None
    result = await session.scalar(
        select(Principal.id)
        .join(
            IdentityLink,
            IdentityLink.principal_id == Principal.id,
        )
        .join(IdentityNamespace, IdentityNamespace.id == IdentityLink.identity_namespace_id)
        .where(
            Principal.tenant_id == tenant_id,
            Principal.status == "active",
            Principal.type == "human",
            Principal.idp_issuer == config.issuer,
            IdentityLink.tenant_id == tenant_id,
            IdentityLink.provider_native_id == email,
            IdentityLink.revoked_at.is_(None),
            IdentityNamespace.tenant_id == tenant_id,
            IdentityNamespace.provider == "email",
            IdentityNamespace.authority == config.issuer,
            IdentityNamespace.kind == "email_domain",
            IdentityNamespace.key == domain,
            IdentityNamespace.status == "active",
        )
    )
    return result if isinstance(result, uuid.UUID) else None


async def check_binding(
    session: AsyncSession,
    binding: AgentChannel,
    *,
    role: Literal["invocation", "approver"],
    slack: SlackEvidence | None = None,
    email: str | None = None,
    principal_id: uuid.UUID | None = None,
) -> TeamDecision:
    """Check the role's current team; a role with no team retains legacy policy."""
    agent = await session.get(Agent, binding.agent_id, populate_existing=True)
    if agent is None or agent.tenant_id != binding.tenant_id:
        return TeamDecision(False, True, "agent_unavailable")
    policy = await session.get(AgentAccessPolicy, agent.id, populate_existing=True)
    team_id = (
        getattr(policy, "invocation_team_id" if role == "invocation" else "approver_team_id")
        if policy is not None
        else None
    )
    if team_id is None:
        return TeamDecision(True, False, "unrestricted")
    tenant = await session.get(Tenant, agent.tenant_id, populate_existing=True)
    if tenant is None or tenant.status != "active" or agent.status != "active":
        return TeamDecision(False, True, "inactive_agent_or_tenant", team_id=team_id)
    unlinked = False
    if principal_id is None and binding.kind == "slack":
        resolved = await resolve_principal(
            session,
            tenant_id=agent.tenant_id,
            provider="slack",
            channel_identity=binding.adapter or "default",
            slack=slack,
        )
        principal_id = resolved.principal_id
        unlinked = resolved.reason == "no_link"
    elif principal_id is None and binding.kind == "email" and email:
        principal_id = await resolve_email(session, tenant_id=agent.tenant_id, email=email)
    if principal_id is None:
        return TeamDecision(False, True, "unresolved", team_id=team_id, unlinked=unlinked)
    config = await session.get(TenantIdentityProvider, agent.tenant_id, populate_existing=True)
    person = await session.get(Principal, principal_id, populate_existing=True)
    if (
        person is None
        or person.tenant_id != agent.tenant_id
        or person.status != "active"
        or person.type != "human"
        or config is None
        or person.idp_issuer != config.issuer
    ):
        return TeamDecision(False, True, "principal_inactive", principal_id, team_id)
    team = await session.get(Team, team_id, populate_existing=True)
    if (
        team is None
        or team.tenant_id != agent.tenant_id
        or team.source != "idp_group"
        or team.managed_by != config.managed_by
        or team.epoch < 1
        or team.last_synced_at is None
    ):
        return TeamDecision(False, True, "team_not_ready", principal_id, team_id)
    member = bool(
        await session.scalar(
            select(
                exists().where(
                    PrincipalTeam.principal_id == principal_id,
                    PrincipalTeam.team_id == team_id,
                    PrincipalTeam.tenant_id == agent.tenant_id,
                    PrincipalTeam.source == "idp_group",
                )
            )
        )
    )
    return TeamDecision(
        member,
        True,
        "member" if member else "not_member",
        principal_id,
        team_id,
        team.epoch,
        team.digest,
    )


async def any_invocation_team(session: AsyncSession) -> bool:
    return bool(
        await session.scalar(
            select(
                exists().where(
                    AgentAccessPolicy.invocation_team_id.is_not(None),
                )
            )
        )
    )
