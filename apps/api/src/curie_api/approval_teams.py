"""Shared current team authority for approvals and gated undo rulings."""

from __future__ import annotations

import uuid
from collections.abc import Mapping
from dataclasses import replace

from sqlalchemy.ext.asyncio import AsyncSession

from .approval_auth import AuthenticatedApprovalPrincipal
from .approvers import shown_off_slack
from .crud import approvals as crud_approvals
from .models import Agent, AgentChannel, Approval
from .slack_approvers import approval_token_identity
from .team_authorization import TeamDecision, check_binding


async def approval_team_decision(
    session: AsyncSession,
    approval: Approval,
    principal: AuthenticatedApprovalPrincipal,
    *,
    agent_id: uuid.UUID | None = None,
) -> TeamDecision:
    owner_id = agent_id if agent_id is not None else approval.agent_id
    if owner_id is None:
        return TeamDecision(True, False, "unrestricted")
    agent = await session.get(Agent, owner_id, populate_existing=True)
    if agent is None:
        return TeamDecision(False, True, "agent_unavailable")
    # This is the authenticated actor's receiving context, never inserted as
    # an agent binding. A fixed Slack card can belong to an email-originated
    # request; its Slack installation is approval_token_identity, not mail's.
    context = AgentChannel(
        agent_id=agent.id,
        tenant_id=agent.tenant_id,
        kind="email" if principal.kind == "adapter" and approval.reply_kind == "email" else "slack",
        adapter=(
            approval.reply_adapter
            if principal.kind == "adapter"
            else approval_token_identity(approval)
        ),
        address=principal.actor_channel or approval.reply_channel,
    )
    decision = await check_binding(
        session,
        context,
        role="approver",
        slack=principal.slack if principal.kind == "chat" else None,
        email=(
            principal.subject
            if principal.kind == "adapter" and approval.reply_kind == "email"
            else None
        ),
        principal_id=principal.principal_id if principal.kind == "console" else None,
    )
    if decision.restricted and principal.kind in {"operator", "test_driver"}:
        return TeamDecision(
            False,
            True,
            "idp_sign_in_required",
            team_id=decision.team_id,
            epoch=decision.epoch,
            digest=decision.digest,
        )
    if decision.restricted:
        binding = await crud_approvals.get_approval_route_binding(session, approval)
        off_slack = shown_off_slack(approval, binding)
        if (
            principal.kind == "adapter"
            and not (approval.reply_kind == "email" and off_slack)
            or principal.kind == "chat"
            and off_slack
        ):
            return replace(decision, allowed=False, reason="wrong_resolution_surface")
    return decision


def team_restriction(approval: Approval, binding: object) -> bool:
    """Only explicit route restrictions narrow the team; no channel default."""
    if approval.route and binding is None:
        return True
    if binding is not None and not isinstance(binding, Mapping):
        return True
    return isinstance(binding, Mapping) and binding.get("approvers") is not None


def authenticated_actor_evidence(principal: AuthenticatedApprovalPrincipal) -> dict[str, object]:
    """Retain only already authenticated transport proof for recovery's reread."""
    proof: dict[str, object] = {"kind": principal.kind}
    if principal.kind == "chat" and principal.slack is not None:
        proof["slack"] = principal.slack.model_dump(mode="json", exclude_none=True)
    if principal.kind == "console" and principal.principal_id is not None:
        proof["principal_id"] = str(principal.principal_id)
    if principal.kind == "adapter":
        proof["adapter_bindings"] = sorted(str(value) for value in principal.adapter_bindings)
    return proof
