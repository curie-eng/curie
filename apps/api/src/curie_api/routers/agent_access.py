"""Read and set an agent's invocation and approver teams (#4585, ADR 0220 s.8).

Under the same credential as the rest of ``/agents`` (``require_api_key``),
like the per-binding caller list it will sit beside (ADR 0175). Nothing here
enforces the policy yet.
"""

import uuid

from fastapi import APIRouter, Depends, HTTPException, status

from .. import agent_access
from ..auth import require_api_key
from ..crud import agents as crud_agents
from ..deps import SessionDep
from ..schemas.agent_access import AgentAccessPolicyOut, AgentAccessPolicyWrite

router = APIRouter(
    prefix="/agents",
    tags=["agents"],
    dependencies=[Depends(require_api_key)],
)


@router.get("/{agent_id}/access-policy", response_model=AgentAccessPolicyOut)
async def get_agent_access_policy(agent_id: uuid.UUID, session: SessionDep) -> AgentAccessPolicyOut:
    agent = await crud_agents.get_agent(session, agent_id)
    if agent is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "agent not found")
    return await agent_access.read_policy(session, agent)


@router.put("/{agent_id}/access-policy", response_model=AgentAccessPolicyOut)
async def put_agent_access_policy(
    agent_id: uuid.UUID, data: AgentAccessPolicyWrite, session: SessionDep
) -> AgentAccessPolicyOut:
    try:
        result = await agent_access.replace_policy(session, agent_id, data)
    except agent_access.PolicyConflict as exc:
        raise HTTPException(status.HTTP_409_CONFLICT, str(exc)) from None
    except agent_access.PolicyInvalid as exc:
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_ENTITY, str(exc)) from None
    if result is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "agent not found")
    return result
