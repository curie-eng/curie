"""Freeze route deadlines at creation. @spec APPROVAL-SCHEDULE-1."""

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from aci_protocol import ApprovalRequest
from fastapi import HTTPException
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from .models import Agent
from .schemas.approvals import ApprovalRouteBinding


@dataclass(frozen=True)
class ApprovalSchedule:
    created_at: datetime
    expires_at: datetime | None


async def resolve_route_schedule(session: AsyncSession, data: ApprovalRequest) -> ApprovalSchedule:
    duration = data.expires_in_seconds
    if data.route:
        agent = (
            await session.get(Agent, data.agent_id, populate_existing=True)
            if data.agent_id is not None
            else None
        )
        routes = agent.approval_routes if agent is not None else None
        try:
            if not isinstance(routes, dict) or data.route not in routes:
                raise ValueError("missing route")
            binding = ApprovalRouteBinding.model_validate(routes[data.route])
        except (TypeError, ValueError) as exc:
            raise HTTPException(422, "approval route is missing or malformed") from exc
        if "expires_in_seconds" in binding.model_fields_set:
            duration = binding.expires_in_seconds
    now = await session.scalar(select(func.clock_timestamp()))
    assert isinstance(now, datetime)
    created_at = now.astimezone(UTC).replace(tzinfo=None)
    try:
        expires_at = created_at + timedelta(seconds=duration) if duration is not None else None
    except OverflowError as exc:
        raise HTTPException(422, "approval deadline exceeds the supported timestamp range") from exc
    return ApprovalSchedule(created_at=created_at, expires_at=expires_at)
