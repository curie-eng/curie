import uuid
from datetime import datetime
from typing import Literal

from pydantic import BaseModel

ScheduleOutcome = Literal["ran", "deferred", "skipped", "blocked", "reclaimed", "failed"]


class ScheduleHookOut(BaseModel):
    """One cron hook on the in-force bundle, with its newest slot."""

    name: str
    trigger: str
    schedule: str
    zone: str
    last_fire_at: datetime | None
    last_outcome: ScheduleOutcome | None
    paused: bool


class ScheduleControlOut(BaseModel):
    """Current operator pause state for a named cron hook."""

    agent: str
    name: str
    paused: bool


class AgentSchedulesOut(BaseModel):
    """The scheduled hooks of one agent's in-force deployment."""

    agent: str
    agent_id: uuid.UUID
    bundle_error: str | None
    hooks: list[ScheduleHookOut]


class ScheduleListOut(BaseModel):
    """Every in-force cron hook the platform can see."""

    schedules: list[AgentSchedulesOut]


class HookFireOut(BaseModel):
    """One test-fire run record. `outcome` is null while the turn is in flight."""

    id: uuid.UUID
    agent_id: uuid.UUID
    agent: str
    name: str
    trigger: str
    slot_utc: datetime
    outcome: ScheduleOutcome | None
    started_at: datetime
    ended_at: datetime | None
