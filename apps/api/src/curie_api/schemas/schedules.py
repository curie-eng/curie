import uuid
from datetime import datetime
from typing import Literal

from pydantic import BaseModel

HookRunSource = Literal["schedule", "manual"]
ScheduleOutcome = Literal["ran", "deferred", "skipped", "blocked", "reclaimed", "failed"]
# Closed set pinned by tests/vectors/hook-run-reasons.json. The worker Literal
# is the other side. NULL means ran, in flight, or a row from before the column.
HookRunReason = Literal[
    "turn_error",
    "target_unbound",
    "approval_gate_targetless",
    "agent_killed",
    "budget_exhausted",
    "run_in_flight",
    "catch_up_expired",
    "deferred_expired",
    "reply_undeliverable",
    "prior_side_effect",
    "deployment_missing",
    "hook_paused",
    "live_session",
    "enqueue_failed",
    "claim_expired",
]


class ScheduleHookOut(BaseModel):
    """One cron hook with its newest scheduled and manual run histories."""

    name: str
    trigger: str
    schedule: str
    zone: str
    last_fire_at: datetime | None
    last_outcome: ScheduleOutcome | None
    last_reason: HookRunReason | None = None
    last_manual_fire_at: datetime | None
    last_manual_outcome: ScheduleOutcome | None
    last_manual_reason: HookRunReason | None
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
    source: HookRunSource
    slot_utc: datetime
    outcome: ScheduleOutcome | None
    reason: HookRunReason | None = None
    started_at: datetime
    ended_at: datetime | None
