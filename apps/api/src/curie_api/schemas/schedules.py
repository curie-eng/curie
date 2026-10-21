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
    "daily_budget",
    "schedule_changed",
]
# What an agent schedule reads back as (ADR 0238): stored active, cancelled or
# done, with paused derived from ``schedule_controls`` while active.
AgentScheduleState = Literal["active", "paused", "cancelled", "done"]


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


class AgentScheduleDestinationOut(BaseModel):
    """The binding an agent schedule replies on, stored at create."""

    kind: str
    address: str
    adapter: str | None


class AgentScheduleOut(BaseModel):
    """One agent-created schedule (ADR 0238). ``conversation`` and ``turn`` are
    shown to the platform caller only; a sandbox response leaves them unset."""

    id: uuid.UUID
    agent: str
    agent_id: uuid.UUID
    revision: int
    state: AgentScheduleState
    cron: str | None
    run_at: datetime | None
    timezone: str
    prompt: str
    created_by: str
    created_source: Literal["agent", "operator"]
    destination: AgentScheduleDestinationOut
    next_run_at: datetime | None
    next_run_local: str | None
    paused: bool
    consecutive_failures: int
    last_fire_at: datetime | None
    last_outcome: ScheduleOutcome | None
    last_reason: HookRunReason | None
    created_at: datetime
    updated_at: datetime
    updated_by: str | None
    conversation: str | None = None
    turn: str | None = None


class ScheduleListOut(BaseModel):
    """Every in-force cron hook the platform can see, and every active agent
    schedule (ADR 0238)."""

    schedules: list[AgentSchedulesOut]
    agent_schedules: list[AgentScheduleOut]


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
