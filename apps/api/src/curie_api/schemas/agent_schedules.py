"""Agent-created schedules (ADR 0248): request bodies and list wrappers.

The schedule itself (``AgentScheduleOut``) lives in ``schemas.schedules`` beside
the hook-run reason set it reports, because ``GET /schedules`` lists it too.
Content rules (prompt length, zone, cron, run_at, minimum interval) are judged
by the router after the caller is authorized.
"""

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, StrictBool

from .schedules import AgentScheduleOut, AgentScheduleState


class AgentScheduleDestination(BaseModel):
    """The binding an operator-created schedule replies on. ``adapter`` omitted
    resolves like a turn's binding: the one row, or the default Slack identity."""

    model_config = ConfigDict(extra="forbid")

    kind: str
    address: str
    adapter: str | None = None


class AgentScheduleCreate(BaseModel):
    """Exactly one of ``cron`` and ``run_at``. ``run_at`` with an offset is that
    instant; without one it is wall time in ``timezone`` (the agent's zone when
    omitted). ``destination`` and ``idempotency_key`` are for the operator: a
    sandbox schedule replies on its turn's binding and is keyed by its turn."""

    model_config = ConfigDict(extra="forbid")

    prompt: str
    cron: str | None = None
    run_at: str | None = None
    timezone: str | None = None
    destination: AgentScheduleDestination | None = None
    idempotency_key: str | None = None


class AgentScheduleUpdate(BaseModel):
    """A compare-and-set change. A prompt, cron, run_at or timezone change moves
    the revision; ``paused`` is logged without moving it. ``cron`` and
    ``run_at`` are exclusive: giving one replaces the other."""

    model_config = ConfigDict(extra="forbid")

    expected_revision: int
    prompt: str | None = None
    cron: str | None = None
    run_at: str | None = None
    timezone: str | None = None
    paused: StrictBool | None = None


class AgentScheduleCancel(BaseModel):
    model_config = ConfigDict(extra="forbid")

    expected_revision: int | None = None


class AgentScheduleRevert(BaseModel):
    """Restore revision ``to_revision``'s configuration as a new revision;
    omitted is the revision before the current one."""

    model_config = ConfigDict(extra="forbid")

    expected_revision: int
    to_revision: int | None = None


class AgentScheduleList(BaseModel):
    """The agent's schedule switch and limits, and its schedules."""

    enabled: bool
    timezone: str
    max_active: int
    min_interval_minutes: int
    schedules: list[AgentScheduleOut]


class AgentScheduleRevisionOut(BaseModel):
    seq: int
    revision: int
    action: Literal["create", "update", "pause", "resume", "cancel", "revert", "complete"]
    state: AgentScheduleState
    prompt: str
    cron: str | None
    run_at: datetime | None
    timezone: str
    author: str
    source: Literal["agent", "operator", "scheduler"]
    conversation: str | None
    turn: str | None
    created_at: datetime


class AgentScheduleRevisions(BaseModel):
    revisions: list[AgentScheduleRevisionOut]
