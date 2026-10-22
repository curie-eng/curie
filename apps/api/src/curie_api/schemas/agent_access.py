"""An agent's access policy: its invocation and approver teams (#4585, ADR 0220 s.8)."""

import uuid
from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field


class AgentAccessPolicyWrite(BaseModel):
    """Replace an agent's policy.

    Both team keys are required so a write states the whole policy; ``null``
    clears that role back to today's behavior. ``expected_version`` is the
    version the writer last read: 0 when the agent has no policy yet.
    """

    model_config = ConfigDict(extra="forbid")

    invocation_team_id: uuid.UUID | None
    approver_team_id: uuid.UUID | None
    expected_version: int = Field(ge=0)


class AgentAccessPolicyOut(BaseModel):
    """An agent's policy; version 0 and no ``updated_at`` mean none was set."""

    agent_id: uuid.UUID
    tenant_id: uuid.UUID
    invocation_team_id: uuid.UUID | None
    approver_team_id: uuid.UUID | None
    version: int
    updated_at: datetime | None
