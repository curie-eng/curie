"""Team directory sync status (#4584, ADR 0220 sections 5 and 6).

What an operator reads to tell whether a team's directory adapter is healthy:
which snapshot the team holds and how the last attempt went. No member list,
no credential. ``synced`` and ``sync_age_seconds`` are derived for alerting;
neither is an authorization input.
"""

import uuid
from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict

TeamSource = Literal["idp_group", "curie_managed"]


class TeamSyncOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    tenant_id: uuid.UUID
    source: TeamSource
    external_id: str | None
    name: str
    managed_by: str | None
    epoch: int
    member_count: int
    digest: str | None
    last_synced_at: datetime | None
    last_attempt_at: datetime | None
    last_error: str | None
    # A complete snapshot was applied at least once (epoch > 0).
    synced: bool
    # Seconds since the last applied snapshot; None when never synced.
    sync_age_seconds: float | None
