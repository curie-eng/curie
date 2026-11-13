"""Cached observations and metadata-only delivery records for ADR 0280."""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Literal

from sqlalchemy.ext.asyncio import AsyncSession

from .factory_facts_schema import Reason
from .models import FactoryFactObservation


@dataclass(frozen=True)
class Observation:
    state: str
    reason: Reason | None
    observed_at: datetime | None
    expires_at: datetime | None
    subject_digest: str | None


async def read_observation(
    session: AsyncSession,
    *,
    kind: Literal["tool", "model_credit"],
    agent_id: uuid.UUID,
    scope: str,
    item: str,
    now: datetime | None = None,
) -> Observation:
    """Return the current cached row; this read never probes or writes."""
    row = await session.get(FactoryFactObservation, (kind, agent_id, scope, item))
    if row is None:
        return Observation("unknown", "observation_missing", None, None, None)
    if row.expires_at <= (now or datetime.now(UTC)):
        return Observation("unknown", "observation_expired", None, None, row.subject_digest)
    return Observation(row.state, row.reason, row.observed_at, row.expires_at, row.subject_digest)  # type: ignore[arg-type]
