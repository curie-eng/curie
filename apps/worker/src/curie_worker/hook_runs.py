"""Persist terminal outcomes for scheduled hook turns."""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Literal

from aci_protocol import HookRunRef
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncEngine

# "blocked" is the kill-switch outcome for a targetless run (#2963, ADR-0099);
# migration 0048 already allows it.
HookRunOutcome = Literal["ran", "failed", "blocked"]


class HookRunRecorderError(RuntimeError):
    """A hook run key or persistence operation could not be accepted."""

    def __init__(
        self,
        message: str,
        *,
        code: Literal["invalid_ref", "missing", "backend"] = "backend",
    ) -> None:
        super().__init__(message)
        self.code = code


@dataclass(frozen=True)
class HookRunState:
    """The stored state for one validated hook run key."""

    agent_id: uuid.UUID
    name: str
    slot_utc: datetime
    outcome: str | None


@dataclass(frozen=True)
class _HookRunKey:
    agent_id: uuid.UUID
    name: str
    slot_utc: datetime


def _parse_ref(ref: HookRunRef) -> _HookRunKey:
    try:
        agent_id = uuid.UUID(ref.agent_id)
    except (AttributeError, ValueError) as exc:
        raise HookRunRecorderError(
            "hook run agent_id is not a UUID",
            code="invalid_ref",
        ) from exc
    try:
        slot_utc = datetime.fromisoformat(ref.slot_utc)
    except (AttributeError, ValueError) as exc:
        raise HookRunRecorderError(
            "hook run slot_utc is not an ISO 8601 datetime",
            code="invalid_ref",
        ) from exc
    if slot_utc.tzinfo is None or slot_utc.utcoffset() != timedelta(0):
        raise HookRunRecorderError(
            "hook run slot_utc must have a UTC offset of zero",
            code="invalid_ref",
        )
    return _HookRunKey(
        agent_id=agent_id,
        name=ref.name,
        slot_utc=slot_utc.astimezone(UTC),
    )


class HookRunRecorder:
    """Read and close hook run rows owned by the API schema."""

    def __init__(self, engine: AsyncEngine) -> None:
        self._engine = engine

    async def get(self, ref: HookRunRef) -> HookRunState | None:
        """Return the exact run row, or None when the key is absent."""
        key = _parse_ref(ref)
        try:
            async with self._engine.connect() as connection:
                row = (
                    await connection.execute(
                        text(
                            "SELECT outcome FROM curie.hook_runs "
                            "WHERE agent_id = :agent_id "
                            "AND name = :name AND slot_utc = :slot_utc"
                        ),
                        {
                            "agent_id": key.agent_id,
                            "name": key.name,
                            "slot_utc": key.slot_utc,
                        },
                    )
                ).one_or_none()
        except SQLAlchemyError as exc:
            raise HookRunRecorderError(
                "hook run state could not be read",
                code="backend",
            ) from exc
        if row is None:
            return None
        return HookRunState(
            agent_id=key.agent_id,
            name=key.name,
            slot_utc=key.slot_utc,
            outcome=row.outcome,
        )

    async def close(self, ref: HookRunRef, outcome: HookRunOutcome) -> None:
        """Set one open run terminally without overwriting an earlier outcome."""
        key = _parse_ref(ref)
        try:
            async with self._engine.begin() as connection:
                updated = (
                    await connection.execute(
                        text(
                            "UPDATE curie.hook_runs "
                            "SET outcome = :outcome, ended_at = now() "
                            "WHERE agent_id = :agent_id "
                            "AND name = :name AND slot_utc = :slot_utc "
                            "AND outcome IS NULL RETURNING outcome"
                        ),
                        {
                            "agent_id": key.agent_id,
                            "name": key.name,
                            "slot_utc": key.slot_utc,
                            "outcome": outcome,
                        },
                    )
                ).one_or_none()
                if updated is not None:
                    return
                existing = (
                    await connection.execute(
                        text(
                            "SELECT outcome FROM curie.hook_runs "
                            "WHERE agent_id = :agent_id "
                            "AND name = :name AND slot_utc = :slot_utc"
                        ),
                        {
                            "agent_id": key.agent_id,
                            "name": key.name,
                            "slot_utc": key.slot_utc,
                        },
                    )
                ).one_or_none()
                if existing is None:
                    raise HookRunRecorderError(
                        "hook run row does not exist",
                        code="missing",
                    )
        except HookRunRecorderError:
            raise
        except SQLAlchemyError as exc:
            raise HookRunRecorderError(
                "hook run outcome could not be stored",
                code="backend",
            ) from exc
