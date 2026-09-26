"""Persist terminal outcomes for scheduled hook turns."""

from __future__ import annotations

import logging
import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Literal

from aci_protocol import HookRunRef
from curie_telemetry import record_metric
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncEngine

# "blocked" is the kill-switch outcome for a targetless run (#2963, ADR-0099);
# migration 0048 already allows it. "deferred" is a fire that met a live session
# on its thread (#2929); the scheduler reopens it on a later tick.
HookRunOutcome = Literal["ran", "failed", "blocked", "deferred", "skipped"]

logger = logging.getLogger(__name__)

_RETRY_MARK = "retry"


def retry_event_id(base: str, expires_at: datetime) -> str:
    """A deferred slot's retry id: fresh per retry, carrying its catch-up expiry.

    The deferred delivery is marked done under its own id, so a retry needs one
    that marker cannot match. The expiry lets the kernel refuse a retry that sat
    in the stream past the slot's catch-up bound (#2929).
    """

    return f"{base}:{_RETRY_MARK}:{int(expires_at.timestamp())}:{uuid.uuid4().hex}"


def retry_expiry(event_id: str) -> datetime | None:
    """The catch-up expiry a retry id carries, or None for a first fire."""

    parts = event_id.rsplit(":", 3)
    if len(parts) != 4 or parts[1] != _RETRY_MARK or not parts[2].isdigit():
        return None
    return datetime.fromtimestamp(int(parts[2]), UTC)


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

    @asynccontextmanager
    async def start_guard(self, ref: HookRunRef) -> AsyncIterator[bool]:
        """Serialize runner admission with an operator pause of this hook."""

        key = _parse_ref(ref)
        try:
            async with self._engine.begin() as connection:
                await connection.execute(
                    text(
                        "SELECT pg_advisory_xact_lock("
                        "hashtextextended(CAST(:agent_id AS text) || ':' || :name, 0))"
                    ),
                    {"agent_id": str(key.agent_id), "name": key.name},
                )
                allowed = (
                    await connection.execute(
                        text(
                            "SELECT r.outcome IS NULL AND c.paused_at IS NULL "
                            "FROM curie.hook_runs r "
                            "LEFT JOIN curie.schedule_controls c "
                            "ON c.agent_id = r.agent_id AND c.name = r.name "
                            "WHERE r.agent_id = :agent_id AND r.name = :name "
                            "AND r.slot_utc = :slot_utc"
                        ),
                        {
                            "agent_id": key.agent_id,
                            "name": key.name,
                            "slot_utc": key.slot_utc,
                        },
                    )
                ).scalar_one_or_none()
                yield allowed is True
        except SQLAlchemyError as exc:
            raise HookRunRecorderError(
                "hook run start control could not be read", code="backend"
            ) from exc

    async def get(self, ref: HookRunRef) -> HookRunState | None:
        """Return the exact run row, or None when the key is absent."""
        key = _parse_ref(ref)
        try:
            async with self._engine.begin() as connection:
                row = (
                    await connection.execute(
                        text(
                            "SELECT r.outcome, c.paused_at IS NOT NULL AS paused "
                            "FROM curie.hook_runs r "
                            "LEFT JOIN curie.schedule_controls c "
                            "ON c.agent_id = r.agent_id AND c.name = r.name "
                            "WHERE r.agent_id = :agent_id "
                            "AND r.name = :name AND r.slot_utc = :slot_utc"
                        ),
                        {
                            "agent_id": key.agent_id,
                            "name": key.name,
                            "slot_utc": key.slot_utc,
                        },
                    )
                ).one_or_none()
                outcome = None if row is None else row.outcome
                if row is not None and row.paused and outcome is None:
                    settled = (
                        await connection.execute(
                            text(
                                "UPDATE curie.hook_runs SET outcome = 'deferred', ended_at = now() "
                                "WHERE agent_id = :agent_id AND name = :name "
                                "AND slot_utc = :slot_utc AND outcome IS NULL RETURNING outcome"
                            ),
                            {
                                "agent_id": key.agent_id,
                                "name": key.name,
                                "slot_utc": key.slot_utc,
                            },
                        )
                    ).scalar_one_or_none()
                    if settled is not None:
                        outcome = settled
                    else:
                        outcome = (
                            await connection.execute(
                                text(
                                    "SELECT outcome FROM curie.hook_runs "
                                    "WHERE agent_id = :agent_id AND name = :name "
                                    "AND slot_utc = :slot_utc"
                                ),
                                {
                                    "agent_id": key.agent_id,
                                    "name": key.name,
                                    "slot_utc": key.slot_utc,
                                },
                            )
                        ).scalar_one()
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
            outcome=outcome,
        )

    async def renew(self, ref: HookRunRef, lease_s: float) -> bool:
        """Extend an open run's claim lease to at least ``lease_s`` from now.

        Returns False when the run is no longer open, for example because the
        hook's next fire reclaimed it after this worker read it (#2931).
        """
        key = _parse_ref(ref)
        try:
            async with self._engine.begin() as connection:
                renewed = (
                    await connection.execute(
                        text(
                            "UPDATE curie.hook_runs SET lease_expires_at = GREATEST("
                            "lease_expires_at, now() + make_interval(secs => :lease_s)) "
                            "WHERE agent_id = :agent_id "
                            "AND name = :name AND slot_utc = :slot_utc "
                            "AND outcome IS NULL RETURNING id"
                        ),
                        {
                            "agent_id": key.agent_id,
                            "name": key.name,
                            "slot_utc": key.slot_utc,
                            "lease_s": lease_s,
                        },
                    )
                ).one_or_none()
        except SQLAlchemyError as exc:
            raise HookRunRecorderError(
                "hook run lease could not be renewed",
                code="backend",
            ) from exc
        return renewed is not None

    async def close(self, ref: HookRunRef, outcome: HookRunOutcome) -> None:
        """Set one open run terminally without overwriting an earlier outcome."""
        key = _parse_ref(ref)
        closed = False
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
                    closed = True
                else:
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
        if closed:
            try:
                record_metric(
                    "curie.schedule.fire",
                    attributes={
                        "service.name": "curie-worker",
                        "trigger": "cron",
                        "outcome": outcome,
                    },
                )
            except Exception:
                logger.exception("hook run metric emission failed after outcome commit")
