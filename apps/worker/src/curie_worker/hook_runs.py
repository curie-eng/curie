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
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

from .binding import ApprovalChainTooLong, approval_chain_origin

# "blocked" is the kill-switch outcome for a targetless run (#2963, ADR-0099);
# migration 0048 already allows it. "deferred" is a fire that met a live session
# on its thread (#2929); the scheduler reopens it on a later tick.
HookRunOutcome = Literal["ran", "failed", "blocked", "deferred", "skipped"]
# Closed set pinned by tests/vectors/hook-run-reasons.json. NULL means ran,
# in flight, or a row written before the reason column existed.
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

logger = logging.getLogger(__name__)

_RETRY_MARK = "retry"

# An agent-created schedule's hook name is this prefix and the schedule id
# (ADR 0238). Its runs share the hook_runs ledger with bundle triggers.
AGENT_SCHEDULE_PREFIX = "agent-schedule:"


def agent_schedule_id(name: str) -> uuid.UUID | None:
    """The schedule id an ``agent-schedule:<id>`` hook name carries, else None.

    Only the canonical lowercase form counts, so one schedule has one name.
    """

    if not name.startswith(AGENT_SCHEDULE_PREFIX):
        return None
    raw = name[len(AGENT_SCHEDULE_PREFIX) :]
    try:
        parsed = uuid.UUID(raw)
    except ValueError:
        return None
    return parsed if str(parsed) == raw else None


def agent_schedule_slot(
    event_id: str, agent_id: uuid.UUID, schedule_id: uuid.UUID
) -> datetime | None:
    """The slot of the agent schedule firing a cron event id names, else None.

    The id is ``cron:<agent>:agent-schedule:<id>:<slot iso>``, or its retry
    form. The hook name and the ISO slot both contain colons, so the known
    prefix is matched rather than split, and a retry suffix is stripped only
    when ``retry_expiry`` reads one.
    """

    base = event_id
    if retry_expiry(base) is not None:
        base = base.rsplit(":", 3)[0]
    prefix = f"cron:{agent_id}:{AGENT_SCHEDULE_PREFIX}{schedule_id}:"
    if not base.startswith(prefix):
        return None
    try:
        slot = datetime.fromisoformat(base[len(prefix) :])
    except ValueError:
        return None
    if slot.tzinfo is None or slot.utcoffset() != timedelta(0):
        return None
    return slot.astimezone(UTC)


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
    # The row's own clock: a sweep checkpoint stated before it is not this
    # run's (ADR-0160, #2878).
    started_at: datetime
    # The agent version the run fired from, whose bundle names the trigger zone.
    version_id: uuid.UUID | None


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

    def __init__(self, engine: AsyncEngine, db_schema: str) -> None:
        self._engine = engine
        # Table identifiers are not user input; the schema comes from config.
        self._schema = db_schema

    @asynccontextmanager
    async def start_guard(self, ref: HookRunRef) -> AsyncIterator[bool]:
        """Serialize runner admission with an operator pause of this hook.

        For an agent schedule (ADR 0238) the run must also still be what was
        admitted: the schedule exists and is not cancelled, the agent's toggle
        is on, its exact destination binding exists, and its revision is the
        run snapshot's. Otherwise the row is closed ``skipped`` with reason
        ``schedule_changed`` and that close COMMITS before this yields False,
        so the kernel's later close or defer is a no-op. A valid run yields
        inside the transaction, holding the lock across the runner start, with
        the agent row and destination row held ``FOR SHARE`` so a toggle-off
        or unbind waits for the start rather than slipping in between.
        """

        key = _parse_ref(ref)
        schedule_id = agent_schedule_id(key.name)
        changed = False
        try:
            async with self._engine.begin() as connection:
                await self._lock(connection, key.agent_id, key.name)
                row = (
                    await connection.execute(
                        text(
                            "SELECT r.id, r.outcome, c.paused_at "
                            f"FROM {self._schema}.hook_runs r "
                            f"LEFT JOIN {self._schema}.schedule_controls c "
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
                ).one_or_none()
                allowed = row is not None and row.outcome is None and row.paused_at is None
                if schedule_id is not None and row is not None and row.outcome is None:
                    current = await self._agent_schedule_revision(
                        connection, key.agent_id, schedule_id
                    )
                    admitted = (
                        await connection.execute(
                            text(
                                f"SELECT revision FROM {self._schema}.agent_schedule_runs "
                                "WHERE hook_run_id = :id"
                            ),
                            {"id": row.id},
                        )
                    ).scalar_one_or_none()
                    if current is None or admitted != current:
                        await connection.execute(
                            text(
                                f"UPDATE {self._schema}.hook_runs SET outcome = 'skipped', "
                                "reason = 'schedule_changed', ended_at = now() "
                                "WHERE id = :id AND outcome IS NULL"
                            ),
                            {"id": row.id},
                        )
                        changed = True
                if not changed:
                    yield allowed
        except SQLAlchemyError as exc:
            raise HookRunRecorderError(
                "hook run start control could not be read", code="backend"
            ) from exc
        if changed:
            logger.info(
                "agent schedule run %s at %s changed before start; skipped",
                key.name,
                key.slot_utc.isoformat(),
            )
            try:
                record_metric(
                    "curie.schedule.fire",
                    attributes={
                        "service.name": "curie-worker",
                        "trigger": "cron",
                        "outcome": "skipped",
                    },
                )
            except Exception:
                logger.exception("hook run metric emission failed after outcome commit")
            yield False

    @asynccontextmanager
    async def resume_guard(
        self, agent_id: uuid.UUID, schedule_id: uuid.UUID, approval_id: uuid.UUID
    ) -> AsyncIterator[bool]:
        """Whether an approved continuation of an agent schedule firing may start.

        Takes the same per (agent, name) lock as ``start_guard`` and holds it,
        with the agent and destination rows ``FOR SHARE``, for the body (the
        runner start or steer). True when the approval's chain
        (``approval_chain_origin``) starts at a firing of this schedule by its
        cron event id, the schedule exists and is not
        cancelled or paused, the toggle is on, the exact destination binding
        exists, and that firing's snapshot revision is the current revision.
        A done one-shot passes. A cycle or a chain past its bound refuses.
        """

        name = f"{AGENT_SCHEDULE_PREFIX}{schedule_id}"
        try:
            async with self._engine.begin() as connection:
                await self._lock(connection, agent_id, name)
                try:
                    origin_key = await approval_chain_origin(
                        connection, self._schema, approval_id, agent_id
                    )
                except ApprovalChainTooLong:
                    origin_key = None
                slot = (
                    agent_schedule_slot(origin_key, agent_id, schedule_id)
                    if origin_key is not None
                    else None
                )
                allowed = False
                if slot is not None:
                    current = await self._agent_schedule_revision(connection, agent_id, schedule_id)
                    if current is not None:
                        paused = (
                            await connection.execute(
                                text(
                                    f"SELECT paused_at FROM {self._schema}.schedule_controls "
                                    "WHERE agent_id = :agent_id AND name = :name"
                                ),
                                {"agent_id": agent_id, "name": name},
                            )
                        ).scalar_one_or_none()
                        origin = (
                            await connection.execute(
                                text(
                                    f"SELECT s.revision FROM {self._schema}.hook_runs r "
                                    f"JOIN {self._schema}.agent_schedule_runs s "
                                    "ON s.hook_run_id = r.id "
                                    "WHERE r.agent_id = :agent_id AND r.name = :name "
                                    "AND r.slot_utc = :slot"
                                ),
                                {"agent_id": agent_id, "name": name, "slot": slot},
                            )
                        ).scalar_one_or_none()
                        allowed = paused is None and origin == current
                yield allowed
        except SQLAlchemyError as exc:
            raise HookRunRecorderError(
                "agent schedule resume control could not be read", code="backend"
            ) from exc

    async def _lock(self, connection: AsyncConnection, agent_id: uuid.UUID, name: str) -> None:
        await connection.execute(
            text(
                "SELECT pg_advisory_xact_lock("
                "hashtextextended(CAST(:agent_id AS text) || ':' || :name, 0))"
            ),
            {"agent_id": str(agent_id), "name": name},
        )

    async def _agent_schedule_revision(
        self, connection: AsyncConnection, agent_id: uuid.UUID, schedule_id: uuid.UUID
    ) -> int | None:
        """The schedule's current revision while it may still run, else None.

        None when the agent's toggle is off, the schedule is missing or
        cancelled, or its exact destination binding (kind, address, adapter,
        no fallback) is gone. Reads the agent and destination rows ``FOR
        SHARE`` so neither can change until the caller's transaction ends.
        """

        toggle = (
            await connection.execute(
                text(
                    f"SELECT agent_schedules FROM {self._schema}.agents "
                    "WHERE id = :agent_id FOR SHARE"
                ),
                {"agent_id": agent_id},
            )
        ).scalar_one_or_none()
        if toggle is not True:
            return None
        schedule = (
            await connection.execute(
                text(
                    "SELECT revision, state, destination_kind, destination_address, "
                    f"destination_adapter FROM {self._schema}.agent_schedules "
                    "WHERE id = :id AND agent_id = :agent_id FOR SHARE"
                ),
                {"id": schedule_id, "agent_id": agent_id},
            )
        ).one_or_none()
        if schedule is None or schedule.state == "cancelled":
            return None
        destination = (
            await connection.execute(
                text(
                    f"SELECT 1 FROM {self._schema}.agent_channels "
                    "WHERE agent_id = :agent_id AND kind = :kind AND address = :address "
                    "AND adapter IS NOT DISTINCT FROM CAST(:adapter AS text) FOR SHARE"
                ),
                {
                    "agent_id": agent_id,
                    "kind": schedule.destination_kind,
                    "address": schedule.destination_address,
                    "adapter": schedule.destination_adapter,
                },
            )
        ).first()
        if destination is None:
            return None
        return int(schedule.revision)

    async def get(self, ref: HookRunRef) -> HookRunState | None:
        """Return the exact run row, or None when the key is absent."""
        key = _parse_ref(ref)
        try:
            async with self._engine.begin() as connection:
                row = (
                    await connection.execute(
                        text(
                            "SELECT outcome, started_at, version_id "
                            f"FROM {self._schema}.hook_runs "
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
            started_at=row.started_at,
            version_id=row.version_id,
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
                            f"UPDATE {self._schema}.hook_runs SET lease_expires_at = GREATEST("
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

    async def close(
        self,
        ref: HookRunRef,
        outcome: HookRunOutcome,
        reason: HookRunReason | None = None,
    ) -> bool:
        """Set one open run terminally without overwriting an earlier outcome.

        Returns True only when this call closes the row.

        ``ran`` stores no reason. Every other outcome requires one.
        """
        if outcome == "ran":
            reason = None
        elif reason is None:
            raise HookRunRecorderError(
                "hook run reason is required unless the outcome is ran",
                code="invalid_ref",
            )
        key = _parse_ref(ref)
        closed = False
        notice: str | None = None
        try:
            async with self._engine.begin() as connection:
                updated = (
                    await connection.execute(
                        text(
                            f"UPDATE {self._schema}.hook_runs "
                            "SET outcome = :outcome, reason = :reason, ended_at = now() "
                            "WHERE agent_id = :agent_id "
                            "AND name = :name AND slot_utc = :slot_utc "
                            "AND outcome IS NULL RETURNING outcome"
                        ),
                        {
                            "agent_id": key.agent_id,
                            "name": key.name,
                            "slot_utc": key.slot_utc,
                            "outcome": outcome,
                            "reason": reason,
                        },
                    )
                ).one_or_none()
                if updated is not None:
                    closed = True
                    from .schedule_safety import account_closed_run, take_reported_usage

                    usage = take_reported_usage()
                    input_tokens, output_tokens = usage if usage is not None else (None, None)
                    model = (
                        await connection.execute(
                            text(f"SELECT model FROM {self._schema}.agents WHERE id = :agent_id"),
                            {"agent_id": key.agent_id},
                        )
                    ).scalar()
                    notice = await account_closed_run(
                        connection,
                        self._schema,
                        agent_id=key.agent_id,
                        name=key.name,
                        outcome=outcome,
                        when=key.slot_utc,
                        model=model if isinstance(model, str) else None,
                        input_tokens=input_tokens,
                        output_tokens=output_tokens,
                    )
                else:
                    existing = (
                        await connection.execute(
                            text(
                                f"SELECT outcome FROM {self._schema}.hook_runs "
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
            if notice:
                try:
                    from .schedule_safety import post_notice

                    async with self._engine.connect() as connection:
                        await post_notice(connection, self._schema, key.agent_id, notice)
                except Exception:
                    logger.exception("schedule pause notice failed after outcome commit")
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
        return closed
