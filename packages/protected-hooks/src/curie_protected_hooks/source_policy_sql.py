"""@spec PROTECTED-HOOK-SOURCE-2/10."""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import AsyncIterator, Mapping
from contextlib import AsyncExitStack, asynccontextmanager
from dataclasses import dataclass, field
from datetime import datetime
from weakref import WeakKeyDictionary

from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine, AsyncTransaction
from sqlalchemy.pool import Pool

from .source_policy_records import SourcePolicyRecordInvalid, canonical_hook, target_intent_sha256

_HELD: WeakKeyDictionary[Pool, WeakKeyDictionary[asyncio.Task[object], set[uuid.UUID]]] = (
    WeakKeyDictionary()
)


class SourceGateInvalid(ValueError):
    """@spec PROTECTED-HOOK-SOURCE-2."""


class SourceSnapshotUnavailable(RuntimeError):
    """@spec PROTECTED-HOOK-SOURCE-2/10."""


class SourceAgentNotFound(LookupError):
    """@spec PROTECTED-HOOK-SOURCE-2."""


@dataclass(frozen=True, eq=False)
class SourceGateContext:
    """@spec PROTECTED-HOOK-SOURCE-2."""

    agent_id: uuid.UUID
    _owner: SourceGate = field(repr=False)
    _task: asyncio.Task[object] = field(repr=False)
    _connection: AsyncConnection = field(repr=False)
    _transaction: AsyncTransaction = field(repr=False)

    @property
    def active(self) -> bool:
        """@spec PROTECTED-HOOK-SOURCE-2."""
        return (
            self in self._owner._contexts
            and not self._connection.closed
            and self._transaction.is_active
        )


class SourceGate:
    """@spec PROTECTED-HOOK-SOURCE-2."""

    def __init__(self, gate_engine: AsyncEngine) -> None:
        """@spec PROTECTED-HOOK-SOURCE-2."""
        self.engine = gate_engine
        self._contexts: set[SourceGateContext] = set()

    @asynccontextmanager
    async def hold(self, agent_id: uuid.UUID) -> AsyncIterator[SourceGateContext]:
        """@spec PROTECTED-HOOK-SOURCE-2."""
        task = asyncio.current_task()
        if type(agent_id) is not uuid.UUID or task is None:
            raise SourceGateInvalid("invalid_source_gate")
        tasks = _HELD.setdefault(self.engine.pool, WeakKeyDictionary())
        agents = tasks.setdefault(task, set())
        if agent_id in agents:
            raise SourceGateInvalid("nested_source_gate")
        agents.add(agent_id)
        try:
            async with AsyncExitStack() as stack:
                try:
                    connection = await stack.enter_async_context(self.engine.connect())
                except Exception:
                    # SQLAlchemy leaves some driver connect errors unwrapped (SOURCE-2).
                    raise SourceSnapshotUnavailable("source_gate_unavailable") from None
                await connection.execution_options(isolation_level="READ COMMITTED")
                async with connection.begin() as transaction:
                    await connection.execute(
                        text("SELECT pg_advisory_xact_lock(hashtextextended(:lock_key, 0))"),
                        {"lock_key": "hook-source:" + str(agent_id)},
                    )
                    context = SourceGateContext(agent_id, self, task, connection, transaction)
                    self._contexts.add(context)
                    try:
                        yield context
                    finally:
                        self._contexts.discard(context)
        except SQLAlchemyError:
            raise SourceSnapshotUnavailable("source_gate_unavailable") from None
        finally:
            agents.discard(agent_id)
            if not agents:
                tasks.pop(task, None)


@dataclass(frozen=True)
class SourcePolicySnapshot:
    """@spec PROTECTED-HOOK-SOURCE-2/10."""

    agent_id: uuid.UUID
    hook: str
    generation: int
    operation_id: uuid.UUID
    mode: str
    tool_access: str | None
    runtime_id: str | None
    qualification_id: str | None
    bundle_digest: str | None
    legacy_generation: int
    updated_at: datetime


@dataclass(frozen=True)
class SourceSnapshot:
    """@spec PROTECTED-HOOK-SOURCE-2/10."""

    agent_id: uuid.UUID
    hook: str
    legacy_generation: int
    policy: SourcePolicySnapshot | None
    attempt_history_present: bool
    attempt_generation_highwater: int

    @property
    def never_configured(self) -> bool:
        """@spec PROTECTED-HOOK-SOURCE-2/10."""
        return self.policy is None and not self.attempt_history_present

    @property
    def refusal_reason(self) -> str | None:
        """@spec PROTECTED-HOOK-SOURCE-10."""
        return "pending_history" if self.policy is None and self.attempt_history_present else None


def _integer(value: object, *, positive: bool = False) -> int:
    """@spec PROTECTED-HOOK-SOURCE-2/10."""
    if type(value) is not int or not (int(positive) <= value <= 2**63 - 1):
        raise SourceSnapshotUnavailable("invalid_source_state")
    return value


def _stored_policy(row: Mapping[str, object]) -> tuple[SourcePolicySnapshot, str]:
    """@spec PROTECTED-HOOK-SOURCE-2/10."""
    if not isinstance(row["agent_id"], uuid.UUID) or not isinstance(row["operation_id"], uuid.UUID):
        raise SourceSnapshotUnavailable("invalid_source_state")
    target = {
        key: row[key]
        for key in ("mode", "tool_access", "runtime_id", "qualification_id", "bundle_digest")
    }
    try:
        intent = target_intent_sha256(target)
        hook = row["hook"]
        if not isinstance(hook, str):
            raise SourcePolicyRecordInvalid("invalid_hook")
        canonical_hook(hook)
    except SourcePolicyRecordInvalid:
        raise SourceSnapshotUnavailable("invalid_source_state") from None
    updated = row["updated_at"]
    if not isinstance(updated, datetime) or updated.tzinfo is None or updated.utcoffset() is None:
        raise SourceSnapshotUnavailable("invalid_source_state")
    # The checked target consists exclusively of strict strings or explicit nulls.
    policy = SourcePolicySnapshot(
        agent_id=row["agent_id"],
        hook=hook,
        generation=_integer(row["generation"], positive=True),
        operation_id=row["operation_id"],
        mode=str(row["mode"]),
        tool_access=None if row["tool_access"] is None else str(row["tool_access"]),
        runtime_id=None if row["runtime_id"] is None else str(row["runtime_id"]),
        qualification_id=None if row["qualification_id"] is None else str(row["qualification_id"]),
        bundle_digest=None if row["bundle_digest"] is None else str(row["bundle_digest"]),
        legacy_generation=_integer(row["legacy_generation"]),
        updated_at=updated,
    )
    return policy, intent


def _validate_gate_context(context: SourceGateContext) -> None:
    """@spec PROTECTED-HOOK-SOURCE-2."""
    if (
        type(context) is not SourceGateContext
        or context not in context._owner._contexts
        or context._task is not asyncio.current_task()
        or context._connection.closed
        or not context._transaction.is_active
    ):
        raise SourceGateInvalid("invalid_source_gate")


async def ensure_source_gate_live(context: SourceGateContext) -> None:
    """@spec PROTECTED-HOOK-SOURCE-2."""
    _validate_gate_context(context)
    try:
        await context._connection.execute(text("SELECT 1"))
    except SQLAlchemyError:
        raise SourceSnapshotUnavailable("source_gate_unavailable") from None


def _validate_context(context: SourceGateContext, work: AsyncConnection) -> None:
    """@spec PROTECTED-HOOK-SOURCE-2."""
    _validate_gate_context(context)
    if work.engine.pool is context._owner.engine.pool or work.closed:
        raise SourceGateInvalid("invalid_source_gate")


async def read_source_snapshot(
    context: SourceGateContext, work_connection: AsyncConnection, hook: str
) -> SourceSnapshot:
    """@spec PROTECTED-HOOK-SOURCE-2/10."""
    _validate_context(context, work_connection)
    try:
        canonical_hook(hook)
    except SourcePolicyRecordInvalid:
        raise SourceGateInvalid("invalid_source_hook") from None
    try:
        if await work_connection.get_isolation_level() != "READ COMMITTED":
            raise SourceGateInvalid("invalid_source_isolation")
        await ensure_source_gate_live(context)
        parameters = {"agent": context.agent_id, "hook": hook}
        agent = (
            (
                await work_connection.execute(
                    text("SELECT hook_generation FROM curie.agents WHERE id=:agent"), parameters
                )
            )
            .mappings()
            .one_or_none()
        )
        if agent is None:
            raise SourceAgentNotFound("source_agent_not_found")
        legacy = _integer(agent["hook_generation"])
        row = (
            (
                await work_connection.execute(
                    text(
                        "SELECT * FROM curie.hook_source_policies "
                        "WHERE agent_id=:agent AND hook=:hook"
                    ),
                    parameters,
                )
            )
            .mappings()
            .one_or_none()
        )
        history = (
            (
                await work_connection.execute(
                    text(
                        "SELECT COUNT(*) AS attempts, MAX(generation) AS highwater "
                        "FROM curie.hook_source_operations WHERE agent_id=:agent AND hook=:hook"
                    ),
                    parameters,
                )
            )
            .mappings()
            .one()
        )
        count = _integer(history["attempts"])
        highwater = _integer(history["highwater"], positive=True) if count else 0
        policy = None
        if row is not None:
            policy, intent = _stored_policy(dict(row))
            ledger = (
                (
                    await work_connection.execute(
                        text(
                            "SELECT status, generation, intent_sha256 "
                            "FROM curie.hook_source_operations "
                            "WHERE agent_id=:agent AND hook=:hook AND operation_id=:operation"
                        ),
                        {**parameters, "operation": policy.operation_id},
                    )
                )
                .mappings()
                .one_or_none()
            )
            if (
                ledger is None
                or ledger["status"] != "committed"
                or ledger["generation"] != policy.generation
                or ledger["intent_sha256"] != intent
            ):
                raise SourceSnapshotUnavailable("inconsistent_source_state")
        await ensure_source_gate_live(context)
        return SourceSnapshot(context.agent_id, hook, legacy, policy, count > 0, highwater)
    except SQLAlchemyError:
        raise SourceSnapshotUnavailable("source_snapshot_unavailable") from None
