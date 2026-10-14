"""@spec PROTECTED-HOOK-SOURCE-2/10."""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass, field

from curie_protected_hooks.source_policy_records import SourcePolicyRecordInvalid, canonical_hook
from curie_protected_hooks.source_policy_sql import (
    SourceAgentNotFound,
    SourceGate,
    SourceGateContext,
    SourceGateInvalid,
    SourceSnapshotUnavailable,
    ensure_source_gate_live,
    read_source_snapshot,
)
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine


@dataclass(frozen=True, eq=False)
class CronSourceContext:
    """@spec PROTECTED-HOOK-SOURCE-2."""

    agent_id: uuid.UUID
    raw_name: str
    _owner: CronHookSourceGuard = field(repr=False)
    _task: asyncio.Task[object] = field(repr=False)
    _held: SourceGateContext = field(repr=False)

    async def ensure_before_effect(self, agent_id: uuid.UUID, raw_name: str) -> None:
        """@spec PROTECTED-HOOK-SOURCE-2."""
        self._owner.validate_context(self, agent_id, raw_name)
        await ensure_source_gate_live(self._held)


class CronHookSourceGuard:
    """@spec PROTECTED-HOOK-SOURCE-2/10."""

    def __init__(self, source_gate: SourceGate, work_engine: AsyncEngine) -> None:
        """@spec PROTECTED-HOOK-SOURCE-2."""
        if (
            source_gate.engine.pool is work_engine.pool
            or source_gate.engine.url != work_engine.url
        ):
            raise SourceGateInvalid("invalid_source_gate")
        self.source_gate = source_gate
        self.work_engine = work_engine
        self._contexts: set[CronSourceContext] = set()

    def validate_context(
        self, context: CronSourceContext, agent_id: uuid.UUID, raw_name: str
    ) -> None:
        """@spec PROTECTED-HOOK-SOURCE-2."""
        if (
            type(context) is not CronSourceContext
            or context._owner is not self
            or context not in self._contexts
            or context._task is not asyncio.current_task()
            or type(agent_id) is not uuid.UUID
            or agent_id != context.agent_id
            or type(raw_name) is not str
            or raw_name != context.raw_name
            or not context._held.active
        ):
            raise SourceGateInvalid("invalid_source_gate")

    @asynccontextmanager
    async def locked_snapshot(
        self, agent_id: uuid.UUID, raw_name: str
    ) -> AsyncIterator[CronSourceContext]:
        """@spec PROTECTED-HOOK-SOURCE-2/10."""
        task = asyncio.current_task()
        if type(raw_name) is not str or not raw_name or task is None:
            raise SourceGateInvalid("invalid_source_hook")
        async with self.source_gate.hold(agent_id) as held:
            try:
                async with self.work_engine.connect() as work:
                    await work.execution_options(isolation_level="READ COMMITTED")
                    await self._require_ordinary(held, work, raw_name)
            except SQLAlchemyError:
                raise SourceSnapshotUnavailable("source_snapshot_unavailable") from None
            context = CronSourceContext(agent_id, raw_name, self, task, held)
            self._contexts.add(context)
            try:
                yield context
            finally:
                self._contexts.discard(context)

    async def _require_ordinary(
        self, held: SourceGateContext, work: AsyncConnection, raw_name: str
    ) -> None:
        """@spec PROTECTED-HOOK-SOURCE-2/10."""
        try:
            canonical_hook(raw_name)
        except SourcePolicyRecordInvalid:
            await ensure_source_gate_live(held)
            parameters = {"agent": held.agent_id, "hook": raw_name}
            if await work.scalar(
                text("SELECT 1 FROM curie.agents WHERE id=:agent"), parameters
            ) is None:
                raise SourceAgentNotFound("source_agent_not_found") from None
            presence = (
                await work.execute(
                    text(
                        "SELECT EXISTS (SELECT 1 FROM curie.hook_source_policies "
                        "WHERE agent_id=:agent AND hook=:hook) AS policy, "
                        "EXISTS (SELECT 1 FROM curie.hook_source_operations "
                        "WHERE agent_id=:agent AND hook=:hook) AS history"
                    ),
                    parameters,
                )
            ).one()
            await ensure_source_gate_live(held)
            if presence.policy or presence.history:
                raise SourceSnapshotUnavailable(
                    "authority_unavailable" if presence.policy else "pending_history"
                ) from None
            return
        snapshot = await read_source_snapshot(held, work, raw_name)
        if not snapshot.never_configured:
            raise SourceSnapshotUnavailable(snapshot.refusal_reason or "authority_unavailable")
