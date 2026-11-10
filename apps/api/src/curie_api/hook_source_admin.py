"""@spec PROTECTED-HOOK-SOURCE-2/3/5/6/10."""

from __future__ import annotations

import uuid
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from typing import Literal

from curie_protected_hooks.source_policy_records import (
    SourcePolicyRecordInvalid,
    canonical_hook,
    canonical_uuid,
)
from curie_protected_hooks.source_policy_sql import (
    SourceAgentNotFound,
    SourceGate,
    SourceGateInvalid,
    SourcePolicySnapshot,
    SourceSnapshot,
    SourceSnapshotUnavailable,
    read_source_snapshot,
)
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

from .hook_source_policy_schemas import HookSourcePolicyOut

GATE_WAIT_SECONDS = 5.0
"""Administrative requests give up on the agent gate after this long.

@spec PROTECTED-HOOK-SOURCE-2 @spec PROTECTED-HOOK-SOURCE-10.
"""


class SourceAdminError(RuntimeError):
    """@spec PROTECTED-HOOK-SOURCE-3."""

    def __init__(self, code: str, status_code: int) -> None:
        """@spec PROTECTED-HOOK-SOURCE-3."""
        super().__init__(code)
        self.code = code
        self.status_code = status_code
        self.committed_generation: str | None = None


def _identity(agent_id: str, hook: str) -> uuid.UUID:
    """@spec PROTECTED-HOOK-SOURCE-3."""
    try:
        canonical_uuid(agent_id)
        canonical_hook(hook)
    except SourcePolicyRecordInvalid:
        raise SourceAdminError("invalid_source_request", 422) from None
    return uuid.UUID(agent_id)


SourceActivation = tuple[Literal["closed", "active"], str | None]
"""(activation, refusal_reason) of a committed tombstone, @spec PROTECTED-HOOK-SOURCE-3."""


def policy_out(
    snapshot_agent: uuid.UUID,
    hook: str,
    policy: SourcePolicySnapshot | None,
    *,
    legacy_generation: int,
    activation: Literal["closed", "active"],
    refusal_reason: str | None,
) -> HookSourcePolicyOut:
    """The SOURCE-3 DTO for one committed row or its absence, @spec PROTECTED-HOOK-SOURCE-3/5."""
    return HookSourcePolicyOut(
        agent_id=str(snapshot_agent),
        hook=hook,
        generation="0" if policy is None else str(policy.generation),
        mode="ordinary" if policy is None or policy.mode == "ordinary" else "protected",
        tool_access=None if policy is None or policy.tool_access is None else "read-only",
        runtime_id=None if policy is None else policy.runtime_id,
        qualification_id=None if policy is None else policy.qualification_id,
        bundle_digest=None if policy is None else policy.bundle_digest,
        legacy_generation=str(legacy_generation),
        activation=activation,
        updated_at=None if policy is None else policy.updated_at,
        refusal_reason=refusal_reason,
    )


class SourceAdminService:
    """@spec PROTECTED-HOOK-SOURCE-2/3/5/6/10."""

    def __init__(self, gate: SourceGate, work_engine: AsyncEngine) -> None:
        """@spec PROTECTED-HOOK-SOURCE-2/3."""
        if gate.engine.pool is work_engine.pool:
            raise SourceAdminError("invalid_source_pools", 503)
        self._gate = gate
        self._work_engine = work_engine

    @asynccontextmanager
    async def _locked(
        self, agent_id: str, hook: str
    ) -> AsyncIterator[tuple[AsyncConnection, SourceSnapshot]]:
        """@spec PROTECTED-HOOK-SOURCE-2/3/10."""
        agent = _identity(agent_id, hook)
        try:
            async with self._gate.hold(agent, wait_seconds=GATE_WAIT_SECONDS) as context:
                async with self._work_engine.connect() as connection:
                    await connection.execution_options(isolation_level="READ COMMITTED")
                    async with connection.begin():
                        snapshot = await read_source_snapshot(context, connection, hook)
                        yield connection, snapshot
        except SourceAgentNotFound:
            raise SourceAdminError("source_agent_not_found", 404) from None
        except (SourceGateInvalid, SourceSnapshotUnavailable, SQLAlchemyError):
            raise SourceAdminError("source_state_unavailable", 503) from None

    async def read_policy(
        self,
        agent_id: str,
        hook: str,
        source_activation: Callable[[SourcePolicySnapshot], Awaitable[SourceActivation]],
    ) -> HookSourcePolicyOut:
        """GET with source publication activation, evaluated after the gate is released.

        No row reports closed with a null or ``pending_history`` reason,
        touching no broker. A tombstone and a protected row are evaluated once
        by the caller supplied reader evaluation, after the gate and the
        transaction ended; a protected row answers on the tombstone's rule with
        mode ``protected``, attesting publication rather than current readiness.
        ``legacy_generation`` is the locked agent counter.
        @spec PROTECTED-HOOK-SOURCE-3 @spec PROTECTED-HOOK-SOURCE-5/6/10.
        """
        async with self._locked(agent_id, hook) as (_, snapshot):
            pass
        policy = snapshot.policy
        activation: Literal["closed", "active"] = "closed"
        if policy is None:
            reason = snapshot.refusal_reason
        else:
            activation, reason = await source_activation(policy)
        return policy_out(
            snapshot.agent_id,
            hook,
            policy,
            legacy_generation=snapshot.legacy_generation,
            activation=activation,
            refusal_reason=reason,
        )

    async def refuse_secret(
        self,
        agent_id: str,
        hook: str,
        source_activation: (
            Callable[[SourcePolicySnapshot], Awaitable[SourceActivation]] | None
        ) = None,
    ) -> SourcePolicySnapshot:
        """Refuse every state whose secret may not be served; else the active protected row.

        Absent, history only and tombstone rows are 409 ``source_not_protected``.
        A protected row is evaluated by the caller supplied reader evaluation
        after the gate and the transaction ended; anything but ``active`` is 503
        with its closed reason (``source_closed``, ``runtime_unavailable`` or
        ``broker_unavailable``) and a null committed generation. With no
        evaluation composed it is ``runtime_unavailable``. Never a key.
        @spec PROTECTED-HOOK-SOURCE-3/6.
        """
        async with self._locked(agent_id, hook) as (_, snapshot):
            pass
        policy = snapshot.policy
        if policy is None or policy.mode == "ordinary":
            raise SourceAdminError("source_not_protected", 409)
        if source_activation is None:
            raise SourceAdminError("runtime_unavailable", 503)
        activation, reason = await source_activation(policy)
        if activation != "active":
            raise SourceAdminError(reason or "source_closed", 503)
        return policy
