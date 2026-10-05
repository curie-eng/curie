"""@spec PROTECTED-HOOK-SOURCE-2/3/5/6/10."""

from __future__ import annotations

import uuid
from collections.abc import AsyncIterator, Mapping
from contextlib import asynccontextmanager

from curie_protected_hooks.source_policy_records import (
    SourcePolicyRecordInvalid,
    canonical_decimal,
    canonical_hook,
    canonical_uuid,
    target_intent_sha256,
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
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

from .hook_source_policy_schemas import HookSourcePolicyOut


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


def _mutation_identity(expected_generation: str, operation_id: str) -> uuid.UUID:
    """@spec PROTECTED-HOOK-SOURCE-3."""
    try:
        canonical_decimal(expected_generation)
        canonical_uuid(operation_id)
    except SourcePolicyRecordInvalid:
        raise SourceAdminError("invalid_source_request", 422) from None
    return uuid.UUID(operation_id)


def _intent(target: Mapping[str, object]) -> str:
    """@spec PROTECTED-HOOK-SOURCE-3/10."""
    try:
        return target_intent_sha256(target)
    except SourcePolicyRecordInvalid:
        raise SourceAdminError("invalid_source_request", 422) from None


def _policy_target(policy: SourcePolicySnapshot) -> dict[str, object]:
    """@spec PROTECTED-HOOK-SOURCE-3/10."""
    return {
        "mode": policy.mode,
        "tool_access": policy.tool_access,
        "runtime_id": policy.runtime_id,
        "qualification_id": policy.qualification_id,
        "bundle_digest": policy.bundle_digest,
    }


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
            async with self._gate.hold(agent) as context:
                async with self._work_engine.connect() as connection:
                    await connection.execution_options(isolation_level="READ COMMITTED")
                    async with connection.begin():
                        snapshot = await read_source_snapshot(context, connection, hook)
                        yield connection, snapshot
        except SourceAgentNotFound:
            raise SourceAdminError("source_agent_not_found", 404) from None
        except (SourceGateInvalid, SourceSnapshotUnavailable, SQLAlchemyError):
            raise SourceAdminError("source_state_unavailable", 503) from None

    async def get_policy(self, agent_id: str, hook: str) -> HookSourcePolicyOut:
        """@spec PROTECTED-HOOK-SOURCE-3/5/6/10."""
        async with self._locked(agent_id, hook) as (_, snapshot):
            policy = snapshot.policy
            return HookSourcePolicyOut(
                agent_id=str(snapshot.agent_id),
                hook=hook,
                generation="0" if policy is None else str(policy.generation),
                mode="ordinary" if policy is None or policy.mode == "ordinary" else "protected",
                tool_access=None if policy is None or policy.tool_access is None else "read-only",
                runtime_id=None if policy is None else policy.runtime_id,
                qualification_id=None if policy is None else policy.qualification_id,
                bundle_digest=None if policy is None else policy.bundle_digest,
                legacy_generation=str(snapshot.legacy_generation),
                activation="closed",
                updated_at=None if policy is None else policy.updated_at,
                refusal_reason=(
                    snapshot.refusal_reason if policy is None else "authority_unavailable"
                ),
            )

    async def _refuse_mutation(
        self,
        connection: AsyncConnection,
        snapshot: SourceSnapshot,
        expected_generation: str,
        operation: uuid.UUID,
        target: Mapping[str, object],
    ) -> None:
        """@spec PROTECTED-HOOK-SOURCE-3/5/6/10."""
        intent = _intent(target)
        policy = snapshot.policy
        if policy is not None and policy.operation_id == operation:
            if target_intent_sha256(_policy_target(policy)) != intent:
                raise SourceAdminError("source_operation_conflict", 409)
            raise SourceAdminError("source_authority_unavailable", 503)
        existing = await connection.scalar(
            text(
                "SELECT 1 FROM curie.hook_source_operations "
                "WHERE agent_id=:agent AND hook=:hook AND operation_id=:operation"
            ),
            {"agent": snapshot.agent_id, "hook": snapshot.hook, "operation": operation},
        )
        if existing is not None:
            raise SourceAdminError("source_operation_conflict", 409)
        current_generation = 0 if policy is None else policy.generation
        if int(expected_generation) != current_generation:
            raise SourceAdminError("stale_source_generation", 409)
        if max(current_generation, snapshot.attempt_generation_highwater) == 2**63 - 1:
            raise SourceAdminError("source_generation_exhausted", 409)
        if (
            target["mode"] == "protected"
            and (policy is None or policy.mode == "ordinary")
            and snapshot.legacy_generation == 2147483647
        ):
            raise SourceAdminError("legacy_generation_exhausted", 409)
        # No qualified authority resolver is composed into this unwired slice.
        raise SourceAdminError("source_authority_unavailable", 503)

    async def mutate(
        self,
        agent_id: str,
        hook: str,
        expected_generation: str,
        operation_id: str,
        desired_target: Mapping[str, object],
    ) -> None:
        """@spec PROTECTED-HOOK-SOURCE-3/5/6/10."""
        operation = _mutation_identity(expected_generation, operation_id)
        if not isinstance(desired_target, Mapping):
            raise SourceAdminError("invalid_source_request", 422)
        target = dict(desired_target)
        _intent(target)
        async with self._locked(agent_id, hook) as (connection, snapshot):
            await self._refuse_mutation(
                connection, snapshot, expected_generation, operation, target
            )

    async def remove(
        self, agent_id: str, hook: str, expected_generation: str, operation_id: str
    ) -> None:
        """@spec PROTECTED-HOOK-SOURCE-3/6/10."""
        await self.mutate(
            agent_id,
            hook,
            expected_generation,
            operation_id,
            {
                "mode": "ordinary",
                "tool_access": None,
                "runtime_id": None,
                "qualification_id": None,
                "bundle_digest": None,
            },
        )

    async def rotate(
        self, agent_id: str, hook: str, expected_generation: str, operation_id: str
    ) -> None:
        """@spec PROTECTED-HOOK-SOURCE-3/6/10."""
        operation = _mutation_identity(expected_generation, operation_id)
        async with self._locked(agent_id, hook) as (connection, snapshot):
            policy = snapshot.policy
            if policy is None or policy.mode != "protected":
                raise SourceAdminError("source_rotation_conflict", 409)
            await self._refuse_mutation(
                connection, snapshot, expected_generation, operation, _policy_target(policy)
            )

    async def read_secret(self, agent_id: str, hook: str) -> None:
        """@spec PROTECTED-HOOK-SOURCE-3/6."""
        async with self._locked(agent_id, hook):
            raise SourceAdminError("source_authority_unavailable", 503)
