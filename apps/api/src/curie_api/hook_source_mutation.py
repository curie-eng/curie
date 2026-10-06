"""@spec PROTECTED-HOOK-SOURCE-2/3/5/6/7/10."""

from __future__ import annotations

import uuid
from collections.abc import AsyncIterator, Callable, Mapping
from contextlib import AbstractAsyncContextManager, AsyncExitStack, asynccontextmanager
from dataclasses import dataclass
from typing import Protocol

from curie_protected_hooks.source_fence import (
    SourceFenceConflict,
    SourceFenceExhausted,
    SourceFenceInvalid,
)
from curie_protected_hooks.source_policy_records import (
    SourcePolicyRecordInvalid,
    canonical_decimal,
    canonical_hook,
    canonical_uuid,
    policy_fingerprint,
    target_intent_sha256,
)
from curie_protected_hooks.source_policy_sql import (
    SourceAgentNotFound,
    SourceGate,
    SourceGateContext,
    SourceGateInvalid,
    SourcePolicySnapshot,
    SourceSnapshot,
    SourceSnapshotUnavailable,
    ensure_source_gate_live,
    read_source_snapshot,
)
from redis.exceptions import RedisError
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

from .hook_source_admin import SourceAdminError


@dataclass(frozen=True)
class SourceIdentity:
    """@spec PROTECTED-HOOK-SOURCE-3/10."""

    agent_id: str
    hook: str

    def __post_init__(self) -> None:
        """@spec PROTECTED-HOOK-SOURCE-3."""
        try:
            canonical_uuid(self.agent_id)
            canonical_hook(self.hook)
        except SourcePolicyRecordInvalid:
            raise SourceAdminError("invalid_source_request", 422) from None


@dataclass(frozen=True)
class DesiredSourceTarget:
    """@spec PROTECTED-HOOK-SOURCE-3/10."""

    mode: str
    tool_access: str | None
    runtime_id: str | None
    qualification_id: str | None
    bundle_digest: str | None

    def __post_init__(self) -> None:
        """@spec PROTECTED-HOOK-SOURCE-3/10."""
        try:
            target_intent_sha256(self.as_dict())
        except SourcePolicyRecordInvalid:
            raise SourceAdminError("invalid_source_request", 422) from None

    @classmethod
    def from_mapping(cls, value: Mapping[str, object]) -> DesiredSourceTarget:
        """@spec PROTECTED-HOOK-SOURCE-3/10."""
        try:
            if not isinstance(value, Mapping):
                raise SourcePolicyRecordInvalid("invalid_target")
            copied = dict(value)
            target_intent_sha256(copied)
        except (SourcePolicyRecordInvalid, TypeError, ValueError):
            raise SourceAdminError("invalid_source_request", 422) from None
        # Shared validation established that these five values are string/null scalars.
        return cls(
            mode=str(copied["mode"]),
            tool_access=None if copied["tool_access"] is None else str(copied["tool_access"]),
            runtime_id=None if copied["runtime_id"] is None else str(copied["runtime_id"]),
            qualification_id=(
                None if copied["qualification_id"] is None else str(copied["qualification_id"])
            ),
            bundle_digest=(
                None if copied["bundle_digest"] is None else str(copied["bundle_digest"])
            ),
        )

    def as_dict(self) -> dict[str, object]:
        """@spec PROTECTED-HOOK-SOURCE-3/10."""
        return {
            "mode": self.mode,
            "tool_access": self.tool_access,
            "runtime_id": self.runtime_id,
            "qualification_id": self.qualification_id,
            "bundle_digest": self.bundle_digest,
        }


class SourceControlReader(Protocol):
    """@spec PROTECTED-HOOK-SOURCE-6/7/10."""

    async def read_reconciled_floor(self) -> int:
        """@spec PROTECTED-HOOK-SOURCE-6/7/10."""
        ...


class SourceControlWriter(Protocol):
    """@spec PROTECTED-HOOK-SOURCE-6/7/10."""

    async def reserve(
        self, *, expected_floor: int, operation_id: str, min_generation: int
    ) -> int:
        """@spec PROTECTED-HOOK-SOURCE-6/7/10."""
        ...

    async def publish_ordinary(
        self, *, generation: int, operation_id: str, fingerprint: str
    ) -> bool:
        """@spec PROTECTED-HOOK-SOURCE-6/7."""
        ...


@dataclass(frozen=True)
class SourceControlSession:
    """@spec PROTECTED-HOOK-SOURCE-6/7/10."""

    source: SourceIdentity
    target: DesiredSourceTarget
    reader: SourceControlReader
    writer: SourceControlWriter


class SourceAuthorityResolver(Protocol):
    """@spec PROTECTED-HOOK-SOURCE-6/7/10."""

    def resolve(
        self,
        source: SourceIdentity,
        target: DesiredSourceTarget,
        *, durable_generation_highwater: int,
    ) -> AbstractAsyncContextManager[SourceControlSession]:
        """@spec PROTECTED-HOOK-SOURCE-6/7/10."""
        ...


def _unavailable(policy: SourcePolicySnapshot | None = None) -> SourceAdminError:
    """@spec PROTECTED-HOOK-SOURCE-3/6/10."""
    error = SourceAdminError("source_authority_unavailable", 503)
    if policy is not None:
        error.committed_generation = str(policy.generation)
    return error


def _policy_target(policy: SourcePolicySnapshot) -> DesiredSourceTarget:
    """@spec PROTECTED-HOOK-SOURCE-3/10."""
    return DesiredSourceTarget(
        policy.mode,
        policy.tool_access,
        policy.runtime_id,
        policy.qualification_id,
        policy.bundle_digest,
    )


def committed_policy_fingerprint(policy: SourcePolicySnapshot) -> str:
    """The SOURCE-6 fingerprint a committed row publishes, @spec PROTECTED-HOOK-SOURCE-6/9."""
    return policy_fingerprint(
        {
            **_policy_target(policy).as_dict(),
            "agent_id": str(policy.agent_id),
            "hook": policy.hook,
            "generation": str(policy.generation),
            "operation_id": str(policy.operation_id),
            "legacy_generation": str(policy.legacy_generation),
        }
    )


class SourceMutationCoordinator:
    """@spec PROTECTED-HOOK-SOURCE-2/3/5/6/7/10."""

    def __init__(
        self,
        gate: SourceGate,
        work_engine: AsyncEngine,
        *,
        authority_resolver: SourceAuthorityResolver | None = None,
        target_check: Callable[[DesiredSourceTarget], None] | None = None,
    ) -> None:
        """``target_check`` applies the deployment's reference rule inside the gate.

        It runs on every protected target before replay, history and CAS
        checks, so rotate checks the current row's references before any
        registration. @spec PROTECTED-HOOK-SOURCE-2/3/10.
        """
        if gate.engine.pool is work_engine.pool:
            raise SourceAdminError("invalid_source_pools", 503)
        self._gate = gate
        self._work_engine = work_engine
        self._resolver = authority_resolver
        self._target_check = target_check

    @asynccontextmanager
    async def _work(self) -> AsyncIterator[AsyncConnection]:
        """@spec PROTECTED-HOOK-SOURCE-2/10."""
        async with self._work_engine.connect() as connection:
            await connection.execution_options(isolation_level="READ COMMITTED")
            async with connection.begin():
                yield connection

    async def _initial(
        self, held: SourceGateContext, source: SourceIdentity, operation: uuid.UUID
    ) -> tuple[SourceSnapshot, bool]:
        """@spec PROTECTED-HOOK-SOURCE-2/3/10."""
        async with self._work() as connection:
            snapshot = await read_source_snapshot(held, connection, source.hook)
            exists = await connection.scalar(
                text(
                    "SELECT 1 FROM curie.hook_source_operations "
                    "WHERE agent_id=:agent AND hook=:hook AND operation_id=:operation"
                ),
                {"agent": snapshot.agent_id, "hook": source.hook, "operation": operation},
            )
        return snapshot, exists is not None

    async def _register(
        self,
        held: SourceGateContext,
        source: SourceIdentity,
        operation: uuid.UUID,
        generation: int,
        intent: str,
    ) -> None:
        """@spec PROTECTED-HOOK-SOURCE-2/10."""
        async with self._work() as connection:
            await ensure_source_gate_live(held)
            agent = await connection.scalar(
                text("SELECT id FROM curie.agents WHERE id=:agent FOR KEY SHARE"),
                {"agent": uuid.UUID(source.agent_id)},
            )
            await ensure_source_gate_live(held)
            if agent is None:
                raise _unavailable()
            await connection.execute(
                text(
                    "INSERT INTO curie.hook_source_operations "
                    "(agent_id,hook,operation_id,generation,intent_sha256,status) "
                    "VALUES (:agent,:hook,:operation,:generation,:intent,'pending')"
                ),
                {
                    "agent": uuid.UUID(source.agent_id),
                    "hook": source.hook,
                    "operation": operation,
                    "generation": generation,
                    "intent": intent,
                },
            )

    async def _persist(
        self,
        held: SourceGateContext,
        source: SourceIdentity,
        previous: SourceSnapshot,
        operation: uuid.UUID,
        generation: int,
        target: DesiredSourceTarget,
        intent: str,
    ) -> SourcePolicySnapshot:
        """@spec PROTECTED-HOOK-SOURCE-2/5/6/10."""
        async with self._work() as connection:
            params: dict[str, object] = {
                "agent": uuid.UUID(source.agent_id),
                "hook": source.hook,
                "operation": operation,
                "generation": generation,
                "intent": intent,
                **target.as_dict(),
            }
            await ensure_source_gate_live(held)
            await connection.execute(
                text("SELECT id FROM curie.agents WHERE id=:agent FOR UPDATE"), params
            )
            await ensure_source_gate_live(held)
            await connection.execute(
                text(
                    "SELECT generation FROM curie.hook_source_policies "
                    "WHERE agent_id=:agent AND hook=:hook FOR UPDATE"
                ),
                params,
            )
            await ensure_source_gate_live(held)
            current = await read_source_snapshot(held, connection, source.hook)
            if (
                current.policy != previous.policy
                or current.legacy_generation != previous.legacy_generation
            ):
                raise SourceAdminError("source_state_unavailable", 503)
            counter = current.legacy_generation
            if target.mode == "protected" and (
                current.policy is None or current.policy.mode == "ordinary"
            ):
                counter += 1
            params["counter"] = counter
            params["previous_generation"] = (
                0 if previous.policy is None else previous.policy.generation
            )
            params["previous_operation"] = (
                None if previous.policy is None else previous.policy.operation_id
            )
            await ensure_source_gate_live(held)
            persisted = await connection.scalar(
                text(
                    "INSERT INTO curie.hook_source_policies AS existing "
                    "(agent_id,hook,generation,operation_id,mode,tool_access,runtime_id,"
                    "qualification_id,bundle_digest,legacy_generation,updated_at) VALUES "
                    "(:agent,:hook,:generation,:operation,:mode,:tool_access,:runtime_id,"
                    ":qualification_id,:bundle_digest,:counter,now()) "
                    "ON CONFLICT (agent_id,hook) DO UPDATE SET generation=EXCLUDED.generation,"
                    "operation_id=EXCLUDED.operation_id,mode=EXCLUDED.mode,"
                    "tool_access=EXCLUDED.tool_access,runtime_id=EXCLUDED.runtime_id,"
                    "qualification_id=EXCLUDED.qualification_id,bundle_digest=EXCLUDED.bundle_digest,"
                    "legacy_generation=EXCLUDED.legacy_generation,updated_at=EXCLUDED.updated_at "
                    "WHERE existing.generation=:previous_generation "
                    "AND existing.operation_id=:previous_operation RETURNING generation"
                ),
                params,
            )
            if persisted != generation:
                raise SourceAdminError("source_state_unavailable", 503)
            if counter != current.legacy_generation:
                await ensure_source_gate_live(held)
                updated_counter = await connection.scalar(
                    text(
                        "UPDATE curie.agents SET hook_generation=:counter WHERE id=:agent "
                        "AND hook_generation=:previous_counter RETURNING hook_generation"
                    ),
                    {**params, "previous_counter": current.legacy_generation},
                )
                if updated_counter != counter:
                    raise SourceAdminError("source_state_unavailable", 503)
            await ensure_source_gate_live(held)
            transitioned = await connection.scalar(
                text(
                    "UPDATE curie.hook_source_operations SET status='committed' "
                    "WHERE agent_id=:agent AND hook=:hook AND operation_id=:operation "
                    "AND generation=:generation AND intent_sha256=:intent AND status='pending' "
                    "RETURNING generation"
                ),
                params,
            )
            if transitioned != generation:
                raise SourceAdminError("source_state_unavailable", 503)
            completed = await read_source_snapshot(held, connection, source.hook)
            if (
                completed.policy is None
                or completed.policy.generation != generation
                or completed.policy.operation_id != operation
            ):
                raise SourceAdminError("source_state_unavailable", 503)
            policy = completed.policy
        # Reaching here, rather than merely reading RETURNING, confirms the work COMMIT.
        return policy

    async def _execute(
        self,
        source: SourceIdentity,
        expected_generation: str,
        operation_id: str,
        target: DesiredSourceTarget | None,
        *,
        refuse_unconfigured: bool = False,
    ) -> SourcePolicySnapshot:
        """@spec PROTECTED-HOOK-SOURCE-2/3/5/6/7/10."""
        try:
            canonical_decimal(expected_generation)
            canonical_uuid(operation_id)
        except SourcePolicyRecordInvalid:
            raise SourceAdminError("invalid_source_request", 422) from None
        operation = uuid.UUID(operation_id)
        committed: SourcePolicySnapshot | None = None
        try:
            async with AsyncExitStack() as authority_scope:
                async with self._gate.hold(uuid.UUID(source.agent_id)) as held:
                    snapshot, seen = await self._initial(held, source, operation)
                    previous = snapshot.policy
                    if refuse_unconfigured and snapshot.never_configured:
                        raise SourceAdminError("source_not_configured", 409)
                    if target is None:
                        if previous is None or previous.mode != "protected":
                            raise SourceAdminError("source_rotation_conflict", 409)
                        target = _policy_target(previous)
                    if self._target_check is not None and target.mode == "protected":
                        self._target_check(target)
                    intent = target_intent_sha256(target.as_dict())
                    replay = previous is not None and previous.operation_id == operation
                    if replay:
                        assert previous is not None
                        if target_intent_sha256(_policy_target(previous).as_dict()) != intent:
                            raise SourceAdminError("source_operation_conflict", 409)
                        committed = previous
                    else:
                        if seen:
                            raise SourceAdminError("source_operation_conflict", 409)
                        current_generation = 0 if previous is None else previous.generation
                        if int(expected_generation) != current_generation:
                            raise SourceAdminError("stale_source_generation", 409)
                        if snapshot.attempt_generation_highwater == 2**63 - 1:
                            raise SourceAdminError("source_generation_exhausted", 409)
                        if (
                            target.mode == "protected"
                            and (previous is None or previous.mode == "ordinary")
                            and snapshot.legacy_generation == 2147483647
                        ):
                            raise SourceAdminError("legacy_generation_exhausted", 409)
                    if self._resolver is None:
                        raise _unavailable(committed)
                    session = await authority_scope.enter_async_context(
                        self._resolver.resolve(
                            source,
                            target,
                            durable_generation_highwater=snapshot.attempt_generation_highwater,
                        )
                    )
                    if (
                        type(session) is not SourceControlSession
                        or type(session.source) is not SourceIdentity
                        or type(session.target) is not DesiredSourceTarget
                        or session.source != source
                        or session.target != target
                    ):
                        raise _unavailable(committed)
                    floor = await session.reader.read_reconciled_floor()
                    if type(floor) is not int or not 0 <= floor <= 2**63 - 1:
                        raise _unavailable(committed)
                    if not replay:
                        highest = max(
                            snapshot.attempt_generation_highwater,
                            0 if previous is None else previous.generation,
                            floor,
                        )
                        if highest == 2**63 - 1:
                            raise SourceAdminError("source_generation_exhausted", 409)
                        generation = highest + 1
                        await self._register(held, source, operation, generation, intent)
                        await ensure_source_gate_live(held)
                        reserved = await session.writer.reserve(
                            expected_floor=floor,
                            operation_id=operation_id,
                            min_generation=generation - 1,
                        )
                        if type(reserved) is not int or reserved != generation:
                            raise _unavailable()
                        committed = await self._persist(
                            held, source, snapshot, operation, generation, target, intent
                        )
                assert committed is not None
                if committed.mode != "ordinary":
                    # Protected publication waits for the LANE-4 ingress admission change.
                    deferred = SourceAdminError("source_publication_deferred", 503)
                    deferred.committed_generation = str(committed.generation)
                    raise deferred
                published = await session.writer.publish_ordinary(
                    generation=committed.generation,
                    operation_id=str(committed.operation_id),
                    fingerprint=committed_policy_fingerprint(committed),
                )
                if published is not True:
                    raise _unavailable(committed)
            return committed
        except SourceAdminError as error:
            if error.status_code == 503 and committed is not None:
                error.committed_generation = str(committed.generation)
            raise
        except SourceAgentNotFound:
            raise SourceAdminError("source_agent_not_found", 404) from None
        except (
            SourceGateInvalid,
            SourceSnapshotUnavailable,
            SQLAlchemyError,
            SourceFenceConflict,
            SourceFenceExhausted,
            SourceFenceInvalid,
            SourcePolicyRecordInvalid,
            RedisError,
            AttributeError,
            TypeError,
            ValueError,
        ):
            raise _unavailable(committed) from None

    async def mutate(
        self,
        agent_id: str,
        hook: str,
        expected_generation: str,
        operation_id: str,
        desired_target: Mapping[str, object],
    ) -> SourcePolicySnapshot:
        """@spec PROTECTED-HOOK-SOURCE-3/10."""
        source = SourceIdentity(agent_id, hook)
        target = DesiredSourceTarget.from_mapping(desired_target)
        return await self._execute(source, expected_generation, operation_id, target)

    async def remove(
        self,
        agent_id: str,
        hook: str,
        expected_generation: str,
        operation_id: str,
        *,
        refuse_unconfigured: bool = False,
    ) -> SourcePolicySnapshot:
        """Target the ordinary tombstone.

        With ``refuse_unconfigured`` an absent row without attempt history is
        409 ``source_not_configured`` right after the agent lookup, while
        pending history alone still commits a fresh tombstone.
        @spec PROTECTED-HOOK-SOURCE-3/6/10.
        """
        return await self._execute(
            SourceIdentity(agent_id, hook),
            expected_generation,
            operation_id,
            DesiredSourceTarget("ordinary", None, None, None, None),
            refuse_unconfigured=refuse_unconfigured,
        )

    async def rotate(
        self, agent_id: str, hook: str, expected_generation: str, operation_id: str
    ) -> SourcePolicySnapshot:
        """@spec PROTECTED-HOOK-SOURCE-3/5/6/10."""
        return await self._execute(
            SourceIdentity(agent_id, hook), expected_generation, operation_id, None
        )
