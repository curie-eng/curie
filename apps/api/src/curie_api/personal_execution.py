"""Who a factory execution runs as: the WorkItem owner tuple (#5376, ADR 0268).

A WorkItem carries one tuple, ``tenant_id``, ``model_auth_mode`` (``service``
or ``personal``), ``owner_principal_id`` and ``enrollment_generation``, and every
ExecutionRequest snapshots it at insertion.

1. The tuple is fixed when the WorkItem is created, from validated admission
   facts. A service tuple names the agent's tenant and no owner. A personal
   tuple names an owner the trusted internal admission supplied, verified in
   the inserting transaction: the owner's tenant is the agent's and active,
   the principal is active in it, and its ``personal_claude_enrollments`` row
   is active at exactly the named generation. Personal mode is never inferred
   from an enrollment, and no requester text, issue text or event author is
   read to choose an owner.
2. Every request copies the locked WorkItem's tuple (``snapshot_columns``).
3. Facts arriving for an existing WorkItem never change its tuple. Service
   facts reuse it; personal facts without an owner reuse it only when it is
   already personal; an explicit owner must equal it after full validation.
4. Only ``revalidate`` moves ``enrollment_generation``, under compare-and-set
   versions, once no runtime of the WorkItem can still be live. The owner never
   changes.

The functions here read and lock rows in the caller's transaction and never
commit, except ``revalidate``, which owns its transaction.
"""

from __future__ import annotations

import uuid
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Literal, cast

from sqlalchemy import func, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from .models import Agent, ExecutionRequest, PersonalClaudeEnrollment, Principal, Tenant, WorkItem

# Declared beside the operator read schemas that serialize it.
from .schemas.workitems import ModelAuthMode as ModelAuthMode

OwnerRefusalCode = Literal[
    "personal_owner_required",
    "owner_context_unexpected",
    "owner_tenant_mismatch",
    "owner_tenant_inactive",
    "owner_principal_missing",
    "owner_principal_inactive",
    "enrollment_inactive",
    "enrollment_generation_mismatch",
    "owner_mismatch",
]

# What an explicit owner that passed validation can still conflict on.
OwnerMismatchCode = Literal["owner_mismatch", "enrollment_generation_mismatch"]

ContextConflictCode = Literal[
    "not_found",
    "owner_mismatch",
    "stale_version",
    "runtime_live",
    "enrollment_inactive",
    "generation_current",
]

# The request columns ``snapshot_columns`` owns; a caller's extra columns never
# name them.
OWNER_COLUMNS = frozenset(
    {"tenant_id", "model_auth_mode", "owner_principal_id", "enrollment_generation"}
)

# A request that may still have a live runtime: running or stopping, or waiting
# under an unexpired acquire lease (a worker is starting it).
_LIVE_STATUSES = ("running", "cancellation_requested")
# Requests revalidation rebinds; a terminal snapshot keeps the generation it ran under.
_REBOUND_STATUSES = ("queued", "waiting")


@dataclass(frozen=True)
class OwnerContext:
    """An owner the trusted internal admission names. Never derived from text."""

    tenant_id: uuid.UUID
    principal_id: uuid.UUID
    enrollment_generation: int


@dataclass(frozen=True)
class OwnerTuple:
    """The persisted owner facts of one WorkItem or request snapshot."""

    tenant_id: uuid.UUID | None
    model_auth_mode: ModelAuthMode
    owner_principal_id: uuid.UUID | None
    enrollment_generation: int | None

    @classmethod
    def personal(cls, owner: OwnerContext) -> OwnerTuple:
        return cls(owner.tenant_id, "personal", owner.principal_id, owner.enrollment_generation)

    @classmethod
    def of(cls, row: WorkItem | ExecutionRequest) -> OwnerTuple:
        return cls(
            row.tenant_id,
            mode_of(row.model_auth_mode),
            row.owner_principal_id,
            row.enrollment_generation,
        )

    @property
    def is_personal(self) -> bool:
        return self.model_auth_mode == "personal"

    def columns(self) -> dict[str, Any]:
        return {
            "tenant_id": self.tenant_id,
            "model_auth_mode": self.model_auth_mode,
            "owner_principal_id": self.owner_principal_id,
            "enrollment_generation": self.enrollment_generation,
        }


@dataclass(frozen=True)
class OwnerRefusal:
    code: OwnerRefusalCode


def mode_of(value: str) -> ModelAuthMode:
    """A stored mode; the column CHECK admits only these two."""

    if value not in ("service", "personal"):
        raise ValueError(f"unknown model_auth_mode {value!r}")
    return cast(ModelAuthMode, value)


def snapshot_columns(work_item: WorkItem) -> dict[str, Any]:
    """The owner tuple a new request of ``work_item`` copies, from the locked row."""

    return OwnerTuple.of(work_item).columns()


def assert_no_owner_columns(columns: Mapping[str, Any]) -> None:
    """A request's owner facts come only from its WorkItem, never a caller."""

    named = OWNER_COLUMNS & set(columns)
    assert not named, f"request columns name owner facts: {sorted(named)}"


def facts_owner(facts: Any) -> tuple[ModelAuthMode, OwnerContext | None]:
    """The mode and owner admission facts carry; facts naming neither are service."""

    mode: ModelAuthMode = getattr(facts, "model_auth_mode", "service")
    owner: OwnerContext | None = getattr(facts, "owner", None)
    return mode, owner


# --- admission ----------------------------------------------------------------------


async def _agent_tenant(session: AsyncSession, agent_id: uuid.UUID) -> uuid.UUID | None:
    tenant_id: uuid.UUID | None = await session.scalar(
        select(Agent.tenant_id).where(Agent.id == agent_id)
    )
    return tenant_id


async def _verify(
    session: AsyncSession, owner: OwnerContext, *, agent_id: uuid.UUID
) -> OwnerRefusalCode | None:
    """Full validation of an owner context against the admitted agent.

    Each row is read ``FOR SHARE`` so it cannot change before the caller's
    transaction commits the WorkItem or request it admits.
    """

    if owner.tenant_id != await _agent_tenant(session, agent_id):
        return "owner_tenant_mismatch"
    tenant_status: str | None = await session.scalar(
        select(Tenant.status).where(Tenant.id == owner.tenant_id).with_for_update(read=True)
    )
    if tenant_status != "active":
        return "owner_tenant_inactive"
    principal_status: str | None = await session.scalar(
        select(Principal.status)
        .where(Principal.tenant_id == owner.tenant_id, Principal.id == owner.principal_id)
        .with_for_update(read=True)
    )
    if principal_status is None:
        return "owner_principal_missing"
    if principal_status != "active":
        return "owner_principal_inactive"
    enrollment = (
        await session.execute(
            select(PersonalClaudeEnrollment.generation, PersonalClaudeEnrollment.active)
            .where(
                PersonalClaudeEnrollment.tenant_id == owner.tenant_id,
                PersonalClaudeEnrollment.principal_id == owner.principal_id,
            )
            .with_for_update(read=True)
        )
    ).one_or_none()
    if enrollment is None or not enrollment.active:
        return "enrollment_inactive"
    if enrollment.generation != owner.enrollment_generation:
        return "enrollment_generation_mismatch"
    return None


async def new_work_item_owner(
    session: AsyncSession, facts: Any
) -> OwnerTuple | OwnerRefusal | None:
    """The tuple a WorkItem these facts create is bound to, or the refusal.

    None is a service WorkItem, whose tenant ``create_or_get_work_item`` reads
    from the agent as it inserts.
    """

    mode, owner = facts_owner(facts)
    if owner is not None and mode != "personal":
        return OwnerRefusal("owner_context_unexpected")
    if mode != "personal":
        return None
    if owner is None:
        return OwnerRefusal("personal_owner_required")
    refused = await _verify(session, owner, agent_id=facts.agent_id)
    if refused is not None:
        return OwnerRefusal(refused)
    return OwnerTuple.personal(owner)


async def existing_work_item_refusal(
    session: AsyncSession, work_item: WorkItem, facts: Any
) -> OwnerRefusal | None:
    """Whether facts for an existing WorkItem conflict with its tuple (never changed)."""

    mode, owner = facts_owner(facts)
    if owner is not None and mode != "personal":
        return OwnerRefusal("owner_context_unexpected")
    if owner is None:
        # The persisted owner is the context verified when the WorkItem was made.
        if mode == "personal" and work_item.model_auth_mode != "personal":
            return OwnerRefusal("personal_owner_required")
        return None
    refused = await _verify(session, owner, agent_id=facts.agent_id)
    if refused is not None:
        return OwnerRefusal(refused)
    mismatch = compare_owner(OwnerTuple.of(work_item), OwnerTuple.personal(owner))
    return None if mismatch is None else OwnerRefusal(mismatch)


def compare_owner(persisted: OwnerTuple, named: OwnerTuple) -> OwnerMismatchCode | None:
    """An explicit personal owner must equal the persisted tuple exactly."""

    if (persisted.model_auth_mode, persisted.tenant_id, persisted.owner_principal_id) != (
        named.model_auth_mode,
        named.tenant_id,
        named.owner_principal_id,
    ):
        return "owner_mismatch"
    if persisted.enrollment_generation != named.enrollment_generation:
        return "enrollment_generation_mismatch"
    return None


# --- the worker's context read and revalidation ------------------------------------------


@dataclass(frozen=True)
class ContextRequest:
    id: uuid.UUID
    sequence: int
    status: str
    version: int
    enrollment_generation: int | None


@dataclass(frozen=True)
class ContextEnrollment:
    generation: int
    active: bool
    version: int


@dataclass(frozen=True)
class PersonalContext:
    """Token-free owner, generation and version facts of one personal WorkItem."""

    work_item_id: uuid.UUID
    agent_id: uuid.UUID
    tenant_id: uuid.UUID
    model_auth_mode: ModelAuthMode
    owner_principal_id: uuid.UUID
    enrollment_generation: int
    work_item_version: int
    request: ContextRequest | None
    enrollment: ContextEnrollment | None


@dataclass(frozen=True)
class Revalidated:
    work_item_id: uuid.UUID
    request_id: uuid.UUID
    previous_generation: int
    enrollment_generation: int
    work_item_version: int
    request_version: int
    enrollment_version: int


@dataclass(frozen=True)
class ContextConflict:
    code: ContextConflictCode


def _scoped(
    work_item: WorkItem | None,
    *,
    tenant_id: uuid.UUID,
    agent_id: uuid.UUID,
) -> bool:
    return (
        work_item is not None
        and work_item.model_auth_mode == "personal"
        and work_item.tenant_id == tenant_id
        and work_item.agent_id == agent_id
    )


async def read_context(
    session: AsyncSession,
    *,
    work_item_id: uuid.UUID,
    tenant_id: uuid.UUID,
    agent_id: uuid.UUID,
    owner_principal_id: uuid.UUID,
) -> PersonalContext | None:
    """The personal WorkItem scoped by tenant, agent and owner, or None.

    None for a missing, other-tenant, other-agent, other-owner or service item
    alike, so the caller answers them with one response. ``request`` is the
    latest by sequence; ``enrollment`` is the owner's row, or None when absent.
    """

    work_item = await session.scalar(select(WorkItem).where(WorkItem.id == work_item_id))
    if (
        work_item is None
        or not _scoped(work_item, tenant_id=tenant_id, agent_id=agent_id)
        or work_item.owner_principal_id != owner_principal_id
    ):
        return None
    assert work_item.owner_principal_id is not None
    assert work_item.tenant_id is not None and work_item.enrollment_generation is not None
    latest = await session.scalar(
        select(ExecutionRequest)
        .where(ExecutionRequest.work_item_id == work_item.id)
        .order_by(ExecutionRequest.sequence.desc())
        .limit(1)
    )
    enrollment = await session.scalar(
        select(PersonalClaudeEnrollment).where(
            PersonalClaudeEnrollment.tenant_id == work_item.tenant_id,
            PersonalClaudeEnrollment.principal_id == work_item.owner_principal_id,
        )
    )
    return PersonalContext(
        work_item_id=work_item.id,
        agent_id=work_item.agent_id,
        tenant_id=work_item.tenant_id,
        model_auth_mode="personal",
        owner_principal_id=work_item.owner_principal_id,
        enrollment_generation=work_item.enrollment_generation,
        work_item_version=work_item.version,
        request=(
            ContextRequest(
                id=latest.id,
                sequence=latest.sequence,
                status=latest.status,
                version=latest.version,
                enrollment_generation=latest.enrollment_generation,
            )
            if latest is not None
            else None
        ),
        enrollment=(
            ContextEnrollment(
                generation=enrollment.generation,
                active=enrollment.active,
                version=enrollment.version,
            )
            if enrollment is not None
            else None
        ),
    )


async def _conflict(session: AsyncSession, code: ContextConflictCode) -> ContextConflict:
    await session.commit()
    return ContextConflict(code)


def _runtime_proven_gone(row: ExecutionRequest) -> bool:
    """Whether a started request carries worker-recorded termination evidence.

    A normal finish, a failed finish and an expired heartbeat are not evidence:
    worker sandbox cleanup after a finish is best effort, so the sandbox may
    outlive it. An explicit epoch-bound termination confirmation for normally
    finished runs is tracked in issue #5378.
    """

    return (
        row.terminal_at is not None
        and row.termination_observation is not None
        and bool(row.termination_observation.strip())
        and row.teardown_unconfirmed_at is None
    )


async def revalidate(
    session: AsyncSession,
    *,
    work_item_id: uuid.UUID,
    tenant_id: uuid.UUID,
    agent_id: uuid.UUID,
    owner_principal_id: uuid.UUID,
    request_id: uuid.UUID,
    expected_work_item_version: int,
    expected_request_version: int,
    expected_enrollment_version: int,
) -> Revalidated | ContextConflict:
    """Bind a personal WorkItem to its owner's current enrollment generation, once.

    In one transaction, with the WorkItem, every request of it and the
    enrollment locked: the caller's WorkItem, request and enrollment versions
    must be current, the owner the WorkItem's, no runtime live (running,
    stopping, holding an acquire lease, force-settled without a teardown
    receipt, or started without a worker-recorded runtime termination), and
    the enrollment active at a generation other than the bound one. It fails
    closed: a terminal status alone, such as a normal finish, does not prove
    the runtime stopped. Success rebinds the WorkItem and its queued and
    waiting requests and bumps their versions; terminal snapshots keep their
    generation. Commits.
    """

    work_item = await session.scalar(
        select(WorkItem)
        .where(WorkItem.id == work_item_id)
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    if work_item is None or not _scoped(work_item, tenant_id=tenant_id, agent_id=agent_id):
        return await _conflict(session, "not_found")
    if work_item.owner_principal_id != owner_principal_id:
        return await _conflict(session, "owner_mismatch")
    requests = list(
        await session.scalars(
            select(ExecutionRequest)
            .where(ExecutionRequest.work_item_id == work_item.id)
            .order_by(ExecutionRequest.sequence)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
    )
    named = next((row for row in requests if row.id == request_id), None)
    if named is None:
        return await _conflict(session, "not_found")
    if work_item.version != expected_work_item_version or named.version != expected_request_version:
        return await _conflict(session, "stale_version")
    now = cast(datetime, await session.scalar(select(func.clock_timestamp())))
    if any(
        row.status in _LIVE_STATUSES
        or row.teardown_unconfirmed_at is not None
        or (
            row.status == "waiting"
            and row.acquire_expires_at is not None
            and row.acquire_expires_at > now
        )
        or (row.started_at is not None and not _runtime_proven_gone(row))
        for row in requests
    ):
        return await _conflict(session, "runtime_live")
    enrollment = await session.scalar(
        select(PersonalClaudeEnrollment)
        .where(
            PersonalClaudeEnrollment.tenant_id == work_item.tenant_id,
            PersonalClaudeEnrollment.principal_id == work_item.owner_principal_id,
        )
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    if enrollment is None or not enrollment.active:
        return await _conflict(session, "enrollment_inactive")
    if enrollment.version != expected_enrollment_version:
        return await _conflict(session, "stale_version")
    previous = work_item.enrollment_generation
    assert previous is not None
    if enrollment.generation == previous:
        return await _conflict(session, "generation_current")

    work_item_version: int | None = await session.scalar(
        update(WorkItem)
        .where(WorkItem.id == work_item.id, WorkItem.version == expected_work_item_version)
        .values(
            enrollment_generation=enrollment.generation,
            version=WorkItem.version + 1,
            updated_at=func.clock_timestamp(),
        )
        .returning(WorkItem.version)
    )
    assert work_item_version is not None
    await session.execute(
        update(ExecutionRequest)
        .where(
            ExecutionRequest.work_item_id == work_item.id,
            ExecutionRequest.status.in_(_REBOUND_STATUSES),
        )
        .values(
            enrollment_generation=enrollment.generation,
            version=ExecutionRequest.version + 1,
            updated_at=func.clock_timestamp(),
        )
    )
    request_version: int | None = await session.scalar(
        select(ExecutionRequest.version).where(ExecutionRequest.id == named.id)
    )
    assert request_version is not None
    result = Revalidated(
        work_item_id=work_item.id,
        request_id=named.id,
        previous_generation=previous,
        enrollment_generation=enrollment.generation,
        work_item_version=work_item_version,
        request_version=request_version,
        enrollment_version=enrollment.version,
    )
    await session.commit()
    return result
