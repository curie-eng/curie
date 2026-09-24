"""WorkItem admission, dispatch ownership, and runtime fencing."""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Literal, cast

from channel_protocol import scoped_conversation_id
from sqlalchemy import func, or_, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from . import workitems
from .config import get_settings
from .models import Agent, AgentChannel, ExecutionRequest, WorkItem
from .workitems import (
    ExecutionRequestSnapshot,
    WorkItemConflict,
    WorkItemOutcome,
    WorkItemSnapshot,
    _database_now,
    _lock_request,
    _lock_request_by_id,
    _lock_work_item,
    _outcome,
    _record_runtime_termination,
    _reload_request,
    _reload_work_item,
    _start_execution,
    _terminalize_execution,
)
from .workspace_policy import repository_is_allowed

RefusalCode = Literal[
    "not_found",
    "identity_mismatch",
    "active_request",
    "work_item_cancelled",
    "not_dispatchable",
    "not_published",
    "duplicate",
    "stale_owner",
    "not_running",
    "waiting_deadline_elapsed",
    "binding_missing",
    "repository_not_allowed",
    "publication_pending",
]

DispatchResult = WorkItemOutcome | WorkItemConflict


@dataclass(frozen=True)
class DispatchConflict:
    code: RefusalCode
    work_item_id: uuid.UUID | None = None
    request_id: uuid.UUID | None = None
    status: str | None = None


@dataclass(frozen=True)
class AcquireGrant:
    generation: int
    work_item_id: uuid.UUID
    conversation_id: str
    wait_deadline: datetime
    repo_full_name: str


@dataclass(frozen=True)
class DeferResult:
    dispatch_generation: int
    not_before: datetime


@dataclass(frozen=True)
class StartResult:
    runtime_epoch: int
    execution_deadline: datetime
    remaining_s: float
    heartbeat_interval_s: int


@dataclass(frozen=True)
class HeartbeatResult:
    status: str
    terminal_cause: str | None
    work_item_cancelled: bool


@dataclass(frozen=True)
class TerminationClaim:
    runtime_epoch: int


@dataclass(frozen=True)
class ClaimedDispatch:
    request_id: uuid.UUID
    epoch: int
    generation: int


@dataclass(frozen=True)
class TerminatePublish:
    request_id: uuid.UUID
    reply_kind: str
    reply_address: str
    reply_conversation_id: str
    requester: str | None


async def _refuse(
    session: AsyncSession,
    code: RefusalCode,
    *,
    work_item_id: uuid.UUID | None = None,
    request_id: uuid.UUID | None = None,
    status: str | None = None,
) -> DispatchConflict:
    result = DispatchConflict(
        code=code,
        work_item_id=work_item_id,
        request_id=request_id,
        status=status,
    )
    await session.commit()
    return result


def _facts_conversation(facts: Any) -> str:
    return scoped_conversation_id(
        facts.kind, facts.address, facts.reply_conversation_id
    )


def _snapshot_values(facts: Any) -> dict[str, str]:
    return {
        "objective": facts.objective,
        "requester": facts.requester,
        "reply_kind": facts.kind,
        "reply_address": facts.address,
        "reply_conversation_id": facts.reply_conversation_id,
    }


def _snapshot_matches(row: ExecutionRequest, facts: Any) -> bool:
    return bool(
        row.objective == facts.objective
        and row.requester == facts.requester
        and row.reply_kind == facts.kind
        and row.reply_address == facts.address
        and row.reply_conversation_id == facts.reply_conversation_id
    )


def _work_item_matches(item: WorkItem, facts: Any) -> bool:
    return (
        item.agent_id == facts.agent_id
        and item.repo_full_name == facts.repo_full_name
        and item.github_repository_id == facts.github_repository_id
        and item.github_issue_number == facts.github_issue_number
        and item.github_installation_id == facts.github_installation_id
        and item.conversation_id == _facts_conversation(facts)
    )


async def _write_snapshot(
    session: AsyncSession, request_id: uuid.UUID, facts: Any
) -> ExecutionRequest | DispatchConflict:
    await session.execute(
        update(ExecutionRequest)
        .where(
            ExecutionRequest.id == request_id,
            ExecutionRequest.objective.is_(None),
        )
        .values(
            **_snapshot_values(facts),
            updated_at=func.clock_timestamp(),
        )
    )
    row = await _reload_request(session, request_id)
    if not _snapshot_matches(row, facts):
        return await _refuse(
            session, "identity_mismatch", request_id=request_id
        )
    return row


async def _replay_existing(
    session: AsyncSession, request: ExecutionRequest, facts: Any
) -> WorkItemOutcome | DispatchConflict:
    work_item = await _lock_work_item(session, request.work_item_id)
    if work_item is None:
        return await _refuse(session, "not_found", request_id=request.id)
    locked = await _lock_request(
        session, work_item_id=work_item.id, request_id=request.id
    )
    if locked is None:
        return await _refuse(
            session, "not_found", work_item_id=work_item.id, request_id=request.id
        )
    if not _work_item_matches(work_item, facts):
        return await _refuse(
            session,
            "identity_mismatch",
            work_item_id=work_item.id,
            request_id=locked.id,
        )
    if locked.objective is None:
        written = await _write_snapshot(session, locked.id, facts)
        if isinstance(written, DispatchConflict):
            return written
        locked = written
    elif not _snapshot_matches(locked, facts):
        return await _refuse(
            session,
            "identity_mismatch",
            work_item_id=work_item.id,
            request_id=locked.id,
        )
    return await _outcome(session, work_item, locked, replayed=True)


async def admit(
    session: AsyncSession, facts: Any
) -> WorkItemOutcome | WorkItemConflict | DispatchConflict:
    agent = await session.get(Agent, facts.agent_id)
    if agent is None:
        return await _refuse(session, "not_found")
    binding = await session.scalar(
        select(AgentChannel).where(
            AgentChannel.agent_id == facts.agent_id,
            AgentChannel.kind == facts.kind,
            AgentChannel.address == facts.address,
        )
    )
    if binding is None:
        return await _refuse(session, "binding_missing")
    if not repository_is_allowed(
        facts.repo_full_name, get_settings().github_repo_allowlist
    ):
        return await _refuse(session, "repository_not_allowed")
    objective = facts.objective
    requester = facts.requester
    if (
        not isinstance(objective, str)
        or not 1 <= len(objective) <= 65536
        or not objective.strip()
        or not isinstance(requester, str)
        or not requester.strip()
    ):
        return await _refuse(session, "identity_mismatch")

    existing = await session.scalar(
        select(ExecutionRequest).where(ExecutionRequest.id == facts.request_id)
    )
    if existing is not None:
        return await _replay_existing(session, existing, facts)

    created = await workitems.create_or_get_work_item(
        session,
        github_repository_id=facts.github_repository_id,
        github_issue_number=facts.github_issue_number,
        github_installation_id=facts.github_installation_id,
        agent_id=facts.agent_id,
        repo_full_name=facts.repo_full_name,
        conversation_id=_facts_conversation(facts),
    )
    if isinstance(created, WorkItemConflict):
        return created
    now = await _database_now(session)
    wait_deadline = now + timedelta(
        seconds=get_settings().work_item_wait_budget_seconds
    )
    requested = await workitems.create_execution_request(
        session,
        work_item_id=created.work_item.id,
        request_id=facts.request_id,
        wait_deadline=wait_deadline,
        expected_work_item_version=created.work_item.version,
    )
    if isinstance(requested, WorkItemConflict):
        if requested.code == "identity_mismatch":
            existing = await session.scalar(
                select(ExecutionRequest).where(
                    ExecutionRequest.id == facts.request_id
                )
            )
            if existing is not None:
                return await _replay_existing(session, existing, facts)
        return requested
    assert requested.request is not None
    written = await _write_snapshot(session, requested.request.id, facts)
    if isinstance(written, DispatchConflict):
        return written
    work_item = await _reload_work_item(session, created.work_item.id)
    return await _outcome(
        session, work_item, written, replayed=requested.replayed
    )


async def claim_due(
    session: AsyncSession, owner: str, limit: int, lease_s: int
) -> list[ClaimedDispatch]:
    due = (
        select(ExecutionRequest.id)
        .where(
            ExecutionRequest.status == "waiting",
            ExecutionRequest.wait_deadline > func.clock_timestamp(),
            ExecutionRequest.dispatch_not_before <= func.clock_timestamp(),
            ExecutionRequest.objective.is_not(None),
            ExecutionRequest.published_generation.is_distinct_from(
                ExecutionRequest.dispatch_generation
            ),
            or_(
                ExecutionRequest.dispatch_lease_expires_at.is_(None),
                ExecutionRequest.dispatch_lease_expires_at
                <= func.clock_timestamp(),
            ),
        )
        .order_by(ExecutionRequest.dispatch_not_before)
        .limit(limit)
        .with_for_update(skip_locked=True)
    )
    rows = (
        await session.execute(
            update(ExecutionRequest)
            .where(ExecutionRequest.id.in_(due))
            .values(
                dispatch_owner=owner,
                dispatch_epoch=ExecutionRequest.dispatch_epoch + 1,
                dispatch_lease_expires_at=func.clock_timestamp()
                + timedelta(seconds=lease_s),
                updated_at=func.clock_timestamp(),
            )
            .returning(
                ExecutionRequest.id,
                ExecutionRequest.dispatch_epoch,
                ExecutionRequest.dispatch_generation,
            )
        )
    ).all()
    await session.commit()
    return [
        ClaimedDispatch(
            request_id=row.id, epoch=row.dispatch_epoch, generation=row.dispatch_generation
        )
        for row in rows
    ]


async def fence_published(
    session: AsyncSession, request_id: uuid.UUID, epoch: int, generation: int
) -> bool:
    changed_id = await session.scalar(
        update(ExecutionRequest)
        .where(
            ExecutionRequest.id == request_id,
            ExecutionRequest.dispatch_epoch == epoch,
            ExecutionRequest.dispatch_generation == generation,
            ExecutionRequest.status == "waiting",
        )
        .values(
            published_generation=generation,
            dispatch_owner=None,
            dispatch_lease_expires_at=None,
            updated_at=func.clock_timestamp(),
        )
        .returning(ExecutionRequest.id)
    )
    await session.commit()
    return changed_id is not None


def _generation_refusal(generation: int, dispatch_generation: int) -> RefusalCode:
    if generation < dispatch_generation:
        return "not_published"
    return "not_dispatchable"


async def _lock_pair(
    session: AsyncSession, request_id: uuid.UUID
) -> tuple[WorkItem, ExecutionRequest] | DispatchConflict:
    found = await session.scalar(
        select(ExecutionRequest).where(ExecutionRequest.id == request_id)
    )
    if found is None:
        return await _refuse(session, "not_found", request_id=request_id)
    work_item = await _lock_work_item(session, found.work_item_id)
    if work_item is None:
        return await _refuse(session, "not_found", request_id=request_id)
    request = await _lock_request(
        session, work_item_id=work_item.id, request_id=request_id
    )
    if request is None:
        return await _refuse(
            session, "not_found", work_item_id=work_item.id, request_id=request_id
        )
    return work_item, request


async def acquire(
    session: AsyncSession,
    request_id: uuid.UUID,
    *,
    owner: str,
    generation: int,
) -> AcquireGrant | DispatchConflict:
    locked = await _lock_pair(session, request_id)
    if isinstance(locked, DispatchConflict):
        return locked
    work_item, request = locked
    if work_item.cancelled_at is not None:
        return await _refuse(
            session,
            "work_item_cancelled",
            work_item_id=work_item.id,
            request_id=request.id,
        )
    if request.status != "waiting":
        return await _refuse(
            session,
            "not_dispatchable",
            work_item_id=work_item.id,
            request_id=request.id,
            status=request.status,
        )
    if generation != request.dispatch_generation:
        return await _refuse(
            session,
            _generation_refusal(generation, request.dispatch_generation),
            work_item_id=work_item.id,
            request_id=request.id,
        )
    now = await _database_now(session)
    if now >= request.wait_deadline:
        return await _refuse(
            session,
            "waiting_deadline_elapsed",
            work_item_id=work_item.id,
            request_id=request.id,
        )
    lease = timedelta(seconds=get_settings().work_item_acquire_lease_seconds)
    held = (
        request.acquire_owner is not None
        and request.acquired_generation == generation
        and request.acquire_expires_at is not None
        and request.acquire_expires_at > now
    )
    if held and request.acquire_owner != owner:
        return await _refuse(
            session,
            "duplicate",
            work_item_id=work_item.id,
            request_id=request.id,
        )
    if held and request.acquire_owner == owner:
        grant = AcquireGrant(
            generation=generation,
            work_item_id=work_item.id,
            conversation_id=work_item.conversation_id,
            wait_deadline=request.wait_deadline,
            repo_full_name=work_item.repo_full_name,
        )
        await session.execute(
            update(ExecutionRequest)
            .where(
                ExecutionRequest.id == request.id,
                ExecutionRequest.acquire_owner == owner,
                ExecutionRequest.acquired_generation == generation,
            )
            .values(
                acquire_expires_at=now + lease,
                updated_at=func.clock_timestamp(),
            )
        )
        await session.commit()
        return grant
    changed_id = await session.scalar(
        update(ExecutionRequest)
        .where(
            ExecutionRequest.id == request.id,
            ExecutionRequest.status == "waiting",
            ExecutionRequest.dispatch_generation == generation,
        )
        .values(
            acquired_generation=generation,
            acquire_owner=owner,
            acquire_expires_at=now + lease,
            updated_at=func.clock_timestamp(),
        )
        .returning(ExecutionRequest.id)
    )
    if changed_id is None:
        return await _refuse(
            session,
            "not_dispatchable",
            work_item_id=work_item.id,
            request_id=request.id,
        )
    grant = AcquireGrant(
        generation=generation,
        work_item_id=work_item.id,
        conversation_id=work_item.conversation_id,
        wait_deadline=request.wait_deadline,
        repo_full_name=work_item.repo_full_name,
    )
    await session.commit()
    return grant


async def defer(
    session: AsyncSession,
    request_id: uuid.UUID,
    *,
    owner: str,
    generation: int,
    reason: str,
    capacity: bool,
) -> DeferResult | DispatchConflict:
    request = await _lock_request_by_id(session, request_id)
    if request is None:
        return await _refuse(session, "not_found", request_id=request_id)
    if request.status != "waiting":
        return await _refuse(
            session,
            "not_dispatchable",
            request_id=request.id,
            status=request.status,
        )
    if generation != request.dispatch_generation:
        return await _refuse(
            session,
            _generation_refusal(generation, request.dispatch_generation),
            request_id=request.id,
        )
    now = await _database_now(session)
    if (
        request.acquire_owner != owner
        or request.acquired_generation != generation
        or request.acquire_expires_at is None
        or request.acquire_expires_at <= now
    ):
        return await _refuse(
            session, "not_dispatchable", request_id=request.id
        )
    settings = get_settings()
    if capacity:
        delay = min(
            settings.work_item_backoff_base_seconds
            * (2 ** request.capacity_deferrals),
            settings.work_item_backoff_max_seconds,
        )
        deferral_values: dict[str, Any] = {
            "capacity_deferrals": ExecutionRequest.capacity_deferrals + 1
        }
    else:
        delay = settings.work_item_backoff_base_seconds
        deferral_values = {}
    not_before = now + timedelta(seconds=delay)
    changed_id = await session.scalar(
        update(ExecutionRequest)
        .where(
            ExecutionRequest.id == request.id,
            ExecutionRequest.status == "waiting",
            ExecutionRequest.acquire_owner == owner,
            ExecutionRequest.acquired_generation == generation,
        )
        .values(
            dispatch_generation=ExecutionRequest.dispatch_generation + 1,
            acquired_generation=None,
            acquire_owner=None,
            acquire_expires_at=None,
            dispatch_not_before=not_before,
            last_deferral_reason=reason,
            updated_at=func.clock_timestamp(),
            **deferral_values,
        )
        .returning(ExecutionRequest.id)
    )
    if changed_id is None:
        return await _refuse(session, "not_dispatchable", request_id=request.id)
    request = await _reload_request(session, request.id)
    result = DeferResult(
        dispatch_generation=request.dispatch_generation,
        not_before=request.dispatch_not_before,
    )
    await session.commit()
    return result


async def start(
    session: AsyncSession,
    request_id: uuid.UUID,
    *,
    owner: str,
    generation: int,
    claim_name: str,
    sandbox_name: str,
) -> StartResult | DispatchConflict:
    locked = await _lock_pair(session, request_id)
    if isinstance(locked, DispatchConflict):
        return locked
    work_item, request = locked
    now = await _database_now(session)
    if (
        request.acquire_owner != owner
        or request.acquired_generation != generation
        or request.acquire_expires_at is None
        or request.acquire_expires_at <= now
    ):
        return await _refuse(
            session,
            "not_dispatchable",
            work_item_id=work_item.id,
            request_id=request.id,
        )
    settings = get_settings()
    ttl = timedelta(seconds=settings.work_item_runtime_ttl_seconds)
    result = await _start_execution(
        session,
        work_item_id=work_item.id,
        request_id=request.id,
        expected_work_item_version=work_item.version,
        expected_request_version=request.version,
        extra_where=(
            ExecutionRequest.acquire_owner == owner,
            ExecutionRequest.acquired_generation == generation,
            ExecutionRequest.acquire_expires_at > func.clock_timestamp(),
        ),
        extra_values={
            "execution_attempts": 1,
            "runtime_owner": owner,
            "runtime_epoch": ExecutionRequest.runtime_epoch + 1,
            "runtime_heartbeat_expires_at": func.clock_timestamp() + ttl,
            "runtime_claim_name": claim_name,
            "runtime_sandbox_name": sandbox_name,
            "acquired_generation": None,
            "acquire_owner": None,
            "acquire_expires_at": None,
        },
    )
    if isinstance(result, WorkItemConflict):
        mapped = _map_start_conflict(result)
        return DispatchConflict(
            code=mapped,
            work_item_id=result.work_item_id,
            request_id=result.request_id,
        )
    assert result.request is not None
    row = await _reload_request(session, result.request.id)
    assert row.execution_deadline is not None
    remaining = (row.execution_deadline - await _database_now(session)).total_seconds()
    return StartResult(
        runtime_epoch=row.runtime_epoch,
        execution_deadline=row.execution_deadline,
        remaining_s=max(0.0, remaining),
        heartbeat_interval_s=settings.work_item_runtime_ttl_seconds // 3,
    )


def _map_start_conflict(result: WorkItemConflict) -> RefusalCode:
    if result.code in {
        "not_found",
        "work_item_cancelled",
        "waiting_deadline_elapsed",
    }:
        return cast(RefusalCode, result.code)
    return "not_dispatchable"


async def heartbeat(
    session: AsyncSession, request_id: uuid.UUID, *, runtime_epoch: int
) -> HeartbeatResult | DispatchConflict:
    request = await _lock_request_by_id(session, request_id)
    if request is None:
        return await _refuse(session, "not_found", request_id=request_id)
    if request.status not in {"running", "cancellation_requested"}:
        return await _refuse(
            session,
            "stale_owner",
            request_id=request.id,
            status=request.status,
        )
    if request.runtime_epoch != runtime_epoch:
        return await _refuse(
            session,
            "stale_owner",
            request_id=request.id,
            status=request.status,
        )
    now = await _database_now(session)
    if (
        request.runtime_heartbeat_expires_at is not None
        and now >= request.runtime_heartbeat_expires_at
    ):
        return await _refuse(
            session,
            "stale_owner",
            request_id=request.id,
            status=request.status,
        )
    settings = get_settings()
    if request.status == "running":
        await session.execute(
            update(ExecutionRequest)
            .where(
                ExecutionRequest.id == request.id,
                ExecutionRequest.runtime_epoch == runtime_epoch,
                ExecutionRequest.status == "running",
            )
            .values(
                runtime_heartbeat_expires_at=func.clock_timestamp()
                + timedelta(seconds=settings.work_item_runtime_ttl_seconds),
                updated_at=func.clock_timestamp(),
            )
        )
        request = await _reload_request(session, request.id)
    work_item = await session.scalar(
        select(WorkItem).where(WorkItem.id == request.work_item_id)
    )
    cancelled = work_item is not None and work_item.cancelled_at is not None
    result = HeartbeatResult(
        status=request.status,
        terminal_cause=request.terminal_cause,
        work_item_cancelled=cancelled,
    )
    await session.commit()
    return result


async def hold_for_approval(
    session: AsyncSession, request_id: uuid.UUID, *, runtime_epoch: int
) -> HeartbeatResult | DispatchConflict:
    """Keep a suspended approval inside the 1800s execution bound.

    The worker stops refreshing the short runtime lease when the turn suspends.
    This sets that lease to the execution deadline. It does not finish the request.
    """

    request = await _lock_request_by_id(session, request_id)
    if request is None:
        return await _refuse(session, "not_found", request_id=request_id)
    if request.status != "running" or request.runtime_epoch != runtime_epoch:
        return await _refuse(
            session,
            "stale_owner",
            request_id=request.id,
            status=request.status,
        )
    now = await _database_now(session)
    if request.execution_deadline is None or now >= request.execution_deadline:
        return await _refuse(
            session,
            "not_running",
            request_id=request.id,
            status=request.status,
        )
    changed_id = await session.scalar(
        update(ExecutionRequest)
        .where(
            ExecutionRequest.id == request.id,
            ExecutionRequest.status == "running",
            ExecutionRequest.runtime_epoch == runtime_epoch,
            ExecutionRequest.execution_deadline > func.clock_timestamp(),
        )
        .values(
            runtime_heartbeat_expires_at=ExecutionRequest.execution_deadline,
            updated_at=func.clock_timestamp(),
        )
        .returning(ExecutionRequest.id)
    )
    if changed_id is None:
        return await _refuse(
            session,
            "stale_owner",
            request_id=request.id,
            status=request.status,
        )
    request = await _reload_request(session, request.id)
    work_item = await session.scalar(select(WorkItem).where(WorkItem.id == request.work_item_id))
    result = HeartbeatResult(
        status=request.status,
        terminal_cause=request.terminal_cause,
        work_item_cancelled=work_item is not None and work_item.cancelled_at is not None,
    )
    await session.commit()
    return result


def _map_finish_conflict(
    result: WorkItemConflict, request: ExecutionRequest | None
) -> RefusalCode:
    if result.code == "not_found":
        return "not_found"
    if result.code == "work_item_cancelled":
        return "work_item_cancelled"
    if result.code == "stale_version":
        return "stale_owner"
    if result.code == "publication_pending":
        return "publication_pending"
    if request is not None and request.status == "cancellation_requested":
        if request.terminal_cause == "issue_cancelled":
            return "work_item_cancelled"
        return "not_running"
    return "not_running"


async def finish(
    session: AsyncSession,
    request_id: uuid.UUID,
    *,
    runtime_epoch: int,
    outcome: Literal["completed", "failed"],
    cause: str,
    detail: str | None,
) -> WorkItemOutcome | DispatchConflict:
    locked = await _lock_pair(session, request_id)
    if isinstance(locked, DispatchConflict):
        return locked
    work_item, request = locked
    if request.runtime_epoch != runtime_epoch:
        return await _refuse(
            session,
            "stale_owner",
            work_item_id=work_item.id,
            request_id=request.id,
            status=request.status,
        )
    now = await _database_now(session)
    if (
        request.runtime_heartbeat_expires_at is not None
        and now >= request.runtime_heartbeat_expires_at
    ):
        return await _refuse(
            session,
            "stale_owner",
            work_item_id=work_item.id,
            request_id=request.id,
            status=request.status,
        )
    result = await _terminalize_execution(
        session,
        work_item_id=work_item.id,
        request_id=request.id,
        expected_work_item_version=work_item.version,
        expected_request_version=request.version,
        status=outcome,
        cause=cause,
        detail=detail,
        extra_where=(ExecutionRequest.runtime_epoch == runtime_epoch,),
    )
    if isinstance(result, WorkItemConflict):
        # The conflict path commits and expires this row.
        await session.refresh(request)
        return DispatchConflict(
            code=_map_finish_conflict(result, request),
            work_item_id=result.work_item_id,
            request_id=result.request_id,
            status=request.status,
        )
    return result


async def claim_termination(
    session: AsyncSession, request_id: uuid.UUID, *, owner: str
) -> TerminationClaim | DispatchConflict:
    request = await _lock_request_by_id(session, request_id)
    if request is None:
        return await _refuse(session, "not_found", request_id=request_id)
    if request.status != "cancellation_requested":
        return await _refuse(
            session,
            "not_running",
            request_id=request.id,
            status=request.status,
        )
    now = await _database_now(session)
    heartbeat_live = (
        request.runtime_heartbeat_expires_at is not None
        and request.runtime_heartbeat_expires_at > now
    )
    allowed = (
        request.runtime_owner is None
        or not heartbeat_live
        or request.runtime_owner == owner
    )
    if not allowed:
        return await _refuse(session, "duplicate", request_id=request.id)
    settings = get_settings()
    changed_id = await session.scalar(
        update(ExecutionRequest)
        .where(
            ExecutionRequest.id == request.id,
            ExecutionRequest.status == "cancellation_requested",
        )
        .values(
            runtime_owner=owner,
            runtime_epoch=ExecutionRequest.runtime_epoch + 1,
            runtime_heartbeat_expires_at=func.clock_timestamp()
            + timedelta(seconds=settings.work_item_runtime_ttl_seconds),
            updated_at=func.clock_timestamp(),
        )
        .returning(ExecutionRequest.id)
    )
    if changed_id is None:
        return await _refuse(session, "duplicate", request_id=request.id)
    request = await _reload_request(session, request.id)
    claimed = TerminationClaim(runtime_epoch=request.runtime_epoch)
    await session.commit()
    return claimed


async def record_termination(
    session: AsyncSession,
    request_id: uuid.UUID,
    *,
    runtime_epoch: int,
    observation: str,
) -> WorkItemOutcome | DispatchConflict:
    locked = await _lock_pair(session, request_id)
    if isinstance(locked, DispatchConflict):
        return locked
    work_item, request = locked
    if request.runtime_epoch != runtime_epoch:
        return await _refuse(
            session,
            "stale_owner",
            work_item_id=work_item.id,
            request_id=request.id,
            status=request.status,
        )
    result = await _record_runtime_termination(
        session,
        work_item_id=work_item.id,
        request_id=request.id,
        termination_observation=observation,
        expected_work_item_version=work_item.version,
        expected_request_version=request.version,
        extra_where=(ExecutionRequest.runtime_epoch == runtime_epoch,),
    )
    if isinstance(result, WorkItemConflict):
        # The conflict path commits and expires this row.
        await session.refresh(request)
        return DispatchConflict(
            code=_map_finish_conflict(result, request),
            work_item_id=result.work_item_id,
            request_id=result.request_id,
            status=request.status,
        )
    return result


async def cancel(
    session: AsyncSession,
    *,
    work_item_id: uuid.UUID,
    expected_version: int,
) -> WorkItemOutcome | WorkItemConflict:
    return await workitems.request_cancellation(
        session,
        work_item_id=work_item_id,
        expected_work_item_version=expected_version,
    )


async def running_for_conversation(
    session: AsyncSession, conversation_id: str
) -> tuple[Literal["absent", "ended", "running"], ExecutionRequest | None]:
    """The execution whose work item uses this scoped conversation.

    ``absent`` is an ordinary approval with no factory run. ``ended`` means
    the run already finished, so a resume must not start another turn.
    ``running`` is the request the continuation still owns.
    """

    work_item = await session.scalar(
        select(WorkItem).where(WorkItem.conversation_id == conversation_id)
    )
    if work_item is None:
        return "absent", None
    running = cast(
        ExecutionRequest | None,
        await session.scalar(
            select(ExecutionRequest).where(
                ExecutionRequest.work_item_id == work_item.id,
                ExecutionRequest.status == "running",
                ExecutionRequest.runtime_epoch.is_not(None),
                ExecutionRequest.execution_deadline.is_not(None),
            )
        ),
    )
    if running is None:
        return "ended", None
    return "running", running


async def get_request(
    session: AsyncSession, request_id: uuid.UUID
) -> ExecutionRequest | None:
    return cast(
        ExecutionRequest | None,
        await session.scalar(
            select(ExecutionRequest).where(ExecutionRequest.id == request_id)
        ),
    )


async def redispatch_lapsed_acquisitions(session: AsyncSession) -> int:
    settings = get_settings()
    not_before = func.clock_timestamp() + timedelta(
        seconds=settings.work_item_backoff_base_seconds
    )
    changed = await session.scalars(
        update(ExecutionRequest)
        .where(
            ExecutionRequest.status == "waiting",
            ExecutionRequest.acquired_generation
            == ExecutionRequest.dispatch_generation,
            ExecutionRequest.acquire_expires_at.is_not(None),
            ExecutionRequest.acquire_expires_at <= func.clock_timestamp(),
        )
        .values(
            dispatch_generation=ExecutionRequest.dispatch_generation + 1,
            acquired_generation=None,
            acquire_owner=None,
            acquire_expires_at=None,
            last_deferral_reason="acquire_lost",
            dispatch_not_before=not_before,
            updated_at=func.clock_timestamp(),
        )
        .returning(ExecutionRequest.id)
    )
    count = len(list(changed.all()))
    await session.commit()
    return count


async def claim_terminate_publishes(
    session: AsyncSession, *, retry_seconds: int, limit: int
) -> list[TerminatePublish]:
    due = (
        select(ExecutionRequest.id)
        .where(
            ExecutionRequest.status == "cancellation_requested",
            ExecutionRequest.reply_kind.is_not(None),
            or_(
                ExecutionRequest.runtime_owner.is_(None),
                ExecutionRequest.runtime_heartbeat_expires_at.is_(None),
                ExecutionRequest.runtime_heartbeat_expires_at
                <= func.clock_timestamp(),
            ),
            or_(
                ExecutionRequest.terminate_published_at.is_(None),
                ExecutionRequest.terminate_published_at
                <= func.clock_timestamp() - timedelta(seconds=retry_seconds),
            ),
        )
        .order_by(ExecutionRequest.updated_at)
        .limit(limit)
        .with_for_update(skip_locked=True)
    )
    rows = (
        await session.execute(
            update(ExecutionRequest)
            .where(ExecutionRequest.id.in_(due))
            .values(
                terminate_published_at=func.clock_timestamp(),
                updated_at=func.clock_timestamp(),
            )
            .returning(
                ExecutionRequest.id,
                ExecutionRequest.reply_kind,
                ExecutionRequest.reply_address,
                ExecutionRequest.reply_conversation_id,
                ExecutionRequest.requester,
            )
        )
    ).all()
    await session.commit()
    published: list[TerminatePublish] = []
    for row in rows:
        if row.reply_kind is None or row.reply_address is None:
            continue
        if row.reply_conversation_id is None:
            continue
        published.append(
            TerminatePublish(
                request_id=row.id,
                reply_kind=row.reply_kind,
                reply_address=row.reply_address,
                reply_conversation_id=row.reply_conversation_id,
                requester=row.requester,
            )
        )
    return published


async def load_execute_wake(
    session: AsyncSession, request_id: uuid.UUID
) -> tuple[ExecutionRequest, WorkItem, AgentChannel | None] | None:
    request = await session.scalar(
        select(ExecutionRequest).where(ExecutionRequest.id == request_id)
    )
    if request is None:
        return None
    work_item = await session.scalar(
        select(WorkItem).where(WorkItem.id == request.work_item_id)
    )
    if work_item is None:
        return None
    binding = None
    if request.reply_kind is not None and request.reply_address is not None:
        binding = await session.scalar(
            select(AgentChannel).where(
                AgentChannel.agent_id == work_item.agent_id,
                AgentChannel.kind == request.reply_kind,
                AgentChannel.address == request.reply_address,
            )
        )
    return request, work_item, binding


def request_view(row: ExecutionRequest) -> dict[str, Any]:
    return {
        "id": row.id,
        "work_item_id": row.work_item_id,
        "sequence": row.sequence,
        "status": row.status,
        "wait_deadline": row.wait_deadline,
        "started_at": row.started_at,
        "execution_deadline": row.execution_deadline,
        "terminal_at": row.terminal_at,
        "terminal_cause": row.terminal_cause,
        "termination_observation": row.termination_observation,
        "version": row.version,
        "dispatch_generation": row.dispatch_generation,
        "published_generation": row.published_generation,
        "dispatch_not_before": row.dispatch_not_before,
        "capacity_deferrals": row.capacity_deferrals,
        "last_deferral_reason": row.last_deferral_reason,
        "execution_attempts": row.execution_attempts,
        "runtime_owner": row.runtime_owner,
        "runtime_epoch": row.runtime_epoch,
        "runtime_heartbeat_expires_at": row.runtime_heartbeat_expires_at,
        "runtime_claim_name": row.runtime_claim_name,
        "runtime_sandbox_name": row.runtime_sandbox_name,
        "objective": row.objective,
        "requester": row.requester,
        "reply_kind": row.reply_kind,
        "reply_address": row.reply_address,
        "reply_conversation_id": row.reply_conversation_id,
    }


def work_item_view(row: WorkItemSnapshot) -> dict[str, Any]:
    return {
        "id": row.id,
        "github_repository_id": row.github_repository_id,
        "github_issue_number": row.github_issue_number,
        "github_installation_id": row.github_installation_id,
        "agent_id": row.agent_id,
        "repo_full_name": row.repo_full_name,
        "conversation_id": row.conversation_id,
        "publication_lineage_id": row.publication_lineage_id,
        "cancelled_at": row.cancelled_at,
        "version": row.version,
        "next_sequence": row.next_sequence,
    }


def request_snapshot_view(row: ExecutionRequestSnapshot) -> dict[str, Any]:
    return {
        "id": row.id,
        "work_item_id": row.work_item_id,
        "sequence": row.sequence,
        "status": row.status,
        "wait_deadline": row.wait_deadline,
        "started_at": row.started_at,
        "execution_deadline": row.execution_deadline,
        "terminal_at": row.terminal_at,
        "terminal_cause": row.terminal_cause,
        "termination_observation": row.termination_observation,
        "version": row.version,
    }
