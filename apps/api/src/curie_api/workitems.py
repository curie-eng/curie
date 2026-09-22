"""Transactional lifecycle operations for durable WorkItems.

Each public operation owns and commits the supplied session transaction,
including conflict outcomes. Callers should dedicate the session to one operation
and avoid attaching unrelated pending work.
"""

from __future__ import annotations

import uuid
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Literal, cast

from sqlalchemy import func, select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql.elements import ColumnElement

from .config import get_settings
from .models import (
    ExecutionRequest,
    FactoryTerminalNotice,
    Publication,
    ThreadPublicationLineage,
    WorkItem,
)

RequestStatus = Literal[
    "waiting",
    "running",
    "cancellation_requested",
    "completed",
    "failed",
    "expired",
    "cancelled",
]

ConflictCode = Literal[
    "not_found",
    "identity_mismatch",
    "stale_version",
    "active_request",
    "work_item_cancelled",
    "illegal_transition",
    "waiting_deadline_elapsed",
    "execution_deadline_elapsed",
    "lineage_mismatch",
    "lineage_already_owned",
    "publication_ineligible",
    "publication_pending",
    "termination_observation_required",
]

_ACTIVE_STATUSES = ("waiting", "running", "cancellation_requested")


@dataclass(frozen=True)
class WorkItemSnapshot:
    id: uuid.UUID
    github_repository_id: int
    github_issue_number: int
    github_installation_id: int
    agent_id: uuid.UUID
    repo_full_name: str
    conversation_id: str
    publication_lineage_id: uuid.UUID | None
    cancelled_at: datetime | None
    version: int
    next_sequence: int


@dataclass(frozen=True)
class ExecutionRequestSnapshot:
    id: uuid.UUID
    work_item_id: uuid.UUID
    sequence: int
    status: RequestStatus
    wait_deadline: datetime
    started_at: datetime | None
    execution_deadline: datetime | None
    terminal_at: datetime | None
    terminal_cause: str | None
    termination_observation: str | None
    version: int


@dataclass(frozen=True)
class WorkItemOutcome:
    work_item: WorkItemSnapshot
    request: ExecutionRequestSnapshot | None
    replayed: bool = False


@dataclass(frozen=True)
class WorkItemConflict:
    code: ConflictCode
    work_item_id: uuid.UUID | None = None
    request_id: uuid.UUID | None = None
    work_item_version: int | None = None
    request_version: int | None = None


WorkItemResult = WorkItemOutcome | WorkItemConflict


def _work_item_snapshot(row: WorkItem) -> WorkItemSnapshot:
    return WorkItemSnapshot(
        id=row.id,
        github_repository_id=row.github_repository_id,
        github_issue_number=row.github_issue_number,
        github_installation_id=row.github_installation_id,
        agent_id=row.agent_id,
        repo_full_name=row.repo_full_name,
        conversation_id=row.conversation_id,
        publication_lineage_id=row.publication_lineage_id,
        cancelled_at=row.cancelled_at,
        version=row.version,
        next_sequence=row.next_sequence,
    )


def _request_snapshot(row: ExecutionRequest) -> ExecutionRequestSnapshot:
    return ExecutionRequestSnapshot(
        id=row.id,
        work_item_id=row.work_item_id,
        sequence=row.sequence,
        status=cast(RequestStatus, row.status),
        wait_deadline=row.wait_deadline,
        started_at=row.started_at,
        execution_deadline=row.execution_deadline,
        terminal_at=row.terminal_at,
        terminal_cause=row.terminal_cause,
        termination_observation=row.termination_observation,
        version=row.version,
    )


async def _outcome(
    session: AsyncSession,
    work_item: WorkItem,
    request: ExecutionRequest | None,
    *,
    replayed: bool = False,
) -> WorkItemOutcome:
    result = WorkItemOutcome(
        work_item=_work_item_snapshot(work_item),
        request=_request_snapshot(request) if request is not None else None,
        replayed=replayed,
    )
    await session.commit()
    return result


async def _conflict(
    session: AsyncSession,
    code: ConflictCode,
    *,
    work_item_id: uuid.UUID | None = None,
    request_id: uuid.UUID | None = None,
    work_item: WorkItem | None = None,
    request: ExecutionRequest | None = None,
) -> WorkItemConflict:
    result = WorkItemConflict(
        code=code,
        work_item_id=work_item.id if work_item is not None else work_item_id,
        request_id=request.id if request is not None else request_id,
        work_item_version=work_item.version if work_item is not None else None,
        request_version=request.version if request is not None else None,
    )
    await session.commit()
    return result


async def _database_now(session: AsyncSession) -> datetime:
    value = await session.scalar(select(func.clock_timestamp()))
    return cast(datetime, value)


def _queue_notice(
    session: AsyncSession, work_item: WorkItem, request: ExecutionRequest
) -> None:
    """Stage the owed comment in the terminal transaction. The caller commits."""

    if request.status == "completed" or request.terminal_at is None:
        return
    cause = request.terminal_cause
    if cause is None or not cause.strip():
        return
    session.add(
        FactoryTerminalNotice(
            execution_request_id=request.id,
            work_item_id=work_item.id,
            terminal_cause=cause.strip(),
        )
    )


async def _opened_pull_request(
    session: AsyncSession, work_item: WorkItem, request: ExecutionRequest
) -> bool:
    if work_item.publication_lineage_id is None:
        return False
    pr_url = await session.scalar(
        select(ThreadPublicationLineage.pr_url).where(
            ThreadPublicationLineage.id == work_item.publication_lineage_id
        )
    )
    if not isinstance(pr_url, str) or not pr_url.strip():
        return False
    succeeded = await session.scalar(
        select(Publication.id).where(
            Publication.execution_request_id == request.id,
            Publication.lineage_id == work_item.publication_lineage_id,
            Publication.status == "succeeded",
        )
    )
    return succeeded is not None


async def _publication_owns_terminus(session: AsyncSession, work_item: WorkItem) -> bool:
    if work_item.publication_lineage_id is not None:
        return True
    found = await session.scalar(
        select(Publication.id)
        .where(Publication.workspace_conversation_id == work_item.conversation_id)
        .limit(1)
    )
    return found is not None


async def _lock_work_item(
    session: AsyncSession, work_item_id: uuid.UUID
) -> WorkItem | None:
    row: WorkItem | None = await session.scalar(
        select(WorkItem)
        .where(WorkItem.id == work_item_id)
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    return row


async def _lock_request(
    session: AsyncSession,
    *,
    work_item_id: uuid.UUID,
    request_id: uuid.UUID,
) -> ExecutionRequest | None:
    row: ExecutionRequest | None = await session.scalar(
        select(ExecutionRequest)
        .where(
            ExecutionRequest.id == request_id,
            ExecutionRequest.work_item_id == work_item_id,
        )
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    return row


async def _lock_request_by_id(
    session: AsyncSession, request_id: uuid.UUID
) -> ExecutionRequest | None:
    row: ExecutionRequest | None = await session.scalar(
        select(ExecutionRequest)
        .where(ExecutionRequest.id == request_id)
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    return row


async def _lock_active_request(
    session: AsyncSession, work_item_id: uuid.UUID
) -> ExecutionRequest | None:
    row: ExecutionRequest | None = await session.scalar(
        select(ExecutionRequest)
        .where(
            ExecutionRequest.work_item_id == work_item_id,
            ExecutionRequest.status.in_(_ACTIVE_STATUSES),
        )
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    return row


async def _reload_work_item(session: AsyncSession, work_item_id: uuid.UUID) -> WorkItem:
    row = await session.scalar(
        select(WorkItem)
        .where(WorkItem.id == work_item_id)
        .execution_options(populate_existing=True)
    )
    assert row is not None
    return row


async def _reload_request(session: AsyncSession, request_id: uuid.UUID) -> ExecutionRequest:
    row = await session.scalar(
        select(ExecutionRequest)
        .where(ExecutionRequest.id == request_id)
        .execution_options(populate_existing=True)
    )
    assert row is not None
    return row


async def create_or_get_work_item(
    session: AsyncSession,
    *,
    github_repository_id: int,
    github_issue_number: int,
    github_installation_id: int,
    agent_id: uuid.UUID,
    repo_full_name: str,
    conversation_id: str,
) -> WorkItemResult:
    new_id = uuid.uuid4()
    statement = (
        insert(WorkItem)
        .values(
            id=new_id,
            github_repository_id=github_repository_id,
            github_issue_number=github_issue_number,
            github_installation_id=github_installation_id,
            agent_id=agent_id,
            repo_full_name=repo_full_name,
            conversation_id=conversation_id,
            version=1,
            next_sequence=1,
        )
        .on_conflict_do_nothing(
            index_elements=[WorkItem.github_repository_id, WorkItem.github_issue_number]
        )
        .returning(WorkItem.id)
    )
    try:
        async with session.begin_nested():
            inserted_id = await session.scalar(statement)
    except IntegrityError:
        return await _conflict(session, "identity_mismatch")

    work_item = await session.scalar(
        select(WorkItem)
        .where(
            WorkItem.github_repository_id == github_repository_id,
            WorkItem.github_issue_number == github_issue_number,
        )
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    if work_item is None:
        return await _conflict(session, "identity_mismatch")
    exact_replay = (
        work_item.github_installation_id == github_installation_id
        and work_item.agent_id == agent_id
        and work_item.repo_full_name == repo_full_name
        and work_item.conversation_id == conversation_id
    )
    if not exact_replay:
        return await _conflict(session, "identity_mismatch", work_item=work_item)
    return await _outcome(session, work_item, None, replayed=inserted_id is None)


async def create_execution_request(
    session: AsyncSession,
    *,
    work_item_id: uuid.UUID,
    request_id: uuid.UUID,
    wait_deadline: datetime,
    expected_work_item_version: int,
) -> WorkItemResult:
    work_item = await _lock_work_item(session, work_item_id)
    if work_item is None:
        return await _conflict(session, "not_found", work_item_id=work_item_id)
    if work_item.cancelled_at is not None:
        return await _conflict(session, "work_item_cancelled", work_item=work_item)

    existing = await _lock_request_by_id(session, request_id)
    if existing is not None:
        if existing.work_item_id != work_item_id or existing.wait_deadline != wait_deadline:
            return await _conflict(
                session, "identity_mismatch", work_item=work_item, request=existing
            )
        return await _outcome(session, work_item, existing, replayed=True)

    if work_item.version != expected_work_item_version:
        return await _conflict(session, "stale_version", work_item=work_item)
    active = await _lock_active_request(session, work_item_id)
    if active is not None:
        return await _conflict(session, "active_request", work_item=work_item, request=active)

    sequence = work_item.next_sequence
    allocation_changed = False
    try:
        async with session.begin_nested():
            changed_id: uuid.UUID | None = await session.scalar(
                update(WorkItem)
                .where(
                    WorkItem.id == work_item_id,
                    WorkItem.version == expected_work_item_version,
                    WorkItem.cancelled_at.is_(None),
                )
                .values(
                    version=WorkItem.version + 1,
                    next_sequence=WorkItem.next_sequence + 1,
                    updated_at=func.clock_timestamp(),
                )
                .returning(WorkItem.id)
            )
            allocation_changed = changed_id is not None
            if allocation_changed:
                request = ExecutionRequest(
                    id=request_id,
                    work_item_id=work_item_id,
                    sequence=sequence,
                    status="waiting",
                    wait_deadline=wait_deadline,
                    version=1,
                )
                session.add(request)
                await session.flush()
    except IntegrityError:
        work_item = await _reload_work_item(session, work_item_id)
        existing = await _lock_request_by_id(session, request_id)
        if existing is not None:
            if existing.work_item_id == work_item_id and existing.wait_deadline == wait_deadline:
                return await _outcome(session, work_item, existing, replayed=True)
            return await _conflict(
                session, "identity_mismatch", work_item=work_item, request=existing
            )
        active = await _lock_active_request(session, work_item_id)
        if active is not None:
            return await _conflict(
                session, "active_request", work_item=work_item, request=active
            )
        return await _conflict(session, "stale_version", work_item=work_item)

    if not allocation_changed:
        work_item = await _reload_work_item(session, work_item_id)
        return await _conflict(session, "stale_version", work_item=work_item)

    work_item = await _reload_work_item(session, work_item_id)
    request = await _reload_request(session, request_id)
    return await _outcome(session, work_item, request)


async def _start_execution(
    session: AsyncSession,
    *,
    work_item_id: uuid.UUID,
    request_id: uuid.UUID,
    expected_work_item_version: int,
    expected_request_version: int,
    extra_where: Sequence[ColumnElement[bool]],
    extra_values: Mapping[str, Any],
) -> WorkItemResult:
    work_item = await _lock_work_item(session, work_item_id)
    if work_item is None:
        return await _conflict(
            session, "not_found", work_item_id=work_item_id, request_id=request_id
        )
    if work_item.cancelled_at is not None:
        return await _conflict(
            session, "work_item_cancelled", work_item=work_item, request_id=request_id
        )
    if work_item.version != expected_work_item_version:
        return await _conflict(
            session, "stale_version", work_item=work_item, request_id=request_id
        )
    request = await _lock_request(
        session, work_item_id=work_item_id, request_id=request_id
    )
    if request is None:
        return await _conflict(
            session, "not_found", work_item=work_item, request_id=request_id
        )
    if request.version != expected_request_version:
        return await _conflict(session, "stale_version", work_item=work_item, request=request)
    if request.status != "waiting":
        return await _conflict(
            session, "illegal_transition", work_item=work_item, request=request
        )

    now = await _database_now(session)
    if now >= request.wait_deadline:
        return await _conflict(
            session, "waiting_deadline_elapsed", work_item=work_item, request=request
        )
    values: dict[str, Any] = {
        "status": "running",
        "started_at": now,
        "execution_deadline": now + timedelta(seconds=1800),
        "version": ExecutionRequest.version + 1,
        "updated_at": func.clock_timestamp(),
        **extra_values,
    }
    changed_id: uuid.UUID | None = await session.scalar(
        update(ExecutionRequest)
        .where(
            ExecutionRequest.id == request_id,
            ExecutionRequest.work_item_id == work_item_id,
            ExecutionRequest.version == expected_request_version,
            ExecutionRequest.status == "waiting",
            ExecutionRequest.wait_deadline > func.clock_timestamp(),
            *extra_where,
        )
        .values(**values)
        .returning(ExecutionRequest.id)
    )
    if changed_id is None:
        request = await _reload_request(session, request_id)
        if await _database_now(session) >= request.wait_deadline:
            return await _conflict(
                session,
                "waiting_deadline_elapsed",
                work_item=work_item,
                request=request,
            )
        return await _conflict(session, "stale_version", work_item=work_item, request=request)
    request = await _reload_request(session, request_id)
    return await _outcome(session, work_item, request)


async def start_execution(
    session: AsyncSession,
    *,
    work_item_id: uuid.UUID,
    request_id: uuid.UUID,
    expected_work_item_version: int,
    expected_request_version: int,
) -> WorkItemResult:
    return await _start_execution(
        session,
        work_item_id=work_item_id,
        request_id=request_id,
        expected_work_item_version=expected_work_item_version,
        expected_request_version=expected_request_version,
        extra_where=(),
        extra_values={"execution_attempts": 1},
    )


async def expire_waiting(
    session: AsyncSession,
    *,
    work_item_id: uuid.UUID,
    request_id: uuid.UUID,
    expected_work_item_version: int,
    expected_request_version: int,
) -> WorkItemResult:
    work_item = await _lock_work_item(session, work_item_id)
    if work_item is None:
        return await _conflict(
            session, "not_found", work_item_id=work_item_id, request_id=request_id
        )
    if work_item.cancelled_at is not None:
        return await _conflict(
            session, "work_item_cancelled", work_item=work_item, request_id=request_id
        )
    if work_item.version != expected_work_item_version:
        return await _conflict(
            session, "stale_version", work_item=work_item, request_id=request_id
        )
    request = await _lock_request(
        session, work_item_id=work_item_id, request_id=request_id
    )
    if request is None:
        return await _conflict(
            session, "not_found", work_item=work_item, request_id=request_id
        )
    if request.version != expected_request_version:
        return await _conflict(session, "stale_version", work_item=work_item, request=request)
    if request.status != "waiting":
        return await _conflict(
            session, "illegal_transition", work_item=work_item, request=request
        )
    now = await _database_now(session)
    if now < request.wait_deadline:
        return await _conflict(
            session, "illegal_transition", work_item=work_item, request=request
        )
    changed_id: uuid.UUID | None = await session.scalar(
        update(ExecutionRequest)
        .where(
            ExecutionRequest.id == request_id,
            ExecutionRequest.work_item_id == work_item_id,
            ExecutionRequest.version == expected_request_version,
            ExecutionRequest.status == "waiting",
        )
        .values(
            status="expired",
            terminal_at=now,
            terminal_cause="capacity_wait_expired",
            version=ExecutionRequest.version + 1,
            updated_at=func.clock_timestamp(),
        )
        .returning(ExecutionRequest.id)
    )
    if changed_id is None:
        request = await _reload_request(session, request_id)
        return await _conflict(session, "stale_version", work_item=work_item, request=request)
    request = await _reload_request(session, request_id)
    _queue_notice(session, work_item, request)
    return await _outcome(session, work_item, request)


async def link_publication_lineage(
    session: AsyncSession,
    *,
    work_item_id: uuid.UUID,
    request_id: uuid.UUID,
    publication_lineage_id: uuid.UUID,
    expected_work_item_version: int,
    expected_request_version: int,
) -> WorkItemResult:
    work_item = await _lock_work_item(session, work_item_id)
    if work_item is None:
        return await _conflict(
            session, "not_found", work_item_id=work_item_id, request_id=request_id
        )
    if work_item.cancelled_at is not None:
        return await _conflict(
            session, "work_item_cancelled", work_item=work_item, request_id=request_id
        )
    if work_item.version != expected_work_item_version:
        return await _conflict(
            session, "stale_version", work_item=work_item, request_id=request_id
        )
    request = await _lock_request(
        session, work_item_id=work_item_id, request_id=request_id
    )
    if request is None:
        return await _conflict(
            session, "not_found", work_item=work_item, request_id=request_id
        )
    if request.version != expected_request_version:
        return await _conflict(session, "stale_version", work_item=work_item, request=request)
    if request.status != "running":
        return await _conflict(
            session, "publication_ineligible", work_item=work_item, request=request
        )
    now = await _database_now(session)
    if request.execution_deadline is None or now >= request.execution_deadline:
        return await _conflict(
            session, "execution_deadline_elapsed", work_item=work_item, request=request
        )

    if (
        work_item.publication_lineage_id is not None
        and work_item.publication_lineage_id != publication_lineage_id
    ):
        return await _conflict(
            session, "lineage_already_owned", work_item=work_item, request=request
        )

    lineage = await session.scalar(
        select(ThreadPublicationLineage)
        .where(ThreadPublicationLineage.id == publication_lineage_id)
        .with_for_update(read=True)
        .execution_options(populate_existing=True)
    )
    if lineage is None or (
        lineage.agent_id != work_item.agent_id
        or lineage.conversation_id != work_item.conversation_id
        or lineage.repo_full_name.casefold() != work_item.repo_full_name.casefold()
        or (
            lineage.github_repository_id is not None
            and lineage.github_repository_id != work_item.github_repository_id
        )
        or (
            lineage.github_installation_id is not None
            and lineage.github_installation_id != work_item.github_installation_id
        )
    ):
        return await _conflict(
            session, "lineage_mismatch", work_item=work_item, request=request
        )
    if work_item.publication_lineage_id == publication_lineage_id:
        return await _outcome(session, work_item, request, replayed=True)
    owner = await session.scalar(
        select(WorkItem).where(WorkItem.publication_lineage_id == publication_lineage_id)
    )
    if owner is not None:
        return await _conflict(
            session, "lineage_already_owned", work_item=work_item, request=request
        )

    lineage_changed = False
    try:
        async with session.begin_nested():
            changed_id: uuid.UUID | None = await session.scalar(
                update(WorkItem)
                .where(
                    WorkItem.id == work_item_id,
                    WorkItem.version == expected_work_item_version,
                    WorkItem.cancelled_at.is_(None),
                    WorkItem.publication_lineage_id.is_(None),
                    select(ExecutionRequest.id)
                    .where(
                        ExecutionRequest.id == request_id,
                        ExecutionRequest.work_item_id == work_item_id,
                        ExecutionRequest.version == expected_request_version,
                        ExecutionRequest.status == "running",
                        ExecutionRequest.execution_deadline
                        > func.clock_timestamp(),
                    )
                    .exists(),
                )
                .values(
                    publication_lineage_id=publication_lineage_id,
                    version=WorkItem.version + 1,
                    updated_at=func.clock_timestamp(),
                )
                .returning(WorkItem.id)
            )
            lineage_changed = changed_id is not None
    except IntegrityError:
        work_item = await _reload_work_item(session, work_item_id)
        current_lineage = await session.scalar(
            select(ThreadPublicationLineage)
            .where(ThreadPublicationLineage.id == publication_lineage_id)
            .execution_options(populate_existing=True)
        )
        if current_lineage is None:
            return await _conflict(
                session, "lineage_mismatch", work_item=work_item, request=request
            )
        return await _conflict(
            session, "lineage_already_owned", work_item=work_item, request=request
        )
    if not lineage_changed:
        work_item = await _reload_work_item(session, work_item_id)
        request = await _reload_request(session, request_id)
        if (
            request.execution_deadline is None
            or await _database_now(session) >= request.execution_deadline
        ):
            return await _conflict(
                session,
                "execution_deadline_elapsed",
                work_item=work_item,
                request=request,
            )
        if work_item.publication_lineage_id is not None:
            return await _conflict(
                session, "lineage_already_owned", work_item=work_item, request=request
            )
        return await _conflict(
            session, "stale_version", work_item=work_item, request=request
        )
    work_item = await _reload_work_item(session, work_item_id)
    return await _outcome(session, work_item, request)


async def _terminalize_execution(
    session: AsyncSession,
    *,
    work_item_id: uuid.UUID,
    request_id: uuid.UUID,
    expected_work_item_version: int,
    expected_request_version: int,
    status: Literal["completed", "failed"],
    cause: str,
    extra_where: Sequence[ColumnElement[bool]],
) -> WorkItemResult:
    work_item = await _lock_work_item(session, work_item_id)
    if work_item is None:
        return await _conflict(
            session, "not_found", work_item_id=work_item_id, request_id=request_id
        )
    if work_item.cancelled_at is not None:
        return await _conflict(
            session, "work_item_cancelled", work_item=work_item, request_id=request_id
        )
    if work_item.version != expected_work_item_version:
        return await _conflict(
            session, "stale_version", work_item=work_item, request_id=request_id
        )
    request = await _lock_request(
        session, work_item_id=work_item_id, request_id=request_id
    )
    if request is None:
        return await _conflict(
            session, "not_found", work_item=work_item, request_id=request_id
        )
    if request.version != expected_request_version:
        return await _conflict(session, "stale_version", work_item=work_item, request=request)
    if request.status != "running" or not cause.strip():
        return await _conflict(
            session, "illegal_transition", work_item=work_item, request=request
        )
    now = await _database_now(session)
    opened = status == "completed" and await _opened_pull_request(
        session, work_item, request
    )
    deadline_elapsed = (
        request.execution_deadline is None or now >= request.execution_deadline
    )
    # A pull request that already opened is the terminus even if reconciliation
    # notices it after the execution deadline.
    if deadline_elapsed and not opened:
        return await _conflict(
            session, "execution_deadline_elapsed", work_item=work_item, request=request
        )
    if status == "completed" and not opened:
        return await _conflict(
            session, "illegal_transition", work_item=work_item, request=request
        )
    if status == "failed" and cause.strip() == "no_pull_request":
        if await _publication_owns_terminus(session, work_item):
            return await _conflict(
                session, "publication_pending", work_item=work_item, request=request
            )
    # The Python check above allows a pull request that already opened to
    # complete after the deadline. The UPDATE has to use the same exception,
    # or the deadline predicate rejects the row and the next pass cancels it.
    deadline_guard = (
        ()
        if opened
        else (ExecutionRequest.execution_deadline > func.clock_timestamp(),)
    )
    changed_id: uuid.UUID | None = await session.scalar(
        update(ExecutionRequest)
        .where(
            ExecutionRequest.id == request_id,
            ExecutionRequest.work_item_id == work_item_id,
            ExecutionRequest.version == expected_request_version,
            ExecutionRequest.status == "running",
            *deadline_guard,
            *extra_where,
        )
        .values(
            status=status,
            terminal_at=now,
            terminal_cause=cause.strip(),
            version=ExecutionRequest.version + 1,
            updated_at=func.clock_timestamp(),
        )
        .returning(ExecutionRequest.id)
    )
    if changed_id is None:
        request = await _reload_request(session, request_id)
        if not opened and (
            request.execution_deadline is None
            or await _database_now(session) >= request.execution_deadline
        ):
            return await _conflict(
                session,
                "execution_deadline_elapsed",
                work_item=work_item,
                request=request,
            )
        return await _conflict(session, "stale_version", work_item=work_item, request=request)
    request = await _reload_request(session, request_id)
    if status != "completed":
        _queue_notice(session, work_item, request)
    return await _outcome(session, work_item, request)


async def complete_execution(
    session: AsyncSession,
    *,
    work_item_id: uuid.UUID,
    request_id: uuid.UUID,
    expected_work_item_version: int,
    expected_request_version: int,
) -> WorkItemResult:
    return await _terminalize_execution(
        session,
        work_item_id=work_item_id,
        request_id=request_id,
        expected_work_item_version=expected_work_item_version,
        expected_request_version=expected_request_version,
        status="completed",
        cause="completed",
        extra_where=(),
    )


async def fail_execution(
    session: AsyncSession,
    *,
    work_item_id: uuid.UUID,
    request_id: uuid.UUID,
    cause: str,
    expected_work_item_version: int,
    expected_request_version: int,
) -> WorkItemResult:
    return await _terminalize_execution(
        session,
        work_item_id=work_item_id,
        request_id=request_id,
        expected_work_item_version=expected_work_item_version,
        expected_request_version=expected_request_version,
        status="failed",
        cause=cause,
        extra_where=(),
    )


async def request_cancellation(
    session: AsyncSession,
    *,
    work_item_id: uuid.UUID,
    expected_work_item_version: int,
) -> WorkItemResult:
    work_item = await _lock_work_item(session, work_item_id)
    if work_item is None:
        return await _conflict(session, "not_found", work_item_id=work_item_id)
    if work_item.version != expected_work_item_version:
        return await _conflict(session, "stale_version", work_item=work_item)
    if work_item.cancelled_at is not None:
        active = await _lock_active_request(session, work_item_id)
        return await _outcome(session, work_item, active, replayed=True)

    now = await _database_now(session)
    changed_id: uuid.UUID | None = await session.scalar(
        update(WorkItem)
        .where(
            WorkItem.id == work_item_id,
            WorkItem.version == expected_work_item_version,
            WorkItem.cancelled_at.is_(None),
        )
        .values(
            cancelled_at=now,
            version=WorkItem.version + 1,
            updated_at=func.clock_timestamp(),
        )
        .returning(WorkItem.id)
    )
    if changed_id is None:
        work_item = await _reload_work_item(session, work_item_id)
        return await _conflict(session, "stale_version", work_item=work_item)

    active = await _lock_active_request(session, work_item_id)
    if active is not None and active.status == "waiting":
        await session.execute(
            update(ExecutionRequest)
            .where(
                ExecutionRequest.id == active.id,
                ExecutionRequest.work_item_id == work_item_id,
                ExecutionRequest.version == active.version,
                ExecutionRequest.status == "waiting",
            )
            .values(
                status="cancelled",
                terminal_at=now,
                terminal_cause="issue_cancelled",
                version=ExecutionRequest.version + 1,
                updated_at=func.clock_timestamp(),
            )
        )
        active = await _reload_request(session, active.id)
        _queue_notice(session, work_item, active)
    elif active is not None and active.status == "running":
        await session.execute(
            update(ExecutionRequest)
            .where(
                ExecutionRequest.id == active.id,
                ExecutionRequest.work_item_id == work_item_id,
                ExecutionRequest.version == active.version,
                ExecutionRequest.status == "running",
            )
            .values(
                status="cancellation_requested",
                terminal_cause="issue_cancelled",
                version=ExecutionRequest.version + 1,
                updated_at=func.clock_timestamp(),
            )
        )
        active = await _reload_request(session, active.id)
    elif (
        active is not None
        and active.status == "cancellation_requested"
        and active.terminal_cause in ("execution_deadline", "owner_lost")
    ):
        await session.execute(
            update(ExecutionRequest)
            .where(
                ExecutionRequest.id == active.id,
                ExecutionRequest.work_item_id == work_item_id,
                ExecutionRequest.version == active.version,
                ExecutionRequest.status == "cancellation_requested",
            )
            .values(
                terminal_cause="issue_cancelled",
                version=ExecutionRequest.version + 1,
                updated_at=func.clock_timestamp(),
            )
        )
        active = await _reload_request(session, active.id)
    work_item = await _reload_work_item(session, work_item_id)
    return await _outcome(session, work_item, active)


async def request_execution_deadline_cancellation(
    session: AsyncSession,
    *,
    work_item_id: uuid.UUID,
    request_id: uuid.UUID,
    expected_work_item_version: int,
    expected_request_version: int,
) -> WorkItemResult:
    work_item = await _lock_work_item(session, work_item_id)
    if work_item is None:
        return await _conflict(
            session, "not_found", work_item_id=work_item_id, request_id=request_id
        )
    if work_item.cancelled_at is not None:
        return await _conflict(
            session, "work_item_cancelled", work_item=work_item, request_id=request_id
        )
    if work_item.version != expected_work_item_version:
        return await _conflict(
            session, "stale_version", work_item=work_item, request_id=request_id
        )
    request = await _lock_request(
        session, work_item_id=work_item_id, request_id=request_id
    )
    if request is None:
        return await _conflict(
            session, "not_found", work_item=work_item, request_id=request_id
        )
    if request.version != expected_request_version:
        return await _conflict(session, "stale_version", work_item=work_item, request=request)
    if request.status != "running":
        return await _conflict(
            session, "illegal_transition", work_item=work_item, request=request
        )
    now = await _database_now(session)
    if request.execution_deadline is None or now < request.execution_deadline:
        return await _conflict(
            session, "illegal_transition", work_item=work_item, request=request
        )
    changed_id: uuid.UUID | None = await session.scalar(
        update(ExecutionRequest)
        .where(
            ExecutionRequest.id == request_id,
            ExecutionRequest.work_item_id == work_item_id,
            ExecutionRequest.version == expected_request_version,
            ExecutionRequest.status == "running",
        )
        .values(
            status="cancellation_requested",
            terminal_cause="execution_deadline",
            version=ExecutionRequest.version + 1,
            updated_at=func.clock_timestamp(),
        )
        .returning(ExecutionRequest.id)
    )
    if changed_id is None:
        request = await _reload_request(session, request_id)
        return await _conflict(session, "stale_version", work_item=work_item, request=request)
    request = await _reload_request(session, request_id)
    return await _outcome(session, work_item, request)


async def request_owner_lost_cancellation(
    session: AsyncSession,
    *,
    work_item_id: uuid.UUID,
    request_id: uuid.UUID,
    expected_work_item_version: int,
    expected_request_version: int,
) -> WorkItemResult:
    work_item = await _lock_work_item(session, work_item_id)
    if work_item is None:
        return await _conflict(
            session, "not_found", work_item_id=work_item_id, request_id=request_id
        )
    if work_item.cancelled_at is not None:
        return await _conflict(
            session, "work_item_cancelled", work_item=work_item, request_id=request_id
        )
    if work_item.version != expected_work_item_version:
        return await _conflict(
            session, "stale_version", work_item=work_item, request_id=request_id
        )
    request = await _lock_request(
        session, work_item_id=work_item_id, request_id=request_id
    )
    if request is None:
        return await _conflict(
            session, "not_found", work_item=work_item, request_id=request_id
        )
    if request.version != expected_request_version:
        return await _conflict(session, "stale_version", work_item=work_item, request=request)
    if request.status != "running":
        return await _conflict(
            session, "illegal_transition", work_item=work_item, request=request
        )
    now = await _database_now(session)
    ttl = timedelta(seconds=get_settings().work_item_runtime_ttl_seconds)
    heartbeat_lapsed = (
        request.runtime_heartbeat_expires_at is not None
        and request.runtime_heartbeat_expires_at <= now
    )
    owner_absent = (
        request.runtime_owner is None
        and request.started_at is not None
        and request.started_at + ttl <= now
    )
    if not heartbeat_lapsed and not owner_absent:
        return await _conflict(
            session, "illegal_transition", work_item=work_item, request=request
        )
    changed_id: uuid.UUID | None = await session.scalar(
        update(ExecutionRequest)
        .where(
            ExecutionRequest.id == request_id,
            ExecutionRequest.work_item_id == work_item_id,
            ExecutionRequest.version == expected_request_version,
            ExecutionRequest.status == "running",
            (
                (
                    ExecutionRequest.runtime_heartbeat_expires_at.is_not(None)
                    & (
                        ExecutionRequest.runtime_heartbeat_expires_at
                        <= func.clock_timestamp()
                    )
                )
                | (
                    ExecutionRequest.runtime_owner.is_(None)
                    & ExecutionRequest.started_at.is_not(None)
                    & (
                        ExecutionRequest.started_at
                        <= func.clock_timestamp() - ttl
                    )
                )
            ),
        )
        .values(
            status="cancellation_requested",
            terminal_cause="owner_lost",
            version=ExecutionRequest.version + 1,
            updated_at=func.clock_timestamp(),
        )
        .returning(ExecutionRequest.id)
    )
    if changed_id is None:
        request = await _reload_request(session, request_id)
        return await _conflict(session, "stale_version", work_item=work_item, request=request)
    request = await _reload_request(session, request_id)
    return await _outcome(session, work_item, request)


async def record_runtime_termination(
    session: AsyncSession,
    *,
    work_item_id: uuid.UUID,
    request_id: uuid.UUID,
    termination_observation: str,
    expected_work_item_version: int,
    expected_request_version: int,
) -> WorkItemResult:
    return await _record_runtime_termination(
        session,
        work_item_id=work_item_id,
        request_id=request_id,
        termination_observation=termination_observation,
        expected_work_item_version=expected_work_item_version,
        expected_request_version=expected_request_version,
        extra_where=(),
    )


async def _record_runtime_termination(
    session: AsyncSession,
    *,
    work_item_id: uuid.UUID,
    request_id: uuid.UUID,
    termination_observation: str,
    expected_work_item_version: int,
    expected_request_version: int,
    extra_where: Sequence[ColumnElement[bool]],
) -> WorkItemResult:
    work_item = await _lock_work_item(session, work_item_id)
    if work_item is None:
        return await _conflict(
            session, "not_found", work_item_id=work_item_id, request_id=request_id
        )
    if work_item.version != expected_work_item_version:
        return await _conflict(
            session, "stale_version", work_item=work_item, request_id=request_id
        )
    request = await _lock_request(
        session, work_item_id=work_item_id, request_id=request_id
    )
    if request is None:
        return await _conflict(
            session, "not_found", work_item=work_item, request_id=request_id
        )
    if request.version != expected_request_version:
        return await _conflict(session, "stale_version", work_item=work_item, request=request)
    if request.status != "cancellation_requested":
        return await _conflict(
            session, "illegal_transition", work_item=work_item, request=request
        )
    if not termination_observation.strip():
        return await _conflict(
            session,
            "termination_observation_required",
            work_item=work_item,
            request=request,
        )
    if request.terminal_cause == "issue_cancelled":
        terminal_status = "cancelled"
    elif request.terminal_cause == "execution_deadline":
        terminal_status = "expired"
    elif request.terminal_cause == "owner_lost":
        terminal_status = "failed"
    else:
        return await _conflict(
            session, "illegal_transition", work_item=work_item, request=request
        )
    now = await _database_now(session)
    changed_id: uuid.UUID | None = await session.scalar(
        update(ExecutionRequest)
        .where(
            ExecutionRequest.id == request_id,
            ExecutionRequest.work_item_id == work_item_id,
            ExecutionRequest.version == expected_request_version,
            ExecutionRequest.status == "cancellation_requested",
            *extra_where,
        )
        .values(
            status=terminal_status,
            terminal_at=now,
            termination_observation=termination_observation,
            version=ExecutionRequest.version + 1,
            updated_at=func.clock_timestamp(),
        )
        .returning(ExecutionRequest.id)
    )
    if changed_id is None:
        request = await _reload_request(session, request_id)
        return await _conflict(session, "stale_version", work_item=work_item, request=request)
    request = await _reload_request(session, request_id)
    _queue_notice(session, work_item, request)
    return await _outcome(session, work_item, request)


_PUBLICATION_CAUSES = {
    "denied": "publication_denied",
    "expired": "publication_expired",
    "failed": "publication_failed",
}


@dataclass(frozen=True)
class PublicationSettlement:
    work_item_id: uuid.UUID
    request_id: uuid.UUID
    work_item_version: int
    request_version: int
    cause: str


async def claim_publication_settlement(
    session: AsyncSession,
) -> PublicationSettlement | None:
    """The only reader that decides a linked publication's execution terminus.

    Callers then use ``complete_execution`` or ``fail_execution``. This function
    does not commit and does not write the request.
    """

    rows = (
        await session.execute(
            select(
                ExecutionRequest,
                WorkItem,
                Publication,
                ThreadPublicationLineage.pr_url,
            )
            .join(WorkItem, WorkItem.id == ExecutionRequest.work_item_id)
            .join(Publication, Publication.execution_request_id == ExecutionRequest.id)
            .join(
                ThreadPublicationLineage,
                ThreadPublicationLineage.id == Publication.lineage_id,
            )
            .where(
                ExecutionRequest.status == "running",
                Publication.status.in_(("denied", "expired", "failed", "succeeded")),
            )
            .order_by(ExecutionRequest.updated_at, Publication.revision_number.desc())
            .limit(20)
            .with_for_update(skip_locked=True, of=ExecutionRequest)
        )
    ).all()
    seen: set[uuid.UUID] = set()
    for request, work_item, publication, pr_url in rows:
        if request.id in seen:
            continue
        seen.add(request.id)
        active = await session.scalar(
            select(Publication.id).where(
                Publication.execution_request_id == request.id,
                Publication.status.in_(("pending", "approved", "launching", "running")),
            )
        )
        if active is not None:
            continue
        if publication.status == "succeeded":
            if not isinstance(pr_url, str) or not pr_url.strip():
                continue
            cause = "completed"
        elif publication.status in _PUBLICATION_CAUSES:
            cause = _PUBLICATION_CAUSES[publication.status]
        else:
            continue
        return PublicationSettlement(
            work_item_id=work_item.id,
            request_id=request.id,
            work_item_version=work_item.version,
            request_version=request.version,
            cause=cause,
        )
    return None
