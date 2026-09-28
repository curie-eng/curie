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

from curie_telemetry.redact import redact_text
from sqlalchemy import func, select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql.elements import ColumnElement

from . import transcripts
from .config import get_settings
from .models import (
    DEFAULT_EXECUTION_DEADLINE_SECONDS,
    Agent,
    ExecutionRequest,
    FactoryStatusComment,
    Publication,
    ThreadPublicationLineage,
    WorkItem,
    new_card_token,
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
_IN_FLIGHT_PUBLICATION = ("pending", "approved", "launching", "running")
# A failed finish that never published defers to an in-flight publication (#2577, #3128).
_UNPUBLISHED_CAUSES = frozenset({"no_pull_request", "early_stop"})
# Longest provider message a factory notice keeps (#3073).
_NOTICE_DETAIL_MAX = 1200

_NO_READMIT: dict[str, Any] = {
    "readmit_request_id": None,
    "readmit_requester": None,
    "readmit_objective": None,
}


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


GITHUB_CHANNEL_KIND = "github"


def github_reply_route(repo_full_name: str, issue_number: int) -> tuple[str, str, str]:
    """The reply kind, address, and conversation id a GitHub issue replies on."""

    return GITHUB_CHANNEL_KIND, repo_full_name, f"issue-{issue_number}"


async def _database_now(session: AsyncSession) -> datetime:
    value = await session.scalar(select(func.clock_timestamp()))
    return cast(datetime, value)


async def _queue_notice(
    session: AsyncSession,
    work_item: WorkItem,
    request: ExecutionRequest,
    *,
    detail: str | None,
) -> None:
    """Stage the terminal result on the request's status comment row (#3077).

    The caller commits. The row normally exists from admission; a request
    admitted before it did gets one here. Reply routing is resolved when the
    comment is delivered. ``detail`` is the provider's own failure message
    (#3073); it is redacted before it is stored, then clipped, so no key or
    token reaches the row or the comment.
    """

    if request.terminal_at is None:
        return
    cause = request.terminal_cause
    if cause is None or not cause.strip():
        return
    stored = _notice_detail(detail)
    await session.execute(
        insert(FactoryStatusComment)
        .values(
            execution_request_id=request.id,
            work_item_id=work_item.id,
            card_token=new_card_token(),
            terminal_cause=cause.strip(),
            detail=stored,
        )
        .on_conflict_do_update(
            index_elements=["execution_request_id"],
            set_={"terminal_cause": cause.strip(), "detail": stored},
        )
    )


def _notice_detail(detail: str | None) -> str | None:
    # Redact before clip so a truncated key still matches the redactor.
    text = redact_text((detail or "").strip())
    if len(text) > _NOTICE_DETAIL_MAX:
        text = text[: _NOTICE_DETAIL_MAX - 3].rstrip() + "..."
    return text or None


async def _settle_terminal(
    session: AsyncSession,
    work_item: WorkItem,
    request: ExecutionRequest,
    *,
    detail: str | None,
) -> None:
    """Stage everything a terminal request owes in its own transaction.

    That is the factory comment and, per ADR-0170, deleting the thread's
    transcript: a terminal WorkItem's history is not resumed again.
    """

    if request.terminal_at is None:
        return
    await _queue_notice(session, work_item, request, detail=detail)
    await transcripts.expire_for_work_item(session, work_item)


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
                # The live status comment's row exists from admission (#3077).
                session.add(
                    FactoryStatusComment(
                        execution_request_id=request_id,
                        work_item_id=work_item_id,
                        applied_label=None,
                    )
                )
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
    # Same transaction as the transition: the owning agent's operator override,
    # else the platform default (#3071).
    agent_deadline = await session.scalar(
        select(Agent.execution_deadline_seconds).where(Agent.id == work_item.agent_id)
    )
    deadline_seconds = (
        agent_deadline if agent_deadline is not None else DEFAULT_EXECUTION_DEADLINE_SECONDS
    )
    values: dict[str, Any] = {
        "status": "running",
        "started_at": now,
        "execution_deadline": now + timedelta(seconds=deadline_seconds),
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
    await _settle_terminal(session, work_item, request, detail=None)
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
    detail: str | None,
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
    # The CI gate's causes (#3097) end a request whose pull request already
    # opened, so they share the opened-PR deadline exception. Keep this literal
    # equal to ``factory_ci.CI_CAUSES`` (importing it here would be circular).
    ci_cause = cause.strip() in {"ci_failed", "ci_timeout", "ci_unverified"}
    opened = (status == "completed" or ci_cause) and await _opened_pull_request(
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
    if status == "failed" and cause.strip() == "ci_fix_unpublished":
        # A CI fix turn that ended without a new publication is terminal, unless
        # its publication is still in flight, or a fix publication already
        # succeeded and awaits the CI gate's verdict; either one settles the
        # request. Every succeeded publication after the request's first is a fix
        # round's. The database does not record which of them the gate already
        # judged failing, so a later round's unpublished turn defers here and the
        # request ends at its execution deadline instead.
        succeeded = (
            select(Publication.id)
            .where(
                Publication.execution_request_id == request.id,
                Publication.status == "succeeded",
            )
            .order_by(Publication.revision_number)
            .offset(1)
            .limit(1)
        )
        in_flight = await session.scalar(
            select(Publication.id)
            .where(
                Publication.execution_request_id == request.id,
                Publication.status.in_(_IN_FLIGHT_PUBLICATION),
            )
            .limit(1)
        )
        if in_flight is None:
            in_flight = await session.scalar(succeeded)
        if in_flight is not None:
            return await _conflict(
                session, "publication_pending", work_item=work_item, request=request
            )
    if status == "failed" and cause.strip() in _UNPUBLISHED_CAUSES:
        if await _publication_owns_terminus(session, work_item):
            return await _conflict(
                session, "publication_pending", work_item=work_item, request=request
            )
    if status == "failed" and cause.strip() == "approval_create_failed":
        pending = await session.scalar(
            select(Publication.id)
            .where(
                Publication.execution_request_id == request.id,
                Publication.status.in_(_IN_FLIGHT_PUBLICATION),
            )
            .limit(1)
        )
        if pending is not None:
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
    await _settle_terminal(session, work_item, request, detail=detail)
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
        detail=None,
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
        detail=None,
        extra_where=(),
    )


async def _ci_fence_holds(
    session: AsyncSession,
    work_item: WorkItem,
    request_id: uuid.UUID,
    publication_id: uuid.UUID,
    head_sha: str,
) -> bool:
    """True while the CI gate's observation still describes the request (#3097).

    Call with the WorkItem and request locked. The lineage row is locked
    ``FOR SHARE``, which serializes against a publication advancing its head,
    so a verdict for an older head can never be written after a newer push.
    """

    if work_item.publication_lineage_id is None:
        return False
    lineage_head = await session.scalar(
        select(ThreadPublicationLineage.head_sha)
        .where(ThreadPublicationLineage.id == work_item.publication_lineage_id)
        .with_for_update(read=True)
    )
    if lineage_head != head_sha:
        return False
    latest = await session.scalar(
        select(Publication.id)
        .where(
            Publication.execution_request_id == request_id,
            Publication.status == "succeeded",
        )
        .order_by(Publication.revision_number.desc())
        .limit(1)
    )
    if latest != publication_id:
        return False
    in_flight = await session.scalar(
        select(Publication.id)
        .where(
            Publication.execution_request_id == request_id,
            Publication.status.in_(_IN_FLIGHT_PUBLICATION),
        )
        .limit(1)
    )
    return in_flight is None


async def settle_ci_verdict(
    session: AsyncSession,
    *,
    work_item_id: uuid.UUID,
    request_id: uuid.UUID,
    expected_work_item_version: int,
    expected_request_version: int,
    expected_publication_id: uuid.UUID,
    expected_head_sha: str,
    status: Literal["completed", "failed"],
    cause: str,
    detail: str | None,
) -> WorkItemResult:
    """Write the CI gate's verdict, fenced to the observed publication and head.

    A newer push, a publication in flight, or a moved head returns
    ``stale_version`` and writes nothing; the next pass observes the new head.
    """

    work_item = await _lock_work_item(session, work_item_id)
    request = (
        await _lock_request(session, work_item_id=work_item_id, request_id=request_id)
        if work_item is not None
        else None
    )
    if work_item is not None and request is not None and not await _ci_fence_holds(
        session, work_item, request_id, expected_publication_id, expected_head_sha
    ):
        return await _conflict(session, "stale_version", work_item=work_item, request=request)
    return await _terminalize_execution(
        session,
        work_item_id=work_item_id,
        request_id=request_id,
        expected_work_item_version=expected_work_item_version,
        expected_request_version=expected_request_version,
        status=status,
        cause=cause,
        detail=detail,
        extra_where=(),
    )


async def hold_for_ci_fix(
    session: AsyncSession,
    *,
    work_item_id: uuid.UUID,
    request_id: uuid.UUID,
    expected_request_version: int,
    expected_publication_id: uuid.UUID,
    expected_head_sha: str,
) -> bool:
    """Hold a running request's lease to its deadline for a CI fix turn (#3097).

    Fenced like ``settle_ci_verdict``: False, with nothing written, when the
    observed publication or head is no longer current or the deadline passed.
    """

    work_item = await _lock_work_item(session, work_item_id)
    if work_item is None:
        await session.commit()
        return False
    request = await _lock_request(session, work_item_id=work_item_id, request_id=request_id)
    if (
        request is None
        or request.version != expected_request_version
        or request.status != "running"
        or not await _ci_fence_holds(
            session, work_item, request_id, expected_publication_id, expected_head_sha
        )
    ):
        await session.commit()
        return False
    changed_id: uuid.UUID | None = await session.scalar(
        update(ExecutionRequest)
        .where(
            ExecutionRequest.id == request_id,
            ExecutionRequest.version == expected_request_version,
            ExecutionRequest.status == "running",
            ExecutionRequest.execution_deadline > func.clock_timestamp(),
        )
        .values(
            runtime_heartbeat_expires_at=ExecutionRequest.execution_deadline,
            updated_at=func.clock_timestamp(),
        )
        .returning(ExecutionRequest.id)
    )
    await session.commit()
    return changed_id is not None


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
        if work_item.readmit_request_id is None:
            return await _outcome(session, work_item, active, replayed=True)
        # The last label action wins: an unlabel drops a pending relabel.
        await session.execute(
            update(WorkItem)
            .where(WorkItem.id == work_item_id)
            .values(
                **_NO_READMIT,
                version=WorkItem.version + 1,
                updated_at=func.clock_timestamp(),
            )
        )
        work_item = await _reload_work_item(session, work_item_id)
        return await _outcome(session, work_item, active)

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
            **_NO_READMIT,
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
        await _settle_terminal(session, work_item, active, detail=None)
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
                cancellation_requested_at=now,
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
    if active is None:
        # Nothing is left to run, so the cancelled WorkItem is terminal now. A
        # waiting or running request expires the transcript when it settles.
        await transcripts.expire_for_work_item(session, work_item)
    return await _outcome(session, work_item, active)


async def readmit(
    session: AsyncSession,
    *,
    work_item_id: uuid.UUID,
    request_id: uuid.UUID,
    wait_deadline: datetime,
    objective: str,
    requester: str,
) -> WorkItemResult:
    """Start a new run on an existing WorkItem because the label was added again.

    A waiting request is superseded in the same transaction. A running request
    is asked to stop, and the relabel is stored on the WorkItem until that
    request reaches a terminus; the returned request is then the old one.
    """

    work_item = await _lock_work_item(session, work_item_id)
    if work_item is None:
        return await _conflict(session, "not_found", work_item_id=work_item_id)
    active = await _lock_active_request(session, work_item_id)
    now = await _database_now(session)
    if active is not None and active.status in ("running", "cancellation_requested"):
        if active.status == "running":
            await session.execute(
                update(ExecutionRequest)
                .where(
                    ExecutionRequest.id == active.id,
                    ExecutionRequest.version == active.version,
                    ExecutionRequest.status == "running",
                )
                .values(
                    status="cancellation_requested",
                    terminal_cause="issue_cancelled",
                    cancellation_requested_at=now,
                    version=ExecutionRequest.version + 1,
                    updated_at=func.clock_timestamp(),
                )
            )
        elif active.terminal_cause != "issue_cancelled":
            await session.execute(
                update(ExecutionRequest)
                .where(
                    ExecutionRequest.id == active.id,
                    ExecutionRequest.version == active.version,
                    ExecutionRequest.status == "cancellation_requested",
                )
                .values(
                    terminal_cause="issue_cancelled",
                    version=ExecutionRequest.version + 1,
                    updated_at=func.clock_timestamp(),
                )
            )
        await session.execute(
            update(WorkItem)
            .where(WorkItem.id == work_item_id)
            .values(
                readmit_request_id=request_id,
                readmit_requester=requester,
                readmit_objective=objective,
                version=WorkItem.version + 1,
                updated_at=func.clock_timestamp(),
            )
        )
        work_item = await _reload_work_item(session, work_item_id)
        active = await _reload_request(session, active.id)
        return await _outcome(session, work_item, active)
    if active is not None:
        # Superseded, not stopped: the new request speaks for the issue. The
        # old run's status comment is still finalized, with the superseded
        # text. Not _settle_terminal: the WorkItem continues, so its
        # transcript must survive.
        await session.execute(
            update(ExecutionRequest)
            .where(
                ExecutionRequest.id == active.id,
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
        superseded = await _reload_request(session, active.id)
        await _queue_notice(session, work_item, superseded, detail=None)
    version: int | None = await session.scalar(
        update(WorkItem)
        .where(WorkItem.id == work_item_id)
        .values(
            cancelled_at=None,
            **_NO_READMIT,
            version=WorkItem.version + 1,
            updated_at=func.clock_timestamp(),
        )
        .returning(WorkItem.version)
    )
    assert version is not None
    return await create_execution_request(
        session,
        work_item_id=work_item_id,
        request_id=request_id,
        wait_deadline=wait_deadline,
        expected_work_item_version=version,
    )


async def admit_pending_readmit(
    session: AsyncSession,
    *,
    work_item_id: uuid.UUID,
    wait_deadline: datetime,
) -> WorkItemResult | None:
    """Create the request a relabel stored while the previous run was stopping.

    Returns None when there is nothing to admit yet.
    """

    work_item = await _lock_work_item(session, work_item_id)
    if work_item is None or work_item.readmit_request_id is None:
        await session.commit()
        return None
    if await _lock_active_request(session, work_item_id) is not None:
        await session.commit()
        return None
    request_id = work_item.readmit_request_id
    reply_kind, reply_address, reply_conversation_id = github_reply_route(
        work_item.repo_full_name, work_item.github_issue_number
    )
    snapshot: dict[str, Any] = {
        "objective": work_item.readmit_objective,
        "requester": work_item.readmit_requester,
        "reply_kind": reply_kind,
        "reply_address": reply_address,
        "reply_conversation_id": reply_conversation_id,
    }
    if snapshot["objective"] is None or snapshot["requester"] is None:
        # Never commit a request that cannot be dispatched.
        await session.rollback()
        return None
    version: int | None = await session.scalar(
        update(WorkItem)
        .where(WorkItem.id == work_item_id)
        .values(
            cancelled_at=None,
            **_NO_READMIT,
            version=WorkItem.version + 1,
            updated_at=func.clock_timestamp(),
        )
        .returning(WorkItem.version)
    )
    assert version is not None
    result = await create_execution_request(
        session,
        work_item_id=work_item_id,
        request_id=request_id,
        wait_deadline=wait_deadline,
        expected_work_item_version=version,
    )
    if isinstance(result, WorkItemOutcome):
        await session.execute(
            update(ExecutionRequest)
            .where(
                ExecutionRequest.id == request_id,
                ExecutionRequest.objective.is_(None),
            )
            .values(**snapshot, updated_at=func.clock_timestamp())
        )
        await session.commit()
    return result


async def settle_overdue_cancellation(
    session: AsyncSession,
    *,
    work_item_id: uuid.UUID,
    request_id: uuid.UUID,
    expected_request_version: int,
    settle_seconds: int,
) -> WorkItemResult:
    """Settle an issue cancellation no worker has confirmed within the window.

    The runtime epoch advances and the owner is cleared, so a late teardown
    receipt from the old owner is refused as stale.
    """

    work_item = await _lock_work_item(session, work_item_id)
    if work_item is None:
        return await _conflict(
            session, "not_found", work_item_id=work_item_id, request_id=request_id
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
    window = timedelta(seconds=settle_seconds)
    now = await _database_now(session)
    if (
        request.status != "cancellation_requested"
        or request.terminal_cause != "issue_cancelled"
        or request.cancellation_requested_at is None
        or request.cancellation_requested_at + window > now
        or request.terminate_published_at is None
        or not (
            request.runtime_owner is None
            or (
                request.runtime_heartbeat_expires_at is not None
                and request.runtime_heartbeat_expires_at <= now
            )
        )
    ):
        return await _conflict(
            session, "illegal_transition", work_item=work_item, request=request
        )
    changed_id: uuid.UUID | None = await session.scalar(
        update(ExecutionRequest)
        .where(
            ExecutionRequest.id == request_id,
            ExecutionRequest.work_item_id == work_item_id,
            ExecutionRequest.version == expected_request_version,
            ExecutionRequest.status == "cancellation_requested",
            ExecutionRequest.terminal_cause == "issue_cancelled",
            ExecutionRequest.terminate_published_at.is_not(None),
            ExecutionRequest.cancellation_requested_at <= now - window,
            ExecutionRequest.runtime_owner.is_(None)
            | (
                ExecutionRequest.runtime_heartbeat_expires_at.is_not(None)
                & (ExecutionRequest.runtime_heartbeat_expires_at <= now)
            ),
        )
        .values(
            status="cancelled",
            terminal_at=now,
            termination_observation=(
                f"settled by the control plane after {settle_seconds}s "
                "without a worker teardown receipt"
            ),
            teardown_unconfirmed_at=now,
            runtime_epoch=ExecutionRequest.runtime_epoch + 1,
            runtime_owner=None,
            version=ExecutionRequest.version + 1,
            updated_at=func.clock_timestamp(),
        )
        .returning(ExecutionRequest.id)
    )
    if changed_id is None:
        request = await _reload_request(session, request_id)
        return await _conflict(session, "stale_version", work_item=work_item, request=request)
    request = await _reload_request(session, request_id)
    await _settle_terminal(session, work_item, request, detail=None)
    return await _outcome(session, work_item, request)


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
            cancellation_requested_at=now,
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
    published = await session.scalar(
        select(Publication.id)
        .where(
            Publication.execution_request_id == request.id,
            Publication.status == "succeeded",
        )
        .limit(1)
    )
    if published is not None:
        # A published request waits on CI; the CI gate owns its terminus.
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
            cancellation_requested_at=now,
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
    if request.status == "cancelled" and request.teardown_unconfirmed_at is not None:
        return await _confirm_settled_teardown(
            session,
            work_item=work_item,
            request=request,
            termination_observation=termination_observation,
            extra_where=extra_where,
        )
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
    await _settle_terminal(session, work_item, request, detail=None)
    return await _outcome(session, work_item, request)


async def _confirm_settled_teardown(
    session: AsyncSession,
    *,
    work_item: WorkItem,
    request: ExecutionRequest,
    termination_observation: str,
    extra_where: Sequence[ColumnElement[bool]],
) -> WorkItemResult:
    """Record a late worker teardown for a force-settled cancellation.

    The request is already terminal and its notice already went out, so this
    only replaces the placeholder observation and clears the flag.
    """

    if not termination_observation.strip():
        return await _conflict(
            session,
            "termination_observation_required",
            work_item=work_item,
            request=request,
        )
    changed_id: uuid.UUID | None = await session.scalar(
        update(ExecutionRequest)
        .where(
            ExecutionRequest.id == request.id,
            ExecutionRequest.work_item_id == work_item.id,
            ExecutionRequest.version == request.version,
            ExecutionRequest.status == "cancelled",
            ExecutionRequest.teardown_unconfirmed_at.is_not(None),
            *extra_where,
        )
        .values(
            termination_observation=termination_observation,
            teardown_unconfirmed_at=None,
            runtime_owner=None,
            version=ExecutionRequest.version + 1,
            updated_at=func.clock_timestamp(),
        )
        .returning(ExecutionRequest.id)
    )
    if changed_id is None:
        reloaded = await _reload_request(session, request.id)
        return await _conflict(
            session, "stale_version", work_item=work_item, request=reloaded
        )
    request = await _reload_request(session, request.id)
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
    publication_id: uuid.UUID


async def claim_publication_settlement(
    session: AsyncSession,
    *,
    exclude: frozenset[uuid.UUID],
) -> PublicationSettlement | None:
    """The only reader that decides a linked publication's execution terminus.

    Callers then use ``fail_execution``, or the CI gate for a ``completed``
    settlement (#3097). ``exclude`` skips requests the caller already handled
    this pass, so one request waiting on CI cannot starve the rest. This
    function does not commit and does not write the request.
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
                *((ExecutionRequest.id.not_in(exclude),) if exclude else ()),
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
                Publication.status.in_(_IN_FLIGHT_PUBLICATION),
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
            publication_id=publication.id,
        )
    return None
