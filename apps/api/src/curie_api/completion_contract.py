"""The fences the issue read capability's sibling routes share.

``/work-items/completion-contract``, ``/work-items/questions`` and
``/work-items/rule-proposal`` (`routers/work_item_issue.py`) each load the
capability's request and WorkItem, check that the capability names exactly
that WorkItem's issue, and refuse a request that can no longer run. The
WorkItem's tracker is resolved here too, for the question route and the
reconciler.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import datetime
from typing import Final

import httpx
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from .config import Settings
from .forges.ports import Tracker
from .forges.registry import forges_registry
from .forges.trackers import tracker_for_work_item
from .forges.types import GITHUB, TrackerIssueRef
from .models import ExecutionRequest, WorkItem

_PROVIDER_TIMEOUT_SECONDS = 5.0
# Statuses in which the request is still, or may still become, its execution.
_LIVE_STATUSES = ("queued", "waiting", "running")

# The refusal code the sibling routes answer with 409 when the capability no
# longer grants the call.
INVALID_CONTEXT: Final = "invalid_context"


async def _database_now(session: AsyncSession) -> datetime:
    now = await session.scalar(select(func.clock_timestamp()))
    assert isinstance(now, datetime)
    return now


def work_item_tracker(
    settings: Settings, client: httpx.AsyncClient, work_item: WorkItem
) -> Tracker:
    """The tracker the WorkItem's issue lives on, with a capped provider timeout.

    The one place the runner-facing routes and the reconciler resolve it, so a
    GitHub issue is read and written only with a token for its recorded
    installation. Raises the forge errors of `tracker_for_work_item`.
    """

    return tracker_for_work_item(
        forges_registry(settings),
        client,
        work_item.tracker_issue,
        work_item.repository,
        work_item.code_host_installation_id,
        timeout_s=_PROVIDER_TIMEOUT_SECONDS,
    )


class RuntimeRefused(Exception):
    """A runner-facing route refuses with ``code``; nothing was written."""

    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(f"runner-facing route refused: {code}")


@dataclass(frozen=True)
class RuntimePair:
    work_item: WorkItem
    request: ExecutionRequest


async def load_pair(
    session: AsyncSession, request_id: uuid.UUID, *, lock: bool = False
) -> RuntimePair | None:
    """The request and its WorkItem.

    ``lock`` takes both row locks in the order every `workitems/lifecycle.py`
    transition takes them, the WorkItem's and then the request's, through the
    lifecycle's own helpers. The reverse order deadlocks against the worker's
    start transition (#5687).
    """

    # Deferred: lifecycle imports modules that import this one.
    from .workitems.lifecycle import lock_request, lock_work_item

    row = (
        await session.execute(
            select(WorkItem, ExecutionRequest)
            .select_from(ExecutionRequest)
            .join(WorkItem, WorkItem.id == ExecutionRequest.work_item_id)
            .where(ExecutionRequest.id == request_id)
            .execution_options(populate_existing=True)
        )
    ).one_or_none()
    if row is None:
        return None
    if not lock:
        return RuntimePair(work_item=row[0], request=row[1])
    work_item = await lock_work_item(session, row[0].id)
    if work_item is None:
        return None
    request = await lock_request(session, work_item_id=work_item.id, request_id=request_id)
    if request is None:
        return None
    return RuntimePair(work_item=work_item, request=request)


def names_issue(pair: RuntimePair, issue: TrackerIssueRef) -> bool:
    """Whether ``issue`` (the capability's) is the durable WorkItem's issue,
    and the WorkItem can be read at all (a GitHub issue only with its
    recorded installation)."""

    item = pair.work_item
    if item.tracker_kind == GITHUB and item.code_host_installation_id is None:
        return False
    stored = item.tracker_issue
    return (stored.kind, stored.host, stored.scope_id, stored.issue_id) == (
        issue.kind,
        issue.host,
        issue.scope_id,
        issue.issue_id,
    )


async def contract_precheck(session: AsyncSession, pair: RuntimePair) -> None:
    """Refuse `INVALID_CONTEXT` when the request can no longer run: its
    WorkItem is cancelled, it left the live statuses, or its execution
    deadline passed on the database clock."""

    now = await _database_now(session)
    request = pair.request
    if (
        pair.work_item.cancelled_at is not None
        or request.status not in _LIVE_STATUSES
        or (request.execution_deadline is not None and request.execution_deadline <= now)
    ):
        raise RuntimeRefused(INVALID_CONTEXT)
