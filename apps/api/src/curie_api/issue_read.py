"""Read one factory WorkItem's ticket for its sandbox (ADR 0187, ADR 0197).

The bundle holds no tracker credential. It presents the execution scoped
``wir`` capability, and the API reads the WorkItem's ticket through its
tracker's `Tracker.read_ticket`, for every tracker kind: a GitHub issue with
an App installation token minted fresh on every read, only for the
installation the WorkItem recorded, so a run longer than the token's one hour
lifetime keeps reading; a Jira issue with the tracker's own credential.
Nothing is parsed, modelled or stored: the ticket is returned as the
tracker's markdown.
"""

from __future__ import annotations

import asyncio
import uuid
from dataclasses import dataclass
from datetime import datetime

import httpx
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from .config import Settings
from .forges.errors import ForgeError
from .forges.registry import forges_registry
from .forges.trackers import tracker_for_work_item
from .forges.types import GITHUB, RepositoryRef, TrackerIssueRef
from .github_app import GitHubAppError
from .models import MAX_EXECUTION_DEADLINE_SECONDS, ExecutionRequest, WorkItem

ISSUE_READ_TIMEOUT_SECONDS = 20.0
_PROVIDER_TIMEOUT_SECONDS = 5.0
# The capability lives as long as the longest execution deadline
# plus room for the boot and wait that precede the start grant. The read route
# still refuses once the durable execution has ended or passed its deadline.
CAPABILITY_TTL_SECONDS = MAX_EXECUTION_DEADLINE_SECONDS + 1_800
# Only these may still become or remain the running execution.
_MINTABLE_STATUSES = ("queued", "waiting", "running")


class IssueReadRefused(RuntimeError):
    """The durable execution no longer grants this read."""


class IssueReadUnavailable(RuntimeError):
    """The tracker could not answer the read."""


@dataclass(frozen=True)
class IssueReadAuthority:
    work_item_id: uuid.UUID
    execution_request_id: uuid.UUID
    issue: TrackerIssueRef
    repository: RepositoryRef
    code_host_installation_id: int | None
    started_at: datetime | None
    execution_deadline: datetime | None


async def read_issue_authority(
    session: AsyncSession, *, execution_request_id: uuid.UUID, running: bool
) -> IssueReadAuthority:
    """The request's WorkItem, refused unless the execution is current.

    ``running`` demands the started, unexpired execution the read route
    serves, held by a runtime whose heartbeat lease is current. Minting at
    sandbox boot precedes the start grant, so it accepts any status that can
    still become that execution.
    """

    result = await session.execute(
        select(WorkItem, ExecutionRequest, func.clock_timestamp())
        .select_from(ExecutionRequest)
        .join(WorkItem, WorkItem.id == ExecutionRequest.work_item_id)
        .where(ExecutionRequest.id == execution_request_id)
        .execution_options(populate_existing=True)
    )
    row = result.one_or_none()
    if row is None:
        raise IssueReadRefused
    item, execution, now = row
    # A GitHub issue is read only with a token for its recorded installation.
    if item.cancelled_at is not None or (
        item.tracker_kind == GITHUB and item.code_host_installation_id is None
    ):
        raise IssueReadRefused
    if running:
        if (
            execution.status != "running"
            or not execution.runtime_owner
            or execution.runtime_heartbeat_expires_at is None
            or execution.runtime_heartbeat_expires_at <= now
            or execution.execution_deadline is None
            or execution.execution_deadline <= now
        ):
            raise IssueReadRefused
    elif execution.status not in _MINTABLE_STATUSES or (
        execution.execution_deadline is not None and execution.execution_deadline <= now
    ):
        raise IssueReadRefused
    return IssueReadAuthority(
        work_item_id=item.id,
        execution_request_id=execution.id,
        issue=item.tracker_issue,
        repository=item.repository,
        code_host_installation_id=item.code_host_installation_id,
        started_at=execution.started_at,
        execution_deadline=execution.execution_deadline,
    )


async def read_issue(
    authority: IssueReadAuthority, *, settings: Settings, client: httpx.AsyncClient
) -> str:
    """The WorkItem's ticket as markdown, through its own tracker.

    A GitHub issue is addressed by its repository's numeric id, so a rename or
    a transfer cannot redirect the read, and its token is minted only for the
    recorded installation with the provider timeout capped for this read.
    """

    try:
        tracker = tracker_for_work_item(
            forges_registry(settings),
            client,
            authority.issue,
            authority.repository,
            authority.code_host_installation_id,
            timeout_s=_PROVIDER_TIMEOUT_SECONDS,
        )
        async with asyncio.timeout(ISSUE_READ_TIMEOUT_SECONDS):
            return await tracker.read_ticket(authority.issue)
    except (GitHubAppError, ForgeError, ValueError, TimeoutError):
        raise IssueReadUnavailable from None
