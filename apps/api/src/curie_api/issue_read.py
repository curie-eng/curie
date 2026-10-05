"""Read one factory WorkItem's issue for its sandbox (ADR 0187).

The bundle holds no GitHub credential. It presents the execution scoped
``wir`` capability, and the API reads the WorkItem's issue and its comments
through the GitHub tracker adapter with the App installation token, minted
fresh on every read so a run longer than the token's one hour lifetime keeps
reading. Nothing is parsed, modelled or stored: the title, body and comments
are returned verbatim.
"""

from __future__ import annotations

import asyncio
import uuid
from dataclasses import dataclass
from datetime import datetime

import httpx
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession
from starlette.concurrency import run_in_threadpool

from .config import Settings
from .forges.errors import ForgeError
from .forges.github.comments import static_token
from .forges.github.tracker import GitHubTracker, IssueContent
from .github_app import GitHubAppError, GitHubCredentials
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
    """GitHub could not answer the read."""


@dataclass(frozen=True)
class IssueReadAuthority:
    work_item_id: uuid.UUID
    execution_request_id: uuid.UUID
    repo_full_name: str
    github_repository_id: int
    github_installation_id: int
    issue_number: int
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
    if item.cancelled_at is not None:
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
        repo_full_name=item.repo_full_name,
        github_repository_id=item.github_repository_id,
        github_installation_id=item.github_installation_id,
        issue_number=item.github_issue_number,
        execution_deadline=execution.execution_deadline,
    )


async def read_issue(
    authority: IssueReadAuthority, *, settings: Settings, client: httpx.AsyncClient
) -> IssueContent:
    """GET the issue and its comments by repository id, with a fresh App token.

    Addressing the repository by its numeric id keeps a rename or a transfer
    from redirecting the read to a different repository than the WorkItem's.
    """

    resolver = GitHubCredentials(
        settings=settings.model_copy(
            update={
                "github_app_timeout_seconds": min(
                    settings.github_app_timeout_seconds, _PROVIDER_TIMEOUT_SECONDS
                ),
            }
        )
    )
    try:
        async with asyncio.timeout(ISSUE_READ_TIMEOUT_SECONDS):
            token = await run_in_threadpool(
                resolver.token_for_verified_installation,
                authority.repo_full_name,
                authority.github_installation_id,
            )
            tracker = GitHubTracker.from_settings(
                settings,
                client,
                repo_full_name=authority.repo_full_name,
                repository_id=authority.github_repository_id,
                token=static_token(token),
            )
            return await tracker.read_issue_content(authority.issue_number)
    except (GitHubAppError, ForgeError, ValueError, TimeoutError):
        raise IssueReadUnavailable from None
