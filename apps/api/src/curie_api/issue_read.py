"""Read one factory WorkItem's issue for its sandbox (ADR 0187).

The bundle holds no GitHub credential. It presents the execution scoped
``wir`` capability, and the API reads the WorkItem's issue and its comments
with the App installation token, minted fresh on every read so a run longer
than the token's one hour lifetime keeps reading. Nothing is parsed, modelled
or stored: the title, body and comments are returned verbatim.
"""

from __future__ import annotations

import asyncio
import json
import uuid
from dataclasses import dataclass
from datetime import datetime
from typing import Any

import httpx
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession
from starlette.concurrency import run_in_threadpool

from .config import Settings
from .forges.github.transport import github_headers
from .github_app import GitHubAppError, GitHubCredentials
from .models import MAX_EXECUTION_DEADLINE_SECONDS, ExecutionRequest, WorkItem

ISSUE_READ_TIMEOUT_SECONDS = 20.0
_PROVIDER_TIMEOUT_SECONDS = 5.0
_MAX_RESPONSE_BYTES = 2_097_152
_COMMENTS_PER_PAGE = 100
# Ten pages is a thousand comments. A longer thread is truncated and says so,
# rather than turning one tool call into an unbounded walk of the API.
_MAX_COMMENT_PAGES = 10
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


@dataclass(frozen=True)
class IssueComment:
    author: str | None
    created_at: str | None
    body: str


@dataclass(frozen=True)
class IssueContent:
    title: str
    body: str
    state: str | None
    author: str | None
    comments: list[IssueComment]
    comments_truncated: bool


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


def _login(user: object) -> str | None:
    login = user.get("login") if isinstance(user, dict) else None
    return login if isinstance(login, str) else None


def _text(value: object) -> str:
    if value is None:
        return ""
    if not isinstance(value, str):
        raise IssueReadUnavailable
    return value


async def _get_json(
    client: httpx.AsyncClient, url: str, *, token: str, params: dict[str, Any] | None = None
) -> tuple[Any, httpx.Headers]:
    async with client.stream(
        "GET",
        url,
        params=params,
        headers=github_headers(token),
        timeout=_PROVIDER_TIMEOUT_SECONDS,
        follow_redirects=False,
    ) as response:
        if response.status_code != 200:
            raise IssueReadUnavailable
        body = bytearray()
        async for chunk in response.aiter_bytes():
            body.extend(chunk)
            if len(body) > _MAX_RESPONSE_BYTES:
                raise IssueReadUnavailable
        return json.loads(body), response.headers


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
    base = (
        f"{settings.github_api_url.rstrip('/')}/repositories/"
        f"{authority.github_repository_id}/issues/{authority.issue_number}"
    )
    try:
        async with asyncio.timeout(ISSUE_READ_TIMEOUT_SECONDS):
            token = await run_in_threadpool(
                resolver.token_for_verified_installation,
                authority.repo_full_name,
                authority.github_installation_id,
            )
            issue, _ = await _get_json(client, base, token=token)
            comments: list[IssueComment] = []
            truncated = False
            for page in range(1, _MAX_COMMENT_PAGES + 1):
                batch, headers = await _get_json(
                    client,
                    f"{base}/comments",
                    token=token,
                    params={"per_page": _COMMENTS_PER_PAGE, "page": page},
                )
                if not isinstance(batch, list):
                    raise IssueReadUnavailable
                for comment in batch:
                    if not isinstance(comment, dict):
                        raise IssueReadUnavailable
                    created = comment.get("created_at")
                    comments.append(
                        IssueComment(
                            author=_login(comment.get("user")),
                            created_at=created if isinstance(created, str) else None,
                            body=_text(comment.get("body")),
                        )
                    )
                if 'rel="next"' not in headers.get("link", ""):
                    break
            else:
                truncated = True
    except (GitHubAppError, httpx.HTTPError, ValueError, TimeoutError):
        raise IssueReadUnavailable from None
    if (
        not isinstance(issue, dict)
        or type(issue.get("number")) is not int
        or issue["number"] != authority.issue_number
        or "pull_request" in issue
        or not isinstance(issue.get("title"), str)
    ):
        raise IssueReadUnavailable
    state = issue.get("state")
    return IssueContent(
        title=issue["title"],
        body=_text(issue.get("body")),
        state=state if isinstance(state, str) else None,
        author=_login(issue.get("user")),
        comments=comments,
        comments_truncated=truncated,
    )
