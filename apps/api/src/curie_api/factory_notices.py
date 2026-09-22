"""Post the one comment owed by a non-PR factory terminus.

The notice row is already committed with the terminal update. This module only
updates that notice. A refused post never rewrites the execution request.

GitHub issue comments:
https://docs.github.com/en/rest/issues/comments#create-an-issue-comment
https://docs.github.com/en/rest/issues/comments#list-issue-comments
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from typing import Any

import httpx
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from starlette.concurrency import run_in_threadpool

from .config import Settings
from .github_app import GitHubAppError, GitHubInstallationRefused, credentials_for
from .models import FactoryTerminalNotice, WorkItem
from .repo_full_name import repo_url_path

_REFUSED_STATUSES = {401, 403, 404}
_PAGES_PER_PASS = 5


@dataclass(frozen=True)
class _MarkerScan:
    comment_id: int | None = None
    refusal: str | None = None
    unavailable: bool = False
    next_page: int | None = None


def marker_for(request_id: uuid.UUID) -> str:
    return f"<!-- curie-execution-request:{request_id} -->"


def comment_body(request_id: uuid.UUID, cause: str) -> str:
    return (
        "This factory run cannot continue.\n"
        f"Cause: {cause}\n"
        f"\n{marker_for(request_id)}\n"
    )


async def post_due_notices(session: AsyncSession, settings: Settings, *, limit: int = 20) -> int:
    """Post or record every due notice this transaction can lock.

    The row lock is held across the GitHub call so a second reconciler skips
    it. A crash before commit leaves the notice pending. The next pass records
    a comment that already carries the marker instead of posting another.
    """

    rows = (
        await session.execute(
            select(FactoryTerminalNotice, WorkItem)
            .join(WorkItem, WorkItem.id == FactoryTerminalNotice.work_item_id)
            .where(
                FactoryTerminalNotice.posted_at.is_(None),
                FactoryTerminalNotice.refused_at.is_(None),
            )
            .order_by(
                FactoryTerminalNotice.attempts,
                FactoryTerminalNotice.created_at,
            )
            .limit(limit)
            .with_for_update(skip_locked=True, of=FactoryTerminalNotice)
        )
    ).all()
    if not rows:
        await session.commit()
        return 0
    delivered = 0
    async with httpx.AsyncClient(timeout=settings.github_app_timeout_seconds) as client:
        for notice, work_item in rows:
            outcome = await _deliver(client, settings, work_item, notice)
            now = await _clock(session)
            notice.attempts += 1
            if outcome is None:
                continue
            kind, detail = outcome
            if kind == "posted":
                notice.comment_id = int(detail)
                notice.posted_at = now
                delivered += 1
            else:
                notice.refusal = detail
                notice.refused_at = now
    await session.commit()
    return delivered


async def _clock(session: AsyncSession) -> Any:
    from sqlalchemy import func

    return await session.scalar(select(func.clock_timestamp()))


async def _deliver(
    client: httpx.AsyncClient,
    settings: Settings,
    work_item: WorkItem,
    notice: FactoryTerminalNotice,
) -> tuple[str, str] | None:
    try:
        token = await run_in_threadpool(
            credentials_for(settings).token_for_verified_installation,
            work_item.repo_full_name,
            work_item.github_installation_id,
        )
    except (GitHubInstallationRefused, GitHubAppError, ValueError):
        return None
    api = settings.github_api_url.rstrip("/")
    path = (
        f"/repos/{repo_url_path(work_item.repo_full_name)}"
        f"/issues/{work_item.github_issue_number}/comments"
    )
    headers = {
        "Accept": "application/vnd.github+json",
        "Authorization": f"Bearer {token}",
        "X-GitHub-Api-Version": "2022-11-28",
    }
    marker = marker_for(notice.execution_request_id)
    existing = await _find_marker(
        client, api, path, headers, marker, start_page=max(1, notice.scan_page)
    )
    if existing.refusal is not None:
        return ("refused", existing.refusal)
    if existing.unavailable:
        return None
    if existing.comment_id is not None:
        return ("posted", str(existing.comment_id))
    if existing.next_page is not None:
        notice.scan_page = existing.next_page
        return None
    body = comment_body(notice.execution_request_id, notice.terminal_cause)
    try:
        created = await client.post(
            f"{api}{path}",
            headers=headers,
            json={"body": body},
            follow_redirects=False,
        )
    except httpx.HTTPError:
        return None
    if created.status_code in _REFUSED_STATUSES:
        return ("refused", f"http_{created.status_code}")
    if created.status_code not in {200, 201}:
        return None
    try:
        payload = created.json()
    except ValueError:
        return None
    if not isinstance(payload, dict) or type(payload.get("id")) is not int:
        return None
    return ("posted", str(payload["id"]))


async def _find_marker(
    client: httpx.AsyncClient,
    api: str,
    path: str,
    headers: dict[str, str],
    marker: str,
    *,
    start_page: int,
) -> _MarkerScan:
    """Find a marker, or remember the next page so a later pass can continue.

    A full page does not mean the marker is absent. Stopping there and posting
    would duplicate a comment that sits further down the list. A short page is
    the end, so a missing marker is safe to post.
    """

    page = start_page
    for _ in range(_PAGES_PER_PASS):
        try:
            listed = await client.get(
                f"{api}{path}",
                headers=headers,
                params={"per_page": 100, "page": page},
                follow_redirects=False,
            )
        except httpx.HTTPError:
            return _MarkerScan(unavailable=True)
        if listed.status_code in _REFUSED_STATUSES:
            return _MarkerScan(refusal=f"http_{listed.status_code}")
        if listed.status_code != 200:
            return _MarkerScan(unavailable=True)
        try:
            payload = listed.json()
        except ValueError:
            return _MarkerScan(unavailable=True)
        found = _comment_id(payload, marker)
        if found is not None:
            return _MarkerScan(comment_id=found)
        if not isinstance(payload, list) or len(payload) < 100:
            return _MarkerScan()
        page += 1
    return _MarkerScan(next_page=page)


def _comment_id(payload: Any, marker: str) -> int | None:
    if not isinstance(payload, list):
        return None
    for item in payload:
        if not isinstance(item, dict):
            continue
        body = item.get("body")
        comment_id = item.get("id")
        if isinstance(body, str) and marker in body and type(comment_id) is int:
            return comment_id
    return None
