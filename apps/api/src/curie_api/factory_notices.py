"""Post the one comment owed by a factory terminus.

The notice row is already committed with the terminal update. This module only
updates that notice. A refused post never rewrites the execution request.

An issue-originated request comments on its issue. A revision asked for from
pull request review feedback (#2798) answers on the pull request: a review
comment gets a reply in its thread, other feedback a linked PR comment, and a
thread reply GitHub refuses with 422 falls back to that linked PR comment.

GitHub issue comments:
https://docs.github.com/en/rest/issues/comments#create-an-issue-comment
https://docs.github.com/en/rest/issues/comments#list-issue-comments
Pull request review comments:
https://docs.github.com/en/rest/pulls/comments#create-a-reply-for-a-review-comment
https://docs.github.com/en/rest/pulls/comments#list-review-comments-on-a-pull-request
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
from .factory_reply_target import ReplyTarget, parse_reply_target
from .github_app import GitHubAppError, GitHubInstallationRefused, credentials_for
from .models import (
    ExecutionRequest,
    FactoryTerminalNotice,
    ThreadPublicationLineage,
    WorkItem,
)
from .repo_full_name import repo_url_path

_REFUSED_STATUSES = {401, 403, 404}
_PAGES_PER_PASS = 5
_UNPROCESSABLE = ("unprocessable", "http_422")
# A thread target scans two lists with one stored page. Pages of the review
# comment list are stored as-is; once that list is exhausted, the conversation
# list page is stored above this offset.
_SECOND_LIST_OFFSET = 1_000_000


@dataclass(frozen=True)
class _MarkerScan:
    comment_id: int | None = None
    refusal: str | None = None
    unavailable: bool = False
    next_page: int | None = None


# The operator-facing sentence for each terminus cause (#3073). The raw code
# still appears on the comment, but never as its headline.
_CAUSE_TEXT = {
    "model_credit_exhausted": (
        "the model provider refused the request because the account has run "
        "out of credits. Add credits or raise the key's limit, then retry."
    ),
    "model_credential_rejected": (
        "the model provider rejected the configured API key. Check the model "
        "credential, then retry."
    ),
    "model_rate_limited": (
        "the model provider kept rate limiting the request. Retry later or raise "
        "the provider limit."
    ),
    "model_error": "the model provider returned an error the run could not recover from.",
    "budget_exceeded": "the run used its whole token budget before it finished.",
    "runner_timeout": "the run took longer than its time limit.",
    "workspace_error": "the repository workspace could not be prepared for the run.",
    "runner_escalated": "the run stopped on an error and was handed to a person.",
    "runner_failed": "the run ended without a result.",
    "no_pull_request": "the run finished but did not open a pull request.",
    "execution_deadline": "the run did not finish before its deadline.",
    "capacity_wait_expired": "no runner capacity came free before the wait expired.",
    "owner_lost": "the worker running this request stopped responding.",
    "issue_cancelled": "the request was cancelled.",
    "publication_denied": "a person denied the request to open the pull request.",
    "publication_expired": (
        "the request to open the pull request expired before anyone approved it."
    ),
    "publication_failed": "the pull request could not be opened.",
}


def cause_text(cause: str) -> str:
    """A plain sentence for a terminus cause; unknown codes get a generic one."""

    return _CAUSE_TEXT.get(cause, "the run stopped for a reason Curie did not recognize.")


def marker_for(request_id: uuid.UUID) -> str:
    return f"<!-- curie-execution-request:{request_id} -->"


def comment_body(
    request_id: uuid.UUID,
    cause: str,
    *,
    pr_url: str | None,
    feedback_url: str | None = None,
    detail: str | None = None,
) -> str:
    if cause == "completed":
        if feedback_url is not None:
            text = "The requested revision is pushed to this pull request.\n"
        elif isinstance(pr_url, str) and pr_url.strip():
            text = f"Completed: {pr_url.strip()}\n"
        else:
            raise ValueError("a completed issue notice requires its pull request URL")
    else:
        text = f"Could not complete: {cause_text(cause)}\n"
        if detail is not None and detail.strip():
            text += f"Provider message: {detail.strip()}\n"
        text += f"Cause: {cause}\n"
    if feedback_url is not None:
        text += f"In response to {feedback_url}\n"
    return f"{text}\n{marker_for(request_id)}\n"


async def post_due_notices(session: AsyncSession, settings: Settings, *, limit: int = 20) -> int:
    """Post or record every due notice this transaction can lock.

    The row lock is held across the GitHub call so a second reconciler skips
    it. A crash before commit leaves the notice pending. The next pass records
    a comment that already carries the marker instead of posting another.
    """

    rows = (
        await session.execute(
            select(
                FactoryTerminalNotice,
                WorkItem,
                ExecutionRequest.objective,
                ThreadPublicationLineage.pr_url,
            )
            .join(WorkItem, WorkItem.id == FactoryTerminalNotice.work_item_id)
            .join(
                ExecutionRequest,
                ExecutionRequest.id == FactoryTerminalNotice.execution_request_id,
            )
            .outerjoin(
                ThreadPublicationLineage,
                ThreadPublicationLineage.id == WorkItem.publication_lineage_id,
            )
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
        for notice, work_item, objective, pr_url in rows:
            target = parse_reply_target(
                objective,
                repo_full_name=work_item.repo_full_name,
                clone_base=settings.github_clone_base,
            )
            outcome = await _deliver(
                client,
                settings,
                work_item,
                notice,
                target,
                pr_url=pr_url,
            )
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
    target: ReplyTarget,
    *,
    pr_url: str | None,
) -> tuple[str, str] | None:
    if (
        notice.terminal_cause == "completed"
        and target.kind == "issue"
        and (not isinstance(pr_url, str) or not pr_url.strip())
    ):
        return None
    try:
        token = await run_in_threadpool(
            credentials_for(settings).token_for_verified_installation,
            work_item.repo_full_name,
            work_item.github_installation_id,
        )
    except (GitHubInstallationRefused, GitHubAppError, ValueError):
        return None
    api = settings.github_api_url.rstrip("/")
    repo_path = f"/repos/{repo_url_path(work_item.repo_full_name)}"
    number = work_item.github_issue_number if target.pr_number is None else target.pr_number
    comments_path = f"{repo_path}/issues/{number}/comments"
    headers = {
        "Accept": "application/vnd.github+json",
        "Authorization": f"Bearer {token}",
        "X-GitHub-Api-Version": "2022-11-28",
    }
    marker = marker_for(notice.execution_request_id)
    stored = max(1, notice.scan_page)
    # (path, first page, offset stored for this list's next page)
    scans = [(comments_path, stored, 0)]
    if target.kind == "thread":
        # A thread reply lands on the review comment list; its 422 fallback on
        # the conversation list. The marker may sit on either.
        review_path = f"{repo_path}/pulls/{number}/comments"
        if stored > _SECOND_LIST_OFFSET:
            scans = [(comments_path, stored - _SECOND_LIST_OFFSET, _SECOND_LIST_OFFSET)]
        else:
            scans = [(review_path, stored, 0), (comments_path, 1, _SECOND_LIST_OFFSET)]
    for path, start, offset in scans:
        existing = await _find_marker(client, api, path, headers, marker, start_page=start)
        if existing.refusal is not None:
            return ("refused", existing.refusal)
        if existing.unavailable:
            return None
        if existing.comment_id is not None:
            return ("posted", str(existing.comment_id))
        if existing.next_page is not None:
            notice.scan_page = existing.next_page + offset
            return None
    body = comment_body(
        notice.execution_request_id,
        notice.terminal_cause,
        pr_url=pr_url,
        feedback_url=target.url,
        detail=notice.detail,
    )
    if target.kind == "thread":
        assert target.comment_id is not None
        root = await _thread_root(client, api, repo_path, headers, target.comment_id)
        replied = await _post(
            client,
            f"{api}{repo_path}/pulls/{number}/comments/{root}/replies",
            headers,
            body,
        )
        if replied != _UNPROCESSABLE:
            if replied is None:
                # Lost response after a possible success: rescan every list next pass.
                notice.scan_page = 1
            return replied
    posted = await _post(client, f"{api}{comments_path}", headers, body)
    if posted is None:
        # Lost response after a possible success: rescan every list next pass.
        notice.scan_page = 1
    # A comment GitHub cannot process stays pending, as before #2798.
    return None if posted == _UNPROCESSABLE else posted


async def _thread_root(
    client: httpx.AsyncClient,
    api: str,
    repo_path: str,
    headers: dict[str, str],
    comment_id: int,
) -> int:
    """Reply to the thread's first comment; replies to replies are refused."""

    try:
        found = await client.get(
            f"{api}{repo_path}/pulls/comments/{comment_id}",
            headers=headers,
            follow_redirects=False,
        )
        payload = found.json() if found.status_code == 200 else None
    except (httpx.HTTPError, ValueError):
        payload = None
    if isinstance(payload, dict):
        root = payload.get("in_reply_to_id")
        if type(root) is int and root > 0:
            return root
    return comment_id


async def _post(
    client: httpx.AsyncClient, url: str, headers: dict[str, str], body: str
) -> tuple[str, str] | None:
    try:
        created = await client.post(
            url, headers=headers, json={"body": body}, follow_redirects=False
        )
    except httpx.HTTPError:
        return None
    if created.status_code == 422:
        return _UNPROCESSABLE
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
