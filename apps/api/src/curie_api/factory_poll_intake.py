"""Poll GitHub for factory intake (#3745).

Polling is the default door. One pass lists labeled issues, mentions, and
review feedback for each bound repository, then admits through the same
verification the webhook uses. It does not write a delivery receipt.

Cursors live in ``curie.factory_poll_cursors``. ``since`` moves only to the
newest timestamp on a page that was applied, because GitHub's ``since`` is
inclusive. An ETag is stored only after that apply. A 304 creates no work.

The pass holds ``pg_try_advisory_lock(3745, 187)`` on one connection. A
replica that does not get the lock makes no GitHub request.

List and conditional reads follow:
https://docs.github.com/en/rest/issues/issues#list-repository-issues
https://docs.github.com/en/rest/issues/events#list-issue-events
https://docs.github.com/en/rest/issues/comments#list-issue-comments-for-a-repository
https://docs.github.com/en/rest/pulls/comments#list-review-comments-in-a-repository
https://docs.github.com/en/rest/pulls/reviews#list-reviews-for-a-pull-request
https://docs.github.com/en/rest/using-the-rest-api/getting-started-with-the-rest-api#conditional-requests
"""

from __future__ import annotations

import logging
import uuid
from dataclasses import replace
from datetime import UTC, datetime
from typing import Any

import httpx
from sqlalchemy import desc, select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from starlette.concurrency import run_in_threadpool

from curie_api.factory_label_reconcile import bound_repositories, parse_time

from . import github_factory
from .config import Settings
from .forges.errors import Unavailable
from .forges.github.comments import static_token
from .forges.github.review_polling import (
    Cursor,
    admit_one_feedback,
    admit_review_comments,
    admit_reviews,
    advance,
    human_actor,
    open_pulls,
    read_pull,
    repository_payload,
    since_param,
    trailing_id,
)
from .forges.github.tracker import (
    GitHubTracker,
    label_names,
    last_event_of_kind,
    last_label_event,
    read_repository,
)
from .forges.github.transport import PollUnavailable
from .github_app import GitHubAppError, GitHubInstallationRefused, credentials_for
from .github_factory_events import FactoryNotice, mentions_login
from .github_review_events import FeedbackIgnored, FeedbackUnavailable
from .models import ExecutionRequest, FactoryPollCursor, WorkItem
from .repo_full_name import InvalidRepoFullName, normalize_repo_full_name, repo_url_path
from .workspace_policy import repository_is_allowed

logger = logging.getLogger(__name__)

_LOCK = text("SELECT pg_try_advisory_lock(CAST(:classid AS integer), CAST(:objid AS integer))")
_UNLOCK = text("SELECT pg_advisory_unlock(CAST(:classid AS integer), CAST(:objid AS integer))")
_LOCK_ARGS = {"classid": 3745, "objid": 187}


def _engine(sessionmaker: async_sessionmaker[AsyncSession]) -> Any:
    bind = sessionmaker.kw.get("bind")
    if bind is not None:
        return bind
    raise RuntimeError("factory poll sessionmaker has no bind")


async def _label_already_admitted(
    sessionmaker: async_sessionmaker[AsyncSession],
    repository_id: int,
    number: int,
    event: dict[str, Any],
) -> bool:
    """True when this label event is not newer than the work item's latest request.

    Requests admitted through a webhook use the delivery id, not the timeline
    event id. Treating that same label as a new admission would cancel the
    live run. A later label event still readmits.
    """

    event_at = parse_time(event.get("created_at"))
    if event_at is None:
        return False
    async with sessionmaker() as session:
        item = await github_factory.work_item_for(session, repository_id, number)
        if item is None or item.cancelled_at is not None:
            return False
        latest = await session.scalar(
            select(ExecutionRequest.created_at)
            .where(ExecutionRequest.work_item_id == item.id)
            .order_by(desc(ExecutionRequest.sequence))
            .limit(1)
        )
    return latest is not None and event_at <= latest


async def poll_once(
    sessionmaker: async_sessionmaker[AsyncSession],
    settings: Settings,
    client: httpx.AsyncClient,
) -> None:
    """One locked poll pass. Returns without a GitHub read when the lock is held."""

    engine = _engine(sessionmaker)
    async with engine.connect() as connection:
        locked = (
            await connection.execute(_LOCK, _LOCK_ARGS)
        ).scalar()
        await connection.commit()
        if not locked:
            return
        try:
            await _poll_locked(sessionmaker, settings, client)
        finally:
            await connection.execute(_UNLOCK, _LOCK_ARGS)
            await connection.commit()


async def _poll_locked(
    sessionmaker: async_sessionmaker[AsyncSession],
    settings: Settings,
    client: httpx.AsyncClient,
) -> None:
    async with sessionmaker() as session:
        repositories = await bound_repositories(session)
    for repository in repositories:
        try:
            repo = normalize_repo_full_name(repository)
        except InvalidRepoFullName:
            continue
        if not repository_is_allowed(repo, settings.github_repo_allowlist):
            continue
        try:
            await _poll_repository(sessionmaker, settings, client, repo)
        except (
            Unavailable,
            PollUnavailable,
            FeedbackUnavailable,
            GitHubAppError,
            GitHubInstallationRefused,
            ValueError,
        ):
            logger.info("factory poll for %s deferred", repo)


async def _poll_repository(
    sessionmaker: async_sessionmaker[AsyncSession],
    settings: Settings,
    client: httpx.AsyncClient,
    repo: str,
) -> None:
    cursor = await _load_cursor(sessionmaker, repo)
    now = datetime.now(UTC)
    installation_id, token = await run_in_threadpool(
        credentials_for(settings).fresh_installation_token, repo, None
    )
    api = settings.github_api_url.rstrip("/")
    repo_path = f"/repos/{repo_url_path(repo)}"
    repository = await read_repository(client, api=api, repo_full_name=repo, token=token)
    repository_id = repository.get("id")
    if type(repository_id) is not int or repository_id <= 0:
        raise Unavailable(repo_path)
    if not isinstance(repository.get("full_name"), str):
        raise Unavailable(repo_path)
    cursor.repository_id = repository_id
    tracker = GitHubTracker.from_settings(
        settings,
        client,
        repo_full_name=repo,
        repository_id=repository_id,
        token=static_token(token),
    )
    await _admit_labeled(
        sessionmaker,
        settings,
        client,
        cursor,
        tracker,
        installation_id=installation_id,
    )
    await _cancel_stale(
        sessionmaker,
        settings,
        client,
        cursor,
        tracker,
        installation_id=installation_id,
    )
    await _admit_mentions(
        sessionmaker,
        settings,
        client,
        cursor,
        tracker,
        now=now,
        api=api,
        token=token,
        repo_path=repo_path,
        installation_id=installation_id,
    )
    owned = await open_pulls(sessionmaker, repo, repository_id)
    await admit_review_comments(
        sessionmaker,
        settings,
        client,
        cursor,
        now=now,
        api=api,
        token=token,
        repo=repo,
        repo_path=repo_path,
        repository_id=repository_id,
        installation_id=installation_id,
        owned=owned,
    )
    await admit_reviews(
        sessionmaker,
        settings,
        client,
        cursor,
        api=api,
        token=token,
        repo=repo,
        repo_path=repo_path,
        repository_id=repository_id,
        installation_id=installation_id,
        owned=owned,
    )
    await _save_cursor(sessionmaker, repo, cursor)


async def _admit_labeled(
    sessionmaker: async_sessionmaker[AsyncSession],
    settings: Settings,
    client: httpx.AsyncClient,
    cursor: Cursor,
    tracker: GitHubTracker,
    *,
    installation_id: int,
) -> None:
    label = settings.github_factory_label
    repo, repository_id = tracker.repo_full_name, tracker.repository_id
    listed, etag = await tracker.labeled_open_issues(cursor.etags.get("issues"))
    if listed is None:
        return
    deferred = False
    for issue in listed:
        if not isinstance(issue, dict) or "pull_request" in issue:
            continue
        number = issue.get("number")
        if type(number) is not int or number <= 0:
            continue
        try:
            events = await tracker.issue_events(number)
            event = last_label_event(events, label)
            if event is None or type(event.get("id")) is not int:
                continue
            sender = human_actor(event)
            if sender is None:
                continue
            sender_id, sender_login = sender
            notice = FactoryNotice(
                uuid.uuid4(),
                "issues",
                "labeled",
                "admit",
                installation_id,
                repository_id,
                repo,
                number,
                sender_id,
                sender_login,
                label=label,
                label_event_id=event["id"],
            )
            if await _label_already_admitted(sessionmaker, repository_id, number, event):
                # Later base labels only report disagreement with the frozen base.
                # Reusing the admission here would cancel or replace the live run.
                notice = replace(notice, disposition="base_label")
            await _apply_notice(sessionmaker, settings, client, notice)
        except (Unavailable, FeedbackUnavailable, FeedbackIgnored):
            deferred = True
            logger.info("factory labeled issue poll for %s issue %s deferred", repo, number)
    if etag and not deferred:
        cursor.etags["issues"] = etag


async def _cancel_stale(
    sessionmaker: async_sessionmaker[AsyncSession],
    settings: Settings,
    client: httpx.AsyncClient,
    cursor: Cursor,
    tracker: GitHubTracker,
    *,
    installation_id: int,
) -> None:
    repo, repository_id = tracker.repo_full_name, tracker.repository_id
    async with sessionmaker() as session:
        items = list(
            await session.scalars(
                select(WorkItem).where(
                    WorkItem.github_repository_id == repository_id,
                    WorkItem.cancelled_at.is_(None),
                    WorkItem.github_issue_number.is_not(None),
                )
            )
        )
        numbers = sorted(
            {item.github_issue_number for item in items if item.github_issue_number}
        )
    label = settings.github_factory_label
    keys = {f"issue:{number}:{label}" for number in numbers}
    for key in list(cursor.etags):
        if key.startswith("issue:") and key not in keys:
            del cursor.etags[key]
    for number in numbers:
        key = f"issue:{number}:{label}"
        try:
            issue, etag = await tracker.conditional_issue(number, cursor.etags.get(key))
            if issue is None or "pull_request" in issue:
                continue
            names = label_names(issue)
            if names is None or issue.get("state") not in {"open", "closed"}:
                raise Unavailable(f"issue {number}")
            if issue.get("state") == "closed":
                action, kind = "closed", "closed"
            elif label not in names:
                action, kind = "unlabeled", "unlabeled"
            else:
                if etag:
                    cursor.etags[key] = etag
                continue
            # Retry cancellation authority on every pass until it succeeds.
            # A cached issue must not hide a later permission or event repair.
            cursor.etags.pop(key, None)
            events = await tracker.issue_events(number)
            event = last_event_of_kind(events, kind, label=label if kind == "unlabeled" else None)
            if event is None:
                continue
            sender = human_actor(event)
            if sender is None:
                continue
            sender_id, sender_login = sender
            notice = FactoryNotice(
                uuid.uuid4(),
                "issues",
                action,
                "cancel",
                installation_id,
                repository_id,
                repo,
                number,
                sender_id,
                sender_login,
                label=label,
            )
            await _apply_notice(sessionmaker, settings, client, notice)
        except (Unavailable, FeedbackUnavailable, FeedbackIgnored):
            logger.info("factory stale issue poll for %s issue %s deferred", repo, number)


async def _issue_numbers(
    sessionmaker: async_sessionmaker[AsyncSession], repository_id: int
) -> set[int]:
    async with sessionmaker() as session:
        rows = await session.scalars(
            select(WorkItem.github_issue_number).where(
                WorkItem.github_repository_id == repository_id,
                WorkItem.github_issue_number.is_not(None),
            )
        )
    return {number for number in rows if isinstance(number, int)}


async def _admit_mentions(
    sessionmaker: async_sessionmaker[AsyncSession],
    settings: Settings,
    client: httpx.AsyncClient,
    cursor: Cursor,
    tracker: GitHubTracker,
    *,
    now: datetime,
    api: str,
    token: str,
    repo_path: str,
    installation_id: int,
) -> None:
    repo, repository_id = tracker.repo_full_name, tracker.repository_id
    listed, etag = await tracker.issue_comments_since(
        since_param(cursor.comments_since, now), cursor.etags.get("issue-comments")
    )
    if listed is None:
        return
    factory_issues = await _issue_numbers(sessionmaker, repository_id)
    owned_pulls = set(await open_pulls(sessionmaker, repo, repository_id))
    for comment in listed:
        if not isinstance(comment, dict):
            continue
        if comment.get("performed_via_github_app") is not None:
            continue
        sender = human_actor({"user": comment.get("user")})
        if sender is None:
            continue
        body = comment.get("body")
        comment_id = comment.get("id")
        number = trailing_id(comment.get("issue_url"))
        if (
            not isinstance(body, str)
            or type(comment_id) is not int
            or number is None
            or not body.strip()
        ):
            continue
        if number in owned_pulls:
            pull = await read_pull(client, api=api, token=token, repo_path=repo_path, number=number)
            await admit_one_feedback(
                sessionmaker,
                settings,
                client,
                event="issue_comment",
                payload={
                    "action": "created",
                    "installation": {"id": installation_id},
                    "repository": repository_payload(repository_id, repo),
                    "sender": comment.get("user"),
                    "issue": {
                        "number": number,
                        "state": pull.get("state"),
                        "pull_request": {},
                    },
                    "comment": comment,
                },
            )
            continue
        if number not in factory_issues or not mentions_login(
            body, settings.github_factory_mention
        ):
            continue
        sender_id, sender_login = sender
        notice = FactoryNotice(
            uuid.uuid4(),
            "issue_comment",
            "created",
            "mention",
            installation_id,
            repository_id,
            repo,
            number,
            sender_id,
            sender_login,
            comment_id=comment_id,
            comment_body=body,
        )
        await _apply_notice(sessionmaker, settings, client, notice)
    cursor.comments_since = advance(cursor.comments_since, listed, "created_at", "updated_at")
    if etag:
        cursor.etags["issue-comments"] = etag


async def _apply_notice(
    sessionmaker: async_sessionmaker[AsyncSession],
    settings: Settings,
    client: httpx.AsyncClient,
    notice: FactoryNotice,
) -> None:
    async with sessionmaker() as session:
        try:
            await github_factory.lock_issue(session, notice.repository_id, notice.issue_number)
            verified = await github_factory.verify_current(
                notice, settings=settings, client=client
            )
            if notice.disposition == "cancel":
                await github_factory.cancel_notice(session, notice)
            elif notice.disposition == "base_label":
                await github_factory.record_base_label_notice(session, notice, verified, settings)
            else:
                await github_factory.admit_notice(session, notice, settings, verified)
        except FeedbackUnavailable:
            await session.rollback()
            raise
        except FeedbackIgnored:
            await session.rollback()
            return
        await session.commit()


async def _load_cursor(sessionmaker: async_sessionmaker[AsyncSession], repo: str) -> Cursor:
    async with sessionmaker() as session:
        row = await session.get(FactoryPollCursor, repo)
        if row is None:
            return Cursor()
        raw = row.etags if isinstance(row.etags, dict) else {}
        etags = {str(key): value for key, value in raw.items() if isinstance(value, str)}
        return Cursor(
            comments_since=row.comments_since,
            review_comments_since=row.review_comments_since,
            reviews_since=row.reviews_since,
            etags=etags,
            repository_id=row.repository_id,
        )


async def _save_cursor(
    sessionmaker: async_sessionmaker[AsyncSession], repo: str, cursor: Cursor
) -> None:
    async with sessionmaker() as session:
        row = await session.get(FactoryPollCursor, repo)
        if row is None:
            row = FactoryPollCursor(
                repo_full_name=repo,
                repository_id=cursor.repository_id,
                comments_since=cursor.comments_since,
                review_comments_since=cursor.review_comments_since,
                reviews_since=cursor.reviews_since,
                etags=dict(cursor.etags),
                updated_at=datetime.now(UTC),
            )
            session.add(row)
        else:
            row.repository_id = cursor.repository_id
            row.comments_since = cursor.comments_since
            row.review_comments_since = cursor.review_comments_since
            row.reviews_since = cursor.reviews_since
            row.etags = dict(cursor.etags)
            row.updated_at = datetime.now(UTC)
        await session.commit()
