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
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx
from sqlalchemy import desc, select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from starlette.concurrency import run_in_threadpool

from curie_api.factory_label_reconcile import bound_repositories, last_label_event, parse_time

from . import github_factory
from .config import Settings
from .github_app import GitHubAppError, GitHubInstallationRefused, credentials_for
from .github_factory_events import FactoryNotice, mentions_login
from .github_factory_review import admit_parsed_feedback
from .github_review_events import (
    FeedbackIgnored,
    FeedbackUnavailable,
    human_sender,
    parse_feedback,
)
from .github_review_truth import get_github_json, github_headers
from .models import ExecutionRequest, FactoryPollCursor, ThreadPublicationLineage, WorkItem
from .repo_full_name import InvalidRepoFullName, normalize_repo_full_name, repo_url_path
from .workspace_policy import repository_is_allowed

logger = logging.getLogger(__name__)

_PER_PAGE = 100
_MAX_PAGES = 50
_LOOKBACK = timedelta(hours=1)
_LOCK = text("SELECT pg_try_advisory_lock(CAST(:classid AS integer), CAST(:objid AS integer))")
_UNLOCK = text("SELECT pg_advisory_unlock(CAST(:classid AS integer), CAST(:objid AS integer))")
_LOCK_ARGS = {"classid": 3745, "objid": 187}


class _Unavailable(Exception):
    """GitHub could not answer; leave this repository's cursor where it is."""


@dataclass
class _Cursor:
    comments_since: datetime | None = None
    review_comments_since: datetime | None = None
    reviews_since: datetime | None = None
    etags: dict[str, str] = field(default_factory=dict)
    repository_id: int | None = None


def _engine(sessionmaker: async_sessionmaker[AsyncSession]) -> Any:
    bind = sessionmaker.kw.get("bind")
    if bind is not None:
        return bind
    raise RuntimeError("factory poll sessionmaker has no bind")


def _since_param(stored: datetime | None, now: datetime) -> str:
    moment = stored if stored is not None else now - _LOOKBACK
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=UTC)
    return moment.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _advance(current: datetime | None, items: list[Any], *keys: str) -> datetime | None:
    newest = current
    for item in items:
        if not isinstance(item, dict):
            continue
        raw = next((item.get(key) for key in keys if isinstance(item.get(key), str)), None)
        parsed = parse_time(raw)
        if parsed is not None and (newest is None or parsed > newest):
            newest = parsed
    return newest


def _trailing_id(url: Any) -> int | None:
    if not isinstance(url, str) or not url:
        return None
    tail = url.rstrip("/").rsplit("/", 1)[-1]
    if not tail.isdigit():
        return None
    number = int(tail)
    return number if number > 0 else None


def _human_actor(event: dict[str, Any]) -> tuple[int, str] | None:
    if event.get("performed_via_github_app") is not None:
        return None
    actor = event.get("actor") if "actor" in event else event.get("user")
    if not isinstance(actor, dict) or actor.get("type") == "Bot":
        return None
    try:
        return human_sender(actor)
    except FeedbackIgnored:
        return None


def _label_names(issue: dict[str, Any]) -> set[str] | None:
    labels = issue.get("labels")
    if not isinstance(labels, list):
        return None
    names: set[str] = set()
    for label in labels:
        if not isinstance(label, dict) or not isinstance(label.get("name"), str):
            return None
        names.add(label["name"])
    return names


def _last_kind(
    events: list[Any], kind: str, *, label: str | None = None
) -> dict[str, Any] | None:
    found: dict[str, Any] | None = None
    for event in events:
        if not isinstance(event, dict) or event.get("event") != kind:
            continue
        if label is not None:
            raw = event.get("label")
            if not isinstance(raw, dict) or raw.get("name") != label:
                continue
        found = event
    return found


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
            _Unavailable,
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
    repository = await get_github_json(
        client, api=api, token=token, path=repo_path, refusal="repository_unavailable"
    )
    repository_id = repository.get("id")
    if type(repository_id) is not int or repository_id <= 0:
        raise _Unavailable(repo_path)
    if not isinstance(repository.get("full_name"), str):
        raise _Unavailable(repo_path)
    cursor.repository_id = repository_id
    await _admit_labeled(
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
    )
    await _cancel_stale(
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
    )
    await _admit_mentions(
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
    )
    owned = await _open_pulls(sessionmaker, repo, repository_id)
    await _admit_review_comments(
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
    await _admit_reviews(
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


async def _list(
    client: httpx.AsyncClient,
    *,
    api: str,
    token: str,
    path: str,
    params: dict[str, Any],
    etag: str | None,
) -> tuple[list[Any] | None, str | None]:
    """Pages of one listing. None means the first page was not modified."""

    headers = github_headers(token)
    if etag:
        headers["If-None-Match"] = etag
    items: list[Any] = []
    seen_etag = etag
    for page in range(1, _MAX_PAGES + 1):
        try:
            response = await client.get(
                f"{api}{path}",
                params={**params, "per_page": _PER_PAGE, "page": page},
                headers=headers,
                follow_redirects=False,
            )
        except httpx.HTTPError:
            raise _Unavailable(path) from None
        if page == 1 and response.status_code == 304:
            return None, response.headers.get("etag") or etag
        if response.status_code != 200:
            raise _Unavailable(path)
        try:
            result = response.json()
        except ValueError:
            raise _Unavailable(path) from None
        if not isinstance(result, list):
            raise _Unavailable(path)
        if page == 1:
            seen_etag = response.headers.get("etag") or etag
        items.extend(result)
        if len(result) < _PER_PAGE:
            return items, seen_etag
        headers = github_headers(token)
    raise _Unavailable(path)


async def _events(
    client: httpx.AsyncClient, *, api: str, token: str, repo_path: str, number: int
) -> list[Any]:
    items, _etag = await _list(
        client,
        api=api,
        token=token,
        path=f"{repo_path}/issues/{number}/events",
        params={},
        etag=None,
    )
    return [] if items is None else items


async def _admit_labeled(
    sessionmaker: async_sessionmaker[AsyncSession],
    settings: Settings,
    client: httpx.AsyncClient,
    cursor: _Cursor,
    *,
    api: str,
    token: str,
    repo: str,
    repo_path: str,
    repository_id: int,
    installation_id: int,
) -> None:
    label = settings.github_factory_label
    listed, etag = await _list(
        client,
        api=api,
        token=token,
        path=f"{repo_path}/issues",
        params={"state": "open", "labels": label},
        etag=cursor.etags.get("issues"),
    )
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
            events = await _events(
                client, api=api, token=token, repo_path=repo_path, number=number
            )
            event = last_label_event(events, label)
            if event is None or type(event.get("id")) is not int:
                continue
            sender = _human_actor(event)
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
        except (_Unavailable, FeedbackUnavailable, FeedbackIgnored):
            deferred = True
            logger.info("factory labeled issue poll for %s issue %s deferred", repo, number)
    if etag and not deferred:
        cursor.etags["issues"] = etag


async def _cancel_stale(
    sessionmaker: async_sessionmaker[AsyncSession],
    settings: Settings,
    client: httpx.AsyncClient,
    cursor: _Cursor,
    *,
    api: str,
    token: str,
    repo: str,
    repo_path: str,
    repository_id: int,
    installation_id: int,
) -> None:
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
            issue, etag = await _conditional_issue(
                client,
                api=api,
                token=token,
                path=f"{repo_path}/issues/{number}",
                etag=cursor.etags.get(key),
            )
            if issue is None or "pull_request" in issue:
                continue
            names = _label_names(issue)
            if names is None or issue.get("state") not in {"open", "closed"}:
                raise _Unavailable(f"{repo_path}/issues/{number}")
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
            events = await _events(
                client, api=api, token=token, repo_path=repo_path, number=number
            )
            event = _last_kind(events, kind, label=label if kind == "unlabeled" else None)
            if event is None:
                continue
            sender = _human_actor(event)
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
        except (_Unavailable, FeedbackUnavailable, FeedbackIgnored):
            logger.info("factory stale issue poll for %s issue %s deferred", repo, number)


async def _conditional_issue(
    client: httpx.AsyncClient,
    *,
    api: str,
    token: str,
    path: str,
    etag: str | None,
) -> tuple[dict[str, Any] | None, str | None]:
    """Read current state, without charging unchanged issues to the rate budget.

    Authenticated conditional requests returning 304 do not count against the
    primary rate limit. A cached value is used only for an open labeled issue.
    https://docs.github.com/en/rest/using-the-rest-api/best-practices-for-using-the-rest-api#use-conditional-requests-if-appropriate
    """

    headers = github_headers(token)
    if etag:
        headers["If-None-Match"] = etag
    try:
        response = await client.get(
            f"{api}{path}", headers=headers, follow_redirects=False
        )
    except httpx.HTTPError:
        raise _Unavailable(path) from None
    if response.status_code == 304 and etag:
        return None, response.headers.get("etag") or etag
    if response.status_code != 200:
        raise _Unavailable(path)
    try:
        issue = response.json()
    except ValueError:
        raise _Unavailable(path) from None
    if not isinstance(issue, dict):
        raise _Unavailable(path)
    return issue, response.headers.get("etag")


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
    cursor: _Cursor,
    *,
    now: datetime,
    api: str,
    token: str,
    repo: str,
    repo_path: str,
    repository_id: int,
    installation_id: int,
) -> None:
    listed, etag = await _list(
        client,
        api=api,
        token=token,
        path=f"{repo_path}/issues/comments",
        params={
            "since": _since_param(cursor.comments_since, now),
            "sort": "created",
            "direction": "asc",
        },
        etag=cursor.etags.get("issue-comments"),
    )
    if listed is None:
        return
    factory_issues = await _issue_numbers(sessionmaker, repository_id)
    owned_pulls = set(
        await _open_pulls(sessionmaker, repo, repository_id)
    )
    for comment in listed:
        if not isinstance(comment, dict):
            continue
        if comment.get("performed_via_github_app") is not None:
            continue
        sender = _human_actor({"user": comment.get("user")})
        if sender is None:
            continue
        body = comment.get("body")
        comment_id = comment.get("id")
        number = _trailing_id(comment.get("issue_url"))
        if (
            not isinstance(body, str)
            or type(comment_id) is not int
            or number is None
            or not body.strip()
        ):
            continue
        if number in owned_pulls:
            pull = await _pull(
                client, api=api, token=token, repo_path=repo_path, number=number
            )
            await _admit_one_feedback(
                sessionmaker,
                settings,
                client,
                event="issue_comment",
                payload={
                    "action": "created",
                    "installation": {"id": installation_id},
                    "repository": _repository_payload(repository_id, repo),
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
    cursor.comments_since = _advance(cursor.comments_since, listed, "created_at", "updated_at")
    if etag:
        cursor.etags["issue-comments"] = etag


async def _open_pulls(
    sessionmaker: async_sessionmaker[AsyncSession], repo: str, repository_id: int
) -> list[int]:
    async with sessionmaker() as session:
        rows = await session.scalars(
            select(ThreadPublicationLineage.pr_number)
            .join(WorkItem, WorkItem.publication_lineage_id == ThreadPublicationLineage.id)
            .where(
                WorkItem.repo_full_name == repo,
                WorkItem.github_repository_id == repository_id,
                WorkItem.cancelled_at.is_(None),
                ThreadPublicationLineage.status == "open",
                ThreadPublicationLineage.pr_number.is_not(None),
            )
        )
    return [number for number in rows if isinstance(number, int) and number > 0]


async def _pull(
    client: httpx.AsyncClient, *, api: str, token: str, repo_path: str, number: int
) -> dict[str, Any]:
    return await get_github_json(
        client,
        api=api,
        token=token,
        path=f"{repo_path}/pulls/{number}",
        refusal="pull_request_unavailable",
    )


def _repository_payload(repository_id: int, repo: str) -> dict[str, Any]:
    return {"id": repository_id, "full_name": repo}


async def _admit_one_feedback(
    sessionmaker: async_sessionmaker[AsyncSession],
    settings: Settings,
    client: httpx.AsyncClient,
    *,
    event: str,
    payload: dict[str, Any],
) -> None:
    try:
        feedback = parse_feedback(
            event,
            payload,
            str(uuid.uuid4()),
            github_html_base=settings.github_html_base,
        )
    except FeedbackIgnored:
        return
    async with sessionmaker() as session:
        try:
            await admit_parsed_feedback(session, feedback, settings=settings, client=client)
        except FeedbackUnavailable:
            await session.rollback()
            raise
        except FeedbackIgnored:
            await session.rollback()
            return
        await session.commit()


async def _admit_review_comments(
    sessionmaker: async_sessionmaker[AsyncSession],
    settings: Settings,
    client: httpx.AsyncClient,
    cursor: _Cursor,
    *,
    now: datetime,
    api: str,
    token: str,
    repo: str,
    repo_path: str,
    repository_id: int,
    installation_id: int,
    owned: list[int],
) -> None:
    listed, etag = await _list(
        client,
        api=api,
        token=token,
        path=f"{repo_path}/pulls/comments",
        params={
            "since": _since_param(cursor.review_comments_since, now),
            "sort": "created",
            "direction": "asc",
        },
        etag=cursor.etags.get("review-comments"),
    )
    if listed is None:
        return
    owned_set = set(owned)
    pulls: dict[int, dict[str, Any]] = {}
    for comment in listed:
        if not isinstance(comment, dict) or comment.get("performed_via_github_app") is not None:
            continue
        if _human_actor({"user": comment.get("user")}) is None:
            continue
        number = _trailing_id(comment.get("pull_request_url"))
        if number is None or number not in owned_set:
            continue
        if number not in pulls:
            pulls[number] = await _pull(
                client, api=api, token=token, repo_path=repo_path, number=number
            )
        user = comment.get("user")
        await _admit_one_feedback(
            sessionmaker,
            settings,
            client,
            event="pull_request_review_comment",
            payload={
                "action": "created",
                "installation": {"id": installation_id},
                "repository": _repository_payload(repository_id, repo),
                "sender": user,
                "pull_request": pulls[number],
                "comment": comment,
            },
        )
    cursor.review_comments_since = _advance(
        cursor.review_comments_since, listed, "created_at", "updated_at"
    )
    if etag:
        cursor.etags["review-comments"] = etag


async def _admit_reviews(
    sessionmaker: async_sessionmaker[AsyncSession],
    settings: Settings,
    client: httpx.AsyncClient,
    cursor: _Cursor,
    *,
    api: str,
    token: str,
    repo: str,
    repo_path: str,
    repository_id: int,
    installation_id: int,
    owned: list[int],
) -> None:
    keys = {f"reviews:{number}" for number in owned}
    for key in list(cursor.etags):
        if key.startswith("reviews:") and key not in keys:
            del cursor.etags[key]
    for number in owned:
        key = f"reviews:{number}"
        listed, etag = await _list(
            client,
            api=api,
            token=token,
            path=f"{repo_path}/pulls/{number}/reviews",
            params={},
            etag=cursor.etags.get(key),
        )
        if listed is None:
            continue
        pull = await _pull(client, api=api, token=token, repo_path=repo_path, number=number)
        for review in listed:
            if not isinstance(review, dict) or review.get("performed_via_github_app") is not None:
                continue
            if _human_actor({"user": review.get("user")}) is None:
                continue
            await _admit_one_feedback(
                sessionmaker,
                settings,
                client,
                event="pull_request_review",
                payload={
                    "action": "submitted",
                    "installation": {"id": installation_id},
                    "repository": _repository_payload(repository_id, repo),
                    "sender": review.get("user"),
                    "pull_request": pull,
                    "review": review,
                },
            )
        if etag:
            cursor.etags[key] = etag


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
                await github_factory.admit_notice(session, notice, settings, verified, client)
        except FeedbackUnavailable:
            await session.rollback()
            raise
        except FeedbackIgnored:
            await session.rollback()
            return
        await session.commit()


async def _load_cursor(
    sessionmaker: async_sessionmaker[AsyncSession], repo: str
) -> _Cursor:
    async with sessionmaker() as session:
        row = await session.get(FactoryPollCursor, repo)
        if row is None:
            return _Cursor()
        raw = row.etags if isinstance(row.etags, dict) else {}
        etags = {str(key): value for key, value in raw.items() if isinstance(value, str)}
        return _Cursor(
            comments_since=row.comments_since,
            review_comments_since=row.review_comments_since,
            reviews_since=row.reviews_since,
            etags=etags,
            repository_id=row.repository_id,
        )


async def _save_cursor(
    sessionmaker: async_sessionmaker[AsyncSession], repo: str, cursor: _Cursor
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
