"""Poll GitHub for factory intake (#3745).

Polling is the default door. One pass lists labeled issues, mentions, and
review feedback for each bound repository, then admits through the same
verification the webhook uses. It does not write a delivery receipt.

A repository may serve several trigger labels, one per surface (#4361 decision
14). The pass still makes one repository read and mints one installation token
per repository, then lists open issues once per effective label under the ETag
key ``issues:<label>``. A repository with open WorkItems and no surface left is
walked for cancellation only: nothing serves it, so nothing is listed or
admitted there.

Cursors live in ``curie.factory_poll_cursors``, keyed by tracker scope. ``since`` moves only to the
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
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from typing import Any

import httpx
from sqlalchemy import desc, select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from sqlalchemy.sql.elements import ColumnElement
from starlette.concurrency import run_in_threadpool

from curie_api.factory_label_reconcile import (
    bound_repositories,
    open_work_item_repositories,
    parse_time,
)

from . import github_factory
from .config import Settings
from .forges.errors import Unavailable
from .forges.github.binding import bound_labels
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
from .forges.hosts import factory_credentials, factory_tracker, repository_ref
from .forges.registry import forges_registry
from .forges.types import TrackerIssueRef
from .github_app import GitHubAppError, GitHubInstallationRefused
from .github_factory_events import FactoryNotice, mentions_login
from .github_review_events import FeedbackIgnored, FeedbackUnavailable
from .github_review_truth import VerifiedRepository
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
    issue: TrackerIssueRef,
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
        item = await github_factory.work_item_for(session, issue)
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
        bound = await bound_repositories(session)
        open_items = await open_work_item_repositories(session)
    served: dict[str, bool] = {}
    for repository, serves in [*((r, True) for r in bound), *((r, False) for r in open_items)]:
        try:
            repo = normalize_repo_full_name(repository)
        except InvalidRepoFullName:
            continue
        served[repo] = served.get(repo, False) or serves
    for repo, serves in served.items():
        if not repository_is_allowed(repo, settings.github_repo_allowlist):
            continue
        try:
            await _poll_repository(sessionmaker, settings, client, repo, serves=serves)
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
    *,
    serves: bool,
) -> None:
    """One repository's pass. ``serves`` is False for a repository no surface
    binds any more: it gets the cancellation pass alone."""

    now = datetime.now(UTC)
    registry = forges_registry(settings)
    intake = factory_tracker(registry)
    async with sessionmaker() as session:
        labels = sorted(await bound_labels(session, registry, repo)) if serves else []
    installation_id, token = await run_in_threadpool(
        factory_credentials(registry).fresh_installation_token, repo, None
    )
    api = intake.config.api_url.rstrip("/")
    repo_path = f"/repos/{repo_url_path(repo)}"
    repository = await read_repository(client, api=api, repo_full_name=repo, token=token)
    repository_id = repository.get("id")
    if type(repository_id) is not int or repository_id <= 0:
        raise Unavailable(repo_path)
    if not isinstance(repository.get("full_name"), str):
        raise Unavailable(repo_path)
    # Review feedback reuses this pass's installation check and repository
    # read; each piece still reads its pull request and author permission.
    verified = VerifiedRepository(api, token, installation_id, repository)
    tracker = intake.for_repository(
        client,
        repo_full_name=repo,
        repository_id=repository_id,
        token=static_token(token),
    )
    # The cursor belongs to the tracker scope, the repository's immutable id,
    # so a rename keeps it (ADR 0197).
    scope = _Scope(tracker.kind, tracker.host, str(repository_id), repo)
    cursor = await _load_cursor(sessionmaker, scope)
    # The single-label key a release before #4361 kept, and the keys of labels
    # no surface binds now, are dropped; the next listing is unconditional.
    for key in list(cursor.etags):
        if key == "issues" or (key.startswith("issues:") and key[7:] not in labels):
            del cursor.etags[key]
    for label in labels:
        await _admit_labeled(
            sessionmaker,
            settings,
            client,
            cursor,
            intake.for_repository(
                client,
                repo_full_name=repo,
                repository_id=repository_id,
                token=static_token(token),
                label=label,
            ),
            installation_id=installation_id,
            repository=repository,
        )
    await _cancel_stale(
        sessionmaker,
        settings,
        client,
        cursor,
        tracker,
        installation_id=installation_id,
        repository=repository,
    )
    if not serves:
        await _save_cursor(sessionmaker, scope, cursor)
        return
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
        repository=repository,
        verified=verified,
    )
    owned = await open_pulls(
        sessionmaker, repository_ref(settings, path=repo, project_id=repository_id)
    )
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
        verified=verified,
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
        verified=verified,
    )
    await _save_cursor(sessionmaker, scope, cursor)


async def _admit_labeled(
    sessionmaker: async_sessionmaker[AsyncSession],
    settings: Settings,
    client: httpx.AsyncClient,
    cursor: Cursor,
    tracker: GitHubTracker,
    *,
    installation_id: int,
    repository: dict[str, Any],
) -> None:
    label = tracker.label
    key = f"issues:{label}"
    repo, repository_id = tracker.repo_full_name, tracker.repository_id
    listed, etag = await tracker.labeled_open_issues(cursor.etags.get(key))
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
            if await _label_already_admitted(sessionmaker, tracker.issue(number), event):
                # Later base labels only report disagreement with the frozen base.
                # Reusing the admission here would cancel or replace the live run.
                notice = replace(notice, disposition="base_label")
            await _apply_notice(sessionmaker, settings, client, notice, tracker, repository)
        except (Unavailable, FeedbackUnavailable, FeedbackIgnored):
            deferred = True
            logger.info("factory labeled issue poll for %s issue %s deferred", repo, number)
    if etag and not deferred:
        cursor.etags[key] = etag


async def _cancel_stale(
    sessionmaker: async_sessionmaker[AsyncSession],
    settings: Settings,
    client: httpx.AsyncClient,
    cursor: Cursor,
    tracker: GitHubTracker,
    *,
    installation_id: int,
    repository: dict[str, Any],
) -> None:
    repo, repository_id = tracker.repo_full_name, tracker.repository_id
    registry = forges_registry(settings)
    async with sessionmaker() as session:
        items = list(
            await session.scalars(
                select(WorkItem).where(
                    *_in_scope(tracker),
                    WorkItem.cancelled_at.is_(None),
                )
            )
        )
        # Each WorkItem against the label that admitted it (#4361 decision 10).
        admitting: dict[int, str] = {}
        for item in items:
            label = github_factory.admitting_label(registry, item)
            if label is not None:
                admitting[int(item.tracker_issue_id)] = label
    keys = {f"issue:{number}:{label}" for number, label in admitting.items()}
    for key in list(cursor.etags):
        if key.startswith("issue:") and key not in keys:
            del cursor.etags[key]
    for number, label in sorted(admitting.items()):
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
            await _apply_notice(sessionmaker, settings, client, notice, tracker, repository)
        except (Unavailable, FeedbackUnavailable, FeedbackIgnored):
            logger.info("factory stale issue poll for %s issue %s deferred", repo, number)


def _in_scope(tracker: GitHubTracker) -> tuple[ColumnElement[bool], ...]:
    """The WorkItems keyed under this tracker's scope (its repository id)."""

    return (
        WorkItem.tracker_kind == tracker.kind,
        WorkItem.tracker_host == tracker.host,
        WorkItem.tracker_scope_id == str(tracker.repository_id),
    )


async def _issue_numbers(
    sessionmaker: async_sessionmaker[AsyncSession], tracker: GitHubTracker
) -> set[int]:
    async with sessionmaker() as session:
        rows = await session.scalars(
            select(WorkItem.tracker_issue_id).where(*_in_scope(tracker))
        )
    return {int(number) for number in rows}


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
    repository: dict[str, Any],
    verified: VerifiedRepository,
) -> None:
    repo, repository_id = tracker.repo_full_name, tracker.repository_id
    listed, etag = await tracker.issue_comments_since(
        since_param(cursor.comments_since, now), cursor.etags.get("issue-comments")
    )
    if listed is None:
        return
    factory_issues = await _issue_numbers(sessionmaker, tracker)
    owned_pulls = set(
        await open_pulls(
            sessionmaker, repository_ref(settings, path=repo, project_id=repository_id)
        )
    )
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
                verified=verified,
            )
            continue
        if number not in factory_issues or not mentions_login(body, tracker.mention):
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
        await _apply_notice(sessionmaker, settings, client, notice, tracker, repository)
    cursor.comments_since = advance(cursor.comments_since, listed, "created_at", "updated_at")
    if etag:
        cursor.etags["issue-comments"] = etag


async def _apply_notice(
    sessionmaker: async_sessionmaker[AsyncSession],
    settings: Settings,
    client: httpx.AsyncClient,
    notice: FactoryNotice,
    tracker: GitHubTracker,
    repository: dict[str, Any],
) -> None:
    """Apply one polled notice, confirmed with the pass's token and against
    its one repository read (#4361 decision 14)."""

    async with sessionmaker() as session:
        try:
            await github_factory.lock_issue(
                session, github_factory.notice_issue(notice, forges_registry(settings))
            )
            verified = await github_factory.verify_polled(
                notice,
                settings=settings,
                client=client,
                tracker=tracker,
                repository=repository,
            )
            if notice.disposition == "cancel":
                await github_factory.cancel_notice(session, notice, settings)
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


@dataclass(frozen=True)
class _Scope:
    """A cursor's key (kind, host, scope id) and the path it is displayed by."""

    kind: str
    host: str
    scope_id: str
    path: str

    @property
    def key(self) -> tuple[str, str, str]:
        return (self.kind, self.host, self.scope_id)


async def _load_cursor(sessionmaker: async_sessionmaker[AsyncSession], scope: _Scope) -> Cursor:
    async with sessionmaker() as session:
        row = await session.get(FactoryPollCursor, scope.key)
        if row is None:
            return Cursor()
        raw = row.etags if isinstance(row.etags, dict) else {}
        etags = {str(key): value for key, value in raw.items() if isinstance(value, str)}
        return Cursor(
            comments_since=row.comments_since,
            review_comments_since=row.review_comments_since,
            reviews_since=row.reviews_since,
            etags=etags,
        )


async def _save_cursor(
    sessionmaker: async_sessionmaker[AsyncSession], scope: _Scope, cursor: Cursor
) -> None:
    async with sessionmaker() as session:
        row = await session.get(FactoryPollCursor, scope.key)
        if row is None:
            row = FactoryPollCursor(
                tracker_kind=scope.kind,
                tracker_host=scope.host,
                tracker_scope_id=scope.scope_id,
                scope_path=scope.path,
                comments_since=cursor.comments_since,
                review_comments_since=cursor.review_comments_since,
                reviews_since=cursor.reviews_since,
                etags=dict(cursor.etags),
                updated_at=datetime.now(UTC),
            )
            session.add(row)
        else:
            row.scope_path = scope.path
            row.comments_since = cursor.comments_since
            row.review_comments_since = cursor.review_comments_since
            row.reviews_since = cursor.reviews_since
            row.etags = dict(cursor.etags)
            row.updated_at = datetime.now(UTC)
        await session.commit()
