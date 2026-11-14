"""Poll GitHub for factory intake (#3745).

Polling is the default door. One pass lists labeled issues, mentions, and
review feedback for each bound repository, then admits through the same
verification the webhook uses. It does not write a delivery receipt.

A repository may serve several trigger labels, one per surface (#4361 decision
14). The pass still makes one repository read and mints one installation token
per repository, then lists open issues once per effective label under the ETag
key ``issues:<label>``. A repository with open WorkItems and no surface left is
walked for cancellation. If it also hosts another tracker's open pull requests
it is walked for review feedback too (conversation comments, review comments
and reviews on owned pull requests), but nothing serves it, so no issue intake
is listed or admitted there (#4910).

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

import asyncio
import logging
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime
from typing import Any

import httpx
from curie_telemetry import record_metric
from sqlalchemy import desc, select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from sqlalchemy.sql.elements import ColumnElement
from starlette.concurrency import run_in_threadpool

from curie_api.factory_label_reconcile import (
    bound_repositories,
    open_work_item_repositories,
    parse_time,
    tracker_work_item_repositories,
)

from . import factory_intake_notices, factory_notices, factory_rediscovery, github_factory
from .code_host_trust import code_host_verify
from .config import Settings
from .factory_facts import FACTS_DEADLINE_SECONDS
from .factory_facts_exchange import FactsExchange
from .factory_facts_schema import REQUEST_MARKER
from .forges.errors import NotFound, Unavailable
from .forges.github.binding import GITHUB_CHANNEL_KIND
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
from .forges.surfaces import bound_labels
from .forges.types import TrackerIssueRef
from .github_app import GitHubAppError, GitHubInstallationRefused
from .github_factory_events import FactoryNotice, mentions_login
from .github_review_events import FeedbackIgnored, FeedbackUnavailable
from .github_review_truth import VerifiedRepository
from .models import (
    ExecutionRequest,
    FactoryIntakeNotice,
    FactoryPollCursor,
    FactoryStatusComment,
    WorkItem,
)
from .repo_full_name import InvalidRepoFullName, normalize_repo_full_name, repo_url_path
from .workspace_policy import repository_is_allowed

logger = logging.getLogger(__name__)

_LOCK = text("SELECT pg_try_advisory_lock(CAST(:classid AS integer), CAST(:objid AS integer))")
_UNLOCK = text("SELECT pg_advisory_unlock(CAST(:classid AS integer), CAST(:objid AS integer))")
_LOCK_ARGS = {"classid": 3745, "objid": 187}


REPOSITORY_CONCURRENCY = 10
ISSUE_CONCURRENCY = 10
LABEL_PASS_AGE = "curie.factory.intake.label_pass.age_seconds"


@dataclass
class IntakeClock:
    time: Callable[[], float]
    wait: Callable[[float], Awaitable[None]]

    @classmethod
    def production(cls) -> IntakeClock:
        return cls(asyncio.get_running_loop().time, asyncio.sleep)


@dataclass
class Cadence:
    clock: IntakeClock
    interval: float
    due: float = 0.0
    next_repository: str | None = None
    last_clean: dict[str, float] = field(default_factory=dict)


async def run_forever(
    sessionmaker: async_sessionmaker[AsyncSession],
    settings: Settings,
    *,
    clock: IntakeClock | None = None,
) -> None:
    tracker = forges_registry(settings).github_tracker()
    if not settings.factory_enabled or settings.factory_intake != "poll" or tracker is None:
        return
    interval = tracker.config.poll_interval_s
    if interval <= 0:
        return
    cadence = Cadence(clock or IntakeClock.production(), interval)
    async with httpx.AsyncClient(
        timeout=settings.github_app_timeout_seconds, verify=code_host_verify(settings)
    ) as client:
        while True:
            cadence.due = cadence.clock.time() + interval
            try:
                await poll_once(sessionmaker, settings, client, cadence=cadence)
            except Exception:
                logger.exception("factory intake pass failed")
            await cadence.clock.wait(max(0.0, cadence.due - cadence.clock.time()))


async def _until_due(work: Awaitable[None], cadence: Cadence) -> None:
    task = asyncio.ensure_future(work)
    timer = asyncio.ensure_future(cadence.clock.wait(max(0.0, cadence.due - cadence.clock.time())))
    try:
        done, _ = await asyncio.wait((task, timer), return_when=asyncio.FIRST_COMPLETED)
        if task in done:
            await task
    finally:
        for pending in (task, timer):
            pending.cancel()
        await asyncio.gather(task, timer, return_exceptions=True)


def _engine(sessionmaker: async_sessionmaker[AsyncSession]) -> Any:
    bind = sessionmaker.kw.get("bind")
    if bind is not None:
        return bind
    raise RuntimeError("factory poll sessionmaker has no bind")


async def _label_already_admitted(
    sessionmaker: async_sessionmaker[AsyncSession],
    issue: TrackerIssueRef,
    event: dict[str, Any],
    installation_id: int,
    *,
    label: str,
    labels: set[str] | None,
    repository: dict[str, Any],
    agent_id: uuid.UUID | None,
    settings: Settings,
) -> tuple[bool, bool]:
    """Return prior admission and whether its unchanged selection is answered.

    Requests admitted through a webhook use the delivery id, not the timeline
    event id. Treating that same label as a new admission would cancel the
    live run. A later label event still readmits.
    """

    event_at = parse_time(event.get("created_at"))
    if event_at is None:
        return False, False
    async with sessionmaker() as session:
        item = await github_factory.work_item_for(session, issue)
        if (
            item is None
            or item.cancelled_at is not None
            or item.code_host_installation_id != installation_id
        ):
            return False, False
        latest = (
            await session.execute(
                select(
                    ExecutionRequest.id,
                    ExecutionRequest.created_at,
                    FactoryStatusComment.posted_at,
                )
                .outerjoin(
                    FactoryStatusComment,
                    FactoryStatusComment.execution_request_id == ExecutionRequest.id,
                )
                .where(ExecutionRequest.work_item_id == item.id)
                .order_by(desc(ExecutionRequest.sequence))
                .limit(1)
            )
        ).one_or_none()
        if latest is None or event_at > latest.created_at:
            return False, False
        if (
            latest.posted_at is None
            or agent_id != item.agent_id
            or item.trigger_label != label
            or labels is None
            or label not in labels
            or item.repository_path != repository.get("full_name")
            or item.repository_project_id != str(repository.get("id"))
            or item.base_resolved_request_id not in (None, latest.id)
        ):
            return True, False
        from .factory_base_reconcile import base_selection_unchanged

        return True, base_selection_unchanged(
            item,
            labels=labels,
            default_branch=repository.get("default_branch"),
            settings=settings,
        )


async def poll_once(
    sessionmaker: async_sessionmaker[AsyncSession],
    settings: Settings,
    client: httpx.AsyncClient,
    *,
    cadence: Cadence | None = None,
) -> None:
    """One locked poll pass. Returns without a GitHub read when the lock is held."""

    engine = _engine(sessionmaker)
    async with engine.connect() as connection:
        locked = (await connection.execute(_LOCK, _LOCK_ARGS)).scalar()
        await connection.commit()
        if not locked:
            return
        try:
            await _poll_locked(sessionmaker, settings, client, cadence=cadence)
        finally:
            await connection.execute(_UNLOCK, _LOCK_ARGS)
            await connection.commit()


async def _poll_locked(
    sessionmaker: async_sessionmaker[AsyncSession],
    settings: Settings,
    client: httpx.AsyncClient,
    *,
    cadence: Cadence | None,
) -> None:
    async with sessionmaker() as session:
        bound = await bound_repositories(session)
        open_items = await open_work_item_repositories(session)
        tracker_items = await tracker_work_item_repositories(session)
    served: dict[str, bool] = {}
    for repository, serves in [*((r, True) for r in bound), *((r, False) for r in open_items)]:
        try:
            repo = normalize_repo_full_name(repository)
        except InvalidRepoFullName:
            continue
        served[repo] = served.get(repo, False) or serves
    # Repositories that host another tracker's open pull requests are walked
    # for review feedback (#4358 plan D10); their intake is that tracker's pass.
    feedback_only: set[str] = set()
    for repository in tracker_items:
        try:
            repo = normalize_repo_full_name(repository)
        except InvalidRepoFullName:
            continue
        feedback_only.add(repo)
        served.setdefault(repo, False)
    clock = cadence.clock if cadence else IntakeClock.production()
    last_clean = cadence.last_clean if cadence else {}
    limit = asyncio.Semaphore(REPOSITORY_CONCURRENCY)
    traffic: dict[str, Callable[[], Awaitable[None]]] = {}

    async def labels(repo: str, serves: bool) -> None:
        if not repository_is_allowed(repo, settings.github_repo_allowlist):
            return
        last_clean.setdefault(repo, clock.time())
        clean = False
        async with limit:
            try:
                traffic[repo], clean = await _poll_repository(
                    sessionmaker,
                    settings,
                    client,
                    repo,
                    serves=serves,
                    feedback=serves or repo in feedback_only,
                )
            except (
                Unavailable,
                PollUnavailable,
                FeedbackUnavailable,
                GitHubAppError,
                GitHubInstallationRefused,
                ValueError,
            ):
                logger.info("factory poll for %s deferred", repo)
            finally:
                record_metric(
                    LABEL_PASS_AGE,
                    max(0.0, clock.time() - last_clean[repo]),
                    attributes={"repository": repo},
                )
                if clean:
                    last_clean[repo] = clock.time()

    async with asyncio.TaskGroup() as tasks:
        for repo, serves in served.items():
            tasks.create_task(labels(repo, serves))
    order = [repo for repo in served if repo in traffic]
    if cadence and cadence.next_repository in order:
        start = order.index(cadence.next_repository)
        order = order[start:] + order[:start]
    for index, repo in enumerate(order):
        if cadence:
            cadence.next_repository = order[(index + 1) % len(order)]
            if clock.time() >= cadence.due:
                break
        try:
            if cadence:
                await _until_due(traffic[repo](), cadence)
            else:
                await traffic[repo]()
        except (
            Unavailable,
            PollUnavailable,
            FeedbackUnavailable,
            GitHubAppError,
            GitHubInstallationRefused,
            ValueError,
        ):
            logger.info("factory feedback for %s deferred", repo)
        if cadence:
            if clock.time() >= cadence.due:
                break
            cadence.next_repository = order[(index + 1) % len(order)]


async def _poll_repository(
    sessionmaker: async_sessionmaker[AsyncSession],
    settings: Settings,
    client: httpx.AsyncClient,
    repo: str,
    *,
    serves: bool,
    feedback: bool,
) -> tuple[Callable[[], Awaitable[None]], bool]:
    """One repository's pass. ``serves`` is False for a repository no surface
    binds any more: it gets the cancellation pass alone, plus the review
    feedback pass when ``feedback`` (it hosts another tracker's open pull
    requests, #4358 plan D10). Feedback includes pull request conversation
    comments; issue mentions are admitted only when ``serves`` (#4910)."""

    now = datetime.now(UTC)
    registry = forges_registry(settings)
    intake = factory_tracker(registry)
    async with sessionmaker() as session:
        labels = (
            sorted(await bound_labels(session, registry, GITHUB_CHANNEL_KIND, repo))
            if serves
            else []
        )
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
    publications = factory_rediscovery.PassPublications()
    issues = asyncio.Semaphore(ISSUE_CONCURRENCY)
    clean = True
    for label in labels:
        clean = (
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
                publications=publications,
                limit=issues,
            )
            and clean
        )
    await _save_cursor(sessionmaker, scope, cursor, labels=True)

    async def traffic() -> None:
        try:
            await _poll_traffic(
                sessionmaker,
                settings,
                client,
                cursor,
                tracker,
                now=now,
                api=api,
                token=token,
                repo=repo,
                repo_path=repo_path,
                repository_id=repository_id,
                installation_id=installation_id,
                repository=repository,
                verified=verified,
                serves=serves,
                feedback=feedback,
            )
        finally:
            await _save_cursor(sessionmaker, scope, cursor, labels=False)

    return traffic, clean


async def _poll_traffic(
    sessionmaker: async_sessionmaker[AsyncSession],
    settings: Settings,
    client: httpx.AsyncClient,
    cursor: Cursor,
    tracker: GitHubTracker,
    *,
    now: datetime,
    api: str,
    token: str,
    repo: str,
    repo_path: str,
    repository_id: int,
    installation_id: int,
    repository: dict[str, Any],
    verified: VerifiedRepository,
    serves: bool,
    feedback: bool,
) -> None:
    await _cancel_stale(
        sessionmaker,
        settings,
        client,
        cursor,
        tracker,
        installation_id=installation_id,
        repository=repository,
    )
    if not feedback:
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
        serves=serves,
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


async def _admit_labeled(
    sessionmaker: async_sessionmaker[AsyncSession],
    settings: Settings,
    client: httpx.AsyncClient,
    cursor: Cursor,
    tracker: GitHubTracker,
    *,
    installation_id: int,
    repository: dict[str, Any],
    publications: factory_rediscovery.PassPublications,
    limit: asyncio.Semaphore,
) -> bool:
    label = tracker.label
    key = f"issues:{label}"
    repo, repository_id = tracker.repo_full_name, tracker.repository_id
    listed, etag = await tracker.labeled_open_issues(cursor.etags.get(key))
    if listed is None:
        return True
    async with sessionmaker() as session:
        bindings = await bound_labels(session, forges_registry(settings), GITHUB_CHANNEL_KIND, repo)
        matches = bindings.get(label, [])
        agent_id = matches[0].agent_id if len(matches) == 1 else None

    async def admit(issue: Any) -> bool:
        if not isinstance(issue, dict) or "pull_request" in issue:
            return True
        number = issue.get("number")
        if type(number) is not int or number <= 0:
            return True
        async with limit:
            return await apply(number, label_names(issue))

    async def apply(number: int, labels: set[str] | None) -> bool:
        try:
            events = await tracker.issue_events(number)
            event = last_label_event(events, label)
            if event is None:
                raise FeedbackUnavailable("label_events_unavailable")
            notice = FactoryNotice(
                uuid.uuid4(),
                "issues",
                "labeled",
                "admit",
                installation_id,
                repository_id,
                repo,
                number,
                0,
                "",
                label_event_at=parse_time(event.get("created_at")),
                label=label,
            )
            notice = factory_intake_notices.observe_label(notice, event, app_id=tracker.app_id)
            assert notice.label_event_id is not None
            if notice.actor_refusal is not None:
                logger.info(
                    "factory label actor refused %s#%d: %s", repo, number, notice.actor_refusal
                )
            async with sessionmaker() as session:
                answered = await session.scalar(
                    select(FactoryIntakeNotice.posted_at).where(
                        *factory_intake_notices.event_key(
                            tracker.issue(number), notice.label_event_id
                        )
                    )
                )
            if answered is not None:
                return True
            admitted, unchanged = await _label_already_admitted(
                sessionmaker,
                tracker.issue(number),
                event,
                installation_id,
                label=label,
                labels=labels,
                repository=repository,
                agent_id=agent_id,
                settings=settings,
            )
            if unchanged and notice.actor_refusal is None:
                return True
            if admitted:
                # Reconcile an observed base edit on the existing work item.
                notice = replace(notice, previously_admitted=True)
            await _apply_notice(
                sessionmaker,
                settings,
                client,
                notice,
                tracker,
                repository,
                publications=publications,
            )
        except (Unavailable, FeedbackUnavailable, FeedbackIgnored):
            logger.info("factory labeled issue poll for %s issue %s deferred", repo, number)
            return False
        return True

    async with asyncio.TaskGroup() as group:
        tasks = [group.create_task(admit(issue)) for issue in listed]
    clean = all(task.result() for task in tasks)
    if etag and clean:
        cursor.etags[key] = etag

    return clean


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
        kickoff = await github_factory.kickoff_work_items(session, [item.id for item in items])
        # Each WorkItem against the label that admitted it (#4361 decision 10).
        # A Slack kickoff's WorkItem has none (#5380): it is watched for its
        # issue closing alone, so no label removal, earlier or later, cancels it.
        watched: dict[int, str | None] = {}
        for item in items:
            label = github_factory.admitting_label(registry, item, kickoff_items=kickoff)
            if label is not None or item.id in kickoff:
                watched[int(item.tracker_issue_id)] = label
    keys = {_etag_key(number, label) for number, label in watched.items()}
    for key in list(cursor.etags):
        if key.startswith("issue:") and key not in keys:
            del cursor.etags[key]
    for number in sorted(watched):
        label = watched[number]
        key = _etag_key(number, label)
        try:
            issue, etag = await tracker.conditional_issue(number, cursor.etags.get(key))
            if issue is None or "pull_request" in issue:
                continue
            names = label_names(issue)
            if names is None or issue.get("state") not in {"open", "closed"}:
                raise Unavailable(f"issue {number}")
            if issue.get("state") == "closed":
                action, kind = "closed", "closed"
            elif label is not None and label not in names:
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
                label_event_at=None,
                label=label,
            )
            await _apply_notice(sessionmaker, settings, client, notice, tracker, repository)
        except (Unavailable, FeedbackUnavailable, FeedbackIgnored):
            logger.info("factory stale issue poll for %s issue %s deferred", repo, number)


def _etag_key(number: int, label: str | None) -> str:
    """The ETag key of one watched issue; a kickoff's carries no label (#5380)."""

    return f"issue:{number}" if label is None else f"issue:{number}:{label}"


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
        rows = await session.scalars(select(WorkItem.tracker_issue_id).where(*_in_scope(tracker)))
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
    serves: bool,
) -> None:
    """Walk issue comments since the cursor. A comment on an owned open pull
    request is review feedback, read wherever ``feedback`` is (#4910); a
    mention on a factory issue is intake, read only where a surface serves the
    repository."""

    repo, repository_id = tracker.repo_full_name, tracker.repository_id
    facts = FactsExchange(sessionmaker, settings, tracker.facts)
    if serves:
        await facts.recover_owed()
    retried: set[int] = set()
    for key in list(cursor.etags):
        if not key.startswith("facts-retry:"):
            continue
        retry_id = trailing_id(key.replace(":", "/"))
        if retry_id is None:
            cursor.etags.pop(key, None)
            continue
        retried.add(retry_id)
        try:
            async with asyncio.timeout(FACTS_DEADLINE_SECONDS):
                comment = await tracker.read_comment(retry_id)
                if not await facts.observe(comment, serves=serves):
                    retried.discard(retry_id)
        except NotFound:
            cursor.etags.pop(key, None)
        except Exception:  # noqa: BLE001 - retry one facts comment without replaying feedback
            logger.info("factory facts observation deferred reason=unreadable")
        else:
            cursor.etags.pop(key, None)
    listed, etag = await tracker.issue_comments_since(
        since_param(cursor.comments_since, now), cursor.etags.get("issue-comments")
    )
    if listed is None:
        await facts.deliver_owed()
        return
    factory_issues = await _issue_numbers(sessionmaker, tracker) if serves else set()
    owned_pulls = set(
        await open_pulls(
            sessionmaker, repository_ref(settings, path=repo, project_id=repository_id)
        )
    )
    for comment in listed:
        if not isinstance(comment, dict):
            continue
        body = comment.get("body")
        if comment.get("id") in retried:
            continue
        if (
            isinstance(body, str)
            and REQUEST_MARKER in body
            and trailing_id(comment.get("issue_url")) not in owned_pulls
        ):
            try:
                if await facts.observe(tracker.facts_comment(comment), serves=serves):
                    continue
            except (Unavailable, FeedbackUnavailable):
                comment_id = comment.get("id")
                if type(comment_id) is int and comment_id > 0:
                    cursor.etags[f"facts-retry:{comment_id}"] = "pending"
                logger.info("factory facts observation deferred reason=unreadable")
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
            label_event_at=None,
            comment_id=comment_id,
            comment_body=body,
        )
        await _apply_notice(sessionmaker, settings, client, notice, tracker, repository)
    await facts.deliver_owed()
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
    *,
    publications: factory_rediscovery.PassPublications | None = None,
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
                await github_factory.record_base_label_notice(
                    session, notice, verified, settings, client=client
                )
            else:
                await github_factory.admit_notice(
                    session, notice, settings, verified, client=client, publications=publications
                )
        except FeedbackUnavailable:
            await session.rollback()
            raise
        except FeedbackIgnored:
            await session.rollback()
            return
        await session.commit()
    if notice.disposition == "admit":
        assert notice.label_event_id is not None
        await factory_intake_notices.sync_intake_notices(
            sessionmaker,
            settings,
            owner="poll-intake",
            client=client,
            issue=tracker.issue(notice.issue_number),
        )
        await factory_notices.sync_status_comments(
            sessionmaker,
            settings,
            owner=f"poll-intake:{notice.request_id}",
            request_ids={notice.request_id},
        )
        async with sessionmaker() as session:
            status = await session.get(FactoryStatusComment, notice.request_id)
            owed = await session.scalar(
                select(FactoryIntakeNotice.id).where(
                    *factory_intake_notices.event_key(
                        tracker.issue(notice.issue_number), notice.label_event_id
                    ),
                    FactoryIntakeNotice.posted_at.is_(None),
                )
            )
            if owed is not None or (
                status is not None and status.posted_at is None and status.refused_at is None
            ):
                raise FeedbackUnavailable("label_answer_unavailable")


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
    sessionmaker: async_sessionmaker[AsyncSession], scope: _Scope, cursor: Cursor, *, labels: bool
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
            if not labels:
                row.comments_since = cursor.comments_since
                row.review_comments_since = cursor.review_comments_since
                row.reviews_since = cursor.reviews_since

            def owned(key: str) -> bool:
                return (key == "issues" or key.startswith("issues:")) == labels

            previous = row.etags if isinstance(row.etags, dict) else {}
            row.etags = {
                **{key: value for key, value in previous.items() if not owned(key)},
                **{key: value for key, value in cursor.etags.items() if owned(key)},
            }
            row.updated_at = datetime.now(UTC)
        await session.commit()
