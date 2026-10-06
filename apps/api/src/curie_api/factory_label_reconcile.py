"""Admit factory-labeled issues whose label delivery never arrived (#3081).

GitHub does not redeliver a webhook that failed, for example while a tunnel
returned 502. Without this pass the label is simply lost. The reconciler asks
GitHub for open issues carrying the factory label on every bound repository,
and admits the ones that have no WorkItem through the same verification the
webhook path uses: the current installation, the issue state and label, and
the labeling user's current write permission.

Reconciliation is idempotent with delivery. It only admits an issue with no
WorkItem, checked again under the per-issue lock the webhook path holds, and
only once the label is older than a grace period, so a delivery that is merely
in flight lands first. Its request id derives from GitHub's labeled event id,
so replicas and repeated passes converge on one request.

The GitHub tracker adapter (`curie_api.forges.github.tracker`) makes the
reads: the open labeled issues and each issue's events.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Any

import httpx
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from starlette.concurrency import run_in_threadpool

from . import github_factory
from .config import Settings
from .forges.errors import Unavailable
from .forges.github.binding import GITHUB_CHANNEL_KIND
from .forges.github.comments import static_token
from .forges.github.tracker import GitHubTracker, last_label_event, read_repository
from .forges.identity import reconcile_delivery_id
from .github_app import GitHubAppError, GitHubInstallationRefused, credentials_for
from .github_factory_events import FactoryNotice
from .github_review_events import FeedbackIgnored, FeedbackUnavailable, human_sender
from .models import Agent, AgentChannel
from .repo_full_name import InvalidRepoFullName, normalize_repo_full_name
from .workspace_policy import repository_is_allowed

logger = logging.getLogger(__name__)


def parse_time(value: Any) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo is not None else None


async def bound_repositories(session: AsyncSession) -> list[str]:
    rows = await session.scalars(
        select(AgentChannel.address)
        .join(Agent, Agent.id == AgentChannel.agent_id)
        .where(
            AgentChannel.kind == GITHUB_CHANNEL_KIND,
            Agent.repo_full_name == AgentChannel.address,
        )
        .distinct()
    )
    return list(rows)


async def reconcile_missed_labels(
    sessionmaker: async_sessionmaker[AsyncSession],
    settings: Settings,
    client: httpx.AsyncClient,
    *,
    now: datetime,
) -> int:
    """Admit labeled open issues with no WorkItem. Returns how many were admitted."""

    async with sessionmaker() as session:
        repositories = await bound_repositories(session)
    admitted = 0
    for repository in repositories:
        try:
            repo = normalize_repo_full_name(repository)
        except InvalidRepoFullName:
            continue
        if not repository_is_allowed(repo, settings.github_repo_allowlist):
            continue
        try:
            admitted += await _reconcile_repository(sessionmaker, settings, client, repo, now)
        except Unavailable as exc:
            logger.info("factory label reconcile for %s deferred: %s unavailable", repo, exc)
        except (GitHubAppError, GitHubInstallationRefused, ValueError):
            logger.info("factory label reconcile for %s deferred: no installation", repo)
    return admitted


async def _reconcile_repository(
    sessionmaker: async_sessionmaker[AsyncSession],
    settings: Settings,
    client: httpx.AsyncClient,
    repo: str,
    now: datetime,
) -> int:
    label = settings.github_factory_label
    installation_id, token = await run_in_threadpool(
        credentials_for(settings).fresh_installation_token, repo, None
    )
    repository = await read_repository(
        client, api=settings.github_api_url, repo_full_name=repo, token=token
    )
    repository_id = repository.get("id")
    if type(repository_id) is not int or repository_id <= 0:
        raise Unavailable(f"/repos/{repo}")
    tracker = GitHubTracker.from_settings(
        settings,
        client,
        repo_full_name=repo,
        repository_id=repository_id,
        token=static_token(token),
    )
    issues, _etag = await tracker.labeled_open_issues(None)
    numbers = [
        issue["number"]
        for issue in issues or []
        if isinstance(issue, dict)
        and "pull_request" not in issue
        and type(issue.get("number")) is int
        and issue["number"] > 0
    ]
    if not numbers:
        return 0
    async with sessionmaker() as session:
        missing = [
            number
            for number in numbers
            if await github_factory.work_item_for(session, tracker.issue(number)) is None
        ]
    grace = timedelta(seconds=settings.github_factory_reconcile_grace_s)
    admitted = 0
    for number in missing:
        try:
            events = await tracker.issue_events(number)
        except Unavailable:
            # One unreadable issue must not hold up the rest of the repository.
            continue
        event = last_label_event(events, label)
        if event is None or type(event.get("id")) is not int:
            continue
        labeled_at = parse_time(event.get("created_at"))
        if labeled_at is None or labeled_at > now - grace:
            # A delivery may still be in flight; let it land first.
            continue
        if event.get("performed_via_github_app") is not None:
            continue
        actor = event.get("actor")
        if not isinstance(actor, dict) or actor.get("type") == "Bot":
            continue
        try:
            sender_id, sender_login = human_sender(actor)
        except FeedbackIgnored:
            continue
        notice = FactoryNotice(
            reconcile_delivery_id(tracker.issue(number), str(event["id"])),
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
        )
        if await _admit(sessionmaker, settings, client, notice):
            admitted += 1
    return admitted


async def _admit(
    sessionmaker: async_sessionmaker[AsyncSession],
    settings: Settings,
    client: httpx.AsyncClient,
    notice: FactoryNotice,
) -> bool:
    async with sessionmaker() as session:
        try:
            issue = github_factory.notice_issue(notice, settings)
            await github_factory.lock_issue(session, issue)
            # A delivery may have admitted it since the listing; the lock orders us.
            if await github_factory.work_item_for(session, issue) is not None:
                await session.rollback()
                return False
            verified = await github_factory.verify_current(notice, settings=settings, client=client)
            outcome = await github_factory.admit_notice(session, notice, settings, verified)
        except (FeedbackUnavailable, FeedbackIgnored) as exc:
            await session.rollback()
            logger.info(
                "factory label reconcile skipped %s#%d: %s",
                notice.repo_full_name,
                notice.issue_number,
                exc.code,
            )
            return False
        await session.commit()
    if outcome.status != "factory_admitted":
        return False
    logger.warning(
        "factory label reconcile admitted %s#%d; its label delivery never arrived",
        notice.repo_full_name,
        notice.issue_number,
    )
    return True
