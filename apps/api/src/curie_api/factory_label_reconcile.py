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

Endpoints follow GitHub's REST reference:
https://docs.github.com/en/rest/issues/issues#list-repository-issues
https://docs.github.com/en/rest/issues/events#list-issue-events
"""

from __future__ import annotations

import logging
import uuid
from datetime import datetime, timedelta
from typing import Any

import httpx
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from starlette.concurrency import run_in_threadpool

from . import github_factory
from .config import Settings
from .github_app import GitHubAppError, GitHubInstallationRefused, credentials_for
from .github_factory_events import FactoryNotice
from .github_review_events import FeedbackIgnored, FeedbackUnavailable, human_sender
from .github_review_truth import get_github_json, github_headers
from .models import Agent, AgentChannel
from .repo_full_name import InvalidRepoFullName, normalize_repo_full_name, repo_url_path
from .workitems import GITHUB_CHANNEL_KIND
from .workspace_policy import repository_is_allowed

logger = logging.getLogger(__name__)

# One page of each listing per pass. A backlog larger than this drains over
# later passes, because admitted issues drop out of the candidate set.
_PER_PAGE = 100


class _Unavailable(Exception):
    """GitHub could not answer; try the repository again next pass."""


async def _get_list(
    client: httpx.AsyncClient, *, api: str, token: str, path: str, params: dict[str, Any]
) -> list[Any]:
    try:
        response = await client.get(
            f"{api}{path}",
            params=params,
            headers=github_headers(token),
            follow_redirects=False,
        )
    except httpx.HTTPError:
        raise _Unavailable(path) from None
    if response.status_code != 200:
        raise _Unavailable(path)
    try:
        result = response.json()
    except ValueError:
        raise _Unavailable(path) from None
    if not isinstance(result, list):
        raise _Unavailable(path)
    return result


def _parse_time(value: Any) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo is not None else None


def _last_label_event(events: list[Any], label: str) -> dict[str, Any] | None:
    """The newest ``labeled`` event for this label, or None."""

    found: dict[str, Any] | None = None
    for event in events:
        if (
            isinstance(event, dict)
            and event.get("event") == "labeled"
            and isinstance(event.get("label"), dict)
            and event["label"].get("name") == label
        ):
            found = event
    return found


def label_event_delivery_id(repository_id: int, issue_number: int, event_id: int) -> uuid.UUID:
    """A stable stand-in delivery id for one labeled event.

    It never collides with a real X-GitHub-Delivery, and the same event always
    yields the same request id.
    """

    return uuid.uuid5(
        uuid.NAMESPACE_URL,
        f"https://github.com/factory/reconcile/{repository_id}/{issue_number}/{event_id}",
    )


async def _bound_repositories(session: AsyncSession) -> list[str]:
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
        repositories = await _bound_repositories(session)
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
        except _Unavailable as exc:
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
    api = settings.github_api_url.rstrip("/")
    repo_path = f"/repos/{repo_url_path(repo)}"
    repository = await get_github_json(
        client, api=api, token=token, path=repo_path, refusal="repository_unavailable"
    )
    repository_id = repository.get("id")
    if type(repository_id) is not int or repository_id <= 0:
        raise _Unavailable(repo_path)
    issues = await _get_list(
        client,
        api=api,
        token=token,
        path=f"{repo_path}/issues",
        params={"state": "open", "labels": label, "per_page": _PER_PAGE},
    )
    numbers = [
        issue["number"]
        for issue in issues
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
            if await github_factory.work_item_for(session, repository_id, number) is None
        ]
    grace = timedelta(seconds=settings.github_factory_reconcile_grace_s)
    admitted = 0
    for number in missing:
        events = await _get_list(
            client,
            api=api,
            token=token,
            path=f"{repo_path}/issues/{number}/events",
            params={"per_page": _PER_PAGE},
        )
        event = _last_label_event(events, label)
        if event is None or type(event.get("id")) is not int:
            continue
        labeled_at = _parse_time(event.get("created_at"))
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
            label_event_delivery_id(repository_id, number, event["id"]),
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
            await github_factory.lock_issue(session, notice.repository_id, notice.issue_number)
            # A delivery may have admitted it since the listing; the lock orders us.
            if (
                await github_factory.work_item_for(
                    session, notice.repository_id, notice.issue_number
                )
                is not None
            ):
                await session.rollback()
                return False
            await github_factory.verify_current(notice, settings=settings, client=client)
            outcome = await github_factory.admit_notice(session, notice, settings)
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
