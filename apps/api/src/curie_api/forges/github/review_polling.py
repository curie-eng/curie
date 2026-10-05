"""Poll GitHub for review feedback on the factory's open pull requests."""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from curie_api.config import Settings
from curie_api.factory_label_reconcile import parse_time
from curie_api.forges.github.transport import _list, get_github_json
from curie_api.github_factory_review import admit_parsed_feedback
from curie_api.github_review_events import (
    FeedbackIgnored,
    FeedbackUnavailable,
    human_sender,
    parse_feedback,
)
from curie_api.models import ThreadPublicationLineage, WorkItem

_LOOKBACK = timedelta(hours=1)


@dataclass
class _Cursor:
    comments_since: datetime | None = None
    review_comments_since: datetime | None = None
    reviews_since: datetime | None = None
    etags: dict[str, str] = field(default_factory=dict)
    repository_id: int | None = None


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
