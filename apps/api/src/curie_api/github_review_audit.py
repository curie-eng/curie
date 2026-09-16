"""Durable authenticated delivery receipts without webhook bodies or credentials."""

import hashlib
import re
import uuid
from typing import Any

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from .models import GitHubReviewDelivery


def _object(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _id(value: Any, maximum: int = 2**63 - 1) -> int | None:
    return (
        value
        if isinstance(value, int) and not isinstance(value, bool) and 0 < value <= maximum
        else None
    )


def _enum(value: Any, choices: set[str]) -> str:
    return value if isinstance(value, str) and value in choices else "other"


async def claim_review_delivery(
    session: AsyncSession,
    *,
    delivery_id: uuid.UUID,
    event: str,
    body: bytes,
    payload: Any,
) -> tuple[GitHubReviewDelivery, bool]:
    """Serialize a delivery header and bind every alias to its original bytes.

    HMAC verification belongs to the caller and must precede this function.
    The body is hashed in memory and never persisted. Holding this row lock
    until admission commits makes same-header races adopt one canonical result.
    """
    data = _object(payload)
    source = _object(data.get("review") if event == "pull_request_review" else data.get("comment"))
    pr = _object(data.get("issue") if event == "issue_comment" else data.get("pull_request"))
    sender = _object(data.get("sender"))
    digest = hashlib.sha256(body).hexdigest()
    login = sender.get("login")
    if not isinstance(login, str) or re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9-]{0,38}", login) is None:
        login = None
    await session.execute(
        insert(GitHubReviewDelivery)
        .values(
            delivery_id=delivery_id,
            event_kind=event,
            body_sha256=digest,
            action=_enum(
                data.get("action"), {"created", "edited", "deleted", "submitted", "dismissed"}
            ),
            repository_id=_id(_object(data.get("repository")).get("id")),
            installation_id=_id(_object(data.get("installation")).get("id")),
            pr_number=_id(pr.get("number"), 2**31 - 1),
            source_object_id=_id(source.get("id")),
            sender_id=_id(sender.get("id")),
            sender_login=login,
            sender_type=_enum(sender.get("type"), {"User", "Bot", "Organization"}),
            author_association=_enum(
                source.get("author_association"),
                {
                    "OWNER",
                    "MEMBER",
                    "COLLABORATOR",
                    "CONTRIBUTOR",
                    "FIRST_TIMER",
                    "FIRST_TIME_CONTRIBUTOR",
                    "NONE",
                    "MANNEQUIN",
                },
            ),
        )
        .on_conflict_do_nothing(index_elements=["delivery_id"])
    )
    row = await session.scalar(
        select(GitHubReviewDelivery)
        .where(GitHubReviewDelivery.delivery_id == delivery_id)
        .with_for_update()
    )
    assert row is not None
    conflict = row.event_kind != event or row.body_sha256 != digest
    if conflict:
        row.replay_conflicts += 1
        row.version += 1
    return row, conflict


def settle_review_delivery(
    row: GitHubReviewDelivery,
    status: str,
    reason: str | None = None,
    *,
    event_id: str | None = None,
) -> None:
    row.status = status
    row.reason = reason
    row.event_id = event_id
    row.version += 1
