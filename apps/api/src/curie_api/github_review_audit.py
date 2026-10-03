"""Durable authenticated delivery receipts without webhook bodies or credentials."""

import hashlib
import re
import uuid
from typing import Any, Literal

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

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


async def _insert_delivery(
    session: AsyncSession,
    *,
    delivery_id: uuid.UUID,
    event: str,
    body: bytes,
    payload: Any,
) -> tuple[str, bool]:
    """Insert the receipt unless the id exists; return the body digest and whether it was new."""
    data = _object(payload)
    if event == "issues":
        source = _object(data.get("issue"))
        pr = source
    elif event == "pull_request_review":
        source = _object(data.get("review"))
        pr = _object(data.get("pull_request"))
    else:
        source = _object(data.get("comment"))
        pr = _object(data.get("issue") if event == "issue_comment" else data.get("pull_request"))
    sender = _object(data.get("sender"))
    digest = hashlib.sha256(body).hexdigest()
    login = sender.get("login")
    if not isinstance(login, str) or re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9-]{0,38}", login) is None:
        login = None
    inserted = await session.scalar(
        insert(GitHubReviewDelivery)
        .values(
            delivery_id=delivery_id,
            event_kind=event,
            body_sha256=digest,
            action=_enum(
                data.get("action"),
                {
                    "created",
                    "edited",
                    "deleted",
                    "submitted",
                    "dismissed",
                    "labeled",
                    "unlabeled",
                    "closed",
                    "opened",
                    "reopened",
                },
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
        .returning(GitHubReviewDelivery.delivery_id)
    )
    return digest, inserted is not None


async def _lock_delivery(
    session: AsyncSession, *, delivery_id: uuid.UUID, event: str, digest: str
) -> tuple[GitHubReviewDelivery, bool]:
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
    digest, _inserted = await _insert_delivery(
        session, delivery_id=delivery_id, event=event, body=body, payload=payload
    )
    return await _lock_delivery(session, delivery_id=delivery_id, event=event, digest=digest)


PushClaim = Literal["process", "duplicate", "unsettled", "conflict"]


async def claim_push_delivery(
    sessionmaker: async_sessionmaker[AsyncSession],
    *,
    delivery_id: uuid.UUID,
    event: str,
    body: bytes,
    payload: Any,
) -> PushClaim:
    """Claim a push delivery id in one short transaction of its own (#3820).

    The committed `pending` receipt is the in-flight marker; no connection or
    row lock is held while the caller processes the push, which then settles
    the receipt with `settle_push_delivery`. Answers:

    `process`: this caller owns the delivery. Either the receipt is new, or a
    previous delivery under this id was ignored, rejected, or retryable and is
    reopened as `pending`.
    `duplicate`: a delivery under this id already deployed or promoted.
    `unsettled`: the receipt is still `pending`. Another delivery under this id
    is in flight, or one crashed before settling, possibly after it deployed.
    Processing it again could deploy twice, so it is never retaken.
    `conflict`: the id is already bound to a different event or body.

    HMAC verification belongs to the caller and must precede this function.
    """
    async with sessionmaker() as session, session.begin():
        digest, inserted = await _insert_delivery(
            session, delivery_id=delivery_id, event=event, body=body, payload=payload
        )
        row, conflict = await _lock_delivery(
            session, delivery_id=delivery_id, event=event, digest=digest
        )
        if conflict:
            return "conflict"
        if inserted:
            return "process"
        if row.status == "accepted":
            return "duplicate"
        if row.status == "pending":
            return "unsettled"
        settle_review_delivery(row, "pending")
        return "process"


async def settle_push_delivery(
    sessionmaker: async_sessionmaker[AsyncSession],
    *,
    delivery_id: uuid.UUID,
    status: str,
    reason: str | None = None,
) -> None:
    """Settle a push receipt claimed by `claim_push_delivery`, in its own transaction."""
    async with sessionmaker() as session, session.begin():
        row = await session.scalar(
            select(GitHubReviewDelivery)
            .where(GitHubReviewDelivery.delivery_id == delivery_id)
            .with_for_update()
        )
        assert row is not None
        settle_review_delivery(row, status, reason)


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
