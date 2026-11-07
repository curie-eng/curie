"""The verified Slack mention receipt the dispatcher writes and the worker reads (#5414).

For a human ``app_mention`` that replies in a channel thread, the dispatcher's
native Socket Mode lane writes one receipt under the delivery's deterministic id
before the dedupe claim, with ``SET NX EX`` so a redelivery never overwrites it.
The receipt carries what the person typed (the stripped body before any thread
context rewrite) and the sender evidence the API judges, so factory steering
never reads either from the queued turn text. The frozen ``QueuedTurn`` is not
changed; the receipt is a separate key in the same Valkey trust domain.

The API rechecks receipt to delivery equality before it decides anything.
"""

from __future__ import annotations

import json
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, ValidationError
from redis import Redis
from redis.asyncio import Redis as AsyncRedis

from .keyspace import SLACK_MENTION_RECEIPT_PREFIX


class MalformedMentionReceiptError(ValueError):
    """A stored receipt is not valid JSON or does not match the receipt schema."""


class SlackMentionReceipt(BaseModel):
    """One threaded human mention, as the dispatcher received it."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    version: Literal[1]
    delivery_id: str
    channel_identity: str
    channel: str
    root_ts: str
    message_ts: str
    user_id: str
    body: str
    evidence: dict[str, Any]


def receipt_key(delivery_id: str) -> str:
    """The Valkey key one delivery's receipt is stored under."""

    return f"{SLACK_MENTION_RECEIPT_PREFIX}{delivery_id}"


def write_receipt(redis: Redis, receipt: SlackMentionReceipt, *, ttl_s: int) -> bool:
    """Store ``receipt`` once; False when a receipt for the delivery already exists."""

    if ttl_s <= 0:
        raise ValueError("mention receipt TTL must be positive")
    stored = redis.set(
        receipt_key(receipt.delivery_id),
        receipt.model_dump_json(),
        nx=True,
        ex=ttl_s,
    )
    return bool(stored)


async def read_receipt(redis: AsyncRedis, delivery_id: str) -> SlackMentionReceipt | None:
    """The stored receipt, or None when absent or expired.

    A stored value that is not a valid receipt raises
    ``MalformedMentionReceiptError``; it is never read as absent.
    """

    raw = await redis.get(receipt_key(delivery_id))
    if raw is None:
        return None
    try:
        text = raw.decode() if isinstance(raw, bytes) else str(raw)
        return SlackMentionReceipt.model_validate(json.loads(text))
    except (UnicodeDecodeError, json.JSONDecodeError, ValidationError) as exc:
        raise MalformedMentionReceiptError("stored Slack mention receipt is malformed") from exc


__all__ = [
    "MalformedMentionReceiptError",
    "SlackMentionReceipt",
    "read_receipt",
    "receipt_key",
    "write_receipt",
]
