"""The verified Slack mention receipt the dispatcher writes and the worker reads (#5414, #5380).

The dispatcher's native Socket Mode lane writes one receipt under the
delivery's deterministic id before the dedupe claim, with ``SET NX EX`` so a
redelivery never overwrites it. The receipt carries what was typed (the event
text exactly as Slack delivered it, mention markup and whitespace included,
before self-mention normalization or any thread context rewrite) and the
sender evidence the API judges, so factory steering and kickoff never read
either from the queued turn text. The frozen ``QueuedTurn`` is not changed; the
receipt is a separate key in the same Valkey trust domain.

Two versions exist:

1. Version 1, a person. Written for a person's mention that replies in a
   channel thread (``root_ts`` is the thread, ``message_ts`` the reply) and for
   a person's top-level channel mention whose text links an issue
   (``curie_internal.issue_links``), where ``root_ts == message_ts``.
2. Version 2, an ADR 0202 declared test driver on a test installation, in the
   driver's own channel. ``user_id`` and ``evidence.user_id`` are the declared
   driver's bot user and ``driver_bot_id`` its declared bot id; the event's
   ``user`` and text are never read as identity.

``driver_bot_id`` is present exactly on version 2 and is left out of a version
1 receipt's serialized form, so version 1 bytes are the ones written before
version 2 existed. A consumer that only judges version 1 (steering) refuses
version 2 and root receipts itself; the API rechecks receipt to delivery
equality before it decides anything.

``kickoff_request_id`` is the deterministic ExecutionRequest id a kickoff
admits under, derived from the receiving identity and the delivery, so a
redelivery finds the request it already admitted.
"""

from __future__ import annotations

import json
import uuid
from typing import Any, Literal, Self

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    SerializerFunctionWrapHandler,
    ValidationError,
    model_serializer,
    model_validator,
)
from redis import Redis
from redis.asyncio import Redis as AsyncRedis

from .keyspace import SLACK_MENTION_RECEIPT_PREFIX


class MalformedMentionReceiptError(ValueError):
    """A stored receipt is not valid JSON or does not match the receipt schema."""


class SlackMentionReceipt(BaseModel):
    """One mention, as the dispatcher received it (see the module docstring)."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    # 1 a person, 2 an ADR 0202 declared test driver.
    version: Literal[1, 2]
    delivery_id: str
    # minted_adapter(slack_identity): the identity the delivery arrived on.
    channel_identity: str
    channel: str
    # The thread's ts, or the message's own ts for a top-level mention.
    root_ts: str
    message_ts: str
    # Version 2: the declared bot user id, never the event's user.
    user_id: str
    # The raw derived event text, mention markup included.
    body: str
    # The API's SlackEvidence shape; version 2 names the declared bot user.
    evidence: dict[str, Any]
    driver_bot_id: str | None = Field(default=None, pattern=r"^B[A-Z0-9]+$")

    @model_validator(mode="after")
    def _driver_only_on_version_2(self) -> Self:
        if (self.version == 2) != (self.driver_bot_id is not None):
            raise ValueError("driver_bot_id is required on version 2 and forbidden on version 1")
        return self

    @model_serializer(mode="wrap")
    def _omit_absent_driver(self, handler: SerializerFunctionWrapHandler) -> dict[str, Any]:
        data: dict[str, Any] = handler(self)
        if self.driver_bot_id is None:
            data.pop("driver_bot_id", None)
        return data


def kickoff_request_id(channel_identity: str, delivery_id: str) -> uuid.UUID:
    """The ExecutionRequest id a kickoff of this delivery on this identity admits under."""

    return uuid.uuid5(
        uuid.NAMESPACE_URL, f"curie-slack://factory/kickoff/{channel_identity}/{delivery_id}"
    )


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
    "kickoff_request_id",
    "read_receipt",
    "receipt_key",
    "write_receipt",
]
