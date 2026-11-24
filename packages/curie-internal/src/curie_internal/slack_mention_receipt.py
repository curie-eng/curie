"""The verified Slack mention receipt the dispatcher writes and the worker reads (#5414, #5380).

The dispatcher's native Socket Mode lane writes one receipt under the
delivery's deterministic id before the dedupe claim, with ``SET NX EX`` so a
redelivery never overwrites it. The receipt carries what was typed (the event
text exactly as Slack delivered it, mention markup and whitespace included,
before self-mention normalization or any thread context rewrite) and the
sender evidence the API judges, so factory steering and kickoff never read
either from the queued turn text. The frozen ``QueuedTurn`` is not changed; the
receipt is a separate key in the same Valkey trust domain.

The receipt version identifies the source:

1. Version 1, a person. Written for a person's mention that replies in a
   channel thread (``root_ts`` is the thread, ``message_ts`` the reply) and for
   a person's top-level channel mention whose text links an issue
   (``curie_internal.issue_links``), where ``root_ts == message_ts``. A coded
   voice reply also binds the receiving workspace, app and addressed bot.
2. Version 2, an ADR 0202 declared test driver on a test installation, in the
   driver's own channel. ``user_id`` and ``evidence.user_id`` are the declared
   driver's bot user and ``driver_bot_id`` its declared bot id; the event's
   ``user`` and text are never read as identity.
3. Version 3, a human thread mention with one native audio clip. The receiving
   app and team, file id and digest of the raw event text bind the source. The
   body is empty and only a confirmed voice draft may supply steering text.

``driver_bot_id`` is present exactly on version 2 and is left out of a version
1 receipt's serialized form. Ordinary text receipts retain their original
bytes. The API checks the receipt against the delivery before admission.

``kickoff_request_id`` is the deterministic ExecutionRequest id a kickoff
admits under, derived from the receiving identity and the delivery, so a
redelivery finds the request it already admitted.
"""

from __future__ import annotations

import hashlib
import json
import re
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

from .factory_voice_reply import VOICE_REPLY
from .keyspace import SLACK_MENTION_RECEIPT_PREFIX, SLACK_VOICE_SOURCE_PREFIX

FACTORY_VOICE_TURN = "[Factory voice feedback]"
_VOICE_FIELDS = ("voice_file_id", "api_app_id", "team_id", "content_sha256")


def voice_reply_text(raw_text: str, bot_user_id: str) -> str | None:
    """Return the exact code-bearing reply after the receiving bot's leading mention."""
    prefix = re.match(rf"^[ \t]*<@{re.escape(bot_user_id)}(?:\|[^>]+)?>[ \t]+", raw_text)
    if prefix is None:
        return None
    reply = raw_text[prefix.end() :]
    return reply if VOICE_REPLY.fullmatch(reply) else None


_STORE_VOICE_SOURCE = """
if redis.call('EXISTS', KEYS[2]) == 1 then
    return 0
end
if redis.call('SET', KEYS[1], ARGV[1], 'NX', 'EX', ARGV[3]) then
    redis.call('SET', KEYS[2], ARGV[2], 'EX', ARGV[3])
    return 1
end
return 0
"""


class MalformedMentionReceiptError(ValueError):
    """A stored receipt is not valid JSON or does not match the receipt schema."""


class SlackMentionReceipt(BaseModel):
    """One mention, as the dispatcher received it (see the module docstring)."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    version: Literal[1, 2, 3]
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

    voice_file_id: str | None = Field(default=None, pattern=r"^F[A-Z0-9]+$")
    api_app_id: str | None = Field(default=None, pattern=r"^A[A-Z0-9]+$")
    team_id: str | None = Field(default=None, pattern=r"^T[A-Z0-9]+$")
    content_sha256: str | None = Field(default=None, pattern=r"^[a-f0-9]{64}$")

    bot_user_id: str | None = Field(default=None, pattern=r"^[UW][A-Z0-9]+$")

    @model_validator(mode="after")
    def _version_fields(self) -> Self:
        if (self.version == 2) != (self.driver_bot_id is not None):
            raise ValueError("driver_bot_id is required exactly on version 2")
        if self.version == 1 and self.bot_user_id is not None:
            if (
                not self.api_app_id
                or not self.team_id
                or {"voice_file_id", "content_sha256"} & self.model_fields_set
                or voice_reply_text(self.body, self.bot_user_id) is None
            ):
                raise ValueError("incomplete voice decision receipt")
        elif self.version == 3:
            if (
                any(getattr(self, name) is None for name in _VOICE_FIELDS)
                or self.body
                or "bot_user_id" in self.model_fields_set
            ):
                raise ValueError("incomplete voice source receipt")
        elif self.model_fields_set.intersection((*_VOICE_FIELDS, "bot_user_id")):
            raise ValueError("unexpected voice receipt fields")
        else:
            return self
        if (
            self.root_ts == self.message_ts
            or not re.fullmatch(r"[CG][A-Z0-9]+", self.channel)
            or not re.fullmatch(r"[UW][A-Z0-9]+", self.user_id)
            or not re.fullmatch(r"[0-9]+\.[0-9]+", self.root_ts)
            or not re.fullmatch(r"[0-9]+\.[0-9]+", self.message_ts)
            or self.evidence.get("delivery") != "app_mention"
            or self.evidence.get("user_id") != self.user_id
        ):
            raise ValueError("invalid voice mention source")
        return self

    @model_serializer(mode="wrap")
    def _omit_other_versions_fields(self, handler: SerializerFunctionWrapHandler) -> dict[str, Any]:
        data: dict[str, Any] = handler(self)
        if self.driver_bot_id is None:
            data.pop("driver_bot_id", None)
        for name in (*_VOICE_FIELDS, "bot_user_id"):
            if getattr(self, name) is None:
                data.pop(name, None)
        return data


def kickoff_request_id(channel_identity: str, delivery_id: str) -> uuid.UUID:
    """The ExecutionRequest id a kickoff of this delivery on this identity admits under."""

    return uuid.uuid5(
        uuid.NAMESPACE_URL, f"curie-slack://factory/kickoff/{channel_identity}/{delivery_id}"
    )


def receipt_key(delivery_id: str) -> str:
    """The Valkey key one delivery's receipt is stored under."""

    return f"{SLACK_MENTION_RECEIPT_PREFIX}{delivery_id}"


def voice_source_key(
    channel_identity: str, team_id: str, app_id: str, channel: str, root_ts: str, message_ts: str
) -> str:
    scope = [channel_identity, team_id, app_id, channel, root_ts, message_ts]
    digest = hashlib.sha256(json.dumps(scope, separators=(",", ":")).encode()).hexdigest()
    return f"{SLACK_VOICE_SOURCE_PREFIX}{digest}"


async def read_voice_source_receipt(
    redis: AsyncRedis,
    *,
    channel_identity: str,
    team_id: str,
    app_id: str,
    channel: str,
    root_ts: str,
    message_ts: str,
) -> SlackMentionReceipt | None:
    scope = (channel_identity, team_id, app_id, channel, root_ts, message_ts)
    delivery = await redis.get(voice_source_key(*scope))
    if delivery is None:
        return None
    delivery_id = delivery.decode() if isinstance(delivery, bytes) else delivery
    receipt = await read_receipt(redis, delivery_id)
    if receipt is None or receipt.version != 3 or receipt.delivery_id != delivery_id:
        return None
    observed = (
        receipt.channel_identity,
        receipt.team_id,
        receipt.api_app_id,
        receipt.channel,
        receipt.root_ts,
        receipt.message_ts,
    )
    return receipt if observed == scope else None


def write_receipt(redis: Redis, receipt: SlackMentionReceipt, *, ttl_s: int) -> bool:
    """Store ``receipt`` once; False if the delivery or its voice source already has a receipt."""

    if ttl_s <= 0:
        raise ValueError("mention receipt TTL must be positive")
    if receipt.version == 3:
        assert receipt.team_id and receipt.api_app_id
        source_key = voice_source_key(
            receipt.channel_identity,
            receipt.team_id,
            receipt.api_app_id,
            receipt.channel,
            receipt.root_ts,
            receipt.message_ts,
        )
        return bool(
            redis.eval(
                _STORE_VOICE_SOURCE,
                2,
                receipt_key(receipt.delivery_id),
                source_key,
                receipt.model_dump_json(),
                receipt.delivery_id,
                ttl_s,
            )
        )
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
    "FACTORY_VOICE_TURN",
    "MalformedMentionReceiptError",
    "SlackMentionReceipt",
    "kickoff_request_id",
    "read_receipt",
    "read_voice_source_receipt",
    "receipt_key",
    "voice_reply_text",
    "voice_source_key",
    "write_receipt",
]
