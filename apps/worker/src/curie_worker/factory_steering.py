"""Factory run steering from a factory run's Slack root (#5414).

The kernel asks ``FactorySteeringClient.steer`` about every turn right after
its identity preparation, before the cron branch and before any binding,
shimmer or sandbox claim. The answer is None for every turn that is not a
Slack turn in a factory run's Slack root, and the ordinary path then runs
unchanged.

A Slack turn's root is mapped when ``factory_slack_threads`` names it, read
here by direct SQL like ``binding.py``. A mapped turn never enters ordinary
chat. It is refused here with ``steering_evidence_missing`` when it carries an
HTTP identity context, is a block action, or has no stored mention receipt that
matches it (``curie_internal.slack_mention_receipt``, written by the native
Socket Mode lane before the dedupe claim). A worker with no internal worker
token refuses it first, with ``steering_unconfigured`` and no API call. Otherwise
the API decides at ``POST /v1/internal/factory-slack/steer`` and its outcome is
the reply. The client is always built when the worker has its database engine,
so no switch lets a mapped root fall through to ordinary chat. A
transport failure, a 404, a 5xx or a malformed answer raises
``SteeringUnavailable``: the delivery stays pending under the existing lease and
delivery cap, so an outage fails closed and is bounded.

The kernel passes the delivery's remaining budget. A mapped root whose budget
is at or below the kernel's minimum attempt budget is refused with
``steering_budget_expired`` before the token check and with no API call; any
other budget bounds the API call, and a call that outlives it raises
``SteeringUnavailable`` (admission is idempotent by its deterministic request
id, so a later delivery admits at most once).
"""

from __future__ import annotations

import asyncio
import logging
import re
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any, Final

import httpx
from aci_protocol import QueuedTurn, TurnSource
from aci_protocol.turn import route_identity
from curie_internal.slack_mention_receipt import (
    MalformedMentionReceiptError,
    SlackMentionReceipt,
    read_receipt,
)
from redis.asyncio import Redis as AsyncRedis
from redis.exceptions import RedisError
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncEngine

from .binding import binding_adapter_for_handle
from .kernel import constants as kernel_constants
from .kernel.identity import CURRENT as HTTP_TURN_CARRY

logger = logging.getLogger(__name__)

_SAFE_SCHEMA = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
STEER_PATH: Final = "/v1/internal/factory-slack/steer"
EVIDENCE_MISSING: Final = "steering_evidence_missing"
UNCONFIGURED: Final = "steering_unconfigured"
BUDGET_EXPIRED: Final = "steering_budget_expired"
NOT_MAPPED: Final = "not_mapped"
ADMITTED: Final = "admitted"
# The dispatcher's block action delivery ids start with this
# (``handlers.process_action``); a click is never steering evidence.
_ACTION_EVENT_PREFIX: Final = "action-"
_SLACK_KIND: Final = "slack"
_TEXT_MAX: Final = 300

_HEADLINES: Final = {
    ADMITTED: "Revision queued for this pull request.",
    EVIDENCE_MISSING: "This factory run cannot be steered from this message.",
    UNCONFIGURED: "Steering is not configured on this worker, so it did not ask for a decision.",
    BUDGET_EXPIRED: (
        "This message ran out of time before a decision was confirmed. "
        "The run thread shows any new revision."
    ),
    "receipt_mismatch": "This factory run cannot be steered from this message.",
    "sender_unverified": "Your Slack account is not verified for this workspace.",
    "code_host_identity_missing": "No code host account is linked to you for this repository.",
    "code_host_identity_ambiguous": "More than one code host account is linked to you.",
    "code_host_identity_unsupported": "This code host does not support linked accounts.",
    "code_host_mismatch": "This pull request's code host is not the one configured here.",
    "repository_mismatch": "This pull request's repository changed, so this message was refused.",
    "not_authorized": "You do not have write access to this pull request's repository.",
    "authority_changed": "Your access changed while this was decided, so this message was refused.",
    "work_item_cancelled": "This factory run was cancelled.",
    "lineage_closed": "This factory run's pull request is closed.",
    "no_pull_request": "This factory run has no pull request yet.",
    "pull_request_not_open": "This factory run's pull request is not open.",
    "pull_request_branch_mismatch": "This factory run's pull request moved to another branch.",
    "objective_too_large": "This message is too long to queue as a revision.",
    "admission_refused": "This message was refused as a revision.",
}
_FALLBACK_HEADLINE: Final = "This message was refused as a revision."
# Every refusal can follow an earlier attempt of the same delivery whose
# admission committed with its answer lost (plan R1-1), so no refusal ever says
# nothing was queued: it names this attempt's refusal and leaves the earlier
# outcome to the run thread.
_EARLIER_ATTEMPTS: Final = (
    "Any earlier attempt of this message is unconfirmed here; "
    "the run thread shows any new revision."
)


UNCONFIRMED_TEXT: Final = (
    "I could not confirm this request yet. Please do not send it again; "
    "the run thread shows any new revision."
)


class SteeringUnavailable(RuntimeError):
    """The steering decision could not be read; the delivery must stay pending.

    Raised only where steering may own the delivery, so its retry never
    invites a resend: a resend is a new delivery id, so a new request, and
    an earlier attempt of this delivery may already have admitted with its
    answer lost. ``pending_text`` is set once this attempt's request may have
    reached the API, and is told to the person; otherwise the thread keeps
    whatever it already shows.
    """

    def __init__(self, message: str, *, pending_text: str | None = None) -> None:
        super().__init__(message)
        self.pending_text = pending_text


@dataclass(frozen=True)
class SteeringReply:
    """What the kernel tells the person, and the decision it reports."""

    outcome: str
    text: str
    request_id: str | None = None
    pull_request_url: str | None = None

    @property
    def admitted(self) -> bool:
        """Whether the decision queued (or had already queued) the revision."""
        return self.outcome == ADMITTED


def _plain(value: str) -> str:
    """A bounded value with Slack's three control characters escaped."""

    bounded = value if len(value) <= _TEXT_MAX else value[: _TEXT_MAX - 1] + "..."
    return bounded.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def render_reply(
    outcome: str,
    *,
    reason: str | None = None,
    pull_request_url: str | None = None,
    replayed: bool = False,
) -> str:
    """The short plain reply for one outcome; it always names the outcome code."""

    headline = _HEADLINES.get(outcome, _FALLBACK_HEADLINE)
    if outcome == ADMITTED and replayed:
        headline = "This revision was already queued."
    lines = [headline]
    if outcome == ADMITTED and pull_request_url:
        lines.append(f"Pull request: {_plain(pull_request_url)}")
    if outcome != ADMITTED and outcome != BUDGET_EXPIRED:
        lines.append(_EARLIER_ATTEMPTS)
    detail = f"Outcome: {_plain(outcome)}"
    if reason:
        detail += f" ({_plain(reason)})"
    lines.append(detail)
    return "\n".join(lines)


def _refused(outcome: str = EVIDENCE_MISSING) -> SteeringReply:
    return SteeringReply(outcome=outcome, text=render_reply(outcome))


def _optional_text(body: dict[str, Any], key: str) -> str | None:
    value = body.get(key)
    if value is None:
        return None
    if not isinstance(value, str):
        raise SteeringUnavailable(f"factory steering answer field {key} is not a string")
    return value


class FactorySteeringClient:
    """Answers whether a turn steers a factory run, and with what reply."""

    def __init__(
        self,
        *,
        engine: AsyncEngine,
        schema: str,
        redis: AsyncRedis,
        api_base_url: str,
        worker_token: str | None,
        client: httpx.AsyncClient,
    ) -> None:
        if not _SAFE_SCHEMA.fullmatch(schema):
            raise ValueError("factory steering database schema is invalid")
        self._engine = engine
        self._threads = f'"{schema}".factory_slack_threads'
        self._redis = redis
        self._url = api_base_url.rstrip("/") + STEER_PATH
        # No token: a mapped root is still intercepted, then refused locally.
        self._headers = {"X-Curie-Worker-Token": worker_token} if worker_token else None
        self._client = client

    async def _mapped(self, identity: str, channel: str, root_ts: str) -> bool:
        """Whether this Slack root is a factory run's root."""

        async with self._engine.connect() as connection:
            row = (
                await connection.execute(
                    text(
                        f"""
                        SELECT work_item_id
                          FROM {self._threads}
                         WHERE channel_identity = :identity
                           AND channel = :channel
                           AND root_ts = :root_ts
                        """
                    ),
                    {"identity": identity, "channel": channel, "root_ts": root_ts},
                )
            ).first()
        return row is not None

    async def steer(
        self,
        qevent: QueuedTurn,
        *,
        budget_s: float | None,
        record_intent: Callable[[], Awaitable[None]],
    ) -> SteeringReply | None:
        """None for an ordinary turn; otherwise the reply that settles it.

        ``budget_s`` is the delivery's remaining budget, None when unbounded.
        ``record_intent`` durably records, before the request is sent, that
        this delivery may have admitted; a request whose record fails is never
        sent.
        Raises ``SteeringUnavailable`` when the decision cannot be read, so the
        delivery stays pending rather than falling back to ordinary chat.
        """

        loop = asyncio.get_running_loop()
        deadline = None if budget_s is None else loop.time() + budget_s
        handle = qevent.reply_handle
        if qevent.source is not TurnSource.SLACK or handle is None or handle.kind != _SLACK_KIND:
            return None
        root_ts = qevent.conversation_id
        if not root_ts:
            return None
        try:
            identity = route_identity(_SLACK_KIND, binding_adapter_for_handle(handle))
        except ValueError:
            # An invalid selector is refused on the ordinary path by name.
            return None
        if identity is None:
            return None
        try:
            mapped = await self._mapped(identity, handle.channel, root_ts)
        except (SQLAlchemyError, OSError, TimeoutError) as exc:
            # Unknown, so possibly a mapped root: never ordinary chat. A
            # refused or timed out connection can escape the driver unwrapped.
            raise SteeringUnavailable("factory run mapping could not be read") from exc
        if not mapped:
            return None
        if (
            deadline is not None
            and deadline - loop.time() <= kernel_constants._MIN_ATTEMPT_BUDGET_S
        ):
            # Mapped, but too little budget is left to decide; never ordinary chat.
            logger.warning("factory steering refused %s: delivery budget expired", qevent.event_id)
            return _refused(BUDGET_EXPIRED)
        if self._headers is None:
            # Mapped but this worker cannot ask the API: refused, never ordinary chat.
            logger.warning("factory steering refused %s: no internal worker token", qevent.event_id)
            return _refused(UNCONFIGURED)

        # Mapped: this turn never enters ordinary chat. Native Socket Mode
        # evidence is the only evidence; an HTTP selected turn is refused
        # before any local link is consulted (plan R1.3).
        carry = HTTP_TURN_CARRY.get()
        if carry is not None and carry.context is not None:
            logger.info("factory steering refused %s: HTTP identity carry", qevent.event_id)
            return _refused()
        if qevent.event_id.startswith(_ACTION_EVENT_PREFIX):
            logger.info("factory steering refused %s: block action", qevent.event_id)
            return _refused()
        try:
            receipt = await read_receipt(self._redis, qevent.event_id)
        except MalformedMentionReceiptError:
            logger.warning("factory steering refused %s: malformed receipt", qevent.event_id)
            return _refused()
        except RedisError as exc:
            raise SteeringUnavailable("mention receipt could not be read") from exc
        if receipt is None or not self._matches(receipt, qevent, identity, root_ts):
            logger.info("factory steering refused %s: no matching receipt", qevent.event_id)
            return _refused()

        try:
            await record_intent()
        except Exception as exc:  # noqa: BLE001 - unrecorded intent never sends
            raise SteeringUnavailable(
                f"steering intent could not be recorded: {type(exc).__name__}"
            ) from exc
        try:
            body = await self._ask(
                {
                    "delivery_id": qevent.event_id,
                    "channel_identity": identity,
                    "channel": handle.channel,
                    "root_ts": root_ts,
                    "author": qevent.author,
                    "receipt": receipt.model_dump(mode="json"),
                },
                deadline=deadline,
            )
            outcome = body.get("outcome")
            if not isinstance(outcome, str) or not outcome:
                raise SteeringUnavailable("factory steering answer has no outcome")
            reason = _optional_text(body, "reason")
            request_id = _optional_text(body, "request_id")
            pull_request_url = _optional_text(body, "pull_request_url")
        except SteeringUnavailable as exc:
            # Sent, so the admission may have committed with its answer lost.
            raise SteeringUnavailable(str(exc), pending_text=UNCONFIRMED_TEXT) from exc
        replayed = body.get("replayed") is True
        if outcome == NOT_MAPPED:
            # The local read said mapped. A disagreement is never ordinary chat.
            logger.warning("factory steering %s: API reports an unmapped root", qevent.event_id)
            return _refused()
        logger.info("factory steering %s outcome=%s", qevent.event_id, outcome)
        return SteeringReply(
            outcome=outcome,
            text=render_reply(
                outcome,
                reason=reason,
                pull_request_url=pull_request_url,
                replayed=replayed,
            ),
            request_id=request_id,
            pull_request_url=pull_request_url,
        )

    @staticmethod
    def _matches(
        receipt: SlackMentionReceipt, qevent: QueuedTurn, identity: str, root_ts: str
    ) -> bool:
        handle = qevent.reply_handle
        assert handle is not None
        return (
            receipt.delivery_id == qevent.event_id
            and receipt.channel_identity == identity
            and receipt.channel == handle.channel
            and receipt.root_ts == root_ts
            and receipt.message_ts != root_ts
            and bool(receipt.user_id)
            and receipt.user_id == qevent.author
        )

    async def _ask(self, payload: dict[str, Any], *, deadline: float | None) -> dict[str, Any]:
        assert self._headers is not None
        try:
            async with asyncio.timeout_at(deadline):
                response = await self._client.post(self._url, json=payload, headers=self._headers)
        except TimeoutError as exc:
            raise SteeringUnavailable("factory steering request outlived its budget") from exc
        except httpx.HTTPError as exc:
            raise SteeringUnavailable(
                f"factory steering request failed: {type(exc).__name__}"
            ) from exc
        if response.status_code != 200:
            raise SteeringUnavailable(f"factory steering answered HTTP {response.status_code}")
        try:
            body = response.json()
        except ValueError as exc:
            raise SteeringUnavailable("factory steering answer is not JSON") from exc
        if not isinstance(body, dict):
            raise SteeringUnavailable("factory steering answer is not an object")
        return body


__all__ = [
    "BUDGET_EXPIRED",
    "EVIDENCE_MISSING",
    "FactorySteeringClient",
    "SteeringReply",
    "SteeringUnavailable",
    "UNCONFIRMED_TEXT",
    "UNCONFIGURED",
    "render_reply",
]
