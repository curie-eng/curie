"""Factory run steering and kickoff from Slack (#5414, #5380).

The kernel asks ``FactorySteeringClient.steer`` about every turn right after
its identity preparation, before the cron branch and before any binding,
shimmer or sandbox claim. The answer is None for every turn that is neither a
Slack turn in a factory run's Slack root nor a factory kickoff, and the
ordinary path then runs unchanged.

A Slack turn's root is mapped when ``factory_slack_threads`` names it, read
here by direct SQL like ``binding.py``. The same read says whether this exact
delivery's kickoff request (``kickoff_request_id``) already exists on the
mapped WorkItem, so one turn is handled in this order:

1. A mapped root whose kickoff request is this delivery's: a redelivered
   kickoff whose admission committed. It replays through kickoff and never
   steers (the steer route would refuse it: the new run has no pull request).
2. Any other mapped root: steering, unchanged. A mapped turn never enters
   ordinary chat. It is refused here with ``steering_evidence_missing`` when
   it carries an HTTP identity context, is a block action, or has no stored
   version 1 thread receipt that matches it (``curie_internal.slack_mention_receipt``,
   written by the native Socket Mode lane before the dedupe claim). A worker
   with no internal worker token refuses it first, with ``steering_unconfigured``
   and no API call. Otherwise the API decides at ``POST
   /v1/internal/factory-slack/steer`` and its outcome is the reply.
3. An unmapped root is a kickoff candidate only when it is not a block
   action, its channel is a public or private channel (``C`` or ``G``; a
   direct message stays ordinary), its queued text holds an issue-shaped link
   (``curie_internal.issue_links``), and the receiving Slack binding's agent
   also holds a ``github`` surface. Anything else is None. A worker with no
   internal worker token returns None for a candidate too: whether the
   installation runs the factory is the API's decision, so ordinary chat runs
   as before. A candidate whose link appears only in quoted thread context,
   not in its receipt's body, is None as well.

A kickoff asks ``POST /v1/internal/factory-slack/kickoff``. It is refused
locally with ``kickoff_evidence_missing`` when it carries an HTTP identity
context or has no stored receipt (version 1 or a version 2 declared driver)
that matches it, with ``kickoff_budget_expired`` when the delivery's budget is
spent, and, for a replay only, with ``kickoff_unconfigured`` when this worker
has no token. The API answer ``not_factory`` (the factory is off, or no
configured GitHub tracker serves the link's repository for this agent) makes
an unmapped candidate None: the intent marker this attempt recorded is
cleared first, so a later failure of the ordinary turn still gets the
generic notice. Every other answer is the reply in the thread.

The client is always built when the worker has its database engine, so no
switch lets a mapped root fall through to ordinary chat. A transport failure,
a 404, a 5xx or a malformed answer raises ``SteeringUnavailable``: the delivery
stays pending under the existing lease and delivery cap, so an outage fails
closed and is bounded.

The kernel passes the delivery's remaining budget. A decision whose budget is
at or below the kernel's minimum attempt budget is refused before any API
call; any other budget bounds the API call, and a call that outlives it raises
``SteeringUnavailable`` (admission is idempotent by its deterministic request
id, so a later delivery admits at most once).
"""

from __future__ import annotations

import asyncio
import logging
import re
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any, Final

import httpx
from aci_protocol import QueuedTurn, ReplyHandle, TurnSource
from aci_protocol.turn import route_identity
from curie_internal.issue_links import find_issue_links
from curie_internal.slack_mention_receipt import (
    MalformedMentionReceiptError,
    SlackMentionReceipt,
    kickoff_request_id,
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
KICKOFF_PATH: Final = "/v1/internal/factory-slack/kickoff"
EVIDENCE_MISSING: Final = "steering_evidence_missing"
UNCONFIGURED: Final = "steering_unconfigured"
BUDGET_EXPIRED: Final = "steering_budget_expired"
KICKOFF_EVIDENCE_MISSING: Final = "kickoff_evidence_missing"
KICKOFF_UNCONFIGURED: Final = "kickoff_unconfigured"
KICKOFF_BUDGET_EXPIRED: Final = "kickoff_budget_expired"
NOT_MAPPED: Final = "not_mapped"
NOT_FACTORY: Final = "not_factory"
ADMITTED: Final = "admitted"
# The dispatcher's block action delivery ids start with this
# (``handlers.process_action``); a click is never steering or kickoff evidence.
_ACTION_EVENT_PREFIX: Final = "action-"
_SLACK_KIND: Final = "slack"
# A public or private channel id. Direct messages (``D``) never kick off.
_KICKOFF_CHANNEL: Final = re.compile(r"^[CG][A-Z0-9]+$")
_TEXT_MAX: Final = 300
# Ways a database read can fail before or inside the driver: a refused or
# timed out connection can escape the driver unwrapped.
_READ_ERRORS: Final = (SQLAlchemyError, OSError, TimeoutError)

_HEADLINES: Final = {
    ADMITTED: "Revision queued for this pull request.",
    EVIDENCE_MISSING: "This factory run cannot be steered from this message.",
    UNCONFIGURED: "Steering is not configured on this worker, so it did not ask for a decision.",
    BUDGET_EXPIRED: (
        "This message ran out of time before a decision was confirmed. "
        "The run thread shows any new revision."
    ),
    # Answered only when the API disagrees with this worker's mapped read,
    # which is refused here as steering_evidence_missing instead.
    NOT_MAPPED: "This thread is not a factory run's thread.",
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

_KICKOFF_HEADLINES: Final = {
    ADMITTED: "Factory run started for this issue.",
    KICKOFF_EVIDENCE_MISSING: "A factory run cannot be started from this message.",
    KICKOFF_UNCONFIGURED: (
        "Starting factory runs is not configured on this worker, so it did not ask for a decision."
    ),
    KICKOFF_BUDGET_EXPIRED: (
        "This message ran out of time before a decision was confirmed. "
        "This thread shows the run if one started."
    ),
    # Never shown for an unmapped root, which takes the ordinary path on this
    # answer; a replay that meets it is refused as kickoff_evidence_missing.
    NOT_FACTORY: "This installation does not run factory work for this repository.",
    "receipt_mismatch": "A factory run cannot be started from this message.",
    "test_driver_refused": "This test driver is not declared for this installation and channel.",
    "binding_missing": "This agent is not bound to this channel as the identity you mentioned.",
    "binding_ambiguous": "This agent has more than one binding in this channel.",
    "link_missing": "This message links no issue.",
    "link_multiple": "This message links more than one issue. Link exactly one.",
    "link_malformed": "This link is not a link to an issue.",
    "link_host_unsupported": "This issue is not on the code host configured here.",
    "link_pull_request": "This link names a pull request, not an issue.",
    "sender_unverified": "Your Slack account is not verified for this workspace.",
    "enrollment_inactive": "You have no active personal subscription enrolled.",
    "issue_inaccessible": "This issue cannot be read here.",
    "issue_closed": "This issue is closed.",
    "publication_exists": "This issue already has a factory pull request.",
    "base_refused": "No base branch could be chosen for this issue.",
    "thread_conflict": "This issue or this thread already belongs to another factory run.",
    "authority_changed": "Your access changed while this was decided, so this message was refused.",
    "admission_refused": "This message was refused as a factory run.",
}
_KICKOFF_FALLBACK_HEADLINE: Final = "This message was refused as a factory run."
# As for steering: a refusal may follow an earlier attempt of this delivery
# whose answer was lost, so it never says no run started.
_KICKOFF_EARLIER_ATTEMPTS: Final = (
    "Any earlier attempt of this message is unconfirmed here; "
    "this thread shows the run if one started."
)
_KICKOFF_PROGRESS: Final = "Progress for this run is posted in this thread."


UNCONFIRMED_TEXT: Final = (
    "I could not confirm this request yet. Please do not send it again; "
    "the run thread shows any new revision."
)
KICKOFF_UNCONFIRMED_TEXT: Final = (
    "I could not confirm this request yet. Please do not send it again; "
    "this thread shows the run if one started."
)


class SteeringUnavailable(RuntimeError):
    """The steering or kickoff decision could not be read; the delivery must stay pending.

    Raised only where steering or kickoff may own the delivery, so its retry
    never invites a resend: a resend is a new delivery id, so a new request,
    and an earlier attempt of this delivery may already have admitted with its
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
        """Whether the decision queued (or had already queued) the request."""
        return self.outcome == ADMITTED


@dataclass(frozen=True)
class MappedRoot:
    """A factory run's Slack root, and whether this delivery's kickoff request is on it."""

    work_item_id: uuid.UUID
    kickoff_replay: bool


@dataclass(frozen=True)
class _Turn:
    """The Slack facts one decision reads, taken once from the queued turn."""

    qevent: QueuedTurn
    handle: ReplyHandle
    identity: str
    root_ts: str


def _plain(value: str) -> str:
    """A bounded value with Slack's three control characters escaped."""

    bounded = value if len(value) <= _TEXT_MAX else value[: _TEXT_MAX - 1] + "..."
    return bounded.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def _detail(outcome: str, reason: str | None) -> str:
    detail = f"Outcome: {_plain(outcome)}"
    if reason:
        detail += f" ({_plain(reason)})"
    return detail


def render_reply(
    outcome: str,
    *,
    reason: str | None = None,
    pull_request_url: str | None = None,
    replayed: bool = False,
) -> str:
    """The short plain steering reply for one outcome; it always names the outcome code."""

    headline = _HEADLINES.get(outcome, _FALLBACK_HEADLINE)
    if outcome == ADMITTED and replayed:
        headline = "This revision was already queued."
    lines = [headline]
    if outcome == ADMITTED and pull_request_url:
        lines.append(f"Pull request: {_plain(pull_request_url)}")
    if outcome != ADMITTED and outcome != BUDGET_EXPIRED:
        lines.append(_EARLIER_ATTEMPTS)
    lines.append(_detail(outcome, reason))
    return "\n".join(lines)


def render_kickoff_reply(
    outcome: str,
    *,
    reason: str | None = None,
    issue_url: str | None = None,
    replayed: bool = False,
) -> str:
    """The short plain kickoff reply for one outcome; it always names the outcome code.

    An admitted reply names the issue and says progress posts in this thread.
    """

    headline = _KICKOFF_HEADLINES.get(outcome, _KICKOFF_FALLBACK_HEADLINE)
    if outcome == ADMITTED and replayed:
        headline = "This factory run was already started."
    lines = [headline]
    if outcome == ADMITTED:
        if issue_url:
            lines.append(f"Issue: {_plain(issue_url)}")
        lines.append(_KICKOFF_PROGRESS)
    elif outcome != KICKOFF_BUDGET_EXPIRED:
        lines.append(_KICKOFF_EARLIER_ATTEMPTS)
    lines.append(_detail(outcome, reason))
    return "\n".join(lines)


def _refused(outcome: str = EVIDENCE_MISSING) -> SteeringReply:
    return SteeringReply(outcome=outcome, text=render_reply(outcome))


def _kickoff_refused(outcome: str = KICKOFF_EVIDENCE_MISSING) -> SteeringReply:
    return SteeringReply(outcome=outcome, text=render_kickoff_reply(outcome))


def _optional_text(body: dict[str, Any], key: str) -> str | None:
    value = body.get(key)
    if value is None:
        return None
    if not isinstance(value, str):
        raise SteeringUnavailable(f"factory Slack answer field {key} is not a string")
    return value


class FactorySteeringClient:
    """Answers whether a turn steers or starts a factory run, and with what reply."""

    def __init__(
        self,
        *,
        engine: AsyncEngine,
        schema: str,
        redis: AsyncRedis,
        api_base_url: str,
        worker_token: str | None,
        client: httpx.AsyncClient,
        intent_key: Callable[[str], str],
    ) -> None:
        """``intent_key`` names the worker's steering-sent marker of a delivery
        (``WorkerConfig.steering_sent_key``), the key ``record_intent`` writes
        in ``redis``. A kickoff answered ``not_factory`` deletes it, so the
        ordinary turn that follows is not read as a possibly admitted one.
        """

        if not _SAFE_SCHEMA.fullmatch(schema):
            raise ValueError("factory steering database schema is invalid")
        self._engine = engine
        self._threads = f'"{schema}".factory_slack_threads'
        self._requests = f'"{schema}".execution_requests'
        self._channels = f'"{schema}".agent_channels'
        self._redis = redis
        base = api_base_url.rstrip("/")
        self._steer_url = base + STEER_PATH
        self._kickoff_url = base + KICKOFF_PATH
        # No token: a mapped root is still intercepted, then refused locally;
        # an unmapped kickoff candidate takes the ordinary path.
        self._headers = {"X-Curie-Worker-Token": worker_token} if worker_token else None
        self._client = client
        self._intent_key = intent_key

    async def _mapping(
        self, identity: str, channel: str, root_ts: str, kickoff_request: uuid.UUID
    ) -> MappedRoot | None:
        """This Slack root's factory run, and whether ``kickoff_request`` is on it."""

        async with self._engine.connect() as connection:
            row = (
                await connection.execute(
                    text(
                        f"""
                        SELECT t.work_item_id,
                               EXISTS (
                                   SELECT 1
                                     FROM {self._requests} r
                                    WHERE r.id = :request
                                      AND r.work_item_id = t.work_item_id
                               ) AS kickoff_replay
                          FROM {self._threads} t
                         WHERE t.channel_identity = :identity
                           AND t.channel = :channel
                           AND t.root_ts = :root_ts
                        """
                    ),
                    {
                        "identity": identity,
                        "channel": channel,
                        "root_ts": root_ts,
                        "request": kickoff_request,
                    },
                )
            ).first()
        if row is None:
            return None
        return MappedRoot(
            work_item_id=uuid.UUID(str(row.work_item_id)), kickoff_replay=bool(row.kickoff_replay)
        )

    async def _factory_agent(self, identity: str, channel: str) -> bool:
        """Whether an agent bound here as ``identity`` also holds a GitHub surface.

        The GitHub surface is the data fact that makes an agent serve a
        repository for the factory (ADR 0214 decision 1). The API decides which
        binding and repository the kickoff names; this only selects candidates.
        """

        async with self._engine.connect() as connection:
            found = (
                await connection.execute(
                    text(
                        f"""
                        SELECT EXISTS (
                            SELECT 1
                              FROM {self._channels} s
                             WHERE s.kind = 'slack'
                               AND s.address = :channel
                               AND CAST(s.adapter AS text) = :identity
                               AND EXISTS (
                                   SELECT 1
                                     FROM {self._channels} g
                                    WHERE g.agent_id = s.agent_id
                                      AND g.kind = 'github'
                               )
                        )
                        """
                    ),
                    {"identity": identity, "channel": channel},
                )
            ).scalar_one()
        return bool(found)

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
        turn = _Turn(qevent, handle, identity, root_ts)
        request = kickoff_request_id(identity, qevent.event_id)
        try:
            mapping = await self._mapping(identity, handle.channel, root_ts, request)
        except _READ_ERRORS as exc:
            # Unknown, so possibly a mapped root: never ordinary chat.
            raise SteeringUnavailable("factory run mapping could not be read") from exc
        if mapping is None:
            return await self._unmapped(turn, deadline=deadline, record_intent=record_intent)
        if mapping.kickoff_replay:
            # This delivery's own kickoff admitted this run: replay it, never steer.
            return await self._kickoff(
                turn, mapped=True, deadline=deadline, record_intent=record_intent
            )
        return await self._steer(turn, deadline=deadline, record_intent=record_intent)

    async def _steer(
        self,
        turn: _Turn,
        *,
        deadline: float | None,
        record_intent: Callable[[], Awaitable[None]],
    ) -> SteeringReply:
        """A turn in a mapped root that is not its own kickoff's replay."""

        qevent = turn.qevent
        refusal = self._precheck(qevent, deadline, BUDGET_EXPIRED, UNCONFIGURED)
        if refusal is not None:
            return _refused(refusal)
        # Mapped: this turn never enters ordinary chat. Native Socket Mode
        # evidence is the only evidence; an HTTP selected turn is refused
        # before any local link is consulted (plan R1.3).
        if _http_selected():
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
        if receipt is None or not self._matches(receipt, turn):
            logger.info("factory steering refused %s: no matching receipt", qevent.event_id)
            return _refused()

        body = await self._decide(
            turn,
            receipt,
            url=self._steer_url,
            deadline=deadline,
            record_intent=record_intent,
            pending_text=UNCONFIRMED_TEXT,
        )
        outcome, reason, request_id, replayed = _decision(body, UNCONFIRMED_TEXT)
        try:
            pull_request_url = _optional_text(body, "pull_request_url")
        except SteeringUnavailable as exc:
            raise SteeringUnavailable(str(exc), pending_text=UNCONFIRMED_TEXT) from exc
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

    async def _unmapped(
        self,
        turn: _Turn,
        *,
        deadline: float | None,
        record_intent: Callable[[], Awaitable[None]],
    ) -> SteeringReply | None:
        """A turn in an unmapped root: None unless it is a kickoff candidate."""

        qevent = turn.qevent
        if qevent.event_id.startswith(_ACTION_EVENT_PREFIX):
            return None
        if not _KICKOFF_CHANNEL.fullmatch(turn.handle.channel):
            return None
        if not find_issue_links(qevent.text):
            return None
        try:
            factory = await self._factory_agent(turn.identity, turn.handle.channel)
        except _READ_ERRORS as exc:
            # Unknown, so possibly a kickoff: never ordinary chat.
            raise SteeringUnavailable("factory agent binding could not be read") from exc
        if not factory:
            return None
        if self._headers is None:
            # Only the API knows whether this installation runs the factory,
            # so a worker that cannot ask leaves the turn to ordinary chat.
            logger.info("factory kickoff skipped %s: no internal worker token", qevent.event_id)
            return None
        return await self._kickoff(
            turn, mapped=False, deadline=deadline, record_intent=record_intent
        )

    async def _kickoff(
        self,
        turn: _Turn,
        *,
        mapped: bool,
        deadline: float | None,
        record_intent: Callable[[], Awaitable[None]],
    ) -> SteeringReply | None:
        """Ask the kickoff route; ``mapped`` is a replay of this delivery's own kickoff."""

        qevent = turn.qevent
        if mapped:
            refusal = self._precheck(qevent, deadline, KICKOFF_BUDGET_EXPIRED, KICKOFF_UNCONFIGURED)
            if refusal is not None:
                return _kickoff_refused(refusal)
        if _http_selected():
            logger.info("factory kickoff refused %s: HTTP identity carry", qevent.event_id)
            return _kickoff_refused()
        try:
            receipt = await read_receipt(self._redis, qevent.event_id)
        except MalformedMentionReceiptError:
            logger.warning("factory kickoff refused %s: malformed receipt", qevent.event_id)
            return _kickoff_refused()
        except RedisError as exc:
            raise SteeringUnavailable("mention receipt could not be read") from exc
        if receipt is None or not self._kickoff_matches(receipt, turn):
            logger.info("factory kickoff refused %s: no matching receipt", qevent.event_id)
            return _kickoff_refused()
        if not mapped:
            if not find_issue_links(receipt.body):
                # The link came only from quoted thread context.
                return None
            refusal = self._precheck(qevent, deadline, KICKOFF_BUDGET_EXPIRED, None)
            if refusal is not None:
                return _kickoff_refused(refusal)

        body = await self._decide(
            turn,
            receipt,
            url=self._kickoff_url,
            deadline=deadline,
            record_intent=record_intent,
            pending_text=KICKOFF_UNCONFIRMED_TEXT,
        )
        outcome, reason, request_id, replayed = _decision(body, KICKOFF_UNCONFIRMED_TEXT)
        try:
            issue_url = _optional_text(body, "issue_url")
        except SteeringUnavailable as exc:
            raise SteeringUnavailable(str(exc), pending_text=KICKOFF_UNCONFIRMED_TEXT) from exc
        if outcome == NOT_FACTORY:
            if mapped:
                # This delivery's kickoff request is on this run, so it did
                # admit: a disagreement is never ordinary chat, and the
                # intent stays recorded.
                logger.warning("factory kickoff %s: API reports no factory", qevent.event_id)
                return _kickoff_refused()
            # Nothing was admitted for this delivery (a committed request would
            # have replayed first), so the recorded intent is withdrawn before
            # the ordinary path runs.
            try:
                await self._redis.delete(self._intent_key(qevent.event_id))
            except RedisError as exc:
                raise SteeringUnavailable("kickoff intent could not be cleared") from exc
            logger.info("factory kickoff %s: not a factory turn", qevent.event_id)
            return None
        logger.info("factory kickoff %s outcome=%s", qevent.event_id, outcome)
        return SteeringReply(
            outcome=outcome,
            text=render_kickoff_reply(
                outcome, reason=reason, issue_url=issue_url, replayed=replayed
            ),
            request_id=request_id,
        )

    def _precheck(
        self,
        qevent: QueuedTurn,
        deadline: float | None,
        expired: str,
        unconfigured: str | None,
    ) -> str | None:
        """The local refusal for a spent budget or, when named, a missing token."""

        if (
            deadline is not None
            and deadline - asyncio.get_running_loop().time()
            <= kernel_constants._MIN_ATTEMPT_BUDGET_S
        ):
            # Too little budget is left to decide; never ordinary chat.
            logger.warning("factory Slack decision refused %s: budget expired", qevent.event_id)
            return expired
        if unconfigured is not None and self._headers is None:
            # This worker cannot ask the API: refused, never ordinary chat.
            logger.warning("factory Slack decision refused %s: no worker token", qevent.event_id)
            return unconfigured
        return None

    async def _decide(
        self,
        turn: _Turn,
        receipt: SlackMentionReceipt,
        *,
        url: str,
        deadline: float | None,
        record_intent: Callable[[], Awaitable[None]],
        pending_text: str,
    ) -> dict[str, Any]:
        """Record the intent, then ask ``url``; the answer body, or raise."""

        headers = self._headers
        if headers is None:
            raise SteeringUnavailable("factory Slack decision has no worker token")
        try:
            await record_intent()
        except Exception as exc:  # noqa: BLE001 - unrecorded intent never sends
            raise SteeringUnavailable(
                f"steering intent could not be recorded: {type(exc).__name__}"
            ) from exc
        try:
            return await self._ask(
                url,
                {
                    "delivery_id": turn.qevent.event_id,
                    "channel_identity": turn.identity,
                    "channel": turn.handle.channel,
                    "root_ts": turn.root_ts,
                    "author": turn.qevent.author,
                    "receipt": receipt.model_dump(mode="json"),
                },
                headers=headers,
                deadline=deadline,
            )
        except SteeringUnavailable as exc:
            # Sent, so the admission may have committed with its answer lost.
            raise SteeringUnavailable(str(exc), pending_text=pending_text) from exc

    @staticmethod
    def _matches(receipt: SlackMentionReceipt, turn: _Turn) -> bool:
        """A person's thread reply receipt for exactly this steering turn.

        Version 2 (a declared test driver) and root receipts never steer.
        """

        return (
            receipt.version == 1
            and receipt.delivery_id == turn.qevent.event_id
            and receipt.channel_identity == turn.identity
            and receipt.channel == turn.handle.channel
            and receipt.root_ts == turn.root_ts
            and receipt.message_ts != turn.root_ts
            and bool(receipt.user_id)
            and receipt.user_id == turn.qevent.author
        )

    @staticmethod
    def _kickoff_matches(receipt: SlackMentionReceipt, turn: _Turn) -> bool:
        """A receipt (a person's, or a declared driver's) for exactly this kickoff turn.

        At root or in a thread: the API rechecks the root rule and the driver
        declaration.
        """

        return (
            receipt.delivery_id == turn.qevent.event_id
            and receipt.channel_identity == turn.identity
            and receipt.channel == turn.handle.channel
            and receipt.root_ts == turn.root_ts
            and bool(receipt.user_id)
            and receipt.user_id == turn.qevent.author
        )

    async def _ask(
        self,
        url: str,
        payload: dict[str, Any],
        *,
        headers: dict[str, str],
        deadline: float | None,
    ) -> dict[str, Any]:
        try:
            async with asyncio.timeout_at(deadline):
                response = await self._client.post(url, json=payload, headers=headers)
        except TimeoutError as exc:
            raise SteeringUnavailable("factory Slack request outlived its budget") from exc
        except httpx.HTTPError as exc:
            raise SteeringUnavailable(
                f"factory Slack request failed: {type(exc).__name__}"
            ) from exc
        if response.status_code != 200:
            raise SteeringUnavailable(
                f"factory Slack decision answered HTTP {response.status_code}"
            )
        try:
            body = response.json()
        except ValueError as exc:
            raise SteeringUnavailable("factory Slack answer is not JSON") from exc
        if not isinstance(body, dict):
            raise SteeringUnavailable("factory Slack answer is not an object")
        return body


def _http_selected() -> bool:
    carry = HTTP_TURN_CARRY.get()
    return carry is not None and carry.context is not None


def _decision(body: dict[str, Any], pending_text: str) -> tuple[str, str | None, str | None, bool]:
    """The outcome, reason, request id and replay flag of a sent decision's answer."""

    try:
        outcome = body.get("outcome")
        if not isinstance(outcome, str) or not outcome:
            raise SteeringUnavailable("factory Slack answer has no outcome")
        reason = _optional_text(body, "reason")
        request_id = _optional_text(body, "request_id")
    except SteeringUnavailable as exc:
        raise SteeringUnavailable(str(exc), pending_text=pending_text) from exc
    return outcome, reason, request_id, body.get("replayed") is True


__all__ = [
    "BUDGET_EXPIRED",
    "EVIDENCE_MISSING",
    "KICKOFF_BUDGET_EXPIRED",
    "KICKOFF_EVIDENCE_MISSING",
    "KICKOFF_PATH",
    "KICKOFF_UNCONFIGURED",
    "KICKOFF_UNCONFIRMED_TEXT",
    "FactorySteeringClient",
    "MappedRoot",
    "STEER_PATH",
    "SteeringReply",
    "SteeringUnavailable",
    "UNCONFIRMED_TEXT",
    "UNCONFIGURED",
    "render_kickoff_reply",
    "render_reply",
]
