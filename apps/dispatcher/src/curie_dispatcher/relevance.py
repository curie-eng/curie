"""The dispatcher's enumerated drop contract: which inbound Slack deliveries are
refused before they ever become a turn, and the documented reason for each.

Two rules govern this module, and both exist because #2006 found the adapter
losing real user messages in silence:

1. **Every refusal the adapter itself makes is enumerated and logged.** A
   ``DropReason`` member with a one-sentence entry in :data:`DROP_RATIONALES`,
   emitted through :func:`drop`. A bare ``return None`` on the ingest path is
   the defect this module closes, not a style preference.
2. **Denylists are closed, allowlists are open.** The filter this replaced
   dropped on *any* message ``subtype`` -- a blanket denial over a set Slack
   keeps growing, so ``file_share`` (a person uploading a file with a comment)
   and ``thread_broadcast`` (a person's thread reply) were swallowed with no log
   line at all. :data:`NON_CONTENT_SUBTYPES` names the small, closed set that is
   structurally not new user content; everything else, including subtypes Slack
   has not shipped yet, is actionable.

**What this module deliberately does not do.** It does not re-implement the
self-authored-event guard. ``slack_bolt``'s built-in ``IgnoringSelfEvents``
middleware compares the incoming identity against the authorized bot identity
and acks the envelope *before any listener runs*, so an event Curie itself
posted never reaches these lanes. That middleware is the real loop guard;
duplicating it here would add an adapter-level relevance decision that buys
nothing and would (as it did before #2006) also discard legitimate posts from
*other* apps. Bolt's admission boundary is documented in
``docs/interfaces/channel-ingress/INTERFACE.md`` rather than reproduced here.

Relevance that can be decided from the routed payload belongs at the routing
seam (``Kernel.process_event`` in the worker), not in this adapter. What stays
here is what the seam structurally cannot see: ``QueuedTurn`` carries no Slack
lane and no subtype, and ``BindingResolver.resolve`` receives only
``(kind, adapter, address)``.
"""

import logging
from collections.abc import Mapping
from enum import StrEnum
from types import MappingProxyType
from typing import Any, Literal

from .config import ThreadedBotAdmission

#: Which subscribed lane a delivery arrived on. The manifest
#: (``apps/dispatcher/slack-app-manifest.yaml``) subscribes to exactly two bot
#: events, so this is the complete set of lanes -- not an open string.
Lane = Literal["mention", "im"]


class DropReason(StrEnum):
    """Why the adapter refused an inbound delivery, as a stable log token.

    Every member is exercised end to end by ``tests/test_inbound_relevance.py``
    and carries a rationale in :data:`DROP_RATIONALES`; adding a member without
    both is a test failure, so a new adapter-level refusal cannot ship without
    a demonstration of what it refuses and why.
    """

    MALFORMED_ENVELOPE = "malformed_envelope"
    UNSUBSCRIBED_LANE = "unsubscribed_lane"
    BOT_AUTHORED_THREAD_REPLY = "bot_authored_thread_reply"
    NON_CONTENT_SUBTYPE = "non_content_subtype"
    DUPLICATE_DELIVERY = "duplicate_delivery"
    NO_ACTION_IN_PAYLOAD = "no_action_in_payload"
    EMPTY_ACTION_COMMAND = "empty_action_command"
    UNADDRESSABLE_ACTION = "unaddressable_action"
    CALLER_NOT_ALLOWED = "caller_not_allowed"
    ADMISSION_UNAVAILABLE = "admission_unavailable"


#: One documented sentence per reason. Asserted total in both directions -- a
#: reason with no rationale is exactly the undocumented drop #2006 closes, and
#: an orphan rationale means a reason was deleted without its explanation.
DROP_RATIONALES: Mapping[DropReason, str] = MappingProxyType(
    {
        DropReason.MALFORMED_ENVELOPE: (
            "The delivery omits a field the turn is minted from -- the event id on an "
            "event, the channel, the thread key, or the interaction identity on a "
            "click -- and refusing it before the idempotency claim beats raising after "
            "it, because Bolt has already acked and would swallow the exception while "
            "the claim survived for work that never happened."
        ),
        DropReason.UNSUBSCRIBED_LANE: (
            "The delivered envelope is outside the adapter's declared subscription "
            "surface -- apps/dispatcher/slack-app-manifest.yaml subscribes to "
            "app_mention and message.im only, so a message event on any other "
            "channel type cannot legitimately arrive and refusing it is envelope "
            "validation rather than a relevance judgement."
        ),
        DropReason.BOT_AUTHORED_THREAD_REPLY: (
            "Loop guard across installations: Curie's own replies and placeholders "
            "are always threaded, so admitting a bot-authored mention that carries "
            "a thread timestamp would let two Curie installations in one workspace "
            "mention-loop each other indefinitely, which Bolt's self filter cannot "
            "stop because the two bot identities differ. Only an explicitly trusted "
            "sender/channel pair may bypass this refusal."
        ),
        DropReason.NON_CONTENT_SUBTYPE: (
            "The subtype marks something other than new user content: an edit, a "
            "delete, a tombstone, a body redacted by Enterprise Key Management, or "
            "an assistant thread-start marker."
        ),
        DropReason.DUPLICATE_DELIVERY: (
            "Slack redelivered a delivery whose idempotency key was already claimed, "
            "so processing it again would post a second placeholder and mint a "
            "second turn for one message."
        ),
        DropReason.NO_ACTION_IN_PAYLOAD: (
            "A block-action payload carrying no actions names no command and "
            "addresses nothing, so there is no turn to mint from it."
        ),
        DropReason.EMPTY_ACTION_COMMAND: (
            "A clicked button carrying neither a value nor a usable action id names "
            "no command, so the turn text would be empty."
        ),
        DropReason.UNADDRESSABLE_ACTION: (
            "An App Home or modal click carries no channel and no message, so there "
            "is no thread in which a reply could be delivered."
        ),
        DropReason.CALLER_NOT_ALLOWED: (
            "The binding this delivery arrived on carries a list of who may talk to "
            "the bot, and the platform API said the caller is not on it (ADR 0175), "
            "so no placeholder is posted and no turn is minted: a polite refusal "
            "would tell a stranger the bot exists."
        ),
        DropReason.ADMISSION_UNAVAILABLE: (
            "The platform API could not answer whether the caller may talk to the "
            "bot and no usable answer was cached, so the delivery is refused rather "
            "than admitted unchecked (ADR 0175 fails closed). This is an outage "
            "signal, not a list typo: the route may carry no list at all."
        ),
    }
)


#: The closed set of message subtypes that are structurally not new user
#: content. Kept deliberately small: the manifest subscribes only to
#: ``app_mention`` and ``message.im``, so channel-lane noise (joins, leaves,
#: topic/purpose/name changes, pins) cannot reach these lanes in production and
#: does not need naming here. Everything not in this set -- including subtypes
#: Slack ships after this was written -- is admitted and left to routing.
NON_CONTENT_SUBTYPES: frozenset[str] = frozenset(
    {
        "message_changed",
        "message_deleted",
        "message_replied",
        "tombstone",
        "ekm_access_denied",
        "assistant_app_thread",
    }
)


def drop(
    log: logging.Logger,
    reason: DropReason,
    *,
    event_id: str,
    **extra: object,
) -> None:
    """Record one refusal: exactly one INFO record naming the reason and its rationale.

    Exactly one record per drop is the property the anti-silent-swallow suite
    rests on, so this must not grow a second emit. Values are rendered with
    ``%r`` so a newline or control character inside a Slack-supplied id cannot
    forge an extra log line; message bodies are never logged at all.

    Args:
        log: The dispatcher's injected logger -- the one the drop must land on.
        reason: The enumerated reason, whose value is the stable log token.
        event_id: The delivery's idempotency key, or "" when none exists yet.
        **extra: Additional non-body context (a channel type, a subtype).
    """
    details = "".join(f" {key}={value!r}" for key, value in sorted(extra.items()))
    log.info(
        "dropped inbound slack delivery %r: %s -- %s%s",
        event_id,
        reason.value,
        DROP_RATIONALES[reason],
        details,
    )


def missing_envelope_fields(required: Mapping[str, object]) -> tuple[str, ...]:
    """The names of the mint-site fields this delivery did not usably supply.

    Envelope validation, deliberately separate from :func:`classify`: it reads
    the *envelope* (``body``) as well as the event, and it must run before the
    idempotency claim, whereas ``classify`` answers "is this event content we
    should answer" from the event alone.

    A field counts as present only when it is a non-blank string. Slack's ids
    and timestamps are strings, and a blank one is no more usable than an absent
    one -- an empty event id yields a dedupe key shared by every delivery that
    omits it, and an empty thread key is a single lock across unrelated threads.

    Args:
        required: Field name -> the value the delivery supplied (or None).

    Returns:
        The unusable field names in the order given, or ``()`` when all are
        present -- an empty tuple is falsy, so callers read as ``if missing:``.
    """
    return tuple(
        name
        for name, value in required.items()
        if not isinstance(value, str) or not value.strip()
    )


def classify(
    event: dict[str, Any],
    *,
    lane: Lane,
    threaded_bot_allowlist: tuple[ThreadedBotAdmission, ...] = (),
) -> DropReason | None:
    """The reason this event must not become a turn, or None to admit it.

    Args:
        event: The Slack event body as delivered.
        lane: Which subscribed lane it arrived on. The bot-authorship rule is
            lane-specific, so this cannot be inferred from the event alone.

    Returns:
        A :class:`DropReason` when the event is refused, else None.
    """
    subtype = event.get("subtype")
    if isinstance(subtype, str) and subtype in NON_CONTENT_SUBTYPES:
        return DropReason.NON_CONTENT_SUBTYPE

    # Bot authorship is a refusal on the mention lane ONLY, and only in a
    # thread. A bot-authored mention at root is the case #2006 is about (an
    # alert app @-mentioning Curie) and must reach routing. On the DM lane bot
    # authorship is not consulted at all: incoming webhooks, Workflow Builder
    # posts and other apps all arrive as bot-authored message.im events, and our
    # own posts are already gone -- Bolt's IgnoringSelfEvents dropped them
    # before this listener ran.
    #
    # Only an exact pair from operator configuration may bypass this refusal.
    # These identities come from the Slack event, never message text or a user
    # field. Bolt's self-event middleware has already run and remains mandatory.
    if lane == "mention" and event.get("bot_id") and event.get("thread_ts"):
        if not any(
            pair.channel_id == event.get("channel") and pair.bot_id == event.get("bot_id")
            for pair in threaded_bot_allowlist
        ):
            return DropReason.BOT_AUTHORED_THREAD_REPLY

    return None
