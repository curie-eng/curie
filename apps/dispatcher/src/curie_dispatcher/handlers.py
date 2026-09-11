"""Slack event handling: the fast-ack path that posts a placeholder and enqueues.

The Socket Mode transport acks the envelope in under three seconds on its own
(the handler sends the ack before dispatching to these listeners). These handlers
own the rest of the lifecycle step: dedupe the delivery, post an in-thread
placeholder reply, and enqueue the normalized job for the worker.

Two event types feed the same processing path, with lane-specific admission
(#2006). Every refusal these listeners make is an enumerated
``relevance.DropReason`` emitted through ``relevance.drop`` -- no drop of ours is
silent, which is the defect that ticket closes.

  - ``app_mention``: the bot was @-mentioned. A human mention is always
    processed, at root or in a thread. A *bot*-authored mention is processed
    at root; one carrying ``thread_ts`` requires an exact sender/channel pair
    in ``CURIE_SLACK_THREADED_BOT_ALLOWLIST`` or is refused as
    ``BOT_AUTHORED_THREAD_REPLY``, because Curie's own replies are always
    threaded and two installations in one workspace could otherwise mention-loop
    each other -- a case Bolt's self filter cannot see, since the two bot
    identities differ.
  - ``message``: only the direct-message lane (``channel_type == "im"``) is
    processed. That is envelope validation, not a relevance judgement:
    ``apps/dispatcher/slack-app-manifest.yaml`` subscribes to ``app_mention``
    and ``message.im`` only, so any other channel type is outside the declared
    subscription surface and is refused as ``UNSUBSCRIBED_LANE``. Bot authorship
    is NOT consulted on this lane -- incoming webhooks, Workflow Builder posts
    and other apps all reach us as bot-authored ``message.im`` events, and our
    own posts are already gone: Bolt's ``IgnoringSelfEvents`` middleware drops
    self-authored events before any listener here runs.

On both lanes the only content filter is the closed ``NON_CONTENT_SUBTYPES``
denylist; unknown and future subtypes are actionable. Refusals made *above*
these listeners by Bolt's own middleware (self events, authorization, listener
matching) are documented in ``docs/interfaces/channel-ingress/INTERFACE.md``
rather than re-implemented here.

We use the dispatcher's own ``WebClient`` (built from the bot token) rather than
Bolt's per-request injected client so the Web API surface is a single, mockable
seam. Routing, retries, and run orchestration are the worker's job (F1), not the
dispatcher's.
"""

import logging
import re
from collections.abc import Callable
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from aci_protocol import Attachment, QueuedTurn, ReplyHandle, TurnSource
from curie_telemetry import operation_span
from opentelemetry.trace import SpanKind
from slack_bolt import App
from slack_sdk.errors import SlackApiError
from slack_sdk.web import WebClient

from .approval_actions import (
    APPROVE_ACTION_ID,
    APPROVE_NOTE_ACTION_ID,
    NOTE_MODAL_CALLBACK_ID,
    REJECT_ACTION_ID,
    REJECT_NOTE_ACTION_ID,
    ApprovalResolveClient,
    build_resolver,
    is_approval_action,
    open_note_dialog,
    process_approval_action,
    render_note_submission,
    resolve_note_submission,
)
from .config import DispatcherConfig
from .inbound_attachments import derive_attachments
from .inbound_text import derive_text
from .queue import claim_event, enqueue, release_event
from .relevance import DropReason, Lane, classify, drop, missing_envelope_fields

if TYPE_CHECKING:
    from redis import Redis

Clock = Callable[[], str]


def _utc_now_iso() -> str:
    return datetime.now(UTC).isoformat()


def _strip_self_mention(text: str, bot_user_id: str | None) -> str:
    """Remove every mention of THIS bot from an event's raw ``text``.

    Slack delivers ``app_mention``/``message`` events with the mention token
    still embedded verbatim -- ``<@BOT_USER_ID>``, optionally followed by a
    ``|display name`` label, and (since the composer always inserts one after
    an inline mention) trailing whitespace. Left unstripped, a mention-only
    message -- ``@Squawk`` and nothing else -- reaches the worker as a
    NON-empty string, the mention markup itself, rather than the empty text
    the sender actually meant. A model-backed agent shrugs this off as prompt
    noise, which is exactly why it went unnoticed; an agent whose behavior
    branches on emptiness (a stack's push-vs-pop, #1525) pushes the literal
    markup instead of popping. ``bot_user_id`` comes from Bolt's own
    ``context["bot_user_id"]`` (resolved from the ``authorize`` result per
    request), never re-derived here.
    """
    if not bot_user_id:
        return text
    return re.sub(rf"<@{re.escape(bot_user_id)}(?:\|[^>]*)?>\s*", "", text).strip()


def _is_unambiguous_rejection(error: SlackApiError) -> bool:
    """True when Slack itself answered this call with an error code.

    That answer is the only evidence we get that NOTHING was posted: Slack
    received the request, refused it by name (``ratelimited``,
    ``channel_not_found``, ``invalid_auth``, ...) and did not deliver a message.
    A transport failure carries no such answer -- the request may well have been
    accepted before the connection died.

    ``fatal_error`` is excluded on Slack's own word: ``chat.postMessage``
    documents that when it is returned "some aspect of the operation succeeded
    before the error was raised", so it is an ambiguous outcome wearing an error
    code, not a refusal.
    """
    response = getattr(error, "response", None)
    code = getattr(response, "get", lambda _key: None)("error") if response is not None else None
    return isinstance(code, str) and bool(code) and code != "fatal_error"


def _post_placeholder(
    *,
    web_client: WebClient,
    redis_client: "Redis",
    config: DispatcherConfig,
    log: logging.Logger,
    slack_event_id: str,
    channel: str,
    thread_ts: str,
) -> Any:
    """Post the in-thread placeholder, releasing the dedupe claim if it fails (#2006).

    ``claim_event`` runs before this call, and Bolt acks an Events API envelope
    before running the listener body and then catches the body's exception in its
    executor. So a raising ``chat_postMessage`` used to leave the key claimed with
    no work done: any later delivery of that event id hit the claim and was
    refused, and the message was lost for good with only a framework log line --
    exactly the silent-loss class this ticket closes.

    HONEST BOUND on what this buys. Because Bolt already acked, a Slack
    redelivery of the failed event is rare; it is not the normal consequence of
    the body raising. Releasing restores idempotency *correctness* -- the adapter
    stops holding a claim for work it never did, so a retry, a replay or an
    operator re-send can still be processed. It does not guarantee that every
    failed placeholder is recovered.

    THE ASYMMETRY IS DELIBERATE, do not "fix" it for consistency: only a failure
    *before* the placeholder releases. Once the placeholder is in the channel
    something user-visible has happened, so a failing ``enqueue`` after it keeps
    the claim -- releasing there would let a redelivery post a SECOND placeholder
    into the same thread, trading an invisible failure for a visible, confusing
    one. Before the placeholder nothing user-visible has occurred, so releasing
    is free.

    ONLY AN UNAMBIGUOUS REFUSAL RELEASES. "The call raised" is not the same
    claim as "nothing was posted". A ``SlackApiError`` carrying Slack's own
    error code means Slack answered and refused, so no placeholder exists and a
    later retry is clean -- release. Everything else (a timeout, a dropped
    connection, ``SlackRequestError``, ``fatal_error``, an exception type we do
    not recognise) may have failed *after* Slack accepted the post, so a
    placeholder may already be sitting in the thread; releasing there would let
    a replay post a SECOND one, which is precisely the duplicate the
    claim-before-placeholder ordering exists to prevent. Both outcomes are
    imperfect and we pick the quieter one: keep the claim, and log at ERROR that
    it is being kept deliberately so the retained key is never mistaken for the
    silent-loss bug this function fixes. Either way the exception is re-raised.
    """
    try:
        return web_client.chat_postMessage(
            channel=channel,
            thread_ts=thread_ts,
            text=config.placeholder_text,
        )
    except BaseException as error:
        if isinstance(error, SlackApiError) and _is_unambiguous_rejection(error):
            release_event(redis_client, config, slack_event_id)
            raise
        log.error(
            "placeholder for slack delivery %r failed with an ambiguous outcome (%s); "
            "keeping the idempotency claim because a placeholder may already be in the "
            "thread and a retry could post a second one",
            slack_event_id,
            type(error).__name__,
        )
        raise


def _mint_turn(
    *,
    web_client: WebClient,
    redis_client: "Redis",
    config: DispatcherConfig,
    log: logging.Logger,
    clock: Clock,
    slack_event_id: str,
    delivery_kind: str,
    author: str,
    text: str,
    attachments: list[Attachment],
    channel: str,
    thread_ts: str,
) -> str:
    """Post the placeholder, enqueue the turn, and return its Stream id.

    THE SHARED TAIL OF BOTH MINT SITES, held in one place so they cannot drift.
    ``process_event`` and ``process_action`` differ in how they validate an
    envelope, judge relevance, take the dedupe claim and derive their text; from
    the placeholder onward they owe the queue an identical ``QueuedTurn``.
    Fixing one mint site and not its twin is this repo's dominant drift shape,
    so the invariants below (#1312's ordering rule, ADR-0096 D4.4's literal
    ``kind`` and explicit ``adapter``) are asserted here once rather than once
    per lane, and a field added at the next protocol bump has one site to touch.

    The caller MUST have taken the dedupe claim already: ``_post_placeholder``
    owns the release-only-on-an-unambiguous-refusal rule (#2006) and is called
    from here, so the claim -> placeholder -> ``XADD`` ordering and the release
    asymmetry are the same on both lanes by construction.

    ``delivery_kind`` names what arrived ("slack event", "block action") for the
    enqueue log line -- the one thing that still differs downstream of here.

    ``attachments`` is a required parameter with no default even though the model
    defaults it to the empty list (#2567): a lane that carries no files says so
    at the call site, for the same reason ``source`` and ``adapter`` are stated
    rather than defaulted. A silent default here is how the next lane to grow
    files would quietly keep dropping them.
    """
    placeholder = _post_placeholder(
        web_client=web_client,
        redis_client=redis_client,
        config=config,
        log=log,
        slack_event_id=slack_event_id,
        channel=channel,
        thread_ts=thread_ts,
    )
    placeholder_ts = placeholder["ts"]

    # Nothing else goes between the placeholder and the XADD below. The Slack
    # assistant-thread status ("shimmer") used to sit right here, and it was the
    # wrong side of the durable write (#1312): a best-effort cosmetic call, whose
    # own failures are swallowed at debug, gating the only moment this turn
    # becomes recoverable. slack_sdk's retry handler puts the worst case near
    # 4.5s (see app.py's timeout constant), and Bolt has five shared listener
    # workers, so a handful of slow status calls could stall ingestion with no
    # visible explanation. The worker raises and lowers the shimmer now, which
    # also puts set and clear in one process instead of racing across two.
    queued = QueuedTurn(
        event_id=slack_event_id,
        conversation_id=thread_ts,
        author=author,
        text=text,
        # A person spoke, or a person clicked a button -- still a person, still
        # steerable -- so this turn MAY steer a live one. Stated rather than
        # left to the model default for the same reason `kind` is: a producer
        # that does not say what it is produces turns nobody can audit.
        source=TurnSource.SLACK,
        # The literal "slack" is this dispatcher stating what it is; it never
        # comes from config, because a Slack Socket Mode dispatcher that could
        # claim another kind is a misrouting vector. `adapter=None` is explicit
        # rather than defaulted so a reader sees that Slack's route is the
        # worker's configured origin, not an oversight (ADR-0096 D4.4). Same
        # literal, same reason, on both lanes.
        reply_handle=ReplyHandle(
            kind="slack", channel=channel, placeholder=placeholder_ts, adapter=None
        ),
        received_at=clock(),
        # Refs only, and derived BESIDE the text rather than folded into it: see
        # `inbound_attachments`. The empty list means "this delivery reported no
        # files I can reference", never "attachments are unsupported here".
        attachments=attachments,
    )
    stream_id = enqueue(redis_client, config, queued)
    log.info("enqueued %s %s as stream entry %s", delivery_kind, slack_event_id, stream_id)
    return stream_id


def process_event(
    *,
    body: dict[str, Any],
    event: dict[str, Any],
    lane: Lane,
    web_client: WebClient,
    redis_client: "Redis",
    config: DispatcherConfig,
    bot_user_id: str | None = None,
    clock: Clock = _utc_now_iso,
    logger: logging.Logger | None = None,
) -> str | None:
    """Dedupe, post the placeholder, and enqueue one Slack event.

    ``lane`` says which subscribed lane the delivery arrived on; the
    bot-authorship rule is lane-specific (see ``relevance.classify``) and cannot
    be inferred from the event body alone.

    Returns the Valkey Stream id when a job was enqueued, or None when the event
    was refused. Every refusal is logged with its enumerated ``DropReason``.
    """
    log = logger or logging.getLogger(__name__)

    # ENVELOPE VALIDATION FIRST, AND BEFORE THE CLAIM (#2006). Everything the
    # mint site needs is read with `.get` and checked here, because indexing it
    # later is the silent-loss shape this ticket closes: `body["event_id"]`
    # raised before any claim (Bolt acked, swallowed the exception, message
    # gone), and `event["ts"]`/`event["channel"]` raised *after* `claim_event`
    # succeeded -- so the claim outlived a turn that never existed and Slack's
    # redelivery was refused as an already-seen delivery. Refusing through the
    # normal `drop` path leaves the key free and the refusal visible.
    #
    # Reply in-thread: for a root message the thread key is its own ts.
    slack_event_id = str(body.get("event_id") or "")
    channel = str(event.get("channel") or "")
    thread_ts = str(event.get("thread_ts") or event.get("ts") or "")
    missing = missing_envelope_fields(
        {"event_id": slack_event_id, "channel": channel, "thread_ts_or_ts": thread_ts}
    )
    if missing:
        drop(
            log,
            DropReason.MALFORMED_ENVELOPE,
            event_id=slack_event_id,
            lane=lane,
            missing=missing,
        )
        return None

    reason = classify(
        event, lane=lane, threaded_bot_allowlist=config.slack_threaded_bot_allowlist
    )
    if reason is not None:
        drop(log, reason, event_id=slack_event_id, lane=lane)
        return None

    with operation_span(
        "curie.turn.ingress",
        kind=SpanKind.CONSUMER,
        attributes={"service.name": "curie-dispatcher", "source": "dispatcher"},
    ):
        if not claim_event(redis_client, config, slack_event_id):
            drop(log, DropReason.DUPLICATE_DELIVERY, event_id=slack_event_id)
            return None

        return _mint_turn(
            web_client=web_client,
            redis_client=redis_client,
            config=config,
            log=log,
            clock=clock,
            slack_event_id=slack_event_id,
            delivery_kind="slack event",
            author=event.get("user", ""),
            # NOT `event.get("text", "")`: a Block Kit or attachment-shaped post
            # carries an empty or fallback-only top-level `text` and its real body in
            # `blocks`/`attachments`, so that read emptied the turn while still
            # burning a placeholder (#2006). `derive_text` returns a non-empty
            # top-level text byte-identically, so existing enqueues are unchanged.
            text=_strip_self_mention(derive_text(event), bot_user_id),
            # Read from the SAME event and deliberately not from `derive_text`,
            # whose non-empty-text passthrough is byte-identical by decision
            # (#2006) and must stay that way. A comment plus an upload therefore
            # carries both halves: the comment as Slack sent it, and the refs.
            attachments=derive_attachments(event),
            channel=channel,
            thread_ts=thread_ts,
        )


def action_command(action: dict[str, Any]) -> str:
    """The command a clicked Block Kit action carries: its ``value`` if set, else
    its ``action_id`` (the ss-template convention where a button's id is the
    command it runs)."""
    value = action.get("value")
    return str(value) if value else str(action.get("action_id", ""))


def process_action(
    *,
    body: dict[str, Any],
    web_client: WebClient,
    redis_client: "Redis",
    config: DispatcherConfig,
    clock: Clock = _utc_now_iso,
    logger: logging.Logger | None = None,
) -> str | None:
    """Normalize a Block Kit button click into a turn: dedupe, post an in-thread
    placeholder, and enqueue a ``QueuedTurn`` whose text is the button's
    command. The worker answers it exactly as if the user had typed that command.

    Same four steps as ``process_event`` (ack is Bolt's, before this runs); no
    decision about *how* the turn is answered lives here -- that is the worker's.
    """
    log = logger or logging.getLogger(__name__)

    # A click carries no Slack event_id; the trigger_id is the interaction's own
    # identity and is what the synthesized dedupe key below is built from, so it
    # is the useful thing to name in a pre-claim drop.
    interaction_id = str(body.get("trigger_id") or "")

    actions = body.get("actions") or []
    if not actions:
        drop(log, DropReason.NO_ACTION_IN_PAYLOAD, event_id=interaction_id)
        return None
    # Approval-card buttons (#246) resolve through the API, never become a
    # turn. Bolt runs every matching listener, so this catch-all sees the
    # click too and must yield to the dedicated approval listener.
    if is_approval_action(str(actions[0].get("action_id", ""))):
        return None
    command = action_command(actions[0])
    if not command:
        drop(log, DropReason.EMPTY_ACTION_COMMAND, event_id=interaction_id)
        return None

    # The catch-all matcher fires on *every* block action, including ones from an
    # App Home tab or a modal, which carry no channel and no message. We can only
    # turn a click into an in-thread reply when both are present. Bail here --
    # before the idempotency claim -- so a channel-less click neither KeyErrors on
    # body["channel"]["id"], nor burns the dedupe key (which would drop the Slack
    # redelivery too), nor posts an un-threaded placeholder against a thread_ts of
    # "" (a lock key shared across all such clicks in the kernel).
    channel = (body.get("channel") or {}).get("id")
    message = body.get("message") or {}
    # Reply in the clicked message's thread (its thread_ts, or its own ts if root).
    thread_ts = message.get("thread_ts") or message.get("ts")
    if not channel or not thread_ts:
        drop(log, DropReason.UNADDRESSABLE_ACTION, event_id=interaction_id)
        return None

    # A click carries no Slack event_id, so synthesize a stable idempotency key
    # from the interaction; a re-delivered click cannot enqueue (or post a second
    # placeholder) twice, same as the event dedupe.
    interaction = interaction_id or (
        f"{actions[0].get('action_ts', '')}-{actions[0].get('action_id', '')}"
    )
    # The same pre-claim envelope validation `process_event` does, for the one
    # field this mint site requires and has not already checked: the identity
    # the dedupe key is built from. `strip("-")` collapses the synthesized
    # "<action_ts>-<action_id>" to "" when the payload carried neither, and
    # claiming `action--` would burn ONE key shared by every such click --
    # refusing the first and every later one as an already-seen delivery.
    missing = missing_envelope_fields({"trigger_id_or_action_ts": interaction.strip("-")})
    if missing:
        drop(log, DropReason.MALFORMED_ENVELOPE, event_id=interaction_id, missing=missing)
        return None
    slack_event_id = f"action-{interaction}"
    with operation_span(
        "curie.turn.ingress",
        kind=SpanKind.CONSUMER,
        attributes={"service.name": "curie-dispatcher", "source": "dispatcher"},
    ):
        if not claim_event(redis_client, config, slack_event_id):
            drop(log, DropReason.DUPLICATE_DELIVERY, event_id=slack_event_id)
            return None

        user = (body.get("user") or {}).get("id", "")

        # The shared tail: same claim -> placeholder -> XADD ordering as
        # `process_event`, because it IS `process_event`'s, so the same release rule
        # and the same `QueuedTurn` shape apply on this lane by construction.
        return _mint_turn(
            web_client=web_client,
            redis_client=redis_client,
            config=config,
            log=log,
            clock=clock,
            slack_event_id=slack_event_id,
            delivery_kind="block action",
            author=user,
            text=command,
            # A Block Kit click carries no upload: its payload has a `message`
            # and an `actions` array, never a `files` array. Stated as empty
            # rather than left to the model default, so this lane's answer is
            # visible at the call site (ADR-0096 D4.4's rule for `adapter`).
            attachments=[],
            channel=channel,
            thread_ts=thread_ts,
        )


def register_handlers(
    app: App,
    *,
    web_client: WebClient,
    redis_client: "Redis",
    config: DispatcherConfig,
    clock: Clock = _utc_now_iso,
    logger: logging.Logger | None = None,
    resolver: ApprovalResolveClient | None = None,
) -> None:
    """Wire the app_mention, (direct-message) message, block-action, and
    approval-card listeners. ``resolver`` (the approvals API client) is
    injectable for tests; None builds the production client from config."""

    approval_resolver = resolver if resolver is not None else build_resolver(config)
    # Resolved once here rather than per listener: the lane filter below drops
    # outside `process_event`, so it needs a logger of its own, and the injected
    # one is the single logger every drop must land on.
    log = logger or logging.getLogger(__name__)

    @app.event("app_mention")
    def _on_app_mention(
        body: dict[str, Any], event: dict[str, Any], context: dict[str, Any]
    ) -> None:
        process_event(
            body=body,
            event=event,
            lane="mention",
            web_client=web_client,
            redis_client=redis_client,
            config=config,
            bot_user_id=context.get("bot_user_id"),
            clock=clock,
            logger=logger,
        )

    @app.event("message")
    def _on_message(
        body: dict[str, Any], event: dict[str, Any], context: dict[str, Any]
    ) -> None:
        # ENVELOPE VALIDATION, not a relevance decision (#2006). The manifest at
        # `apps/dispatcher/slack-app-manifest.yaml` subscribes bot_events to
        # exactly `app_mention` and `message.im`, so a message on any other
        # channel type cannot legitimately arrive in production -- a burst on
        # this reason means the installed app is subscribed to something the
        # manifest does not declare. It also cannot move to the routing seam even
        # in principle: `QueuedTurn` carries no lane and no subtype, and
        # `BindingResolver.resolve` sees only (kind, channel).
        channel_type = event.get("channel_type")
        if channel_type != "im":
            drop(
                log,
                DropReason.UNSUBSCRIBED_LANE,
                event_id=str(body.get("event_id", "")),
                channel_type=channel_type,
            )
            return
        process_event(
            body=body,
            event=event,
            lane="im",
            web_client=web_client,
            redis_client=redis_client,
            config=config,
            bot_user_id=context.get("bot_user_id"),
            clock=clock,
            logger=logger,
        )

    # Approval-card clicks (#246): resolve through the API (which enforces the
    # authorizer server-side) and render the verdict; never enqueue a turn.
    #
    # MIGRATION-ONLY as of #1059 (#1076). The kernel now emits every approval
    # Confirm intent with `allow_free_text`, so every card this worker posts
    # carries the note-collecting pair below. This pair is still registered
    # because a card posted by a PRE-#1059 worker can still be pending and
    # clickable, and dropping the listener would answer that click with nothing.
    #
    # Removal condition, and it is not a clock: an approval with no SLA never
    # expires (`crud.create_approval` leaves `expires_at` NULL when the request
    # named no `expires_in_seconds`, and the sweeper only selects rows where it
    # is NOT NULL), so pre-#1059 cards do not all drain on their own. This pair
    # can go once `curie <tier> approvals <AGENT> --list` shows no pending
    # approval created before the deploy that introduced the note variants --
    # checked per install, not assumed after some interval.
    @app.action(APPROVE_ACTION_ID)
    def _on_approve(ack: Callable[..., None], body: dict[str, Any]) -> None:
        ack()
        process_approval_action(
            body=body,
            decision="approved",
            web_client=web_client,
            resolver=approval_resolver,
            logger=logger,
        )

    @app.action(REJECT_ACTION_ID)
    def _on_reject(ack: Callable[..., None], body: dict[str, Any]) -> None:
        ack()
        process_approval_action(
            body=body,
            decision="rejected",
            web_client=web_client,
            resolver=approval_resolver,
            logger=logger,
        )

    # The note-collecting variants (#1053): a click OPENS a dialog and resolves
    # nothing; the submission below does the resolving. This is the pair EVERY
    # card posted by this worker carries -- the kernel sets `allow_free_text`
    # unconditionally, a decision recorded there and in docs/approvals.md
    # (#1076). The pair above is the migration entry point for older cards, not
    # a second live behavior; the earlier wording claimed otherwise.
    @app.action(APPROVE_NOTE_ACTION_ID)
    def _on_approve_with_note(ack: Callable[..., None], body: dict[str, Any]) -> None:
        ack()
        open_note_dialog(
            body=body,
            decision="approved",
            web_client=web_client,
            resolver=approval_resolver,
            logger=logger,
        )

    @app.action(REJECT_NOTE_ACTION_ID)
    def _on_reject_with_note(ack: Callable[..., None], body: dict[str, Any]) -> None:
        ack()
        open_note_dialog(
            body=body,
            decision="rejected",
            web_client=web_client,
            resolver=approval_resolver,
            logger=logger,
        )

    # The dialog's submit. Unlike an action ack, a view ack CARRIES the response:
    # ack() closes the dialog, ack(response_action="errors", ...) keeps it open
    # with the refusal attached to the note field. That is the only surface a
    # loser of the claim race can actually see, since an ephemeral posts behind
    # the open modal.
    @app.view(NOTE_MODAL_CALLBACK_ID)
    def _on_note_submitted(ack: Callable[..., Any], body: dict[str, Any]) -> None:
        # Decide first, with no Slack round trip in the way: Slack allows three
        # seconds for the ack and the resolve POST is the only network hop the
        # decision needs (#1077).
        submission = resolve_note_submission(
            body=body,
            resolver=approval_resolver,
            logger=logger,
        )
        if submission is None:
            # Unusable private_metadata: nothing resolved, nothing to render.
            ack()
            return
        response = submission.response_action
        if response is None:
            ack()
        else:
            ack(response_action=response["response_action"], errors=response["errors"])

        # Continue inline. Bolt returns the ack the moment ack() sets its
        # response and keeps running the listener body on its own executor, so
        # the card stamp needs no extra thread pool and no shutdown story of its
        # own -- the same arrangement _on_action below already relies on.
        render_note_submission(submission, web_client=web_client, logger=logger)

    # Any other Block Kit button click (a reply's action) becomes a turn. The
    # catch-all matches every action_id (including the approval ids above --
    # Bolt runs all matching listeners -- so process_action skips those); ack
    # first (Bolt's 3s budget), then normalize+enqueue.
    @app.action(re.compile(r".+"))
    def _on_action(ack: Callable[..., None], body: dict[str, Any]) -> None:
        ack()
        process_action(
            body=body,
            web_client=web_client,
            redis_client=redis_client,
            config=config,
            clock=clock,
            logger=logger,
        )
