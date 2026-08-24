"""Slack event handling: the fast-ack path that posts a placeholder and enqueues.

The Socket Mode transport acks the envelope in under three seconds on its own
(the handler sends the ack before dispatching to these listeners). These handlers
own the rest of the lifecycle step: dedupe the delivery, post an in-thread
placeholder reply, and enqueue the normalized job for the worker.

Two event types feed the same processing path:
  - ``app_mention``: the bot was @-mentioned in a channel; always process.
  - ``message``: only direct messages to the bot (``channel_type == "im"``) are
    processed, so ordinary channel chatter is not enqueued.

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

from aci_protocol import QueuedTurn, ReplyHandle, TurnSource
from slack_bolt import App
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
from .queue import claim_event, enqueue

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


def is_actionable(event: dict[str, Any]) -> bool:
    """False for events the dispatcher must ignore to avoid loops and noise.

    Bot-authored messages (including the dispatcher's own placeholder) and
    subtyped messages (edits, joins, deletions) are not user requests.
    """
    if event.get("bot_id"):
        return False
    if event.get("subtype"):
        return False
    return True


def process_event(
    *,
    body: dict[str, Any],
    event: dict[str, Any],
    web_client: WebClient,
    redis_client: "Redis",
    config: DispatcherConfig,
    bot_user_id: str | None = None,
    clock: Clock = _utc_now_iso,
    logger: logging.Logger | None = None,
) -> str | None:
    """Dedupe, post the placeholder, and enqueue one Slack event.

    Returns the Valkey Stream id when a job was enqueued, or None when the event
    was skipped (non-actionable, or a duplicate delivery already claimed).
    """
    log = logger or logging.getLogger(__name__)

    if not is_actionable(event):
        return None

    slack_event_id = body["event_id"]

    if not claim_event(redis_client, config, slack_event_id):
        log.info("duplicate slack event %s, skipping", slack_event_id)
        return None

    # Reply in-thread: for a root message the thread key is its own ts.
    thread_ts = event.get("thread_ts") or event["ts"]
    channel = event["channel"]

    placeholder = web_client.chat_postMessage(
        channel=channel,
        thread_ts=thread_ts,
        text=config.placeholder_text,
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
        author=event.get("user", ""),
        text=_strip_self_mention(event.get("text", ""), bot_user_id),
        # A person spoke, so this turn MAY steer a live one. Stated rather than
        # left to the model default for the same reason `kind` is: a producer
        # that does not say what it is produces turns nobody can audit.
        source=TurnSource.SLACK,
        # The literal "slack" is this dispatcher stating what it is; it never
        # comes from config, because a Slack Socket Mode dispatcher that could
        # claim another kind is a misrouting vector. `adapter=None` is explicit
        # rather than defaulted so a reader sees that Slack's route is the
        # worker's configured origin, not an oversight (ADR-0096 D4.4).
        reply_handle=ReplyHandle(
            kind="slack", channel=channel, placeholder=placeholder_ts, adapter=None
        ),
        received_at=clock(),
    )
    stream_id = enqueue(redis_client, config, queued)
    log.info("enqueued slack event %s as stream entry %s", slack_event_id, stream_id)
    return stream_id


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

    actions = body.get("actions") or []
    if not actions:
        return None
    # Approval-card buttons (#246) resolve through the API, never become a
    # turn. Bolt runs every matching listener, so this catch-all sees the
    # click too and must yield to the dedicated approval listener.
    if is_approval_action(str(actions[0].get("action_id", ""))):
        return None
    command = action_command(actions[0])
    if not command:
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
        log.info("block action without channel/message, skipping")
        return None

    # A click carries no Slack event_id, so synthesize a stable idempotency key
    # from the interaction; a re-delivered click cannot enqueue (or post a second
    # placeholder) twice, same as the event dedupe.
    interaction = body.get("trigger_id") or (
        f"{actions[0].get('action_ts', '')}-{actions[0].get('action_id', '')}"
    )
    slack_event_id = f"action-{interaction}"
    if not claim_event(redis_client, config, slack_event_id):
        log.info("duplicate block action %s, skipping", slack_event_id)
        return None

    user = (body.get("user") or {}).get("id", "")

    placeholder = web_client.chat_postMessage(
        channel=channel,
        thread_ts=thread_ts,
        text=config.placeholder_text,
    )
    queued = QueuedTurn(
        event_id=slack_event_id,
        conversation_id=thread_ts,
        author=user,
        text=command,
        # A person clicked a button. Still a person, still steerable.
        source=TurnSource.SLACK,
        # Same literal, same reason, on the sibling lane (ADR-0096 D4.4).
        reply_handle=ReplyHandle(
            kind="slack", channel=channel, placeholder=placeholder["ts"], adapter=None
        ),
        received_at=clock(),
    )
    stream_id = enqueue(redis_client, config, queued)
    log.info("enqueued block action %s as stream entry %s", slack_event_id, stream_id)
    return stream_id


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

    @app.event("app_mention")
    def _on_app_mention(
        body: dict[str, Any], event: dict[str, Any], context: dict[str, Any]
    ) -> None:
        process_event(
            body=body,
            event=event,
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
        if event.get("channel_type") != "im":
            return
        process_event(
            body=body,
            event=event,
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
