"""The anti-silent-swallow matrix (#2006, AC 3) and the drop-reason contract (AC 2).

Every row below is a realistic inbound Slack payload driven end to end through
Bolt's real ``SocketModeHandler.handle``, exactly as ``test_dispatch.py`` does:
real Valkey, real Bolt middleware chain, real listener thread pool. Only the
Slack Web API client and the socket transport are faked.

**What makes this an oracle rather than a smoke test** -- three properties, and
losing any one of them turns the whole file into a test that passes while the
adapter eats messages:

1. Each row must end in exactly one of two visible dispositions: a new entry on
   the Valkey stream, or exactly one log record from the *injected* dispatcher
   logger naming a specific ``DropReason``. A row that produces **neither**
   fails, and that failure is the whole point of the file -- "neither" is what
   silent loss looks like from the outside.
2. A ``@app.error`` collector records every listener/middleware exception, and
   every matrix row asserts it saw **zero**. Bolt acks an Events API envelope
   *before* it runs the listener body and then routes the body's exception to
   this handler (``slack_bolt/listener/thread_runner.py``,
   ``ThreadListenerRunner.run``; ``slack_bolt/app/app.py``, ``App.dispatch``'s
   outer ``except``). Without this collector, an exception after the ack is
   indistinguishable from a silent drop -- neither enqueue nor drop log.
3. The logger is **injected** (``register_handlers``'s existing ``logger``
   parameter, forwarded by ``build_app``) and asserted on directly. ``caplog``
   would depend on propagation out of Bolt's listener executor threads, which is
   not a property this suite should rest on.

Slack payload shapes below are cited to Slack's API reference, never to what the
implementation happens to assume; framework claims are cited to the installed
``slack_bolt`` source path.
"""

import logging
import uuid
from dataclasses import dataclass
from enum import Enum
from typing import Any
from unittest.mock import MagicMock

import pytest
import redis
from aci_protocol.turn import DEFAULT_IDENTITY
from curie_dispatcher.app import build_app
from curie_dispatcher.config import DispatcherConfig
from curie_dispatcher.handlers import process_action
from curie_dispatcher.queue import from_stream_fields
from curie_dispatcher.relevance import DROP_RATIONALES, DropReason
from slack_bolt import App
from slack_bolt.adapter.socket_mode import SocketModeHandler
from slack_sdk.errors import SlackApiError, SlackRequestError
from slack_sdk.socket_mode.request import SocketModeRequest
from slack_sdk.web import WebClient

from .conftest import FakeSocketClient, _authorize
from .test_dispatch import BOT_TS, _drain, _events_api_request

# ---------------------------------------------------------------------------
# Harness
# ---------------------------------------------------------------------------


class _RecordCollector(logging.Handler):
    """Captures records off the injected logger, including from Bolt's executor
    threads. ``list.append`` is atomic, and every assertion runs after
    ``_drain``, so no extra locking is needed."""

    def __init__(self) -> None:
        super().__init__(level=logging.DEBUG)
        self.records: list[logging.LogRecord] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.records.append(record)


@dataclass
class Harness:
    """One Bolt app plus the two collectors the oracle rests on."""

    app: App
    web_client: WebClient
    sock: FakeSocketClient
    handler: SocketModeHandler
    #: The logger injected into ``register_handlers`` -- the one every drop the
    #: adapter logs must land on. Held here so the direct-driver rows can pass
    #: the same logger the Bolt-driven rows assert against.
    logger: logging.Logger
    records: list[logging.LogRecord]
    errors: list[BaseException]


def _build_harness(
    config: DispatcherConfig,
    redis_client: redis.Redis,
    *,
    chat_post_message: Any | None = None,
) -> Harness:
    """A dispatcher app wired for observation: injected logger + error collector.

    ``build_app`` forwards ``logger`` straight into ``register_handlers``, which
    hands it to ``process_event`` / ``process_action``, so every drop the adapter
    logs lands in this harness's ``records`` and nowhere else (``propagate`` is
    off, and the logger name is unique per harness so rows cannot cross-talk).
    """
    web_client = WebClient(token="xoxb-test")
    post_message: Any = chat_post_message or MagicMock(return_value={"ts": BOT_TS})
    web_client.chat_postMessage = post_message  # type: ignore[method-assign]

    collector = _RecordCollector()
    logger = logging.getLogger(f"curie_dispatcher.test.{uuid.uuid4().hex}")
    logger.setLevel(logging.DEBUG)
    logger.propagate = False
    logger.addHandler(collector)

    app = build_app(
        config,
        web_client=web_client,
        redis_client=redis_client,
        authorize=_authorize,
        logger=logger,
    )

    errors: list[BaseException] = []

    # Bolt routes both listener-body exceptions (ThreadListenerRunner.run) and
    # dispatch-level exceptions (App.dispatch's outer except -> the middleware
    # error handler) to the function registered here. `App.error` installs it as
    # BOTH handlers, so this list sees every framework-swallowed failure.
    @app.error
    def _collect_error(error: Exception) -> None:
        errors.append(error)

    return Harness(
        app=app,
        web_client=web_client,
        sock=FakeSocketClient(),
        handler=SocketModeHandler(app, app_token="xapp-test"),
        logger=logger,
        records=collector.records,
        errors=errors,
    )


def _drop_reasons_logged(records: list[logging.LogRecord]) -> list[DropReason]:
    """Every ``DropReason`` named by a record on the injected logger.

    Tolerant of the exact log format on purpose: the contract is "the reason is
    named", not "the reason is formatted this way". Matches on the member's
    value or its name so a ``StrEnum`` declared with either explicit values or
    ``auto()`` satisfies it.
    """
    found: list[DropReason] = []
    for record in records:
        text = record.getMessage().lower()
        for reason in DropReason:
            if str(reason.value).lower() in text or reason.name.lower() in text:
                found.append(reason)
                break
    return found


def _stream_entries(redis_client: redis.Redis, config: DispatcherConfig) -> list[Any]:
    return list(redis_client.xrange(config.stream))


# ---------------------------------------------------------------------------
# Payload builders -- shapes cited to Slack's API reference
# ---------------------------------------------------------------------------


def _alert_blocks() -> list[dict[str, Any]]:
    """An alert-shaped Block Kit body: the #2006 case.

    A post built from blocks carries an empty or near-empty top-level ``text``;
    Slack documents ``text`` as a fallback for notifications, not as the body
    (Slack: "Blocks" reference, docs.slack.dev/reference/block-kit/blocks, and
    "Creating rich message layouts"). Block shapes below:
    ``header`` (plain_text), ``section`` with ``fields`` (mrkdwn text objects),
    ``rich_text`` with ``rich_text_section`` elements of type ``text`` /
    ``link`` / ``emoji`` / ``user``, and ``context`` with a mrkdwn element.
    """
    return [
        {"type": "header", "text": {"type": "plain_text", "text": "Disk usage critical"}},
        {
            "type": "section",
            "fields": [
                {"type": "mrkdwn", "text": "*Host*\nweb-01"},
                {"type": "mrkdwn", "text": "*Usage*\n97 percent"},
            ],
        },
        {
            "type": "rich_text",
            "elements": [
                {
                    "type": "rich_text_section",
                    "elements": [
                        {"type": "text", "text": "runbook at "},
                        {"type": "link", "url": "https://example.invalid/runbook"},
                        {"type": "emoji", "name": "rotating_light"},
                        {"type": "user", "user_id": "U123"},
                    ],
                }
            ],
        },
        {
            "type": "context",
            "elements": [{"type": "mrkdwn", "text": "fired by prometheus"}],
        },
    ]


BLOCK_CONTENT = (
    "Disk usage critical",
    "web-01",
    "97 percent",
    "runbook at",
    "https://example.invalid/runbook",
    ":rotating_light:",
    "<@U123>",
    "fired by prometheus",
)


def _alert_attachments() -> list[dict[str, Any]]:
    """Legacy secondary-content attachments (Slack: "Secondary message
    attachments", docs.slack.dev/messaging/formatting-message-text). The fields
    exercised are the documented ones an alert integration populates: ``pretext``,
    ``title``, ``text``, ``fields[].title`` / ``fields[].value``, ``footer`` --
    plus ``fallback``, which Slack defines as the plain-text summary shown where
    the attachment cannot render.
    """
    return [
        {
            "color": "#d00000",
            "pretext": "Alert from Grafana",
            "title": "Latency SLO burn",
            "text": "p99 is 3.2s over the last 5m",
            "fields": [{"title": "Service", "value": "checkout-api", "short": True}],
            "footer": "grafana",
            "fallback": "SUMMARY-ONLY-FALLBACK",
        }
    ]


ATTACHMENT_CONTENT = (
    "Alert from Grafana",
    "Latency SLO burn",
    "p99 is 3.2s over the last 5m",
    "Service",
    "checkout-api",
    "grafana",
)


def _mention(
    *,
    text: str = "",
    blocks: list[dict[str, Any]] | None = None,
    attachments: list[dict[str, Any]] | None = None,
    bot_id: str | None = None,
    thread_ts: str | None = None,
    ts: str = "1700.0001",
) -> dict[str, Any]:
    """An ``app_mention`` event (Slack: docs.slack.dev/reference/events/app_mention)."""
    event: dict[str, Any] = {"type": "app_mention", "channel": "C123", "text": text, "ts": ts}
    if bot_id is None:
        event["user"] = "U123"
    else:
        # A bot-authored post carries ``bot_id`` and no ``user``; Slack documents
        # ``bot_id`` on messages posted by apps/incoming webhooks.
        event["bot_id"] = bot_id
    if blocks is not None:
        event["blocks"] = blocks
    if attachments is not None:
        event["attachments"] = attachments
    if thread_ts is not None:
        event["thread_ts"] = thread_ts
    return event


def _mention_missing(field: str, **kwargs: Any) -> dict[str, Any]:
    """An ``app_mention`` Slack always stamps ``field`` on, delivered without it.

    Slack documents ``channel`` and ``ts`` as present on every delivered
    ``app_mention`` (docs.slack.dev/reference/events/app_mention), so these are
    shapes production should never see -- which is exactly why the adapter must
    refuse them by name instead of indexing them and raising after the claim.
    Built by removing the key from the well-formed builder above, so the two
    cannot drift apart.
    """
    event = _mention(**kwargs)
    del event[field]
    return event


def _dm(
    *,
    text: str = "hello bot",
    subtype: str | None = None,
    bot_id: str | None = None,
    channel_type: str = "im",
    ts: str = "1800.0001",
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """A ``message`` event on the DM lane (Slack: docs.slack.dev/reference/events/message.im).

    ``channel_type`` is the field Slack stamps on the delivered ``message``
    envelope; ``im`` is the direct-message lane the app manifest subscribes to.
    """
    event: dict[str, Any] = {
        "type": "message",
        "channel_type": channel_type,
        "channel": "D1",
        "text": text,
        "ts": ts,
    }
    if bot_id is None:
        event["user"] = "U9"
    else:
        event["bot_id"] = bot_id
    if subtype is not None:
        event["subtype"] = subtype
    if extra:
        event.update(extra)
    return event


def _block_action_body(
    *,
    actions: list[dict[str, Any]],
    trigger_id: str,
    with_channel: bool = True,
) -> dict[str, Any]:
    """A ``block_actions`` interaction payload (Slack: docs.slack.dev/reference/
    interaction-payloads/block-actions-payload)."""
    body: dict[str, Any] = {
        "type": "block_actions",
        "trigger_id": trigger_id,
        "team": {"id": "T1"},
        "user": {"id": "U123"},
        "api_app_id": "A1",
        "token": "verif",
        "actions": actions,
    }
    if with_channel:
        body["container"] = {"type": "message", "message_ts": "1700.0001"}
        body["channel"] = {"id": "C123"}
        body["message"] = {"ts": "1700.0001", "thread_ts": "1700.0001"}
    else:
        # An App Home / modal click: container is a view, and the payload carries
        # neither ``channel`` nor ``message``, so there is no thread to answer in.
        body["container"] = {"type": "view", "view_id": "V1"}
        body["view"] = {"id": "V1", "type": "home"}
    return body


def _anonymous_action_body() -> dict[str, Any]:
    """A click carrying no identity to build an idempotency key from.

    Slack stamps ``trigger_id`` on the ``block_actions`` payload and
    ``action_ts`` / ``action_id`` on each entry of ``actions`` (Slack:
    docs.slack.dev/reference/interaction-payloads/block-actions-payload). A
    payload carrying none of the three still addresses a real thread and still
    names a command through the button's ``value`` -- it just has no interaction
    identity, so the synthesized key collapses to ``action--``.
    """
    body = _block_action_body(
        actions=[{"type": "button", "value": "restart the deploy"}],
        trigger_id="unused",
    )
    del body["trigger_id"]
    return body


def _interactive_request(envelope_id: str, payload: dict[str, Any]) -> SocketModeRequest:
    return SocketModeRequest(type="interactive", envelope_id=envelope_id, payload=payload)


# ---------------------------------------------------------------------------
# The matrix
# ---------------------------------------------------------------------------


class Disposition(Enum):
    """The two non-``DropReason`` outcomes a row may declare."""

    ENQUEUED = "enqueued"
    #: Refused by Bolt ABOVE our listeners. Not our code's drop -- see the B1 row.
    DROPPED_BY_BOLT = "dropped_by_bolt"


class Driver(Enum):
    BOLT = "bolt"
    #: Driven by calling ``process_action`` directly. Used ONLY where Bolt's own
    #: catch-all matcher provably cannot deliver the payload to a listener; see
    #: ``_DIRECT_DRIVER_NOTE``.
    DIRECT_ACTION = "direct_action"


_DIRECT_DRIVER_NOTE = """
Two rows cannot be driven through Bolt, and the reason is in Bolt's matcher, not
in our code. ``register_handlers`` registers the catch-all as
``@app.action(re.compile(r".+"))``. Bolt resolves that against the first action
in the payload (``slack_bolt/listener_matcher/builtins.py``, ``_block_action``:
``action = to_action(body)`` then ``_matches(pattern, action["action_id"])``,
with ``to_action`` returning ``body["actions"][0]``). Therefore:

  * ``actions: []`` -> ``to_action`` raises ``IndexError`` inside the matcher,
    which ``App.dispatch``'s outer ``except`` turns into a framework error. No
    listener ever runs, so ``NO_ACTION_IN_PAYLOAD`` is unreachable end to end.
  * an action whose ``action_id`` is ``""`` (and which carries no ``value``) ->
    ``re.compile(r".+").search("")`` is ``None``, so the matcher returns False
    and no listener runs. ``EMPTY_ACTION_COMMAND`` is likewise unreachable.

Both guards are still live defence inside ``process_action`` (a second listener,
a matcher change, or a direct caller reaches them), and the ticket's contract
requires every ``DropReason`` to be exercised -- so these two rows call
``process_action`` directly. They still assert only user-visible outcomes: no
stream entry, exactly one logged reason, and no dedupe key burned.
"""


@dataclass(frozen=True)
class Row:
    """One inbound payload plus the disposition the adapter owes it."""

    name: str
    expected: Disposition | DropReason
    #: Why this row expects what it expects -- printed in every failure message.
    why: str
    request: SocketModeRequest | None = None
    action_body: dict[str, Any] | None = None
    driver: Driver = Driver.BOLT
    #: Delivered first, on a SEPARATE app, so the asserted delivery is measured
    #: in isolation (``_drain`` shuts an executor down permanently).
    prime: SocketModeRequest | None = None
    text_contains: tuple[str, ...] = ()
    text_not_contains: tuple[str, ...] = ()
    dedupe_id: str | None = None


MATRIX: tuple[Row, ...] = (
    # -- AC 1: bodies that are empty at the top level but carry real content ---
    Row(
        name="mention_block_kit_body_only",
        expected=Disposition.ENQUEUED,
        why=(
            "AC 1, the ticket's headline defect: an alert-shaped Block Kit post has "
            "text='' and its content in `blocks`. On the current code this enqueues a "
            "turn with empty text -- the message is emptied, not dropped."
        ),
        request=_events_api_request(
            "env-blocks", "Ev-blocks", _mention(text="", blocks=_alert_blocks())
        ),
        text_contains=BLOCK_CONTENT,
    ),
    Row(
        name="mention_attachments_body_only",
        expected=Disposition.ENQUEUED,
        why=(
            "AC 1 on the attachment-shaped variant. `fallback` must NOT appear: Slack "
            "defines it as the summary used where the attachment cannot render, so it "
            "duplicates content that was already emitted."
        ),
        request=_events_api_request(
            "env-attach", "Ev-attach", _mention(text="", attachments=_alert_attachments())
        ),
        text_contains=ATTACHMENT_CONTENT,
        text_not_contains=("SUMMARY-ONLY-FALLBACK",),
    ),
    Row(
        name="mention_whitespace_text_with_blocks",
        expected=Disposition.ENQUEUED,
        why=(
            "'   ' is truthy in Python, so a whitespace-only `text` must not be mistaken "
            "for a real body; the block content is what reaches the model."
        ),
        request=_events_api_request(
            "env-ws", "Ev-ws", _mention(text="   ", blocks=_alert_blocks())
        ),
        text_contains=("Disk usage critical",),
    ),
    # -- AC 3: the open-world subtype inversion (D3) --------------------------
    Row(
        name="dm_file_share_with_comment",
        expected=Disposition.ENQUEUED,
        why=(
            "`file_share` is a real user message: a person uploaded a file WITH a "
            "comment (Slack: docs.slack.dev/reference/events/message, subtypes). The "
            "current blanket 'any subtype drops' filter swallows it unlogged."
        ),
        request=_events_api_request(
            "env-file",
            "Ev-file",
            _dm(
                text="here is the incident report",
                subtype="file_share",
                extra={"files": [{"id": "F1", "title": "incident.pdf", "name": "incident.pdf"}]},
            ),
        ),
        text_contains=("here is the incident report",),
    ),
    Row(
        name="dm_thread_broadcast",
        expected=Disposition.ENQUEUED,
        why=(
            "`thread_broadcast` is a human thread reply also sent to the conversation "
            "(Slack: message subtypes). It is content, not lifecycle."
        ),
        request=_events_api_request(
            "env-broadcast",
            "Ev-broadcast",
            _dm(
                text="also sending this to the channel",
                subtype="thread_broadcast",
                ts="1801.0002",
                extra={"thread_ts": "1801.0001"},
            ),
        ),
        text_contains=("also sending this to the channel",),
    ),
    Row(
        name="dm_unknown_future_subtype",
        expected=Disposition.ENQUEUED,
        why=(
            "THE row that proves the open-world inversion (D3). Slack keeps adding "
            "subtypes; an unknown one must reach routing rather than be denied by a "
            "filter that denies an open set. Bolt's message matcher is type-only and "
            "subtype-blind (slack_bolt/listener_matcher/builtins.py, event matching), "
            "so every message.im subtype does reach our listener."
        ),
        request=_events_api_request(
            "env-future",
            "Ev-future",
            _dm(text="a subtype nobody has shipped yet", subtype="some_future_subtype"),
        ),
        text_contains=("a subtype nobody has shipped yet",),
    ),
    Row(
        name="dm_message_changed_is_not_content",
        expected=DropReason.NON_CONTENT_SUBTYPE,
        why=(
            "An edit of an existing message: the payload nests the edited `message` and "
            "`previous_message` and carries no new user request (Slack: message_changed)."
        ),
        request=_events_api_request(
            "env-changed",
            "Ev-changed",
            {
                "type": "message",
                "subtype": "message_changed",
                "channel": "D1",
                "channel_type": "im",
                "ts": "1802.0002",
                "message": {"type": "message", "user": "U9", "text": "edited", "ts": "1802.0001"},
                "previous_message": {
                    "type": "message",
                    "user": "U9",
                    "text": "original",
                    "ts": "1802.0001",
                },
            },
        ),
    ),
    Row(
        name="dm_ekm_access_denied_is_not_content",
        expected=DropReason.NON_CONTENT_SUBTYPE,
        why=(
            "Enterprise Key Management redacted the body, so there is no request to "
            "answer; admitting it would mint an empty turn (Slack: ekm_access_denied)."
        ),
        request=_events_api_request(
            "env-ekm", "Ev-ekm", _dm(text="", subtype="ekm_access_denied", ts="1803.0001")
        ),
    ),
    Row(
        name="dm_assistant_app_thread_is_not_content",
        expected=DropReason.NON_CONTENT_SUBTYPE,
        why=(
            "A thread-start marker carrying assistant metadata rather than user content "
            "(Slack: message subtype assistant_app_thread). Curie enables assistant "
            "functionality, so this needs an explicit disposition rather than riding the "
            "unknown/future rule."
        ),
        request=_events_api_request(
            "env-assist",
            "Ev-assist",
            _dm(
                text="",
                subtype="assistant_app_thread",
                ts="1804.0001",
                extra={"assistant_app_thread": {"title": "New chat"}},
            ),
        ),
    ),
    Row(
        name="dm_message_deleted_is_not_content",
        expected=DropReason.NON_CONTENT_SUBTYPE,
        why=(
            "A deletion notice: Slack names the removed `deleted_ts` and nests the "
            "`previous_message`, and there is no new user request anywhere in it "
            "(Slack: docs.slack.dev/reference/events/message, subtype message_deleted). "
            "NON_CONTENT_SUBTYPES is a closed denylist, so every member needs its own "
            "row -- without one, deleting this member from the set is a mutation the "
            "suite cannot see."
        ),
        request=_events_api_request(
            "env-deleted",
            "Ev-deleted",
            {
                "type": "message",
                "subtype": "message_deleted",
                "channel": "D1",
                "channel_type": "im",
                "hidden": True,
                "ts": "1808.0002",
                "deleted_ts": "1808.0001",
                "previous_message": {
                    "type": "message",
                    "user": "U9",
                    "text": "never mind",
                    "ts": "1808.0001",
                },
            },
        ),
    ),
    Row(
        name="dm_message_replied_is_not_content",
        expected=DropReason.NON_CONTENT_SUBTYPE,
        why=(
            "Parent-message bookkeeping: Slack re-sends the PARENT, with its `message` "
            "nested and `reply_count` bumped, when someone replies in its thread "
            "(Slack: message subtype message_replied). The reply itself arrives as its "
            "own event, so admitting this one would mint a second turn for text the "
            "user sent once."
        ),
        request=_events_api_request(
            "env-replied",
            "Ev-replied",
            {
                "type": "message",
                "subtype": "message_replied",
                "channel": "D1",
                "channel_type": "im",
                "hidden": True,
                "ts": "1809.0002",
                "message": {
                    "type": "message",
                    "user": "U9",
                    "text": "the parent message",
                    "ts": "1809.0001",
                    "thread_ts": "1809.0001",
                    "reply_count": 1,
                },
            },
        ),
    ),
    Row(
        name="dm_tombstone_is_not_content",
        expected=DropReason.NON_CONTENT_SUBTYPE,
        why=(
            "The marker Slack leaves where a threaded parent was removed (Slack: "
            "message subtype tombstone). Its `text` is Slack's own chrome, not "
            "something a person wrote, so admitting it would enqueue a turn whose "
            "prompt Slack authored."
        ),
        request=_events_api_request(
            "env-tombstone",
            "Ev-tombstone",
            {
                "type": "message",
                "subtype": "tombstone",
                "channel": "D1",
                "channel_type": "im",
                "hidden": True,
                "ts": "1810.0001",
                "text": "This message was deleted.",
            },
        ),
    ),
    # -- AC 3: bot identity. TWO DISTINCT IDS -- see the comments on each row --
    Row(
        name="mention_from_self_b1_is_dropped_by_bolt",
        expected=Disposition.DROPPED_BY_BOLT,
        why=(
            "UPSTREAM FRAMEWORK BEHAVIOR -- THIS ROW DOES NOT EXERCISE OUR CODE. "
            "`bot_id: 'B1'` is the SAME identity conftest._authorize returns "
            "(AuthorizeResult(bot_id='B1', bot_user_id='U0BOT')), so this is a "
            "self-authored event. slack_bolt's IgnoringSelfEvents middleware "
            "(slack_bolt/middleware/ignoring_self_events/ignoring_self_events.py: "
            "`bot_id == auth_result.bot_id` -> `req.context.ack()` with no `next()`) "
            "acks the envelope and returns BEFORE any listener runs, logging its reason "
            "at DEBUG only. That middleware is the real self-loop guard, and it is why "
            "the adapter's blanket DM bot filter is deleted. The assertion here is "
            "therefore 'no enqueue, no listener invoked, no error' -- attributed to "
            "slack_bolt, not claimed as a drop of ours."
        ),
        request=_events_api_request(
            "env-self", "Ev-self", _mention(text="Working on it.", bot_id="B1", ts="1900.0001")
        ),
    ),
    Row(
        name="mention_from_foreign_bot_b2_at_root",
        expected=Disposition.ENQUEUED,
        why=(
            "`B2` is a FOREIGN alert bot -- a different identity from the authorized "
            "`B1`, so IgnoringSelfEvents passes it through and our code decides. D4: a "
            "bot-authored mention at ROOT (no thread_ts) is the ticket's case (an alert "
            "bot @-mentioning Curie) and must reach routing."
        ),
        request=_events_api_request(
            "env-b2-root",
            "Ev-b2-root",
            _mention(text="<@U0BOT> disk is full on web-01", bot_id="B2", ts="1901.0001"),
        ),
        text_contains=("disk is full on web-01",),
    ),
    Row(
        name="mention_from_foreign_bot_b2_in_thread",
        expected=DropReason.BOT_AUTHORED_THREAD_REPLY,
        why=(
            "D4's cross-installation loop guard. Curie's own replies and placeholders "
            "are ALWAYS threaded, so two Curie installations in one workspace could "
            "mention-loop each other indefinitely; Bolt's self filter cannot stop that "
            "because the two bot identities differ. Root-only admission separates the "
            "ticket's case from the loop with no schema change."
        ),
        request=_events_api_request(
            "env-b2-thread",
            "Ev-b2-thread",
            _mention(
                text="<@U0BOT> replying in your thread",
                bot_id="B2",
                thread_ts="1902.0001",
                ts="1902.0002",
            ),
        ),
    ),
    Row(
        name="mention_from_a_human_in_a_thread",
        expected=Disposition.ENQUEUED,
        why=(
            "THE negative control for the loop guard above, and the mutation it exists "
            "to kill. BOT_AUTHORED_THREAD_REPLY is bot-authored AND threaded; widening "
            "it to every threaded mention would swallow the single most ordinary "
            "inbound Curie has -- a person answering in the thread Curie is already "
            "replying in. Without this row that mutation passes the whole file."
        ),
        request=_events_api_request(
            "env-human-thread",
            "Ev-human-thread",
            _mention(
                text="<@U0BOT> and what about staging",
                thread_ts="1903.0001",
                ts="1903.0002",
            ),
        ),
        text_contains=("and what about staging",),
    ),
    # -- AC 3: the DM lane ----------------------------------------------------
    Row(
        name="dm_from_a_human",
        expected=Disposition.ENQUEUED,
        why="The baseline DM lane: a person messaging the bot directly (message.im).",
        request=_events_api_request(
            "env-dm", "Ev-dm", _dm(text="what is our error rate", ts="1805.0001")
        ),
        text_contains=("what is our error rate",),
    ),
    Row(
        name="dm_from_a_foreign_bot",
        expected=Disposition.ENQUEUED,
        why=(
            "D4/R2: the blanket DM bot filter is DELETED. Incoming webhooks, Workflow "
            "Builder posts and other apps all reach Curie as bot-authored `message.im` "
            "events, and discarding them was a real silent loss. Self-authored DMs are "
            "still dropped upstream by IgnoringSelfEvents -- which is exactly why the "
            "id here is `B2` and not `B1`."
        ),
        request=_events_api_request(
            "env-dm-bot",
            "Ev-dm-bot",
            _dm(text="deploy finished, 3 warnings", bot_id="B2", ts="1806.0001"),
        ),
        text_contains=("deploy finished, 3 warnings",),
    ),
    Row(
        name="dm_from_a_foreign_bot_in_a_thread",
        expected=Disposition.ENQUEUED,
        why=(
            "The lane half of the same mutation. `relevance.classify` consults bot "
            "authorship only when `lane == 'mention'`, because the cross-installation "
            "mention loop is a mention-lane phenomenon. A bot-authored DM that happens "
            "to be threaded -- an incoming webhook or Workflow Builder post answering "
            "in an existing DM thread -- carries no such loop, so extending the guard "
            "to this lane would be a brand-new silent drop that today's rows miss."
        ),
        request=_events_api_request(
            "env-dm-bot-thread",
            "Ev-dm-bot-thread",
            _dm(
                text="deploy rolled back",
                bot_id="B2",
                ts="1806.0003",
                extra={"thread_ts": "1806.0002"},
            ),
        ),
        text_contains=("deploy rolled back",),
    ),
    # -- AC 3: envelope validation, dedupe, and the action lane ---------------
    Row(
        name="mention_without_a_channel_is_malformed",
        expected=DropReason.MALFORMED_ENVELOPE,
        why=(
            "Slack stamps `channel` on every delivered app_mention (Slack: "
            "docs.slack.dev/reference/events/app_mention), so a delivery without one "
            "cannot be answered -- there is nowhere to post the placeholder. Reading it "
            "as `event['channel']` used to raise AFTER `claim_event` succeeded: Bolt "
            "had already acked and swallowed the exception, so the claim outlived a "
            "turn that never existed and Slack's redelivery was then refused as an "
            "already-seen delivery. The dedupe assertion below is the half that pins "
            "validation ahead of the claim -- move the check back after `claim_event` "
            "and this row fails on the burned key even though the log line is "
            "unchanged."
        ),
        request=_events_api_request(
            "env-no-channel", "Ev-no-channel", _mention_missing("channel", text="are we up")
        ),
        dedupe_id="Ev-no-channel",
    ),
    Row(
        name="mention_without_a_thread_key_is_malformed",
        expected=DropReason.MALFORMED_ENVELOPE,
        why=(
            "`ts` is the message's own timestamp and doubles as the thread key for a "
            "root post (Slack: docs.slack.dev/reference/events/app_mention); a reply is "
            "posted with `thread_ts` set to one or the other. With neither there is no "
            "thread to answer in, and the blank key would become ONE conversation id "
            "shared by every such delivery. Same ordering pin as the row above."
        ),
        request=_events_api_request(
            "env-no-ts", "Ev-no-ts", _mention_missing("ts", text="are we up")
        ),
        dedupe_id="Ev-no-ts",
    ),
    Row(
        name="event_without_an_event_id_is_malformed",
        expected=DropReason.MALFORMED_ENVELOPE,
        why=(
            "`event_id` is the Events API wrapper's own identifier (Slack: "
            "docs.slack.dev/apis/events-api, the event_callback envelope) and is the "
            "idempotency key this adapter claims. A blank one would claim a single key "
            "shared by every delivery that omits it, refusing all but the first as "
            "duplicates -- so it is refused before the claim, leaving that key free."
        ),
        request=_events_api_request("env-no-id", "", _mention(text="are we up")),
        dedupe_id="",
    ),
    Row(
        name="message_outside_the_subscribed_lane",
        expected=DropReason.UNSUBSCRIBED_LANE,
        why=(
            "ENVELOPE VALIDATION, NOT A RELEVANCE DECISION. "
            "`apps/dispatcher/slack-app-manifest.yaml` subscribes "
            "`settings.event_subscriptions.bot_events` to exactly `app_mention` and "
            "`message.im`, so a `message` whose channel_type is not `im` cannot "
            "legitimately arrive in production; refusing it asserts the delivered "
            "envelope matches the declared subscription surface. It also cannot move to "
            "the routing seam even in principle: QueuedTurn carries no lane and no "
            "subtype, and BindingResolver.resolve sees only (kind, adapter, address). "
            "A burst on "
            "this reason means the installed app is subscribed to something the manifest "
            "does not declare -- not 'a chatty channel'."
        ),
        request=_events_api_request(
            "env-chan",
            "Ev-chan",
            _dm(text="just chatting", channel_type="channel", ts="1807.0001"),
        ),
    ),
    Row(
        name="duplicate_redelivery_of_a_claimed_event",
        expected=DropReason.DUPLICATE_DELIVERY,
        why=(
            "Slack redelivers an event when it does not see a timely ack. The first "
            "delivery (driven on a SEPARATE app below, so this row's app sees only the "
            "redelivery) claimed the dedupe key; the redelivery must be refused with an "
            "enumerated reason rather than a bare log line."
        ),
        prime=_events_api_request("env-dup-1", "Ev-dup", _mention(text="first delivery")),
        request=_events_api_request("env-dup-2", "Ev-dup", _mention(text="first delivery")),
    ),
    Row(
        name="app_home_click_has_nowhere_to_reply",
        expected=DropReason.UNADDRESSABLE_ACTION,
        why=(
            "An App Home / modal click arrives with `container.type == 'view'` and no "
            "`channel` and no `message`, so no in-thread reply is possible. This drop "
            "must stay AHEAD of the dedupe claim -- burning the key here would drop "
            "Slack's redelivery too (test_dispatch.py pins that separately)."
        ),
        request=_interactive_request(
            "env-home",
            _block_action_body(
                actions=[{"type": "button", "action_id": "reports", "action_ts": "1.5"}],
                trigger_id="trig-home",
                with_channel=False,
            ),
        ),
        dedupe_id="action-trig-home",
    ),
    Row(
        name="block_action_with_no_actions",
        expected=DropReason.NO_ACTION_IN_PAYLOAD,
        why=(
            "A block-action payload carrying no actions names no command and addresses "
            "nothing. Driven directly -- see _DIRECT_DRIVER_NOTE: Bolt's catch-all "
            "matcher raises IndexError on `actions: []` before any listener runs."
        ),
        driver=Driver.DIRECT_ACTION,
        action_body=_block_action_body(actions=[], trigger_id="trig-empty-actions"),
        dedupe_id="action-trig-empty-actions",
    ),
    Row(
        name="block_action_naming_no_command",
        expected=DropReason.EMPTY_ACTION_COMMAND,
        why=(
            "A button carrying neither `value` nor a usable `action_id` names no command, "
            "so there is nothing to enqueue as turn text. Driven directly -- see "
            "_DIRECT_DRIVER_NOTE: `re.compile(r'.+').search('')` is None, so Bolt's "
            "catch-all never matches this payload."
        ),
        driver=Driver.DIRECT_ACTION,
        action_body=_block_action_body(
            actions=[{"type": "button", "action_id": "", "value": "", "action_ts": "1.5"}],
            trigger_id="trig-no-command",
        ),
        dedupe_id="action-trig-no-command",
    ),
    Row(
        name="block_action_with_no_interaction_identity",
        expected=DropReason.MALFORMED_ENVELOPE,
        why=(
            "The action lane's own malformed shape. Slack stamps `trigger_id` on the "
            "payload and `action_ts` / `action_id` on each `actions` entry (Slack: "
            "docs.slack.dev/reference/interaction-payloads/block-actions-payload). With "
            "none of the three the synthesized key collapses to `action--`: ONE key "
            "shared by every such click, so the first would burn it and every later "
            "click -- from any user, on any card -- would be refused as an already-seen "
            "delivery. This click names a command and addresses a real thread, so it "
            "reaches the identity check rather than the two guards above it. Driven "
            "directly -- see _DIRECT_DRIVER_NOTE: with no `action_id` key at all, "
            "Bolt's catch-all matcher indexes `action['action_id']` and raises "
            "KeyError, so no listener ever runs."
        ),
        driver=Driver.DIRECT_ACTION,
        action_body=_anonymous_action_body(),
        dedupe_id="action--",
    ),
)


# ---------------------------------------------------------------------------
# The oracle
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("row", MATRIX, ids=[row.name for row in MATRIX])
def test_every_inbound_payload_is_enqueued_or_refused_with_a_named_reason(
    row: Row, redis_client: redis.Redis, config: DispatcherConfig
) -> None:
    """AC 3. Every row ends visibly: on the stream, or in the log naming a reason.

    THIS ASSERTION CANNOT PASS VACUOUSLY, and that is the entire point of the
    file. The "produced neither" branch fires first and names the row, so a
    payload the adapter swallows in silence -- no stream entry, no drop log, no
    exception -- fails here rather than looking like a clean pass. Deleting the
    `enqueued or logged` check, or widening any expectation to "anything", is the
    mutation this test exists to make impossible.
    """
    if row.prime is not None:
        # The prime runs on its own app: `_drain` shuts an executor down for
        # good, so the same app cannot handle a second delivery, and a shared one
        # would blend the two deliveries' log records.
        primer = _build_harness(config, redis_client)
        primer.handler.handle(primer.sock, row.prime)
        _drain(primer.app)
        assert primer.errors == [], f"the priming delivery for {row.name!r} itself failed"
        assert len(_stream_entries(redis_client, config)) == 1, "the prime must have enqueued"

    before = len(_stream_entries(redis_client, config))
    harness = _build_harness(config, redis_client)

    if row.driver is Driver.BOLT:
        assert row.request is not None
        harness.handler.handle(harness.sock, row.request)
        _drain(harness.app)
        # Slack is told "received" either way; a drop is never a failed delivery.
        assert harness.sock.acked_envelope_ids == [row.request.envelope_id], row.why
    else:
        assert row.action_body is not None
        process_action(
            body=row.action_body,
            web_client=harness.web_client,
            redis_client=redis_client,
            config=config,
            slack_identity=DEFAULT_IDENTITY,
            logger=harness.logger,
        )
        _drain(harness.app)

    entries = _stream_entries(redis_client, config)
    enqueued = len(entries) - before
    logged = _drop_reasons_logged(harness.records)

    # --- the anti-silent-swallow property -----------------------------------
    if row.expected is not Disposition.DROPPED_BY_BOLT:
        assert enqueued or logged, (
            f"row {row.name!r} produced NEITHER a stream entry NOR a logged DropReason. "
            f"That is a silent swallow -- the exact defect #2006 closes. Expected "
            f"{row.expected}. Why this row exists: {row.why}"
        )

    # --- an exception after Bolt's ack looks identical to a silent drop ------
    assert harness.errors == [], (
        f"row {row.name!r} raised inside the listener: {harness.errors!r}. Bolt acks "
        f"the envelope before running the body and swallows the exception into its own "
        f"error handler, so this failure is invisible to the two checks above."
    )

    if row.expected is Disposition.ENQUEUED:
        assert enqueued == 1, f"row {row.name!r} must enqueue exactly one turn. {row.why}"
        assert logged == [], f"row {row.name!r} enqueued AND logged a drop: {logged}"
        queued = from_stream_fields(entries[-1][1])
        for fragment in row.text_contains:
            assert fragment in queued.text, (
                f"row {row.name!r}: enqueued turn text is missing {fragment!r}. "
                f"Text was {queued.text!r}. {row.why}"
            )
        for fragment in row.text_not_contains:
            assert fragment not in queued.text, (
                f"row {row.name!r}: enqueued turn text must not contain {fragment!r}. "
                f"Text was {queued.text!r}. {row.why}"
            )
    elif row.expected is Disposition.DROPPED_BY_BOLT:
        assert enqueued == 0, f"row {row.name!r} must not enqueue. {row.why}"
        assert harness.records == [], (
            f"row {row.name!r} must be refused by slack_bolt BEFORE any listener runs, "
            f"so the dispatcher's own logger must have recorded nothing at all. "
            f"Records: {[r.getMessage() for r in harness.records]}. {row.why}"
        )
    else:
        assert enqueued == 0, f"row {row.name!r} must not enqueue. {row.why}"
        assert logged == [row.expected], (
            f"row {row.name!r} must log exactly one drop naming {row.expected}; "
            f"logged {logged}. Messages: {[r.getMessage() for r in harness.records]}. "
            f"{row.why}"
        )

    if row.dedupe_id is not None:
        # These rows refuse BEFORE the idempotency claim, so no key may be burned:
        # a burned key would linger for the TTL and drop Slack's redelivery too.
        assert redis_client.exists(config.dedupe_key(row.dedupe_id)) == 0, row.why


def test_a_non_empty_top_level_text_is_derived_before_nexts_self_mention_policy(
    redis_client: redis.Redis, config: DispatcherConfig
) -> None:
    """The negative control for AC 1: derivation must not rewrite authored text.

    The next release train already strips its own Slack mention after text
    derivation (#1525), so the end-to-end queue value reflects that established
    policy. The dedicated inbound-text tests retain the byte-identical contract
    at the derivation seam itself; this assertion makes the forward merge prove
    that main's Block Kit fallback composes with next's mention normalization.
    """
    original = "  <@U0BOT> deploy prod  "
    harness = _build_harness(config, redis_client)
    harness.handler.handle(
        harness.sock,
        _events_api_request(
            "env-identity", "Ev-identity", _mention(text=original, blocks=_alert_blocks())
        ),
    )
    _drain(harness.app)

    assert harness.errors == []
    entries = _stream_entries(redis_client, config)
    assert len(entries) == 1
    assert from_stream_fields(entries[0][1]).text == "deploy prod"


# ---------------------------------------------------------------------------
# The claim-release regression (EB-20 / EB-21 / EB-22)
# ---------------------------------------------------------------------------
#
# HONEST CAVEAT, recorded so nobody reads more into these three tests than they
# prove. In Socket Mode, Bolt acks the envelope BEFORE running the listener body
# (slack_bolt/listener/thread_runner.py, ThreadListenerRunner.run: `ack()` is
# called for auto-acknowledged Events API listeners, then the body is submitted
# to the executor). Slack has therefore already been told "received", so a Slack
# redelivery of the failed event is RARE -- it is not the normal consequence of
# the listener raising. Releasing the claim restores idempotency *correctness*:
# the adapter no longer holds a dedupe claim for work it never did, so any later
# delivery of that event id -- a Slack retry, a replay, an operator-driven
# re-send -- can still be processed. It is not a guarantee that every failed
# placeholder is recovered.


def _raising_post_message(exc: BaseException) -> Any:
    def _raise(**_kwargs: object) -> None:
        raise exc

    return _raise


def test_a_failed_placeholder_releases_the_claim_on_the_event_lane(
    redis_client: redis.Redis, config: DispatcherConfig
) -> None:
    """`claim_event` runs before `chat_postMessage`; if the post raises, the key
    must not stay claimed, or the event is terminally lost -- #2006's own class."""
    boom = SlackApiError("ratelimited", {"ok": False, "error": "ratelimited"})
    failing = _build_harness(config, redis_client, chat_post_message=_raising_post_message(boom))

    failing.handler.handle(
        failing.sock, _events_api_request("env-boom", "Ev-boom", _mention(text="please answer"))
    )
    _drain(failing.app)

    # Bolt acked first and then swallowed the exception into its error handler:
    # without this collector the failure is invisible from the outside.
    assert failing.sock.acked_envelope_ids == ["env-boom"]
    assert len(failing.errors) == 1, failing.errors
    assert _stream_entries(redis_client, config) == []

    # (a) The claim was released.
    assert redis_client.exists(config.dedupe_key("Ev-boom")) == 0, (
        "the dedupe claim survived a failed placeholder, so any later delivery of "
        "Ev-boom is refused as a duplicate and the message is lost for good"
    )

    # (b) A redelivery of the SAME event id now succeeds.
    retry = _build_harness(config, redis_client)
    retry.handler.handle(
        retry.sock, _events_api_request("env-boom-2", "Ev-boom", _mention(text="please answer"))
    )
    _drain(retry.app)

    assert retry.errors == []
    entries = _stream_entries(redis_client, config)
    assert len(entries) == 1
    assert from_stream_fields(entries[0][1]).text == "please answer"


def test_a_failed_placeholder_releases_the_claim_on_the_action_lane(
    redis_client: redis.Redis, config: DispatcherConfig
) -> None:
    """The identical defect on the sibling mint site. `process_action` claims and
    posts in the same order, so fixing one lane and not its twin would leave
    button clicks silently lost -- this repo's dominant drift shape."""
    boom = SlackApiError("ratelimited", {"ok": False, "error": "ratelimited"})
    failing = _build_harness(config, redis_client, chat_post_message=_raising_post_message(boom))

    click = _block_action_body(
        actions=[{"type": "button", "action_id": "reports", "action_ts": "1.5"}],
        trigger_id="trig-boom",
    )
    failing.handler.handle(failing.sock, _interactive_request("env-click-boom", click))
    _drain(failing.app)

    assert len(failing.errors) == 1, failing.errors
    assert _stream_entries(redis_client, config) == []
    assert redis_client.exists(config.dedupe_key("action-trig-boom")) == 0, (
        "the dedupe claim survived a failed placeholder on the action lane"
    )

    retry = _build_harness(config, redis_client)
    retry.handler.handle(retry.sock, _interactive_request("env-click-retry", click))
    _drain(retry.app)

    assert retry.errors == []
    entries = _stream_entries(redis_client, config)
    assert len(entries) == 1
    assert from_stream_fields(entries[0][1]).text == "reports"


def test_a_failed_enqueue_after_a_successful_placeholder_keeps_the_claim(
    redis_client: redis.Redis, config: DispatcherConfig, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The deliberate asymmetry, pinned so a later 'consistency' refactor fails.

    Once the placeholder is posted, something user-visible has happened. Releasing
    the claim at that point would let a redelivery post a SECOND placeholder into
    the same thread -- trading an invisible failure for a visible, confusing one.
    So the release is legitimate only before any user-visible side effect.

    The broker failure is forced on the real Valkey client rather than faked:
    nothing about Valkey's behavior is mocked, one call is made to raise.
    """
    post_message = MagicMock(return_value={"ts": BOT_TS})
    harness = _build_harness(config, redis_client, chat_post_message=post_message)

    def _xadd_fails(*_args: object, **_kwargs: object) -> str:
        raise redis.RedisError("stream unavailable")

    monkeypatch.setattr(redis_client, "xadd", _xadd_fails)

    harness.handler.handle(
        harness.sock, _events_api_request("env-xadd", "Ev-xadd", _mention(text="hello"))
    )
    _drain(harness.app)

    assert len(harness.errors) == 1, harness.errors
    # The placeholder DID reach the channel; the user has already seen it.
    assert post_message.call_count == 1
    assert redis_client.exists(config.dedupe_key("Ev-xadd")) == 1, (
        "the claim must be HELD after a successful placeholder: releasing it would "
        "let Slack's redelivery post a second placeholder in the same thread"
    )


# ---------------------------------------------------------------------------
# The other half of the release rule: ONLY an unambiguous refusal releases
# ---------------------------------------------------------------------------
#
# The three tests above all fail the placeholder with a `SlackApiError` carrying
# a real Slack error code, which is the one case where the claim is released.
# That is a narrow evidence claim, not "the call raised": Slack answered, named
# its refusal, and delivered nothing, so a later retry is clean.
#
# Everything else is ambiguous and KEEPS the claim. Two shapes are pinned below.
#
#   * A transport failure (`SlackRequestError`, a timeout, a dropped connection)
#     carries no answer from Slack at all -- the request may well have been
#     accepted before the connection died.
#   * `fatal_error` wears an error code but is not a refusal. Slack documents it
#     on chat.postMessage as "The server could not complete your operation(s)
#     without encountering a catastrophic error. It's possible some aspect of
#     the operation succeeded before the error was raised."
#     (Slack: docs.slack.dev/reference/methods/chat.postMessage, errors table.)
#
# In both cases a placeholder may already be sitting in the thread, so releasing
# would let a replay post a SECOND one -- exactly the duplicate the
# claim-before-placeholder ordering exists to prevent. Keeping the claim is the
# quieter of two imperfect outcomes, and it is a deliberate choice rather than
# the #2006 bug reappearing, which is why it is pinned here.


def test_an_ambiguous_transport_failure_keeps_the_claim_on_the_event_lane(
    redis_client: redis.Redis, config: DispatcherConfig
) -> None:
    """A failure with no answer from Slack must NOT release: the post may have
    landed, and a released claim would let a replay post a second placeholder."""
    boom = SlackRequestError("the connection died mid-request")
    failing = _build_harness(config, redis_client, chat_post_message=_raising_post_message(boom))

    failing.handler.handle(
        failing.sock,
        _events_api_request("env-transport", "Ev-transport", _mention(text="please answer")),
    )
    _drain(failing.app)

    assert failing.sock.acked_envelope_ids == ["env-transport"]
    assert len(failing.errors) == 1, failing.errors
    assert _stream_entries(redis_client, config) == []
    assert redis_client.exists(config.dedupe_key("Ev-transport")) == 1, (
        "a transport failure released the dedupe claim: Slack never answered, so the "
        "placeholder may already be in the thread and a replay would post a second one"
    )

    # The user-visible consequence of holding the claim, asserted rather than
    # inferred from the key: a redelivery of the same event id is refused with an
    # enumerated reason instead of posting into the thread a second time.
    retry = _build_harness(config, redis_client)
    retry.handler.handle(
        retry.sock,
        _events_api_request("env-transport-2", "Ev-transport", _mention(text="please answer")),
    )
    _drain(retry.app)

    assert retry.errors == []
    assert _stream_entries(redis_client, config) == []
    assert _drop_reasons_logged(retry.records) == [DropReason.DUPLICATE_DELIVERY]


def test_a_fatal_error_response_keeps_the_claim_on_the_event_lane(
    redis_client: redis.Redis, config: DispatcherConfig
) -> None:
    """`fatal_error` is an error code, but Slack's own chat.postMessage docs say
    "some aspect of the operation succeeded before the error was raised" -- so it
    is an ambiguous outcome, and treating it as a refusal would release a claim
    whose placeholder may already be visible."""
    boom = SlackApiError("fatal_error", {"ok": False, "error": "fatal_error"})
    failing = _build_harness(config, redis_client, chat_post_message=_raising_post_message(boom))

    failing.handler.handle(
        failing.sock,
        _events_api_request("env-fatal", "Ev-fatal", _mention(text="please answer")),
    )
    _drain(failing.app)

    assert len(failing.errors) == 1, failing.errors
    assert _stream_entries(redis_client, config) == []
    assert redis_client.exists(config.dedupe_key("Ev-fatal")) == 1, (
        "`fatal_error` released the dedupe claim, but Slack documents it as possibly "
        "having succeeded in part -- a replay could post a second placeholder"
    )


def test_an_ambiguous_transport_failure_keeps_the_claim_on_the_action_lane(
    redis_client: redis.Redis, config: DispatcherConfig
) -> None:
    """The identical rule on the sibling mint site: `process_action` shares
    `_post_placeholder`, and a lane-specific divergence here is this repo's
    dominant drift shape."""
    boom = SlackRequestError("the connection died mid-request")
    failing = _build_harness(config, redis_client, chat_post_message=_raising_post_message(boom))

    click = _block_action_body(
        actions=[{"type": "button", "action_id": "reports", "action_ts": "1.5"}],
        trigger_id="trig-transport",
    )
    failing.handler.handle(failing.sock, _interactive_request("env-click-transport", click))
    _drain(failing.app)

    assert len(failing.errors) == 1, failing.errors
    assert _stream_entries(redis_client, config) == []
    assert redis_client.exists(config.dedupe_key("action-trig-transport")) == 1, (
        "a transport failure released the dedupe claim on the action lane"
    )

    retry = _build_harness(config, redis_client)
    retry.handler.handle(retry.sock, _interactive_request("env-click-transport-2", click))
    _drain(retry.app)

    assert retry.errors == []
    assert _stream_entries(redis_client, config) == []
    assert _drop_reasons_logged(retry.records) == [DropReason.DUPLICATE_DELIVERY]


def test_a_fatal_error_response_keeps_the_claim_on_the_action_lane(
    redis_client: redis.Redis, config: DispatcherConfig
) -> None:
    """`fatal_error`'s ambiguity is a property of Slack's Web API, not of a lane,
    so the action lane must hold its claim for exactly the same reason."""
    boom = SlackApiError("fatal_error", {"ok": False, "error": "fatal_error"})
    failing = _build_harness(config, redis_client, chat_post_message=_raising_post_message(boom))

    click = _block_action_body(
        actions=[{"type": "button", "action_id": "reports", "action_ts": "1.5"}],
        trigger_id="trig-fatal",
    )
    failing.handler.handle(failing.sock, _interactive_request("env-click-fatal", click))
    _drain(failing.app)

    assert len(failing.errors) == 1, failing.errors
    assert _stream_entries(redis_client, config) == []
    assert redis_client.exists(config.dedupe_key("action-trig-fatal")) == 1, (
        "`fatal_error` released the dedupe claim on the action lane"
    )


# ---------------------------------------------------------------------------
# AC 2 -- the drop contract
# ---------------------------------------------------------------------------


def test_every_drop_reason_carries_a_documented_rationale() -> None:
    """A reason with no documented rationale is exactly the undocumented drop
    AC 2 forbids, so the mapping is asserted total in both directions."""
    assert set(DROP_RATIONALES) == set(DropReason), (
        "DROP_RATIONALES must have one entry per DropReason and no orphans"
    )
    for reason in DropReason:
        assert DROP_RATIONALES[reason].strip(), f"{reason} has an empty rationale"


def test_every_drop_reason_is_exercised_by_a_matrix_row() -> None:
    """The mechanical half of AC 2: adding a DropReason without adding a matrix
    row for it fails the suite, so a new adapter-level refusal cannot ship
    without an end-to-end demonstration of what it refuses and why."""
    exercised = {row.expected for row in MATRIX if isinstance(row.expected, DropReason)}
    missing = set(DropReason) - exercised
    assert not missing, f"DropReason members with no matrix row: {sorted(missing)}"
    assert exercised == set(DropReason)
