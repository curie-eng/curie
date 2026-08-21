"""End-to-end dispatch through Bolt's real Socket Mode handler, offline.

We drive ``SocketModeHandler.handle`` with a fake socket client and a mocked Web
API client (the only two things faked, per test discipline), and assert the full
lifecycle step: envelope -> ack -> in-thread placeholder -> XADD to real Valkey.
"""

import threading
import time
from typing import Any
from unittest.mock import MagicMock

import redis
from curie_dispatcher.app import build_app
from curie_dispatcher.config import DispatcherConfig
from curie_dispatcher.queue import from_stream_fields
from slack_bolt import App
from slack_bolt.adapter.socket_mode import SocketModeHandler
from slack_sdk.socket_mode.request import SocketModeRequest
from slack_sdk.web import WebClient

from .conftest import FakeSocketClient, _authorize

BOT_TS = "555.000"


def _build(config: DispatcherConfig, redis_client: redis.Redis) -> tuple[App, WebClient]:
    web_client = WebClient(token="xoxb-test")
    web_client.chat_postMessage = MagicMock(return_value={"ts": BOT_TS})  # type: ignore[method-assign]
    app = build_app(
        config,
        web_client=web_client,
        redis_client=redis_client,
        authorize=_authorize,
    )
    return app, web_client


def _drain(app: App) -> None:
    """Wait for Bolt's async listeners (run on a thread pool for fast ack) to finish.

    Production acks the envelope immediately and processes in the background; this
    drains that background work so assertions are deterministic.
    """
    app.listener_runner.listener_executor.shutdown(wait=True)


def _events_api_request(
    envelope_id: str,
    event_id: str,
    event: dict[str, Any],
) -> SocketModeRequest:
    return SocketModeRequest(
        type="events_api",
        envelope_id=envelope_id,
        payload={
            "type": "event_callback",
            "event_id": event_id,
            "team_id": "T1",
            "event": event,
        },
    )


def _mention_event(text: str = "hi there") -> dict[str, Any]:
    return {
        "type": "app_mention",
        "channel": "C123",
        "user": "U123",
        "text": text,
        "ts": "1700.0001",
    }


def _block_action_request(
    envelope_id: str,
    *,
    action_id: str = "reports",
    value: str | None = None,
    trigger_id: str | None = None,
    message: dict[str, Any] | None = None,
) -> SocketModeRequest:
    action: dict[str, Any] = {"type": "button", "action_id": action_id, "action_ts": "1.5"}
    if value is not None:
        action["value"] = value
    return SocketModeRequest(
        type="interactive",
        envelope_id=envelope_id,
        payload={
            "type": "block_actions",
            "trigger_id": trigger_id or f"trig-{envelope_id}",
            "team": {"id": "T1"},
            "user": {"id": "U123"},
            "api_app_id": "A1",
            "token": "verif",
            "container": {"type": "message", "message_ts": "1700.0001"},
            "channel": {"id": "C123"},
            "message": message or {"ts": "1700.0001", "thread_ts": "1700.0001"},
            "actions": [action],
        },
    )


def test_envelope_acked_placeholder_posted_and_enqueued(
    redis_client: redis.Redis, config: DispatcherConfig
) -> None:
    app, web_client = _build(config, redis_client)
    handler = SocketModeHandler(app, app_token="xapp-test")
    sock = FakeSocketClient()

    handler.handle(sock, _events_api_request("env-1", "Ev-100", _mention_event()))

    # 1) The envelope was acked (fast-ack path), before background work completes.
    assert sock.acked_envelope_ids == ["env-1"]

    _drain(app)

    # 2) A placeholder was posted in-thread (root ts becomes the thread key).
    web_client.chat_postMessage.assert_called_once_with(
        channel="C123", thread_ts="1700.0001", text=config.placeholder_text
    )

    # 3) Exactly one job was enqueued, carrying the placeholder ts for the worker.
    assert redis_client.xlen(config.stream) == 1
    _, fields = redis_client.xrange(config.stream)[0]
    queued = from_stream_fields(fields)
    assert queued.event_id == "Ev-100"
    assert queued.reply_handle.channel == "C123"
    assert queued.author == "U123"
    assert queued.text == "hi there"
    assert queued.conversation_id == "1700.0001"
    assert queued.reply_handle.placeholder == BOT_TS


def test_the_dispatcher_makes_no_assistant_status_call(
    redis_client: redis.Redis, config: DispatcherConfig
) -> None:
    """The shimmer left this side entirely (#1312).

    It used to be set right here, between the placeholder and the XADD. That put
    a best-effort cosmetic call -- one whose own failures are swallowed at debug
    -- in front of the only moment a turn becomes durable. The worker raises and
    lowers the caption now, so nothing on the ingest path can be slow on its
    behalf.
    """
    app, web_client = _build(config, redis_client)
    web_client.assistant_threads_setStatus = MagicMock()  # type: ignore[method-assign]
    handler = SocketModeHandler(app, app_token="xapp-test")
    sock = FakeSocketClient()

    handler.handle(sock, _events_api_request("env-1", "Ev-shim", _mention_event()))
    _drain(app)

    web_client.assistant_threads_setStatus.assert_not_called()
    # The placeholder and the enqueue are untouched by the removal.
    assert redis_client.xlen(config.stream) == 1


def test_a_slow_status_call_cannot_delay_the_enqueue(
    redis_client: redis.Redis, config: DispatcherConfig
) -> None:
    """AC2, asserted on the clock rather than on a call count.

    The Slack client here would block far past slack_sdk's own worst case (about
    4.5s once its ConnectionErrorRetryHandler is counted) if anything on this
    path still called it. Bolt runs listeners on five shared workers, so five
    such calls used to be enough to stall ingestion with no visible explanation.
    The turn must reach Valkey regardless -- and quickly, because nothing on this
    path calls Slack for a status at all any more.
    """
    stalled = threading.Event()

    def _never_returns_in_time(**_kwargs: object) -> None:
        stalled.set()
        time.sleep(30)

    app, web_client = _build(config, redis_client)
    web_client.assistant_threads_setStatus = _never_returns_in_time  # type: ignore[method-assign]
    handler = SocketModeHandler(app, app_token="xapp-test")
    sock = FakeSocketClient()

    started = time.monotonic()
    handler.handle(sock, _events_api_request("env-1", "Ev-slow-status", _mention_event()))
    _drain(app)
    elapsed = time.monotonic() - started

    assert redis_client.xlen(config.stream) == 1, "the turn must be durable"
    assert not stalled.is_set(), "nothing on the ingest path may call setStatus"
    # Generous by design: this is a regression guard against a multi-second
    # blocking call reappearing here, not a latency benchmark on CI hardware.
    assert elapsed < 5.0, f"enqueue waited {elapsed:.1f}s on a cosmetic call"


def test_duplicate_delivery_enqueues_exactly_once(
    redis_client: redis.Redis, config: DispatcherConfig
) -> None:
    app, web_client = _build(config, redis_client)
    handler = SocketModeHandler(app, app_token="xapp-test")
    sock = FakeSocketClient()

    # Same Slack event id delivered twice (a Slack retry).
    req_first = _events_api_request("env-1", "Ev-dup", _mention_event())
    req_retry = _events_api_request("env-2", "Ev-dup", _mention_event())
    handler.handle(sock, req_first)
    handler.handle(sock, req_retry)
    _drain(app)

    # Both envelopes are acked, but only one job is enqueued and one placeholder posted.
    assert sock.acked_envelope_ids == ["env-1", "env-2"]
    assert web_client.chat_postMessage.call_count == 1
    assert redis_client.xlen(config.stream) == 1


def test_message_in_dm_is_enqueued(
    redis_client: redis.Redis, config: DispatcherConfig
) -> None:
    app, _ = _build(config, redis_client)
    handler = SocketModeHandler(app, app_token="xapp-test")
    sock = FakeSocketClient()

    dm_event = {
        "type": "message",
        "channel_type": "im",
        "channel": "D1",
        "user": "U9",
        "text": "dm to the bot",
        "ts": "1800.0001",
    }
    handler.handle(sock, _events_api_request("env-1", "Ev-dm", dm_event))
    _drain(app)

    assert redis_client.xlen(config.stream) == 1


def test_message_in_channel_is_ignored(
    redis_client: redis.Redis, config: DispatcherConfig
) -> None:
    app, _ = _build(config, redis_client)
    handler = SocketModeHandler(app, app_token="xapp-test")
    sock = FakeSocketClient()

    channel_event = {
        "type": "message",
        "channel_type": "channel",
        "channel": "C1",
        "user": "U9",
        "text": "just chatting, not for the bot",
        "ts": "1900.0001",
    }
    handler.handle(sock, _events_api_request("env-1", "Ev-chan", channel_event))
    _drain(app)

    # Ordinary channel chatter is acked but never enqueued.
    assert sock.acked_envelope_ids == ["env-1"]
    assert redis_client.xlen(config.stream) == 0


def test_button_click_enqueues_a_turn(
    redis_client: redis.Redis, config: DispatcherConfig
) -> None:
    app, web_client = _build(config, redis_client)
    handler = SocketModeHandler(app, app_token="xapp-test")
    sock = FakeSocketClient()

    handler.handle(sock, _block_action_request("env-1", action_id="reports"))
    _drain(app)

    # A placeholder is posted in the clicked message's thread, and one turn is
    # enqueued whose text is the button's command (its action_id here).
    web_client.chat_postMessage.assert_called_once_with(
        channel="C123", thread_ts="1700.0001", text=config.placeholder_text
    )
    assert redis_client.xlen(config.stream) == 1
    _, fields = redis_client.xrange(config.stream)[0]
    queued = from_stream_fields(fields)
    assert queued.text == "reports"
    assert queued.reply_handle.channel == "C123"
    assert queued.conversation_id == "1700.0001"
    assert queued.reply_handle.placeholder == BOT_TS
    assert queued.event_id == "action-trig-env-1"


def test_both_mint_sites_stamp_kind_slack_and_no_adapter(
    redis_client: redis.Redis, config: DispatcherConfig
) -> None:
    """T-A17 / AC1 (plan EB-A19, round-5 item 2). The Slack mint, BOTH lanes.

    The dispatcher has two `QueuedTurn` producers -- the message handler and the
    block-action handler -- and they are the sibling pair this repo's dominant
    bug class drifts apart: a `kind` added to one lane leaves the other minting a
    turn a 0.3.0 worker dead-letters, and only for button clicks. Both are driven
    here, in one test, so neither can be fixed alone.

    `kind` is the literal `"slack"`, not config-derived: a Socket Mode dispatcher
    that could claim another kind is itself a misrouting vector. `adapter` is
    None because Slack's egress route is the worker's configured origin (D4.4),
    and it is asserted rather than left unmentioned so a later change cannot
    quietly start stamping a slug the worker would then look a credential up
    under.
    """

    app, _ = _build(config, redis_client)
    handler = SocketModeHandler(app, app_token="xapp-test")
    sock = FakeSocketClient()

    handler.handle(sock, _events_api_request("env-msg", "Ev-kind-1", _mention_event()))
    handler.handle(sock, _block_action_request("env-btn", action_id="reports"))
    _drain(app)

    entries = redis_client.xrange(config.stream)
    assert len(entries) == 2, entries
    minted = [from_stream_fields(fields) for _, fields in entries]
    by_event = {turn.event_id: turn for turn in minted}
    assert set(by_event) == {"Ev-kind-1", "action-trig-env-btn"}, by_event.keys()

    for event_id, turn in by_event.items():
        assert turn.reply_handle.kind == "slack", event_id
        assert turn.reply_handle.adapter is None, event_id


CHANNEL_A = "C0EXAMPLE1"
CHANNEL_B = "C0EXAMPLE2"


def test_the_mint_keeps_the_thread_ts_bare_on_every_channel(
    redis_client: redis.Redis, config: DispatcherConfig
) -> None:
    """BASELINE-GREEN. The conversation id on the wire is Slack's own thread_ts.

    A Slack ``thread_ts`` is unique only within a channel, so with one agent
    bound to two channels the same id can arrive for two unrelated
    conversations. The tempting fix is to make it unique HERE, by folding the
    channel into the minted ``conversation_id``. That fix is wrong and this test
    is what refuses it: the worker sends ``ReplyTarget.conversation_id`` straight
    back to Slack as ``thread_ts`` (``slack_sink.py``), so a channel-scoped id
    addresses a thread that does not exist and every reply is lost. The
    disambiguation belongs in the worker's own keys, where it never reaches a
    Slack API call.

    Both channels are driven with the SAME ``ts`` -- the collision itself -- so
    the pin is that the mint is channel-independent, not merely that one
    conversation id happens to round-trip.
    """

    app, web_client = _build(config, redis_client)
    handler = SocketModeHandler(app, app_token="xapp-test")
    sock = FakeSocketClient()

    thread_ts = "1700.0001"
    for index, channel in enumerate((CHANNEL_A, CHANNEL_B)):
        event = {
            "type": "app_mention",
            "channel": channel,
            "user": "U123",
            "text": "hi there",
            "ts": thread_ts,
        }
        handler.handle(sock, _events_api_request(f"env-bare-{index}", f"Ev-bare-{index}", event))
    _drain(app)

    entries = redis_client.xrange(config.stream)
    assert len(entries) == 2, entries
    minted = [from_stream_fields(fields) for _, fields in entries]
    by_channel = {turn.reply_handle.channel: turn for turn in minted}
    assert set(by_channel) == {CHANNEL_A, CHANNEL_B}, by_channel.keys()

    for channel, turn in by_channel.items():
        # Bare and identical across the two channels: the id is Slack's, verbatim.
        assert turn.conversation_id == thread_ts, channel
        assert channel not in turn.conversation_id, channel
        # The reply handle is likewise bare -- the placeholder is the raw ts of
        # the message the worker will edit, nothing composed around it.
        assert turn.reply_handle.placeholder == BOT_TS, channel

    # And the placeholder itself was posted into the bare thread on each channel.
    posted = [
        (call.kwargs["channel"], call.kwargs["thread_ts"])
        for call in web_client.chat_postMessage.call_args_list
    ]
    assert posted == [(CHANNEL_A, thread_ts), (CHANNEL_B, thread_ts)]


def test_button_click_prefers_value_over_action_id(
    redis_client: redis.Redis, config: DispatcherConfig
) -> None:
    app, _ = _build(config, redis_client)
    handler = SocketModeHandler(app, app_token="xapp-test")
    sock = FakeSocketClient()

    handler.handle(sock, _block_action_request("env-1", action_id="btn", value="show top 5"))
    _drain(app)

    _, fields = redis_client.xrange(config.stream)[0]
    assert from_stream_fields(fields).text == "show top 5"


def test_duplicate_click_enqueues_exactly_once(
    redis_client: redis.Redis, config: DispatcherConfig
) -> None:
    app, web_client = _build(config, redis_client)
    handler = SocketModeHandler(app, app_token="xapp-test")
    sock = FakeSocketClient()

    # Same interaction (trigger_id) redelivered: dedupe drops the second.
    handler.handle(sock, _block_action_request("env-1", trigger_id="trig-dup"))
    handler.handle(sock, _block_action_request("env-2", trigger_id="trig-dup"))
    _drain(app)

    assert web_client.chat_postMessage.call_count == 1
    assert redis_client.xlen(config.stream) == 1


def _home_tab_action_request(
    envelope_id: str,
    *,
    action_id: str = "reports",
    trigger_id: str | None = None,
) -> SocketModeRequest:
    """A block action from an App Home tab: container is a view, and the payload
    carries no ``channel`` and no ``message`` (the shape that KeyErrored)."""
    return SocketModeRequest(
        type="interactive",
        envelope_id=envelope_id,
        payload={
            "type": "block_actions",
            "trigger_id": trigger_id or f"trig-{envelope_id}",
            "team": {"id": "T1"},
            "user": {"id": "U123"},
            "api_app_id": "A1",
            "token": "verif",
            "container": {"type": "view", "view_id": "V1"},
            "view": {"id": "V1", "type": "home"},
            "actions": [{"type": "button", "action_id": action_id, "action_ts": "1.5"}],
        },
    )


def test_channel_less_action_is_skipped_without_burning_idempotency_key(
    redis_client: redis.Redis, config: DispatcherConfig
) -> None:
    app, web_client = _build(config, redis_client)
    handler = SocketModeHandler(app, app_token="xapp-test")
    sock = FakeSocketClient()

    # A Home-tab click (no channel, no message) must not KeyError, must not post a
    # placeholder, and must not enqueue -- there is no thread to answer in.
    handler.handle(sock, _home_tab_action_request("env-1", trigger_id="trig-home"))
    _drain(app)

    assert sock.acked_envelope_ids == ["env-1"]  # Bolt still acked the envelope
    assert web_client.chat_postMessage.call_count == 0
    assert redis_client.xlen(config.stream) == 0

    # The idempotency key was NOT claimed: no dedupe key was written, so a Slack
    # redelivery of this interaction is not silently dropped. (A burned key here
    # would linger for the TTL and drop the redelivery.)
    dedupe_key = f"{config.dedupe_prefix}action-trig-home"
    assert redis_client.exists(dedupe_key) == 0


def test_bot_authored_message_is_ignored(
    redis_client: redis.Redis, config: DispatcherConfig
) -> None:
    app, _ = _build(config, redis_client)
    handler = SocketModeHandler(app, app_token="xapp-test")
    sock = FakeSocketClient()

    # The dispatcher's own placeholder shows up as a bot message; it must not loop.
    bot_event = {
        "type": "message",
        "channel_type": "im",
        "channel": "D1",
        "bot_id": "B1",
        "text": "Working on it.",
        "ts": "2000.0001",
    }
    handler.handle(sock, _events_api_request("env-1", "Ev-bot", bot_event))
    _drain(app)

    assert redis_client.xlen(config.stream) == 0
