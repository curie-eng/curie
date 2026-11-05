"""Quote earlier one to one direct messages into a top-level turn.

The history filter is the boundary on whose messages may be quoted. Slack is
faked at the WebClient. Valkey is the shared dispatcher fixture. Selection and
rendering are also checked on their own.

conversations.history returns newest first. Each message carries type, user,
text, ts, and team, and may carry bot_id and subtype. These tests also send
client_msg_id, which the selector ignores. The page may carry has_more and
response_metadata.
https://docs.slack.dev/reference/methods/conversations.history
The same message shape is what FakeSlack serves in
apps/api/tests/test_channel_read.py.

A root direct message often has no thread_ts. Slack also echoes thread_ts equal
to ts on a root. A reply is the only case where thread_ts differs from ts.
is_ext_shared_channel is a field of the event callback envelope, not of the
message event. slack_evidence in apps/dispatcher/src/curie_dispatcher/identity.py
reads it from the body. The admitted direct message in
test_identity_dispatch.test_a_direct_message_to_an_identity_mints_that_identity
has neither field on the event.
"""

from __future__ import annotations

import html
import json
import logging
from decimal import Decimal
from typing import Any
from unittest.mock import MagicMock

import pytest
import redis
from curie_dispatcher.config import DispatcherConfig
from curie_dispatcher.dm_continuity import (
    SlackDmContinuity,
    render_dm_continuity,
    select_dm_messages,
)
from pydantic import ValidationError
from slack_sdk.errors import SlackApiError
from slack_sdk.socket_mode.request import SocketModeRequest
from slack_sdk.web import WebClient
from slack_sdk.web.slack_response import SlackResponse

from .test_config import _clear_all_config_env, _set_valid_chat_attester_secret
from .test_http_receiver import BOT, SENDER, HttpRig, api, receiving
from .test_http_receiver_direct_messages import dm_bindings, dm_native
from .test_http_receiver_dispatch import DM_CHANNEL, _deliver_selected
from .test_identity_dispatch import _mention, _ops

# The ops connection authorizes U0OPS / B0OPS. The selected HTTP listener
# authorizes U0BOT / B1 through conftest._authorize. History rows use those ids.
LEGACY_BOT_USER = "U0OPS"
LEGACY_BOT_ID = "B0OPS"
HTTP_BOT_ID = "B1"

CURRENT_TS = "1700000000.000400"
CURRENT_TEXT = "what is the status"
PERSON_TEXT = "see status a/b <note>"
AGENT_TEXT = "earlier agent note"
REPLY_TEXT = "only the reply"
THREAD_TS = "1699900000.000100"
_WEEK_SECONDS = 7 * 24 * 60 * 60
FOREIGN_USER = "U0OTHER"
FOREIGN_CHANNEL = "C0EXAMPLE1"
FOREIGN_BOT = "B0EXAMPLEOTHER"

# Comfortably inside the 7 day window that opens at CURRENT_TS, and clearly
# outside it. 1699395200.000400 is exactly 7 days earlier.
INSIDE_AGENT_TS = "1699500000.000400"
INSIDE_PERSON_TS = "1699800000.000400"
OUTSIDE_TS = "1699000000.000400"
JUST_OUTSIDE_TS = "1699395199.000400"
FUTURE_TS = "1700000001.000400"


def _escaped(text: str) -> str:
    return html.escape(text, quote=False).replace("/", "&#x2F;")


def _shift(ts: str, seconds: int) -> str:
    micros = int((Decimal(ts) + Decimal(seconds)) * Decimal(1_000_000))
    return f"{micros // 1_000_000}.{micros % 1_000_000:06d}"


def _history_page(*, agent_user: str, agent_bot_id: str) -> dict[str, Any]:
    """Newest first, as conversations.history returns a page."""

    return {
        "ok": True,
        "has_more": False,
        "pin_count": 0,
        "response_metadata": {"next_cursor": ""},
        "messages": [
            _slack_message(CURRENT_TS, CURRENT_TEXT, user=SENDER, msg_id="c0current00"),
            _slack_message(
                _shift(CURRENT_TS, -86_400),
                PERSON_TEXT,
                user=SENDER,
                msg_id="c0person000",
            ),
            _slack_message(
                _shift(CURRENT_TS, -2 * 86_400),
                AGENT_TEXT,
                user=agent_user,
                bot_id=agent_bot_id,
                msg_id="c0agent0000",
            ),
        ],
    }


def _history_response(page: dict[str, Any]) -> SlackResponse:
    """What ``WebClient.conversations_history`` returns. The page is on ``data``."""

    return SlackResponse(
        client=None,
        http_verb="GET",
        api_url="https://slack.com/api/conversations.history",
        req_args={},
        data=page,
        headers={},
        status_code=200,
    )


def _history_client(result: Any) -> WebClient:
    client = WebClient(token="xoxb-test")
    if isinstance(result, BaseException):
        mocked = MagicMock(side_effect=result)
    else:
        # A decoded page is wrapped in the SlackResponse WebClient returns.
        value = _history_response(result) if isinstance(result, dict) else result
        mocked = MagicMock(return_value=value)
    client.conversations_history = mocked  # type: ignore[method-assign]
    return client


def _legacy_event(**changes: Any) -> dict[str, Any]:
    """A top-level im. thread_ts is absent unless a test sets it."""

    event: dict[str, Any] = {
        "type": "message",
        "channel_type": "im",
        "channel": DM_CHANNEL,
        "user": SENDER,
        "text": CURRENT_TEXT,
        "ts": CURRENT_TS,
    }
    event.update(changes)
    return event


def _run_legacy(
    config: DispatcherConfig,
    redis_client: redis.Redis,
    event: dict[str, Any],
    *,
    event_id: str,
    history: Any,
) -> WebClient:
    conn = _ops(config, redis_client)
    client = _history_client(history)
    # The connection already holds the WebClient build_app will call.
    mocked = client.conversations_history
    conn.web_client.conversations_history = mocked  # type: ignore[method-assign]
    conn.handle(
        SocketModeRequest(
            type="events_api",
            envelope_id="env-" + event_id,
            payload={
                "type": "event_callback",
                "event_id": event_id,
                "team_id": "T1",
                "authorizations": receiving(),
                "is_ext_shared_channel": False,
                "event": event,
            },
        )
    )
    conn.drain()
    return conn.web_client


def _queued_text(redis_client: redis.Redis, config: DispatcherConfig) -> str:
    _entry_id, fields = redis_client.xrange(config.stream)[0]
    text = json.loads(fields["payload"])["text"]
    assert isinstance(text, str)
    return text


def _http_body(**changes: Any) -> dict[str, Any]:
    # dm_native puts is_ext_shared_channel on the envelope only. Later keys
    # override, so a reply can replace the text and set thread_ts.
    fields = {"ts": CURRENT_TS, "text": CURRENT_TEXT, **changes}
    body, event = dm_native(**fields)
    assert "is_ext_shared_channel" not in event
    return body


def _run_http(
    config: DispatcherConfig,
    redis_client: redis.Redis,
    body: dict[str, Any],
    history: Any,
) -> tuple[WebClient, str, bytes]:
    module = api()
    rig = HttpRig()
    # _deliver_selected replaces a fixed method list and leaves this one.
    client = _history_client(history)
    mocked = client.conversations_history
    rig.web.conversations_history = mocked  # type: ignore[method-assign]
    web = _deliver_selected(
        config,
        redis_client,
        rig,
        body,
        bindings=dm_bindings(module),
    )
    assert len(rig.requests) == 1
    raw = rig.requests[0].content
    rig.closed()
    return web, json.loads(raw)["text"], raw


def _assert_history_call(web: WebClient) -> None:
    history = web.conversations_history
    history.assert_called_once()
    assert history.call_args.args == ()
    kwargs = history.call_args.kwargs
    assert kwargs["channel"] == DM_CHANNEL
    assert kwargs["latest"] == CURRENT_TS
    assert kwargs["inclusive"] is False
    assert kwargs["limit"] == 100
    assert isinstance(kwargs["oldest"], str)
    assert Decimal(kwargs["oldest"]) == Decimal(CURRENT_TS) - Decimal(_WEEK_SECONDS)
    assert set(kwargs) == {"channel", "latest", "inclusive", "oldest", "limit"}


def _denies(lowered: str, noun: str) -> bool:
    return any(
        phrase in lowered
        for phrase in (
            f"not {noun}",
            f"not as {noun}",
            f"never {noun}",
            f"never as {noun}",
        )
    )


def _assert_header(header: str) -> None:
    assert "/" not in header
    lowered = header.lower()
    assert "earlier" in lowered and "direct message" in lowered
    assert "this person" in lowered and "this agent" in lowered
    assert "context only" in lowered
    assert _denies(lowered, "instructions")
    assert _denies(lowered, "authorization")
    assert "follows" in lowered


def _assert_quoted(text: str, *, current: str) -> None:
    assert text.count("<prior_dm_turns>") == 1
    assert text.count("</prior_dm_turns>") == 1
    header, _, rest = text.partition("<prior_dm_turns>")
    block, _, after = rest.partition("</prior_dm_turns>")
    _assert_header(header)
    person_line = "person: " + _escaped(PERSON_TEXT)
    agent_line = "agent: " + AGENT_TEXT
    assert agent_line in block and person_line in block
    assert block.index(agent_line) < block.index(person_line)
    assert current not in block
    assert PERSON_TEXT not in text
    assert "<note>" not in text
    assert text.rindex(current) > text.index("</prior_dm_turns>")
    assert current in after


def _slack_message(
    ts: str,
    text: Any,
    *,
    user: Any = SENDER,
    channel: str | None = None,
    bot_id: str | None = None,
    subtype: str | None = None,
    msg_id: str | None = None,
) -> dict[str, Any]:
    message: dict[str, Any] = {
        "type": "message",
        "text": text,
        "ts": ts,
        "team": "T1",
        "client_msg_id": msg_id or ("c0ignored" + ts.replace(".", "")),
    }
    if user is not None:
        message["user"] = user
    if channel is not None:
        message["channel"] = channel
    if bot_id is not None:
        message["bot_id"] = bot_id
    if subtype is not None:
        message["subtype"] = subtype
    return message


@pytest.mark.parametrize("root_thread_ts", [None, CURRENT_TS])
def test_legacy_top_level_includes_prior_messages(
    redis_client: redis.Redis,
    config: DispatcherConfig,
    root_thread_ts: str | None,
) -> None:
    """A top-level im quotes the earlier person and agent lines, oldest first."""

    event = _legacy_event()
    if root_thread_ts is not None:
        event["thread_ts"] = root_thread_ts
    page = _history_page(agent_user=LEGACY_BOT_USER, agent_bot_id=LEGACY_BOT_ID)
    web = _run_legacy(
        config,
        redis_client,
        event,
        event_id="Ev0EXAMPLE7",
        history=page,
    )
    assert redis_client.xlen(config.stream) == 1
    text = _queued_text(redis_client, config)
    _assert_history_call(web)
    _assert_quoted(text, current=CURRENT_TEXT)


@pytest.mark.parametrize("root_thread_ts", [None, CURRENT_TS])
def test_http_top_level_includes_prior_messages(
    redis_client: redis.Redis,
    config: DispatcherConfig,
    root_thread_ts: str | None,
) -> None:
    """The selected message listener signs the same oldest-first quote."""

    changes: dict[str, Any] = {}
    if root_thread_ts is not None:
        changes["thread_ts"] = root_thread_ts
    page = _history_page(agent_user=BOT, agent_bot_id=HTTP_BOT_ID)
    web, text, raw = _run_http(config, redis_client, _http_body(**changes), page)
    _assert_history_call(web)
    _assert_quoted(text, current=CURRENT_TEXT)
    assert ("person: " + _escaped(PERSON_TEXT)).encode() in raw
    assert AGENT_TEXT.encode() in raw
    assert json.loads(raw)["conversation_id"] == CURRENT_TS
    assert CURRENT_TEXT.encode() in raw


def test_thread_reply_legacy_does_not_load_dm_history(
    redis_client: redis.Redis,
    config: DispatcherConfig,
) -> None:
    """A thread_ts other than ts queues only the reply and does not read history."""

    page = _history_page(agent_user=LEGACY_BOT_USER, agent_bot_id=LEGACY_BOT_ID)
    event = _legacy_event(thread_ts=THREAD_TS, text=REPLY_TEXT)
    web = _run_legacy(
        config,
        redis_client,
        event,
        event_id="Ev0EXAMPLE8",
        history=page,
    )
    web.conversations_history.assert_not_called()
    assert redis_client.xlen(config.stream) == 1
    assert _queued_text(redis_client, config) == REPLY_TEXT


def test_thread_reply_http_does_not_load_dm_history(
    redis_client: redis.Redis,
    config: DispatcherConfig,
) -> None:
    """A threaded direct message is signed as the reply alone."""

    page = _history_page(agent_user=BOT, agent_bot_id=HTTP_BOT_ID)
    body = _http_body(thread_ts=THREAD_TS, text=REPLY_TEXT)
    web, text, raw = _run_http(config, redis_client, body, page)
    web.conversations_history.assert_not_called()
    assert text == REPLY_TEXT
    assert json.loads(raw)["conversation_id"] == THREAD_TS
    assert AGENT_TEXT.encode() not in raw


def test_channel_mention_does_not_load_dm_history(
    redis_client: redis.Redis,
    config: DispatcherConfig,
) -> None:
    """An app_mention is still queued and does not call conversations_history."""

    page = _history_page(agent_user=LEGACY_BOT_USER, agent_bot_id=LEGACY_BOT_ID)
    conn = _ops(config, redis_client)
    client = _history_client(page)
    mocked = client.conversations_history
    conn.web_client.conversations_history = mocked  # type: ignore[method-assign]
    conn.handle(_mention("env-dm-mention", "Ev0EXAMPLE1", bot_user_id=LEGACY_BOT_USER))
    conn.drain()
    conn.web_client.conversations_history.assert_not_called()
    assert redis_client.xlen(config.stream) == 1
    text = _queued_text(redis_client, config)
    assert text == "status"
    assert "<prior_dm_turns>" not in text


def test_selection_drops_foreign_messages() -> None:
    """Only this caller and this agent remain, oldest first, and the quote is escaped."""

    rich = 'say "hi" see <tag> & a/b'
    page = [
        _slack_message(FUTURE_TS, "future secret"),
        # No channel key: history is already scoped, so a missing channel stays.
        _slack_message(INSIDE_PERSON_TS, rich),
        _slack_message(CURRENT_TS, "repeated current secret"),
        _slack_message(INSIDE_AGENT_TS, AGENT_TEXT, user=BOT, channel=DM_CHANNEL),
        _slack_message(INSIDE_PERSON_TS, "foreign user secret", user=FOREIGN_USER),
        _slack_message(
            INSIDE_PERSON_TS,
            "foreign channel secret",
            channel=FOREIGN_CHANNEL,
        ),
        _slack_message(
            INSIDE_AGENT_TS,
            "subtype secret",
            user=BOT,
            bot_id=HTTP_BOT_ID,
            subtype="bot_message",
        ),
        _slack_message(INSIDE_PERSON_TS, ""),
        _slack_message(INSIDE_PERSON_TS, None),
        _slack_message(INSIDE_PERSON_TS, 12),
        _slack_message(OUTSIDE_TS, "outside window secret"),
        _slack_message(JUST_OUTSIDE_TS, "just outside secret"),
        _slack_message(INSIDE_PERSON_TS, "blank channel secret", channel=""),
        _slack_message(
            INSIDE_AGENT_TS,
            "foreign bot secret",
            user=FOREIGN_USER,
            bot_id=FOREIGN_BOT,
        ),
    ]
    assert "channel" not in page[1]
    assert "client_msg_id" in page[1]

    selected = select_dm_messages(
        page,
        channel=DM_CHANNEL,
        current_ts=CURRENT_TS,
        caller_user_id=SENDER,
        bot_user_id=BOT,
        bot_id=HTTP_BOT_ID,
        max_messages=20,
    )
    assert selected == [("agent", AGENT_TEXT), ("person", rich)]
    assert (
        select_dm_messages(
            page,
            channel=DM_CHANNEL,
            current_ts=CURRENT_TS,
            caller_user_id=SENDER,
            bot_user_id=BOT,
            bot_id=HTTP_BOT_ID,
            max_messages=0,
        )
        == []
    )
    assert render_dm_continuity([], CURRENT_TEXT) == CURRENT_TEXT

    rendered = render_dm_continuity(selected, CURRENT_TEXT)
    _assert_quoted_roles(rendered, rich)
    for secret in (
        "future secret",
        "repeated current secret",
        "foreign user secret",
        "foreign channel secret",
        "subtype secret",
        "outside window secret",
        "just outside secret",
        "blank channel secret",
        "foreign bot secret",
    ):
        assert secret not in rendered

    capped = select_dm_messages(
        [
            _slack_message("1699500000.000400", "oldest agent", user=BOT),
            _slack_message("1699700000.000400", "middle person"),
            _slack_message("1699900000.000400", "newest agent", user=BOT),
        ],
        channel=DM_CHANNEL,
        current_ts=CURRENT_TS,
        caller_user_id=SENDER,
        bot_user_id=BOT,
        bot_id=HTTP_BOT_ID,
        max_messages=2,
    )
    assert capped == [("person", "middle person"), ("agent", "newest agent")]

    by_bot_id = select_dm_messages(
        [
            _slack_message(
                INSIDE_PERSON_TS,
                "user matches only a real bot user",
                user=BOT,
            ),
            _slack_message(
                INSIDE_AGENT_TS,
                "agent by bot id",
                user=None,
                bot_id=HTTP_BOT_ID,
            ),
            {
                "type": "message",
                "user": None,
                "text": "anonymous secret",
                "ts": "1699700000.000400",
                "team": "T1",
                "client_msg_id": "c0anonymous",
            },
        ],
        channel=DM_CHANNEL,
        current_ts=CURRENT_TS,
        caller_user_id=SENDER,
        bot_user_id=None,
        bot_id=HTTP_BOT_ID,
        max_messages=20,
    )
    assert by_bot_id == [("agent", "agent by bot id")]


def _assert_quoted_roles(rendered: str, person_text: str) -> None:
    assert rendered.count("<prior_dm_turns>") == 1
    assert rendered.count("</prior_dm_turns>") == 1
    header, _, rest = rendered.partition("<prior_dm_turns>")
    block, _, after = rest.partition("</prior_dm_turns>")
    _assert_header(header)
    lines = [line for line in block.splitlines() if line.strip()]
    assert lines == [
        "agent: " + AGENT_TEXT,
        "person: " + _escaped(person_text),
    ]
    assert "<tag>" not in rendered
    assert "a/b" not in rendered
    assert rendered.rstrip().endswith(CURRENT_TEXT)
    assert CURRENT_TEXT not in block
    assert CURRENT_TEXT in after


def _assert_int_bound(value: object, expected: int) -> None:
    assert type(value) is int
    assert value == expected


def test_bound_unset_is_20(monkeypatch: pytest.MonkeyPatch) -> None:
    """The env alias defaults to 20 and accepts the closed range."""

    _clear_all_config_env(monkeypatch)
    _set_valid_chat_attester_secret(monkeypatch)
    _assert_int_bound(DispatcherConfig().dm_continuity_max_messages, 20)
    monkeypatch.setenv("DM_CONTINUITY_MAX_MESSAGES", "7")
    _assert_int_bound(DispatcherConfig().dm_continuity_max_messages, 20)
    monkeypatch.setenv("CURIE_DM_CONTINUITY_MAX_MESSAGES", "7")
    _assert_int_bound(DispatcherConfig().dm_continuity_max_messages, 7)
    monkeypatch.setenv("CURIE_DM_CONTINUITY_MAX_MESSAGES", "0")
    _assert_int_bound(DispatcherConfig().dm_continuity_max_messages, 0)
    monkeypatch.setenv("CURIE_DM_CONTINUITY_MAX_MESSAGES", "40")
    _assert_int_bound(DispatcherConfig().dm_continuity_max_messages, 40)


def test_bound_zero_skips_slack(
    redis_client: redis.Redis,
    config: DispatcherConfig,
) -> None:
    """A bound of 0 leaves both listeners' text unchanged and does not call Slack."""

    zero = config.model_copy(update={"dm_continuity_max_messages": 0})
    page = _history_page(agent_user=BOT, agent_bot_id=HTTP_BOT_ID)
    web, text, _raw = _run_http(zero, redis_client, _http_body(), page)
    web.conversations_history.assert_not_called()
    assert text == CURRENT_TEXT

    legacy_page = _history_page(agent_user=LEGACY_BOT_USER, agent_bot_id=LEGACY_BOT_ID)
    legacy_web = _run_legacy(
        zero,
        redis_client,
        _legacy_event(),
        event_id="Ev0EXAMPLE6",
        history=legacy_page,
    )
    legacy_web.conversations_history.assert_not_called()
    assert redis_client.xlen(zero.stream) == 1
    assert _queued_text(redis_client, zero) == CURRENT_TEXT


def test_bound_41_refuses_config(monkeypatch: pytest.MonkeyPatch) -> None:
    """41 is outside 0 through 40, as a kwarg and as the env alias."""

    _clear_all_config_env(monkeypatch)
    _set_valid_chat_attester_secret(monkeypatch)
    with pytest.raises(ValidationError):
        DispatcherConfig(dm_continuity_max_messages=41)
    monkeypatch.setenv("CURIE_DM_CONTINUITY_MAX_MESSAGES", "41")
    with pytest.raises(ValidationError):
        DispatcherConfig()


def test_bound_boolean_refuses_config(monkeypatch: pytest.MonkeyPatch) -> None:
    """A boolean is not a message bound, including False, which must not become 0."""

    _clear_all_config_env(monkeypatch)
    _set_valid_chat_attester_secret(monkeypatch)
    for value in (True, False):
        with pytest.raises(ValidationError):
            DispatcherConfig(dm_continuity_max_messages=value)
    for raw in ("true", "false", "True", "False"):
        monkeypatch.setenv("CURIE_DM_CONTINUITY_MAX_MESSAGES", raw)
        with pytest.raises(ValidationError):
            DispatcherConfig()


def _assert_failure_log(
    caplog: pytest.LogCaptureFixture,
    error: BaseException | None,
) -> None:
    """The log names the error class and does not carry the page or the exception text."""

    assert "malformed payload secret" not in caplog.text
    assert "channel_not_found" not in caplog.text
    if error is None:
        # A page that does not raise is still a validation failure. The class
        # name is the only record. The page text stays out of the log.
        assert "_MalformedHistoryPage" in caplog.text
        assert "not-a-payload" not in caplog.text
        return
    assert type(error).__name__ in caplog.text
    if isinstance(error, SlackApiError):
        assert "ratelimited" not in caplog.text
        return
    assert "synthetic history failure" not in caplog.text


_LEAK = {
    "type": "message",
    "user": SENDER,
    "text": "malformed payload secret",
    "ts": INSIDE_PERSON_TS,
    "team": "T1",
    "client_msg_id": "c0malformed",
}


@pytest.mark.parametrize(
    ("error", "payload"),
    [
        (
            SlackApiError("ratelimited", response={"ok": False, "error": "ratelimited"}),
            None,
        ),
        (Exception("synthetic history failure"), None),
        (None, {"ok": False, "error": "channel_not_found", "messages": [_LEAK]}),
        (None, {"ok": "true", "messages": [_LEAK]}),
        (None, {"ok": True, "messages": _LEAK}),
        (None, {"ok": True, "messages": None}),
        (None, {"ok": True}),
        (None, [_LEAK]),
        (None, "not-a-payload"),
    ],
)
def test_history_failure_legacy_still_queues(
    redis_client: redis.Redis,
    config: DispatcherConfig,
    caplog: pytest.LogCaptureFixture,
    error: BaseException | None,
    payload: Any,
) -> None:
    """A Slack error or a bad page still queues the person's text alone."""

    result: Any = error if error is not None else payload
    caplog.set_level(logging.DEBUG)
    web = _run_legacy(
        config,
        redis_client,
        _legacy_event(),
        event_id="Ev0EXAMPLE5",
        history=result,
    )
    web.conversations_history.assert_called_once()
    assert redis_client.xlen(config.stream) == 1
    assert _queued_text(redis_client, config) == CURRENT_TEXT
    _assert_failure_log(caplog, error)


@pytest.mark.parametrize(
    ("error", "payload"),
    [
        (
            SlackApiError("ratelimited", response={"ok": False, "error": "ratelimited"}),
            None,
        ),
        (Exception("synthetic history failure"), None),
        (None, {"ok": False, "error": "channel_not_found", "messages": [_LEAK]}),
        (None, {"ok": "true", "messages": [_LEAK]}),
        (None, {"ok": True, "messages": _LEAK}),
        (None, {"ok": True, "messages": None}),
        (None, {"ok": True}),
        (None, [_LEAK]),
        (None, "not-a-payload"),
    ],
)
def test_history_failure_http_still_signs(
    redis_client: redis.Redis,
    config: DispatcherConfig,
    caplog: pytest.LogCaptureFixture,
    error: BaseException | None,
    payload: Any,
) -> None:
    """A Slack error or a bad page still signs the person's text alone."""

    result: Any = error if error is not None else payload
    caplog.set_level(logging.DEBUG)
    web, text, raw = _run_http(config, redis_client, _http_body(), result)
    web.conversations_history.assert_called_once()
    assert text == CURRENT_TEXT
    assert b"malformed payload secret" not in raw
    assert b"<prior_dm_turns>" not in raw
    _assert_failure_log(caplog, error)


@pytest.mark.parametrize(
    "damage",
    [
        "channel_type",
        "subtype",
        "bot_id",
        "empty_user",
        "shared_missing",
        "shared_true",
        "shared_not_bool",
    ],
)
def test_admission_shape_skips_history(
    config: DispatcherConfig,
    damage: str,
) -> None:
    """A direct message that is not a plain top-level im does not read history."""

    event = _legacy_event()
    body: dict[str, Any] = {
        "type": "event_callback",
        "event_id": "Ev0EXAMPLE9",
        "team_id": "T1",
        "is_ext_shared_channel": False,
        "event": event,
    }
    if damage == "channel_type":
        event["channel_type"] = "channel"
    elif damage == "subtype":
        event["subtype"] = "message_changed"
    elif damage == "bot_id":
        event["bot_id"] = FOREIGN_BOT
    elif damage == "empty_user":
        event["user"] = ""
    elif damage == "shared_missing":
        del body["is_ext_shared_channel"]
    elif damage == "shared_true":
        body["is_ext_shared_channel"] = True
    else:
        body["is_ext_shared_channel"] = "false"
    assert "is_ext_shared_channel" not in event
    page = _history_page(agent_user=BOT, agent_bot_id=HTTP_BOT_ID)
    web = _history_client(page)
    rendered = SlackDmContinuity(web, config).resolve(
        event,
        BOT,
        HTTP_BOT_ID,
        CURRENT_TEXT,
        body=body,
    )
    assert rendered == CURRENT_TEXT
    web.conversations_history.assert_not_called()
