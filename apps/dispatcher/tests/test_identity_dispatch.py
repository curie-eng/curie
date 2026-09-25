"""Each identity's Bolt app stamps its own identity on what it mints.

ADR-0168 decision 2. Driven through Bolt's real Socket Mode handler with a fake
socket and mocked Web API clients, against the real Valkey, as test_dispatch.py
is. Two apps built for two identities feed one stream. The identity is fixed
when an app is built, so nothing in a delivery can move it.
"""

import json
from typing import Any
from unittest.mock import MagicMock

import pytest
import redis
from curie_dispatcher import app as app_module
from curie_dispatcher.app import build_app, build_web_client
from curie_dispatcher.approval_actions import (
    APPROVE_ACTION_ID,
    APPROVE_NOTE_ACTION_ID,
    ResolveOutcome,
)
from curie_dispatcher.config import DispatcherConfig
from curie_dispatcher.identities import SlackIdentityCredentials
from curie_dispatcher.queue import from_stream_fields
from slack_bolt import App
from slack_bolt.adapter.socket_mode import SocketModeHandler
from slack_bolt.authorization import AuthorizeResult
from slack_sdk.socket_mode.request import SocketModeRequest
from slack_sdk.web import WebClient

from .conftest import FakeSocketClient
from .test_approval_actions import ScriptedResolver, _approval_click, _stub_dialog_web

CHANNEL = "C0EXAMPLE1"
DEFAULT = SlackIdentityCredentials(
    name="default", app_token="xapp-default", bot_token="xoxb-default"
)
OPS = SlackIdentityCredentials(name="ops-bot", app_token="xapp-ops", bot_token="xoxb-ops")


class _Connection:
    """One identity's app, its mocked Web API client, and a fake socket."""

    def __init__(
        self,
        config: DispatcherConfig,
        redis_client: redis.Redis,
        identity: SlackIdentityCredentials,
        *,
        bot_id: str,
        bot_user_id: str,
        placeholder_ts: str,
        resolver: ScriptedResolver | None = None,
    ) -> None:
        self.web_client = WebClient(token="xoxb-test")
        self.web_client.chat_postMessage = MagicMock(  # type: ignore[method-assign]
            return_value={"ts": placeholder_ts}
        )
        self.web_client.chat_update = MagicMock(return_value={"ok": True})  # type: ignore[method-assign]
        self.web_client.chat_postEphemeral = MagicMock(return_value={"ok": True})  # type: ignore[method-assign]

        def authorize(**_kwargs: Any) -> AuthorizeResult:
            return AuthorizeResult(
                enterprise_id=None,
                team_id="T1",
                bot_token="xoxb-test",
                bot_id=bot_id,
                bot_user_id=bot_user_id,
            )

        kwargs: dict[str, Any] = {}
        if resolver is not None:
            kwargs["resolver"] = resolver
        self.app = build_app(
            config,
            identity=identity,
            web_client=self.web_client,
            redis_client=redis_client,
            authorize=authorize,
            **kwargs,
        )
        self.handler = SocketModeHandler(self.app, app_token="xapp-test")
        self.sock = FakeSocketClient()

    def handle(self, request: SocketModeRequest) -> None:
        self.handler.handle(self.sock, request)

    def drain(self) -> None:
        self.app.listener_runner.listener_executor.shutdown(wait=True)


def _default(
    config: DispatcherConfig, redis_client: redis.Redis, **kwargs: Any
) -> _Connection:
    return _Connection(
        config,
        redis_client,
        DEFAULT,
        bot_id="B0DEFAULT",
        bot_user_id="U0DEFAULT",
        placeholder_ts="100.0001",
        **kwargs,
    )


def _ops(config: DispatcherConfig, redis_client: redis.Redis, **kwargs: Any) -> _Connection:
    return _Connection(
        config,
        redis_client,
        OPS,
        bot_id="B0OPS",
        bot_user_id="U0OPS",
        placeholder_ts="200.0001",
        **kwargs,
    )


def _mention(
    envelope_id: str,
    event_id: str,
    *,
    bot_user_id: str,
    body_extra: dict[str, Any] | None = None,
    event_extra: dict[str, Any] | None = None,
) -> SocketModeRequest:
    event = {
        "type": "app_mention",
        "channel": CHANNEL,
        "user": "U123",
        "text": f"<@{bot_user_id}> status",
        "ts": "1700.0001",
        **(event_extra or {}),
    }
    payload = {
        "type": "event_callback",
        "event_id": event_id,
        "team_id": "T1",
        "event": event,
        **(body_extra or {}),
    }
    return SocketModeRequest(type="events_api", envelope_id=envelope_id, payload=payload)


def _button(envelope_id: str) -> SocketModeRequest:
    return SocketModeRequest(
        type="interactive",
        envelope_id=envelope_id,
        payload={
            "type": "block_actions",
            "trigger_id": f"trig-{envelope_id}",
            "team": {"id": "T1"},
            "user": {"id": "U123"},
            "api_app_id": "A1",
            "token": "verif",
            "container": {"type": "message", "message_ts": "1700.0001"},
            "channel": {"id": CHANNEL},
            "message": {"ts": "1700.0001", "thread_ts": "1700.0001"},
            "actions": [{"type": "button", "action_id": "reports", "action_ts": "1.5"}],
        },
    )


def _payloads(redis_client: redis.Redis, config: DispatcherConfig) -> dict[str, dict[str, Any]]:
    """Every stream entry's raw JSON payload, by its event id."""

    payloads: dict[str, dict[str, Any]] = {}
    for _, fields in redis_client.xrange(config.stream):
        from_stream_fields(fields)  # every entry must still parse as a QueuedTurn
        payload = json.loads(fields["payload"])
        payloads[payload["event_id"]] = payload
    return payloads


def test_two_identities_feed_one_stream_each_stamping_its_own_identity(
    redis_client: redis.Redis, config: DispatcherConfig
) -> None:
    default = _default(config, redis_client)
    ops = _ops(config, redis_client)

    default.handle(_mention("env-d1", "Ev0EXAMPLE1", bot_user_id="U0DEFAULT"))
    ops.handle(_mention("env-o1", "Ev0EXAMPLE2", bot_user_id="U0OPS"))
    default.drain()
    ops.drain()

    payloads = _payloads(redis_client, config)
    assert set(payloads) == {"Ev0EXAMPLE1", "Ev0EXAMPLE2:ops-bot"}
    assert payloads["Ev0EXAMPLE1"]["reply_handle"]["adapter"] is None
    assert payloads["Ev0EXAMPLE2:ops-bot"]["reply_handle"]["adapter"] == "ops-bot"
    # Each placeholder is posted by, and recorded from, the app it arrived on.
    assert payloads["Ev0EXAMPLE1"]["reply_handle"]["placeholder"] == "100.0001"
    assert payloads["Ev0EXAMPLE2:ops-bot"]["reply_handle"]["placeholder"] == "200.0001"
    default.web_client.chat_postMessage.assert_called_once()
    ops.web_client.chat_postMessage.assert_called_once()
    # Each app strips its own bot's mention, from its own authorization.
    assert payloads["Ev0EXAMPLE1"]["text"] == "status"
    assert payloads["Ev0EXAMPLE2:ops-bot"]["text"] == "status"


def test_a_button_click_on_an_identity_mints_that_identity(
    redis_client: redis.Redis, config: DispatcherConfig
) -> None:
    ops = _ops(config, redis_client)

    ops.handle(_button("env-b1"))
    ops.drain()

    payloads = _payloads(redis_client, config)
    assert set(payloads) == {"action-trig-env-b1:ops-bot"}
    assert payloads["action-trig-env-b1:ops-bot"]["reply_handle"]["adapter"] == "ops-bot"
    ops.web_client.chat_postMessage.assert_called_once()


@pytest.mark.parametrize(
    "forged",
    [
        {"api_app_id": "A0OPS"},
        {"authorizations": [{"team_id": "T1", "user_id": "U0OPS", "is_bot": True}]},
        {"adapter": "ops-bot", "identity": "ops-bot"},
    ],
)
def test_the_delivery_cannot_choose_the_identity(
    redis_client: redis.Redis, config: DispatcherConfig, forged: dict[str, Any]
) -> None:
    default = _default(config, redis_client)
    ops = _ops(config, redis_client)
    event_forgery = {"adapter": "ops-bot", "identity": "ops-bot"}

    default.handle(
        _mention(
            "env-f1",
            "Ev0EXAMPLE3",
            bot_user_id="U0DEFAULT",
            body_extra=forged,
            event_extra=event_forgery,
        )
    )
    ops.handle(
        _mention(
            "env-f2",
            "Ev0EXAMPLE4",
            bot_user_id="U0OPS",
            body_extra={**forged, "adapter": "default", "identity": "default"},
            event_extra={"adapter": "default", "identity": "default"},
        )
    )
    default.drain()
    ops.drain()

    payloads = _payloads(redis_client, config)
    assert set(payloads) == {"Ev0EXAMPLE3", "Ev0EXAMPLE4:ops-bot"}
    assert payloads["Ev0EXAMPLE3"]["reply_handle"]["adapter"] is None
    assert payloads["Ev0EXAMPLE4:ops-bot"]["reply_handle"]["adapter"] == "ops-bot"


def test_the_default_identity_mints_exactly_the_handle_it_always_did(
    redis_client: redis.Redis, config: DispatcherConfig
) -> None:
    """Both lanes, with `default` named explicitly.

    `adapter` stays null on the wire until #3146 stores the name, so every
    phase-1 reader resolves it as `default` and a worker from before the route
    triple still resolves every default turn.
    """

    default = _default(config, redis_client)

    default.handle(_mention("env-r1", "Ev0EXAMPLE5", bot_user_id="U0DEFAULT"))
    default.handle(_button("env-r2"))
    default.drain()

    payloads = _payloads(redis_client, config)
    assert set(payloads) == {"Ev0EXAMPLE5", "action-trig-env-r2"}
    for payload in payloads.values():
        assert payload["reply_handle"] == {
            "kind": "slack",
            "channel": CHANNEL,
            "placeholder": "100.0001",
            "endpoint": None,
            "adapter": None,
        }


def test_one_message_on_two_identities_is_two_turns_and_a_redelivery_is_one(
    redis_client: redis.Redis, config: DispatcherConfig
) -> None:
    default = _default(config, redis_client)
    ops = _ops(config, redis_client)

    default.handle(_mention("env-s1", "Ev0EXAMPLE6", bot_user_id="U0DEFAULT"))
    ops.handle(_mention("env-s2", "Ev0EXAMPLE6", bot_user_id="U0OPS"))
    ops.handle(_mention("env-s3", "Ev0EXAMPLE6", bot_user_id="U0OPS"))
    default.drain()
    ops.drain()

    assert set(_payloads(redis_client, config)) == {"Ev0EXAMPLE6", "Ev0EXAMPLE6:ops-bot"}
    assert redis_client.xlen(config.stream) == 2
    ops.web_client.chat_postMessage.assert_called_once()


def test_an_approval_click_is_answered_by_the_identity_whose_card_was_clicked(
    redis_client: redis.Redis, config: DispatcherConfig
) -> None:
    resolver = ScriptedResolver(
        ResolveOutcome(status_code=200, resolved_by="U_MANAGER", decision="approved")
    )
    default = _default(config, redis_client, resolver=resolver)
    ops = _ops(config, redis_client, resolver=resolver)
    _stub_dialog_web(ops.web_client)
    _stub_dialog_web(default.web_client)

    ops.handle(_approval_click("env-c1", action_id=APPROVE_ACTION_ID))
    ops.handle(_approval_click("env-c2", action_id=APPROVE_NOTE_ACTION_ID))
    ops.drain()
    default.drain()

    ops.web_client.chat_update.assert_called_once()
    ops.web_client.views_open.assert_called_once()
    default.web_client.chat_update.assert_not_called()
    default.web_client.views_open.assert_not_called()
    default.web_client.chat_postEphemeral.assert_not_called()
    assert redis_client.xlen(config.stream) == 0


def test_bolt_and_the_web_client_are_built_from_the_identity_given(
    redis_client: redis.Redis, config: DispatcherConfig, monkeypatch: pytest.MonkeyPatch
) -> None:
    app_calls: list[dict[str, Any]] = []
    client_calls: list[dict[str, Any]] = []

    class RecordingApp(App):
        def __init__(self, **kwargs: Any) -> None:
            app_calls.append(kwargs)
            super().__init__(**kwargs)

    def recording_web_client(**kwargs: Any) -> WebClient:
        client_calls.append(kwargs)
        return WebClient(**kwargs)

    monkeypatch.setattr(app_module, "App", RecordingApp)
    monkeypatch.setattr(app_module, "WebClient", recording_web_client)

    build_app(config, identity=OPS, web_client=build_web_client(config, OPS), redis_client=redis_client)

    assert client_calls == [{"token": "xoxb-ops", "timeout": 2}]
    assert app_calls == [
        {
            "signing_secret": "unused-in-socket-mode",
            "token": "xoxb-ops",
            "token_verification_enabled": False,
        }
    ]


def test_a_stock_config_builds_bolt_and_the_web_client_as_it_always_did(
    redis_client: redis.Redis, config: DispatcherConfig, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Passes before and after this change: it pins the stock construction."""

    app_calls: list[dict[str, Any]] = []
    client_calls: list[dict[str, Any]] = []

    class RecordingApp(App):
        def __init__(self, **kwargs: Any) -> None:
            app_calls.append(kwargs)
            super().__init__(**kwargs)

    def recording_web_client(**kwargs: Any) -> WebClient:
        client_calls.append(kwargs)
        return WebClient(**kwargs)

    monkeypatch.setattr(app_module, "App", RecordingApp)
    monkeypatch.setattr(app_module, "WebClient", recording_web_client)

    build_app(config, web_client=build_web_client(config), redis_client=redis_client)

    assert client_calls == [{"token": "xoxb-test", "timeout": 2}]
    assert app_calls == [
        {
            "signing_secret": "unused-in-socket-mode",
            "token": "xoxb-test",
            "token_verification_enabled": False,
        }
    ]
