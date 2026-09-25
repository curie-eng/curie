"""The dispatcher connects one Bolt app per admitted Slack identity.

ADR-0168 decision 2. The Slack transport is recorded, never opened: Bolt's App,
the Web client and the Socket Mode handler are replaced with recorders at the
module seam `curie_dispatcher.app` builds them through.
"""

from __future__ import annotations

import json
import logging
import threading
from types import SimpleNamespace
from typing import Any

import pytest
import redis
from curie_dispatcher import app as app_module
from curie_dispatcher.app import SocketModeConnection
from curie_dispatcher.config import DispatcherConfig
from curie_dispatcher.identities import (
    SlackBotIds,
    SlackIdentityCredentials,
    default_identity_credentials,
)
from curie_dispatcher.preflight import PreflightedIdentity
from slack_bolt import App
from slack_sdk.web import WebClient

from .conftest import _authorize
from .test_preflight import _set_run_env, _TestTelemetry

OPS = SlackIdentityCredentials(name="ops-bot", app_token="xapp-ops", bot_token="xoxb-ops")
_STOCK_APP_KWARGS = {
    "signing_secret": "unused-in-socket-mode",
    "token": "xoxb-test",
    "token_verification_enabled": False,
}


@pytest.fixture
def recorded(monkeypatch: pytest.MonkeyPatch) -> SimpleNamespace:
    calls = SimpleNamespace(apps=[], clients=[], handlers=[])

    class RecordingApp(App):
        def __init__(self, **kwargs: Any) -> None:
            calls.apps.append(kwargs)
            super().__init__(**kwargs)

    def recording_web_client(**kwargs: Any) -> WebClient:
        calls.clients.append(kwargs)
        return WebClient(**kwargs)

    class RecordingHandler:
        def __init__(self, app: App, app_token: str) -> None:
            calls.handlers.append((app, app_token))
            self.client = SimpleNamespace(message_listeners=[])

    monkeypatch.setattr(app_module, "App", RecordingApp)
    monkeypatch.setattr(app_module, "WebClient", recording_web_client)
    monkeypatch.setattr(app_module, "SocketModeHandler", RecordingHandler)
    return calls


def test_a_stock_config_builds_one_connection_from_the_legacy_settings(
    config: DispatcherConfig, redis_client: redis.Redis, recorded: SimpleNamespace
) -> None:
    from curie_dispatcher.run import build_identity_connections, build_supervisor

    (connection,) = build_identity_connections(
        config,
        (PreflightedIdentity(default_identity_credentials(config), None),),
        redis_client=redis_client,
        logger=logging.getLogger("test-run-stock"),
    )
    connection.connect()

    assert connection.name == "default"
    assert connection.bot_ids is None
    assert recorded.clients == [{"token": "xoxb-test", "timeout": 2}]
    assert recorded.apps == [_STOCK_APP_KWARGS]
    assert recorded.handlers == [(connection.app, "xapp-test")]
    assert list(build_supervisor(config, logger=logging.getLogger("t")).members) == ["default"]


def test_two_identities_build_two_connections_each_from_its_own_tokens(
    config: DispatcherConfig, redis_client: redis.Redis, recorded: SimpleNamespace
) -> None:
    from curie_dispatcher.run import build_identity_connections

    ids = SlackBotIds(team_id="T1", app_id=None, bot_id="B0OPS", bot_user_id="U0OPS")
    connections = build_identity_connections(
        config,
        (
            PreflightedIdentity(default_identity_credentials(config), None),
            PreflightedIdentity(OPS, ids),
        ),
        redis_client=redis_client,
        logger=logging.getLogger("test-run-two"),
    )
    for connection in connections:
        connection.connect()

    assert [c.name for c in connections] == ["default", "ops-bot"]
    assert connections[1].bot_ids == ids
    assert [call["token"] for call in recorded.clients] == ["xoxb-test", "xoxb-ops"]
    assert [call["token"] for call in recorded.apps] == ["xoxb-test", "xoxb-ops"]
    assert recorded.handlers == [
        (connections[0].app, "xapp-test"),
        (connections[1].app, "xapp-ops"),
    ]
    assert connections[0].app is not connections[1].app
    assert connections[0].supervisor is not connections[1].supervisor


def test_run_main_connects_only_the_identities_that_passed_preflight(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from curie_dispatcher import run

    _set_run_env(monkeypatch)
    monkeypatch.setenv("SLACK_APP_TOKEN", "xapp-default")
    monkeypatch.setenv("SLACK_BOT_TOKEN", "xoxb-default")
    monkeypatch.setenv(
        "CURIE_SLACK_IDENTITIES",
        json.dumps(
            [
                {
                    "name": "default",
                    "app_token_env": "SLACK_APP_TOKEN",
                    "bot_token_env": "SLACK_BOT_TOKEN",
                    "signing_secret_env": None,
                },
                {
                    "name": "ops-bot",
                    "app_token_env": "CURIE_SLACK_APP_TOKEN__1",
                    "bot_token_env": "CURIE_SLACK_BOT_TOKEN__1",
                    "signing_secret_env": None,
                },
            ]
        ),
    )
    monkeypatch.setenv("CURIE_SLACK_APP_TOKEN__1", "xapp-ops")
    monkeypatch.setenv("CURIE_SLACK_BOT_TOKEN__1", "xoxb-ops")
    seen: dict[str, object] = {}

    def check_slack(
        _config: DispatcherConfig,
        *,
        logger: logging.Logger,
        identities: tuple[SlackIdentityCredentials, ...],
    ) -> tuple[PreflightedIdentity, ...]:
        seen["preflight"] = [(i.name, i.app_token, i.bot_token) for i in identities]
        return (PreflightedIdentity(identities[0], None),)

    class Group:
        def run(self) -> None:
            seen["ran"] = True

        def request_stop(self) -> None:
            pass

    def build_supervisor(
        _config: DispatcherConfig,
        *,
        logger: logging.Logger,
        identities: tuple[PreflightedIdentity, ...],
    ) -> Group:
        seen["connected"] = [identity.name for identity in identities]
        return Group()

    monkeypatch.setattr(run, "bootstrap_service_telemetry", lambda *a, **k: _TestTelemetry())
    monkeypatch.setattr(run, "check_api_reachable", lambda *a, **k: None)
    monkeypatch.setattr(run, "check_slack_channel_capabilities", check_slack)
    monkeypatch.setattr(run, "build_supervisor", build_supervisor)
    monkeypatch.setattr(run, "start_heartbeat", lambda *a, **k: threading.Event())
    monkeypatch.setattr(run.signal, "signal", lambda *a, **k: None)

    run.main()

    assert seen == {
        "preflight": [
            ("default", "xapp-default", "xoxb-default"),
            ("ops-bot", "xapp-ops", "xoxb-ops"),
        ],
        "connected": ["default"],
        "ran": True,
    }


def test_a_named_connection_names_its_identity_when_slack_reports_extra_clients(
    caplog: pytest.LogCaptureFixture,
) -> None:
    app = App(
        signing_secret="unused-in-socket-mode",
        authorize=_authorize,
        token_verification_enabled=False,
    )
    logger = logging.getLogger("test-run-hello")
    connection = SocketModeConnection(app, "xapp-test", logger=logger, slack_identity="ops-bot")
    listener = connection._handler.client.message_listeners[-1]

    with caplog.at_level(logging.WARNING, logger=logger.name):
        listener(connection._handler.client, {"type": "hello", "num_connections": 2}, "")

    warning = " ".join(record.getMessage() for record in caplog.records)
    assert "exactly one Curie release may connect to a given Slack app" in warning
    assert "ops-bot" in warning
