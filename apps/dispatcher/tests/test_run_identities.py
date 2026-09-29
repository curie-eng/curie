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

from .conftest import _authorize, _set_run_env, _TestTelemetry

OPS = SlackIdentityCredentials(name="ops-bot", app_token="xapp-ops", bot_token="xoxb-ops")
_STOCK_APP_KWARGS = {
    "signing_secret": "unused-in-socket-mode",
    "token": "xoxb-test",
    "token_verification_enabled": False,
}


@pytest.fixture
def recorded(monkeypatch: pytest.MonkeyPatch) -> SimpleNamespace:
    calls = SimpleNamespace(apps=[], clients=[], handlers=[], registered=[])

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

        def connect(self) -> None:
            pass

        def close(self) -> None:
            pass

    real_register_handlers = app_module.register_handlers

    def recording_register_handlers(app: App, **kwargs: Any) -> None:
        # Which client a delivery to THIS app answers through (ADR-0168
        # decision 2). Recorded at the seam ``build_app`` calls through, so a
        # mistake that hands one identity's app another's client shows here
        # even though every ``WebClient(...)`` call still logs its own token.
        calls.registered.append((app, kwargs["web_client"]))
        real_register_handlers(app, **kwargs)

    monkeypatch.setattr(app_module, "App", RecordingApp)
    monkeypatch.setattr(app_module, "WebClient", recording_web_client)
    monkeypatch.setattr(app_module, "SocketModeHandler", RecordingHandler)
    monkeypatch.setattr(app_module, "register_handlers", recording_register_handlers)
    return calls


def _run_connection_and_capture(
    connection: Any,
    logger_name: str,
    caplog: pytest.LogCaptureFixture,
) -> list[str]:
    """Drive the extra-clients warning and the connected line, without blocking.

    The recorded ``SocketModeHandler.connect``/``close`` are no-ops, so ``run``
    reaches its log line and then would block on ``self._closed.wait()``
    forever; a handler whose own connect never blocks is exactly what a real
    Socket Mode connection is not, so the wait is the one piece worth faking.
    """
    sm_connection = connection.connect()
    listener = sm_connection._handler.client.message_listeners[-1]
    with caplog.at_level(logging.INFO, logger=logger_name):
        listener(sm_connection._handler.client, {"type": "hello", "num_connections": 2}, "")
        sm_connection._closed.wait = lambda *_a, **_k: True  # type: ignore[method-assign]
        sm_connection.run()
    return [record.getMessage() for record in caplog.records]


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
    assert connection.supervisor._prefix == ""
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
    # ADR-0168 decision 2: a delivery to one app's handlers must answer
    # through that app's own client, never another identity's -- so pin what
    # register_handlers, not just what WebClient(...), was actually given.
    assert recorded.registered == [
        (connections[0].app, connections[0].web_client),
        (connections[1].app, connections[1].web_client),
    ]
    assert [c.supervisor._prefix for c in connections] == [
        "Slack identity default: ",
        "Slack identity ops-bot: ",
    ]


def test_a_lone_survivor_of_a_declared_pair_still_names_its_identity(
    config: DispatcherConfig, redis_client: redis.Redis, recorded: SimpleNamespace
) -> None:
    """Labelling follows what was DECLARED, not what preflight let through.

    Two identities are declared and only ``ops-bot`` is admitted; its
    connection is still built as one of a pair, not as a lone stock survivor.
    """
    from curie_dispatcher.run import build_identity_connections

    (connection,) = build_identity_connections(
        config,
        (PreflightedIdentity(OPS, None),),
        redis_client=redis_client,
        logger=logging.getLogger("test-run-one-of-two"),
        declared_count=2,
    )

    assert connection.supervisor._prefix == "Slack identity ops-bot: "
    assert connection.connect()._slack_identity == "ops-bot"


def test_build_supervisor_with_two_identities_builds_two_labelled_members(
    config: DispatcherConfig, recorded: SimpleNamespace
) -> None:
    from curie_dispatcher.run import build_supervisor

    group = build_supervisor(
        config,
        logger=logging.getLogger("test-build-supervisor-two"),
        identities=(
            PreflightedIdentity(default_identity_credentials(config), None),
            PreflightedIdentity(OPS, None),
        ),
    )

    members = group.members
    assert list(members) == ["default", "ops-bot"]
    assert members["default"] is not members["ops-bot"]

    for member in members.values():
        member._connect()

    assert [token for _, token in recorded.handlers] == ["xapp-test", "xapp-ops"]


def test_a_stock_connections_log_lines_name_no_identity(
    config: DispatcherConfig,
    redis_client: redis.Redis,
    recorded: SimpleNamespace,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    from curie_dispatcher.run import build_identity_connections

    monkeypatch.setenv("CURIE_RELEASE_IDENTITY", "curie-run-identities-stock")
    logger_name = "test-run-stock-lines"

    (connection,) = build_identity_connections(
        config,
        (PreflightedIdentity(default_identity_credentials(config), None),),
        redis_client=redis_client,
        logger=logging.getLogger(logger_name),
    )

    messages = _run_connection_and_capture(connection, logger_name, caplog)

    assert (
        "curie-run-identities-stock: exactly one Curie release may connect to a "
        "given Slack app; disconnect extra clients"
    ) in messages
    assert "socket mode connected identity=curie-run-identities-stock" in messages


def test_two_identities_log_lines_each_name_their_own_identity(
    config: DispatcherConfig,
    redis_client: redis.Redis,
    recorded: SimpleNamespace,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    from curie_dispatcher.run import build_identity_connections

    monkeypatch.setenv("CURIE_RELEASE_IDENTITY", "curie-run-identities-two")
    logger_name = "test-run-two-lines"

    connections = build_identity_connections(
        config,
        (
            PreflightedIdentity(default_identity_credentials(config), None),
            PreflightedIdentity(OPS, None),
        ),
        redis_client=redis_client,
        logger=logging.getLogger(logger_name),
    )

    # `caplog` accumulates across calls, so the last return already carries
    # both connections' lines.
    messages: list[str] = []
    for connection in connections:
        messages = _run_connection_and_capture(connection, logger_name, caplog)

    for name in ("default", "ops-bot"):
        assert (
            "curie-run-identities-two: exactly one Curie release may connect to a "
            f"given Slack app; disconnect extra clients of Slack identity {name}"
        ) in messages
        connected_line = (
            f"socket mode connected identity=curie-run-identities-two slack_identity={name}"
        )
        assert connected_line in messages


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
        declared_count: int,
    ) -> Group:
        seen["connected"] = [identity.name for identity in identities]
        seen["declared_count"] = declared_count
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
        "declared_count": 2,
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


def test_the_shutdown_signal_handler_takes_no_lock_the_interrupted_thread_may_hold(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A signal handler runs on the main thread, between two bytecodes of
    whatever that thread was doing. A group of one runs its supervisor there,
    and stopping it takes locks that thread may be holding at that moment, so
    the handler itself must not take them.

    Stopping the supervisor and the heartbeat both need ``held``, and the fake
    group invokes the installed handler while holding it, exactly as a signal
    landing inside the supervisor's critical section would. Every acquire is
    bounded, so a handler that takes the lock itself fails here, not hangs.
    """
    from curie_dispatcher import run

    _set_run_env(monkeypatch)
    monkeypatch.setenv("SLACK_APP_TOKEN", "xapp-default")
    monkeypatch.setenv("SLACK_BOT_TOKEN", "xoxb-default")
    held = threading.Lock()
    handlers: dict[int, Any] = {}
    stopped = threading.Event()
    outcome: dict[str, object] = {}

    def acquire_held(what: str) -> None:
        got = held.acquire(timeout=1.0)
        outcome[what] = (got, threading.current_thread() is threading.main_thread())
        if got:
            held.release()

    class Heartbeat:
        def set(self) -> None:
            acquire_held("heartbeat")

    class Group:
        def run(self) -> None:
            with held:
                handlers[run.signal.SIGTERM](run.signal.SIGTERM, None)
            # The deferred stop may now take the lock the handler left alone.
            outcome["stopped"] = stopped.wait(timeout=2.0)

        def request_stop(self) -> None:
            acquire_held("supervisor")
            stopped.set()

    def record_handler(signum: int, handler: Any) -> None:
        handlers[signum] = handler

    monkeypatch.setattr(run, "bootstrap_service_telemetry", lambda *a, **k: _TestTelemetry())
    monkeypatch.setattr(run, "check_api_reachable", lambda *a, **k: None)
    monkeypatch.setattr(
        run, "check_slack_channel_capabilities", lambda *a, identities, **k: tuple(
            PreflightedIdentity(i, None) for i in identities
        )
    )
    monkeypatch.setattr(run, "build_supervisor", lambda *a, **k: Group())
    monkeypatch.setattr(run, "start_heartbeat", lambda *a, **k: Heartbeat())
    monkeypatch.setattr(run.signal, "signal", record_handler)

    run.main()

    assert outcome["stopped"] is True
    # Acquired, and not on the main thread the signal interrupted.
    assert outcome["supervisor"] == (True, False)
    assert outcome["heartbeat"][0] is True  # type: ignore[index]
