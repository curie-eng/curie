"""Connect-time Socket Mode hello: warn when more than one client is connected.

Invokes the listener registered on ``SocketModeConnection`` directly. Does not
call ``run()`` or ``connect()``, and does not send Slack.
"""

import logging
from typing import Any

import pytest
from curie_dispatcher.app import SocketModeConnection
from slack_bolt import App

from .conftest import _authorize

_IDENTITY = "curie-hello-identity-2660"
_ONE_RELEASE_PHRASE = "exactly one Curie release may connect to a given Slack app"


def _connection(logger: logging.Logger | None = None) -> SocketModeConnection:
    app = App(
        signing_secret="unused-in-socket-mode",
        authorize=_authorize,
        token_verification_enabled=False,
    )
    return SocketModeConnection(app, app_token="xapp-test", logger=logger)


def _invoke_registered_listener(
    conn: SocketModeConnection, message: dict[str, Any], raw: str = ""
) -> None:
    client = conn._handler.client
    conn._handler.client.message_listeners[-1](client, message, raw)


def _warning_text(caplog: pytest.LogCaptureFixture) -> str:
    return "\n".join(
        record.getMessage() for record in caplog.records if record.levelno >= logging.WARNING
    )


def test_hello_with_two_connections_warns_with_release_identity(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A hello with num_connections > 1 warns, naming this release.

    Socket Mode hello includes num_connections (Slack multiple-connections
    contract). https://docs.slack.dev/apis/events-api/using-socket-mode/
    """

    monkeypatch.setenv("CURIE_RELEASE_IDENTITY", _IDENTITY)
    logger = logging.getLogger("test-socket-mode-hello")
    conn = _connection(logger)

    with caplog.at_level(logging.WARNING):
        _invoke_registered_listener(
            conn,
            {"type": "hello", "num_connections": 2},
        )

    warnings = _warning_text(caplog)
    assert any(record.levelno == logging.WARNING for record in caplog.records), warnings
    # The full stock message, not a substring: with no ``slack_identity`` this
    # must never grow an "of Slack identity ..." suffix.
    assert warnings == f"{_IDENTITY}: {_ONE_RELEASE_PHRASE}; disconnect extra clients"


def test_hello_with_one_connection_does_not_warn(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A solo Socket Mode client is the expected shape; do not warn."""

    monkeypatch.setenv("CURIE_RELEASE_IDENTITY", _IDENTITY)
    conn = _connection(logging.getLogger("test-socket-mode-hello-solo"))

    with caplog.at_level(logging.WARNING):
        _invoke_registered_listener(
            conn,
            {"type": "hello", "num_connections": 1},
        )

    assert _ONE_RELEASE_PHRASE not in _warning_text(caplog)


def test_non_hello_message_does_not_warn(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Only the hello frame is a connection-count signal."""

    monkeypatch.setenv("CURIE_RELEASE_IDENTITY", _IDENTITY)
    conn = _connection(logging.getLogger("test-socket-mode-hello-nonhello"))

    with caplog.at_level(logging.WARNING):
        _invoke_registered_listener(
            conn,
            {"type": "events_api", "num_connections": 2},
        )

    assert _ONE_RELEASE_PHRASE not in _warning_text(caplog)


def test_unparseable_num_connections_does_not_raise(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A hello whose num_connections is not an int is a no-op, not a crash."""

    monkeypatch.setenv("CURIE_RELEASE_IDENTITY", _IDENTITY)
    conn = _connection(logging.getLogger("test-socket-mode-hello-unparseable"))

    with caplog.at_level(logging.WARNING):
        _invoke_registered_listener(
            conn,
            {"type": "hello", "num_connections": "nope"},
        )

    assert _ONE_RELEASE_PHRASE not in _warning_text(caplog)
