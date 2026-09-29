"""Connect-time Socket Mode hello: warn when more than one client is connected.

The first tests invoke the listener registered on ``SocketModeConnection``
directly. The refresh tests drive slack_sdk's real ``SocketModeClient`` over a
stand-in websocket (``offline_socket_mode``), feeding Slack's frames through the
SDK's own message queue. Nothing here sends Slack.
"""

import logging
from typing import Any

import pytest
from curie_dispatcher.app import SocketModeConnection
from slack_bolt import App

from .conftest import _authorize, deliver_frames

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


def _connected(logger: logging.Logger) -> SocketModeConnection:
    """A connection whose SDK client has made its first connect, as ``run`` does."""
    conn = _connection(logger)
    conn._handler.connect()
    deliver_frames(conn._handler.client, {"type": "hello", "num_connections": 1})
    return conn


def test_hello_after_a_slack_refresh_does_not_count_this_clients_previous_socket(
    offline_socket_mode: None,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Slack's periodic connection refresh is not a second client.

    Slack asks for a refresh with a ``disconnect`` frame. slack_sdk answers it
    by opening the replacement socket first and closing the old one only once
    the new one is up (``SocketModeClient.connect``, slack_sdk 3.44.1), so the
    hello Slack sends on the replacement counts both of this client's sockets.
    """

    monkeypatch.setenv("CURIE_RELEASE_IDENTITY", _IDENTITY)
    conn = _connected(logging.getLogger("test-socket-mode-hello-refresh"))
    client = conn._handler.client
    previous = client.current_session
    try:
        with caplog.at_level(logging.WARNING):
            deliver_frames(
                client,
                {"type": "disconnect", "reason": "refresh_requested"},
                {"type": "hello", "num_connections": 2},
            )

        assert client.current_session is not previous, "the SDK did not replace its socket"
        assert previous is not None and not previous.is_active()
        assert _ONE_RELEASE_PHRASE not in _warning_text(caplog)
    finally:
        conn.close()


def test_hello_after_a_slack_refresh_still_warns_about_a_second_client(
    offline_socket_mode: None,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Discounting this client's previous socket leaves another client visible.

    With a second client on the app, the refresh hello counts the replacement,
    the socket it replaces, and the other client.
    """

    monkeypatch.setenv("CURIE_RELEASE_IDENTITY", _IDENTITY)
    conn = _connected(logging.getLogger("test-socket-mode-hello-refresh-shared"))
    try:
        with caplog.at_level(logging.WARNING):
            deliver_frames(
                conn._handler.client,
                {"type": "disconnect", "reason": "refresh_requested"},
                {"type": "hello", "num_connections": 3},
            )

        warnings = _warning_text(caplog)
        assert _ONE_RELEASE_PHRASE in warnings
        assert _IDENTITY in warnings
    finally:
        conn.close()


def test_hello_after_reconnecting_a_closed_socket_warns_about_a_second_client(
    offline_socket_mode: None,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Only a socket still open at the new handshake is discounted.

    When the link is already gone (Slack closed it, or the SDK's ping check
    dropped it as stale), the SDK reconnects with nothing left open, so a
    second connection in the hello is another client.
    """

    monkeypatch.setenv("CURIE_RELEASE_IDENTITY", _IDENTITY)
    conn = _connected(logging.getLogger("test-socket-mode-hello-reconnect"))
    client = conn._handler.client
    try:
        assert client.current_session is not None
        client.current_session.close()
        # What the SDK's close listener and session monitor call on a lost link.
        client.connect_to_new_endpoint()
        assert client.is_connected()

        with caplog.at_level(logging.WARNING):
            deliver_frames(client, {"type": "hello", "num_connections": 2})

        assert _ONE_RELEASE_PHRASE in _warning_text(caplog)
    finally:
        conn.close()
