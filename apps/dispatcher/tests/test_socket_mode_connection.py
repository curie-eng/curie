"""The Socket Mode connection's lifecycle: what a failed connect and an early
stop leave behind.

ADR-0168 decision 2 makes one identity reconnecting forever, while the others
serve, a steady state, so a failed attempt must cost nothing that outlives it.
No test here opens a socket: the WSS URL request is made to fail, or the
handler is replaced with a recorder.
"""

import threading
from typing import Any

import pytest
from curie_dispatcher import app as app_module
from curie_dispatcher.app import SocketModeConnection
from curie_dispatcher.supervisor import Supervisor
from slack_bolt import App
from slack_sdk.socket_mode.builtin.client import SocketModeClient

from .conftest import _authorize


def _app() -> App:
    return App(
        signing_secret="unused-in-socket-mode",
        authorize=_authorize,
        token_verification_enabled=False,
    )


def test_repeated_failed_connects_leave_no_thread_running(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Each attempt builds a real Socket Mode client, which starts its own
    threads at construction; a connect that fails must stop all of them."""

    def refuse(_self: SocketModeClient) -> str:
        raise RuntimeError("invalid_auth")

    monkeypatch.setattr(SocketModeClient, "issue_new_wss_url", refuse)
    app = _app()
    failed_attempts = 3
    built: list[SocketModeConnection] = []

    def connect() -> SocketModeConnection:
        if len(built) == failed_attempts:
            # The next attempt is the last: stop before it runs, so the
            # supervisor closes it without connecting and returns.
            supervisor.request_stop()
        connection = SocketModeConnection(app, "xapp-test")
        built.append(connection)
        return connection

    supervisor = Supervisor(connect, sleep=lambda _seconds: None)
    before = set(threading.enumerate())

    supervisor.run()

    assert len(built) == failed_attempts + 1
    leaked = [t for t in threading.enumerate() if t not in before and t.is_alive()]
    assert len(leaked) <= 1, [t.name for t in leaked]


class _RecordingHandler:
    """Stands in for Bolt's SocketModeHandler: records connect and close."""

    def __init__(self, app: App, app_token: str) -> None:
        del app, app_token
        self.events: list[str] = []
        self.client = type("Client", (), {"message_listeners": []})()
        self.during_connect: Any = None

    def connect(self) -> None:
        self.events.append("connect")
        if self.during_connect is not None:
            self.during_connect()
        self.events.append("connected")

    def close(self) -> None:
        self.events.append("close")


@pytest.fixture
def recorded_handler(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(app_module, "SocketModeHandler", _RecordingHandler)


def _run_bounded(connection: SocketModeConnection) -> bool:
    """Run ``connection`` on a thread; True when ``run`` returned within 2 s."""
    thread = threading.Thread(target=connection.run, daemon=True)
    thread.start()
    thread.join(timeout=2.0)
    returned = not thread.is_alive()
    # Unblock a run that did not return, so it does not outlive the test.
    connection._closed.set()
    return returned


def test_a_close_before_run_is_not_undone_by_run(recorded_handler: None) -> None:
    """The supervisor publishes a connection and then runs it; a stop in
    between closes it first, and that close must stand."""
    connection = SocketModeConnection(_app(), "xapp-test")
    handler: _RecordingHandler = connection._handler  # type: ignore[assignment]

    connection.close()

    assert _run_bounded(connection)
    assert "connect" not in handler.events


def test_a_close_during_connect_closes_the_session_connect_opened(
    recorded_handler: None,
) -> None:
    """A close that lands while connect is in flight tears down a handler
    that connect then brings up; the session it opened must be closed too."""
    connection = SocketModeConnection(_app(), "xapp-test")
    handler: _RecordingHandler = connection._handler  # type: ignore[assignment]
    handler.during_connect = connection.close

    assert _run_bounded(connection)
    assert handler.events[:3] == ["connect", "close", "connected"]
    assert handler.events[3:] == ["close"]
