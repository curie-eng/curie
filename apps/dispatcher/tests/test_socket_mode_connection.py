"""The Socket Mode connection's lifecycle: what a failed connect and an early
stop leave behind.

ADR-0168 decision 2 makes one identity reconnecting forever, while the others
serve, a steady state, so a failed attempt must cost nothing that outlives it.
No test here opens a socket: the WSS URL request is made to fail, or the
handler is replaced with a recorder.
"""

import logging
import threading
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any

import pytest
from curie_dispatcher import app as app_module
from curie_dispatcher.app import SocketModeConnection
from curie_dispatcher.socket_presence import SocketPresence
from curie_dispatcher.supervisor import Supervisor
from slack_bolt import App
from slack_sdk.socket_mode.builtin import client as sdk_client
from slack_sdk.socket_mode.builtin.client import SocketModeClient

from .conftest import SlackSocketStandIn, _authorize


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

    def __init__(self, app: App, app_token: str, logger: Any = None) -> None:
        del app, app_token, logger
        self.events: list[str] = []
        self.client = type(
            "Client",
            (),
            {
                "message_listeners": [],
                "connect": lambda _self: None,
                "is_connected": lambda _self: False,
            },
        )()
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
    if not returned:
        connection.close()
        thread.join(timeout=2.0)
        assert not thread.is_alive(), "Socket Mode run thread survived cleanup"
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


@dataclass
class _SocketControl:
    attempts: int = 0
    fail_next: bool = False
    block_next: bool = False
    entered: threading.Event = field(default_factory=threading.Event)
    release: threading.Event = field(default_factory=threading.Event)


@pytest.fixture
def controlled_socket(
    offline_socket_mode: None, monkeypatch: pytest.MonkeyPatch
) -> Iterator[_SocketControl]:
    control = _SocketControl()

    class ControlledSocket(SlackSocketStandIn):
        # Observed in slack_sdk 3.45.0 builtin SocketModeClient.connect:
        # Connection.connect() runs before current_session is replaced. Both
        # refresh and reconnect enter the client's wrapped connect method.
        def connect(self) -> None:
            control.attempts += 1
            if control.block_next:
                control.block_next = False
                control.entered.set()
                if not control.release.wait(5):
                    raise TimeoutError("test did not release the Slack transport")
            if control.fail_next:
                control.fail_next = False
                raise RuntimeError("Slack transport refused connection")
            super().connect()

    monkeypatch.setattr(sdk_client, "Connection", ControlledSocket)
    try:
        yield control
    finally:
        control.release.set()


class _ConnectedLog(logging.Handler):
    def __init__(self) -> None:
        super().__init__()
        self.seen = threading.Event()

    def emit(self, record: logging.LogRecord) -> None:
        if record.getMessage().startswith("socket mode connected identity="):
            self.seen.set()


@dataclass
class _RunningSocket:
    connection: SocketModeConnection
    thread: threading.Thread
    done: threading.Event
    errors: list[BaseException]
    close_threads: list[threading.Thread]
    now: float = 0.0

    def replace(self, at: float) -> None:
        self.now = at
        self.connection._handler.client.connect()


@contextmanager
def _running_socket(
    monkeypatch: pytest.MonkeyPatch,
    *,
    presence: SocketPresence | None = None,
    slack_identity: str | None = "acme-bot",
) -> Iterator[_RunningSocket]:
    logger = logging.getLogger("test-socket-sessions")
    previous_level = logger.level
    logger.setLevel(logging.INFO)
    connected = _ConnectedLog()
    logger.addHandler(connected)
    done = threading.Event()
    errors: list[BaseException] = []
    close_threads: list[threading.Thread] = []
    connection = SocketModeConnection(
        _app(), "xapp-test", logger=logger, slack_identity=slack_identity,
        presence=presence, clock=lambda: running.now,
    )
    sdk_close = connection._handler.close

    def close() -> None:
        close_threads.append(threading.current_thread())
        sdk_close()

    monkeypatch.setattr(connection._handler, "close", close)

    def run() -> None:
        try:
            connection.run()
        except BaseException as exc:  # noqa: BLE001 - report thread failures to the test
            errors.append(exc)
        finally:
            done.set()

    thread = threading.Thread(target=run, name="test-socket-run", daemon=True)
    running = _RunningSocket(connection, thread, done, errors, close_threads)
    try:
        thread.start()
        assert connected.seen.wait(2), "initial Socket Mode connect did not complete"
        yield running
    finally:
        connection.close()
        thread.join(2)
        logger.removeHandler(connected)
        logger.setLevel(previous_level)
        assert not thread.is_alive(), "Socket Mode run thread survived cleanup"


def _session_warnings(caplog: pytest.LogCaptureFixture) -> list[str]:
    return [
        record.getMessage() for record in caplog.records
        if record.levelno == logging.WARNING
        and record.getMessage().startswith("socket mode sessions failing ")
    ]


@pytest.mark.parametrize("with_presence", [False, True])
@pytest.mark.parametrize("slack_identity", [None, "acme-bot"])
def test_three_failed_sessions_escalate(
    controlled_socket: _SocketControl,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    with_presence: bool,
    slack_identity: str | None,
) -> None:
    monkeypatch.setenv("CURIE_RELEASE_IDENTITY", "acme-release")
    presence = SocketPresence(1, sample_interval_s=3600) if with_presence else None
    with _running_socket(monkeypatch, presence=presence, slack_identity=slack_identity) as running:
        running.replace(10)
        running.replace(20)
        assert not running.done.is_set()
        running.now = 30
        # Observed in slack_sdk 3.45.0 BaseSocketModeClient.process_message:
        # a refresh runs connect on the message processor. Close joins that
        # thread, so teardown must run outside this callback.
        running.connection._handler.client.enqueue_message('{"type":"disconnect"}')

        assert running.done.wait(5), "third replacement did not wake run"
        assert len(running.errors) == 1
        assert isinstance(running.errors[0], app_module.SocketSessionsFailing)
        assert str(running.errors[0]) == "Socket Mode sessions failed 3 times within 300 seconds"
        assert controlled_socket.attempts == 4
        assert running.close_threads == [running.thread]
        assert not running.connection.is_connected()
        assert _session_warnings(caplog) == [
            "socket mode sessions failing identity=acme-release "
            f"slack_identity={slack_identity or 'absent'} failures=3 window_s=300"
        ]


@pytest.mark.parametrize("times", [(10, 20), (0, 150, 300.001), (0, 301, 602)])
def test_replacements_below_the_sliding_limit_remain_blocking(
    controlled_socket: _SocketControl,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    times: tuple[float, ...],
) -> None:
    with _running_socket(monkeypatch) as running:
        for at in times:
            running.replace(at)
        assert not running.done.wait(0.05)
        assert running.errors == []
        assert running.close_threads == []
        assert controlled_socket.attempts == 1 + len(times)
        assert _session_warnings(caplog) == []
        running.connection.close()
        assert running.done.wait(2)
        assert running.errors == []
        assert not running.connection.is_connected()


def test_replacements_at_exact_window_boundary_escalate(
    controlled_socket: _SocketControl, monkeypatch: pytest.MonkeyPatch
) -> None:
    with _running_socket(monkeypatch) as running:
        for at in (10, 160, 310):
            running.replace(at)
        assert running.done.wait(5)
        assert len(running.errors) == 1
        assert isinstance(running.errors[0], app_module.SocketSessionsFailing)
        assert controlled_socket.attempts == 4


def test_replacement_connect_exceptions_count_once(
    controlled_socket: _SocketControl,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    with _running_socket(monkeypatch) as running:
        for at in (10, 20, 30):
            controlled_socket.fail_next = True
            with pytest.raises(RuntimeError, match="Slack transport refused connection"):
                running.replace(at)
            if at < 30:
                assert not running.done.wait(0.05)
                assert _session_warnings(caplog) == []
        assert running.done.wait(5)
        assert len(running.errors) == 1
        assert isinstance(running.errors[0], app_module.SocketSessionsFailing)
        assert controlled_socket.attempts == 4
        assert running.close_threads == [running.thread]
        assert len(_session_warnings(caplog)) == 1


def test_initial_connect_failure_remains_ordinary(
    controlled_socket: _SocketControl, caplog: pytest.LogCaptureFixture
) -> None:
    controlled_socket.fail_next = True
    connection = SocketModeConnection(_app(), "xapp-test")
    try:
        with pytest.raises(RuntimeError, match="Slack transport refused connection") as raised:
            connection.run()
        assert not isinstance(raised.value, app_module.SocketSessionsFailing)
        assert controlled_socket.attempts == 1
        assert connection._handler.client.closed
        assert _session_warnings(caplog) == []
    finally:
        connection.close()


def test_escalation_waits_for_the_in_flight_replacement(
    controlled_socket: _SocketControl, monkeypatch: pytest.MonkeyPatch
) -> None:
    with _running_socket(monkeypatch) as running:
        running.replace(10)
        running.replace(20)
        controlled_socket.block_next = True
        replacement_errors: list[BaseException] = []

        def replace() -> None:
            try:
                running.replace(30)
            except BaseException as exc:  # noqa: BLE001 - report thread failures to the test
                replacement_errors.append(exc)

        replacement = threading.Thread(target=replace, daemon=True)
        replacement.start()
        try:
            assert controlled_socket.entered.wait(2)
            assert not running.done.wait(0.05)
            assert running.close_threads == []
        finally:
            controlled_socket.release.set()
            replacement.join(2)
        assert not replacement.is_alive()
        assert replacement_errors == []
        assert running.done.wait(5)
        assert len(running.errors) == 1
        assert isinstance(running.errors[0], app_module.SocketSessionsFailing)
        assert running.close_threads == [running.thread]
        assert not running.connection.is_connected()


def test_close_during_replacement_does_not_leave_a_reopened_session(
    controlled_socket: _SocketControl,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    with _running_socket(monkeypatch) as running:
        controlled_socket.block_next = True
        replacement_errors: list[BaseException] = []

        def replace() -> None:
            try:
                running.replace(10)
            except BaseException as exc:  # noqa: BLE001 - report thread failures to the test
                replacement_errors.append(exc)

        replacement = threading.Thread(target=replace, daemon=True)
        replacement.start()
        try:
            assert controlled_socket.entered.wait(2)
            running.connection.close()
        finally:
            controlled_socket.release.set()
            replacement.join(2)
        assert not replacement.is_alive()
        assert replacement_errors == []
        assert running.done.wait(5)
        assert running.errors == []
        assert not running.connection.is_connected()
        running.connection._handler.client.connect()
        assert controlled_socket.attempts == 2
        assert not running.connection.is_connected()
        assert _session_warnings(caplog) == []
