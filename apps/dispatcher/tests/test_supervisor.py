"""Reconnect supervision: backoff on drops, and graceful shutdown. No real socket."""

import logging
import threading

import pytest
from curie_dispatcher.supervisor import BackoffPolicy, Supervisor


def test_backoff_is_exponential_and_capped() -> None:
    policy = BackoffPolicy(initial_seconds=1.0, max_seconds=10.0, multiplier=2.0)
    assert policy.delay(0) == 1.0
    assert policy.delay(1) == 2.0
    assert policy.delay(2) == 4.0
    assert policy.delay(3) == 8.0
    # Capped: 16 -> 10.
    assert policy.delay(4) == 10.0
    assert policy.delay(10) == 10.0


class DroppingConnection:
    """A connection whose run() returns immediately, simulating an instant drop."""

    def __init__(self) -> None:
        self.closed = False

    def run(self) -> None:
        return

    def close(self) -> None:
        self.closed = True


def test_supervisor_reconnects_with_growing_backoff() -> None:
    connect_count = 0

    def connect() -> DroppingConnection:
        nonlocal connect_count
        connect_count += 1
        return DroppingConnection()

    slept: list[float] = []

    def fake_sleep(seconds: float) -> None:
        slept.append(seconds)
        # Stop after the third reconnect so run() terminates deterministically.
        if len(slept) >= 3:
            supervisor.request_stop()

    supervisor = Supervisor(
        connect,
        backoff=BackoffPolicy(initial_seconds=1.0, max_seconds=100.0, multiplier=2.0),
        sleep=fake_sleep,
    )

    supervisor.run()

    # Three drops -> three backoff sleeps, growing exponentially.
    assert slept == [1.0, 2.0, 4.0]
    # One initial connect plus one per reconnect after the first two sleeps.
    assert connect_count >= 3


class BlockingConnection:
    """Blocks in run() until close() is called; models a healthy live connection."""

    def __init__(self) -> None:
        self._released = threading.Event()
        self.closed = False
        self.ran = False

    def run(self) -> None:
        self.ran = True
        self._released.wait()

    def close(self) -> None:
        self.closed = True
        self._released.set()


def test_request_stop_closes_connection_and_exits_without_reconnect() -> None:
    conn = BlockingConnection()
    connect_count = 0

    def connect() -> BlockingConnection:
        nonlocal connect_count
        connect_count += 1
        return conn

    slept: list[float] = []
    supervisor = Supervisor(connect, sleep=slept.append)

    thread = threading.Thread(target=supervisor.run)
    thread.start()

    # Wait until the connection is actually running, then ask to stop.
    assert _wait_for(lambda: conn.ran, timeout=2.0)
    supervisor.request_stop()

    thread.join(timeout=2.0)
    assert not thread.is_alive()
    assert conn.closed is True
    # Graceful shutdown: no reconnect attempt, no backoff sleep.
    assert connect_count == 1
    assert slept == []


def _wait_for(predicate: object, timeout: float) -> bool:
    import time

    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():  # type: ignore[operator]
            return True
        time.sleep(0.01)
    return False


def test_backoff_never_raises_however_many_attempts() -> None:
    policy = BackoffPolicy(initial_seconds=1.0, max_seconds=30.0, multiplier=2.0)
    # 2.0**1024 overflows a float; a revoked token reaches it in about 8.5 hours.
    assert policy.delay(1024) == 30.0
    assert policy.delay(10_000) == 30.0


class _SignalOn(logging.Handler):
    """Sets ``seen`` once a record containing ``needle`` is emitted."""

    def __init__(self, needle: str) -> None:
        super().__init__()
        self.needle = needle
        self.seen = threading.Event()

    def emit(self, record: logging.LogRecord) -> None:
        if self.needle in record.getMessage():
            self.seen.set()


def _signalling_logger(name: str, needle: str) -> tuple[logging.Logger, _SignalOn]:
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    signal = _SignalOn(needle)
    logger.addHandler(signal)
    return logger, signal


def test_a_stop_during_a_long_backoff_returns_promptly() -> None:
    def refused() -> BlockingConnection:
        raise RuntimeError("app token revoked")

    logger, backing_off = _signalling_logger("test-supervisor-backoff-stop", "reconnecting in")
    supervisor = Supervisor(
        refused,
        backoff=BackoffPolicy(initial_seconds=60.0, max_seconds=60.0),
        logger=logger,
    )
    thread = threading.Thread(target=supervisor.run, daemon=True)
    try:
        thread.start()
        assert backing_off.seen.wait(timeout=2.0)

        supervisor.request_stop()
        thread.join(timeout=2.0)

        assert not thread.is_alive()
    finally:
        logger.removeHandler(backing_off)


def test_a_stop_that_arrives_while_connecting_is_not_lost() -> None:
    conn = BlockingConnection()

    def connect() -> BlockingConnection:
        # The stop lands after the loop's check and before the connection runs.
        supervisor.request_stop()
        return conn

    supervisor = Supervisor(connect, sleep=lambda _seconds: None)
    thread = threading.Thread(target=supervisor.run, daemon=True)
    thread.start()
    thread.join(timeout=2.0)

    assert not thread.is_alive()
    assert not conn.ran
    assert conn.closed


class _CloseRaises(BlockingConnection):
    def close(self) -> None:
        super().close()
        raise RuntimeError("close failed")


def test_a_close_error_on_shutdown_names_the_identity(caplog: pytest.LogCaptureFixture) -> None:
    logger = logging.getLogger("test-supervisor-close-error")
    conn = _CloseRaises()
    supervisor = Supervisor(
        lambda: conn, logger=logger, label="Slack identity ops-bot", sleep=lambda _s: None
    )
    thread = threading.Thread(target=supervisor.run, daemon=True)

    with caplog.at_level(logging.INFO, logger=logger.name):
        thread.start()
        assert _wait_for(lambda: conn.ran, timeout=2.0)
        supervisor.request_stop()
        thread.join(timeout=2.0)

    assert not thread.is_alive()
    messages = [r.getMessage() for r in caplog.records if r.name == logger.name]
    assert messages == ["Slack identity ops-bot: error closing connection during shutdown"]
