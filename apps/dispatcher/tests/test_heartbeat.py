"""Heartbeat liveness for the dispatcher (issue #71).

The dispatcher runs its Socket Mode loop on a background thread, so its
heartbeat is a plain daemon thread that touches a file every interval. A probe
reads the file's mtime; if the thread dies or wedges, the mtime goes stale.
These tests drive the real thread against a real file (tmp_path) with a tiny
interval, using the polling helper style of test_supervisor.py.
"""

import threading
import time

import pytest
from curie_channel_gateway.slack.config import DispatcherConfig
from curie_dispatcher.heartbeat import start_heartbeat


def _wait_for(predicate, timeout: float) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.01)
    return False


def test_start_heartbeat_touches_file_then_stops_on_event(tmp_path) -> None:
    path = tmp_path / "dispatcher.heartbeat"

    stop = start_heartbeat(str(path), 0.02)
    assert isinstance(stop, threading.Event)
    try:
        # Touched immediately (well within a second at a 0.02s interval).
        assert _wait_for(path.exists, timeout=1.0)
        assert abs(path.stat().st_mtime - time.time()) < 1.0

        # It keeps touching: the mtime advances across intervals.
        first = path.stat().st_mtime_ns
        assert _wait_for(lambda: path.stat().st_mtime_ns > first, timeout=1.0)
    finally:
        stop.set()

    # After stop, touching ceases: the mtime freezes.
    time.sleep(0.1)
    frozen = path.stat().st_mtime_ns
    time.sleep(0.1)
    assert path.stat().st_mtime_ns == frozen


def test_dispatcher_config_heartbeat_defaults(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("CURIE_HEARTBEAT_FILE", raising=False)
    monkeypatch.delenv("CURIE_HEARTBEAT_INTERVAL_SECONDS", raising=False)
    monkeypatch.setenv(
        "CURIE_APPROVAL_CHAT_ATTESTER_SECRET", "dispatcher-attester-test-secret"
    )

    cfg = DispatcherConfig()

    assert cfg.heartbeat_file == "/tmp/curie-dispatcher.heartbeat"
    assert cfg.heartbeat_interval_s == 10.0


def test_dispatcher_config_reads_heartbeat_env(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("CURIE_HEARTBEAT_FILE", "/var/run/curie/dp.hb")
    monkeypatch.setenv("CURIE_HEARTBEAT_INTERVAL_SECONDS", "3.5")
    monkeypatch.setenv(
        "CURIE_APPROVAL_CHAT_ATTESTER_SECRET", "dispatcher-attester-test-secret"
    )

    cfg = DispatcherConfig()

    assert cfg.heartbeat_file == "/var/run/curie/dp.hb"
    assert cfg.heartbeat_interval_s == 3.5
