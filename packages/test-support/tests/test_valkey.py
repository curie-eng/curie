"""Contract tests for the shared Valkey test helper."""

from __future__ import annotations

import socket
from collections.abc import Callable

import pytest
import redis
from curie_test_support import valkey


def _configure_unreachable_valkey(monkeypatch: pytest.MonkeyPatch) -> socket.socket:
    """Bind a loopback port without listening, preventing a port-reuse race."""
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.bind(("127.0.0.1", 0))
    monkeypatch.setattr(valkey, "VALKEY_HOST", "127.0.0.1")
    monkeypatch.setattr(valkey, "VALKEY_PORT", int(listener.getsockname()[1]))
    return listener


def _capture_constructed_clients(monkeypatch: pytest.MonkeyPatch) -> list[redis.Redis]:
    """Wrap redis.Redis so the test can inspect the client connect_or_skip built."""
    captured: list[redis.Redis] = []
    original: Callable[..., redis.Redis] = valkey.redis.Redis

    def capture(*args: object, **kwargs: object) -> redis.Redis:
        client = original(*args, **kwargs)
        captured.append(client)
        return client

    monkeypatch.setattr(valkey.redis, "Redis", capture)
    return captured


def test_connect_or_skip_skips_an_unreachable_valkey_locally(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("CI_REQUIRE_VALKEY_TESTS", raising=False)
    listener = _configure_unreachable_valkey(monkeypatch)

    with listener:
        with pytest.raises(pytest.skip.Exception, match="Valkey not reachable"):
            valkey.connect_or_skip()


# Which error an unreachable port produces is a platform detail, not a contract.
# A bound but unlistening loopback port refuses on Linux (ConnectionError) and is
# dropped on macOS (TimeoutError), so pinning ConnectionError alone made this
# test fail on every Mac. What connect_or_skip promises is that a required run
# RAISES rather than skips; both classes are that promise kept.
UNREACHABLE = (redis.exceptions.ConnectionError, redis.exceptions.TimeoutError)


def test_connect_or_skip_fails_for_an_unreachable_valkey_when_required(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("CI_REQUIRE_VALKEY_TESTS", "")
    listener = _configure_unreachable_valkey(monkeypatch)

    with listener:
        with pytest.raises(UNREACHABLE):
            valkey.connect_or_skip()


def test_connect_or_skip_client_has_no_retry_and_a_connect_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Removing either constructor pin must fail this test on Linux.

    The previous control asserted elapsed < 5 against a bound-unlistening
    loopback port. On Linux that port answers RST immediately, so redis-py
    8.1.0 without these pins already returned in ~4.3s and both mutations
    stayed green. The ~59s figures are macOS, where the SYN is dropped.
    Inspect the client's effective configuration instead: that is what the
    two pins change, and it does not depend on SYN-drop vs RST.

    redis-py 8.1.0 defaults socket_connect_timeout to 5s, so asserting the
    timeout is merely set would not catch deleting the kwarg. The bound must
    equal CONNECT_TIMEOUT_SECONDS. Default retry.get_retries() is 10, so
    retries == 0 catches deleting retry=NO_RETRY.
    """
    monkeypatch.delenv("CI_REQUIRE_VALKEY_TESTS", raising=False)
    captured = _capture_constructed_clients(monkeypatch)
    listener = _configure_unreachable_valkey(monkeypatch)

    with listener:
        with pytest.raises(pytest.skip.Exception, match="Valkey not reachable"):
            valkey.connect_or_skip()

    assert captured, "connect_or_skip never constructed a Redis client"
    client = captured[0]
    try:
        retry = client.get_retry()
        assert retry is not None
        assert retry.get_retries() == 0
        timeout = client.connection_pool.connection_kwargs.get("socket_connect_timeout")
        assert timeout == valkey.CONNECT_TIMEOUT_SECONDS
    finally:
        client.close()
