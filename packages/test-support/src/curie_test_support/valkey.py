"""Shared Valkey connect-and-skip helpers for the test suites.

The three constants read the same env vars with the same compose defaults every
test site used before consolidation (compose.dev.yaml maps Valkey to host port
26379, password ``valkeypass``); ``connect_or_skip`` is the sync build+ping
block those sites duplicated. Local developer loops can skip an unreachable
Valkey, but required CI must surface the original connection error.
"""

from __future__ import annotations

import os

import pytest
import redis
from redis.backoff import NoBackoff
from redis.retry import Retry

VALKEY_HOST = os.environ.get("TEST_VALKEY_HOST", "localhost")
VALKEY_PORT = int(os.environ.get("TEST_VALKEY_PORT", "26379"))
VALKEY_PW = os.environ.get("TEST_VALKEY_PW", "valkeypass")

# How long one connect attempt may block. redis-py 8.1.0 defaults
# socket_connect_timeout to 5s. A SYN-drop (typical on macOS) waits that bound;
# a RST (typical on Linux loopback) returns immediately, so this pin is not the
# Linux cost driver. Keep it anyway: the helper must not inherit a library
# default, and TEST_VALKEY_CONNECT_TIMEOUT raises the bound for a slow
# environment.
CONNECT_TIMEOUT_SECONDS = float(os.environ.get("TEST_VALKEY_CONNECT_TIMEOUT", "2"))

# No retries, deliberately, including after a successful connect.
# redis-py 8.1.0 defaults to Retry(ExponentialWithJitterBackoff, 10). That Retry
# is client-level: redis-py copies it onto each connection, so it governs
# connect AND command-path ConnectionError/TimeoutError. On Linux a
# bound-unlistening loopback port answers RST immediately, so the default retry
# ladder (not the connect timeout) is what costs ~4s; with NO_RETRY the same
# path returns in ~0s. The ~59s figures are macOS, where the SYN is dropped and
# each attempt waits the timeout. Retrying is wrong for this helper regardless
# of the cost. Its whole contract is one ping that decides skip-or-raise, the
# compose Valkey is either up before the suite starts or it is not, and a helper
# that retries turns "your stack is not running" into seconds of silence per
# fixture. Command-path retries are off for the same reason: fixtures should
# fail loud on a dropped Valkey rather than paper over it. Do not restore the
# default retry ladder to "fix" command-path transients.
NO_RETRY = Retry(NoBackoff(), 0)


def connect_or_skip(*, decode_responses: bool = True) -> redis.Redis:
    """Connect to the compose Valkey, skipping only in optional local loops.

    Builds a ``redis.Redis`` on the shared ``TEST_VALKEY_*`` connection params,
    then pings it. An unreachable Valkey skips a local test only when
    ``CI_REQUIRE_VALKEY_TESTS`` is absent. Its presence makes the original
    ``RedisError`` fail the required CI job instead. The caller owns the returned
    client (yield it from a fixture and ``.close()`` on teardown).

    The connect is bounded by ``CONNECT_TIMEOUT_SECONDS`` and does not retry
    (connect or command path), so an unreachable Valkey costs that timeout once
    rather than a retry ladder. See the constants above for why;
    ``TEST_VALKEY_CONNECT_TIMEOUT`` raises the bound for a slow environment.
    """
    client: redis.Redis = redis.Redis(
        host=VALKEY_HOST,
        port=VALKEY_PORT,
        password=VALKEY_PW or None,
        decode_responses=decode_responses,
        socket_connect_timeout=CONNECT_TIMEOUT_SECONDS,
        retry=NO_RETRY,
    )
    try:
        client.ping()
    except redis.exceptions.RedisError as exc:
        if "CI_REQUIRE_VALKEY_TESTS" in os.environ:
            raise
        pytest.skip(f"Valkey not reachable at {VALKEY_HOST}:{VALKEY_PORT}: {exc}")
    return client
