"""build_redis TLS selection (#2315).

A BYO TLS-only Valkey/Redis (in-transit-encrypted ElastiCache, Azure Cache for
Redis, Redis Cloud, Upstash, ...) must produce an SSL-wrapped connection pool,
mirroring the worker's `_valkey_kwargs` seam and the API's `valkey_dsn` scheme
switch (#2315). Construction performs no I/O -- no Valkey is contacted here --
so these assertions read redis-py's own connection-class selection rather than
mocking the thing under test.
"""

from __future__ import annotations

import pytest
import redis
from curie_dispatcher.app import build_redis
from curie_dispatcher.config import DispatcherConfig

# An independent, non-secret placeholder credential -- unrelated to any real
# deployment. DispatcherConfig requires it non-blank and distinct from
# api_key (ADR-0106); the tests here don't touch that boundary, they just need
# a valid config to build a client from.
_ATTESTER_PLACEHOLDER = "dispatcher-redis-test-value"


def _config(**overrides: object) -> DispatcherConfig:
    return DispatcherConfig(
        approval_chat_attester_secret=_ATTESTER_PLACEHOLDER, **overrides
    )


def test_build_redis_selects_the_plain_connection_by_default() -> None:
    client = build_redis(_config())
    assert client.connection_pool.connection_class is redis.connection.Connection


def test_build_redis_selects_ssl_connection_when_tls_is_set() -> None:
    client = build_redis(_config(valkey_tls=True))
    assert client.connection_pool.connection_class is redis.connection.SSLConnection


# --- VALKEY_TLS env -> DispatcherConfig.valkey_tls (the seam the chart
# actually drives; a field that only works via kwargs is not wired) ---------


def test_valkey_tls_env_reaches_the_dispatcher_config(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("VALKEY_TLS", "true")
    assert _config().valkey_tls is True


def test_valkey_tls_defaults_false_on_a_clean_env(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # VALKEY_TLS may be set in the ambient host/CI env; clear it explicitly so
    # this default-value assertion cannot be flipped by something outside the
    # test.
    monkeypatch.delenv("VALKEY_TLS", raising=False)
    assert _config().valkey_tls is False
