"""Every Valkey client the API builds is bounded (#5995).

The lifespan clients are read off a real application; the behavior cases use
a client built the way production builds one, against a real loopback port
that accepts and never answers, and against the suite's real Valkey.
"""

from __future__ import annotations

import time
import uuid
from collections.abc import Iterator
from typing import Any

import pytest
from _tcp_blackhole import silent_listener, within
from curie_api.config import get_settings
from curie_api.main import create_app
from curie_internal.valkey import AsyncRedis
from fastapi.testclient import TestClient
from identity_writer_lifetime_support import run
from redis.exceptions import TimeoutError as RedisTimeoutError


@pytest.fixture
def valkey_bounds(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    monkeypatch.setenv("VALKEY_SOCKET_TIMEOUT_SECONDS", "0.5")
    monkeypatch.setenv("VALKEY_SOCKET_CONNECT_TIMEOUT_SECONDS", "0.4")
    get_settings.cache_clear()
    yield
    monkeypatch.undo()
    get_settings.cache_clear()


def bounded_kwargs(client: Any) -> tuple[Any, Any, Any]:
    kwargs = client.connection_pool.connection_kwargs
    return (
        kwargs.get("socket_timeout"),
        kwargs.get("socket_connect_timeout"),
        kwargs.get("socket_keepalive"),
    )


def test_lifespan_valkey_clients_carry_the_socket_bounds(
    valkey_bounds: None, _disposable_db: Any
) -> None:
    with TestClient(create_app()) as client:
        assert bounded_kwargs(client.app.state.valkey) == (0.5, 0.4, True)
        assert bounded_kwargs(client.app.state.credential_valkey) == (0.5, 0.4, True)
        # retry_reads stays off on the credential client only.
        assert client.app.state.credential_valkey._retry_reads is False
        assert client.app.state.valkey._retry_reads is True


def test_unanswered_command_times_out_within_the_bound(valkey_bounds: None) -> None:
    async def check() -> None:
        async with silent_listener() as listener:
            client = AsyncRedis.from_url(
                f"redis://127.0.0.1:{listener.port}/0", **get_settings().valkey_client_options()
            )
            try:
                assert bounded_kwargs(client) == (0.5, 0.4, True)
                started = time.monotonic()
                async with within(10, "a command Valkey never answers"):
                    with pytest.raises(RedisTimeoutError):
                        await client.set("curie:test:valkey-bound", "1")
                assert time.monotonic() - started < 3
            finally:
                await client.aclose()

    run(check)


def test_bounded_client_still_serves_real_commands(valkey_bounds: None) -> None:
    async def check() -> None:
        settings = get_settings()
        client = AsyncRedis.from_url(settings.valkey_dsn(), **settings.valkey_client_options())
        key = f"curie:test:valkey-bound:{uuid.uuid4().hex}"
        try:
            assert await client.set(key, "value") is True
            assert await client.get(key) == b"value"
        finally:
            await client.delete(key)
            await client.aclose()

    run(check)
