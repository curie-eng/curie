"""Renew-only upgrade authority against real Valkey (#4295 AC1)."""

from __future__ import annotations

import asyncio
import importlib
import json
import uuid
from collections.abc import Iterator
from types import ModuleType

import pytest
from curie_test_support.valkey import NO_RETRY, VALKEY_HOST, VALKEY_PORT, VALKEY_PW
from redis import Redis
from redis.asyncio import Redis as AsyncRedis


def _pause() -> ModuleType:
    return importlib.import_module("curie_upgrade_pause")


@pytest.fixture
def markers() -> Iterator[tuple[Redis, tuple[str, str]]]:
    client = Redis(
        host=VALKEY_HOST,
        port=VALKEY_PORT,
        password=VALKEY_PW or None,
        decode_responses=True,
        socket_connect_timeout=2,
        retry=NO_RETRY,
    )
    client.ping()  # A required service failure is a setup error, never a skip.
    prefix = f"test:pause:{uuid.uuid4().hex}"
    keys = (f"{prefix}:upgrade:quiesce:acme-test", f"{prefix}:upgrade:quiesce")
    try:
        yield client, keys
    finally:
        client.delete(*keys)
        assert client.exists(*keys) == 0
        client.close()


def _renew(keys: tuple[str, ...], *, revision: int = 17, ttl_ms: int = 2_000) -> str:
    async def run() -> str:
        async with AsyncRedis(
            host=VALKEY_HOST,
            port=VALKEY_PORT,
            password=VALKEY_PW or None,
            decode_responses=True,
            socket_connect_timeout=2,
            retry=NO_RETRY,
        ) as client:
            return await _pause().renew_pause(client, keys, revision, ttl_ms)

    return asyncio.run(run())


@pytest.mark.parametrize("installation", ("", "acme-test"))
@pytest.mark.parametrize("bridge", (False, True))
def test_keys_keep_authority_first_and_bridge_only_scoped_installs(
    installation: str, bridge: bool
) -> None:
    pause = _pause()
    legacy = "test:worker:upgrade:quiesce"
    authoritative = f"{legacy}:{installation}" if installation else legacy
    assert pause.legacy_key("test:worker") == legacy
    assert pause.authoritative_key("test:worker", installation) == authoritative
    expected = (authoritative, legacy) if installation and bridge else (authoritative,)
    assert pause.marker_keys("test:worker", installation, bridge) == expected
    assert pause.PAUSE_LEASE_S == 300


def test_renew_extends_owned_authority_and_existing_legacy_without_rewriting(
    markers: tuple[Redis, tuple[str, str]],
) -> None:
    client, keys = markers
    raw = '{ "since": "2026-01-01T00:00:00Z", "revision": 17 }'
    client.set(keys[0], raw, px=500)
    client.set(keys[1], "1", px=500)
    assert _renew(keys) == "renewed"
    assert [client.get(key) for key in keys] == [raw, "1"]
    assert all(1_000 < client.pttl(key) <= 2_000 for key in keys)


def test_renew_accepts_integral_cjson_number_and_never_shortens_longer_pttl(
    markers: tuple[Redis, tuple[str, str]],
) -> None:
    client, keys = markers
    raw = json.dumps({"revision": 17.0, "since": "retained"})
    for key in keys:
        client.set(key, raw, px=30_000)
    assert _renew(keys) == "renewed"
    assert all(29_000 < client.pttl(key) <= 30_000 for key in keys)
    assert all(client.get(key) == raw for key in keys)


def test_renew_keeps_persistent_keys_persistent(
    markers: tuple[Redis, tuple[str, str]],
) -> None:
    client, keys = markers
    raw = json.dumps({"revision": 17})
    for key in keys:
        client.set(key, raw)
    assert _renew(keys) == "renewed"
    assert [client.pttl(key) for key in keys] == [-1, -1]
    assert all(client.get(key) == raw for key in keys)


def test_renew_never_creates_an_absent_bridge(
    markers: tuple[Redis, tuple[str, str]],
) -> None:
    client, keys = markers
    client.set(keys[0], json.dumps({"revision": 17}), px=500)
    assert _renew(keys) == "renewed"
    assert client.pttl(keys[0]) > 1_000
    assert client.exists(keys[1]) == 0


def test_absent_authority_leaves_existing_bridge_and_absent_authority_untouched(
    markers: tuple[Redis, tuple[str, str]],
) -> None:
    client, keys = markers
    client.set(keys[1], "1", px=30_000)
    assert _renew(keys) == "absent"
    assert client.exists(keys[0]) == 0
    assert client.get(keys[1]) == "1"
    assert 29_000 < client.pttl(keys[1]) <= 30_000


@pytest.mark.parametrize(
    "raw",
    (
        '{"revision":18}',
        '{"revision":16}',
        "not-json",
        "1",
        "null",
        "[]",
        "{}",
        '{"revision":null}',
        '{"revision":"17"}',
        '{"revision":true}',
        '{"revision":17.5}',
        '{"revision":-1}',
    ),
)
def test_foreign_or_unparseable_authority_leaves_all_values_and_ttls_untouched(
    markers: tuple[Redis, tuple[str, str]], raw: str
) -> None:
    client, keys = markers
    client.set(keys[0], raw, px=30_000)
    client.set(keys[1], json.dumps({"revision": 17}), px=30_000)
    before = [client.get(key) for key in keys]
    assert _renew(keys, ttl_ms=60_000) == "foreign"
    assert [client.get(key) for key in keys] == before
    assert all(29_000 < client.pttl(key) <= 30_000 for key in keys)


def test_standalone_renew_uses_one_existing_marker(
    markers: tuple[Redis, tuple[str, str]],
) -> None:
    client, keys = markers
    client.set(keys[1], '{"revision":0}', px=500)
    assert _renew((keys[1],), revision=0) == "renewed"
    assert client.pttl(keys[1]) > 1_000
    assert client.exists(keys[0]) == 0
