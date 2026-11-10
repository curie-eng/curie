"""The deliberate progress ingress: ``POST /v1/turn-progress/{progress_id}`` (ADR 0130).

The runner's ``progress`` tool posts each command here with the per-turn
``turn.progress`` sandbox token the worker minted for the turn chain. The API
accepts only that token for that chain, validates the body as a
``ProgressCommand`` plus the runner's ``epoch`` and ``seq``, rate limits the
token, and appends the command to the chain's inbox stream for the worker's
pump.

The app under test is the router alone on real Valkey: the route needs no
database and no object store, and the full app's wiring is asserted separately
from its OpenAPI document.
"""

from __future__ import annotations

import contextlib
import json
import time
import uuid
from collections.abc import AsyncIterator, Iterator
from typing import Any

import pytest
import redis
import redis.asyncio as aioredis
from curie_api import channel_token
from curie_api.config import get_settings
from curie_api.main import create_app
from curie_api.routers import turn_progress as turn_progress_router
from curie_api.turn_progress import INBOX_MAXLEN, INBOX_TTL_S
from curie_internal import sandbox_token
from curie_test_support.valkey import VALKEY_HOST, VALKEY_PORT, VALKEY_PW, connect_or_skip
from fastapi import FastAPI
from fastapi.testclient import TestClient

_COMMAND = {
    "version": "1.0",
    "update_id": "u1",
    "state": "investigating",
    "summary": "Reading the failing test",
}


@pytest.fixture
def valkey() -> Iterator[redis.Redis]:
    client = connect_or_skip()
    yield client
    client.close()


@pytest.fixture
def prefix(monkeypatch: pytest.MonkeyPatch, valkey: redis.Redis) -> Iterator[str]:
    """A per-test worker key prefix, so no test reads another's inbox."""

    value = f"test:curie:worker:{uuid.uuid4().hex}"
    with monkeypatch.context() as patched:
        patched.setenv("KEY_PREFIX", value)
        get_settings.cache_clear()
        yield value
        keys = list(valkey.scan_iter(match=f"{value}*"))
        if keys:
            valkey.delete(*keys)
    # Outside the patch, so no later read keeps this test's prefix cached.
    get_settings.cache_clear()


@pytest.fixture
def api(prefix: str) -> Iterator[TestClient]:
    @contextlib.asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        app.state.valkey = aioredis.Redis(
            host=VALKEY_HOST, port=VALKEY_PORT, password=VALKEY_PW or None
        )
        try:
            yield
        finally:
            await app.state.valkey.aclose()

    app = FastAPI(lifespan=lifespan)
    app.include_router(turn_progress_router.router)
    with TestClient(app) as test_client:
        yield test_client


def _token(
    progress_id: str,
    *,
    generation: int = 1,
    scope: str = "turn.progress",
    exp: int | None = None,
) -> str:
    return sandbox_token.mint(
        get_settings().api_key,
        agent=f"{progress_id}:{generation}",
        scope=scope,
        exp=exp if exp is not None else int(time.time()) + 3600,
    )


def _post(
    client: TestClient,
    progress_id: str,
    token: str | None,
    /,
    *,
    seq: int = 1,
    generation: int = 1,
    **overrides: Any,
) -> Any:
    body: dict[str, Any] = {**_COMMAND, "generation": generation, "seq": seq, **overrides}
    headers = {"X-API-Key": token} if token is not None else {}
    return client.post(f"/v1/turn-progress/{progress_id}", json=body, headers=headers)


def _inbox(valkey: redis.Redis, prefix: str, progress_id: str) -> list[tuple[str, dict[str, str]]]:
    return valkey.xrange(f"{prefix}:progress:inbox:{progress_id}")  # type: ignore[return-value]


def _activate(valkey: redis.Redis, prefix: str, progress_id: str, generation: int = 1) -> None:
    valkey.hset(
        f"{prefix}:progress:{progress_id}",
        mapping={
            "active_generation": generation,
            "active_until_ms": int(time.time() * 1000) + 60_000,
        },
    )


def test_an_update_is_appended_to_its_chains_inbox(
    api: TestClient, valkey: redis.Redis, prefix: str
) -> None:
    progress_id = str(uuid.uuid4())
    _activate(valkey, prefix, progress_id)

    response = _post(api, progress_id, _token(progress_id), milestone="evidence")

    assert response.status_code == 202, response.text
    assert response.json()["accepted"] is True
    entries = _inbox(valkey, prefix, progress_id)
    assert len(entries) == 1
    entry_id, fields = entries[0]
    assert response.json()["id"] == entry_id
    assert set(fields) == {"command", "generation", "seq"}
    assert json.loads(fields["command"]) == {**_COMMAND, "milestone": "evidence"}
    assert (fields["generation"], fields["seq"]) == ("1", "1")
    assert valkey.sismember(f"{prefix}:progress:inbox:pending", progress_id)
    ttl = valkey.ttl(f"{prefix}:progress:inbox:{progress_id}")
    assert 0 < int(ttl) <= INBOX_TTL_S  # type: ignore[arg-type]


def test_the_platform_key_is_refused(api: TestClient, valkey: redis.Redis, prefix: str) -> None:
    progress_id = str(uuid.uuid4())

    response = _post(api, progress_id, get_settings().api_key)

    assert response.status_code == 401
    assert _inbox(valkey, prefix, progress_id) == []


def test_a_channel_token_is_refused_before_command_validation(
    api: TestClient, valkey: redis.Redis, prefix: str
) -> None:
    """@spec ADR-0130 d1: sibling channel authority never reaches this ingress."""

    progress_id = str(uuid.uuid4())
    token = channel_token.mint(
        get_settings().api_key,
        channel_id=str(uuid.uuid4()),
        generation=1,
        scope=channel_token.CHANNEL_ENQUEUE_SCOPE,
        exp=int(time.time()) + 3600,
    )

    response = api.post(
        f"/v1/turn-progress/{progress_id}",
        json={},
        headers={"X-API-Key": token},
    )

    assert response.status_code == 401, response.text
    assert _inbox(valkey, prefix, progress_id) == []


def test_another_chains_token_is_refused(api: TestClient, valkey: redis.Redis, prefix: str) -> None:
    mine, theirs = str(uuid.uuid4()), str(uuid.uuid4())

    response = _post(api, theirs, _token(mine))

    assert response.status_code == 401
    assert _inbox(valkey, prefix, theirs) == []
    assert _inbox(valkey, prefix, mine) == []


def test_an_expired_token_is_refused(api: TestClient, valkey: redis.Redis, prefix: str) -> None:
    progress_id = str(uuid.uuid4())

    response = _post(api, progress_id, _token(progress_id, exp=int(time.time()) - 1))

    assert response.status_code == 401
    assert _inbox(valkey, prefix, progress_id) == []


def test_a_failed_close_loses_ingress_authority_within_the_short_lease(
    api: TestClient, valkey: redis.Redis, prefix: str
) -> None:
    """@spec ADR-0130 d1: a leaked turn token fails closed at its durable deadline."""

    progress_id = str(uuid.uuid4())
    _activate(valkey, prefix, progress_id)
    valkey.hset(
        f"{prefix}:progress:{progress_id}",
        "active_until_ms",
        int(time.time() * 1000) + 50,
    )
    # Model an end-turn clear that never reached Valkey. The still-active
    # generation must stop authorizing this token without another write.
    time.sleep(0.075)

    response = _post(api, progress_id, _token(progress_id))

    assert response.status_code == 401, response.text
    assert _inbox(valkey, prefix, progress_id) == []


@pytest.mark.parametrize("scope", ["work_item.progress", "state", "state.app"])
def test_a_token_of_another_scope_is_refused(
    api: TestClient, valkey: redis.Redis, prefix: str, scope: str
) -> None:
    progress_id = str(uuid.uuid4())

    response = _post(api, progress_id, _token(progress_id, scope=scope))

    assert response.status_code == 401
    assert _inbox(valkey, prefix, progress_id) == []


def test_a_missing_token_is_refused(api: TestClient, valkey: redis.Redis, prefix: str) -> None:
    progress_id = str(uuid.uuid4())

    response = _post(api, progress_id, None)

    assert response.status_code == 401
    assert _inbox(valkey, prefix, progress_id) == []


@pytest.mark.parametrize(
    "overrides",
    [
        pytest.param({"channel": "C0EXAMPLE1"}, id="routing-field"),
        pytest.param({"delivery_id": str(uuid.uuid4())}, id="delivery-id"),
        pytest.param({"progress_id": str(uuid.uuid4())}, id="progress-id"),
        pytest.param({"version": "1.1"}, id="unknown-version"),
        pytest.param({"state": "done"}, id="unknown-state"),
        pytest.param({"summary": "two\nlines"}, id="multi-line-summary"),
        pytest.param({"generation": 0}, id="zero-generation"),
        pytest.param({"seq": 0}, id="zero-seq"),
        pytest.param({"seq": True}, id="bool-seq"),
        pytest.param({"generation": "1"}, id="string-generation"),
        pytest.param({"seq": 2**53}, id="seq-past-a-double"),
    ],
)
def test_a_body_that_is_not_a_command_plus_its_position_is_refused(
    api: TestClient, valkey: redis.Redis, prefix: str, overrides: dict[str, Any]
) -> None:
    progress_id = str(uuid.uuid4())

    response = _post(api, progress_id, _token(progress_id), **overrides)

    assert response.status_code == 422, response.text
    assert _inbox(valkey, prefix, progress_id) == []


def test_a_body_without_its_position_is_refused(
    api: TestClient, valkey: redis.Redis, prefix: str
) -> None:
    progress_id = str(uuid.uuid4())
    token = _token(progress_id)

    for missing in ("generation", "seq"):
        body = {**_COMMAND, "generation": 1, "seq": 1}
        del body[missing]
        response = api.post(
            f"/v1/turn-progress/{progress_id}", json=body, headers={"X-API-Key": token}
        )
        assert response.status_code == 422, (missing, response.text)
    assert _inbox(valkey, prefix, progress_id) == []


def test_a_token_is_limited_to_a_burst_of_five_then_one_a_second(
    api: TestClient, valkey: redis.Redis, prefix: str
) -> None:
    progress_id = str(uuid.uuid4())
    _activate(valkey, prefix, progress_id)
    token = _token(progress_id)

    statuses = [_post(api, progress_id, token, seq=seq).status_code for seq in range(1, 7)]

    assert statuses == [202, 202, 202, 202, 202, 429]
    assert len(_inbox(valkey, prefix, progress_id)) == 5
    # Another generation of the same chain holds its own token and bucket.
    _activate(valkey, prefix, progress_id, 2)
    other = _token(progress_id, generation=2, exp=int(time.time()) + 7200)
    assert _post(api, progress_id, other, generation=2, seq=7).status_code == 202
    # The bucket refills at one a second.
    time.sleep(1.1)
    # The previous generation is fenced even after its bucket refills.
    assert _post(api, progress_id, token, seq=8).status_code == 401
    assert len(_inbox(valkey, prefix, progress_id)) == 6


def test_the_inbox_keeps_only_its_newest_entries(
    api: TestClient, valkey: redis.Redis, prefix: str
) -> None:
    progress_id = str(uuid.uuid4())
    _activate(valkey, prefix, progress_id)
    key = f"{prefix}:progress:inbox:{progress_id}"
    for index in range(INBOX_MAXLEN):
        valkey.xadd(key, {"command": "{}", "generation": "1", "seq": str(index + 1)})

    response = _post(api, progress_id, _token(progress_id), seq=INBOX_MAXLEN + 1)

    assert response.status_code == 202
    assert valkey.xlen(key) == INBOX_MAXLEN
    newest = _inbox(valkey, prefix, progress_id)[-1][1]
    assert newest["seq"] == str(INBOX_MAXLEN + 1)


def test_a_stale_or_closed_generation_cannot_append(
    api: TestClient, valkey: redis.Redis, prefix: str
) -> None:
    progress_id = str(uuid.uuid4())
    _activate(valkey, prefix, progress_id, 2)

    stale = _post(api, progress_id, _token(progress_id, generation=1), generation=1)
    assert stale.status_code == 401
    valkey.hset(f"{prefix}:progress:{progress_id}", "active_generation", "0")
    closed = _post(api, progress_id, _token(progress_id, generation=2), generation=2)
    assert closed.status_code == 401
    assert _inbox(valkey, prefix, progress_id) == []
    assert not valkey.sismember(f"{prefix}:progress:inbox:pending", progress_id)


def test_the_full_app_serves_the_route() -> None:
    paths = create_app().openapi()["paths"]
    assert "post" in paths["/v1/turn-progress/{progress_id}"]
