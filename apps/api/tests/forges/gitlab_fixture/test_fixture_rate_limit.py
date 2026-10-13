"""The GitLab transport under the fixture's real API throttle (#4399 AC 7, decision 7).

Select with ``-k rate_limit``. The test turns on the authenticated API throttle
at 5 requests per 60 seconds through the root application settings, drives the
code host until GitLab answers 429, and counts every request the adapter's
client sends. The settings are restored in ``finally`` whatever happens; the
root token is throttled like any other, so the restore waits out its own 429.
"""

from __future__ import annotations

import time
from typing import Any

import anyio
import httpx
import pytest
from curie_api.forges.errors import Unavailable

from .conftest import Fixture

pytestmark = pytest.mark.anyio

SETTINGS = "/application/settings"
THROTTLE = {
    "throttle_authenticated_api_enabled": True,
    "throttle_authenticated_api_requests_per_period": 5,
    "throttle_authenticated_api_period_in_seconds": 60,
}
# GitLab caches application settings per process for about a minute, so the
# throttle can take that long to apply.
APPLY_TIMEOUT_S = 180.0
RESTORE_TIMEOUT_S = 240.0


def _settings_call(fixture: Fixture, method: str, json: Any = None) -> dict[str, Any]:
    """A root settings call that waits out the throttle's own 429."""

    deadline = time.monotonic() + RESTORE_TIMEOUT_S
    while True:
        response = fixture.root.request(method, SETTINGS, json=json)
        if response.status_code == 200:
            found: dict[str, Any] = response.json()
            return found
        if response.status_code != 429 or time.monotonic() >= deadline:
            pytest.fail(f"{method} {SETTINGS}: {response.status_code}")
        time.sleep(int(response.headers.get("retry-after", "5")) + 1)


def _restore(fixture: Fixture, original: dict[str, Any]) -> None:
    restored = _settings_call(fixture, "PUT", original)
    assert {key: restored[key] for key in original} == original
    # Wait until the bot's token is not throttled any more, so a run right
    # after this one does not meet the throttle through a stale settings cache.
    deadline = time.monotonic() + RESTORE_TIMEOUT_S
    while True:
        answers = [fixture.bot.request("GET", "/version").status_code for _ in range(8)]
        if answers == [200] * 8:
            return
        if time.monotonic() >= deadline:
            pytest.fail(f"the throttle still answers after the restore: {answers}")
        time.sleep(5)


class Counter:
    """Every request the adapter's client sends, and the last answer's headers."""

    def __init__(self) -> None:
        self.sent = 0
        self.last: httpx.Response | None = None

    async def on_request(self, request: httpx.Request) -> None:
        self.sent += 1

    async def on_response(self, response: httpx.Response) -> None:
        self.last = response


async def _rate_limited(call: Any) -> bool:
    """True when ``call`` raised ``Unavailable("rate_limited")``, False when it answered."""

    try:
        await call
    except Unavailable as refused:
        assert str(refused) == "rate_limited"
        return True
    return False


async def test_rate_limit_holds_every_request_until_retry_after(fixture: Fixture) -> None:
    current = _settings_call(fixture, "GET")
    original = {key: current[key] for key in THROTTLE}
    assert original["throttle_authenticated_api_enabled"] is False
    repository = fixture.repository()
    counter = Counter()
    try:
        _settings_call(fixture, "PUT", THROTTLE)
        async with fixture.client() as client:
            client.event_hooks = {
                "request": [counter.on_request],
                "response": [counter.on_response],
            }

            # 1. Spend the quota. The fifth answer carries RateLimit-Remaining: 0
            # (or a 429 arrives), after which the adapter holds every call.
            first = fixture.code_host(client)
            deadline = time.monotonic() + APPLY_TIMEOUT_S
            while not await _rate_limited(first.resolve_repository(repository.project_id)):
                if time.monotonic() >= deadline:
                    pytest.fail(f"no throttle after {counter.sent} requests in {APPLY_TIMEOUT_S} s")
                await anyio.sleep(0.2)
            assert counter.last is not None
            assert counter.last.headers.get("ratelimit-remaining") == "0"
            sent = counter.sent
            for call in (
                first.resolve_repository(repository.project_id),
                first.branch_head(repository, "main"),
                first.user_can_write(repository, fixture.actor("developer")),
            ):
                assert await _rate_limited(call)
            assert counter.sent == sent

            # 2. A fresh adapter on the spent quota meets the 429 itself: one
            # request, Unavailable("rate_limited"), and nothing sent after it
            # until Retry-After passes.
            second = fixture.code_host(client)
            sent = counter.sent
            assert await _rate_limited(second.resolve_repository(repository.project_id))
            assert counter.sent == sent + 1
            assert counter.last is not None and counter.last.status_code == 429
            limited_at = time.monotonic()
            retry_after = int(counter.last.headers["retry-after"])
            assert 0 < retry_after <= 60
            sent = counter.sent
            assert await _rate_limited(second.branch_head(repository, "main"))
            await anyio.sleep(max(0.0, limited_at + retry_after - 2 - time.monotonic()))
            assert await _rate_limited(second.resolve_repository(repository.project_id))
            assert counter.sent == sent

            # 3. Once Retry-After passes, the next call is sent.
            # GitLab computes Retry-After from the period's wall clock boundary
            # while its counter window is offset per user, so the first try
            # after it can meet one more 429; that 429 holds again by its own
            # Retry-After, and the call after it succeeds.
            for _ in range(3):
                await anyio.sleep(max(0.0, limited_at + retry_after + 1 - time.monotonic()))
                sent = counter.sent
                answered = not await _rate_limited(second.resolve_repository(repository.project_id))
                if answered:
                    # The 429 answered this adapter's first request, its
                    # version read, so the answer takes that read and the call.
                    assert counter.sent == sent + 2
                    break
                assert counter.sent == sent + 1
                assert counter.last is not None and counter.last.status_code == 429
                limited_at = time.monotonic()
                retry_after = int(counter.last.headers["retry-after"])
                assert await _rate_limited(second.resolve_repository(repository.project_id))
                assert counter.sent == sent + 1
            else:
                pytest.fail("the adapter never got an answer after Retry-After")
            assert counter.last is not None and counter.last.status_code == 200
            resolved = await second.resolve_repository(repository.project_id)
            assert resolved == repository and resolved.path == repository.path
    finally:
        _restore(fixture, original)
