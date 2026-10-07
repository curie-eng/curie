"""The deterministic worker API retry window never retries a turn or a 4xx."""

from __future__ import annotations

import asyncio

import httpx
import pytest


@pytest.mark.parametrize("status", [500, 502, 503, 504])
def test_retryable_status_recovers_without_changing_the_write(status, monkeypatch) -> None:
    from curie_worker import api_retry
    from curie_worker.api_retry import post_with_retry

    async def go() -> None:
        now = 0.0
        sleeps: list[float] = []
        requests: list[httpx.Request] = []

        async def sleep(delay: float) -> None:
            nonlocal now
            sleeps.append(delay)
            now += delay

        def handler(request: httpx.Request) -> httpx.Response:
            requests.append(request)
            return httpx.Response(status if len(requests) == 1 else 201, json={"id": "record-1"})

        monkeypatch.setattr(api_retry, "_clock", lambda: now)
        monkeypatch.setattr(api_retry, "_sleep", sleep)
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            response = await post_with_retry(
                client,
                "http://api.example/write",
                json={"dedupe_key": "same-write"},
            )
        assert response.status_code == 201
        assert len(requests) == 2
        assert requests[0].content == requests[1].content
        assert sleeps == [0.5]

    asyncio.run(go())


def test_persistent_503_returns_last_response_at_the_clamped_window(monkeypatch) -> None:
    from curie_worker import api_retry
    from curie_worker.api_retry import post_with_retry

    async def go() -> None:
        now = 0.0
        sleeps: list[float] = []
        attempts = 0

        async def sleep(delay: float) -> None:
            nonlocal now
            sleeps.append(delay)
            now += delay

        def handler(_request: httpx.Request) -> httpx.Response:
            nonlocal attempts
            attempts += 1
            return httpx.Response(503, text="unavailable")

        monkeypatch.setattr(api_retry, "_clock", lambda: now)
        monkeypatch.setattr(api_retry, "_sleep", sleep)
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            response = await post_with_retry(
                client,
                "http://api.example/write",
                json={},
                budget_s=3,
            )
        assert response.status_code == 503
        assert response.text == "unavailable"
        assert sleeps == [0.5, 1, 1.5]
        assert now == 3
        assert attempts == 4

    asyncio.run(go())


def test_cancellation_interrupts_backoff_without_another_post(monkeypatch) -> None:
    from curie_worker import api_retry
    from curie_worker.api_retry import post_with_retry

    async def go() -> None:
        attempts = 0

        async def cancel(_delay: float) -> None:
            raise asyncio.CancelledError

        def handler(request: httpx.Request) -> httpx.Response:
            nonlocal attempts
            attempts += 1
            raise httpx.ConnectError("API restarting", request=request)

        monkeypatch.setattr(api_retry, "_clock", lambda: 0)
        monkeypatch.setattr(api_retry, "_sleep", cancel)
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            with pytest.raises(asyncio.CancelledError):
                await post_with_retry(
                    client, "http://api.example/write", json={}
                )
        assert attempts == 1

    asyncio.run(go())
