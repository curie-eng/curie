"""Bounded replay of idempotent worker API writes."""

from __future__ import annotations

import asyncio
import time
from contextlib import nullcontext
from typing import Any

import httpx

DEFAULT_BUDGET_S = 120.0
_RETRY_STATUSES = frozenset({500, 502, 503, 504})
_BACKOFF_S = (0.5, 1.0, 2.0, 4.0, 8.0, 10.0)


def _clock() -> float:
    return time.monotonic()


async def _sleep(delay: float) -> None:
    await asyncio.sleep(delay)


async def post_with_retry(
    client: httpx.AsyncClient,
    url: str,
    *,
    budget_s: float = DEFAULT_BUDGET_S,
    json: dict[str, Any] | None = None,
    content: str | None = None,
    headers: dict[str, str] | None = None,
    follow_redirects: bool = False,
) -> httpx.Response:
    """Return the first decision response, or the last exhausted server fault.

    Payload and authority stay identical on every replay. Even a zero budget
    permits the caller's first attempt, but never permits a retry sleep.
    """

    deadline = _clock() + max(0.0, budget_s)
    attempt = 0
    while True:
        remaining = max(0.0, deadline - _clock())
        transport_error: httpx.TransportError | None = None
        response: httpx.Response | None = None
        timeout = httpx.Timeout(
            **{
                phase: remaining if ceiling is None else min(remaining, ceiling)
                for phase, ceiling in client.timeout.as_dict().items()
            }
        )
        try:
            # A phase timeout alone can spend the budget once per phase. Bound
            # the whole attempt too, while allowing the mandatory zero-budget
            # first request to reach the transport with zero phase timeouts.
            async with asyncio.timeout(remaining) if remaining > 0 else nullcontext():
                response = await client.post(
                    url,
                    json=json,
                    content=content,
                    headers=headers,
                    follow_redirects=follow_redirects,
                    timeout=timeout,
                )
        except httpx.TransportError as exc:
            transport_error = exc
        except TimeoutError:
            transport_error = httpx.TimeoutException("API write attempt exhausted its budget")
        else:
            if response.status_code not in _RETRY_STATUSES:
                return response
        remaining = deadline - _clock()
        if remaining <= 0:
            if transport_error is not None:
                raise transport_error
            assert response is not None
            return response
        await _sleep(min(_BACKOFF_S[min(attempt, len(_BACKOFF_S) - 1)], remaining))
        attempt += 1
