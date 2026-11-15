"""Live non-generating OpenRouter allowance read through the worker probe (#5426, AC 3).

Runs only with ``CURIE_RETRY_PROVIDER_READ_PROBE=1``. It then requires
``CURIE_LIVE_OPENROUTER_KEY`` and fails setup without it. One GET /api/v1/key
(https://openrouter.ai/docs/api/api-reference/api-keys/get-current-api-key)
goes through the production ``read_openrouter_allowance``; no model is called.
The printed line names the outcome, the type of each allowance field and the
scope provider only: never an amount, the key or the body.
"""

from __future__ import annotations

import asyncio
import os

import httpx
import pytest

pytestmark = pytest.mark.skipif(
    os.environ.get("CURIE_RETRY_PROVIDER_READ_PROBE") != "1",
    reason="set CURIE_RETRY_PROVIDER_READ_PROBE=1 to read the live OpenRouter key",
)

_KEY_ENV = "CURIE_LIVE_OPENROUTER_KEY"


@pytest.fixture
def live_key() -> str:
    key = os.environ.get(_KEY_ENV, "").strip()
    if not key:
        raise RuntimeError(f"{_KEY_ENV} is required when CURIE_RETRY_PROVIDER_READ_PROBE=1")
    return key


def _kind(value: object) -> str:
    return "null" if value is None else "numeric"


def test_live_openrouter_key_read_is_authenticated(live_key: str) -> None:
    from curie_internal.provider_scope import AllowanceReading, provider_scope
    from curie_worker import factory_credit, provider_probe

    async def read() -> AllowanceReading:
        async with httpx.AsyncClient(
            timeout=httpx.Timeout(10.0, connect=5.0), follow_redirects=False
        ) as client:
            return await provider_probe.read_openrouter_allowance(
                live_key, api_url=factory_credit.DEFAULT_OPENROUTER_API, client=client
            )

    try:
        reading = asyncio.run(read())
        scope = provider_scope({"CURIE_CREDENTIALS": live_key})
        assert reading.outcome == "authenticated", f"outcome={reading.outcome}"
        assert scope is not None and scope.provider == "openrouter", "scope is not openrouter"
    except (AssertionError, OSError, RuntimeError, ValueError, httpx.HTTPError) as exc:
        message = str(exc).replace(live_key, "<redacted>")
        raise AssertionError(f"{type(exc).__name__}: {message}") from None
    print(
        f"outcome={reading.outcome} limit={_kind(reading.limit)} "
        f"limit_remaining={_kind(reading.limit_remaining)} scope_provider={scope.provider}"
    )
