"""One real OpenRouter credit read. Skipped unless the key is in the environment.

No model request. The output of a pass is the two states and nothing else.
"""

from __future__ import annotations

import asyncio
import os
from decimal import Decimal

import httpx
import pytest
from curie_worker.factory_credit import DEFAULT_OPENROUTER_API, read_openrouter

pytestmark = pytest.mark.skipif(
    not os.environ.get("CURIE_LIVE_OPENROUTER_KEY"),
    reason="CURIE_LIVE_OPENROUTER_KEY is unset",
)


def test_live_openrouter_states() -> None:
    key = os.environ.get("CURIE_LIVE_OPENROUTER_KEY", "")

    async def read() -> tuple[str, str]:
        async with httpx.AsyncClient(timeout=httpx.Timeout(10.0, connect=5.0)) as client:
            default = await read_openrouter(
                key, minimum=Decimal("10"), api_url=DEFAULT_OPENROUTER_API, client=client
            )
            high = await read_openrouter(
                key,
                minimum=Decimal("1000000000"),
                api_url=DEFAULT_OPENROUTER_API,
                client=client,
            )
        return default.state, high.state

    try:
        default_state, high_state = asyncio.run(read())
    except (httpx.HTTPError, OSError, RuntimeError) as exc:
        message = str(exc).replace(key, "[redacted]") if key else str(exc)
        raise AssertionError(message) from None
    print(f"state_default={default_state} state_high={high_state}")
    assert default_state == "sufficient"
    assert high_state == "insufficient"
    assert key not in f"{default_state} {high_state}"
