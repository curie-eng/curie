"""Approval create failures retain the underlying transport exception class."""

from __future__ import annotations

import asyncio

import httpx
import pytest
from curie_worker import api_retry
from curie_worker.approvals import ApprovalBackendError, ApprovalClient, ApprovalRequest


@pytest.mark.parametrize("message", ["", "read timed out"])
def test_approval_create_timeout_retains_the_exception_type(monkeypatch, message: str) -> None:
    async def go() -> None:
        now = 0.0
        sleeps: list[float] = []
        seen: list[httpx.Request] = []

        async def sleep(delay: float) -> None:
            nonlocal now
            sleeps.append(delay)
            now += delay

        monkeypatch.setattr(api_retry, "_clock", lambda: now)
        monkeypatch.setattr(api_retry, "_sleep", sleep)

        def handler(request: httpx.Request) -> httpx.Response:
            seen.append(request)
            raise httpx.ReadTimeout(message, request=request)

        request = ApprovalRequest(
            conversation_id="thread-example",
            author="U0REQUEST1",
            summary="Approve the bounded action",
            reply_kind="slack",
            reply_channel="C0EXAMPLE1",
            reply_placeholder="1700000000.000001",
            dedupe_key="event-example",
        )
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
            client = ApprovalClient(
                api_base_url="http://api.example", api_key="k", client=http, read_timeout_s=1.0
            )
            with pytest.raises(ApprovalBackendError, match="ReadTimeout") as caught:
                await client.create(request, budget_s=3)

        assert isinstance(caught.value.__cause__, httpx.ReadTimeout)
        assert str(caught.value.__cause__) == message
        assert str(caught.value) == f"approval create failed: ReadTimeout: {message}"
        assert sleeps == [0.5, 1.0, 1.5]
        assert len(seen) == 4
        assert all(attempt.content == seen[0].content for attempt in seen)

    asyncio.run(go())
