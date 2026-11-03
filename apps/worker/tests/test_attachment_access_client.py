"""Worker-only attachment API calls never redeem provider bodies."""

import asyncio
import json

import httpx
import pytest
from curie_worker.attachment_access_client import (
    AttachmentAccessClient,
    AttachmentAccessUnavailable,
)


def test_metadata_and_context_use_internal_authority() -> None:
    async def run() -> None:
        requests = []

        def serve(request: httpx.Request) -> httpx.Response:
            requests.append(request)
            if request.url.path.endswith("/context"):
                return httpx.Response(
                    200, json={"token": "opaque", "expires_at": 1234, "lease_seconds": 90}
                )
            return httpx.Response(200, json={"appended": 1})

        async with httpx.AsyncClient(transport=httpx.MockTransport(serve)) as http:
            client = AttachmentAccessClient(
                api_base_url="http://api", worker_token="worker", client=http
            )
            await client.register(
                agent_id="agent",
                thread_key="thread",
                event_id="event",
                refs=[{"size_bytes": 400_000_000}],
            )
            minted = await client.context(
                agent_id="agent",
                thread_key="thread",
                event_id="event",
                owner="owner",
                deployment_id="deployment",
                session_id="session",
                sandbox_name="sandbox",
                route_generation=7,
                ttl_s=100,
            )
            assert minted.token == "opaque"
            assert len(requests) == 2
            assert requests[0].url.path == "/v1/internal/attachment-access/register"
            assert requests[0].headers["X-Curie-Worker-Token"] == "worker"
            assert json.loads(requests[0].content)["refs"][0]["size_bytes"] == 400_000_000

    asyncio.run(run())


def test_old_api_is_refused_without_eager_fallback() -> None:
    async def run() -> None:
        async with httpx.AsyncClient(
            transport=httpx.MockTransport(lambda _: httpx.Response(404))
        ) as http:
            client = AttachmentAccessClient(
                api_base_url="http://api", worker_token="worker", client=http
            )
            with pytest.raises(AttachmentAccessUnavailable, match="compatible API"):
                await client.register(
                    agent_id="agent", thread_key="thread", event_id="event", refs=[]
                )

    asyncio.run(run())
