"""Fresh authorization reads neither consume nor populate the provider cache."""

import asyncio

import httpx
import pytest
from curie_channel_gateway.slack.groups import SlackUserGroupClient


@pytest.mark.parametrize("warm_cache", [False, True])
def test_fresh_members_preserves_cache_independence(warm_cache: bool) -> None:
    async def run() -> None:
        calls: list[httpx.Request] = []

        def members(request: httpx.Request) -> httpx.Response:
            calls.append(request)
            return httpx.Response(200, json={"ok": True, "users": [f"user-{len(calls)}"]})

        async with httpx.AsyncClient(transport=httpx.MockTransport(members)) as http:
            source = SlackUserGroupClient(http, token="xoxb-test", ttl_s=60)
            if warm_cache:
                assert (await source.members("S0EXAMPLE1")).users == {"user-1"}
            fresh = await source.fresh_members("S0EXAMPLE1")
            assert fresh.cache_age_s == 0
            assert fresh.users == ({"user-2"} if warm_cache else {"user-1"})
            cached = await source.members("S0EXAMPLE1")
            assert cached.users == ({"user-1"} if warm_cache else {"user-2"})
            assert len(calls) == 2

    asyncio.run(run())
