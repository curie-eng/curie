"""Account display reads keep Slack credentials in the gateway (#5718)."""

import asyncio

import httpx
import pytest
from aiohttp.test_utils import TestServer
from curie_channel_gateway.read_models import AccountProfile
from curie_channel_gateway.read_transport import (
    GatewayClient,
    GatewayError,
    GatewayProviders,
    gateway_application,
)
from curie_channel_gateway.request_replay import RequestReplayStore
from curie_channel_gateway.slack.account_profiles import SlackAccountProfiles
from pydantic import TypeAdapter


@pytest.mark.parametrize(
    "failure", [None, "user", "team", "user_mismatch", "team_mismatch", "user_timeout", "team_http"]
)
def test_connect_plan_profile_reads_names_with_receiving_identity(failure):
    async def scenario():
        calls = []

        def slack(request):
            # Shapes: https://docs.slack.dev/reference/methods/users.info/
            # and https://docs.slack.dev/reference/methods/team.info/.
            calls.append(request)
            if request.url.path.endswith("users.info"):
                if failure == "user_timeout":
                    raise httpx.ReadTimeout("provider unavailable", request=request)
                return httpx.Response(
                    200,
                    json={
                        "ok": failure != "user",
                        "user": {
                            "id": "U0EXAMPLE2" if failure == "user_mismatch" else "U0EXAMPLE1",
                            "name": "alice",
                            "profile": {"display_name": "Alice"},
                        },
                    },
                )
            return httpx.Response(
                503 if failure == "team_http" else 200,
                json={
                    "ok": failure != "team",
                    "team": {
                        "id": "T0EXAMPLE2" if failure == "team_mismatch" else "T0EXAMPLE1",
                        "name": "Example",
                    },
                },
            )

        async with httpx.AsyncClient(transport=httpx.MockTransport(slack)) as provider:
            app = gateway_application(
                tokens={
                    "api": "api-credential",
                    "worker": "worker-credential",
                    "identity": "identity-credential",
                },
                replay=RequestReplayStore(":memory:"),
                allow_insecure_http=True,
                providers=GatewayProviders(
                    profiles=SlackAccountProfiles(
                        provider,
                        {"acme-bot": "xoxb-example"},
                        base_url="https://slack.example.com/api/",
                    )
                ),
            )
            async with TestServer(app) as server, httpx.AsyncClient() as http:
                client = GatewayClient(
                    http,
                    str(server.make_url("")),
                    "api-credential",
                    allow_insecure_http=True,
                    identities=("acme-bot",),
                )

                async def read():
                    return await client.call(
                        "acme-bot",
                        "account_profile",
                        {"user_id": "U0EXAMPLE1", "workspace": "T0EXAMPLE1"},
                        TypeAdapter(AccountProfile),
                    )

                user_ok = failure not in {"user", "user_mismatch", "user_timeout"}
                team_ok = failure not in {"team", "team_mismatch", "team_http"}
                assert await read() == AccountProfile(
                    "Alice" if user_ok else None,
                    "alice" if user_ok else None,
                    "Example" if team_ok else None,
                )
                assert len(calls) == 2
                assert all(r.headers["authorization"] == "Bearer xoxb-example" for r in calls)
                assert calls[0].url.params["user"] == "U0EXAMPLE1"
                assert calls[1].url.params["team"] == "T0EXAMPLE1"
                for token in ("worker-credential", "identity-credential"):
                    denied = GatewayClient(
                        http,
                        str(server.make_url("")),
                        token,
                        allow_insecure_http=True,
                        identities=("acme-bot",),
                    )
                    with pytest.raises(GatewayError):
                        await denied.call(
                            "acme-bot",
                            "account_profile",
                            {"user_id": "U0EXAMPLE1", "workspace": "T0EXAMPLE1"},
                            TypeAdapter(AccountProfile),
                        )
                assert len(calls) == 2

    asyncio.run(scenario())
