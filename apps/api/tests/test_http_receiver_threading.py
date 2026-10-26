"""Receiver-to-reply threading with real ingress and an owned synthetic Slack endpoint.

@spec HTTP-INVOCATION-EXECUTION-19.
"""

from __future__ import annotations

import asyncio
import importlib
import json
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock

import httpx
import redis
from aci_protocol import QueuedTurn
from aiohttp import web
from aiohttp.test_utils import TestServer
from channel_protocol import parse_scoped_conversation_id
from channel_protocol.reply import REPLY_WIRE_VERSION, ReplyUpdate
from curie_api.identity.http_transport import HttpIdentityClient
from curie_api.main import create_app
from curie_worker.kernel.routing import _target_for, _thread_key_for
from curie_worker.reply_sink import TargetRoute
from curie_worker.slack_sink import SlackReplyAdapter
from fastapi.testclient import TestClient
from slack_sdk.web import WebClient

from apps.api.tests import test_http_invocation_membership as membership
from apps.api.tests.test_http_invocation_membership import synthetic_provider as synthetic_provider

PARENT_TS = "1720000000.000100"
MESSAGE_TS = "1720000000.000200"
BOT = "U0EXAMPLEBOT"
SLACK_TOKEN = "xoxb-synthetic-threading-receiver"


def test_http_receiver_preserves_native_parent_through_queue_and_actual_slack_reply(
    clean_db: None,
    valkey: redis.Redis,
    runs_stream: str,
    synthetic_provider: SimpleNamespace,
) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-19: channel scoping belongs only to the worker key."""
    receiver = importlib.import_module("curie_dispatcher.http_receiver")
    scope = membership._scope(synthetic_provider.url)
    identity_client = HttpIdentityClient(tls_context=synthetic_provider.tls)
    requests: list[httpx.Request] = []
    owned_clients: list[httpx.AsyncClient] = []
    try:
        with TestClient(create_app(
            http_identity_runtime=membership._runtime(membership._runtime_api(), scope),
            http_identity_client=identity_client,
        )) as client:
            credential = membership._credential(client, scope)

            async def forward(request: httpx.Request) -> httpx.Response:
                """@spec HTTP-INVOCATION-EXECUTION-19: no fake admission or queue producer."""
                requests.append(request)
                assert request.method == "POST" and request.url.path == "/channels/turns"
                response = client.post(
                    request.url.path, content=request.content, headers=dict(request.headers),
                )
                assert response.status_code == 200, response.text
                return httpx.Response(
                    response.status_code, content=response.content, headers=response.headers,
                )

            def factory() -> httpx.AsyncClient:
                """@spec HTTP-INVOCATION-EXECUTION-19: own the receiver's HTTP client."""
                transport = httpx.AsyncClient(transport=httpx.MockTransport(forward))
                owned_clients.append(transport)
                return transport

            slack = WebClient(token=SLACK_TOKEN)
            slack.auth_test = MagicMock(return_value={
                "ok": True, "team_id": membership.ACCOUNT, "user_id": BOT,
                "bot_id": "B0EXAMPLE1", "is_enterprise_install": False,
            })
            runtime = receiver.build_http_receiver_runtime(
                receiver.HttpReceiverScopeV1(
                    schema_version=1, receiver_id=scope["receiver_id"], receiver_generation=1,
                    channel_identity_id=scope["identity"], channel_identity_name=scope["adapter"],
                    provider_installation_id=scope["installation"],
                    namespace=scope["namespace"].model_dump(),
                    installation_account=membership.ACCOUNT, bot_user_id=BOT,
                ),
                (receiver.HttpReceiverBindingV1(
                    schema_version=1, native_channel_id=scope["address"],
                    binding_id=scope["binding"], binding_generation=scope["generation"],
                    channel_credential=credential,
                ),),
                receiver_key=scope["receiver_key"], web_client=slack,
                api_base_url="https://synthetic-core.example.test", http_client_factory=factory,
            )
            event = {
                "type": "app_mention", "channel": scope["address"], "user": membership.SENDER,
                "user_team": membership.ACCOUNT, "text": f"<@{BOT}> Preserve this thread",
                "ts": MESSAGE_TS, "thread_ts": PARENT_TS,
            }
            body = {
                "type": "event_callback", "event_id": "EvSyntheticThreading",
                "team_id": membership.ACCOUNT, "is_ext_shared_channel": False, "event": event,
            }
            submitted = runtime.submit(
                body=body, event=event, slack_identity=scope["adapter"], bot_user_id=BOT,
            )
            assert submitted.status == "accepted"
            slack.auth_test.assert_called_once_with()
            assert len(requests) == len(owned_clients) == 1 and owned_clients[0].is_closed
            entries = valkey.xrange(runs_stream)
            assert len(entries) == 1 and entries[0][0] == submitted.stream_id
            assert set(entries[0][1]) == {"payload", "curie_identity_context_v1"}
            turn = QueuedTurn.model_validate_json(entries[0][1]["payload"])
            assert turn.reply_handle is not None and turn.reply_handle.placeholder is None
            assert turn.reply_handle.channel == scope["address"]
            assert turn.reply_handle.adapter == scope["adapter"]
            assert len(synthetic_provider.requests) == 1
            captured: list[dict[str, Any]] = []

            async def deliver() -> None:
                """@spec HTTP-INVOCATION-EXECUTION-19: actual SDK request to owned Slack stub."""
                async def capture(request: web.Request) -> web.Response:
                    """@spec HTTP-INVOCATION-EXECUTION-19: observe the outgoing Slack body."""
                    assert request.match_info["method"] == "chat.postMessage"
                    assert request.headers["Authorization"] == "Bearer " + SLACK_TOKEN
                    captured.append(await request.json())
                    return web.json_response({"ok": True, "ts": MESSAGE_TS})

                endpoint = web.Application()
                endpoint.router.add_post("/slack/api/{method}", capture)
                async with TestServer(endpoint) as server:
                    sink = SlackReplyAdapter(
                        "", identity_tokens={scope["adapter"]: SLACK_TOKEN},
                        base_url=str(server.make_url("/slack/api/")),
                    )
                    try:
                        ack = await sink.emit(
                            ReplyUpdate(
                                version=REPLY_WIRE_VERSION, event="reply.update",
                                target=_target_for(turn), text="Synthetic threaded answer",
                            ),
                            route=TargetRoute(adapter=turn.reply_handle.adapter),
                        )
                        assert ack.ref == MESSAGE_TS
                    finally:
                        # Close only SDK sessions created by this adapter in this event loop.
                        for sdk in sink._clients.values():
                            if sdk.session is not None and not sdk.session.closed:
                                await sdk.session.close()

            asyncio.run(deliver())
            assert len(captured) == 1
            assert captured[0].get("thread_ts") == PARENT_TS
            assert captured[0]["channel"] == scope["address"]
            assert turn.conversation_id == PARENT_TS
            assert json.loads(requests[0].content)["conversation_id"] == PARENT_TS
            assert parse_scoped_conversation_id(_thread_key_for(turn)) == (
                "slack", scope["adapter"], scope["address"], PARENT_TS,
            )
        assert identity_client.close_status().state == "closed"
    finally:
        keys = membership._keys(valkey, scope["binding"])
        if keys:
            valkey.delete(*keys)
