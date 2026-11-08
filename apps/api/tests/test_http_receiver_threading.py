"""Receiver-to-reply threading with real ingress and an owned synthetic Slack endpoint.

@spec HTTP-INVOCATION-EXECUTION-19.
"""

from __future__ import annotations

import asyncio
import importlib
import json
import socket
from collections.abc import Iterator
from contextlib import contextmanager
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock

import httpx
import pytest
import redis
import uvicorn
from aci_protocol import QueuedTurn
from aiohttp import web
from aiohttp.test_utils import TestServer
from channel_protocol import parse_scoped_conversation_id
from channel_protocol.reply import REPLY_WIRE_VERSION, ReplyUpdate
from curie_api.identity.http_transport import HttpIdentityClient
from curie_api.main import create_app
from curie_channel_gateway.slack.slack_sink import SlackReplyAdapter
from curie_worker.kernel.routing import _target_for, _thread_key_for
from curie_worker.reply_sink import TargetRoute
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
    receiver = importlib.import_module("curie_channel_gateway.slack.http_receiver")
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
                "team_id": membership.ACCOUNT,
                "authorizations": [{"team_id": membership.ACCOUNT, "user_id": BOT, "is_bot": True}],
                "is_ext_shared_channel": False, "event": event,
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


@contextmanager
def _live_api(client: TestClient) -> Iterator[str]:
    """Serve the actual API on its existing lifespan loop, with an owned TCP listener."""
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    listener.setblocking(False)
    server = uvicorn.Server(uvicorn.Config(
        client.app, lifespan="off", log_config=None, access_log=False,
    ))

    async def serve() -> None:
        await server.serve(sockets=[listener])

    async def ready() -> None:
        async with asyncio.timeout(5):
            while not server.started:
                await asyncio.sleep(0)

    assert client.portal is not None
    future = client.portal.start_task_soon(serve)
    try:
        client.portal.call(ready)
        yield f"http://127.0.0.1:{listener.getsockname()[1]}"
    finally:
        server.should_exit = True
        try:
            future.result(timeout=5)
        finally:
            listener.close()
        assert future.done() and listener.fileno() == -1


@pytest.mark.parametrize("command", ["act", "ping"])
def test_declared_http_driver_reaches_real_group_admission(
    clean_db: None, valkey: redis.Redis, runs_stream: str,
    synthetic_provider: SimpleNamespace, command: str,
) -> None:
    """A configured driver user passes receiver proofs and is refused by directory group."""
    from curie_channel_gateway.slack.config import DispatcherConfig
    from curie_channel_gateway.slack.http_receiver import (
        HttpReceiverBindingV1,
        HttpReceiverScopeV1,
        build_http_receiver_runtime,
    )

    scope = membership._scope(synthetic_provider.url)
    identity_client = HttpIdentityClient(tls_context=synthetic_provider.tls)
    responses: list[tuple[str, int]] = []
    requests: list[httpx.Request] = []
    try:
        with TestClient(create_app(
            http_identity_runtime=membership._runtime(membership._runtime_api(), scope),
            http_identity_client=identity_client,
        )) as client, _live_api(client) as api_url:
            credential = membership._credential(client, scope)

            async def observe(result: httpx.Response) -> None:
                requests.append(result.request)
                responses.append((result.request.url.path, result.status_code))
                if result.status_code == 403:
                    await result.aread()
                    assert result.json() == {"detail": "caller_not_allowed"}

            slack = WebClient(token=SLACK_TOKEN)
            slack.auth_test = MagicMock(return_value={
                "ok": True, "team_id": membership.ACCOUNT, "user_id": BOT,
                "bot_id": "B0EXAMPLE1", "is_enterprise_install": False,
            })
            slack.chat_postMessage = MagicMock(return_value={"ok": True, "ts": MESSAGE_TS})
            config = DispatcherConfig(
                api_key="synthetic-test-installation-key",
                approval_chat_attester_secret="synthetic-test-installation-attester",
                test_installation_enabled=True,
                test_installation_drivers=[{
                    "channel_id": scope["address"], "bot_id": "B0EXAMPLEDRIVER",
                    "bot_user_id": membership.SENDER,
                }],
                dedupe_prefix="ut10:" + scope["binding"] + ":",
            )
            runtime = build_http_receiver_runtime(
                HttpReceiverScopeV1(
                    schema_version=1, receiver_id=scope["receiver_id"], receiver_generation=1,
                    channel_identity_id=scope["identity"], channel_identity_name=scope["adapter"],
                    provider_installation_id=scope["installation"],
                    namespace=scope["namespace"].model_dump(),
                    installation_account=membership.ACCOUNT, bot_user_id=BOT,
                ),
                (HttpReceiverBindingV1(
                    schema_version=1, native_channel_id=scope["address"],
                    binding_id=scope["binding"], binding_generation=scope["generation"],
                    channel_credential=credential,
                ),),
                receiver_key=scope["receiver_key"], web_client=slack,
                api_base_url=api_url,
                http_client_factory=lambda: httpx.AsyncClient(
                    event_hooks={"response": [observe]},
                ),
                config=config, redis_client=valkey,
            )
            # Slack bot mentions carry bot_id and may name another user:
            # https://docs.slack.dev/reference/events/app_mention/
            event = {
                "type": "app_mention", "channel": scope["address"], "user": "U0EXAMPLEOTHER",
                "bot_id": "B0EXAMPLEDRIVER", "user_team": membership.ACCOUNT,
                "text": f"<@{BOT}> [test action] {command}", "ts": MESSAGE_TS,
            }
            body = {
                "type": "event_callback", "event_id": "EvDriverGroupRefused",
                "team_id": membership.ACCOUNT,
                "authorizations": [{"team_id": membership.ACCOUNT, "user_id": BOT, "is_bot": True}],
                "is_ext_shared_channel": False, "event": event,
            }
            synthetic_provider.member = False
            result = runtime.submit(
                body=body, event=event, slack_identity=scope["adapter"], bot_user_id=BOT,
            )
            assert result.status == "refused"
            assert responses == [("/channels/turns/admission", 403)]
            observed_sender = synthetic_provider.requests[0]["scope"]["sender"]["native_id"]
            assert observed_sender == membership.SENDER
            assert valkey.xlen(runs_stream) == 0
            assert not list(valkey.scan_iter(match=config.dedupe_prefix + "*"))
            slack.chat_postMessage.assert_not_called()
            synthetic_provider.member = True
            body["event_id"] = "EvDriverGroupAllowed"
            result = runtime.submit(
                body=body, event=event, slack_identity=scope["adapter"], bot_user_id=BOT,
            )
            assert result.status == "accepted"
            entries = valkey.xrange(runs_stream)
            if command == "ping":
                assert not entries
                slack.chat_postMessage.assert_called_once_with(
                    channel=scope["address"], thread_ts=MESSAGE_TS,
                    text=f"This installation accepts test actions from <@{membership.SENDER}>.",
                )
            else:
                assert len(entries) == 1
                turn = QueuedTurn.model_validate_json(entries[0][1]["payload"])
                assert turn.author == membership.SENDER
                assert responses[-2:] == [
                    ("/channels/turns/admission", 204), ("/channels/turns", 200),
                ]
            count = len(synthetic_provider.requests)
            original = requests[0]
            for tamper in ("body", "proof", "credential", "missing"):
                raw, headers = original.content, dict(original.headers)
                headers.pop("content-length", None)
                if tamper == "body":
                    changed = json.loads(raw)
                    changed["text"] = "tampered signed text"
                    raw = json.dumps(changed).encode()
                elif tamper == "proof":
                    headers["x-curie-receiver-proof"] = "invalid"
                elif tamper == "credential":
                    headers["x-api-key"] = "chn.invalid"
                else:
                    headers.pop("x-curie-receiver-proof")
                refused = client.post("/channels/turns/admission", content=raw, headers=headers)
                assert refused.status_code == 401, refused.text
                assert len(synthetic_provider.requests) == count
        assert identity_client.close_status().state == "closed"
    finally:
        keys = membership._keys(valkey, scope["binding"])
        keys.update(valkey.scan_iter(match="ut10:" + scope["binding"] + ":*"))
        if keys:
            valkey.delete(*keys)
