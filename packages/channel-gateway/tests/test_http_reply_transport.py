"""Drive worker routing through authenticated HTTP to the real Slack renderer."""

import asyncio
import json
from pathlib import Path
from typing import Any

import aiohttp
import pytest
from aiohttp import web
from aiohttp.test_utils import TestServer
from channel_protocol import ReplyEvent, ReplyUpdate
from curie_channel_gateway.http_reply import HttpReplyTransport, reply_application
from curie_channel_gateway.ports import ReplyCapabilities
from curie_channel_gateway.reply_transport import (
    MissingAdapterCredentialError,
    RejectedAdapterResponseError,
)
from curie_channel_gateway.slack.slack_sink import SlackReplyAdapter
from curie_worker.reply_sink import ReplySinkRouter, TargetRoute
from pydantic import TypeAdapter

_CORPUS = json.loads(
    (Path(__file__).parents[2] / "channel-protocol/schema/reply-wire.corpus.json").read_text()
)
_EVENTS = TypeAdapter(ReplyEvent)
_BODIES = [
    case["body"]
    for case in _CORPUS["v1_0"] + _CORPUS["v1_1"]
    if case["body"]["target"]["kind"] == "slack"
]
_BODIES.append(
    ReplyUpdate(
        version="1.2",
        event="reply.update",
        target=_BODIES[0]["target"],
        text='```curie-reply\n{"text":"literal"}\n```\n  ',
        text_mode="verbatim",
        delivery_id="00000000-0000-4000-8000-000000000001",
    ).model_dump(mode="json")
)


@pytest.mark.parametrize("body", _BODIES)
def test_gateway_reply_wire_and_provider_outcomes(body: dict[str, Any]) -> None:
    async def run() -> None:
        calls: list[dict[str, Any]] = []

        async def provider(request: web.Request) -> web.Response:
            calls.append(await request.json())
            return web.json_response({"ok": True, "ts": "1700.000200"})

        provider_app = web.Application()
        provider_app.router.add_post("/{method}", provider)
        slack_server = TestServer(provider_app)
        await slack_server.start_server()
        slack = SlackReplyAdapter("xoxb-test", base_url=str(slack_server.make_url("/")))
        provider_route = TargetRoute()
        received: list[bytes] = []

        @web.middleware
        async def capture(request: web.Request, handler: Any) -> web.StreamResponse:
            wire = await request.read()
            response = await handler(request)
            if response.status == 200:
                received.append(wire)
            return response

        application = reply_application(slack, secret="test-egress", route=provider_route)
        application.middlewares.append(capture)
        server = TestServer(application)
        transport: HttpReplyTransport[TargetRoute] | None = None
        try:
            await server.start_server()
            route = TargetRoute(endpoint=str(server.make_url("/reply")), adapter="acme-slack")
            transport = HttpReplyTransport(
                {"acme-slack": "test-egress"},
                {
                    ("slack", route): ReplyCapabilities(
                        edits_in_place=slack.edits_in_place("slack", provider_route),
                        undeliverable_reason=slack.undeliverable_reason("slack", provider_route),
                    )
                },
            )
            router = ReplySinkRouter(adapters={"slack": transport}, default=transport)
            event = _EVENTS.validate_python(body)
            async with aiohttp.ClientSession() as client:
                for headers in ({}, {"X-Curie-Adapter-Secret": "wrong"}):
                    async with client.post(
                        server.make_url("/reply"), data=event.model_dump_json(), headers=headers
                    ) as response:
                        assert response.status == 401
                assert calls == []
            wrong = HttpReplyTransport(
                {"acme-slack": "wrong"},
                {("slack", route): ReplyCapabilities(True, None)},
            )
            try:
                with pytest.raises(RejectedAdapterResponseError, match="401"):
                    await wrong.emit(event, route=route, best_effort_unreachable=True)
                assert calls == []
            finally:
                await wrong.aclose()
            async with aiohttp.ClientSession() as client:
                for wire in ("{}", event.model_dump_json()[:-1] + ',"route":{}}'):
                    async with client.post(
                        server.make_url("/reply"),
                        data=wire,
                        headers={"X-Curie-Adapter-Secret": "test-egress"},
                    ) as response:
                        assert response.status == 400
                assert calls == []
            assert router.edits_in_place("slack", route)
            assert router.undeliverable_reason("slack", route) is None
            ack = await router.emit(event, route=route)
            assert received == [event.model_dump_json().encode()]
            # Validation at the receiver preserves each version, including verbatim bytes.
            if event.event in {"reply.update", "reply.post"}:
                assert calls
                assert calls[-1]["channel"] == event.target.address
                assert ack.ref == (
                    event.target.reply_ref
                    if isinstance(event, ReplyUpdate) and event.target.reply_ref
                    else "1700.000200"
                )
            if isinstance(event, ReplyUpdate) and event.text_mode == "verbatim":
                assert calls[-1]["text"] == event.text
            with pytest.raises(KeyError):
                await transport.emit(event, route=TargetRoute(adapter="undeclared"))
        finally:
            if transport is not None:
                await transport.aclose()
            await server.close()
            await slack_server.close()

    asyncio.run(run())


def test_gateway_reply_rejects_missing_credential_and_invalid_configuration() -> None:
    async def run() -> None:
        route = TargetRoute(endpoint="https://example.com/reply", adapter="acme-slack")
        transport = HttpReplyTransport({}, {("slack", route): ReplyCapabilities(True, None)})
        try:
            with pytest.raises(MissingAdapterCredentialError):
                await transport.emit(_EVENTS.validate_python(_BODIES[0]), route=route)
        finally:
            await transport.aclose()

    asyncio.run(run())
    with pytest.raises(ValueError, match="secret"):
        reply_application(SlackReplyAdapter("xoxb-test"), secret="", route=TargetRoute())


def test_gateway_http_routes_keep_each_slack_identity() -> None:
    async def run() -> None:
        tokens: list[str | None] = []

        async def provider(request: web.Request) -> web.Response:
            tokens.append(request.headers.get("Authorization"))
            return web.json_response({"ok": True, "ts": "1700.000200"})

        provider_app = web.Application()
        provider_app.router.add_post("/chat.update", provider)
        provider_server = TestServer(provider_app)
        await provider_server.start_server()
        slack = SlackReplyAdapter(
            "xoxb-default",
            identity_tokens={"alpha": "xoxb-alpha", "beta": "xoxb-beta"},
            base_url=str(provider_server.make_url("/")),
        )
        application = web.Application()
        for identity in ("alpha", "beta"):
            application.add_subapp(
                f"/{identity}",
                reply_application(
                    slack, secret=f"test-egress-{identity}", route=TargetRoute(adapter=identity)
                ),
            )
        server = TestServer(application)
        transport: HttpReplyTransport[TargetRoute] | None = None
        try:
            await server.start_server()
            routes = [
                TargetRoute(endpoint=str(server.make_url(f"/{identity}/reply")), adapter=identity)
                for identity in ("alpha", "beta")
            ]
            transport = HttpReplyTransport(
                {identity: f"test-egress-{identity}" for identity in ("alpha", "beta")},
                {("slack", route): ReplyCapabilities(True, None) for route in routes},
            )
            router = ReplySinkRouter(adapters={"slack": transport}, default=transport)
            event = ReplyUpdate(
                version="1.0", event="reply.update", target=_BODIES[0]["target"], text="Reply"
            )
            for route in routes:
                await router.emit(event, route=route)
            # Slack token headers: https://docs.slack.dev/authentication/tokens/
            assert tokens == ["Bearer xoxb-alpha", "Bearer xoxb-beta"]
        finally:
            if transport is not None:
                await transport.aclose()
            await server.close()
            await provider_server.close()

    asyncio.run(run())


@pytest.mark.parametrize("status", [410, 424])
def test_gateway_preserves_explicit_provider_completion_refusals(status: int) -> None:
    from curie_channel_gateway.reply_transport import (
        DeletedReplyTargetError,
        HttpReplyAdapter,
        ProviderEgressRefusedError,
    )

    async def run() -> None:
        error = DeletedReplyTargetError if status == 410 else ProviderEgressRefusedError
        requests: list[bytes] = []

        async def provider(request: web.Request) -> web.Response:
            assert request.headers["X-Curie-Adapter-Secret"] == "test-provider"
            requests.append(await request.read())
            return web.json_response({"detail": error.reason}, status=status)

        provider_app = web.Application()
        provider_app.router.add_post("/reply", provider)
        provider_server = TestServer(provider_app)
        await provider_server.start_server()
        inner = HttpReplyAdapter({"provider": "test-provider"})
        server = TestServer(
            reply_application(
                inner,
                secret="test-gateway",
                route=TargetRoute(
                    endpoint=str(provider_server.make_url("/reply")), adapter="provider"
                ),
            )
        )
        outer: HttpReplyTransport[TargetRoute] | None = None
        try:
            await server.start_server()
            event = _EVENTS.validate_python(
                next(
                    case["body"]
                    for case in _CORPUS["v1_0"]
                    if case["body"]["event"] == "turn.completed"
                )
            )
            route = TargetRoute(endpoint=str(server.make_url("/reply")), adapter="gateway")
            outer = HttpReplyTransport(
                {"gateway": "test-gateway"},
                {(event.target.kind, route): ReplyCapabilities(False, None)},
            )
            with pytest.raises(error):
                await outer.emit(event, route=route, best_effort_unreachable=True)
            assert requests == [event.model_dump_json().encode()]
        finally:
            if outer is not None:
                await outer.aclose()
            await inner.aclose()
            await server.close()
            await provider_server.close()

    asyncio.run(run())
