"""Kernel hooks retain Slack's declarations when delivery crosses HTTP."""

import asyncio

import pytest
from aiohttp import web
from aiohttp.test_utils import TestServer
from channel_protocol import ReplyEvent, ReplyTarget, ReplyUpdate
from curie_channel_gateway.ports import (
    AuthenticatedReplyTransport,
    DeclaredReplyTransport,
    ReplyCapabilities,
)
from curie_worker.reply_sink import HttpReplyAdapter, ReplySinkRouter, TargetRoute
from curie_worker.slack_sink import SlackReplyAdapter
from pydantic import TypeAdapter


def test_slack_capabilities_survive_http_delivery_and_kernel_routing() -> None:
    async def run() -> None:
        posts: list[dict[str, object]] = []

        async def slack_update(request: web.Request) -> web.Response:
            posts.append(await request.json())
            return web.json_response({"ok": True, "ts": "1700.000200"})

        provider_app = web.Application()
        provider_app.router.add_post("/chat.update", slack_update)
        provider = TestServer(provider_app)
        await provider.start_server()
        slack = SlackReplyAdapter(
            "xoxb-test",
            identity_tokens={"present": "xoxb-present"},
            base_url=str(provider.make_url("/")),
        )
        receiver_calls: list[ReplyEvent] = []

        async def receive(request: web.Request) -> web.Response:
            assert request.headers["X-Curie-Adapter-Secret"] == "test-egress"
            event = TypeAdapter(ReplyEvent).validate_json(await request.text())
            receiver_calls.append(event)
            ack = await slack.emit(event, route=TargetRoute(adapter="present"))
            return web.json_response(ack.model_dump())

        receiver_app = web.Application()
        receiver_app.router.add_post("/reply", receive)
        receiver = TestServer(receiver_app)
        http = HttpReplyAdapter({"present": "test-egress"})
        try:
            await receiver.start_server()
            declarations: dict[tuple[str, TargetRoute], ReplyCapabilities] = {}
            for identity in ("present", "absent"):
                route = TargetRoute(endpoint=str(receiver.make_url("/reply")), adapter=identity)
                provider_route = TargetRoute(adapter=identity)
                declarations["slack", route] = ReplyCapabilities(
                    edits_in_place=slack.edits_in_place("slack", provider_route),
                    undeliverable_reason=slack.undeliverable_reason("slack", provider_route),
                )
            transport: AuthenticatedReplyTransport[TargetRoute] = DeclaredReplyTransport(
                http, declarations
            )
            router = ReplySinkRouter(adapters={"slack": transport}, default=http)
            good, missing = (key[1] for key in declarations)
            assert router.edits_in_place("slack", good) is True
            assert router.edits_in_place("slack", missing) is True
            assert router.undeliverable_reason("slack", good) is None
            assert router.undeliverable_reason("slack", missing) == slack.undeliverable_reason(
                "slack", TargetRoute(adapter="absent")
            )
            assert router.undeliverable_reason("slack", missing) is not None
            event = ReplyUpdate(
                version="1.0",
                event="reply.update",
                text="Sibling limit notice",
                target=ReplyTarget(
                    kind="slack",
                    address="C0EXAMPLE1",
                    conversation_id="1700.000100",
                    reply_ref="1700.000200",
                ),
            )
            await router.emit(event, route=good)
            assert receiver_calls == [event]
            assert len(posts) == 1
            assert posts[0]["text"] == "Sibling limit notice"
            assert posts[0]["ts"] == "1700.000200"
            with pytest.raises(KeyError):
                transport.edits_in_place("email", good)
            with pytest.raises(KeyError):
                router.undeliverable_reason("slack", TargetRoute(adapter="undeclared"))
            with pytest.raises(KeyError) as refused:
                await router.emit(
                    event,
                    route=TargetRoute(
                        endpoint="https://example.com/reply?secret=test-private-value",
                        adapter="undeclared",
                    ),
                )
            assert "test-private-value" not in str(refused.value)
            assert receiver_calls == [event]
            assert len(posts) == 1
        finally:
            await http.aclose()
            await receiver.close()
            await provider.close()

    asyncio.run(run())
