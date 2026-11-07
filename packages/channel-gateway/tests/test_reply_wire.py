"""Freeze existing HTTP delivery using the shared reply compatibility corpus."""

import asyncio
import json
from pathlib import Path
from typing import Any

import pytest
from aiohttp import web
from aiohttp.test_utils import TestServer
from channel_protocol import ReplyAck, ReplyEvent, ReplyUpdate
from curie_channel_gateway.ports import ReplyEventTransport
from curie_channel_gateway.reply_transport import HttpReplyAdapter
from curie_worker.reply_sink import TargetRoute
from pydantic import TypeAdapter

_CORPUS = json.loads(
    (Path(__file__).parents[2] / "channel-protocol/schema/reply-wire.corpus.json").read_text()
)
_CASES = _CORPUS["v1_0"] + _CORPUS["v1_1"]
_EVENTS = TypeAdapter(ReplyEvent)


@pytest.mark.parametrize("body", [case["body"] for case in _CASES], ids=[c["name"] for c in _CASES])
def test_existing_http_transport_preserves_the_shared_wire(body: dict[str, Any]) -> None:
    _round_trip(_EVENTS.validate_python(body))


def test_existing_http_transport_preserves_verbatim_text() -> None:
    _round_trip(
        ReplyUpdate(
            version="1.2",
            event="reply.update",
            target=_CASES[0]["body"]["target"],
            text='```curie-reply\n{"text":"literal"}\n```\n  ',
            text_mode="verbatim",
            delivery_id="00000000-0000-4000-8000-000000000001",
        )
    )


def _round_trip(event: ReplyEvent) -> None:
    async def run() -> None:
        received: list[ReplyEvent] = []

        async def accept(request: web.Request) -> web.Response:
            assert request.headers["X-Curie-Adapter-Secret"] == "test-egress-secret"
            wire = await request.text()
            assert wire == event.model_dump_json()
            received.append(_EVENTS.validate_json(wire))
            return web.json_response({"ref": "opaque-provider-ref"})

        app = web.Application()
        app.router.add_post("/reply", accept)
        server = TestServer(app)
        await server.start_server()
        adapter = HttpReplyAdapter({"acme-adapter": "test-egress-secret"})
        try:
            transport: ReplyEventTransport[TargetRoute] = adapter
            ack = await transport.emit(
                event,
                route=TargetRoute(endpoint=str(server.make_url("/reply")), adapter="acme-adapter"),
            )
            assert received == [event]
            assert ack == ReplyAck(ref="opaque-provider-ref")
        finally:
            await adapter.aclose()
            await server.close()

    asyncio.run(run())
