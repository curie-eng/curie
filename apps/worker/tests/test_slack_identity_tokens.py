"""The Slack sink speaks as the identity its route names (ADR-0168 decision 5).

Slack is an in-process capture server, so the bot token behind each call is
read off the Authorization header that left the process, never off a patched
client.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Mapping

import pytest
from aiohttp import web
from aiohttp.test_utils import TestServer
from channel_protocol import MESSAGE_VERSION, Action, ConfirmIntent, OutboundMessage
from channel_protocol.reply import (
    REPLY_WIRE_VERSION,
    ReplyAck,
    ReplyEvent,
    ReplyPost,
    ReplyTarget,
    ReplyUpdate,
    SettledOutcome,
    TurnCompleted,
    TurnStatus,
)
from curie_worker.config import WorkerConfig
from curie_worker.reply_sink import (
    CLUSTER_MESSAGE_ADAPTER,
    ObservedReplySink,
    ReplySinkRouter,
    TargetRoute,
    build_reply_sink,
)
from curie_worker.slack_sink import SlackReplyAdapter, UnconfiguredSlackIdentityError

_DEFAULT_TOKEN = "xoxb-default-sentinel"
_OPS_TOKEN = "xoxb-ops-bot-sentinel"
_TOKENS = {"default": _DEFAULT_TOKEN, "ops-bot": _OPS_TOKEN}
_APPROVAL_ID = "123e4567-e89b-42d3-a456-426614174000"
_CHANNEL = "C0EXAMPLE1"
_THREAD = "1720000000.000100"


class _Capture:
    """Records the method and the Authorization header of every Slack call."""

    def __init__(self) -> None:
        self.requests: list[tuple[str, str | None]] = []
        self.app = web.Application()
        self.app.add_routes([web.post("/slack/api/{method}", self._slack)])

    async def _slack(self, request: web.Request) -> web.Response:
        self.requests.append(
            (request.match_info["method"], request.headers.get("Authorization"))
        )
        return web.json_response({"ok": True, "ts": "1720000000.000200"})

    def methods(self) -> list[str]:
        return [method for method, _auth in self.requests]

    def tokens(self) -> set[str | None]:
        return {auth for _method, auth in self.requests}


def _run(
    body: Callable[[ReplySinkRouter, int], Awaitable[None]],
    *,
    tokens: Mapping[str, str] = _TOKENS,
) -> _Capture:
    capture = _Capture()

    async def go() -> None:
        server = TestServer(capture.app)
        await server.start_server()
        sink: ReplySinkRouter | None = None
        try:
            port = server.port
            assert port is not None
            sink = build_reply_sink(
                WorkerConfig(
                    slack_bot_token=_DEFAULT_TOKEN,
                    slack_api_base_url=f"http://127.0.0.1:{port}/slack/api/",
                ),
                slack_tokens=tokens,
            )
            await body(sink, port)
        finally:
            if sink is not None:
                await sink.aclose()
            await server.close()

    asyncio.run(go())
    return capture


def _target(reply_ref: str | None = _THREAD) -> ReplyTarget:
    return ReplyTarget(
        kind="slack", address=_CHANNEL, conversation_id=_THREAD, reply_ref=reply_ref
    )


def _update(text: str = "the answer") -> ReplyUpdate:
    return ReplyUpdate(
        version=REPLY_WIRE_VERSION, event="reply.update", target=_target(), text=text
    )


def _card() -> ReplyPost:
    return ReplyPost(
        version=REPLY_WIRE_VERSION,
        event="reply.post",
        target=_target(None),
        message=OutboundMessage(
            version=MESSAGE_VERSION,
            text="Approve the refund?",
            interaction=ConfirmIntent(
                kind="confirm",
                id=_APPROVAL_ID,
                prompt="Approve the refund?",
                confirm=Action(label="Approve", value=_APPROVAL_ID),
                cancel=Action(label="Reject", value=_APPROVAL_ID),
            ),
        ),
        requested_by="U1",
    )


def _status() -> TurnStatus:
    return TurnStatus(
        version=REPLY_WIRE_VERSION, event="turn.status", target=_target(None), status="Thinking"
    )


def _every_slack_call() -> list[ReplyEvent]:
    return [
        _status(),
        _update(),
        ReplyUpdate(
            version=REPLY_WIRE_VERSION,
            event="reply.update",
            target=_target(None),
            text="a job's first reply",
        ),
        _card(),
        ReplyUpdate(
            version=REPLY_WIRE_VERSION,
            event="reply.update",
            target=_target(),
            message=OutboundMessage(version=MESSAGE_VERSION, text="Approve the refund?"),
            settled=SettledOutcome(
                requested_by="U1", decision="approved", resolver="U2", note=None
            ),
        ),
    ]


def test_every_slack_call_on_a_named_identity_carries_that_identitys_token() -> None:
    async def body(sink: ReplySinkRouter, _port: int) -> None:
        for event in _every_slack_call():
            await sink.emit(event, route=TargetRoute(adapter="ops-bot"))

    capture = _run(body)

    # Status, reply edit, placeholder-less post, approval card, card settle.
    assert capture.methods() == [
        "assistant.threads.setStatus",
        "chat.update",
        "chat.postMessage",
        "chat.postMessage",
        "chat.update",
    ]
    assert capture.tokens() == {f"Bearer {_OPS_TOKEN}"}


@pytest.mark.parametrize("adapter", [None, "default"])
def test_a_route_naming_default_or_no_identity_keeps_the_default_token(
    adapter: str | None,
) -> None:
    async def body(sink: ReplySinkRouter, _port: int) -> None:
        for event in _every_slack_call():
            await sink.emit(event, route=TargetRoute(adapter=adapter))

    capture = _run(body)

    assert len(capture.requests) == 5
    assert capture.tokens() == {f"Bearer {_DEFAULT_TOKEN}"}


def test_a_per_turn_slack_origin_still_speaks_as_the_routes_identity() -> None:
    # A CLI stub turn's endpoint is a per-turn Slack origin (#19), never a
    # credential selector (ADR-0168 decision 3): the identity picks the token,
    # and an undeclared one is refused before any request, endpoint or not.
    async def body(sink: ReplySinkRouter, port: int) -> None:
        origin = f"http://127.0.0.1:{port}/slack/api/"
        await sink.emit(_update(), route=TargetRoute(endpoint=origin, adapter="ops-bot"))
        with pytest.raises(UnconfiguredSlackIdentityError, match="'agentmail-sandbox'"):
            await sink.emit(
                _update(), route=TargetRoute(endpoint=origin, adapter="agentmail-sandbox")
            )

    capture = _run(body)

    assert capture.requests == [("chat.update", f"Bearer {_OPS_TOKEN}")]


def test_an_identity_with_no_token_is_refused_before_any_request() -> None:
    ghost = TargetRoute(adapter="ghost")

    async def body(sink: ReplySinkRouter, _port: int) -> None:
        with pytest.raises(UnconfiguredSlackIdentityError, match="'ghost'") as refused:
            await sink.emit(_update(), route=ghost)
        assert _DEFAULT_TOKEN not in str(refused.value)
        assert _OPS_TOKEN not in str(refused.value)
        # Best-effort delivery does not swallow it, and a card is refused alike.
        with pytest.raises(UnconfiguredSlackIdentityError):
            await sink.emit(_update(), route=ghost, best_effort_unreachable=True)
        with pytest.raises(UnconfiguredSlackIdentityError):
            await sink.emit(_card(), route=ghost)
        # Status is best effort and a completion sends nothing on Slack: neither
        # raises, and neither reaches Slack as another bot.
        assert await sink.emit(_status(), route=ghost) == ReplyAck(ref=None)
        await sink.emit(
            TurnCompleted(
                version=REPLY_WIRE_VERSION,
                event="turn.completed",
                target=_target(),
                event_id="ev-ghost",
                outcome="dropped",
            ),
            route=ghost,
        )

    capture = _run(body)

    assert capture.requests == []


def test_the_router_names_the_routes_it_cannot_deliver() -> None:
    sink = build_reply_sink(WorkerConfig(slack_bot_token=_DEFAULT_TOKEN), slack_tokens=_TOKENS)

    for kind, route in [
        ("slack", TargetRoute()),
        ("slack", TargetRoute(adapter="default")),
        ("slack", TargetRoute(adapter="ops-bot")),
        ("slack", TargetRoute(adapter=CLUSTER_MESSAGE_ADAPTER)),
        ("slack", TargetRoute(endpoint="https://slack.com/api/", adapter="ops-bot")),
        ("email", TargetRoute(endpoint="https://adapter.example/hook", adapter="ghost")),
    ]:
        assert sink.undeliverable_reason(kind, route) is None, (kind, route)

    reason = sink.undeliverable_reason("slack", TargetRoute(adapter="ghost"))
    assert reason is not None and "'ghost'" in reason
    # An endpoint does not exempt a Slack route from naming a declared identity.
    with_origin = TargetRoute(endpoint="https://slack.com/api/", adapter="agentmail-sandbox")
    stub_reason = sink.undeliverable_reason("slack", with_origin)
    assert stub_reason is not None and "'agentmail-sandbox'" in stub_reason
    assert _DEFAULT_TOKEN not in reason and _OPS_TOKEN not in reason
    # The kernel holds the sink behind ObservedReplySink, which must forward.
    assert ObservedReplySink(sink).undeliverable_reason(
        "slack", TargetRoute(adapter="ghost")
    ) == reason


def test_a_sink_without_the_check_reports_nothing_undeliverable() -> None:
    class _EmitOnly:
        async def emit(
            self, event: ReplyEvent, *, route: TargetRoute, best_effort_unreachable: bool = False
        ) -> ReplyAck:
            return ReplyAck()

    router = ReplySinkRouter(adapters={}, default=_EmitOnly())
    assert router.undeliverable_reason("slack", TargetRoute(adapter="ghost")) is None
    assert ObservedReplySink(_EmitOnly()).undeliverable_reason(
        "slack", TargetRoute(adapter="ghost")
    ) is None


def test_a_stock_adapter_builds_one_client_per_endpoint_as_before() -> None:
    adapter = SlackReplyAdapter("xoxb-test")

    default = adapter._client_for(None)

    assert adapter._client_for(None, "default") is default
    assert len(adapter._clients) == 1
    with pytest.raises(UnconfiguredSlackIdentityError):
        adapter._client_for(None, "ops-bot")
    assert len(adapter._clients) == 1


def test_two_identities_on_one_sink_each_keep_their_own_token() -> None:
    """A cache keyed only on base URL would let whichever identity spoke first
    go on answering for every identity after it: the wrong-bot reply
    ADR-0168 decision 5 exists to prevent. ``default``, ``ops-bot``, then
    ``default`` again, on the SAME sink, must carry three different requests
    with the right token each time, and the same identity must reuse its own
    cached client rather than minting a new one per call.
    """

    async def body(sink: ReplySinkRouter, _port: int) -> None:
        await sink.emit(_update(), route=TargetRoute(adapter=None))
        await sink.emit(_update(), route=TargetRoute(adapter="ops-bot"))
        await sink.emit(_update(), route=TargetRoute(adapter=None))
        adapter = sink._adapters["slack"]
        assert len(adapter._clients) == 2

    capture = _run(body)

    assert [auth for _method, auth in capture.requests] == [
        f"Bearer {_DEFAULT_TOKEN}",
        f"Bearer {_OPS_TOKEN}",
        f"Bearer {_DEFAULT_TOKEN}",
    ]


def test_default_is_never_refused_even_with_a_blank_token() -> None:
    """``default``'s token is always in the map, blank or not, so a stock
    install with an empty ``SLACK_BOT_TOKEN`` still sends what it always sent
    and ``undeliverable_reason`` never refuses it.
    """
    capture = _Capture()

    async def go() -> None:
        server = TestServer(capture.app)
        await server.start_server()
        sink: ReplySinkRouter | None = None
        try:
            port = server.port
            assert port is not None
            sink = build_reply_sink(
                WorkerConfig(
                    slack_bot_token="",
                    slack_api_base_url=f"http://127.0.0.1:{port}/slack/api/",
                )
            )
            assert sink.undeliverable_reason("slack", TargetRoute()) is None
            await sink.emit(_update(), route=TargetRoute())
        finally:
            if sink is not None:
                await sink.aclose()
            await server.close()

    asyncio.run(go())

    assert capture.methods() == ["chat.update"]
