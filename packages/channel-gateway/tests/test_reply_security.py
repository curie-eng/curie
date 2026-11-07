"""Exercise the authenticated gateway boundary through real HTTP requests."""

import asyncio
import time
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

import aiohttp
import pytest
from aiohttp import web
from aiohttp.test_utils import TestServer
from channel_protocol import OutboundMessage, ReplyPost, ReplyTarget, ReplyUpdate
from curie_channel_gateway.http_reply import HttpReplyTransport, reply_application
from curie_channel_gateway.ports import ReplyCapabilities
from curie_channel_gateway.reply_delivery import (
    BEST_EFFORT_HEADER,
    FRESHNESS_WINDOW_S,
    REQUEST_KEY_HEADER,
    signed_reply_headers,
)
from curie_channel_gateway.reply_errors import (
    ConflictingReplyRequestError,
    DeletedReplyTargetError,
    IndeterminateReplyDeliveryError,
    InvalidReplyTargetError,
    MissingAdapterCredentialError,
    OversizedAdapterResponseError,
    ProviderEgressRefusedError,
    ProviderReplyUnreachableError,
    RedirectedAdapterEndpointError,
    RejectedAdapterResponseError,
    ReplayedReplyRequestError,
    ReplyPayloadTooLargeError,
    UnconfiguredSlackIdentityError,
    UntrustedSlackEndpointError,
)
from curie_channel_gateway.reply_route import ReplyDestination
from curie_channel_gateway.reply_transport import BestEffortUnreachableAck, HttpReplyAdapter
from curie_channel_gateway.slack.slack_sink import SlackReplyAdapter


def test_http_gateway_requires_tls_unless_operator_explicitly_opts_out() -> None:
    route = ReplyDestination(endpoint="http://127.0.0.1:12345/reply", adapter="gateway")
    with pytest.raises(ValueError, match="HTTPS"):
        HttpReplyTransport(
            {"gateway": "test-secret"}, {("slack", route): ReplyCapabilities(True, None)}
        )


def test_non_http_schemes_are_refused_even_with_insecure_opt_out() -> None:
    route = ReplyDestination(endpoint="file:///reply", adapter="gateway")
    with pytest.raises(ValueError, match="HTTPS"):
        HttpReplyTransport(
            {"gateway": "test-secret"},
            {("slack", route): ReplyCapabilities(True, None)},
            allow_insecure_http=True,
        )


@pytest.mark.parametrize("timeout_s", [30.0, float("nan"), float("inf")])
def test_gateway_timeout_cannot_be_shorter_than_provider_delivery_budget(timeout_s: float) -> None:
    route = ReplyDestination(endpoint="https://example.com/reply", adapter="gateway")
    with pytest.raises(ValueError, match="budget"):
        HttpReplyTransport(
            {"gateway": "test-secret"},
            {("slack", route): ReplyCapabilities(True, None)},
            timeout_s=timeout_s,
        )


def update(text: str = "reply") -> ReplyUpdate:
    return ReplyUpdate(
        version="1.0",
        event="reply.update",
        text=text,
        target=ReplyTarget(
            kind="email", address="agent@example.com", conversation_id="thread", reply_ref=None
        ),
    )


@asynccontextmanager
async def gateway(
    path: Path,
    provider: Callable[[web.Request], Awaitable[web.Response]],
    seen_duplicate: asyncio.Event | None = None,
    **options: Any,
) -> AsyncIterator[tuple[TestServer, web.Application]]:
    provider_app = web.Application(client_max_size=64 * 1024 * 1024)
    provider_app.router.add_post("/reply", provider)
    provider_server = TestServer(provider_app)
    await provider_server.start_server()
    transport = HttpReplyAdapter({"provider": "test-provider"})
    app = reply_application(
        transport,
        secret="test-gateway",
        kind="email",
        state_path=path,
        route=ReplyDestination(str(provider_server.make_url("/reply")), "provider"),
        **options,
    )
    arrivals = 0

    @web.middleware
    async def observe(request: web.Request, handler: Any) -> web.StreamResponse:
        nonlocal arrivals
        arrivals += 1
        if seen_duplicate is not None and arrivals == 2:
            seen_duplicate.set()
        return await handler(request)

    app.middlewares.append(observe)
    server = TestServer(app)
    try:
        await server.start_server()
        yield server, app
    finally:
        await server.close()
        await transport.aclose()
        await provider_server.close()


def client(server: TestServer) -> tuple[HttpReplyTransport[ReplyDestination], ReplyDestination]:
    route = ReplyDestination(str(server.make_url("/reply")), "gateway")
    return HttpReplyTransport(
        {"gateway": "test-gateway"},
        {("email", route): ReplyCapabilities(False, None)},
        allow_insecure_http=True,
    ), route


def test_signed_replay_and_expired_or_modified_requests_never_reach_provider(
    tmp_path: Path,
) -> None:
    async def run() -> None:
        calls: list[bytes] = []

        async def provider(request: web.Request) -> web.Response:
            calls.append(await request.read())
            return web.json_response({"ref": "provider-ref"})

        body = update().model_dump_json().encode()
        headers = signed_reply_headers(
            "test-gateway",
            path="/reply",
            body=body,
            best_effort_unreachable=False,
        )
        path = tmp_path / "replies.sqlite3"
        async with gateway(path, provider) as (server, _):
            async with aiohttp.ClientSession() as http:
                async with http.post(
                    server.make_url("/reply"), data=body, headers=headers
                ) as reply:
                    assert reply.status == 200
        # Authentication nonces and delivery results survive receiver restart.
        async with gateway(path, provider) as (server, _):
            async with aiohttp.ClientSession() as http:
                async with http.post(
                    server.make_url("/reply"), data=body, headers=headers
                ) as reply:
                    assert reply.status == 425
                    assert await reply.json() == {"detail": "reply_request_replayed"}
                expired = signed_reply_headers(
                    "test-gateway",
                    path="/reply",
                    body=body,
                    best_effort_unreachable=False,
                    timestamp=int(time.time()) - FRESHNESS_WINDOW_S - 1,
                )
                tampered = signed_reply_headers(
                    "test-gateway",
                    path="/reply",
                    body=body,
                    best_effort_unreachable=False,
                )
                tampered[BEST_EFFORT_HEADER] = "1"
                for wire, proof in ((body, expired), (body + b" ", headers), (body, tampered)):
                    async with http.post(
                        server.make_url("/reply"),
                        data=wire,
                        headers=proof,
                    ) as reply:
                        assert reply.status == 401
            transport, route = client(server)
            try:
                assert (await transport.emit(update(), route=route)).ref == "provider-ref"
            finally:
                await transport.aclose()
        assert calls == [body]

    asyncio.run(run())


def test_reused_request_key_cannot_change_the_event(tmp_path: Path) -> None:
    async def run() -> None:
        calls: list[bytes] = []

        async def provider(request: web.Request) -> web.Response:
            calls.append(await request.read())
            return web.json_response({"ref": "provider-ref"})

        first = update().model_dump_json().encode()
        second = update("different").model_dump_json().encode()
        async with gateway(tmp_path / "replies.sqlite3", provider) as (server, _):
            async with aiohttp.ClientSession() as http:
                for body, status in ((first, 200), (second, 412)):
                    headers = signed_reply_headers(
                        "test-gateway",
                        path="/reply",
                        body=body,
                        best_effort_unreachable=False,
                        request_key="a" * 64,
                    )
                    assert headers[REQUEST_KEY_HEADER] == "a" * 64
                    async with http.post(
                        server.make_url("/reply"),
                        data=body,
                        headers=headers,
                    ) as reply:
                        assert reply.status == status
        assert calls == [first]

    asyncio.run(run())


def test_disconnected_v1_post_retry_shares_one_inflight_delivery_and_persistent_ack(
    tmp_path: Path,
) -> None:
    async def run() -> None:
        calls: list[bytes] = []
        entered = asyncio.Event()
        release = asyncio.Event()
        duplicate = asyncio.Event()

        async def provider(request: web.Request) -> web.Response:
            calls.append(await request.read())
            entered.set()
            await release.wait()
            return web.json_response({"ref": "one-provider-message"})

        event = ReplyPost(
            version="1.0",
            event="reply.post",
            target=update().target,
            message=OutboundMessage(version="1.0", text="approval"),
            requested_by="user-example",
        )
        path = tmp_path / "replies.sqlite3"
        async with gateway(path, provider, seen_duplicate=duplicate) as (server, _):
            transport, route = client(server)
            first = asyncio.create_task(transport.emit(event, route=route))
            try:
                await asyncio.wait_for(entered.wait(), 2)
                first.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await first
                retry = asyncio.create_task(transport.emit(event, route=route))
                await asyncio.wait_for(duplicate.wait(), 2)
                release.set()
                assert (await asyncio.wait_for(retry, 2)).ref == "one-provider-message"
                assert len(calls) == 1
            finally:
                release.set()
                await transport.aclose()
        async with gateway(path, provider) as (server, _):
            transport, route = client(server)
            try:
                assert (await transport.emit(event, route=route)).ref == "one-provider-message"
                assert len(calls) == 1
            finally:
                await transport.aclose()

    asyncio.run(run())


def test_budget_expiry_is_indeterminate_and_never_posts_again(tmp_path: Path) -> None:
    async def run() -> None:
        calls: list[bytes] = []
        release = asyncio.Event()

        async def provider(request: web.Request) -> web.Response:
            calls.append(await request.read())
            await release.wait()
            return web.json_response({"ref": "late-provider-message"})

        async with gateway(
            tmp_path / "replies.sqlite3",
            provider,
            delivery_timeout_s=1.0,
        ) as (server, _):
            transport, route = client(server)
            try:
                for _ in range(2):
                    with pytest.raises(IndeterminateReplyDeliveryError):
                        await asyncio.wait_for(transport.emit(update(), route=route), 2)
                assert len(calls) == 1
            finally:
                release.set()
                await transport.aclose()

    asyncio.run(run())


def test_large_reply_passes_and_configured_oversize_is_permanent(tmp_path: Path) -> None:
    async def run() -> None:
        calls: list[bytes] = []

        async def provider(request: web.Request) -> web.Response:
            calls.append(await request.read())
            return web.json_response({"ref": "provider-ref"})

        event = update("large answer\n" * 100_000)
        assert len(event.model_dump_json().encode()) > 1024 * 1024
        async with gateway(tmp_path / "large.sqlite3", provider) as (server, _):
            transport, route = client(server)
            try:
                assert (await transport.emit(event, route=route)).ref == "provider-ref"
                assert calls == [event.model_dump_json().encode()]
            finally:
                await transport.aclose()
        calls.clear()
        async with gateway(
            tmp_path / "bounded.sqlite3",
            provider,
            max_body_bytes=1024,
        ) as (server, _):
            transport, route = client(server)
            try:
                with pytest.raises(ReplyPayloadTooLargeError):
                    await transport.emit(update("x" * 2048), route=route)
                assert calls == []
                assert (await transport.emit(update(), route=route)).ref == "provider-ref"
            finally:
                await transport.aclose()

    asyncio.run(run())


_PERMANENT_ERRORS = [
    (InvalidReplyTargetError, 400, "invalid_reply_target"),
    (MissingAdapterCredentialError, 403, "missing_adapter_credential"),
    (UnconfiguredSlackIdentityError, 404, "unconfigured_slack_identity"),
    (ValueError, 406, "invalid_reply_event"),
    (IndeterminateReplyDeliveryError, 409, "reply_delivery_indeterminate"),
    (DeletedReplyTargetError, 410, "thread deleted at provider"),
    (ConflictingReplyRequestError, 412, "reply_request_conflict"),
    (ReplyPayloadTooLargeError, 413, "reply_payload_too_large"),
    (UntrustedSlackEndpointError, 421, "untrusted_slack_endpoint"),
    (RejectedAdapterResponseError, 422, "rejected_adapter_response"),
    (ProviderEgressRefusedError, 424, "provider egress refused"),
    (ReplayedReplyRequestError, 425, "reply_request_replayed"),
    (OversizedAdapterResponseError, 502, "oversized_adapter_response"),
    (RedirectedAdapterEndpointError, 508, "redirected_adapter_endpoint"),
]


@pytest.mark.parametrize("error_type,status,detail", _PERMANENT_ERRORS)
def test_each_permanent_class_survives_the_gateway_hop(
    tmp_path: Path,
    error_type: type[Exception],
    status: int,
    detail: str,
) -> None:
    async def run() -> None:
        async def provider(request: web.Request) -> web.Response:
            await request.read()
            return web.json_response(
                {"detail": detail, "private": "test-private-value"}, status=status
            )

        from channel_protocol import TurnCompleted

        event = TurnCompleted(
            version="1.0",
            event="turn.completed",
            target=update().target,
            event_id="event-example",
            outcome="delivered",
        )
        async with gateway(tmp_path / "replies.sqlite3", provider) as (server, _):
            transport, route = client(server)
            try:
                with pytest.raises(error_type) as caught:
                    await transport.emit(event, route=route, best_effort_unreachable=True)
                assert type(caught.value) is error_type
                assert "test-private-value" not in str(caught.value)
            finally:
                await transport.aclose()

    asyncio.run(run())


def test_best_effort_reaches_provider_and_preserves_unreachable_ack(tmp_path: Path) -> None:
    async def run() -> None:
        empty = TestServer(web.Application())
        await empty.start_server()
        endpoint = str(empty.make_url("/reply"))
        await empty.close()
        inner = HttpReplyAdapter({"provider": "test-provider"})
        app = reply_application(
            inner,
            secret="test-gateway",
            kind="email",
            state_path=tmp_path / "replies.sqlite3",
            route=ReplyDestination(endpoint, "provider"),
        )
        server = TestServer(app)
        await server.start_server()
        transport, route = client(server)
        try:
            with pytest.raises(ProviderReplyUnreachableError):
                await transport.emit(update(), route=route)
            ack = await transport.emit(update(), route=route, best_effort_unreachable=True)
            assert isinstance(ack, BestEffortUnreachableAck)
            assert ack.ref is None
        finally:
            await transport.aclose()
            await inner.aclose()
            await server.close()

    asyncio.run(run())


def test_slack_per_turn_endpoint_overrides_are_explicitly_refused(tmp_path: Path) -> None:
    with pytest.raises(InvalidReplyTargetError, match="per-turn endpoints"):
        reply_application(
            SlackReplyAdapter("xoxb-test"),
            secret="test-gateway",
            kind="slack",
            state_path=tmp_path / "replies.sqlite3",
            route=ReplyDestination("http://127.0.0.1:12345/api/", "identity"),
        )


def test_transport_refuses_slack_cli_stub_endpoints() -> None:
    route = ReplyDestination("https://example.com/slack/api/", "gateway")
    with pytest.raises(InvalidReplyTargetError, match="per-turn endpoints"):
        HttpReplyTransport(
            {"gateway": "test-secret"}, {("slack", route): ReplyCapabilities(True, None)}
        )


@pytest.mark.parametrize("body", [b"not-json", b'{"ref":42}', b'{"unknown":true}'])
def test_gateway_client_refuses_malformed_success_acknowledgements(body: bytes) -> None:
    async def run() -> None:
        async def malformed(request: web.Request) -> web.Response:
            await request.read()
            return web.Response(body=body, content_type="application/json")

        app = web.Application()
        app.router.add_post("/reply", malformed)
        server = TestServer(app)
        await server.start_server()
        transport, route = client(server)
        try:
            with pytest.raises(RejectedAdapterResponseError, match="acknowledgement"):
                await transport.emit(update(), route=route, best_effort_unreachable=True)
        finally:
            await transport.aclose()
            await server.close()

    asyncio.run(run())


def test_client_oversize_is_permanent_even_with_best_effort() -> None:
    async def run() -> None:
        calls: list[str] = []

        async def provider(request: web.Request) -> web.Response:
            calls.append(request.path)
            return web.json_response({"ref": "should-not-send"})

        app = web.Application()
        app.router.add_post("/reply", provider)
        server = TestServer(app)
        await server.start_server()
        transport, route = client(server)
        try:
            with pytest.raises(ReplyPayloadTooLargeError):
                await transport.emit(
                    update("x" * (64 * 1024 * 1024)),
                    route=route,
                    best_effort_unreachable=True,
                )
            assert calls == []
        finally:
            await transport.aclose()
            await server.close()

    asyncio.run(run())
