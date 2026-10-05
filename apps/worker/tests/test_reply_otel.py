"""Reply transport latency and bounded delivery outcomes (#1818)."""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path
from typing import Any

import pytest
from aiohttp import web
from aiohttp.test_utils import TestServer
from channel_protocol import OutboundMessage
from channel_protocol.reply import (
    REPLY_WIRE_VERSION,
    ReplyAck,
    ReplyEvent,
    ReplyPost,
    ReplyTarget,
    ReplyUpdate,
)
from curie_telemetry import operation_span, record_metric
from curie_worker import reply_sink as reply_sink_module
from curie_worker.reply_sink import (
    HttpReplyAdapter,
    RejectedAdapterResponseError,
    ReplySinkRouter,
    TargetRoute,
)

# importlib import mode does not add this test directory to sys.path.
sys.path.insert(0, str(Path(__file__).parent))

from otel_fixtures import Probe, install  # noqa: E402

_BOUNDED_KEYS = {"service.name", "operation", "role", "source", "outcome"}


class _SlackReplies:
    def __init__(self) -> None:
        self.events: list[ReplyEvent] = []

    async def emit(
        self,
        event: ReplyEvent,
        *,
        route: TargetRoute,
        best_effort_unreachable: bool = False,
    ) -> ReplyAck:
        del route, best_effort_unreachable
        self.events.append(event)
        return ReplyAck(ref="1700000000.000050")


def _install(monkeypatch: pytest.MonkeyPatch) -> Probe:
    return install(monkeypatch, reply_sink_module)


def _target() -> ReplyTarget:
    return ReplyTarget(
        kind="email",
        address="agent@example.test",
        conversation_id="thread-example",
        reply_ref="message-example",
    )


def test_reply_update_post_and_failure_record_duration_and_bounded_result(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Drive actual HTTP bytes for both healthy and injected failure outcomes."""

    async def go() -> None:
        assert callable(operation_span)
        assert callable(record_metric)
        probe = _install(monkeypatch)

        async def handler(request: web.Request) -> web.Response:
            if request.path == "/fail":
                return web.Response(status=503)
            return web.json_response({"ref": "message-minted"})

        app = web.Application()
        app.add_routes([web.post("/{result}", handler)])
        server = TestServer(app)
        await server.start_server()
        adapter = HttpReplyAdapter({"mail-adapter": "example-secret"})
        try:
            endpoint = f"http://127.0.0.1:{server.port}/ok"
            route = TargetRoute(endpoint=endpoint, adapter="mail-adapter")
            update = ReplyUpdate(
                version=REPLY_WIRE_VERSION,
                event="reply.update",
                target=_target(),
                text="working",
            )
            post = ReplyPost(
                version=REPLY_WIRE_VERSION,
                event="reply.post",
                target=_target(),
                message=OutboundMessage(version="1.0", text="Approve this?"),
                requested_by="U0EXAMPLE1",
            )
            await adapter.emit(update, route=route)
            assert (await adapter.emit(post, route=route)).ref == "message-minted"

            with pytest.raises(RejectedAdapterResponseError):
                await adapter.emit(
                    update,
                    route=TargetRoute(
                        endpoint=f"http://127.0.0.1:{server.port}/fail",
                        adapter="mail-adapter",
                    ),
                )
        finally:
            await adapter.aclose()
            await server.close()

        for name in (
            "curie.reply.update.duration",
            "curie.reply.post.duration",
        ):
            points = [point for point in probe.metrics if point.name == name]
            assert points and all(point.value >= 0 for point in points)
        outcomes = {
            point.attributes["outcome"]
            for point in probe.metrics
            if point.name == "curie.reply.delivery"
        }
        assert outcomes >= {"success", "failure"}
        assert {"curie.reply.update", "curie.reply.post"} <= set(probe.span_names())
        assert all(set(point.attributes) <= _BOUNDED_KEYS for point in probe.metrics)
        assert all("thread-example" not in point.attributes.values() for point in probe.metrics)

    asyncio.run(go())


def test_best_effort_outcome_requires_an_actual_unreachable_delivery(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A successful ref-less channel must not look like a swallowed outage."""

    async def go() -> None:
        probe = _install(monkeypatch)

        async def handler(_request: web.Request) -> web.Response:
            return web.json_response({})

        app = web.Application()
        app.add_routes([web.post("/ok", handler)])
        server = TestServer(app)
        await server.start_server()
        adapter = HttpReplyAdapter({"mail-adapter": "example-secret"})
        update = ReplyUpdate(
            version=REPLY_WIRE_VERSION,
            event="reply.update",
            target=_target(),
            text="working",
        )
        try:
            success = await adapter.emit(
                update,
                route=TargetRoute(
                    endpoint=f"http://127.0.0.1:{server.port}/ok",
                    adapter="mail-adapter",
                ),
                best_effort_unreachable=True,
            )
            assert success.ref is None

            listener = await asyncio.start_server(lambda *_args: None, "127.0.0.1", 0)
            dead_port = listener.sockets[0].getsockname()[1]
            listener.close()
            await listener.wait_closed()
            unreachable = await adapter.emit(
                update,
                route=TargetRoute(
                    endpoint=f"http://127.0.0.1:{dead_port}/dead",
                    adapter="mail-adapter",
                ),
                best_effort_unreachable=True,
            )
            assert unreachable.ref is None
        finally:
            await adapter.aclose()
            await server.close()

        outcomes = [
            point.attributes["outcome"]
            for point in probe.metrics
            if point.name == "curie.reply.delivery"
        ]
        assert outcomes == ["success", "best-effort"]

    asyncio.run(go())


def test_publication_slack_egress_is_observed_exactly_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The worker-owned publication loop bypasses Kernel, but not reply telemetry."""

    async def go() -> None:
        from curie_worker import run as run_module
        from curie_worker.config import WorkerConfig

        probe = _install(monkeypatch)
        slack = _SlackReplies()
        router = ReplySinkRouter(adapters={"slack": slack}, default=slack)

        monkeypatch.setattr(run_module, "KubernetesPublicationCluster", lambda _namespace: object())
        monkeypatch.setattr(
            run_module, "PostgresPublicationStore", lambda *_args, **_kwargs: object()
        )
        engine_dependency: Any = object()
        http_dependency: Any = object()
        card_store_dependency: Any = object()
        github_api_url = "https://github.example.com/forge/api/v3"
        platform_api_url = "https://api.example.com"
        worker_token = "fixture-publication-worker-token"
        code_host_calls: list[tuple[str, str, Any]] = []
        credential_calls: list[tuple[str, str, str, Any]] = []

        def publication_code_host(*, api_base_url: str, worker_token: str, client: Any) -> object:
            code_host_calls.append((api_base_url, worker_token, client))
            return object()

        def publication_credentials(
            *, api_base_url: str, github_html_base: str, worker_token: str, client: Any
        ) -> object:
            credential_calls.append((api_base_url, github_html_base, worker_token, client))
            return object()

        monkeypatch.setattr(run_module, "PublicationCodeHostClient", publication_code_host)
        monkeypatch.setattr(run_module, "PublicationCredentialClient", publication_credentials)
        loop = run_module._build_publication_loop(
            WorkerConfig(
                publication_github_api_url=github_api_url,
                api_base_url=platform_api_url,
                internal_worker_token=worker_token,
            ),
            {"CURIE_SANDBOX_SUBSTRATE": "kubernetes"},
            engine_dependency,
            router,
            http_dependency,
            card_store_dependency,
        )
        assert loop is not None
        assert code_host_calls == [(platform_api_url, worker_token, http_dependency)]
        assert credential_calls == [
            (platform_api_url, "https://github.example.com/forge", worker_token, http_dependency)
        ]
        assert loop._reconciler._job_settings.github_api_url == github_api_url  # noqa: SLF001
        publication_replies = loop._reconciler._replies  # noqa: SLF001
        post = ReplyPost(
            version=REPLY_WIRE_VERSION,
            event="reply.post",
            target=ReplyTarget(
                kind="slack",
                address="C0EXAMPLE1",
                conversation_id="1700000000.000100",
                reply_ref=None,
            ),
            message=OutboundMessage(version="1.0", text="Publication approved."),
            requested_by="U0EXAMPLE1",
        )

        ack = await publication_replies.emit(
            post,
            route=TargetRoute(endpoint=None, adapter=None),
        )

        assert ack.ref == "1700000000.000050"
        assert slack.events == [post]
        assert probe.span_names().count("curie.reply.post") == 1
        deliveries = [point for point in probe.metrics if point.name == "curie.reply.delivery"]
        assert len(deliveries) == 1
        assert deliveries[0].attributes["outcome"] == "success"

    asyncio.run(go())
