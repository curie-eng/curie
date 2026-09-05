"""Exercise the resilience oracle through the real runner HTTP surface.

Only the external model is scripted. A classified failure is a real runner
terminal, and must never qualify as a successful resilience turn.
"""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path

import pytest
from aci_protocol import PROTOCOL_VERSION
from aiohttp import web
from aiohttp.test_utils import TestServer
from claude_agent_sdk import ResultMessage
from curie_runner import RunTracer, SideEffectClassifier, create_app
from curie_runner.fake import FakeModelSession
from curie_runner.session import SessionRunner
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

sys.path.insert(0, str(Path(__file__).parent))

from resilience_harness import post_event  # noqa: E402


@pytest.mark.parametrize("failed", [False, True])
def test_resilience_requires_success_from_the_actual_runner(failed: bool) -> None:
    async def run() -> None:
        model = FakeModelSession(
            lambda: [
                ResultMessage(
                    subtype="error_during_execution" if failed else "success",
                    duration_ms=1,
                    duration_api_ms=1,
                    is_error=failed,
                    num_turns=1,
                    session_id="resilience-session",
                    result="provider refused" if failed else "completed",
                )
            ]
        )
        runner = SessionRunner(
            session_factory=lambda: model,
            ceiling=0,
            tracer=RunTracer(None),
            classifier=SideEffectClassifier(),
            trace_name="resilience-oracle",
        )
        await runner.start()
        async with TestServer(create_app(runner)) as server:
            base = str(server.make_url("/")).rstrip("/")
            if failed:
                with pytest.raises(AssertionError, match="classified-failure"):
                    await asyncio.to_thread(post_event, base, "run the task")
            else:
                frames = await asyncio.to_thread(post_event, base, "run the task")
                assert frames[-1]["status"] == "done"

    asyncio.run(run())


def test_resilience_can_drive_the_cold_resumed_runner_token() -> None:
    async def run() -> None:
        runner = SessionRunner(
            session_factory=FakeModelSession,
            ceiling=0,
            tracer=RunTracer(None),
            classifier=SideEffectClassifier(),
            trace_name="resilience-token",
        )
        await runner.start()
        async with TestServer(create_app(runner, token="example-conversation-token")) as server:
            base = str(server.make_url("/")).rstrip("/")
            from urllib.error import HTTPError

            with pytest.raises(HTTPError) as refused:
                await asyncio.to_thread(post_event, base, "unauthorized")
            assert refused.value.code == 401
            frames = await asyncio.to_thread(
                post_event, base, "authorized", token="example-conversation-token"
            )
            assert frames[-1]["status"] == "done"

    asyncio.run(run())


def test_resilience_turn_trace_is_the_runner_trace() -> None:
    async def run() -> None:
        exporter = InMemorySpanExporter()
        provider = TracerProvider()
        provider.add_span_processor(SimpleSpanProcessor(exporter))
        runner = SessionRunner(
            session_factory=FakeModelSession,
            ceiling=0,
            tracer=RunTracer(provider),
            classifier=SideEffectClassifier(),
            trace_name="resilience-trace",
        )
        await runner.start()
        async with TestServer(create_app(runner)) as server:
            base = str(server.make_url("/")).rstrip("/")
            trace_id = "1234567890abcdef1234567890abcdef"
            await asyncio.to_thread(post_event, base, "trace the turn", trace_id=trace_id)
            spans = exporter.get_finished_spans()
            roots = [span for span in spans if span.name == "agent.run"]
            assert len(roots) == 1
            assert f"{roots[0].context.trace_id:032x}" == trace_id
            await asyncio.to_thread(post_event, base, "independent turn")
            roots = [span for span in exporter.get_finished_spans() if span.name == "agent.run"]
            assert len(roots) == 2
            assert f"{roots[1].context.trace_id:032x}" != trace_id
        provider.shutdown()

    asyncio.run(run())


@pytest.mark.parametrize("wire", [
    '{"type":"final","version":"private-wire-sentinel","status":"done","text":"private-output-sentinel"}',
    '{"type":"final","version":"' + PROTOCOL_VERSION + '",'
    '"status":"private-output-sentinel","text":"private-output-sentinel"}',
])
def test_resilience_malformed_wire_diagnostics_are_redacted(wire: str) -> None:
    async def run() -> None:
        async def malformed(_request: web.Request) -> web.Response:
            return web.Response(text=wire, content_type="application/x-ndjson")

        producer = web.Application()
        producer.router.add_post("/v1/event", malformed)
        async with TestServer(producer) as server:
            with pytest.raises(AssertionError) as failed:
                await asyncio.to_thread(post_event, str(server.make_url("/")).rstrip("/"), "run")
            assert str(failed.value) == "runner emitted invalid NDJSON"
            assert failed.value.__suppress_context__

    asyncio.run(run())
