"""A confirmed runner pod termination is a closed reason on the turn span (#4940).

The exported ``curie.turn.process`` lifecycle span carries the termination
reason as one of ``OOMKilled``, ``Evicted``, ``Deleted`` or ``Unknown``, plus
the exit code when it is a real 0..255 int. Kubelet message text never reaches
any exported span. These tests drive the real ``operation_span`` through an SDK
tracer provider, so they see exactly what is exported.
"""

from __future__ import annotations

import asyncio
import sys
from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
from aci_protocol import QueuedTurn, TextDelta
from curie_telemetry.tracing import configure_tracer_provider
from curie_worker.sandbox.types import SandboxTermination
from opentelemetry.sdk.trace import ReadableSpan, TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

# importlib import mode does not add the tests directory to sys.path.
sys.path.insert(0, str(Path(__file__).parent.parent))

from queue_fixtures import qevent  # noqa: E402


@pytest.fixture
def exporter() -> Iterator[InMemorySpanExporter]:
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    configure_tracer_provider(provider)
    try:
        yield exporter
    finally:
        configure_tracer_provider(None)
        provider.shutdown()


def _qevent(text: str, **overrides: Any) -> QueuedTurn:
    return qevent(
        text,
        channel="C0EXAMPLE1",
        received_at=datetime.now(UTC).isoformat(),
        **overrides,
    )


def _turn_span(exporter: InMemorySpanExporter) -> ReadableSpan:
    [span] = [s for s in exporter.get_finished_spans() if s.name == "curie.turn.process"]
    return span


async def _drop_stream(h, termination: SandboxTermination | None, thread: str) -> None:
    h.fake_k8s.termination = termination
    h.runner.default_script = [TextDelta(text="partial")]
    h.runner.abort_after_frames = True
    await h.kernel.process_event(_qevent("go", thread=thread))
    assert h.runner.opened == ["go"]
    assert h.fake_k8s.termination_queries


def test_oomkilled_termination_stamps_reason_and_int_exit_code(
    make_harness, exporter: InMemorySpanExporter
) -> None:
    async def go() -> None:
        async with make_harness(max_attempts=1) as h:
            await _drop_stream(
                h, SandboxTermination("OOMKilled", "exit code 137", 137), "thread-oom"
            )

            assert "sandbox-terminated" in (h.sink.last_text or "")
            attributes = _turn_span(exporter).attributes or {}
            assert attributes.get("curie.sandbox.termination.reason") == "OOMKilled"
            exit_code = attributes.get("curie.sandbox.termination.exit_code")
            assert exit_code == 137
            assert type(exit_code) is int

    asyncio.run(go())


def test_unlisted_termination_reason_is_exported_as_unknown(
    make_harness, exporter: InMemorySpanExporter
) -> None:
    async def go() -> None:
        async with make_harness(max_attempts=1) as h:
            await _drop_stream(h, SandboxTermination("NodeLost", "node went away"), "thread-lost")

            assert "sandbox-terminated" in (h.sink.last_text or "")
            attributes = _turn_span(exporter).attributes or {}
            assert attributes.get("curie.sandbox.termination.reason") == "Unknown"
            assert "curie.sandbox.termination.exit_code" not in attributes

    asyncio.run(go())


def test_evicted_termination_keeps_kubelet_message_off_the_span(
    make_harness, exporter: InMemorySpanExporter
) -> None:
    canary = "CANARY-4940-kubelet-detail-7f3a"
    message = 'Usage of EmptyDir volume "workspace" exceeds the limit "1Gi".'
    detail = f"{message} {canary}"

    async def go() -> None:
        async with make_harness(max_attempts=1) as h:
            await _drop_stream(h, SandboxTermination("Evicted", detail), "thread-evicted")

            # The terminal notice still carries the detail; only spans are closed.
            assert canary in (h.sink.last_text or "")
            attributes = _turn_span(exporter).attributes or {}
            assert attributes.get("curie.sandbox.termination.reason") == "Evicted"
            assert "curie.sandbox.termination.exit_code" not in attributes

            for span in exporter.get_finished_spans():
                values = [str(value) for value in (span.attributes or {}).values()]
                for event in span.events:
                    values.extend(str(value) for value in (event.attributes or {}).values())
                for value in values:
                    assert canary not in value, span.name
                    assert message not in value, span.name
                    assert "EmptyDir" not in value, span.name

    asyncio.run(go())


def test_stream_drop_without_pod_termination_sets_neither_key(
    make_harness, exporter: InMemorySpanExporter
) -> None:
    async def go() -> None:
        async with make_harness(max_attempts=1) as h:
            await _drop_stream(h, None, "thread-plain-drop")

            assert "runner-error" in (h.sink.last_text or "")
            assert "sandbox-terminated" not in (h.sink.last_text or "")
            attributes = _turn_span(exporter).attributes or {}
            assert "curie.sandbox.termination.reason" not in attributes
            assert "curie.sandbox.termination.exit_code" not in attributes

    asyncio.run(go())
