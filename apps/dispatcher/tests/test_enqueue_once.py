"""Slack-free one-shot dispatcher producer against real Valkey."""

from __future__ import annotations

import io
from collections.abc import Iterator
from contextlib import contextmanager

import redis
from aci_protocol import QueuedTurn, ReplyHandle
from curie_channel_gateway.slack.config import DispatcherConfig
from curie_dispatcher import enqueue_once
from curie_dispatcher.queue import from_stream_fields
from curie_telemetry import TRACEPARENT_STREAM_FIELD, extract_trace_context
from curie_telemetry.tracing import configure_tracer_provider
from opentelemetry import context as otel_context
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.trace import NonRecordingSpan, SpanContext, TraceFlags, TraceState

_TRACE_ID = int("fedcba9876543210fedcba9876543210", 16)
_SPAN_ID = int("fedcba9876543210", 16)
_TRACEPARENT = "00-fedcba9876543210fedcba9876543210-fedcba9876543210-01"


@contextmanager
def _remote_parent() -> Iterator[None]:
    parent = SpanContext(
        trace_id=_TRACE_ID,
        span_id=_SPAN_ID,
        is_remote=True,
        trace_flags=TraceFlags.SAMPLED,
        trace_state=TraceState(),
    )
    token = otel_context.attach(trace.set_span_in_context(NonRecordingSpan(parent)))
    try:
        yield
    finally:
        otel_context.detach(token)


def _turn() -> QueuedTurn:
    return QueuedTurn(
        event_id="EvSIM-one-shot",
        conversation_id="1700000000.000100",
        author="U0EXAMPLE1",
        text="exercise the dispatcher producer",
        reply_handle=ReplyHandle(
            kind="slack",
            channel="C0EXAMPLE1",
            placeholder="1700000000.000200",
        ),
        received_at="2026-08-23T00:00:00+00:00",
    )


@contextmanager
def _captured_spans() -> Iterator[InMemorySpanExporter]:
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    configure_tracer_provider(provider)
    try:
        yield exporter
    finally:
        configure_tracer_provider(None)
        provider.shutdown()


def test_enqueue_payload_uses_dispatcher_carrier_beside_unchanged_payload(
    redis_client: redis.Redis,
    config: DispatcherConfig,
) -> None:
    """Red-on-revert: the CLI bridge must call dispatcher ``enqueue``."""
    turn = _turn()
    with _remote_parent():
        stream_id = enqueue_once.enqueue_payload(
            turn.model_dump_json(),
            config=config,
            redis_client=redis_client,
        )

    entries = redis_client.xrange(config.stream)
    assert len(entries) == 1
    assert entries[0][0] == stream_id
    fields = entries[0][1]
    assert fields["payload"] == turn.model_dump_json()
    carrier = fields[TRACEPARENT_STREAM_FIELD]
    _, trace_id, span_id, flags = carrier.split("-")
    assert trace_id == _TRACEPARENT.split("-")[1]
    assert len(span_id) == 16
    assert flags == "01"
    assert from_stream_fields(fields) == turn


def test_enqueue_payload_owns_the_same_ingress_root_as_slack(
    redis_client: redis.Redis,
    config: DispatcherConfig,
) -> None:
    """The CLI bridge is a sibling ingress, not a parentless producer."""

    turn = _turn()
    with _captured_spans() as exporter:
        stream_id = enqueue_once.enqueue_payload(
            turn.model_dump_json(),
            config=config,
            redis_client=redis_client,
        )

    spans = exporter.get_finished_spans()
    receipt, = [span for span in spans if span.name == "curie.turn.ingress"]
    producer, = [span for span in spans if span.name == "curie.queue.enqueue"]
    assert receipt.parent is None
    assert producer.context.trace_id == receipt.context.trace_id
    assert producer.parent is not None
    assert producer.parent.span_id == receipt.context.span_id

    (entry_id, fields), = redis_client.xrange(config.stream)
    assert entry_id == stream_id
    transported = trace.get_current_span(extract_trace_context(fields)).get_span_context()
    assert transported.trace_id == receipt.context.trace_id
    assert transported.span_id == producer.context.span_id
    assert fields["payload"] == turn.model_dump_json()


def test_main_without_otlp_endpoint_prints_only_the_stream_id(
    redis_client: redis.Redis,
    config: DispatcherConfig,
    monkeypatch,
    capsys,
) -> None:
    """No-endpoint mode keeps the real Valkey producer functional and quiet."""
    turn = _turn()
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")
    monkeypatch.setenv("VALKEY_HOST", config.valkey_host)
    monkeypatch.setenv("VALKEY_PORT", str(config.valkey_port))
    monkeypatch.setenv("VALKEY_PASSWORD", config.valkey_password)
    monkeypatch.setenv("CURIE_STREAM", config.stream)
    monkeypatch.setenv(
        "CURIE_APPROVAL_CHAT_ATTESTER_SECRET", "dispatcher-attester-test-secret"
    )
    monkeypatch.setattr(enqueue_once.sys, "stdin", io.StringIO(turn.model_dump_json()))

    assert enqueue_once.main() == 0

    captured = capsys.readouterr()
    stream_id = captured.out.strip()
    milliseconds, sequence = stream_id.split("-", maxsplit=1)
    assert milliseconds.isdigit()
    assert sequence.isdigit()
    assert redis_client.xlen(config.stream) == 1
    assert from_stream_fields(redis_client.xrange(config.stream)[0][1]) == turn


def test_main_rejects_input_without_echoing_payload_secrets(
    monkeypatch,
    capsys,
) -> None:
    """Invalid stdin fails on stderr without replaying rejected values."""
    sentinel = "not-a-real-secret-value"
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")
    monkeypatch.setattr(
        enqueue_once.sys,
        "stdin",
        io.StringIO(f'{{"text":"{sentinel}"}}'),
    )

    assert enqueue_once.main() == 1

    captured = capsys.readouterr()
    assert captured.out == ""
    assert "one-shot dispatcher enqueue failed" in captured.err
    assert "ValidationError" in captured.err
    assert sentinel not in captured.err
