"""Shared spans export closed failure details and preserve caller status."""

from __future__ import annotations

import pytest
from curie_telemetry import operation_span
from curie_telemetry.tracing import configure_tracer_provider
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.trace import SpanKind, StatusCode


def _provider() -> tuple[TracerProvider, InMemorySpanExporter]:
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    configure_tracer_provider(provider)
    return provider, exporter


def test_escaped_exception_exports_class_only_without_message_or_stack() -> None:
    secret = "sk-" + "FAKEFAKEFAKEFAKEFAKEFAKEFAKEFAKE2222"
    provider, exporter = _provider()
    try:
        with pytest.raises(RuntimeError, match="failed"):
            with operation_span("worker.process", kind=SpanKind.CONSUMER):
                raise RuntimeError(f"failed credential={secret}")

        (span,) = exporter.get_finished_spans()
        assert span.status.status_code is StatusCode.ERROR
        (event,) = span.events
        assert event.name == "exception"
        assert dict(event.attributes or {}) == {"exception.type": "RuntimeError"}
        wire = repr(span.to_json())
        assert secret not in wire
        assert "failed credential" not in wire
        assert "exception.stacktrace" not in wire
        assert "exception.message" not in wire
    finally:
        configure_tracer_provider(None)
        provider.shutdown()


def test_normal_exit_does_not_overwrite_explicit_error_status() -> None:
    provider, exporter = _provider()
    try:
        with operation_span("api.request", kind=SpanKind.SERVER) as span:
            span.set_status(StatusCode.ERROR)

        (finished,) = exporter.get_finished_spans()
        assert finished.status.status_code is StatusCode.ERROR
    finally:
        configure_tracer_provider(None)
        provider.shutdown()


def test_unclassified_normal_exit_sets_ok_status() -> None:
    provider, exporter = _provider()
    try:
        with operation_span("api.request", kind=SpanKind.SERVER):
            pass

        (finished,) = exporter.get_finished_spans()
        assert finished.status.status_code is StatusCode.OK
    finally:
        configure_tracer_provider(None)
        provider.shutdown()


def test_operation_span_remains_a_noop_without_an_export_provider() -> None:
    configure_tracer_provider(None)

    with operation_span("offline.operation", kind=SpanKind.INTERNAL):
        pass


def test_operation_span_rejects_undeclared_platform_attribute() -> None:
    with pytest.raises(ValueError, match="undeclared platform span attribute"):
        with operation_span(
            "curie.turn.process",
            kind=SpanKind.INTERNAL,
            attributes={"session.id": "session-example"},
        ):
            pass


def test_operation_span_exports_custom_attributes_only_under_curie_namespace() -> None:
    provider, exporter = _provider()
    try:
        with operation_span(
            "curie.turn.process",
            kind=SpanKind.INTERNAL,
            attributes={
                "service.name": "curie-worker",
                "operation": "process",
                "role": "consumer",
                "source": "worker",
                "outcome": "pending",
                "retry_class": "redelivery",
            },
        ) as span:
            span.set_attribute("outcome", "done")
            span.add_event(
                "turn.processing.failed",
                {"outcome": "classified_failure", "error.class": "RuntimeError"},
            )

        (finished,) = exporter.get_finished_spans()
        assert dict(finished.attributes or {}) == {
            "service.name": "curie-worker",
            "curie.operation": "process",
            "curie.role": "consumer",
            "curie.source": "worker",
            "curie.outcome": "done",
            "curie.retry_class": "redelivery",
        }
        (event,) = finished.events
        assert dict(event.attributes or {}) == {
            "curie.outcome": "classified_failure",
            "error.type": "RuntimeError",
        }
        exported_keys = set(finished.attributes or {}) | set(event.attributes or {})
        assert not exported_keys & {"operation", "role", "source", "outcome", "retry_class"}
    finally:
        configure_tracer_provider(None)
        provider.shutdown()


def test_operation_span_rejects_undeclared_mutation_and_event_attributes() -> None:
    provider, _ = _provider()
    try:
        with operation_span("curie.turn.process", kind=SpanKind.INTERNAL) as span:
            with pytest.raises(ValueError, match="undeclared platform span attribute"):
                span.set_attribute("session.id", "session-example")
            with pytest.raises(ValueError, match="undeclared platform event attribute"):
                span.add_event("turn.received", {"user.id": "U0EXAMPLE1"})
    finally:
        configure_tracer_provider(None)
        provider.shutdown()
