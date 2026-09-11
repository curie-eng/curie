"""The channel event id is a span-only correlation attribute (#2622).

T1-T3 of the plan. These are the authoritative pins for the *exported* name and
for the contextvar lifecycle: the worker suite (``test_otel_runtime.py``)
monkeypatches ``operation_span`` wholesale, so it asserts CALLER spellings only
and can never see the export name or the ``operation_span`` wiring.
"""

from __future__ import annotations

import pytest
from curie_telemetry import (
    build_resource,
    channel_event_id_scope,
    configure_meter_provider,
    operation_span,
    record_metric,
    stamp_event_id,
)
from curie_telemetry.tracing import (
    PLATFORM_SPAN_ATTRIBUTE_KEYS,
    configure_tracer_provider,
    current_channel_event_id,
)
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricReader
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.trace import SpanKind

# The two production shapes an event id actually arrives in: a Slack ``Ev...``
# id, and the neutral channel-agnostic form the non-Slack adapters mint.
_SLACK_EVENT_ID = "Ev0EXAMPLE2622A"
_NEUTRAL_EVENT_ID = "chn-0example-2622-b"

# The Tempo-visible name. AC 3 is a query on this exact string, so it is spelled
# out here as a literal rather than read from the vocabulary: a rename must fail
# this file, not silently follow along.
_EXPORT_KEY = "curie.channel.event_id"


def _provider() -> tuple[TracerProvider, InMemorySpanExporter]:
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    configure_tracer_provider(provider)
    return provider, exporter


def _attributes(exporter: InMemorySpanExporter) -> dict[str, object]:
    (finished,) = exporter.get_finished_spans()
    return dict(finished.attributes or {})


# --- T1: the vocabulary and the exported name --------------------------------


def test_event_id_is_declared_in_the_closed_span_vocabulary() -> None:
    assert PLATFORM_SPAN_ATTRIBUTE_KEYS["event_id"] == _EXPORT_KEY


@pytest.mark.parametrize(
    "event_id",
    [_SLACK_EVENT_ID, _NEUTRAL_EVENT_ID],
    ids=["slack", "neutral"],
)
def test_caller_event_id_exports_under_the_channel_namespace_unmangled(
    event_id: str,
) -> None:
    """THE pin for the Tempo attribute name and for redaction survival.

    ``_closed_attributes`` runs ``redact_span_attribute`` over every value, so a
    redactor that treated id-shaped strings as secrets would scrub the very
    value AC 3 queries on -- fail-closed, but silently unqueryable. Assert the
    id arrives byte-for-byte (no truncation, no case folding: the channel logged
    the raw string and the Tempo query must match it).
    """

    provider, exporter = _provider()
    try:
        with operation_span(
            "curie.queue.process",
            kind=SpanKind.CONSUMER,
            attributes={"service.name": "curie-worker", "event_id": event_id},
        ):
            pass

        attributes = _attributes(exporter)
        assert attributes.get(_EXPORT_KEY) == event_id, attributes
        assert "event_id" not in attributes, "the caller spelling must not reach the wire"
    finally:
        configure_tracer_provider(None)
        provider.shutdown()


def test_event_id_can_be_stamped_post_hoc_on_an_open_span() -> None:
    """``curie.queue.process`` opens before the parse, so AC 1 needs this."""

    provider, exporter = _provider()
    try:
        with operation_span("curie.queue.process", kind=SpanKind.CONSUMER) as span:
            span.set_attribute("event_id", _SLACK_EVENT_ID)

        assert _attributes(exporter).get(_EXPORT_KEY) == _SLACK_EVENT_ID
    finally:
        configure_tracer_provider(None)
        provider.shutdown()


def test_widening_the_vocabulary_by_one_key_leaves_it_closed() -> None:
    with pytest.raises(ValueError, match="undeclared platform span attribute"):
        with operation_span(
            "curie.queue.process",
            kind=SpanKind.CONSUMER,
            attributes={"thread_key": "thread-example"},
        ):
            pass

    with operation_span("curie.queue.process", kind=SpanKind.CONSUMER) as span:
        with pytest.raises(ValueError, match="undeclared platform span attribute"):
            span.set_attribute("thread_key", "thread-example")


# --- T2: contextvar semantics ------------------------------------------------


def test_operation_span_stamps_the_ambient_event_id() -> None:
    """The pin that ``operation_span`` itself calls ``stamp_event_id``.

    Without this the worker suite's T6 would pass even if the wiring inside
    ``operation_span`` were never added, because its probe calls the stamping
    helper directly.
    """

    provider, exporter = _provider()
    try:
        with channel_event_id_scope(_SLACK_EVENT_ID):
            with operation_span("curie.runner.rpc", kind=SpanKind.CLIENT):
                pass

        assert _attributes(exporter).get(_EXPORT_KEY) == _SLACK_EVENT_ID
    finally:
        configure_tracer_provider(None)
        provider.shutdown()


def test_spans_outside_any_scope_omit_the_key_entirely() -> None:
    """Omit, never ``""`` and never ``"None"`` (E4c).

    An empty-string value would make the AC 3 Tempo query match every
    context-free span in the fleet -- worse than no attribute at all.
    """

    provider, exporter = _provider()
    try:
        assert current_channel_event_id() is None
        with operation_span("curie.runner.rpc", kind=SpanKind.CLIENT):
            pass

        attributes = _attributes(exporter)
        assert _EXPORT_KEY not in attributes, attributes
    finally:
        configure_tracer_provider(None)
        provider.shutdown()


def test_scope_restores_the_outer_value_on_exit() -> None:
    assert current_channel_event_id() is None
    with channel_event_id_scope(_SLACK_EVENT_ID):
        assert current_channel_event_id() == _SLACK_EVENT_ID
        with channel_event_id_scope(_NEUTRAL_EVENT_ID):
            assert current_channel_event_id() == _NEUTRAL_EVENT_ID
        # Reset BY TOKEN, not by re-setting a remembered value: a bare ``.set()``
        # leaks a stale id onto the next entry handled by a reused worker task,
        # which is silent wrong correlation (E4b).
        assert current_channel_event_id() == _SLACK_EVENT_ID
    assert current_channel_event_id() is None


def test_scope_resets_even_when_the_body_raises() -> None:
    with pytest.raises(RuntimeError, match="boom"):
        with channel_event_id_scope(_SLACK_EVENT_ID):
            raise RuntimeError("boom")
    assert current_channel_event_id() is None


def test_caller_supplied_event_id_wins_over_the_ambient_one() -> None:
    with channel_event_id_scope(_SLACK_EVENT_ID):
        assert stamp_event_id({"event_id": _NEUTRAL_EVENT_ID}) == {
            "event_id": _NEUTRAL_EVENT_ID
        }
        assert stamp_event_id({"service.name": "curie-worker"}) == {
            "service.name": "curie-worker",
            "event_id": _SLACK_EVENT_ID,
        }
        assert stamp_event_id(None) == {"event_id": _SLACK_EVENT_ID}


def test_stamping_outside_a_scope_is_the_identity() -> None:
    assert stamp_event_id(None) == {}
    assert stamp_event_id({"service.name": "curie-worker"}) == {
        "service.name": "curie-worker"
    }


def test_stamping_does_not_mutate_the_callers_dict() -> None:
    """``consumer.py`` reuses ONE dict for its span and its metrics."""

    supplied = {"service.name": "curie-worker", "source": "worker"}
    with channel_event_id_scope(_SLACK_EVENT_ID):
        stamped = stamp_event_id(supplied)
    assert supplied == {"service.name": "curie-worker", "source": "worker"}
    assert stamped["event_id"] == _SLACK_EVENT_ID


# --- T3: the metric surface stays closed to it -------------------------------


def test_metrics_never_carry_the_event_id_inside_a_scope() -> None:
    """The regression pin for the shared-dict trap in ``consumer.py``.

    The same attribute dict feeds ``operation_span`` and ``record_metric`` there
    (and again in ``reply_sink.py``). Stamping lives only in the span path, so
    the metric point must be byte-identical to what the caller supplied.
    """

    reader = InMemoryMetricReader()
    provider = MeterProvider(
        metric_readers=[reader],
        resource=build_resource(
            "curie-worker",
            service_version="0.0.0",
            service_instance_id="event-id-scope",
            deployment_environment="test",
        ),
    )
    configure_meter_provider(provider)
    try:
        supplied = {
            "service.name": "curie-worker",
            "source": "worker",
            "outcome": "success",
        }
        with channel_event_id_scope(_SLACK_EVENT_ID):
            record_metric("curie.queue.process", attributes=supplied)

        data = reader.get_metrics_data()
        assert data is not None
        points = [
            dict(point.attributes or {})
            for resource_metrics in data.resource_metrics
            for scope_metrics in resource_metrics.scope_metrics
            for metric in scope_metrics.metrics
            if metric.name == "curie.queue.process"
            for point in getattr(metric.data, "data_points", ())
        ]
        assert points, "the metric under test did not record"
        for point in points:
            assert point == supplied, point
            assert _EXPORT_KEY not in point
            assert _SLACK_EVENT_ID not in {str(value) for value in point.values()}
    finally:
        provider.shutdown()


def test_record_metric_rejects_the_event_id_as_an_undeclared_attribute() -> None:
    """Belt and braces: even a deliberate future attempt fails loudly."""

    with pytest.raises(ValueError, match="undeclared attribute"):
        record_metric(
            "curie.queue.process",
            attributes={
                "service.name": "curie-worker",
                "source": "worker",
                "outcome": "success",
                "event_id": _SLACK_EVENT_ID,
            },
        )
