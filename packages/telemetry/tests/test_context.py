"""W3C propagation helpers keep the transport carrier outside frozen payloads."""

from __future__ import annotations

import json
from pathlib import Path

from curie_telemetry import (
    TRACEPARENT_STREAM_FIELD,
    extract_trace_context,
    inject_trace_context,
)
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider

_VECTOR = (
    Path(__file__).parents[3] / "tests" / "vectors" / "telemetry-transport.json"
)


def test_traceparent_stream_field_matches_the_shared_vector() -> None:
    vector = json.loads(_VECTOR.read_text())
    assert TRACEPARENT_STREAM_FIELD == vector["traceparent_stream_field"]
    assert TRACEPARENT_STREAM_FIELD == "traceparent"


def test_inject_and_extract_round_trip_the_current_w3c_context() -> None:
    provider = TracerProvider()
    tracer = provider.get_tracer("curie-telemetry-tests")
    carrier: dict[str, str] = {}

    with tracer.start_as_current_span("producer") as producer:
        producer_context = producer.get_span_context()
        inject_trace_context(carrier)

    assert set(carrier) == {TRACEPARENT_STREAM_FIELD}
    extracted = trace.get_current_span(extract_trace_context(carrier)).get_span_context()
    assert extracted.is_remote
    assert extracted.trace_id == producer_context.trace_id
    assert extracted.span_id == producer_context.span_id


def test_missing_or_malformed_traceparent_extracts_a_safe_root_context() -> None:
    for carrier in ({}, {TRACEPARENT_STREAM_FIELD: "not-a-traceparent"}):
        extracted = trace.get_current_span(extract_trace_context(carrier)).get_span_context()
        assert not extracted.is_valid
