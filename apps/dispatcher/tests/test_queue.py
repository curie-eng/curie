"""Queue seam + dedupe against real Valkey."""

import json
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

import redis
from aci_protocol import QueuedTurn, ReplyHandle
from curie_dispatcher import queue as queue_module
from curie_dispatcher.config import DispatcherConfig
from curie_dispatcher.queue import (
    claim_event,
    enqueue,
    from_stream_fields,
    to_stream_fields,
)
from curie_telemetry import TRACEPARENT_STREAM_FIELD, extract_trace_context
from opentelemetry import context as otel_context
from opentelemetry import trace
from opentelemetry.trace import NonRecordingSpan, SpanContext, TraceFlags, TraceState

# The committed wire golden the Rust CLI consumer (cli/src/queue.rs) round-trips too.
_GOLDEN_FIXTURE = (
    Path(__file__).resolve().parents[3]
    / "packages"
    / "aci-protocol"
    / "schema"
    / "queued-turn.fixture.json"
)

_TRACE_ID = int("0123456789abcdef0123456789abcdef", 16)
_SPAN_ID = int("0123456789abcdef", 16)
_TRACEPARENT = "00-0123456789abcdef0123456789abcdef-0123456789abcdef-01"


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


def _event(event_id: str = "Ev1") -> QueuedTurn:
    return QueuedTurn(
        event_id=event_id,
        conversation_id="123.45",
        author="U1",
        text="hello",
        reply_handle=ReplyHandle(kind="slack", channel="C1", placeholder="999.00"),
        received_at="2026-07-05T00:00:00+00:00",
    )


def test_queued_turn_stream_fields_roundtrip() -> None:
    event = _event()
    fields = to_stream_fields(event)
    # The worker (F1) consumes a single JSON payload field.
    assert set(fields) == {"payload"}
    assert from_stream_fields(fields) == event


def test_from_stream_fields_tolerates_unknown_payload_field() -> None:
    # A payload written by a newer producer (image skew across the queue boundary)
    # carries a field this build does not model. The consumer must decode it
    # tolerantly rather than raising, so a mid-deploy skew does not drop turns.
    event = _event()
    payload = json.loads(event.model_dump_json())
    payload["future_field"] = "from a newer image"
    fields = {"payload": json.dumps(payload)}

    assert from_stream_fields(fields) == event


def test_traceparent_is_transport_owned_beside_byte_identical_payload() -> None:
    """The carrier is Stream transport metadata, never a QueuedTurn field."""

    event = _event("Ev-traced")
    with _remote_parent():
        fields = to_stream_fields(event)

    assert fields == {
        "payload": event.model_dump_json(),
        TRACEPARENT_STREAM_FIELD: _TRACEPARENT,
    }
    assert from_stream_fields(fields) == event
    extracted = trace.get_current_span(extract_trace_context(fields)).get_span_context()
    assert extracted.trace_id == _TRACE_ID
    assert extracted.span_id == _SPAN_ID
    assert extracted.is_remote is True


def test_removing_dispatcher_injection_produces_a_safe_root_not_a_false_parent(
    monkeypatch,
) -> None:
    """Falsifiable control: severing the producer removes only causality.

    The payload still deserializes and the consumer receives an invalid parent
    context, which is the signal to start a safe root. This test must stay beside
    the positive injection test so a carrierless green path cannot mask a broken
    dispatcher producer.
    """

    monkeypatch.setattr(queue_module, "inject_trace_context", lambda _carrier: None)
    event = _event("Ev-carrierless")
    with _remote_parent():
        fields = to_stream_fields(event)

    assert fields == {"payload": event.model_dump_json()}
    assert from_stream_fields(fields) == event
    extracted = trace.get_current_span(extract_trace_context(fields)).get_span_context()
    assert extracted.is_valid is False


def test_queued_turn_matches_cross_language_golden() -> None:
    # One committed wire fixture pins the QueuedTurn bytes across the Python
    # producer here and the Rust consumer in cli/src/queue.rs: both must
    # deserialize it and re-serialize to identical bytes. Guards the frozen queue
    # payload against silent field rename/reorder drift. The model itself now
    # lives in packages/aci-protocol (#7); this test pins its wire bytes.
    raw = _GOLDEN_FIXTURE.read_text().strip()
    event = QueuedTurn.model_validate_json(raw)
    assert event.event_id == "Ev0GOLDEN0001"
    assert event.model_dump_json() == raw


def test_enqueue_writes_one_stream_entry_the_worker_can_read(
    redis_client: redis.Redis, config: DispatcherConfig
) -> None:
    event = _event()
    stream_id = enqueue(redis_client, config, event)

    assert redis_client.xlen(config.stream) == 1
    entries = redis_client.xrange(config.stream)
    entry_id, fields = entries[0]
    assert entry_id == stream_id
    # Round-trips through the wire back into the model the worker reconstructs.
    assert from_stream_fields(fields) == event


def test_claim_event_is_first_writer_wins(
    redis_client: redis.Redis, config: DispatcherConfig
) -> None:
    assert claim_event(redis_client, config, "Ev-dedupe") is True
    # A second (retried) delivery of the same event id is rejected.
    assert claim_event(redis_client, config, "Ev-dedupe") is False
    # A different event id is unaffected.
    assert claim_event(redis_client, config, "Ev-other") is True


def test_claim_event_sets_a_ttl_so_the_guard_is_bounded(
    redis_client: redis.Redis, config: DispatcherConfig
) -> None:
    claim_event(redis_client, config, "Ev-ttl")
    ttl = redis_client.ttl(config.dedupe_key("Ev-ttl"))
    assert 0 < ttl <= config.dedupe_ttl_seconds


def test_dedupe_decision_is_observed_without_exporting_the_event_id(
    redis_client: redis.Redis,
    config: DispatcherConfig,
    monkeypatch,
) -> None:
    calls: list[tuple[str, dict[str, str], list[tuple[str, dict[str, str]]]]] = []

    class Span:
        def __init__(self, events: list[tuple[str, dict[str, str]]]) -> None:
            self.events = events

        def add_event(self, name: str, attributes: dict[str, str]) -> None:
            self.events.append((name, attributes))

        def set_status(self, _status) -> None:
            pass

    @contextmanager
    def capture(name: str, *, kind, attributes: dict[str, str]):
        del kind
        events: list[tuple[str, dict[str, str]]] = []
        calls.append((name, attributes, events))
        yield Span(events)

    monkeypatch.setattr(queue_module, "operation_span", capture)

    assert claim_event(redis_client, config, "Ev-private-example") is True
    assert claim_event(redis_client, config, "Ev-private-example") is False

    assert calls == [
        (
            "curie.queue.dedupe",
            {"service.name": "curie-dispatcher", "source": "dispatcher"},
            [("queue.dedupe.decided", {"outcome": "claimed"})],
        ),
        (
            "curie.queue.dedupe",
            {"service.name": "curie-dispatcher", "source": "dispatcher"},
            [("queue.dedupe.decided", {"outcome": "duplicate"})],
        ),
    ]
    assert "Ev-private-example" not in repr(calls)
