"""A queued transport receipt can precede its durable SQL queued mark."""

import copy
import json
from collections.abc import Iterator

import pytest
from curie_telemetry import tracing as telemetry_tracing
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.trace import SpanKind
from sqlalchemy.exc import DBAPIError

from apps.api.tests.test_github_review_events import HEAD, post_review, review_rows
from apps.api.tests.test_github_review_events import review_app_key as review_app_key
from apps.api.tests.test_github_review_events import review_stack as review_stack

STORED_PARENT = "00-3123456789abcdef0123456789abcdef-3123456789abcdef-01"


@pytest.fixture
def review_spans(
    monkeypatch: pytest.MonkeyPatch,
) -> Iterator[tuple[TracerProvider, InMemorySpanExporter]]:
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    monkeypatch.setattr(telemetry_tracing, "_tracer", provider.get_tracer("curie-telemetry"))
    try:
        yield provider, exporter
    finally:
        provider.shutdown()


@pytest.mark.parametrize(
    "stored", [STORED_PARENT, None, "invalid"], ids=["stored", "absent", "invalid"]
)
def test_review_enqueue_span_uses_the_canonical_stored_parent(
    review_stack,
    review_spans: tuple[TracerProvider, InMemorySpanExporter],
    stored: str | None,
) -> None:
    client, truth, valkey, stream = review_stack
    provider, exporter = review_spans
    exporter.clear()
    if stored is not None:
        client.headers["traceparent"] = f"  {stored}  "
    try:
        with provider.get_tracer("test-maintenance").start_as_current_span("test.unrelated"):
            first = post_review(client, truth)
            assert first.status_code == 200, first.text
            assert first.json()["status"] == "feedback_queued"
            duplicate = post_review(client, truth)
            assert duplicate.status_code == 200, duplicate.text
            assert duplicate.json()["status"] == "feedback_duplicate"
    finally:
        client.headers.pop("traceparent", None)
    entries = valkey.xrange(stream)
    assert len(entries) == 1
    carrier = entries[0][1]["traceparent"].split("-")
    spans = exporter.get_finished_spans()
    enqueue = [span for span in spans if span.name == "curie.queue.enqueue"]
    assert len(enqueue) == 1
    assert enqueue[0].kind is SpanKind.PRODUCER
    assert enqueue[0].attributes["curie.source"] == "api"
    assert enqueue[0].context.trace_id == int(carrier[1], 16)
    assert enqueue[0].context.span_id == int(carrier[2], 16)
    if stored == STORED_PARENT:
        assert enqueue[0].parent.span_id == int(STORED_PARENT.split("-")[2], 16)
        assert enqueue[0].context.trace_id == int(STORED_PARENT.split("-")[1], 16)
    else:
        assert enqueue[0].parent is None
    assert review_rows("SELECT traceparent FROM curie.github_review_feedback") == [
        {"traceparent": STORED_PARENT if stored == STORED_PARENT else None}
    ]


@pytest.mark.parametrize("stored", [STORED_PARENT, None], ids=["stored", "absent"])
def test_review_enqueue_without_a_tracer_preserves_only_stored_context(
    review_stack, monkeypatch: pytest.MonkeyPatch, stored: str | None,
) -> None:
    client, truth, valkey, stream = review_stack
    monkeypatch.setattr(
        telemetry_tracing, "_tracer", trace.NoOpTracerProvider().get_tracer("test")
    )
    if stored is not None:
        client.headers["traceparent"] = stored
    try:
        first = post_review(client, truth)
        assert first.status_code == 200, first.text
        assert first.json()["status"] == "feedback_queued"
    finally:
        client.headers.pop("traceparent", None)
    entries = valkey.xrange(stream)
    assert len(entries) == 1
    fields = entries[0][1]
    assert fields.get("traceparent") == stored
    assert set(fields) == ({"payload", "traceparent"} if stored else {"payload"})
    assert json.loads(fields["payload"]) == review_rows(
        "SELECT turn FROM curie.github_review_feedback"
    )[0]["turn"]


@pytest.mark.parametrize("operation", ["verify", "reserve"])
def test_waiting_outbox_is_retryable_until_exact_queue_receipt_is_reconciled(
    review_stack, operation,
):
    client, truth, valkey, stream = review_stack
    review_rows("""
        CREATE FUNCTION curie.review_pending_reject() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
          IF NEW.status = 'queued' THEN RAISE EXCEPTION 'task-owned queued-mark failure'; END IF;
          RETURN NEW;
        END $$
    """)
    review_rows("""
        CREATE TRIGGER review_pending_reject BEFORE UPDATE ON curie.github_review_feedback
        FOR EACH ROW EXECUTE FUNCTION curie.review_pending_reject()
    """)
    try:
        with pytest.raises(DBAPIError):
            post_review(client, truth)
        rows = valkey.xrange(stream)
        assert len(rows) == 1
        stream_id, fields = rows[0]
        turn = json.loads(fields["payload"])
        lineage = review_rows(
            "SELECT deployment_id,version FROM curie.thread_publication_lineages"
        )[0]
        payload = {"turn": turn, "deployment_id": str(lineage["deployment_id"])}
        if operation == "reserve":
            payload.update(expected_lineage_version=lineage["version"], expected_head_sha=HEAD)
        headers = {"X-Curie-Worker-Token": "fixture-review-worker-token"}
        path = f"/v1/internal/github/reviews/{turn['event_id']}/{operation}"
        forged = copy.deepcopy(payload)
        forged["turn"]["author"] = "U0REQUEST1"
        rejected = client.post(path, json=forged, headers=headers)
        assert rejected.status_code == 409
        assert rejected.json()["detail"]["code"] == "feedback_turn_mismatch"
        calls = len(truth.calls)
        waiting = client.post(path, json=payload, headers=headers)
        assert waiting.status_code == 503, waiting.text
        assert waiting.json()["detail"]["code"] == "feedback_outbox_pending"
        assert waiting.headers["cache-control"] == "no-store"
        assert len(truth.calls) == calls  # No provider read or model authority while pending.
        assert review_rows(
            "SELECT status,version,error_code FROM curie.github_review_feedback"
        ) == [
            {"status": "waiting", "version": 1, "error_code": None}
        ]
        assert review_rows(
            "SELECT status FROM curie.github_review_deliveries"
        ) == [{"status": "accepted"}]
        assert review_rows("SELECT id FROM curie.publication_review_reservations") == []
    finally:
        review_rows("DROP TRIGGER review_pending_reject ON curie.github_review_feedback")
        review_rows("DROP FUNCTION curie.review_pending_reject()")
    duplicate = post_review(client, truth)
    assert duplicate.status_code == 200, duplicate.text
    assert duplicate.json()["status"] == "feedback_duplicate"
    assert valkey.xrange(stream) == [(stream_id, fields)]
    assert review_rows("SELECT status FROM curie.github_review_feedback") == [
        {"status": "queued"}
    ]
    ready = client.post(path, json=payload, headers=headers)
    assert ready.status_code == 200, ready.text
    assert ready.json()["origin_key"] == turn["event_id"]
    assert review_rows("SELECT status FROM curie.github_review_feedback") == [
        {"status": "reserved" if operation == "reserve" else "queued"}
    ]
    reservations = review_rows(
        "SELECT origin_key,status FROM curie.publication_review_reservations"
    )
    assert reservations == (
        [{"origin_key": turn["event_id"], "status": "reserved"}]
        if operation == "reserve" else []
    )
