"""Causal worker telemetry and bounded operational metrics (#1817/#1818)."""

from __future__ import annotations

import asyncio
import threading
import time
import uuid
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

import pytest
from aci_protocol import (
    ErrorEvent,
    Event,
    Final,
    QueuedTurn,
    ReplyHandle,
    SessionStatus,
    SideEffectFlag,
    TextDelta,
)
from curie_telemetry import (
    TRACEPARENT_STREAM_FIELD,
    extract_trace_context,
    inject_trace_context,
    operation_span,
    record_metric,
    stamp_event_id,
)
from curie_worker import consumer as consumer_module
from curie_worker import kernel as kernel_module
from curie_worker import runner_client as runner_client_module
from curie_worker import stream_consumer as stream_consumer_module
from curie_worker import threadlock as threadlock_module
from curie_worker.approvals import ApprovalRequest, CreatedApproval
from curie_worker.consumer import Consumer
from curie_worker.reply_sink import TargetRoute
from curie_worker.sandbox import substrate as substrate_module
from opentelemetry import context as otel_context
from opentelemetry import trace
from opentelemetry.trace import (
    NonRecordingSpan,
    SpanContext,
    SpanKind,
    StatusCode,
    TraceFlags,
    TraceState,
)

DONE = SessionStatus.DONE
AWAITING = SessionStatus.AWAITING_APPROVAL
_REMOTE_TRACE_ID = int("3123456789abcdef0123456789abcdef", 16)
_REMOTE_SPAN_ID = int("3123456789abcdef", 16)
_TRACEPARENT = "00-3123456789abcdef0123456789abcdef-3123456789abcdef-01"
_BOUNDED_KEYS = {
    "service.name",
    "operation",
    "role",
    "source",
    "outcome",
    "retry_class",
}


@dataclass(frozen=True)
class _Metric:
    name: str
    value: float
    attributes: dict[str, str]


@dataclass
class _SpanCall:
    name: str
    kind: Any
    parent_trace_id: int
    parent_span_id: int
    span_id: int
    attributes: dict[str, str]
    events: list[tuple[str, dict[str, str]]] = field(default_factory=list)
    status: Any = None


class _ProbeSpan:
    def __init__(self, call: _SpanCall) -> None:
        self._call = call

    def add_event(
        self, name: str, attributes: Mapping[str, str] | None = None, **_kwargs: Any
    ) -> None:
        self._call.events.append((name, dict(attributes or {})))

    def set_attribute(self, name: str, value: Any) -> None:
        self._call.attributes[name] = str(value)

    def record_exception(self, _exc: BaseException) -> None:
        pass

    def set_status(self, status: Any, _description: str | None = None) -> None:
        self._call.status = status


class _Probe:
    def __init__(self) -> None:
        self.spans: list[_SpanCall] = []
        self.metrics: list[_Metric] = []
        self._next_span_id = 0x4000000000000000
        # operation_span is called from asyncio.to_thread worker threads (the
        # sandbox claim/release hops in kernel.py), not just the event loop.
        # Without a lock, two threads can read the same _next_span_id and mint
        # duplicate span ids, which collapses T6's {span_id: call} root-join map.
        self._lock = threading.Lock()

    @contextmanager
    def operation_span(
        self,
        name: str,
        *,
        kind: Any,
        parent: Any = None,
        attributes: Mapping[str, str] | None = None,
    ) -> Iterator[_ProbeSpan]:
        parent_span = trace.get_current_span(parent).get_span_context()
        if parent is None:
            parent_span = trace.get_current_span().get_span_context()
        trace_id = parent_span.trace_id if parent_span.is_valid else _REMOTE_TRACE_ID + 1
        with self._lock:
            span_id = self._next_span_id
            self._next_span_id += 1
            call = _SpanCall(
                name=name,
                kind=kind,
                parent_trace_id=parent_span.trace_id,
                parent_span_id=parent_span.span_id,
                span_id=span_id,
                # The REAL stamping helper, not a reimplementation: this probe
                # replaces ``operation_span`` wholesale, so anything reimplemented
                # here would be tested instead of the production code path.
                attributes=dict(stamp_event_id(attributes)),
            )
            self.spans.append(call)
        child = SpanContext(
            trace_id=trace_id,
            span_id=span_id,
            is_remote=False,
            trace_flags=TraceFlags.SAMPLED,
            trace_state=TraceState(),
        )
        token = otel_context.attach(trace.set_span_in_context(NonRecordingSpan(child)))
        try:
            yield _ProbeSpan(call)
        finally:
            otel_context.detach(token)

    def record_metric(
        self,
        name: str,
        value: float = 1,
        *,
        attributes: Mapping[str, str] | None = None,
    ) -> None:
        with self._lock:
            self.metrics.append(_Metric(name, float(value), dict(attributes or {})))


def _install(monkeypatch: pytest.MonkeyPatch) -> _Probe:
    """Capture direct imports and module-qualified shared API calls alike."""

    import curie_telemetry

    probe = _Probe()
    monkeypatch.setattr(curie_telemetry, "operation_span", probe.operation_span)
    monkeypatch.setattr(curie_telemetry, "record_metric", probe.record_metric)
    for module in (
        consumer_module,
        stream_consumer_module,
        kernel_module,
        threadlock_module,
        runner_client_module,
        substrate_module,
    ):
        if hasattr(module, "operation_span"):
            monkeypatch.setattr(module, "operation_span", probe.operation_span)
        if hasattr(module, "record_metric"):
            monkeypatch.setattr(module, "record_metric", probe.record_metric)
    return probe


def _qevent(
    text: str,
    *,
    thread: str = "thread-otel",
    event_id: str | None = None,
) -> QueuedTurn:
    return QueuedTurn(
        event_id=event_id or uuid.uuid4().hex,
        conversation_id=thread,
        author="U1",
        text=text,
        reply_handle=ReplyHandle(kind="slack", channel="C0EXAMPLE1", placeholder="p-1"),
        received_at=datetime.now(UTC).isoformat(),
    )


async def _deliver(consumer: Consumer, h, fields: dict[str, str]) -> str:
    entry_id = await h.async_redis.xadd(h.config.stream, fields)
    rows = await h.async_redis.xreadgroup(
        h.config.consumer_group,
        h.config.consumer_name,
        {h.config.stream: ">"},
        count=1,
    )
    assert rows and rows[0][1]
    delivered_id, delivered_fields = rows[0][1][0]
    assert delivered_id == entry_id
    await consumer._dispatch(delivered_id, delivered_fields)
    await asyncio.gather(*list(consumer._inflight))
    return entry_id


def _metrics(probe: _Probe, name: str) -> list[_Metric]:
    return [point for point in probe.metrics if point.name == name]


def _spans(probe: _Probe, name: str) -> list[_SpanCall]:
    return [span for span in probe.spans if span.name == name]


@pytest.mark.parametrize(
    "carrier",
    [None, "not-a-valid-traceparent"],
    ids=["missing", "malformed"],
)
def test_missing_or_malformed_carrier_runs_and_acks_under_a_safe_root(
    make_harness,
    monkeypatch: pytest.MonkeyPatch,
    carrier: str | None,
) -> None:
    async def go() -> None:
        probe = _install(monkeypatch)
        async with make_harness() as h:
            h.runner.default_script = [Final(text="safe", status=DONE)]
            consumer = Consumer(redis=h.async_redis, kernel=h.kernel, config=h.config)
            await consumer.ensure_group()
            fields = {"payload": _qevent("safe root").model_dump_json()}
            if carrier is not None:
                fields[TRACEPARENT_STREAM_FIELD] = carrier

            await _deliver(consumer, h, fields)
            # Inventory is sampled by the maintenance cadence, not while the
            # message handler still owns a concurrency slot.
            await consumer._observe_queue_state()

            assert h.runner.opened == ["safe root"]
            pending = await h.async_redis.xpending(
                h.config.stream, h.config.consumer_group
            )
            assert pending["pending"] == 0
            process = _spans(probe, "curie.queue.process")
            assert len(process) == 1
            assert process[0].parent_trace_id == 0
            assert process[0].parent_span_id == 0
            turn_process = _spans(probe, "curie.turn.process")
            assert turn_process[-1].attributes["outcome"] == "done"
            assert turn_process[-1].status is StatusCode.OK
            assert _metrics(probe, "curie.queue.settle")[-1].attributes["outcome"] == "ack"
            for name in (
                "curie.queue.wait.duration",
                "curie.queue.process.duration",
                "curie.queue.message.age",
            ):
                points = _metrics(probe, name)
                assert points and all(point.value >= 0 for point in points)
            for name in (
                "curie.queue.pending",
                "curie.queue.lag",
                "curie.queue.depth",
            ):
                assert _metrics(probe, name)
            assert {
                point.attributes["outcome"]
                for point in _metrics(probe, "curie.turn.accepted")
            } == {"accepted"}
            assert {
                point.attributes["outcome"]
                for point in _metrics(probe, "curie.turn.completed")
            } == {"done"}
            for point in probe.metrics:
                assert set(point.attributes) <= _BOUNDED_KEYS

    asyncio.run(go())


def test_platform_completion_reply_is_observed_once_at_the_kernel_sink_seam(
    make_harness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A non-stream completion crosses the same observed reply boundary once."""

    async def go() -> None:
        probe = _install(monkeypatch)
        async with make_harness() as h:
            event = _qevent("completion telemetry", thread="thread-completion-telemetry")

            await h.kernel._complete(
                event,
                TargetRoute(),
                "delivered",
                telemetry_outcome="done",
            )

            delivery = _metrics(probe, "curie.reply.delivery")
            assert len(delivery) == 1
            assert delivery[0].attributes == {
                "service.name": "curie-worker",
                "operation": "update",
                "role": "client",
                "outcome": "success",
            }
            spans = _spans(probe, "curie.reply.update")
            assert len(spans) == 1
            assert spans[0].attributes == {
                "service.name": "curie-worker",
                "operation": "update",
                "role": "client",
            }
            assert [reply.event for reply, _route, _best_effort in h.sink.events] == [
                "turn.completed"
            ]

    asyncio.run(go())


@pytest.mark.parametrize(
    ("classification", "terminal_outcome"),
    [
        ("model-error", "classified_failure"),
        ("budget-exceeded", "budget_halted"),
    ],
)
def test_turn_process_span_exports_bounded_terminal_failures_as_error(
    make_harness,
    monkeypatch: pytest.MonkeyPatch,
    classification: str,
    terminal_outcome: str,
) -> None:
    async def go() -> None:
        probe = _install(monkeypatch)
        async with make_harness(max_attempts=1) as h:
            h.runner.default_script = [
                ErrorEvent(message="failed", classification=classification),
                Final(text="failed", status=SessionStatus.CLASSIFIED_FAILURE),
            ]
            await h.kernel.process_event(_qevent("fail", thread=f"thread-{classification}"))

            span = _spans(probe, "curie.turn.process")[-1]
            assert span.attributes["outcome"] == terminal_outcome
            assert span.status is StatusCode.ERROR
            assert ("turn.processing.completed", {"outcome": terminal_outcome}) in span.events

    asyncio.run(go())


def test_side_effect_failure_has_its_own_terminal_metric_and_span_class(
    make_harness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def go() -> None:
        probe = _install(monkeypatch)
        async with make_harness(max_attempts=3) as h:
            h.runner.default_script = [
                SideEffectFlag(tool="deploy"),
                ErrorEvent(message="failed", classification="runner-error"),
                Final(text="failed", status=SessionStatus.CLASSIFIED_FAILURE),
            ]
            await h.kernel.process_event(_qevent("act", thread="thread-side-effect"))

            assert h.runner.opened == ["act"], "a side effect must prevent automatic retry"
            span = _spans(probe, "curie.turn.process")[-1]
            assert span.attributes["outcome"] == "side_effect_halted"
            assert span.status is StatusCode.ERROR
            assert {
                point.attributes["outcome"]
                for point in _metrics(probe, "curie.turn.completed")
            } == {"side_effect_halted"}

    asyncio.run(go())


def test_worker_process_parent_flows_to_exact_runner_http_client_boundary(
    make_harness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Exact continuity stops at the client boundary; runner internals are separate."""

    async def go() -> None:
        probe = _install(monkeypatch)
        async with make_harness() as h:
            h.runner.default_script = [Final(text="traced", status=DONE)]
            consumer = Consumer(redis=h.async_redis, kernel=h.kernel, config=h.config)
            await consumer.ensure_group()
            event = _qevent("trace me", event_id="Ev0EXAMPLETRACE1")
            fields = {
                "payload": event.model_dump_json(),
                TRACEPARENT_STREAM_FIELD: _TRACEPARENT,
            }

            await _deliver(consumer, h, fields)

            process = _spans(probe, "curie.queue.process")
            assert len(process) == 1
            assert process[0].parent_trace_id == _REMOTE_TRACE_ID
            assert process[0].parent_span_id == _REMOTE_SPAN_ID

            rpc = _spans(probe, "curie.runner.rpc")
            assert rpc, "the HTTP client boundary must own a CLIENT span"
            headers = {key.lower(): value for key, value in h.runner.event_headers[-1].items()}
            header = headers["traceparent"]
            version, trace_hex, parent_hex, flags = header.split("-")
            assert version == "00" and flags == "01"
            assert int(trace_hex, 16) == _REMOTE_TRACE_ID
            assert int(parent_hex, 16) == rpc[-1].span_id
            durations = _metrics(probe, "curie.runner.rpc.request.duration")
            results = _metrics(probe, "curie.runner.rpc.result")
            assert durations and all(point.value >= 0 for point in durations)
            assert any(
                point.attributes.get("operation") == "event"
                and point.attributes.get("outcome") == "success"
                for point in results
            )

    asyncio.run(go())


def test_queue_success_retry_and_dead_letter_emit_bounded_outcomes_and_keep_carrier(
    make_harness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def go() -> None:
        probe = _install(monkeypatch)
        async with make_harness(max_delivery=2, reclaim_min_idle_ms=0) as h:
            consumer = Consumer(redis=h.async_redis, kernel=h.kernel, config=h.config)
            await consumer.ensure_group()

            async def fail(_turn: QueuedTurn) -> None:
                raise RuntimeError("injected processing failure")

            h.kernel.process_event = fail  # type: ignore[method-assign]
            event = _qevent("poison", event_id="Ev0EXAMPLEPOISON1")
            fields = {
                "payload": event.model_dump_json(),
                TRACEPARENT_STREAM_FIELD: _TRACEPARENT,
            }
            entry_id = await _deliver(consumer, h, fields)
            await consumer._reclaim_once()
            await asyncio.gather(*list(consumer._inflight))
            await consumer._reclaim_once()
            await asyncio.gather(*list(consumer._inflight))

            dead = await h.async_redis.xrange(h.config.dead_letter_stream_name())
            assert len(dead) == 1
            graveyard = dead[0][1]
            assert graveyard["dl_original_id"] == entry_id
            assert graveyard["payload"] == event.model_dump_json()
            assert graveyard[TRACEPARENT_STREAM_FIELD] == _TRACEPARENT

            settle_outcomes = {
                point.attributes["outcome"]
                for point in _metrics(probe, "curie.queue.settle")
            }
            assert settle_outcomes >= {
                "pending",
                "dead-letter",
            }
            assert {
                point.attributes["retry_class"]
                for point in _metrics(probe, "curie.queue.retry")
            } == {"redelivery"}
            assert {
                point.attributes["outcome"]
                for point in _metrics(probe, "curie.queue.dead_letter")
            } == {
                "success",
            }
            for point in probe.metrics:
                assert set(point.attributes) <= _BOUNDED_KEYS
                assert event.event_id not in point.attributes.values()
                assert event.conversation_id not in point.attributes.values()
            await h.async_redis.delete(h.config.dead_letter_stream_name())

    asyncio.run(go())


def test_turn_lifecycle_covers_lock_start_steer_reply_and_retry(
    make_harness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def go() -> None:
        probe = _install(monkeypatch)
        async with make_harness() as h:
            hold = asyncio.Event()
            h.runner.hold = hold
            h.runner.default_script = [TextDelta(text="working")]
            h.runner.tail = [Final(text="done", status=DONE)]

            first_event = _qevent("first", thread="thread-route")
            first = asyncio.create_task(h.kernel.process_event(first_event))
            deadline = time.monotonic() + 5
            while not h.runner.turn_active and time.monotonic() < deadline:
                await asyncio.sleep(0.01)
            assert h.runner.turn_active
            await h.kernel.process_event(_qevent("second", thread="thread-route"))
            hold.set()
            await first
            h.runner.hold = None
            h.runner.tail = []

            h.runner.default_script = [Final(text="fresh", status=DONE)]
            await h.kernel.process_event(_qevent("third", thread="thread-route"))

            h.runner.turn_scripts = [
                [
                    ErrorEvent(message="limited", classification="rate-limit"),
                    Final(text="limited", status=SessionStatus.CLASSIFIED_FAILURE),
                ],
                [Final(text="recovered", status=DONE)],
            ]
            await h.kernel.process_event(_qevent("retry", thread="thread-retry"))
            await h.kernel.reap_orphans()
            await h.kernel.release_thread(kernel_module._thread_key_for(first_event))
            await h.kernel.reap_orphans()

            route = {p.attributes["outcome"] for p in _metrics(probe, "curie.thread.route")}
            assert route >= {"start", "steer"}
            assert "finish-race" not in route
            lock_wait = _metrics(probe, "curie.thread.lock.wait.duration")
            assert lock_wait and all(p.value >= 0 for p in lock_wait)
            assert {p.attributes["outcome"] for p in lock_wait} == {"acquired"}
            assert {p.attributes["outcome"] for p in _metrics(probe, "curie.reply.delivery")} <= {
                "success",
                "best-effort",
            }
            assert _metrics(probe, "curie.reply.delivery")
            assert {
                p.attributes["retry_class"]
                for p in _metrics(probe, "curie.queue.retry")
            } >= {"rate-limit"}
            assert not _metrics(probe, "curie.reply.retry"), (
                "a model rate-limit retry is a queue retry, not a reply-sink retry"
            )
            # thread-route and thread-retry remain live sibling routes after
            # their turns complete. Releasing only thread-route must report one,
            # not the per-event last value zero that erases thread-retry.
            active_routes = _metrics(probe, "curie.thread.route.active")
            assert max(point.value for point in active_routes) == 2
            assert active_routes[-1].value == 1

            event_names = {
                event for span in probe.spans for event, _attributes in span.events
            }
            assert {
                "thread.lock.acquired",
                "runner.turn.started",
                "runner.turn.steered",
            } <= event_names
            assert "runner.finish_race" not in event_names

    asyncio.run(go())


def test_fresh_claim_failed_steer_is_not_reported_as_a_finish_race(
    make_harness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def go() -> None:
        probe = _install(monkeypatch)
        async with make_harness() as h:
            h.runner.default_script = [Final(text="fresh", status=DONE)]

            await h.kernel.process_event(_qevent("first", thread="thread-fresh-route"))

            # A new runner has no active turn, so its first steer probe is an
            # expected 409 before start_turn. That expected bootstrap probe is
            # not the existing-live-route finish race operators need to count.
            assert len(h.runner.steer_headers) == 1
            assert h.runner.opened == ["first"]
            route_outcomes = [
                point.attributes["outcome"]
                for point in _metrics(probe, "curie.thread.route")
            ]
            assert route_outcomes == ["start"]
            lifecycle_events = [
                name for span in probe.spans for name, _attributes in span.events
            ]
            assert lifecycle_events.count("runner.finish_race") == 0
            assert lifecycle_events.count("runner.turn.started") == 1

    asyncio.run(go())


def test_idle_retained_route_is_not_reported_as_a_finish_race(
    make_harness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def go() -> None:
        probe = _install(monkeypatch)
        async with make_harness() as h:
            h.runner.default_script = [Final(text="first", status=DONE)]
            await h.kernel.process_event(_qevent("first", thread="thread-existing-route"))

            # Isolate the follow-up. The sandbox route remains live, but its
            # first turn was already observably idle before the rejected steer.
            probe.metrics.clear()
            probe.spans.clear()
            steer_attempts = len(h.runner.steer_headers)
            status_reads = len(h.runner.status_headers)
            h.runner.default_script = [Final(text="second", status=DONE)]

            await h.kernel.process_event(_qevent("second", thread="thread-existing-route"))

            assert len(h.runner.status_headers) == status_reads + 1
            assert len(h.runner.steer_headers) == steer_attempts + 1
            assert h.runner.opened == ["first", "second"]
            route_outcomes = [
                point.attributes["outcome"]
                for point in _metrics(probe, "curie.thread.route")
            ]
            assert route_outcomes == ["start"]
            lifecycle_events = [
                name for span in probe.spans for name, _attributes in span.events
            ]
            assert lifecycle_events.count("runner.finish_race") == 0
            assert lifecycle_events.count("runner.turn.started") == 1

    asyncio.run(go())


def test_observed_active_turn_that_ends_before_steer_reports_one_finish_race(
    make_harness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def go() -> None:
        probe = _install(monkeypatch)
        async with make_harness() as h:
            h.runner.default_script = [Final(text="first", status=DONE)]
            await h.kernel.process_event(_qevent("first", thread="thread-finish-race"))

            probe.metrics.clear()
            probe.spans.clear()
            steer_attempts = len(h.runner.steer_headers)
            status_reads = 0

            async def active_before_steer(_base_url: str) -> dict[str, object]:
                nonlocal status_reads
                status_reads += 1
                return {"turn_active": True}

            monkeypatch.setattr(h.kernel._runner, "status", active_before_steer)
            h.runner.default_script = [Final(text="second", status=DONE)]

            # The liveness observation says active, while the real fake runner
            # is idle by the time /v1/steer arrives and therefore returns 409.
            await h.kernel.process_event(_qevent("second", thread="thread-finish-race"))

            assert status_reads == 1
            assert len(h.runner.steer_headers) == steer_attempts + 1
            assert h.runner.opened == ["first", "second"]
            route_outcomes = [
                point.attributes["outcome"]
                for point in _metrics(probe, "curie.thread.route")
            ]
            assert route_outcomes == ["finish-race", "start"]
            lifecycle_events = [
                name for span in probe.spans for name, _attributes in span.events
            ]
            assert lifecycle_events.count("runner.finish_race") == 1
            assert lifecycle_events.count("runner.turn.started") == 1

    asyncio.run(go())


def test_cancellation_is_interrupted_error_not_completed_success(
    make_harness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def go() -> None:
        probe = _install(monkeypatch)
        async with make_harness() as h:
            entered = asyncio.Event()
            release = asyncio.Event()

            async def cancelled_process(_qevent: QueuedTurn) -> None:
                entered.set()
                await release.wait()

            monkeypatch.setattr(h.kernel, "_process_event", cancelled_process)
            task = asyncio.create_task(h.kernel.process_event(_qevent("cancelled")))
            await entered.wait()
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task

            turn = next(span for span in probe.spans if span.name == "curie.turn.process")
            assert turn.attributes["outcome"] == "interrupted"
            assert turn.status is StatusCode.ERROR
            assert ("turn.processing.interrupted", {"outcome": "interrupted"}) in turn.events
            assert (
                "turn.processing.completed",
                {"outcome": "interrupted"},
            ) in turn.events

    asyncio.run(go())


class _Approvals:
    def __init__(self) -> None:
        self.requests: list[ApprovalRequest] = []

    async def create(self, request: ApprovalRequest) -> CreatedApproval:
        self.requests.append(request)
        return CreatedApproval(id=f"appr-example-{len(self.requests)}", status="pending")


def test_approval_suspend_and_resume_have_bounded_lifecycle_outcomes(
    make_harness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def go() -> None:
        probe = _install(monkeypatch)
        approvals = _Approvals()
        async with make_harness(approvals=approvals) as h:
            h.runner.default_script = [
                TextDelta(text="Requesting sign-off"),
                Final(
                    text="Requesting sign-off",
                    status=AWAITING,
                    approval_summary="Publish the report",
                ),
            ]
            await h.kernel.process_event(
                _qevent("publish", thread="thread-approval", event_id="Ev0EXAMPLEAPPROVAL1")
            )
            assert len(approvals.requests) == 1

            h.runner.default_script = [Final(text="continued", status=DONE)]
            await h.kernel.process_event(
                _qevent(
                    "[approval resolved] approved",
                    thread="thread-approval",
                    event_id="approval-appr-example-1-resolved",
                )
            )

            outcomes = {
                point.attributes["outcome"]
                for point in _metrics(probe, "curie.approval.lifecycle")
            }
            assert outcomes >= {"suspended", "resumed"}
            assert {
                "curie.approval.suspend",
                "curie.approval.resume",
            } <= {span.name for span in probe.spans}
            process_spans = _spans(probe, "curie.turn.process")
            assert process_spans[0].attributes["outcome"] == "awaiting_approval"
            assert process_spans[0].status is StatusCode.ERROR
            assert process_spans[1].attributes["outcome"] == "done"
            assert process_spans[1].status is StatusCode.OK
            for point in _metrics(probe, "curie.approval.lifecycle"):
                assert set(point.attributes) <= _BOUNDED_KEYS
                assert "thread-approval" not in point.attributes.values()

    asyncio.run(go())


def test_worker_does_not_fabricate_global_pending_approval_inventory(
    make_harness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def go() -> None:
        probe = _install(monkeypatch)
        approvals = _Approvals()
        async with make_harness(approvals=approvals) as h:
            for index in (1, 2):
                h.runner.default_script = [
                    Final(
                        text=f"approval {index}",
                        status=AWAITING,
                        approval_summary=f"Approve request {index}",
                    )
                ]
                await h.kernel.process_event(
                    _qevent(
                        f"request {index}",
                        thread=f"thread-pending-{index}",
                        event_id=f"Ev0EXAMPLEPENDING{index}",
                    )
                )

            h.runner.default_script = [Final(text="continued", status=DONE)]
            await h.kernel.process_event(
                _qevent(
                    "[approval resolved] approved",
                    thread="thread-pending-1",
                    event_id="approval-appr-example-1-resolved",
                )
            )

            assert not _metrics(probe, "curie.approval.pending")
            assert not _metrics(probe, "curie.approval.pending.age")
            assert _metrics(probe, "curie.approval.lifecycle"), (
                "the worker still owns lifecycle telemetry; only global DB inventory "
                "is reserved for the API's authoritative pending query"
            )

    asyncio.run(go())


def test_planned_shared_telemetry_api_is_the_runtime_dependency() -> None:
    """Collection-level guard against app-local copies of the shared contract."""

    assert TRACEPARENT_STREAM_FIELD == "traceparent"
    assert callable(inject_trace_context)
    assert callable(extract_trace_context)
    assert callable(operation_span)
    assert callable(record_metric)


def test_stream_timeout_emits_a_timeout_rpc_result_and_a_failed_span(
    make_harness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """#2011: the runner RPC boundary must produce terminal evidence when the
    streaming budget expires.

    ``start_turn``'s span closes as soon as the response headers arrive, so a
    budget that expires while the NDJSON body is being read currently emits
    nothing here at all: the only ``curie.runner.rpc.result`` point for the turn
    says ``outcome="success"``. The stream boundary must record its own
    ``outcome="timeout"`` point and mark its span ERROR, or a timed-out turn is
    invisible in the RPC telemetry."""

    async def go() -> None:
        probe = _install(monkeypatch)
        async with make_harness() as h:
            hold = asyncio.Event()  # never set: the response hangs after a prefix
            h.runner.hold = hold
            h.runner.default_script = [TextDelta(text="x")]
            handle = await asyncio.to_thread(h.substrate.claim, "thread-stream-timeout")
            client = runner_client_module.RunnerClient(total_timeout_s=0.5)
            try:
                turn = await client.start_turn(
                    handle.base_url, Event(type="message", text="hi", user="U", ts="1")
                )
                with pytest.raises(TimeoutError):
                    async with turn:
                        async for _frame in turn:
                            pass

                results = _metrics(probe, "curie.runner.rpc.result")
                timeouts = [
                    point for point in results if point.attributes.get("outcome") == "timeout"
                ]
                assert timeouts, [point.attributes for point in results]
                attributes = timeouts[-1].attributes
                assert attributes["service.name"] == "curie-worker"
                assert attributes["operation"] == "event"
                assert attributes["role"] == "client"
                assert set(attributes) <= _BOUNDED_KEYS

                failed = [
                    span
                    for span in _spans(probe, "curie.runner.rpc")
                    if span.status is StatusCode.ERROR
                ]
                assert failed, "the stream boundary must mark its span failed"
            finally:
                hold.set()
                await client.close()

    asyncio.run(go())


# --- #2622: the channel event id on request-path spans ------------------------
#
# R3: the probe stores CALLER keys (``stamp_event_id`` returns caller
# spellings, and ``_ProbeSpan.set_attribute`` writes ``name`` raw), so every
# assertion below names ``event_id``. The EXPORTED name
# (``curie.channel.event_id``) is pinned exactly once, in
# ``packages/telemetry/tests/test_event_id_scope.py``; a rename must fail there.

_REQUEST_PATH_SPANS = (
    "curie.queue.process",
    "curie.sandbox.claim",
    "curie.runner.rpc",
    "curie.reply.update",
)


def _event_ids(probe: _Probe, name: str) -> list[str | None]:
    return [span.attributes.get("event_id") for span in _spans(probe, name)]


def test_every_named_request_path_span_carries_the_delivered_event_id(
    make_harness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """T4 (AC 1, AC 2): one turn stamps all four named spans with one id."""

    async def go() -> None:
        probe = _install(monkeypatch)
        async with make_harness() as h:
            h.runner.default_script = [Final(text="stamped", status=DONE)]
            consumer = Consumer(redis=h.async_redis, kernel=h.kernel, config=h.config)
            await consumer.ensure_group()
            event = _qevent("stamp me", event_id="Ev0EXAMPLEKNOWN1")

            await _deliver(consumer, h, {"payload": event.model_dump_json()})

            for name in _REQUEST_PATH_SPANS:
                observed = _event_ids(probe, name)
                assert observed, f"{name} was never opened; the turn did not run"
                assert set(observed) == {"Ev0EXAMPLEKNOWN1"}, (
                    f"{name} spans carried {observed!r}, not the delivered event id"
                )
            # The consequence sites (§2 #6-#9) are inside the scope too.
            assert set(_event_ids(probe, "curie.turn.process")) == {"Ev0EXAMPLEKNOWN1"}
            for point in probe.metrics:
                assert set(point.attributes) <= _BOUNDED_KEYS
                assert "Ev0EXAMPLEKNOWN1" not in point.attributes.values()

    asyncio.run(go())


def test_failed_processing_keeps_the_event_id_on_the_span_and_off_the_metrics(
    make_harness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """T5a (AC 1): the ``queue.processing.failed`` branch is inside the span."""

    async def go() -> None:
        probe = _install(monkeypatch)
        async with make_harness() as h:
            consumer = Consumer(redis=h.async_redis, kernel=h.kernel, config=h.config)
            await consumer.ensure_group()

            async def fail(_turn: QueuedTurn) -> None:
                raise RuntimeError("injected processing failure")

            h.kernel.process_event = fail  # type: ignore[method-assign]
            event = _qevent("failing", event_id="Ev0EXAMPLEFAILED1")

            await _deliver(consumer, h, {"payload": event.model_dump_json()})

            process = _spans(probe, "curie.queue.process")
            assert len(process) == 1
            assert process[0].attributes.get("event_id") == "Ev0EXAMPLEFAILED1"
            assert any(
                name == "queue.processing.failed" for name, _a in process[0].events
            ), "the failure branch did not run; this test proved nothing"
            assert {
                point.attributes["outcome"]
                for point in _metrics(probe, "curie.queue.settle")
            } == {"pending"}
            for point in probe.metrics:
                assert set(point.attributes) <= _BOUNDED_KEYS
                assert "Ev0EXAMPLEFAILED1" not in point.attributes.values()

    asyncio.run(go())


def test_lease_loss_pending_path_keeps_the_event_id_on_the_span(
    make_harness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """T5b (AC 1): the pre-ack lease-loss branch shares the same span object."""

    async def go() -> None:
        from curie_worker.delivery_lease import LeaseLostError

        probe = _install(monkeypatch)
        async with make_harness() as h:
            h.runner.default_script = [Final(text="pending", status=DONE)]
            consumer = Consumer(redis=h.async_redis, kernel=h.kernel, config=h.config)
            await consumer.ensure_group()

            class _LostLease:
                def raise_if_lost(self) -> None:
                    raise LeaseLostError("the delivery lease moved to a replacement")

            class _LostLeaseCM:
                def __init__(self, *_args: Any, **_kwargs: Any) -> None:
                    pass

                async def __aenter__(self) -> _LostLease:
                    return _LostLease()

                async def __aexit__(self, *_exc: Any) -> bool:
                    return False

            monkeypatch.setattr(consumer, "_delivery_lease", _LostLeaseCM)
            event = _qevent("lease loss", event_id="Ev0EXAMPLEPENDING1")

            await _deliver(consumer, h, {"payload": event.model_dump_json()})

            process = _spans(probe, "curie.queue.process")
            assert len(process) == 1
            assert process[0].attributes.get("event_id") == "Ev0EXAMPLEPENDING1"
            assert not any(
                name == "queue.message.acked" for name, _a in process[0].events
            ), "the entry was acked; the lease-loss pending branch did not run"
            assert "pending" in {
                point.attributes["outcome"]
                for point in _metrics(probe, "curie.queue.settle")
            }
            for point in probe.metrics:
                assert "Ev0EXAMPLEPENDING1" not in point.attributes.values()

    asyncio.run(go())


class _Rendezvous:
    """A barrier shaped like ``FakeRunner.hold`` (it is awaited as ``.wait()``).

    The fake runner parks on it AFTER writing its prefix frame and BEFORE its
    terminal frame -- i.e. mid-turn, inside the event-id scope -- so every turn
    that reaches it is provably still in flight. ``admitted`` is what the test
    asserts on: without that assertion a harness change that serialized the
    deliveries would silently reduce T6 to N sequential turns, which cannot
    distinguish a contextvar from a process global.
    """

    def __init__(self, parties: int, timeout: float = 20.0) -> None:
        self._barrier = asyncio.Barrier(parties)
        self._timeout = timeout
        self.arrived = 0
        self.admitted = 0

    async def wait(self) -> None:
        self.arrived += 1
        try:
            await asyncio.wait_for(self._barrier.wait(), self._timeout)
        except (TimeoutError, asyncio.BrokenBarrierError):
            # Return rather than raise: the turn then completes normally and the
            # test fails on ``admitted``, naming the real problem (no overlap),
            # instead of on a runner error that looks like something else.
            return
        self.admitted += 1


def _root_of(probe: _Probe, span: _SpanCall) -> _SpanCall | None:
    """The ``curie.queue.process`` ancestor of ``span``, by span-id linkage."""

    by_id = {call.span_id: call for call in probe.spans}
    seen: set[int] = set()
    current: _SpanCall | None = span
    while current is not None and current.name != "curie.queue.process":
        if current.span_id in seen:
            return None
        seen.add(current.span_id)
        current = by_id.get(current.parent_span_id)
    return current


def test_twelve_overlapping_turns_keep_their_event_ids_on_their_own_subtrees(
    make_harness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """T6 (AC 5), the centrepiece.

    NOT written with ``_deliver``: that helper gathers all of
    ``consumer._inflight`` before returning, so N concurrent ``_deliver`` calls
    run strictly sequentially and the test would pass against a process-global
    id. Here all N entries are XADDed, read in ONE ``xreadgroup``, dispatched,
    and only then awaited.

    > Why disjointness alone is TAUTOLOGICAL -- do not "simplify" this back.
    > Grouping spans by their own event id and asserting the groups are
    > pairwise disjoint is true by construction of the grouping. It cannot fail
    > even under a process-global id, because the post-hoc
    > ``span.set_attribute`` gives each ``curie.queue.process`` root the correct
    > id regardless of the stamping mechanism, and every other check is
    > satisfied by any labelling at all. MEMBERSHIP is what breaks under a
    > global: with N overlapping turns a turn's nested spans collapse onto the
    > last writer's id. So the primary assertion below joins each child span to
    > its root by span-id linkage and compares the two ids. Coverage and the
    > negative checks are secondary.
    """

    turns = 12

    async def go() -> None:
        probe = _install(monkeypatch)
        async with make_harness(per_sandbox_runners=turns) as h:
            rendezvous = _Rendezvous(turns)
            for runner in (h.runner, *h.runners.values()):
                runner.default_script = [TextDelta(text="working")]
                runner.tail = [Final(text="done", status=DONE)]
                runner.hold = rendezvous

            consumer = Consumer(redis=h.async_redis, kernel=h.kernel, config=h.config)
            await consumer.ensure_group()
            assert consumer._max_concurrency >= turns, (
                "the consumer semaphore must admit every turn at once, or the "
                "deliveries cannot overlap and T6 tests nothing"
            )

            events = [
                _qevent(
                    f"concurrent {index}",
                    thread=f"thread-concurrent-{index}",
                    event_id=f"Ev0EXAMPLECONCURRENT{index:02d}",
                )
                for index in range(turns)
            ]
            delivered_ids = {event.event_id for event in events}
            assert len(delivered_ids) == turns

            for event in events:
                await h.async_redis.xadd(
                    h.config.stream, {"payload": event.model_dump_json()}
                )
            rows = await h.async_redis.xreadgroup(
                h.config.consumer_group,
                h.config.consumer_name,
                {h.config.stream: ">"},
                count=turns,
            )
            entries = rows[0][1]
            assert len(entries) == turns, f"expected {turns} entries, got {len(entries)}"
            for entry_id, fields in entries:
                await consumer._dispatch(entry_id, fields)
            await asyncio.wait_for(
                asyncio.gather(*list(consumer._inflight)), timeout=120
            )

            # PRECONDITION, not decoration: all N were parked mid-turn, inside
            # their own scope, at the same instant.
            assert rendezvous.admitted == turns, (
                f"only {rendezvous.admitted} of {turns} turns were simultaneously "
                f"in flight ({rendezvous.arrived} reached the barrier); the "
                "deliveries serialized and this test cannot distinguish a "
                "contextvar from a process-global id"
            )

            roots = _spans(probe, "curie.queue.process")
            assert len(roots) == turns
            by_event_id: dict[str, _SpanCall] = {}
            for root in roots:
                event_id = root.attributes.get("event_id")
                assert event_id is not None, "a queue.process root was not stamped"
                assert event_id not in by_event_id, f"two roots claim {event_id!r}"
                by_event_id[event_id] = root
            assert set(by_event_id) == delivered_ids

            # 1. MEMBERSHIP (primary). Every stamped span belongs to the subtree
            #    of the root carrying the same id. This is the assertion that
            #    fails under a process-global.
            stamped = [
                span for span in probe.spans if span.attributes.get("event_id") is not None
            ]
            assert stamped
            for span in stamped:
                root = _root_of(probe, span)
                assert root is not None, (
                    f"{span.name} carries an event id but descends from no "
                    "curie.queue.process root"
                )
                assert span.attributes["event_id"] == root.attributes.get("event_id"), (
                    f"{span.name} carries {span.attributes['event_id']!r} but "
                    f"descends from the turn for {root.attributes.get('event_id')!r}"
                )

            # Each id's group holds that turn's OWN four named spans.
            for event_id, root in by_event_id.items():
                group = {
                    span.name
                    for span in stamped
                    if span.attributes["event_id"] == event_id
                    and _root_of(probe, span) is root
                }
                assert set(_REQUEST_PATH_SPANS) <= group, (
                    f"turn {event_id} is missing {set(_REQUEST_PATH_SPANS) - group}"
                )

            # 2. COVERAGE (secondary). Includes the post-hoc-stamped roots.
            assert {span.attributes["event_id"] for span in stamped} == delivered_ids

            # 3. NEGATIVE (secondary).
            for span in probe.spans:
                value = span.attributes.get("event_id")
                assert value is None or value in delivered_ids, (
                    f"{span.name} carries an undelivered event id {value!r}"
                )
            for point in probe.metrics:
                assert delivered_ids.isdisjoint(point.attributes.values())

    asyncio.run(go())


def test_no_event_id_survives_a_turn_or_crosses_into_the_next_one(
    make_harness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """T8 (E4b, AC 5): the scope is entered by token and always reset."""

    async def go() -> None:
        probe = _install(monkeypatch)
        async with make_harness() as h:
            h.runner.default_script = [Final(text="a", status=DONE)]
            consumer = Consumer(redis=h.async_redis, kernel=h.kernel, config=h.config)
            await consumer.ensure_group()
            first = _qevent("first", thread="thread-leak-a", event_id="Ev0EXAMPLELEAKA1")
            await _deliver(consumer, h, {"payload": first.model_dump_json()})

            # Between turns, on the very task that just ran one.
            with probe.operation_span("curie.queue.process", kind=SpanKind.CONSUMER):
                pass
            between = probe.spans[-1]
            assert "event_id" not in between.attributes, (
                f"a context-free span inherited {between.attributes.get('event_id')!r}; "
                "the scope leaked past the turn"
            )

            probe.spans.clear()
            second = _qevent("second", thread="thread-leak-b", event_id="Ev0EXAMPLELEAKB1")
            await _deliver(consumer, h, {"payload": second.model_dump_json()})

            observed = {
                span.attributes["event_id"]
                for span in probe.spans
                if "event_id" in span.attributes
            }
            assert observed == {"Ev0EXAMPLELEAKB1"}, (
                f"the second turn's spans carried {observed!r}; a stale id crossed "
                "from the first turn"
            )

    asyncio.run(go())


def test_thread_reset_drain_spans_are_not_stamped_with_the_turns_event_id(
    make_harness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """T9 (R1, AC 3/AC 5): the wrong-correlation pin for the scope placement.

    Keyed on ``curie.sandbox.release`` ONLY. ``release_thread`` always opens it
    (kernel.py:1966) and the turn path does not open it on this route.
    ``curie.thread.lock`` and ``curie.runner.rpc`` are unusable as drain
    identifiers: the turn path opens both names, and AC 2 REQUIRES the rpc one
    stamped, so keying on either would false-fail a correct implementation.
    (``_rpc("interrupt")`` is also not emitted at all unless the reset thread
    already has a live sandbox, which is the second reason.)
    """

    async def go() -> None:
        probe = _install(monkeypatch)
        async with make_harness() as h:
            h.runner.default_script = [Final(text="drained", status=DONE)]
            consumer = Consumer(redis=h.async_redis, kernel=h.kernel, config=h.config)
            await consumer.ensure_group()
            # A DIFFERENT thread than the turn's: the drain tears down thread B
            # while turn A is in flight on the same handler task.
            await h.async_redis.sadd(
                consumer_module.THREAD_RESET_SET, "thread-reset-victim"
            )
            try:
                event = _qevent(
                    "turn under a drain",
                    thread="thread-drain-turn",
                    event_id="Ev0EXAMPLEDRAIN1",
                )
                await _deliver(consumer, h, {"payload": event.model_dump_json()})

                releases = _spans(probe, "curie.sandbox.release")
                # (a) Fail loudly rather than pass vacuously: no release span
                #     means the drain never ran and (b) below proves nothing.
                assert releases, (
                    "the thread-reset drain did not run, so this test could not "
                    "observe whether its spans were wrongly stamped"
                )
                # (b) The drain describes ANOTHER thread; a Tempo query for this
                #     turn must not return it.
                assert [span.attributes.get("event_id") for span in releases] == [
                    None
                ] * len(releases), (
                    "a thread-reset teardown span was stamped with the in-flight "
                    "turn's event id: the scope opens before the drain"
                )
                # The turn's own spans are stamped, so this is not passing
                # because stamping is broken everywhere.
                for name in _REQUEST_PATH_SPANS:
                    assert set(_event_ids(probe, name)) == {"Ev0EXAMPLEDRAIN1"}, (
                        f"{name} lost the event id"
                    )
            finally:
                await h.async_redis.delete(consumer_module.THREAD_RESET_SET)
                await h.async_redis.delete(consumer_module.THREAD_RESET_INFLIGHT_SET)

    asyncio.run(go())


def test_claim_latency_log_line_is_unchanged(request: pytest.FixtureRequest) -> None:
    """T7 (AC 6): a no-change pin on the kernel's claim-latency log line.

    The other half of D8 -- an EMPTY ``git diff origin/main`` for ``kernel.py``
    -- belongs to the done-check, not to a unit test that would shell out.
    """

    from pathlib import Path

    kernel_source = (
        Path(__file__).resolve().parents[2] / "src" / "curie_worker" / "kernel.py"
    ).read_text()
    line = 'logger.info("claim latency for %s: %d ms", thread_key, claim_ms)'
    assert kernel_source.count(line) == 1, (
        "AC 6 is a no-change assertion: the claim-latency log line must stay "
        "exactly as it is, and kernel.py must not be edited by this ticket"
    )
