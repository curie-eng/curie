"""Causal worker telemetry and bounded operational metrics (#1817/#1818)."""

from __future__ import annotations

import asyncio
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
        span_id = self._next_span_id
        self._next_span_id += 1
        call = _SpanCall(
            name=name,
            kind=kind,
            parent_trace_id=parent_span.trace_id,
            parent_span_id=parent_span.span_id,
            span_id=span_id,
            attributes=dict(attributes or {}),
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
