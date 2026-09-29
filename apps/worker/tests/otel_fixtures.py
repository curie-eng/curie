"""Shared OTel probe for worker telemetry tests.

``install`` swaps ``operation_span`` and ``record_metric`` for a recording
probe on ``curie_telemetry`` and on every module passed in, so direct imports
and module-qualified calls are both captured.
"""

from __future__ import annotations

import threading
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass, field
from types import ModuleType
from typing import Any

import pytest
from curie_telemetry import stamp_event_id
from opentelemetry import context as otel_context
from opentelemetry import trace
from opentelemetry.trace import NonRecordingSpan, SpanContext, TraceFlags, TraceState

# Trace id of the remote parent the kernel tests inject; a span opened with no
# valid parent gets ``REMOTE_TRACE_ID + 1`` so it is distinguishable from it.
REMOTE_TRACE_ID = int("3123456789abcdef0123456789abcdef", 16)


@dataclass(frozen=True)
class Metric:
    name: str
    value: float
    attributes: dict[str, str]


@dataclass
class SpanCall:
    name: str
    kind: Any
    parent_trace_id: int
    parent_span_id: int
    span_id: int
    attributes: dict[str, str]
    events: list[tuple[str, dict[str, str]]] = field(default_factory=list)
    status: Any = None


class ProbeSpan:
    def __init__(self, call: SpanCall) -> None:
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


class Probe:
    def __init__(self) -> None:
        self.spans: list[SpanCall] = []
        self.metrics: list[Metric] = []
        self._next_span_id = 0x4000000000000000
        # operation_span is called from asyncio.to_thread worker threads (the
        # sandbox claim/release hops in kernel.py), not just the event loop.
        # Without a lock, two threads can read the same _next_span_id and mint
        # duplicate span ids, which collapses a {span_id: call} root-join map.
        self._lock = threading.Lock()

    def span_names(self) -> list[str]:
        return [span.name for span in self.spans]

    @contextmanager
    def operation_span(
        self,
        name: str,
        *,
        kind: Any,
        parent: Any = None,
        attributes: Mapping[str, str] | None = None,
    ) -> Iterator[ProbeSpan]:
        parent_span = trace.get_current_span(parent).get_span_context()
        if parent is None:
            parent_span = trace.get_current_span().get_span_context()
        trace_id = parent_span.trace_id if parent_span.is_valid else REMOTE_TRACE_ID + 1
        with self._lock:
            span_id = self._next_span_id
            self._next_span_id += 1
            call = SpanCall(
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
            yield ProbeSpan(call)
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
            self.metrics.append(Metric(name, float(value), dict(attributes or {})))

    def points(self, name: str) -> list[Metric]:
        return [point for point in self.metrics if point.name == name]


def install(monkeypatch: pytest.MonkeyPatch, *modules: ModuleType) -> Probe:
    """Capture direct imports and module-qualified shared API calls alike."""

    import curie_telemetry

    probe = Probe()
    monkeypatch.setattr(curie_telemetry, "operation_span", probe.operation_span)
    monkeypatch.setattr(curie_telemetry, "record_metric", probe.record_metric)
    for module in modules:
        if hasattr(module, "operation_span"):
            monkeypatch.setattr(module, "operation_span", probe.operation_span)
        if hasattr(module, "record_metric"):
            monkeypatch.setattr(module, "record_metric", probe.record_metric)
    return probe
