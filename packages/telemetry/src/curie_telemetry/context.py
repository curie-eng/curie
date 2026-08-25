"""W3C trace context helpers for Stream fields and HTTP headers."""

from __future__ import annotations

from collections.abc import Mapping, MutableMapping

from opentelemetry import trace
from opentelemetry.context import Context, get_current
from opentelemetry.propagators.textmap import Getter
from opentelemetry.trace import (
    NonRecordingSpan,
    SpanContext,
    TraceFlags,
    set_span_in_context,
)
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator

TRACEPARENT_STREAM_FIELD = "traceparent"

_PROPAGATOR = TraceContextTextMapPropagator()


class _MappingGetter(Getter[Mapping[str, str]]):
    def get(self, carrier: Mapping[str, str], key: str) -> list[str] | None:
        value = carrier.get(key)
        return [value] if value is not None else None

    def keys(self, carrier: Mapping[str, str]) -> list[str]:
        return list(carrier)


_GETTER = _MappingGetter()


def _normalize_trace_context(context: Context | None = None) -> Context:
    """Normalize trace flags while retaining the active Context and its values."""

    base = get_current() if context is None else context
    span_context = trace.get_current_span(base).get_span_context()
    if not span_context.is_valid:
        return base
    normalized = SpanContext(
        trace_id=span_context.trace_id,
        span_id=span_context.span_id,
        is_remote=span_context.is_remote,
        trace_flags=TraceFlags(int(span_context.trace_flags)),
        trace_state=span_context.trace_state,
    )
    return set_span_in_context(NonRecordingSpan(normalized), base)


def inject_trace_context(
    carrier: MutableMapping[str, str], context: Context | None = None
) -> MutableMapping[str, str]:
    """Inject only the W3C traceparent field and return the mutated carrier."""

    injected: dict[str, str] = {}
    _PROPAGATOR.inject(injected, context=_normalize_trace_context(context))
    traceparent = injected.get(TRACEPARENT_STREAM_FIELD)
    if traceparent is not None:
        carrier[TRACEPARENT_STREAM_FIELD] = traceparent
    return carrier


def extract_trace_context(carrier: Mapping[str, str]) -> Context:
    """Extract from a clean base so missing or malformed input is a safe root."""

    traceparent = carrier.get(TRACEPARENT_STREAM_FIELD)
    selected = {TRACEPARENT_STREAM_FIELD: traceparent} if traceparent is not None else {}
    return _PROPAGATOR.extract(selected, context=Context(), getter=_GETTER)
