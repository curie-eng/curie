"""Shared operation spans with explicit parenting and honest status."""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from contextvars import ContextVar
from typing import Any, cast

from opentelemetry import trace
from opentelemetry.context import Context
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.trace import (
    Span,
    SpanKind,
    StatusCode,
    Tracer,
)
from opentelemetry.util.types import AttributeValue

from .context import _normalize_trace_context
from .redact import redact_span_attribute

_tracer: Tracer = trace.NoOpTracerProvider().get_tracer("curie-telemetry")

# The channel event id for the turn currently being handled on this asyncio task.
# ``asyncio`` copies the context per task and ``asyncio.to_thread`` copies it per
# hop, so two concurrent turns physically cannot read each other's value.
_CHANNEL_EVENT_ID: ContextVar[str | None] = ContextVar("curie_channel_event_id", default=None)


@contextmanager
def channel_event_id_scope(event_id: str | None) -> Iterator[None]:
    """Bind ``event_id`` for the duration of the block, always resetting it.

    The reset is by token in a ``finally``: a bare ``set()`` would leak a stale id
    onto the next entry handled by a reused worker task, attributing one request's
    spans to another. That is silent wrong correlation, which is worse than a
    missing attribute.
    """

    token = _CHANNEL_EVENT_ID.set(event_id)
    try:
        yield
    finally:
        _CHANNEL_EVENT_ID.reset(token)


def current_channel_event_id() -> str | None:
    """Return the channel event id bound to the current context, if any."""

    return _CHANNEL_EVENT_ID.get()


def stamp_event_id(
    attributes: Mapping[str, AttributeValue] | None,
) -> Mapping[str, AttributeValue]:
    """Return ``attributes`` carrying the ambient channel event id.

    The key is omitted entirely when no id is bound -- never ``""`` and never
    ``"None"``, either of which would make a correlation query match every
    context-free span. A caller-supplied ``event_id`` always wins. The caller's
    mapping is never mutated: the worker reuses one dict for its span and its
    metrics, and the metric surface must stay closed to the id. When there is
    nothing to stamp -- no id bound, or the caller already supplied one -- the
    caller's mapping is returned unchanged rather than copied, since this runs
    on every ``operation_span`` call across the platform.
    """

    event_id = _CHANNEL_EVENT_ID.get()
    if not event_id or (attributes is not None and "event_id" in attributes):
        return attributes if attributes is not None else {}
    stamped: dict[str, AttributeValue] = dict(attributes or {})
    stamped["event_id"] = event_id
    return stamped


# Platform lifecycle spans deliberately use a small closed vocabulary. Callers
# use the metric-catalog spelling while the exported custom keys are namespaced.
# The vocabulary admits exactly one correlation identifier, ``event_id``, and it
# is admitted on spans only. The metric catalog stays closed to it: ``record_metric``
# enumerates a value domain per attribute and an identifier has no domain to
# enumerate, so an id reaching a metric fails loudly rather than exploding
# cardinality. New custom attributes must be added here and to the telemetry
# interface before they can reach the exporter.
PLATFORM_SPAN_ATTRIBUTE_KEYS = {
    "service.name": "service.name",
    "operation": "curie.operation",
    "role": "curie.role",
    "source": "curie.source",
    "outcome": "curie.outcome",
    "retry_class": "curie.retry_class",
    "event_id": "curie.channel.event_id",
}
PLATFORM_EVENT_ATTRIBUTE_KEYS = {
    "outcome": "curie.outcome",
    "error.class": "error.type",
}


def _closed_attributes(
    attributes: Mapping[str, AttributeValue] | None,
    vocabulary: Mapping[str, str],
    *,
    surface: str,
) -> dict[str, AttributeValue]:
    unknown = set(attributes or {}) - set(vocabulary)
    if unknown:
        raise ValueError(f"undeclared platform {surface} attribute: {sorted(unknown)!r}")
    return {
        vocabulary[key]: cast(AttributeValue, redact_span_attribute(value))
        for key, value in (attributes or {}).items()
    }


class OperationSpan:
    """Restrict post-construction span mutations to the documented schema."""

    def __init__(self, span: Span) -> None:
        self._span = span

    def set_attribute(self, key: str, value: AttributeValue) -> OperationSpan:
        safe = _closed_attributes(
            {key: value}, PLATFORM_SPAN_ATTRIBUTE_KEYS, surface="span"
        )
        export_key, export_value = next(iter(safe.items()))
        self._span.set_attribute(export_key, export_value)
        return self

    def add_event(
        self,
        name: str,
        attributes: Mapping[str, AttributeValue] | None = None,
        timestamp: int | None = None,
    ) -> OperationSpan:
        safe = _closed_attributes(
            attributes, PLATFORM_EVENT_ATTRIBUTE_KEYS, surface="event"
        )
        self._span.add_event(name, safe, timestamp)
        return self

    def set_status(self, *args: Any, **kwargs: Any) -> OperationSpan:
        self._span.set_status(*args, **kwargs)
        return self

def configure_tracer_provider(provider: TracerProvider | None) -> TracerProvider | None:
    global _tracer
    _tracer = (
        provider.get_tracer("curie-telemetry")
        if provider is not None
        else trace.NoOpTracerProvider().get_tracer("curie-telemetry")
    )
    return provider


@contextmanager
def operation_span(
    name: str,
    *,
    kind: SpanKind,
    parent: Context | None = None,
    attributes: Mapping[str, AttributeValue] | None = None,
) -> Iterator[OperationSpan]:
    """Open one operation span and convert escaped failures to ERROR."""

    # Stamp before the closed-vocabulary check so an undeclared caller key still
    # raises. ``record_metric`` is deliberately not stamped.
    safe_attributes = _closed_attributes(
        stamp_event_id(attributes), PLATFORM_SPAN_ATTRIBUTE_KEYS, surface="span"
    )
    with _tracer.start_as_current_span(
        name,
        context=_normalize_trace_context(parent),
        kind=kind,
        attributes=safe_attributes,
        record_exception=False,
        set_status_on_exception=False,
    ) as sdk_span:
        span = OperationSpan(sdk_span)
        try:
            yield span
        except BaseException as exc:
            # Never hand the SDK the exception object: record_exception would
            # serialize its message and traceback verbatim. The exception class
            # is enough to distinguish failure families without exporting user
            # input, credentials, paths, or stack contents.
            sdk_span.add_event(
                "exception",
                {"exception.type": type(exc).__name__},
            )
            sdk_span.set_status(StatusCode.ERROR)
            raise
        else:
            # A caller may classify a caught failure and deliberately continue.
            # Normal context exit must not erase that explicit ERROR decision.
            status = getattr(sdk_span, "status", None)
            if status is None or status.status_code is StatusCode.UNSET:
                sdk_span.set_status(StatusCode.OK)
