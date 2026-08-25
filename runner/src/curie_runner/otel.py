"""OTel tracing for the runner: gen_ai spans exported by standard OTLP.

Productizes the PT-4/PT-E prototype span shape. Each turn is a root ``agent.run``
(SERVER) span carrying a ``langfuse.trace.name``, with a child ``llm.generation``
span holding ``gen_ai.request.model`` and ``gen_ai.usage.*`` token counts, plus a
child ``execute_tool`` span per tool call (``gen_ai.tool.name`` /
``gen_ai.operation.name``). Langfuse maps a model-bearing span to a generation and
nests tool spans as observations, so this reconstructs the tool-call tree (S1).

Traces go to the OTel Collector over OTLP, never directly to Langfuse: the
collector is the adapter that authenticates and forwards (Langfuse OTLP ingest is
HTTP-only). Endpoint, headers, and protocol come from the standard
``OTEL_EXPORTER_OTLP_*`` variables via ``SessionConfig.otel``; signal-specific
configuration wins over the general variables. When no endpoint is configured
the tracer is a no-op, so unit tests and offline runs neither export nor fail.

Per ADR-0076, every attribute this module attaches comes from the closed
``SpanAttributeKey`` enum below rather than a bare string, so a future call site
with an unlisted key is a construction-time error, not a silent addition to the
wire shape. ``SCHEMA_VERSION`` is bumped only when a key is removed, renamed, or
changes value type; a new optional key is additive and does not bump it.
``SPAN_ATTRIBUTE_VALUE_TYPES`` declares each key's value type, the mirror the
drift gate diffs to catch a retype.
"""

from __future__ import annotations

import os
import threading
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from enum import StrEnum
from typing import Any, cast

from aci_protocol import BootEnv, OtelConfig
from curie_telemetry import (
    build_otlp_span_exporter,
    build_resource,
    deployment_environment,
    service_instance_id,
)
from opentelemetry import trace
from opentelemetry.attributes import BoundedAttributes
from opentelemetry.context import Context
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import ReadableSpan, SpanProcessor, TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.trace import (
    NonRecordingSpan,
    SpanContext,
    SpanKind,
    StatusCode,
    TraceFlags,
    Tracer,
    set_span_in_context,
)

from .redact import redact_span_attribute, redact_text

_OTEL_ENDPOINT_ENV = BootEnv.env_key("otel_endpoint")
_OTEL_PROTOCOL_ENV = BootEnv.env_key("otel_protocol")
_OTEL_HEADERS_ENV = BootEnv.env_key("otel_headers")
_OTEL_TRACES_ENDPOINT_ENV = "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT"
_OTEL_TRACES_PROTOCOL_ENV = "OTEL_EXPORTER_OTLP_TRACES_PROTOCOL"
_OTEL_TRACES_HEADERS_ENV = "OTEL_EXPORTER_OTLP_TRACES_HEADERS"
_SERVICE_NAME = "curie-runner"
_EXPORT_TIMEOUT_MILLIS = 5000
_MAX_QUEUE_SIZE = 2048
_MAX_EXPORT_BATCH_SIZE = 512
_SCHEDULE_DELAY_MILLIS = 1000

# ADR-0076 decision 2: additive (a new optional key) does not bump this; removing,
# renaming, or retyping an existing key does.
SCHEMA_VERSION = "v1"


class SpanAttributeKey(StrEnum):
    """The closed set of keys the runner may attach to a span or resource.

    ADR-0076 decision 1. Str-mixin so a member is usable anywhere a plain
    attribute-value string is expected (e.g. dict keys, f-strings), but every
    ``set_attribute``/``Resource.create`` call site should pass a member here
    rather than a literal, so an unlisted key is a construction-time error.
    """

    TRACE_NAME = "langfuse.trace.name"
    SESSION_ID = "langfuse.session.id"
    USER_ID = "langfuse.user.id"
    # ADR-0076 Stone 3 (#889, epic #512): the resolved terminal decision
    # (approved/rejected/expired) of the approval a resume turn is resuming
    # from, threaded in from the worker's authority-free CURIE_APPROVAL_DECISION
    # boot-env fact. Closes the "did an approval get requested" gap ADR-0038
    # named open, on the existing span stream.
    APPROVAL_DECISION = "gen_ai.approval.decision"
    REQUEST_MODEL = "gen_ai.request.model"
    MODEL = "model"
    USAGE_INPUT_TOKENS = "gen_ai.usage.input_tokens"
    USAGE_OUTPUT_TOKENS = "gen_ai.usage.output_tokens"
    USAGE_CACHE_READ_INPUT_TOKENS = "gen_ai.usage.cache_read_input_tokens"
    USAGE_CACHE_CREATION_INPUT_TOKENS = "gen_ai.usage.cache_creation_input_tokens"
    TOOL_NAME = "gen_ai.tool.name"
    OPERATION_NAME = "gen_ai.operation.name"
    SERVICE_NAME = "service.name"
    CURIE_SESSION_ID = "curie.session_id"
    CURIE_SANDBOX_ID = "curie.sandbox_id"
    SCHEMA_VERSION_KEY = "schema.version"


# ADR-0076 decision 2: a value-type change to an existing key is a breaking,
# version-bump-worthy change exactly like a remove or rename, so it needs its
# own source of truth to diff against -- the type half of the closed schema,
# parallel to ``SpanAttributeKey`` being the key half. Every member above must
# appear here exactly once, mapped to its value-type name ("str" or "int");
# the ``gen_ai.usage.*`` token counts are the only "int" members (see
# ``record_usage`` below and ``redact.py``'s "only str and int attributes are
# set today").
SPAN_ATTRIBUTE_VALUE_TYPES: Mapping[SpanAttributeKey, str] = {
    SpanAttributeKey.TRACE_NAME: "str",
    SpanAttributeKey.SESSION_ID: "str",
    SpanAttributeKey.USER_ID: "str",
    SpanAttributeKey.APPROVAL_DECISION: "str",
    SpanAttributeKey.REQUEST_MODEL: "str",
    SpanAttributeKey.MODEL: "str",
    SpanAttributeKey.USAGE_INPUT_TOKENS: "int",
    SpanAttributeKey.USAGE_OUTPUT_TOKENS: "int",
    SpanAttributeKey.USAGE_CACHE_READ_INPUT_TOKENS: "int",
    SpanAttributeKey.USAGE_CACHE_CREATION_INPUT_TOKENS: "int",
    SpanAttributeKey.TOOL_NAME: "str",
    SpanAttributeKey.OPERATION_NAME: "str",
    SpanAttributeKey.SERVICE_NAME: "str",
    SpanAttributeKey.CURIE_SESSION_ID: "str",
    SpanAttributeKey.CURIE_SANDBOX_ID: "str",
    SpanAttributeKey.SCHEMA_VERSION_KEY: "str",
}


# The ``usage`` mapping's own field names (SDK wire shape) to the span attribute
# they stamp, so ``record_usage`` can iterate without a per-field literal.
_USAGE_ATTRIBUTE_KEYS: Mapping[str, SpanAttributeKey] = {
    "input_tokens": SpanAttributeKey.USAGE_INPUT_TOKENS,
    "output_tokens": SpanAttributeKey.USAGE_OUTPUT_TOKENS,
    "cache_read_input_tokens": SpanAttributeKey.USAGE_CACHE_READ_INPUT_TOKENS,
    "cache_creation_input_tokens": SpanAttributeKey.USAGE_CACHE_CREATION_INPUT_TOKENS,
}


def _set(span: Any, key: SpanAttributeKey, value: object) -> None:
    """Set a span attribute through the redaction pass.

    Every ``set_attribute`` in this module goes through here so a future attribute
    cannot bypass the scrub by being written directly (see ``redact.py``), and
    ``key`` is a closed ``SpanAttributeKey`` member so an unlisted key cannot be
    attached by construction (ADR-0076).
    """

    span.set_attribute(key.value, redact_span_attribute(value))


def _still_leaks(value: object) -> bool:
    """Whether an attribute value still carries an unscrubbed secret.

    Type-agnostic on purpose (#935). ADR-0076 decision 3 frames the export
    validator as a universal backstop, but it only inspected ``str``, so a
    sequence-valued attribute on an ALLOWED key slipped a secret past BOTH the
    scrub and this validator — the two layers failing together, which is exactly
    what defense in depth is supposed to prevent. ``str`` is itself a Sequence, so
    it is matched first; anything that is neither a str nor a list/tuple (int,
    float, bool) cannot carry a pattern match and is clean by construction.
    """

    if isinstance(value, str):
        return redact_text(value) != value
    if isinstance(value, (list, tuple)):
        return any(_still_leaks(item) for item in value)
    return False


class _SchemaValidatingSpanProcessor(SpanProcessor):
    """Fail-closed export-time backstop (ADR-0076 decision 3).

    ``_set()`` already gates every attribute this module attaches through the
    closed ``SpanAttributeKey`` enum and the ``redact.py`` scrub; this processor
    exists for the call site that bypasses both by calling ``span.set_attribute``
    directly. On each span ending, it strips (does not replace) any attribute
    whose key is outside the closed schema, or whose value — or any element of a
    sequence value (#935) — still matches
    an unscrubbed-secret pattern after the existing redaction pass — dropping the
    offending attribute rather than the whole span, so one bad key costs a single
    field of trace data rather than the whole record.

    Must be registered on the provider ahead of the exporting processor
    (``TracerProvider.add_span_processor`` invokes processors in registration
    order); it mutates the span's attributes in place so the exporter that runs
    after it sees the cleaned set.
    """

    _ALLOWED_KEYS = frozenset(member.value for member in SpanAttributeKey)

    def on_end(self, span: ReadableSpan) -> None:
        # ReadableSpan.attributes is a read-only MappingProxyType view; the
        # underlying BoundedAttributes (span._attributes) is the same object the
        # concrete Span held, flagged immutable at Span.end() (see the SDK's own
        # `self._attributes._immutable = True` in Span.end()). Toggling that
        # private flag to mutate here mirrors the SDK's own pattern. Always a
        # BoundedAttributes at runtime (Span.__init__ constructs it directly);
        # the cast narrows past the Mapping-typed private attribute.
        raw = span._attributes  # noqa: SLF001
        if raw is None:
            return
        attributes = cast(BoundedAttributes, raw)
        was_immutable = attributes._immutable  # noqa: SLF001
        attributes._immutable = False  # noqa: SLF001
        try:
            for key in list(attributes.keys()):
                value = attributes[key]
                still_leaks = _still_leaks(value)
                if key not in self._ALLOWED_KEYS or still_leaks:
                    del attributes[key]
        finally:
            attributes._immutable = was_immutable  # noqa: SLF001


def build_tracer_provider(
    otel: OtelConfig, session_id: str, sandbox_id: str | None = None
) -> TracerProvider | None:
    """Build a TracerProvider exporting to the collector, or None if unconfigured.

    The resource uses the same stable process identity as every Curie service.
    Per-turn session and sandbox correlation stays on the root span so backends
    do not create a resource for every sandbox.
    """

    exporter_env = dict(os.environ)
    if otel.endpoint and not any(
        key in exporter_env
        for key in (
            _OTEL_ENDPOINT_ENV,
            _OTEL_TRACES_ENDPOINT_ENV,
        )
    ):
        exporter_env[_OTEL_ENDPOINT_ENV] = otel.endpoint
    if otel.protocol and not any(
        key in exporter_env
        for key in (
            _OTEL_PROTOCOL_ENV,
            _OTEL_TRACES_PROTOCOL_ENV,
        )
    ):
        exporter_env[_OTEL_PROTOCOL_ENV] = otel.protocol
    if otel.headers and not any(
        key in exporter_env
        for key in (
            _OTEL_HEADERS_ENV,
            _OTEL_TRACES_HEADERS_ENV,
        )
    ):
        exporter_env[_OTEL_HEADERS_ENV] = otel.headers
    exporter = build_otlp_span_exporter(
        exporter_env,
    )
    if exporter is None:
        return None
    resource = build_resource(
        _SERVICE_NAME,
        service_version="0.0.0",
        service_instance_id=service_instance_id(_SERVICE_NAME),
        deployment_environment=deployment_environment(exporter_env),
    ).merge(Resource({SpanAttributeKey.SCHEMA_VERSION_KEY.value: SCHEMA_VERSION}))
    provider = TracerProvider(resource=resource, shutdown_on_exit=False)
    # The provider crosses the existing construction seam into RunTracer. Keep
    # correlation off its Resource while preserving that seam and avoiding a
    # second runner configuration surface.
    provider._curie_session_id = session_id  # type: ignore[attr-defined]
    provider._curie_sandbox_id = sandbox_id or None  # type: ignore[attr-defined]
    # The validator must run before the exporting processor (registration order)
    # so the exporter only ever sees attributes the closed schema allows.
    provider.add_span_processor(_SchemaValidatingSpanProcessor())
    provider.add_span_processor(
        BatchSpanProcessor(
            exporter,
            max_queue_size=_MAX_QUEUE_SIZE,
            schedule_delay_millis=_SCHEDULE_DELAY_MILLIS,
            max_export_batch_size=_MAX_EXPORT_BATCH_SIZE,
            export_timeout_millis=_EXPORT_TIMEOUT_MILLIS,
        )
    )
    return provider


def _bounded_provider_call(provider: TracerProvider, method: str, timeout_millis: int) -> bool:
    """Call one exporter lifecycle method without trusting its wall clock bound."""

    complete = threading.Event()
    succeeded = False

    def invoke() -> None:
        nonlocal succeeded
        try:
            function = getattr(provider, method)
            result = (
                function(timeout_millis=timeout_millis) if method == "force_flush" else function()
            )
            succeeded = result is not False
        except BaseException:
            succeeded = False
        finally:
            complete.set()

    threading.Thread(target=invoke, daemon=True).start()
    return complete.wait(timeout_millis / 1000) and succeeded


def _normalize_parent(parent: Context | None) -> Context:
    """Return an explicit clean parent with SDK-compatible trace flags."""

    if parent is None:
        return Context()
    span_context = trace.get_current_span(parent).get_span_context()
    if not span_context.is_valid:
        return parent
    normalized = SpanContext(
        trace_id=span_context.trace_id,
        span_id=span_context.span_id,
        is_remote=span_context.is_remote,
        trace_flags=TraceFlags(int(span_context.trace_flags)),
        trace_state=span_context.trace_state,
    )
    return set_span_in_context(NonRecordingSpan(normalized), parent)


class RunTracer:
    """Thin wrapper over an OTel tracer emitting the runner's gen_ai span tree.

    A None provider yields a no-op tracer so callers need no branching.
    """

    def __init__(self, provider: TracerProvider | None) -> None:
        self._provider = provider
        self._session_id = getattr(provider, "_curie_session_id", None)
        self._sandbox_id = getattr(provider, "_curie_sandbox_id", None)
        self._tracer: Tracer = (
            provider.get_tracer("curie-runner")
            if provider is not None
            else trace.get_tracer("curie-runner")
        )

    @contextmanager
    def run_span(
        self,
        trace_name: str,
        model: str | None,
        session_id: str | None = None,
        user_id: str | None = None,
        approval_decision: str | None = None,
        *,
        parent: Context | None = None,
    ) -> Iterator[_GenerationSpan]:
        """Open the root ``agent.run`` span and its child ``llm.generation`` span.

        ``session_id`` (the ACI ``CURIE_SESSION_ID``, one Slack thread) and
        ``user_id`` (the inbound event's Slack user) are stamped on the root span
        so Langfuse maps them to its Sessions and Users features respectively.
        Langfuse reads these from the trace-root span, exactly as it does
        ``langfuse.trace.name``; an empty or absent value is omitted rather than
        stamped, so a turn with no event user (eval runs etc.) carries no user id.

        ``approval_decision`` (ADR-0076 Stone 3, #889) is the authority-free
        CURIE_APPROVAL_DECISION fact -- present only when this turn is
        resuming a resolved approval -- stamped unconditionally when given so
        an operator can see the outcome from the trace.
        """

        # An absent carrier is deliberately a fresh root. Passing ``None`` to
        # start_as_current_span would inherit ambient task context and let an
        # unrelated request become the parent of this long lived session.
        safe_parent = _normalize_parent(parent)
        with self._tracer.start_as_current_span(
            "agent.run",
            context=safe_parent,
            kind=SpanKind.SERVER,
            record_exception=False,
            set_status_on_exception=False,
        ) as root:
            span: _GenerationSpan | None = None
            try:
                _set(root, SpanAttributeKey.TRACE_NAME, trace_name)
                if session_id:
                    _set(root, SpanAttributeKey.SESSION_ID, session_id)
                effective_session_id = session_id or self._session_id
                if effective_session_id:
                    _set(root, SpanAttributeKey.CURIE_SESSION_ID, effective_session_id)
                if self._sandbox_id:
                    _set(root, SpanAttributeKey.CURIE_SANDBOX_ID, self._sandbox_id)
                if user_id:
                    _set(root, SpanAttributeKey.USER_ID, user_id)
                if approval_decision:
                    _set(root, SpanAttributeKey.APPROVAL_DECISION, approval_decision)
                with self._tracer.start_as_current_span(
                    "llm.generation",
                    record_exception=False,
                    set_status_on_exception=False,
                ) as gen:
                    span = _GenerationSpan(self._tracer, root, gen)
                    try:
                        # Stamp the configured model at span open when CURIE_MODEL is
                        # set; otherwise the span stays model-less until the SDK reports
                        # the actual model on its first assistant message (record_model).
                        span.record_model(model)
                        yield span
                    except BaseException:
                        # Classify both spans while the generation is still open.
                        # Doing this in the outer handler is too late: exiting
                        # start_as_current_span ends the generation first.
                        span.set_failed()
                        raise
                    else:
                        span.set_succeeded()
            except BaseException:
                if span is None:
                    root.set_status(StatusCode.ERROR)
                raise

    def force_flush(self, *, timeout_millis: int = _EXPORT_TIMEOUT_MILLIS) -> bool:
        """Flush current spans within a hard wall clock bound."""

        if self._provider is None:
            return True
        return _bounded_provider_call(self._provider, "force_flush", timeout_millis)

    def shutdown(self, *, timeout_millis: int = _EXPORT_TIMEOUT_MILLIS) -> None:
        """Flush and shut down the exporter within a hard wall clock bound."""

        if self._provider is not None:
            _bounded_provider_call(self._provider, "shutdown", timeout_millis)


class _GenerationSpan:
    """Handle for annotating the generation span and emitting tool child spans."""

    def __init__(self, tracer: Tracer, root: Any, span: Any) -> None:
        self._tracer = tracer
        self._root = root
        self._span = span
        self._model_recorded = False

    def set_succeeded(self) -> None:
        """Mark both generation and run successful unless already classified."""

        for span in (self._span, self._root):
            if span.is_recording() and span.status.status_code is StatusCode.UNSET:
                span.set_status(StatusCode.OK)

    def set_failed(self) -> None:
        """Mark both generation and run failed without exporting error text."""

        self._span.set_status(StatusCode.ERROR)
        self._root.set_status(StatusCode.ERROR)

    def record_model(self, model: str | None) -> None:
        """Stamp the generation model attribute once, first non-empty value wins.

        Langfuse only maps ``llm.generation`` to a GENERATION observation (and so
        records the ``gen_ai.usage.*`` token counts) when the span carries a model
        attribute; a model-less span ingests as an untyped SPAN with zero usage.
        The configured ``CURIE_MODEL`` is stamped at span open when set; when it
        is unset the runner backfills the actual model the SDK reports on its first
        assistant message, so the generation is typed either way. Only genuinely
        unknown models leave the attribute absent.
        """

        if self._model_recorded or not model:
            return
        _set(self._span, SpanAttributeKey.REQUEST_MODEL, model)
        _set(self._span, SpanAttributeKey.MODEL, model)
        self._model_recorded = True

    def record_usage(self, usage: Mapping[str, Any] | None) -> None:
        """Attach gen_ai token-usage attributes from an SDK usage mapping.

        Prompt-cache tokens (``cache_read_input_tokens`` /
        ``cache_creation_input_tokens``, the Anthropic wire shape preserved even
        through OpenRouter) are recorded alongside the plain input/output counts,
        so a warm thread's cache reuse is observable in the trace rather than
        silently folded away. This is the signal the prompt-cache smoke test
        asserts on: a translating gateway that silently breaks caching shows up
        here as a warm turn with zero cache-read tokens.
        """

        if not usage:
            return
        for usage_field, attribute_key in _USAGE_ATTRIBUTE_KEYS.items():
            value = usage.get(usage_field)
            if isinstance(value, int):
                _set(self._span, attribute_key, value)

    def tool_span(self, tool_name: str) -> None:
        """Emit a short ``execute_tool`` child span for one tool call."""

        with self._tracer.start_as_current_span(
            "execute_tool",
            record_exception=False,
            set_status_on_exception=False,
        ) as tool:
            _set(tool, SpanAttributeKey.TOOL_NAME, tool_name)
            _set(tool, SpanAttributeKey.OPERATION_NAME, "execute_tool")
