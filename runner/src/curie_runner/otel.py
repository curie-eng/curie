"""OTel tracing for the runner: gen_ai spans exported by standard OTLP.

Each turn is a root ``agent.run`` (SERVER) span carrying a
``langfuse.trace.name``. Provider waits are duration-bearing ``llm.generation``
children and tool waits are duration-bearing ``execute_tool`` children; both are
siblings under the root. The SDK's opaque call ids are retained only in the
in-process matching map, while exported round and call identities are bounded
turn-local integers.

Traces go to the OTel Collector over OTLP, never directly to Langfuse: the
collector is the adapter that authenticates and forwards (Langfuse OTLP ingest is
HTTP-only). Endpoint, headers, and protocol come from the standard
``OTEL_EXPORTER_OTLP_*`` variables via ``SessionConfig.otel``; signal-specific
configuration wins over the general variables. When no endpoint is configured
the tracer is a no-op, so unit tests and offline runs neither export nor fail.

Per ADR-0076, every attribute this module attaches comes from the shared closed
``SpanAttributeKey`` enum rather than a bare string, so a future call site
with an unlisted key is a construction-time error, not a silent addition to the
wire shape. ``SCHEMA_VERSION`` is bumped only when a key is removed, renamed, or
changes value type; a new optional key is additive and does not bump it.
``SPAN_ATTRIBUTE_VALUE_TYPES`` declares each key's value type, the mirror the
drift gate diffs to catch a retype.
"""

from __future__ import annotations

import os
import threading
import time
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from typing import Any, cast

from aci_protocol import BootEnv, OtelConfig
from curie_telemetry import (
    build_otlp_span_exporter,
    build_resource,
    deployment_environment,
    service_instance_id,
)
from curie_telemetry_schema import SpanAttributeKey as SpanAttributeKey
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


# ADR-0076 decision 2: a value-type change to an existing key is a breaking,
# version-bump-worthy change exactly like a remove or rename, so it needs its
# own source of truth to diff against -- the type half of the closed schema,
# parallel to ``SpanAttributeKey`` being the key half. Every member must
# appear here exactly once, mapped to its value-type name ("str" or "int");
# usage counts, TTFT milliseconds, and turn-local round/call indices are the
# integer members. No boolean or float attributes cross this boundary.
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
    SpanAttributeKey.PHASE: "str",
    SpanAttributeKey.PHASE_START_KIND: "str",
    SpanAttributeKey.PHASE_END_KIND: "str",
    SpanAttributeKey.TERMINAL_CAUSE: "str",
    SpanAttributeKey.TERMINAL_STATUS: "str",
    SpanAttributeKey.GENERATION_TTFT_MS: "int",
    SpanAttributeKey.GENERATION_ROUND: "int",
    SpanAttributeKey.TOOL_CALL_INDEX: "int",
    SpanAttributeKey.TOOL_OUTCOME: "str",
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
        """Open a root ``agent.run`` span and yield its lazy phase handle.

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
                span = _GenerationSpan(self._tracer, root, model)
                try:
                    yield span
                except BaseException:
                    span.set_abandoned()
                    raise
                else:
                    span.finish_if_needed()
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
    """Lazy, turn-local phase manager yielded by :meth:`RunTracer.run_span`.

    The private class name is retained for compatibility with the runner's
    existing type annotations and direct instrumentation callers. It now owns
    every provider/tool interval beneath one root rather than representing one
    eagerly opened generation.
    """

    _ABORT_CAUSES = frozenset(("aborted_streaming", "aborted_tools"))

    def __init__(self, tracer: Tracer, root: Any, model: str | None) -> None:
        self._tracer = tracer
        self._root = root
        self._root_context = set_span_in_context(root)
        self._configured_model = model
        self._active_generation: Any | None = None
        self._deferred_generation_end_ns: int | None = None
        self._generation_started_ns = 0
        self._generation_model_recorded = False
        self._generation_ttft_recorded = False
        self._generation_usage: dict[str, int] = {}
        self._generation_round = 0
        self._active_tools: dict[str, tuple[Any, int]] = {}
        self._tool_call_index = 0
        self._result_observed = False
        self._result_abort_cause: str | None = None
        self._terminal = False
        self._has_activity = False

    @property
    def result_observed(self) -> bool:
        """Whether the SDK supplied a terminal ResultMessage boundary."""

        return self._result_observed

    def query_observed(self) -> None:
        """Open generation round one for the initial provider wait.

        A steer joins an already active wait, so repeated calls deliberately do
        nothing. A provider phase is never opened while a tool is still pending.
        """

        if self._terminal or self._active_generation is not None or self._active_tools:
            return
        self._open_generation("query_observed")

    def record_first_response_boundary(self) -> None:
        """Record TTFT once on the active generation from a stripped boundary."""

        if self._active_generation is None or self._generation_ttft_recorded:
            return
        elapsed_ms = max(0, (time.monotonic_ns() - self._generation_started_ns) // 1_000_000)
        _set(
            self._active_generation,
            SpanAttributeKey.GENERATION_TTFT_MS,
            int(elapsed_ms),
        )
        self._generation_ttft_recorded = True

    def record_assistant(
        self,
        model: str | None,
        usage: Mapping[str, Any] | None,
    ) -> None:
        """Accumulate one assistant message's model and token usage."""

        if self._active_generation is None:
            return
        self._record_model(model)
        self._record_usage(usage)

    def set_succeeded(self) -> None:
        """Compatibility helper: close an unfinished active run as completed."""

        self._finish("completed", "succeeded", failed=False)

    def set_failed(self) -> None:
        """Close an unfinished active run as a classified failure."""

        self._finish("classified_failure", "failed", failed=True)

    def set_abandoned(self) -> None:
        """Close unfinished phases after an exception or generator abandonment."""

        self._finish("abandoned", "abandoned", failed=True)

    def finish_if_needed(self) -> None:
        """Complete direct instrumentation while leaving an empty root lazy."""

        if self._has_activity and not self._terminal:
            self.set_succeeded()

    def finish_turn(
        self,
        *,
        interrupt_requested: bool,
        classified_failure: bool,
        timeout_requested: bool = False,
        approval_paused: bool = False,
        completed_without_result: bool = False,
    ) -> None:
        """Apply the closed terminal mapping after the ACI final is decided."""

        if timeout_requested:
            self._finish("runner_timeout", "failed", failed=True)
        elif interrupt_requested:
            self._finish("interrupt_requested", "cancelled", failed=False)
        elif approval_paused:
            self._finish("approval_required", "paused", failed=False)
        elif self._result_abort_cause is not None:
            self._finish(
                self._result_abort_cause,
                "failed",
                failed=True,
            )
        elif classified_failure:
            self.set_failed()
        elif self._result_observed or completed_without_result:
            self.set_succeeded()
        else:
            self.set_abandoned()

    def result_boundary_observed(
        self,
        *,
        failed: bool,
        terminal_reason: str | None,
        approval_paused: bool = False,
    ) -> None:
        """Close the active generation at an SDK ResultMessage boundary."""

        self._result_observed = True
        if terminal_reason in self._ABORT_CAUSES:
            self._result_abort_cause = terminal_reason
        self._close_deferred_generation()
        self._close_generation(
            "result_observed",
            failed=failed and not approval_paused,
        )

    def record_model(self, model: str | None) -> None:
        """Compatibility setter for direct instrumentation callers.

        The session path uses :meth:`record_assistant`, which never fabricates a
        provider phase. Direct callers historically received an eager generation,
        so this shim lazily opens their first generation on demand.
        """

        self._ensure_compat_generation()
        self._record_model(model)

    def record_usage(self, usage: Mapping[str, Any] | None) -> None:
        """Compatibility setter for a direct generation usage observation.

        Session-driven telemetry uses per-AssistantMessage increments through
        :meth:`record_assistant`; ResultMessage turn totals never call this path.
        """

        self._ensure_compat_generation()
        self._record_usage(usage)

    def tool_use(self, call_id: str, tool_name: str) -> None:
        """Close provider wait and begin an inferred SDK tool interval."""

        if self._terminal:
            return
        self._close_deferred_generation()
        if call_id in self._active_tools:
            return
        self._close_generation("tool_use_observed", failed=False)
        self._open_tool(call_id, tool_name)

    def streamed_tool_use(
        self,
        call_id: str,
        tool_name: str,
        *,
        observed_time_ns: int,
    ) -> None:
        """Begin a streamed tool interval while deferring generation closure."""

        if self._terminal or call_id in self._active_tools:
            return
        if self._active_generation is not None:
            pending_end = self._deferred_generation_end_ns
            self._deferred_generation_end_ns = (
                observed_time_ns
                if pending_end is None
                else min(pending_end, observed_time_ns)
            )
        self._open_tool(
            call_id,
            tool_name,
            start_time_ns=observed_time_ns,
        )

    def _open_tool(
        self,
        call_id: str,
        tool_name: str,
        *,
        start_time_ns: int | None = None,
    ) -> None:
        self._tool_call_index += 1
        if start_time_ns is None:
            tool = self._tracer.start_span(
                "execute_tool",
                context=self._root_context,
                record_exception=False,
                set_status_on_exception=False,
            )
        else:
            tool = self._tracer.start_span(
                "execute_tool",
                context=self._root_context,
                start_time=start_time_ns,
                record_exception=False,
                set_status_on_exception=False,
            )
        _set(tool, SpanAttributeKey.PHASE, "tool_wait")
        _set(tool, SpanAttributeKey.PHASE_START_KIND, "tool_use_inferred")
        _set(tool, SpanAttributeKey.TOOL_CALL_INDEX, self._tool_call_index)
        if isinstance(tool_name, str) and tool_name:
            _set(tool, SpanAttributeKey.TOOL_NAME, tool_name)
        _set(tool, SpanAttributeKey.OPERATION_NAME, "execute_tool")
        self._active_tools[call_id] = (tool, self._tool_call_index)
        self._has_activity = True
        _set(self._root, SpanAttributeKey.PHASE, "tool_wait")

    def tool_result(self, call_id: str, *, failed: bool) -> None:
        """End a matching tool interval, then infer the next provider wait."""

        self._close_deferred_generation()
        pending = self._active_tools.pop(call_id, None)
        if pending is None:
            return
        tool, _index = pending
        self._end_tool(
            tool,
            end_kind="tool_result_inferred",
            outcome="error" if failed else "success",
            failed=failed,
        )
        if not self._active_tools and not self._terminal:
            self._open_generation("tool_result_inferred")

    def tool_span(self, tool_name: str) -> None:
        """Compatibility shim for legacy immediate tool instrumentation."""

        call_id = f"compat:{self._tool_call_index + 1}"
        self.tool_use(call_id, tool_name)
        pending = self._active_tools.pop(call_id, None)
        if pending is None:
            return
        tool, _index = pending
        self._end_tool(
            tool,
            end_kind="tool_result_inferred",
            outcome="success",
            failed=False,
        )

    def _ensure_compat_generation(self) -> None:
        if self._active_generation is None and not self._active_tools and not self._terminal:
            self._open_generation("query_observed")

    def _open_generation(self, start_kind: str) -> None:
        if self._active_generation is not None or self._active_tools or self._terminal:
            return
        self._generation_round += 1
        self._active_generation = self._tracer.start_span(
            "llm.generation",
            context=self._root_context,
            record_exception=False,
            set_status_on_exception=False,
        )
        self._generation_started_ns = time.monotonic_ns()
        self._generation_model_recorded = False
        self._generation_ttft_recorded = False
        self._generation_usage = {}
        _set(self._active_generation, SpanAttributeKey.PHASE, "provider_wait")
        _set(self._active_generation, SpanAttributeKey.PHASE_START_KIND, start_kind)
        _set(
            self._active_generation,
            SpanAttributeKey.GENERATION_ROUND,
            self._generation_round,
        )
        _set(self._root, SpanAttributeKey.PHASE, "provider_wait")
        self._has_activity = True
        self._record_model(self._configured_model)

    def _record_model(self, model: str | None) -> None:
        if (
            self._active_generation is None
            or self._generation_model_recorded
            or not isinstance(model, str)
            or not model
        ):
            return
        _set(self._active_generation, SpanAttributeKey.REQUEST_MODEL, model)
        _set(self._active_generation, SpanAttributeKey.MODEL, model)
        self._generation_model_recorded = True

    def _record_usage(self, usage: Mapping[str, Any] | None) -> None:
        if self._active_generation is None or not usage:
            return
        for usage_field, attribute_key in _USAGE_ATTRIBUTE_KEYS.items():
            value = usage.get(usage_field)
            if type(value) is not int or value < 0:
                continue
            total = self._generation_usage.get(usage_field, 0) + value
            self._generation_usage[usage_field] = total
            _set(self._active_generation, attribute_key, total)

    def _close_deferred_generation(self) -> None:
        end_time_ns = self._deferred_generation_end_ns
        if end_time_ns is None:
            return
        self._close_generation(
            "tool_use_observed",
            failed=False,
            end_time_ns=end_time_ns,
        )

    def _close_generation(
        self,
        end_kind: str,
        *,
        failed: bool,
        end_time_ns: int | None = None,
    ) -> None:
        span = self._active_generation
        self._deferred_generation_end_ns = None
        if span is None:
            return
        _set(span, SpanAttributeKey.PHASE_END_KIND, end_kind)
        span.set_status(StatusCode.ERROR if failed else StatusCode.OK)
        if end_time_ns is None:
            span.end()
        else:
            span.end(end_time=end_time_ns)
        self._active_generation = None

    def _end_tool(
        self,
        tool: Any,
        *,
        end_kind: str,
        outcome: str,
        failed: bool,
    ) -> None:
        _set(tool, SpanAttributeKey.PHASE_END_KIND, end_kind)
        _set(tool, SpanAttributeKey.TOOL_OUTCOME, outcome)
        tool.set_status(StatusCode.ERROR if failed else StatusCode.OK)
        tool.end()

    def _finish(self, cause: str, status: str, *, failed: bool) -> None:
        if self._terminal:
            return
        self._close_deferred_generation()
        self._close_generation("terminal_inferred", failed=failed)
        for call_id, (tool, _index) in sorted(
            self._active_tools.items(), key=lambda item: item[1][1]
        ):
            del self._active_tools[call_id]
            self._end_tool(
                tool,
                end_kind="terminal_inferred",
                outcome="cancelled",
                failed=failed,
            )
        _set(self._root, SpanAttributeKey.TERMINAL_CAUSE, cause)
        _set(self._root, SpanAttributeKey.TERMINAL_STATUS, status)
        self._root.set_status(StatusCode.ERROR if failed else StatusCode.OK)
        self._terminal = True
