"""OTel: the gen_ai span tree is emitted for a turn; exporter wiring is gated."""

import time
from typing import Any

import anyio
import pytest
from aci_protocol import (
    ErrorEvent,
    Event,
    Final,
    OtelConfig,
    SessionStatus,
    parse_ndjson,
    parse_ndjson_line,
)
from curie_runner import RunTracer, SideEffectClassifier, build_tracer_provider
from curie_runner import otel as otel_module
from curie_runner import session as session_module
from curie_runner.fake import FakeModelSession
from curie_runner.otel import _SchemaValidatingSpanProcessor
from curie_runner.session import SessionRunner
from opentelemetry.context import Context
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import (
    OTLPSpanExporter as GrpcOTLPSpanExporter,
)
from opentelemetry.exporter.otlp.proto.http.trace_exporter import (
    OTLPSpanExporter as HttpOTLPSpanExporter,
)
from opentelemetry.sdk.trace import ReadableSpan, TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.trace import (
    NonRecordingSpan,
    SpanContext,
    StatusCode,
    TraceFlags,
    TraceState,
    set_span_in_context,
)


def test_run_emits_agent_generation_and_tool_spans() -> None:
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))

    runner = SessionRunner(
        session_factory=FakeModelSession,  # default_turn: text + Bash tool + result usage
        ceiling=0,
        tracer=RunTracer(provider),
        classifier=SideEffectClassifier(),
        trace_name="curie-run:test",
        model="fake-model",
    )

    async def go() -> None:
        await runner.start()
        async for _ in runner.run_turn(Event(type="message", text="go", user="U", ts="1")):
            pass

    anyio.run(go)

    spans = {s.name: s for s in exporter.get_finished_spans()}
    assert {"agent.run", "llm.generation", "execute_tool"} <= set(spans)
    assert spans["agent.run"].attributes["langfuse.trace.name"] == "curie-run:test"
    gen = spans["llm.generation"]
    assert gen.attributes["gen_ai.request.model"] == "fake-model"
    assert gen.attributes["gen_ai.usage.output_tokens"] == 8
    assert spans["execute_tool"].attributes["gen_ai.tool.name"] == "Bash"


def test_generation_model_backfilled_from_sdk_when_unconfigured() -> None:
    # CURIE_MODEL unset (model=None) must NOT leave the generation span
    # model-less: Langfuse would then ingest it as an untyped span and drop token
    # usage to zero. The runner backfills the model the SDK reports on its first
    # assistant message (the fake scripts model="fake-model"), so the span stays a
    # typed generation with usage intact.
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))

    runner = SessionRunner(
        session_factory=FakeModelSession,
        ceiling=0,
        tracer=RunTracer(provider),
        classifier=SideEffectClassifier(),
        trace_name="curie-run:test",
        model=None,
    )

    async def go() -> None:
        await runner.start()
        async for _ in runner.run_turn(Event(type="message", text="go", user="U", ts="1")):
            pass

    anyio.run(go)

    gen = {s.name: s for s in exporter.get_finished_spans()}["llm.generation"]
    assert gen.attributes["gen_ai.request.model"] == "fake-model"
    # The usage counts only land on a model-bearing generation, so their presence
    # is the end-to-end proof the span was typed as a generation, not a bare span.
    assert gen.attributes["gen_ai.usage.output_tokens"] == 8


def test_run_stamps_langfuse_session_and_user_ids() -> None:
    # Langfuse maps langfuse.session.id -> Sessions and langfuse.user.id -> Users,
    # but only from the trace-root span (same as langfuse.trace.name). The session
    # id is stable per session; the user id is the inbound event's Slack user.
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))

    runner = SessionRunner(
        session_factory=FakeModelSession,
        ceiling=0,
        tracer=RunTracer(provider),
        classifier=SideEffectClassifier(),
        trace_name="curie-run:test",
        session_id="agent-abc-thread-123",
        model="fake-model",
    )

    async def go() -> None:
        await runner.start()
        async for _ in runner.run_turn(Event(type="message", text="go", user="U42", ts="1")):
            pass

    anyio.run(go)

    root = {s.name: s for s in exporter.get_finished_spans()}["agent.run"]
    assert root.attributes["langfuse.session.id"] == "agent-abc-thread-123"
    assert root.attributes["langfuse.user.id"] == "U42"


def test_run_omits_langfuse_user_id_when_event_user_empty() -> None:
    # A turn with no event user (eval runs etc.) omits the attribute rather than
    # stamping an empty value; the session id still lands.
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))

    runner = SessionRunner(
        session_factory=FakeModelSession,
        ceiling=0,
        tracer=RunTracer(provider),
        classifier=SideEffectClassifier(),
        trace_name="curie-run:test",
        session_id="agent-abc-thread-123",
        model="fake-model",
    )

    async def go() -> None:
        await runner.start()
        async for _ in runner.run_turn(Event(type="message", text="go", user="", ts="1")):
            pass

    anyio.run(go)

    root = {s.name: s for s in exporter.get_finished_spans()}["agent.run"]
    assert "langfuse.user.id" not in root.attributes
    assert root.attributes["langfuse.session.id"] == "agent-abc-thread-123"


def test_run_stamps_approval_decision_when_resuming_a_resolved_approval() -> None:
    # ADR-0076 Stone 3 (#889): the authority-free CURIE_APPROVAL_DECISION fact
    # threaded from the worker lands on the root span, so an operator can see
    # the outcome of an approval gate from the trace.
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))

    runner = SessionRunner(
        session_factory=FakeModelSession,
        ceiling=0,
        tracer=RunTracer(provider),
        classifier=SideEffectClassifier(),
        trace_name="curie-run:test",
        model="fake-model",
        approval_decision="rejected",
    )

    async def go() -> None:
        await runner.start()
        async for _ in runner.run_turn(Event(type="message", text="go", user="U", ts="1")):
            pass

    anyio.run(go)

    root = {s.name: s for s in exporter.get_finished_spans()}["agent.run"]
    assert root.attributes["gen_ai.approval.decision"] == "rejected"


def test_run_omits_approval_decision_on_an_ordinary_turn() -> None:
    # No approval was resumed, so the attribute is absent rather than stamped
    # empty or None -- the ordinary-turn default posture.
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))

    runner = SessionRunner(
        session_factory=FakeModelSession,
        ceiling=0,
        tracer=RunTracer(provider),
        classifier=SideEffectClassifier(),
        trace_name="curie-run:test",
        model="fake-model",
    )

    async def go() -> None:
        await runner.start()
        async for _ in runner.run_turn(Event(type="message", text="go", user="U", ts="1")):
            pass

    anyio.run(go)

    root = {s.name: s for s in exporter.get_finished_spans()}["agent.run"]
    assert "gen_ai.approval.decision" not in root.attributes


@pytest.mark.parametrize("otel", (OtelConfig(), OtelConfig(endpoint="")))
def test_tracer_provider_none_without_endpoint(otel: OtelConfig) -> None:
    assert build_tracer_provider(otel, "s1") is None


def test_tracer_provider_built_with_endpoint(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("OTEL_EXPORTER_OTLP_PROTOCOL", raising=False)
    monkeypatch.delenv("OTEL_EXPORTER_OTLP_TRACES_PROTOCOL", raising=False)
    otel = OtelConfig(endpoint="http://localhost:24318")
    provider = build_tracer_provider(otel, "s1")
    assert isinstance(provider, TracerProvider)
    provider.shutdown()


def test_tracer_provider_accepts_standard_traces_endpoint_without_typed_fallback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(
        "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT",
        "http://otel-collector.example.com:4318/v1/traces",
    )
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_TRACES_PROTOCOL", "http/protobuf")

    provider = build_tracer_provider(OtelConfig(), "s1")

    assert provider is not None
    provider.shutdown()


def test_tracer_provider_honors_standard_sdk_disable_switch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("OTEL_SDK_DISABLED", "true")
    monkeypatch.setenv(
        "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT",
        "http://otel-collector.example.com:4318/v1/traces",
    )

    assert (
        build_tracer_provider(
            OtelConfig(endpoint="http://typed-fallback.example.com:4318"),
            "s1",
        )
        is None
    )


@pytest.mark.parametrize(
    ("protocol", "expected_type"),
    (
        ("grpc", GrpcOTLPSpanExporter),
        ("http/protobuf", HttpOTLPSpanExporter),
    ),
)
def test_tracer_provider_honors_standard_protocol_selection(
    protocol: str,
    expected_type: type[object],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("OTEL_EXPORTER_OTLP_PROTOCOL", raising=False)
    monkeypatch.delenv("OTEL_EXPORTER_OTLP_TRACES_PROTOCOL", raising=False)
    endpoint = (
        "http://otel-collector.example.com:4317"
        if protocol == "grpc"
        else "http://otel-collector.example.com:4318"
    )
    provider = build_tracer_provider(
        OtelConfig(endpoint=endpoint, protocol=protocol),
        "s1",
    )
    assert provider is not None
    processors = tuple(provider._active_span_processor._span_processors)  # noqa: SLF001
    exporter = processors[1].span_exporter
    assert isinstance(exporter, expected_type)
    provider.shutdown()


def test_signal_protocol_overrides_general_runner_protocol(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_PROTOCOL", "grpc")
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_TRACES_PROTOCOL", "http/protobuf")
    monkeypatch.setenv(
        "OTEL_EXPORTER_OTLP_ENDPOINT",
        "http://otel-collector.example.com:4318",
    )
    provider = build_tracer_provider(
        OtelConfig(
            endpoint="http://otel-collector.example.com:4318",
            protocol="grpc",
        ),
        "s1",
    )
    assert provider is not None
    processors = tuple(provider._active_span_processor._span_processors)  # noqa: SLF001
    assert isinstance(processors[1].span_exporter, HttpOTLPSpanExporter)
    provider.shutdown()


def test_tracer_provider_registers_validator_before_bounded_batch_export() -> None:
    otel = OtelConfig(endpoint="http://otel-collector.example.com:4318")
    provider = build_tracer_provider(otel, "s1")
    assert provider is not None

    active = provider._active_span_processor  # noqa: SLF001
    processors = tuple(active._span_processors)  # noqa: SLF001
    assert isinstance(processors[0], _SchemaValidatingSpanProcessor)
    assert isinstance(processors[1], BatchSpanProcessor)
    assert not any(isinstance(processor, SimpleSpanProcessor) for processor in processors)
    provider.shutdown()


def _remote_parent() -> tuple[Context, SpanContext]:
    span_context = SpanContext(
        trace_id=0x1234567890ABCDEF1234567890ABCDEF,
        span_id=0x1234567890ABCDEF,
        is_remote=True,
        trace_flags=TraceFlags.SAMPLED,
        trace_state=TraceState(),
    )
    return set_span_in_context(NonRecordingSpan(span_context)), span_context


def test_run_span_uses_explicit_parent_instead_of_ambient_context() -> None:
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    tracer = RunTracer(provider)
    parent, parent_span_context = _remote_parent()
    ambient_tracer = TracerProvider().get_tracer("ambient")

    with ambient_tracer.start_as_current_span("ambient"):
        with tracer.run_span("curie-run:test", "fake-model", parent=parent):
            pass

    spans = {span.name: span for span in exporter.get_finished_spans()}
    root = spans["agent.run"]
    generation = spans["llm.generation"]
    assert root.context is not None
    assert root.context.trace_id == parent_span_context.trace_id
    assert root.parent is not None
    assert root.parent.span_id == parent_span_context.span_id
    assert generation.parent is not None
    assert generation.parent.span_id == root.context.span_id


def test_run_span_with_missing_parent_starts_a_safe_root() -> None:
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    tracer = RunTracer(provider)
    ambient_tracer = TracerProvider().get_tracer("ambient")

    with ambient_tracer.start_as_current_span("ambient") as ambient:
        with tracer.run_span("curie-run:test", "fake-model", parent=None):
            pass

    root = {span.name: span for span in exporter.get_finished_spans()}["agent.run"]
    assert root.context is not None
    assert root.parent is None
    assert root.context.trace_id != ambient.get_span_context().trace_id


def _run_and_export(
    session_factory: Any = FakeModelSession,
) -> tuple[list[object], dict[str, ReadableSpan]]:
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    runner = SessionRunner(
        session_factory=session_factory,
        ceiling=0,
        tracer=RunTracer(provider),
        classifier=SideEffectClassifier(),
        trace_name="curie-run:test",
        model="fake-model",
    )

    async def go() -> list[object]:
        await runner.start()
        lines = [
            line
            async for line in runner.run_turn(
                Event(type="message", text="go", user="U0EXAMPLE1", ts="1")
            )
        ]
        await runner.close()
        return parse_ndjson("".join(lines))

    events = anyio.run(go)
    spans = {span.name: span for span in exporter.get_finished_spans()}
    return events, spans


def test_successful_turn_sets_explicit_ok_status() -> None:
    events, spans = _run_and_export()
    terminal = events[-1]
    assert isinstance(terminal, Final)
    assert terminal.status is SessionStatus.DONE
    assert spans["agent.run"].status.status_code is StatusCode.OK
    assert spans["llm.generation"].status.status_code is StatusCode.OK


def test_caught_runner_failure_sets_explicit_error_status() -> None:
    def fail_script() -> list[object]:
        raise RuntimeError("placeholder runner failure")

    events, spans = _run_and_export(lambda: FakeModelSession(script_factory=fail_script))
    assert any(isinstance(event, ErrorEvent) for event in events)
    terminal = events[-1]
    assert isinstance(terminal, Final)
    assert terminal.status is SessionStatus.CLASSIFIED_FAILURE
    assert spans["agent.run"].status.status_code is StatusCode.ERROR
    assert spans["llm.generation"].status.status_code is StatusCode.ERROR


def test_escaping_exception_exports_only_bounded_error_status() -> None:
    """OTel must not auto-record exception text or a stacktrace on span exit."""

    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    detail = "private-runner-detail-PLACEHOLDER"

    with pytest.raises(RuntimeError, match=detail):
        with RunTracer(provider).run_span("curie-run:test", "fake-model"):
            raise RuntimeError(detail)

    spans = {span.name: span for span in exporter.get_finished_spans()}
    for name in ("agent.run", "llm.generation"):
        span = spans[name]
        assert span.status.status_code is StatusCode.ERROR
        assert span.status.description is None
        assert not any(event.name == "exception" for event in span.events)
        assert detail not in repr((span.attributes, span.events, span.status))


def test_abandoned_turn_reports_interrupted_not_stale_prior_success(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    points: list[tuple[str, dict[str, str]]] = []

    def capture(
        name: str,
        _value: float = 1,
        *,
        attributes: dict[str, str],
    ) -> None:
        points.append((name, attributes))

    monkeypatch.setattr(session_module, "record_metric", capture)
    runner = SessionRunner(
        session_factory=FakeModelSession,
        ceiling=0,
        tracer=RunTracer(None),
        classifier=SideEffectClassifier(),
        trace_name="curie-run:test",
        model="fake-model",
    )

    async def go() -> None:
        await runner.start()
        async for _ in runner.run_turn(
            Event(type="message", text="complete", user="U0EXAMPLE1", ts="1")
        ):
            pass
        abandoned = runner.run_turn(
            Event(type="message", text="abandon", user="U0EXAMPLE1", ts="2")
        )
        await anext(abandoned)
        await abandoned.aclose()

        terminally_delivered = runner.run_turn(
            Event(type="message", text="terminal", user="U0EXAMPLE1", ts="3")
        )
        async for line in terminally_delivered:
            if isinstance(parse_ndjson_line(line), Final):
                break
        await terminally_delivered.aclose()
        await runner.close()

    anyio.run(go)
    outcomes = [
        attributes["outcome"]
        for name, attributes in points
        if name == "curie.turn.completed"
    ]
    assert outcomes == ["done", "interrupted", "done"]


class _SlowProvider:
    def __init__(self) -> None:
        self._provider = TracerProvider()

    def get_tracer(self, name: str) -> Any:
        return self._provider.get_tracer(name)

    def force_flush(self, timeout_millis: int) -> bool:
        time.sleep(0.2)
        return False

    def shutdown(self) -> None:
        time.sleep(0.2)


def test_force_flush_and_shutdown_are_wall_clock_bounded() -> None:
    tracer = RunTracer(_SlowProvider())  # type: ignore[arg-type]

    started = time.monotonic()
    assert tracer.force_flush(timeout_millis=20) is False
    force_flush_elapsed = time.monotonic() - started

    started = time.monotonic()
    tracer.shutdown(timeout_millis=20)
    shutdown_elapsed = time.monotonic() - started

    assert force_flush_elapsed < 0.15
    assert shutdown_elapsed < 0.15


def test_resource_uses_shared_stable_identity_without_correlation_ids(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(
        "OTEL_RESOURCE_ATTRIBUTES",
        "deployment.environment.name=production,service.name=ignored",
    )
    otel = OtelConfig(endpoint="http://localhost:24318")
    provider = build_tracer_provider(otel, "s1", "sandbox-abc")
    assert provider is not None
    attrs = provider.resource.attributes
    assert attrs["service.namespace"] == "curie"
    assert attrs["service.name"] == "curie-runner"
    assert attrs["service.version"] == "0.0.0"
    assert str(attrs["service.instance.id"]).startswith("curie-runner-")
    assert attrs["deployment.environment.name"] == "production"
    assert "curie.session_id" not in attrs
    assert "curie.sandbox_id" not in attrs
    provider.shutdown()


def test_session_and_sandbox_correlation_are_root_span_attributes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    otel = OtelConfig(endpoint="http://localhost:24318")
    monkeypatch.setattr(
        otel_module,
        "build_otlp_span_exporter",
        lambda *_args, **_kwargs: InMemorySpanExporter(),
    )
    provider = build_tracer_provider(otel, "s1", "sandbox-abc")
    assert provider is not None
    exporter = InMemorySpanExporter()
    provider.add_span_processor(SimpleSpanProcessor(exporter))

    with RunTracer(provider).run_span("curie-run:test", "fake-model", session_id="s1"):
        pass

    root = {span.name: span for span in exporter.get_finished_spans()}["agent.run"]
    assert root.attributes["curie.session_id"] == "s1"
    assert root.attributes["curie.sandbox_id"] == "sandbox-abc"
    provider.shutdown()


@pytest.mark.parametrize("sandbox_id", (None, ""))
def test_root_span_omits_sandbox_id_when_absent_or_empty(
    sandbox_id: str | None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        otel_module,
        "build_otlp_span_exporter",
        lambda *_args, **_kwargs: InMemorySpanExporter(),
    )
    provider = build_tracer_provider(
        OtelConfig(endpoint="http://localhost:24318"),
        "s1",
        sandbox_id,
    )
    assert provider is not None
    exporter = InMemorySpanExporter()
    provider.add_span_processor(SimpleSpanProcessor(exporter))

    with RunTracer(provider).run_span("curie-run:test", "fake-model", session_id="s1"):
        pass

    root = {span.name: span for span in exporter.get_finished_spans()}["agent.run"]
    assert "curie.sandbox_id" not in root.attributes
    provider.shutdown()


def test_resource_stamps_schema_version() -> None:
    # ADR-0076: every exported trace carries the closed schema's version so a
    # consumer can tell which attribute-key set it was produced under.
    otel = OtelConfig(endpoint="http://localhost:24318")
    provider = build_tracer_provider(otel, "s1")
    assert provider is not None
    assert provider.resource.attributes["schema.version"] == "v1"
    provider.shutdown()


def _validated_exporter() -> tuple[TracerProvider, InMemorySpanExporter]:
    # The validator only strips attributes on export; wire it ahead of an
    # in-memory exporter so tests can assert on what actually got through.
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(_SchemaValidatingSpanProcessor())
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    return provider, exporter


def test_validator_strips_attribute_key_outside_the_closed_schema() -> None:
    # A call site bypassing _set() (e.g. a future span.set_attribute call) must
    # not reach the exporter with an unlisted key (ADR-0076 decision 3).
    provider, exporter = _validated_exporter()
    tracer = provider.get_tracer("test")
    with tracer.start_as_current_span("agent.run") as span:
        span.set_attribute("langfuse.trace.name", "ok")
        span.set_attribute("some.unlisted.key", "should not survive export")

    (finished,) = exporter.get_finished_spans()
    assert finished.attributes["langfuse.trace.name"] == "ok"
    assert "some.unlisted.key" not in finished.attributes


def test_validator_strips_value_still_matching_an_unscrubbed_secret() -> None:
    # A value that reaches set_attribute without going through redact_span_attribute
    # (bypassing _set()) is caught at export time even though its key is allowed.
    provider, exporter = _validated_exporter()
    tracer = provider.get_tracer("test")
    with tracer.start_as_current_span("agent.run") as span:
        span.set_attribute("langfuse.trace.name", "sk-abcdefghijklmnopqrstuvwx")

    (finished,) = exporter.get_finished_spans()
    assert "langfuse.trace.name" not in finished.attributes


def test_validator_strips_sequence_value_hiding_an_unscrubbed_secret() -> None:
    # #935: the validator is framed as a type-agnostic backstop (ADR-0076 decision
    # 3), but it only inspected `str` values -- so a SEQUENCE-valued attribute on
    # an allowed key carried a secret straight to the exporter, past both the scrub
    # and this validator. OTel permits sequence values, so the backstop must
    # recurse rather than trust that no call site ever sets one.
    provider, exporter = _validated_exporter()
    tracer = provider.get_tracer("test")
    with tracer.start_as_current_span("agent.run") as span:
        span.set_attribute("langfuse.trace.name", ["sk-abcdefghijklmnopqrstuvwx", "clean"])

    (finished,) = exporter.get_finished_spans()
    assert "langfuse.trace.name" not in finished.attributes


def test_validator_keeps_a_clean_sequence_value() -> None:
    # The recursion must not become a blanket "drop all sequences": a clean
    # sequence on an allowed key is legitimate telemetry and survives.
    provider, exporter = _validated_exporter()
    tracer = provider.get_tracer("test")
    with tracer.start_as_current_span("agent.run") as span:
        span.set_attribute("langfuse.trace.name", ["curie-run:test", "clean"])

    (finished,) = exporter.get_finished_spans()
    assert finished.attributes["langfuse.trace.name"] == ("curie-run:test", "clean")


def test_validator_leaves_clean_allowed_attributes_untouched() -> None:
    provider, exporter = _validated_exporter()
    tracer = provider.get_tracer("test")
    with tracer.start_as_current_span("agent.run") as span:
        span.set_attribute("langfuse.trace.name", "curie-run:test")
        span.set_attribute("gen_ai.usage.input_tokens", 12)

    (finished,) = exporter.get_finished_spans()
    assert finished.attributes["langfuse.trace.name"] == "curie-run:test"
    assert finished.attributes["gen_ai.usage.input_tokens"] == 12
