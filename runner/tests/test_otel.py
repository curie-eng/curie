"""OTel: the gen_ai span tree is emitted for a turn; exporter wiring is gated."""

import os
import time
from collections import defaultdict
from collections.abc import AsyncIterator, Callable
from typing import Any

import anyio
import httpx
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
from claude_agent_sdk import (
    AssistantMessage,
    ResultMessage,
    StreamEvent,
    TextBlock,
    ToolResultBlock,
    ToolUseBlock,
    UserMessage,
)
from claude_agent_sdk.types import PermissionResultDeny
from curie_runner import RunTracer, SideEffectClassifier, build_tracer_provider
from curie_runner import otel as otel_module
from curie_runner import session as session_module
from curie_runner.adapter import ClaudeAgentSession, PartialMessageBoundary
from curie_runner.approval import ApprovalGate
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
from opentelemetry.sdk.resources import Resource
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

_STREAM_BODY = "private-partial-body-PLACEHOLDER"
_STREAM_ARGUMENT = "private-partial-argument-PLACEHOLDER"
_TOOL_ARGUMENT = "private-tool-argument-PLACEHOLDER"
_TOOL_RESULT = "private-tool-result-PLACEHOLDER"
_STREAM_UUID = "stream-uuid-PLACEHOLDER"
_STREAM_SESSION_ID = "stream-session-PLACEHOLDER"
_PARENT_TOOL_ID = "parent-tool-id-PLACEHOLDER"
_TOOL_CALL_ID = "tool-call-id-PLACEHOLDER"


def _result(
    *,
    text: str = "done",
    is_error: bool = False,
    terminal_reason: str | None = None,
    usage: dict[str, Any] | None = None,
) -> ResultMessage:
    return ResultMessage(
        subtype="error_during_execution" if is_error else "success",
        duration_ms=1,
        duration_api_ms=1,
        is_error=is_error,
        num_turns=1,
        session_id="sdk-result-session-PLACEHOLDER",
        result=text,
        terminal_reason=terminal_reason,
        usage=usage,
    )


def _tool_result(
    call_id: str,
    *,
    content: str = _TOOL_RESULT,
    is_error: bool = False,
) -> UserMessage:
    return UserMessage(
        content=[
            ToolResultBlock(
                tool_use_id=call_id,
                content=content,
                is_error=is_error,
            )
        ]
    )


def _spans_by_name(spans: list[ReadableSpan]) -> dict[str, list[ReadableSpan]]:
    grouped: dict[str, list[ReadableSpan]] = defaultdict(list)
    for span in spans:
        grouped[span.name].append(span)
    for same_name in grouped.values():
        same_name.sort(key=lambda span: span.start_time or 0)
    return dict(grouped)


def _export_turn(
    session_factory: Callable[[], Any],
    *,
    model: str | None = "configured-model",
    collector_endpoint: str | None = None,
) -> tuple[list[object], list[ReadableSpan]]:
    exporter = InMemorySpanExporter()
    resource = (
        Resource.create({"service.name": "curie-runner-integration"})
        if collector_endpoint
        else None
    )
    provider = TracerProvider(resource=resource)
    if collector_endpoint:
        provider.add_span_processor(_SchemaValidatingSpanProcessor())
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    if collector_endpoint:
        provider.add_span_processor(
            SimpleSpanProcessor(HttpOTLPSpanExporter(endpoint=collector_endpoint))
        )
    runner = SessionRunner(
        session_factory=session_factory,
        ceiling=0,
        tracer=RunTracer(provider),
        classifier=SideEffectClassifier(),
        trace_name="curie-run:test",
        model=model,
    )

    async def go() -> list[object]:
        await runner.start()
        try:
            lines = [
                line
                async for line in runner.run_turn(
                    Event(type="message", text="go", user="U0EXAMPLE1", ts="1")
                )
            ]
            return parse_ndjson("".join(lines))
        finally:
            await runner.close()

    events = anyio.run(go)
    return events, list(exporter.get_finished_spans())


class _ScriptedSDKClient:
    """SDK-shaped client that drives the real adapter without a provider call."""

    def __init__(self, script: list[object]) -> None:
        self._script = script
        self.queries: list[str] = []

    async def connect(self) -> None:
        return None

    async def query(self, text: str) -> None:
        self.queries.append(text)

    def receive_response(self) -> AsyncIterator[object]:
        async def messages() -> AsyncIterator[object]:
            for message in self._script:
                yield message

        return messages()

    async def interrupt(self) -> None:
        return None

    async def disconnect(self) -> None:
        return None


def _adapter_session_factory(script: list[object]) -> Callable[[], ClaudeAgentSession]:
    def factory() -> ClaudeAgentSession:
        session = ClaudeAgentSession.__new__(ClaudeAgentSession)
        session._client = _ScriptedSDKClient(script)  # type: ignore[attr-defined]
        return session

    return factory


def test_adapter_aclose_closes_the_inner_sdk_stream_on_the_driving_task() -> None:
    finalizer_task_ids: list[int] = []

    class FinalizingSDKClient:
        def receive_response(self) -> AsyncIterator[object]:
            async def messages() -> AsyncIterator[object]:
                try:
                    yield AssistantMessage(
                        content=[TextBlock(text="first response")],
                        model="observed-model",
                    )
                    await anyio.sleep_forever()
                finally:
                    finalizer_task_ids.append(anyio.get_current_task().id)

            return messages()

    async def go() -> None:
        session = ClaudeAgentSession.__new__(ClaudeAgentSession)
        session._client = FinalizingSDKClient()  # type: ignore[attr-defined]
        response: Any = session.receive_turn()
        driving_task_id = anyio.get_current_task().id

        assert isinstance(await anext(response), AssistantMessage)
        await response.aclose()

        assert finalizer_task_ids == [driving_task_id]

    anyio.run(go)


def _partial_boundary(event_type: str, *, second: bool = False) -> StreamEvent:
    event: dict[str, Any] = {
        "type": event_type,
        "message": {"content": _STREAM_BODY},
    }
    if second:
        event["content_block"] = {
            "type": "tool_use",
            "input": _STREAM_ARGUMENT,
        }
    return StreamEvent(
        uuid=_STREAM_UUID,
        session_id=_STREAM_SESSION_ID,
        parent_tool_use_id=_PARENT_TOOL_ID,
        event=event,
    )


def _raw_stream_event(event: dict[str, Any]) -> StreamEvent:
    return StreamEvent(
        uuid=_STREAM_UUID,
        session_id=_STREAM_SESSION_ID,
        parent_tool_use_id=_PARENT_TOOL_ID,
        event=event,
    )


# Anthropic documents tool starts as ``content_block_start`` events carrying a
# ``tool_use`` block, exposed by the SDK through ``StreamEvent.event``.
# https://platform.claude.com/docs/en/build-with-claude/streaming
def _streamed_tool_start(call_id: str, name: str) -> StreamEvent:
    return _raw_stream_event(
        {
            "type": "content_block_start",
            "message": {"content": _STREAM_BODY},
            "content_block": {
                "type": "tool_use",
                "id": call_id,
                "name": name,
                "input": {"argument": _STREAM_ARGUMENT},
            },
        }
    )


def _two_round_script(*, include_boundaries: bool = True) -> list[object]:
    first_usage = {
        "input_tokens": 3,
        "output_tokens": 2,
        "cache_read_input_tokens": 4,
    }
    second_first_round_usage = {
        "input_tokens": 5,
        "output_tokens": 7,
        "cache_creation_input_tokens": 6,
    }
    second_round_usage = {
        "input_tokens": 11,
        "output_tokens": 13,
        "cache_read_input_tokens": 17,
        "cache_creation_input_tokens": 19,
    }
    result_total = {
        "input_tokens": 101,
        "output_tokens": 103,
        "cache_read_input_tokens": 107,
        "cache_creation_input_tokens": 109,
    }
    script: list[object] = []
    if include_boundaries:
        script.extend(
            [
                _partial_boundary("message_start"),
                # A later allowed partial in the same provider wait must not
                # replace/backdate the first-response observation.
                _partial_boundary("content_block_start", second=True),
            ]
        )
    script.extend(
        [
            AssistantMessage(
                content=[TextBlock(text="first response")],
                model="observed-model",
                usage=first_usage,
            ),
            AssistantMessage(
                content=[
                    TextBlock(text="using tool"),
                    ToolUseBlock(
                        id=_TOOL_CALL_ID,
                        name="Read",
                        input={"path": _TOOL_ARGUMENT},
                    ),
                ],
                model="observed-model",
                usage=second_first_round_usage,
            ),
            _tool_result(_TOOL_CALL_ID),
        ]
    )
    if include_boundaries:
        script.append(_partial_boundary("message_start"))
    script.extend(
        [
            AssistantMessage(
                content=[TextBlock(text="second response")],
                model="observed-model",
                usage=second_round_usage,
            ),
            _result(text="done", usage=result_total),
        ]
    )
    return script


_FIRST_STREAMED_CALL_ID = "streamed-call-one-PLACEHOLDER"
_SECOND_STREAMED_CALL_ID = "streamed-call-two-PLACEHOLDER"
_PRIVATE_OTEL_VALUES = (
    _STREAM_BODY, _STREAM_ARGUMENT, _TOOL_ARGUMENT, _TOOL_RESULT,
    _STREAM_UUID, _STREAM_SESSION_ID, _PARENT_TOOL_ID, _TOOL_CALL_ID,
    _FIRST_STREAMED_CALL_ID, _SECOND_STREAMED_CALL_ID,
    "sdk-result-session-PLACEHOLDER",
)


def _two_streamed_tool_script(*, repeat_final_blocks: bool = False) -> list[object]:
    repeated: list[object] = []
    if repeat_final_blocks:
        repeated = [
            AssistantMessage(
                content=[
                    ToolUseBlock(id=_FIRST_STREAMED_CALL_ID, name="Bash", input={}),
                    ToolUseBlock(id=_SECOND_STREAMED_CALL_ID, name="Write", input={}),
                ],
                model="observed-model",
                usage={
                    "input_tokens": 23,
                    "output_tokens": 29,
                    "cache_read_input_tokens": 31,
                    "cache_creation_input_tokens": 37,
                },
            )
        ]
    return [
        _streamed_tool_start(_FIRST_STREAMED_CALL_ID, "Bash"),
        _streamed_tool_start(_SECOND_STREAMED_CALL_ID, "Write"),
        *repeated,
        _tool_result(_FIRST_STREAMED_CALL_ID),
        _tool_result(_SECOND_STREAMED_CALL_ID),
        _result(),
    ]


def _span_wire_material(spans: list[ReadableSpan]) -> str:
    return repr(
        [
            (
                span.name,
                dict(span.attributes or {}),
                [(event.name, dict(event.attributes or {})) for event in span.events],
                span.status.description,
            )
            for span in spans
        ]
    )


def test_two_true_provider_wait_rounds_accumulate_assistant_usage_only() -> None:
    _, finished = _export_turn(_adapter_session_factory(_two_round_script()))
    spans = _spans_by_name(finished)

    generations = spans["llm.generation"]
    assert len(generations) == 2
    assert [span.attributes["curie.generation.round"] for span in generations] == [1, 2]
    assert [span.attributes["curie.phase"] for span in generations] == [
        "provider_wait",
        "provider_wait",
    ]
    assert [span.attributes["curie.phase.start_kind"] for span in generations] == [
        "query_observed",
        "tool_result_inferred",
    ]
    assert [span.attributes["curie.phase.end_kind"] for span in generations] == [
        "tool_use_observed",
        "result_observed",
    ]

    first, second = generations
    assert first.attributes["gen_ai.usage.input_tokens"] == 8
    assert first.attributes["gen_ai.usage.output_tokens"] == 9
    assert first.attributes["gen_ai.usage.cache_read_input_tokens"] == 4
    assert first.attributes["gen_ai.usage.cache_creation_input_tokens"] == 6
    assert second.attributes["gen_ai.usage.input_tokens"] == 11
    assert second.attributes["gen_ai.usage.output_tokens"] == 13
    assert second.attributes["gen_ai.usage.cache_read_input_tokens"] == 17
    assert second.attributes["gen_ai.usage.cache_creation_input_tokens"] == 19

    # ResultMessage usage is the turn total. Stamping it on the last generation
    # would double-count earlier rounds and overwrite the per-message evidence.
    emitted_values = {
        value
        for generation in generations
        for key, value in generation.attributes.items()
        if key.startswith("gen_ai.usage.")
    }
    assert emitted_values.isdisjoint({101, 103, 107, 109})

    for generation in generations:
        ttft = generation.attributes["curie.generation.ttft_ms"]
        assert type(ttft) is int
        assert ttft >= 0

    root = spans["agent.run"][0]
    tool = spans["execute_tool"][0]
    assert root.attributes["curie.phase"] == "provider_wait"
    assert root.attributes["curie.terminal.cause"] == "completed"
    assert root.attributes["curie.terminal.status"] == "succeeded"
    assert tool.attributes["curie.tool.call.index"] == 1


def test_generation_omits_ttft_without_an_allowlisted_partial_boundary() -> None:
    _, finished = _export_turn(
        lambda: FakeModelSession(lambda: _two_round_script(include_boundaries=False))
    )
    generations = _spans_by_name(finished)["llm.generation"]

    assert len(generations) == 2
    assert all("curie.generation.ttft_ms" not in span.attributes for span in generations)


def test_unallowlisted_stream_event_produces_no_boundary_ttft_or_private_material() -> None:
    raw_event = _partial_boundary("content_block_delta", second=True)

    async def collect_adapter_output() -> list[object]:
        session = _adapter_session_factory([raw_event])()
        return [message async for message in session.receive_turn()]

    assert anyio.run(collect_adapter_output) == []

    script = [
        raw_event,
        AssistantMessage(
            content=[TextBlock(text="visible response")],
            model="observed-model",
        ),
        _result(),
    ]
    _, finished = _export_turn(_adapter_session_factory(script))
    generation = _spans_by_name(finished)["llm.generation"][0]

    assert "curie.generation.ttft_ms" not in generation.attributes
    material = _span_wire_material(finished)
    for private_value in (
        _STREAM_BODY,
        _STREAM_ARGUMENT,
        _STREAM_UUID,
        _STREAM_SESSION_ID,
        _PARENT_TOOL_ID,
    ):
        assert private_value not in material


def test_ttft_is_first_boundary_once_and_is_measured_from_each_round_start(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    timestamps = (
        1_000_000_000,
        1_007_000_000,
        10_000_000_000,
        10_011_000_000,
    )
    observed_timestamps: list[int] = []

    def monotonic_ns() -> int:
        value = timestamps[len(observed_timestamps)]
        observed_timestamps.append(value)
        return value

    monkeypatch.setattr(otel_module.time, "monotonic_ns", monotonic_ns)
    _, finished = _export_turn(_adapter_session_factory(_two_round_script()))
    generations = _spans_by_name(finished)["llm.generation"]

    assert observed_timestamps == list(timestamps)
    assert [
        generation.attributes["curie.generation.ttft_ms"]
        for generation in generations
    ] == [7, 11]


def test_steer_during_provider_wait_preserves_generation_and_ttft(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    timestamps = (
        1_000_000_000,
        1_013_000_000,
        # A steer that restarts the phase would consume these deliberately
        # different values and make both the call-count and span assertions fail.
        50_000_000_000,
        50_029_000_000,
    )
    observed_timestamps: list[int] = []

    def monotonic_ns() -> int:
        value = timestamps[len(observed_timestamps)]
        observed_timestamps.append(value)
        return value

    monkeypatch.setattr(otel_module.time, "monotonic_ns", monotonic_ns)
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))

    class BlockingSteerSession:
        def __init__(self) -> None:
            self.queries: list[str] = []
            self.waiting_after_boundary = anyio.Event()
            self.release = anyio.Event()

        async def connect(self) -> None:
            return None

        async def query(self, text: str) -> None:
            self.queries.append(text)

        def receive_turn(self) -> AsyncIterator[object]:
            async def messages() -> AsyncIterator[object]:
                # This is the same payload-free, allowlisted adapter boundary the
                # real SessionRunner sees; provider content never enters the test.
                yield PartialMessageBoundary(event_type="message_start")
                self.waiting_after_boundary.set()
                await self.release.wait()
                yield AssistantMessage(
                    content=[TextBlock(text="completed after steer")],
                    model="observed-model",
                )
                yield _result(text="completed after steer")

            return messages()

        async def interrupt(self) -> None:
            self.release.set()

        async def close(self) -> None:
            return None

    async def go() -> tuple[list[object], BlockingSteerSession]:
        session = BlockingSteerSession()
        runner = SessionRunner(
            session_factory=lambda: session,
            ceiling=0,
            tracer=RunTracer(provider),
            classifier=SideEffectClassifier(),
            trace_name="curie-run:steer-telemetry",
            model="configured-model",
        )
        lines: list[str] = []

        async def consume() -> None:
            async for line in runner.run_turn(
                Event(type="message", text="initial", user="U0EXAMPLE1", ts="1")
            ):
                lines.append(line)

        await runner.start()
        try:
            async with anyio.create_task_group() as tasks:
                tasks.start_soon(consume)
                await session.waiting_after_boundary.wait()
                assert runner.turn_active
                assert session.queries == ["initial"]
                assert await runner.steer("steered follow-up") is True
                session.release.set()
        finally:
            await runner.close()
        return parse_ndjson("".join(lines)), session

    events, session = anyio.run(go)
    assert session.queries == ["initial", "steered follow-up"]
    assert isinstance(events[-1], Final)
    assert events[-1].status is SessionStatus.DONE

    spans = _spans_by_name(list(exporter.get_finished_spans()))
    generations = spans["llm.generation"]
    assert len(generations) == 1
    assert generations[0].attributes["curie.generation.round"] == 1
    assert generations[0].attributes["curie.generation.ttft_ms"] == 13
    assert observed_timestamps == list(timestamps[:2])

    root = spans["agent.run"][0]
    assert root.attributes["curie.terminal.cause"] == "completed"
    assert root.attributes["curie.terminal.status"] == "succeeded"
    assert root.status.status_code is StatusCode.OK


def test_fake_partial_boundaries_are_opt_in_payload_free_and_precede_each_assistant() -> None:
    script: list[object] = [
        AssistantMessage(
            content=[TextBlock(text=_STREAM_BODY)],
            model="fake-model",
        ),
        _tool_result("fake-result-call-PLACEHOLDER"),
        AssistantMessage(
            content=[
                ToolUseBlock(
                    id="fake-call-PLACEHOLDER",
                    name="Read",
                    input={"path": _STREAM_ARGUMENT},
                )
            ],
            model="fake-model",
        ),
        _result(),
    ]

    async def replay(session: FakeModelSession) -> list[object]:
        await session.connect()
        await session.query("go")
        try:
            return [message async for message in session.receive_turn()]
        finally:
            await session.close()

    default_messages = anyio.run(replay, FakeModelSession(lambda: script))
    assert default_messages == script
    assert not any(
        isinstance(message, PartialMessageBoundary) for message in default_messages
    )

    boundary_messages = anyio.run(
        replay,
        FakeModelSession(lambda: script, emit_partial_boundaries=True),
    )
    assistant_positions = [
        index
        for index, message in enumerate(boundary_messages)
        if isinstance(message, AssistantMessage)
    ]
    boundaries = [
        boundary_messages[index - 1]
        for index in assistant_positions
        if index > 0
    ]

    assert len(assistant_positions) == 2
    assert boundaries == [
        PartialMessageBoundary(event_type="message_start"),
        PartialMessageBoundary(event_type="message_start"),
    ]
    assert _STREAM_BODY not in repr(boundaries)
    assert _STREAM_ARGUMENT not in repr(boundaries)


def test_raw_stream_tool_payloads_and_provider_ids_never_reach_otel() -> None:
    _, finished = _export_turn(_adapter_session_factory(_two_round_script()))
    streamed_script = _two_streamed_tool_script(repeat_final_blocks=True)

    async def adapter_output() -> list[object]:
        session = _adapter_session_factory([streamed_script[0]])()
        return [message async for message in session.receive_turn()]

    material = _span_wire_material(finished) + repr(anyio.run(adapter_output))

    for private_value in _PRIVATE_OTEL_VALUES:
        assert private_value not in material


@pytest.mark.parametrize("repeat_final_blocks", (False, True), ids=("stream-only", "dedupe"))
def test_two_streamed_tool_starts_without_final_tool_blocks_emit_two_intervals(
    repeat_final_blocks: bool,
) -> None:
    script = _two_streamed_tool_script(repeat_final_blocks=repeat_final_blocks)
    events, finished = _export_turn(_adapter_session_factory(script))
    spans = _spans_by_name(finished)
    root = spans["agent.run"][0]
    generations = spans["llm.generation"]
    tools = spans["execute_tool"]

    assert len(generations) == 2
    assert len(tools) == 2
    assert next(
        span for span in generations if span.attributes["curie.generation.round"] == 1
    ).end_time == tools[0].start_time
    assert [span.attributes["gen_ai.tool.name"] for span in tools] == ["Bash", "Write"]
    assert [span.attributes["curie.tool.call.index"] for span in tools] == [1, 2]
    assert root.context is not None
    assert all(
        span.start_time is not None
        and span.end_time is not None
        and span.end_time > span.start_time
        and span.parent is not None
        and span.parent.span_id == root.context.span_id
        and span.context is not None
        and span.context.trace_id == root.context.trace_id
        for span in tools
    )
    assert sum(getattr(event, "type", None) == "tool_note" for event in events) == (
        2 if repeat_final_blocks else 0
    )
    if repeat_final_blocks:
        first = next(
            span for span in generations if span.attributes["curie.generation.round"] == 1
        )
        assert first.attributes["gen_ai.usage.input_tokens"] == 23
        assert first.attributes["gen_ai.usage.output_tokens"] == 29
        assert first.attributes["gen_ai.usage.cache_read_input_tokens"] == 31
        assert first.attributes["gen_ai.usage.cache_creation_input_tokens"] == 37
    material = _span_wire_material(finished)
    for private_value in _PRIVATE_OTEL_VALUES:
        assert private_value not in material


@pytest.mark.parametrize(
    ("tool_failed", "outcome", "status", "end_kind"),
    (
        pytest.param(False, "success", StatusCode.OK, "tool_result_inferred", id="success"),
        pytest.param(True, "error", StatusCode.ERROR, "tool_result_inferred", id="failure"),
        pytest.param(None, "cancelled", StatusCode.OK, "terminal_inferred", id="missing"),
    ),
)
def test_streamed_tool_failure_status_matches_success_and_error_result(
    tool_failed: bool | None,
    outcome: str,
    status: StatusCode,
    end_kind: str,
) -> None:
    tail: list[object] = (
        [_result()]
        if tool_failed is None
        else [_tool_result(_TOOL_CALL_ID, is_error=tool_failed), _result()]
    )
    _, finished = _export_turn(
        _adapter_session_factory(
            [_streamed_tool_start(_TOOL_CALL_ID, "Read"), *tail]
        )
    )
    tool = _spans_by_name(finished)["execute_tool"][0]

    assert tool.attributes["curie.phase.end_kind"] == end_kind
    assert tool.attributes["curie.tool.outcome"] == outcome
    assert tool.status.status_code is status
    assert tool.start_time is not None
    assert tool.end_time is not None
    assert tool.end_time > tool.start_time


def test_streamed_tool_malformed_and_no_tool_scripts_emit_no_tool_spans() -> None:
    malformed_blocks: list[dict[str, object]] = [
        {"type": "tool_use", "name": "Read"},
        {"type": "tool_use", "id": 7, "name": "Read"},
        {"type": "tool_use", "id": "", "name": "Read"},
        {"type": "tool_use", "id": _TOOL_CALL_ID},
        {"type": "tool_use", "id": _TOOL_CALL_ID, "name": 7},
        {"type": "tool_use", "id": _TOOL_CALL_ID, "name": ""},
        {"type": "text"},
    ]
    scripts = [
        [_raw_stream_event({"type": "content_block_start", "content_block": block})]
        for block in malformed_blocks
    ]
    scripts.extend(([_partial_boundary("message_start")], []))

    for stream_messages in scripts:
        script = [*stream_messages, _result()]
        events, finished = _export_turn(_adapter_session_factory(script))
        assert isinstance(events[-1], Final)
        assert events[-1].status is SessionStatus.DONE
        assert "execute_tool" not in _spans_by_name(finished)


def _finished_trace_id(spans: list[ReadableSpan]) -> str:
    roots = [span for span in spans if span.name == "agent.run"]
    assert len(roots) == 1
    root = roots[0]
    assert root.context is not None
    return format(root.context.trace_id, "032x")


def _wait_for_exact_trace_counts(
    client: httpx.Client,
    *,
    langfuse_host: str,
    auth: tuple[str, str],
    trace_id: str,
    expected_generations: int,
    expected_tool_names: tuple[str, ...],
) -> tuple[int, int, tuple[str, ...]] | None:
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        try:
            trace_body = client.get(
                f"{langfuse_host}/api/public/traces/{trace_id}", auth=auth
            ).json()
            observations = client.get(
                f"{langfuse_host}/api/public/observations",
                params={"traceId": trace_id, "limit": 100},
                auth=auth,
            ).json()["data"]
        except (KeyError, TypeError, httpx.HTTPError, ValueError):
            trace_body = observations = None
        if (
            isinstance(trace_body, dict)
            and trace_body.get("id") == trace_id
            and isinstance(observations, list)
            and all(isinstance(item, dict) for item in observations)
            and all(item.get("traceId") == trace_id for item in observations)
        ):
            roots = [
                item
                for item in observations
                if item.get("name") == "agent.run" and item.get("endTime")
            ]
            names = [item.get("name") for item in observations]
            counts = (
                len(roots),
                names.count("llm.generation"),
                tuple(
                    sorted(
                        item["name"]
                        for item in observations
                        if item.get("type") == "TOOL"
                    )
                ),
            )
            if counts == (1, expected_generations, expected_tool_names):
                return counts
        time.sleep(1)
    return None


@pytest.mark.skipif(
    os.environ.get("CURIE_RUNNER_OTEL_INTEGRATION") != "1",
    reason="set CURIE_RUNNER_OTEL_INTEGRATION=1 for the local observability stack",
)
def test_streamed_tool_starts_reach_langfuse_through_collector() -> None:
    """Scripted SDK events cross real OTLP/HTTP without a model credential."""

    collector_endpoint = "http://localhost:24318/v1/traces"
    langfuse_host = "http://localhost:23000"
    auth = ("pk-lf-curie-dev", "sk-lf-curie-dev")

    with httpx.Client(timeout=2.0) as preflight_client:
        try:
            health = preflight_client.get(f"{langfuse_host}/api/public/health")
            preflight_client.get(collector_endpoint)
        except (httpx.HTTPError, ValueError):
            pytest.skip("local Collector and Langfuse are not reachable")
        if health.status_code != 200:
            pytest.skip("local Collector and Langfuse are not reachable")

    _, tool_spans = _export_turn(
        _adapter_session_factory(_two_streamed_tool_script()),
        collector_endpoint=collector_endpoint,
    )
    _, no_tool_spans = _export_turn(
        _adapter_session_factory([_result()]),
        collector_endpoint=collector_endpoint,
    )

    assert len(_spans_by_name(tool_spans)["execute_tool"]) == 2
    assert "execute_tool" not in _spans_by_name(no_tool_spans)

    with httpx.Client(timeout=5.0) as langfuse_client:
        tool_counts = _wait_for_exact_trace_counts(
            langfuse_client,
            langfuse_host=langfuse_host,
            auth=auth,
            trace_id=_finished_trace_id(tool_spans),
            expected_generations=2,
            expected_tool_names=("Bash", "Write"),
        )
        no_tool_counts = _wait_for_exact_trace_counts(
            langfuse_client,
            langfuse_host=langfuse_host,
            auth=auth,
            trace_id=_finished_trace_id(no_tool_spans),
            expected_generations=1,
            expected_tool_names=(),
        )
    assert tool_counts == (1, 2, ("Bash", "Write"))
    assert no_tool_counts == (1, 1, ())


@pytest.mark.parametrize(
    ("tool_failed", "expected_outcome", "expected_status"),
    (
        (False, "success", StatusCode.OK),
        (True, "error", StatusCode.ERROR),
    ),
)
def test_tool_use_result_is_a_duration_bearing_agent_run_sibling(
    tool_failed: bool,
    expected_outcome: str,
    expected_status: StatusCode,
) -> None:
    script = [
        AssistantMessage(
            content=[
                ToolUseBlock(
                    id=_TOOL_CALL_ID,
                    name="Read",
                    input={"path": _TOOL_ARGUMENT},
                )
            ],
            model="observed-model",
        ),
        _tool_result(_TOOL_CALL_ID, is_error=tool_failed),
        AssistantMessage(
            content=[TextBlock(text="done")],
            model="observed-model",
            usage={"output_tokens": 1},
        ),
        _result(usage={"output_tokens": 999}),
    ]
    _, finished = _export_turn(lambda: FakeModelSession(lambda: script))
    spans = _spans_by_name(finished)
    root = spans["agent.run"][0]
    tool = spans["execute_tool"][0]

    assert root.context is not None
    assert tool.parent is not None
    assert tool.parent.span_id == root.context.span_id
    assert all(
        generation.context is None or tool.parent.span_id != generation.context.span_id
        for generation in spans["llm.generation"]
    )
    assert tool.start_time is not None
    assert tool.end_time is not None
    assert tool.end_time > tool.start_time
    assert tool.attributes["curie.phase"] == "tool_wait"
    assert tool.attributes["curie.phase.start_kind"] == "tool_use_inferred"
    assert tool.attributes["curie.phase.end_kind"] == "tool_result_inferred"
    assert tool.attributes["curie.tool.outcome"] == expected_outcome
    assert tool.attributes["curie.tool.call.index"] == 1
    assert tool.status.status_code is expected_status
    assert _TOOL_CALL_ID not in _span_wire_material(finished)


def test_parallel_tools_are_root_siblings_and_reopen_provider_after_both_results() -> None:
    first_call_id = "parallel-call-one-PLACEHOLDER"
    second_call_id = "parallel-call-two-PLACEHOLDER"
    script = [
        AssistantMessage(
            content=[
                ToolUseBlock(id=first_call_id, name="Read", input={}),
                ToolUseBlock(id=second_call_id, name="Grep", input={}),
            ],
            model="observed-model",
        ),
        _tool_result(first_call_id),
        _tool_result(second_call_id),
        AssistantMessage(
            content=[TextBlock(text="done")],
            model="observed-model",
        ),
        _result(),
    ]
    _, finished = _export_turn(lambda: FakeModelSession(lambda: script))
    spans = _spans_by_name(finished)
    root = spans["agent.run"][0]
    generations = spans["llm.generation"]
    tools = spans["execute_tool"]

    assert len(generations) == 2
    assert len(tools) == 2
    assert [tool.attributes["curie.tool.call.index"] for tool in tools] == [1, 2]
    assert root.context is not None
    assert all(
        tool.parent is not None and tool.parent.span_id == root.context.span_id
        for tool in tools
    )
    second_tool = next(
        tool for tool in tools if tool.attributes["gen_ai.tool.name"] == "Grep"
    )
    assert generations[1].start_time is not None
    assert second_tool.end_time is not None
    assert generations[1].start_time >= second_tool.end_time


def test_unmatched_tool_result_creates_no_tool_span() -> None:
    script = [
        _tool_result("unmatched-call-id-PLACEHOLDER"),
        AssistantMessage(content=[TextBlock(text="done")], model="observed-model"),
        _result(),
    ]
    _, finished = _export_turn(lambda: FakeModelSession(lambda: script))
    spans = _spans_by_name(finished)

    assert "execute_tool" not in spans
    assert len(spans["llm.generation"]) == 1
    assert "unmatched-call-id-PLACEHOLDER" not in _span_wire_material(finished)


def test_dangling_tool_is_closed_as_cancelled_without_exporting_call_id() -> None:
    script = [
        AssistantMessage(
            content=[ToolUseBlock(id=_TOOL_CALL_ID, name="Read", input={})],
            model="observed-model",
        ),
        _result(),
    ]
    _, finished = _export_turn(lambda: FakeModelSession(lambda: script))
    spans = _spans_by_name(finished)
    tool = spans["execute_tool"][0]

    assert tool.attributes["curie.tool.outcome"] == "cancelled"
    assert tool.attributes["curie.phase.end_kind"] == "terminal_inferred"
    assert tool.end_time is not None
    assert tool.start_time is not None
    assert tool.end_time > tool.start_time
    assert _TOOL_CALL_ID not in _span_wire_material(finished)


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

    spans = _spans_by_name(list(exporter.get_finished_spans()))
    assert {"agent.run", "llm.generation", "execute_tool"} <= set(spans)
    assert spans["agent.run"][0].attributes["langfuse.trace.name"] == "curie-run:test"
    generations = spans["llm.generation"]
    assert len(generations) == 2
    assert all(
        generation.attributes["gen_ai.request.model"] == "fake-model"
        for generation in generations
    )
    assert generations[1].attributes["gen_ai.usage.output_tokens"] == 8
    assert spans["execute_tool"][0].attributes["gen_ai.tool.name"] == "Bash"


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

    generations = _spans_by_name(list(exporter.get_finished_spans()))["llm.generation"]
    assert len(generations) == 2
    assert all(
        generation.attributes["gen_ai.request.model"] == "fake-model"
        for generation in generations
    )
    # The usage counts only land on a model-bearing generation, so their presence
    # is the end-to-end proof the span was typed as a generation, not a bare span.
    assert generations[1].attributes["gen_ai.usage.output_tokens"] == 8


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
def test_tracer_provider_none_without_endpoint(
    otel: OtelConfig, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("OTEL_EXPORTER_OTLP_ENDPOINT", raising=False)
    monkeypatch.delenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", raising=False)
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
    parent, parent_span_context = _remote_parent()
    ambient_tracer = TracerProvider().get_tracer("ambient")
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
        try:
            async for _ in runner.run_turn(
                Event(type="message", text="go", user="U0EXAMPLE1", ts="1"),
                parent=parent,
            ):
                pass
        finally:
            await runner.close()

    with ambient_tracer.start_as_current_span("ambient"):
        anyio.run(go)

    spans = _spans_by_name(list(exporter.get_finished_spans()))
    root = spans["agent.run"][0]
    assert root.context is not None
    assert root.context.trace_id == parent_span_context.trace_id
    assert root.parent is not None
    assert root.parent.span_id == parent_span_context.span_id
    for child in spans["llm.generation"] + spans["execute_tool"]:
        assert child.context is not None
        assert child.context.trace_id == parent_span_context.trace_id
        assert child.parent is not None
        assert child.parent.span_id == root.context.span_id


def test_empty_run_span_is_lazy_and_exports_the_root_only() -> None:
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))

    with RunTracer(provider).run_span("curie-run:test", "fake-model"):
        pass

    spans = list(exporter.get_finished_spans())
    assert [span.name for span in spans] == ["agent.run"]


def test_direct_compat_usage_and_tools_do_not_fabricate_ttft_or_generations() -> None:
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))

    with RunTracer(provider).run_span("curie-run:test", "fake-model") as span:
        span.record_usage({"input_tokens": 3, "output_tokens": 2})
        span.tool_span("Read")
        span.tool_span("Grep")

    spans = _spans_by_name(list(exporter.get_finished_spans()))
    generations = spans["llm.generation"]
    tools = spans["execute_tool"]

    assert len(generations) == 1
    assert "curie.generation.ttft_ms" not in generations[0].attributes
    assert generations[0].attributes["gen_ai.usage.input_tokens"] == 3
    assert generations[0].attributes["gen_ai.usage.output_tokens"] == 2
    assert len(tools) == 2
    assert [tool.attributes["curie.tool.call.index"] for tool in tools] == [1, 2]


def test_run_span_with_missing_parent_starts_a_safe_root() -> None:
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    tracer = RunTracer(provider)
    ambient_tracer = TracerProvider().get_tracer("ambient")

    with ambient_tracer.start_as_current_span("ambient") as ambient:
        with tracer.run_span("curie-run:test", "fake-model", parent=None):
            pass

    spans = list(exporter.get_finished_spans())
    assert [span.name for span in spans] == ["agent.run"]
    root = spans[0]
    assert root.context is not None
    assert root.parent is None
    assert root.context.trace_id != ambient.get_span_context().trace_id


def _run_and_export(
    session_factory: Any = FakeModelSession,
) -> tuple[list[object], dict[str, list[ReadableSpan]]]:
    events, finished = _export_turn(session_factory, model="fake-model")
    return events, _spans_by_name(finished)


def test_successful_turn_sets_explicit_ok_status() -> None:
    events, spans = _run_and_export()
    terminal = events[-1]
    assert isinstance(terminal, Final)
    assert terminal.status is SessionStatus.DONE
    root = spans["agent.run"][0]
    assert root.status.status_code is StatusCode.OK
    assert root.attributes["curie.terminal.cause"] == "completed"
    assert root.attributes["curie.terminal.status"] == "succeeded"
    assert all(
        generation.status.status_code is StatusCode.OK
        for generation in spans["llm.generation"]
    )


def test_caught_runner_failure_sets_explicit_error_status() -> None:
    def fail_script() -> list[object]:
        raise RuntimeError("placeholder runner failure")

    events, spans = _run_and_export(lambda: FakeModelSession(script_factory=fail_script))
    assert any(isinstance(event, ErrorEvent) for event in events)
    terminal = events[-1]
    assert isinstance(terminal, Final)
    assert terminal.status is SessionStatus.CLASSIFIED_FAILURE
    root = spans["agent.run"][0]
    assert root.status.status_code is StatusCode.ERROR
    assert root.attributes["curie.terminal.cause"] == "classified_failure"
    assert root.attributes["curie.terminal.status"] == "failed"
    assert all(
        generation.status.status_code is StatusCode.ERROR
        for generation in spans["llm.generation"]
    )


def test_classified_failure_uses_bounded_cause_without_raw_provider_reason() -> None:
    raw_reason = "private-provider-failure-reason-PLACEHOLDER"
    events, spans = _run_and_export(
        lambda: FakeModelSession(
            lambda: [_result(is_error=True, terminal_reason=raw_reason)]
        )
    )
    assert isinstance(events[-1], Final)
    assert events[-1].status is SessionStatus.CLASSIFIED_FAILURE
    root = spans["agent.run"][0]
    assert root.status.status_code is StatusCode.ERROR
    assert root.attributes["curie.phase"] == "provider_wait"
    assert root.attributes["curie.terminal.cause"] == "classified_failure"
    assert root.attributes["curie.terminal.status"] == "failed"
    assert raw_reason not in _span_wire_material(
        [span for same_name in spans.values() for span in same_name]
    )


@pytest.mark.parametrize("reason", ("aborted_streaming", "aborted_tools"))
def test_sdk_abort_without_runner_interrupt_is_a_classified_failure(reason: str) -> None:
    events, spans = _run_and_export(
        lambda: FakeModelSession(
            lambda: [_result(is_error=True, terminal_reason=reason)]
        )
    )
    assert [event.type for event in events] == ["error", "final"]
    assert isinstance(events[0], ErrorEvent)
    terminal = events[-1]
    assert isinstance(terminal, Final)
    assert terminal.status is SessionStatus.CLASSIFIED_FAILURE
    root = spans["agent.run"][0]
    generation = spans["llm.generation"][0]
    assert root.status.status_code is StatusCode.ERROR
    assert generation.status.status_code is StatusCode.ERROR
    assert root.attributes["curie.phase"] == "provider_wait"
    assert root.attributes["curie.terminal.cause"] == reason
    assert root.attributes["curie.terminal.status"] == "failed"


def test_approval_halt_abort_is_a_paused_non_error_terminal() -> None:
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    gate = ApprovalGate(
        required=frozenset({"Bash"}), route_by_tool={"Bash": "ops"}
    )

    async def record_gate(
        tool_name: str, tool_input: dict[str, Any], _context: Any
    ) -> PermissionResultDeny:
        gate.block(tool_name, tool_input)
        # Let the scripted SDK result through: this fixture models the real
        # post-denial shape where the CLI still supplies its abort result.
        return PermissionResultDeny(message="approval required", interrupt=False)

    script = [
        AssistantMessage(content=[TextBlock(text="working")], model="observed-model"),
        AssistantMessage(
            content=[
                ToolUseBlock(
                    id=_TOOL_CALL_ID,
                    name="Bash",
                    input={"command": "echo approval-pause-PLACEHOLDER"},
                )
            ],
            model="observed-model",
        ),
        _result(is_error=True, terminal_reason="aborted_tools"),
    ]
    fake = FakeModelSession(
        lambda: script,
        truncate_on_interrupt=False,
        can_use_tool=record_gate,
    )
    runner = SessionRunner(
        session_factory=lambda: fake,
        ceiling=0,
        tracer=RunTracer(provider),
        classifier=SideEffectClassifier(),
        trace_name="curie-run:test",
        model="configured-model",
        approval_gate=gate,
    )

    async def go() -> list[object]:
        await runner.start()
        try:
            lines = [
                line
                async for line in runner.run_turn(
                    Event(type="message", text="go", user="U0EXAMPLE1", ts="1")
                )
            ]
            return parse_ndjson("".join(lines))
        finally:
            await runner.close()

    events = anyio.run(go)
    terminal = events[-1]
    assert isinstance(terminal, Final)
    assert terminal.status is SessionStatus.AWAITING_APPROVAL

    spans = _spans_by_name(list(exporter.get_finished_spans()))
    root = spans["agent.run"][0]
    generation = spans["llm.generation"][0]
    tool = spans["execute_tool"][0]
    assert root.status.status_code is StatusCode.OK
    assert generation.status.status_code is StatusCode.OK
    assert tool.status.status_code is StatusCode.OK
    assert root.attributes["curie.terminal.cause"] == "approval_required"
    assert root.attributes["curie.terminal.status"] == "paused"
    assert root.attributes["curie.phase"] == "tool_wait"
    assert generation.attributes["curie.phase.end_kind"] == "tool_use_observed"
    assert tool.attributes["curie.phase.end_kind"] == "terminal_inferred"
    assert tool.attributes["curie.tool.outcome"] == "cancelled"


def test_interrupt_requested_wins_over_error_result_and_sdk_abort_reason() -> None:
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    script = [
        AssistantMessage(content=[TextBlock(text="working")], model="observed-model"),
        _result(is_error=True, terminal_reason="aborted_tools"),
    ]
    fake = FakeModelSession(lambda: script, truncate_on_interrupt=False)
    runner = SessionRunner(
        session_factory=lambda: fake,
        ceiling=0,
        tracer=RunTracer(provider),
        classifier=SideEffectClassifier(),
        trace_name="curie-run:test",
        model="configured-model",
    )
    events: list[object] = []

    async def go() -> None:
        await runner.start()
        turn = runner.run_turn(
            Event(type="message", text="go", user="U0EXAMPLE1", ts="1")
        )
        events.append(parse_ndjson_line(await anext(turn)))
        await runner.interrupt("operator stop")
        async for line in turn:
            events.append(parse_ndjson_line(line))
        await runner.close()

    anyio.run(go)
    terminal = events[-1]
    assert isinstance(terminal, Final)
    assert terminal.status is SessionStatus.IDLE_AWAITING_INPUT
    spans = _spans_by_name(list(exporter.get_finished_spans()))
    root = spans["agent.run"][0]
    generation = spans["llm.generation"][0]
    assert root.status.status_code is StatusCode.OK
    assert generation.status.status_code is StatusCode.OK
    assert root.attributes["curie.terminal.cause"] == "interrupt_requested"
    assert root.attributes["curie.terminal.status"] == "cancelled"


def test_runner_timeout_is_a_closed_error_terminal_and_is_first_store_stable() -> None:
    """The private timeout control is a failure, never an ACI cancellation.

    Timeout has higher precedence than an ordinary interrupt when both race, and
    the existing idempotent terminal guard must keep a later abandonment fallback
    from replacing the first stored cause.
    """

    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))

    with RunTracer(provider).run_span("curie-run:test", "fake-model") as gen:
        gen.query_observed()
        gen.finish_turn(
            timeout_requested=True,
            interrupt_requested=True,
            classified_failure=True,
        )
        gen.set_abandoned()

    spans = _spans_by_name(list(exporter.get_finished_spans()))
    root = spans["agent.run"][0]
    generation = spans["llm.generation"][0]
    assert root.attributes["curie.terminal.cause"] == "runner_timeout"
    assert root.attributes["curie.terminal.status"] == "failed"
    assert root.status.status_code is StatusCode.ERROR
    assert generation.status.status_code is StatusCode.ERROR
    assert generation.attributes["curie.phase.end_kind"] == "terminal_inferred"


def test_exhausted_stream_is_abandoned_not_a_false_success() -> None:
    _, spans = _run_and_export(lambda: FakeModelSession(lambda: []))
    root = spans["agent.run"][0]
    assert root.status.status_code is StatusCode.ERROR
    assert root.attributes["curie.phase"] == "provider_wait"
    assert root.attributes["curie.terminal.cause"] == "abandoned"
    assert root.attributes["curie.terminal.status"] == "abandoned"


def test_escaping_exception_exports_only_bounded_error_status() -> None:
    """OTel must not auto-record exception text or a stacktrace on span exit."""

    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    detail = "private-runner-detail-PLACEHOLDER"

    with pytest.raises(RuntimeError, match=detail):
        with RunTracer(provider).run_span("curie-run:test", "fake-model"):
            raise RuntimeError(detail)

    spans = list(exporter.get_finished_spans())
    assert [span.name for span in spans] == ["agent.run"]
    root = spans[0]
    assert root.status.status_code is StatusCode.ERROR
    assert root.attributes["curie.terminal.cause"] == "abandoned"
    assert root.attributes["curie.terminal.status"] == "abandoned"
    assert root.status.description is None
    assert not any(event.name == "exception" for event in root.events)
    assert detail not in repr((root.attributes, root.events, root.status))


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
