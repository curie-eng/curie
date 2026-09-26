"""SessionRunner: turn streaming, interrupt reclassification, rehydrate options."""

import asyncio
import functools
import logging

import anyio
import pytest
from aci_protocol import ErrorEvent, Event, Final, Interrupt, SessionStatus, parse_ndjson
from aiohttp import web
from aiohttp.test_utils import TestClient, TestServer
from claude_agent_sdk import AssistantMessage, ResultMessage, TextBlock, ToolUseBlock
from curie_runner import RunTracer, SideEffectClassifier, build_options, create_app
from curie_runner import session as session_module
from curie_runner.adapter import ClaudeAgentSession, McpServerReconnector
from curie_runner.fake import FakeModelSession, approval_turn, default_turn
from curie_runner.history import (
    HarnessReplayState,
    StateApiTranscriptStore,
    TurnRecord,
)
from curie_runner.hooks import build_gated_pre_tool_use_hooks
from curie_runner.mcp_tool_capability import (
    ConnectorAvailability,
    ConnectorCapabilityFailure,
)
from curie_runner.session import SessionRunner
from opentelemetry.sdk.trace import ReadableSpan, TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.trace import StatusCode


class _ProviderStallSession:
    def __init__(self, entered: anyio.Event, release: anyio.Event) -> None:
        self.entered = entered
        self.release = release
        self.interrupts = 0

    async def connect(self) -> None: ...

    async def query(self, _text: str) -> None: ...

    async def receive_turn(self):
        self.entered.set()
        await self.release.wait()
        if False:  # pragma: no cover - makes this an async generator
            yield None

    async def interrupt(self) -> None:
        self.interrupts += 1
        self.release.set()

    async def close(self) -> None:
        self.release.set()


class _ToolStallSession(_ProviderStallSession):
    async def receive_turn(self):
        yield AssistantMessage(
            content=[
                ToolUseBlock(
                    id="internal-stalled-call-PLACEHOLDER",
                    name="Read",
                    input={"path": "private-stalled-argument-PLACEHOLDER"},
                )
            ],
            model="observed-model",
        )
        self.entered.set()
        await self.release.wait()


class _ProviderInterruptErrorSession(_ProviderStallSession):
    async def receive_turn(self):
        if False:  # pragma: no cover - makes this an async generator
            yield None
        self.entered.set()
        await self.release.wait()
        assert self.interrupts > 0
        raise RuntimeError("provider transport stopped after interrupt")


class _ToolInterruptErrorSession(_ProviderStallSession):
    async def receive_turn(self):
        yield AssistantMessage(
            content=[
                ToolUseBlock(
                    id="internal-interrupt-error-call-PLACEHOLDER",
                    name="Read",
                    input={"path": "private-interrupt-error-argument-PLACEHOLDER"},
                )
            ],
            model="observed-model",
        )
        self.entered.set()
        await self.release.wait()
        assert self.interrupts > 0
        raise RuntimeError("tool transport stopped after interrupt")


def _span_named(spans: list[ReadableSpan], name: str) -> list[ReadableSpan]:
    return sorted(
        (span for span in spans if span.name == name),
        key=lambda span: span.start_time or 0,
    )


def _runner(
    script_factory=default_turn, ceiling: int = 0
) -> tuple[SessionRunner, FakeModelSession]:
    fake = FakeModelSession(script_factory)
    runner = SessionRunner(
        session_factory=lambda: fake,
        ceiling=ceiling,
        tracer=RunTracer(None),
        classifier=SideEffectClassifier(),
        trace_name="t",
    )
    return runner, fake


class _RecordingTranscriptStore:
    def __init__(self) -> None:
        self.attempts: list[TurnRecord] = []
        self.turns: list[TurnRecord] = []

    async def load(self) -> list[TurnRecord]:
        return list(self.turns)

    async def append(self, record: TurnRecord) -> bool:
        self.attempts.append(record)
        self.turns.append(record)
        return record.harness_replay is not None


class _BlockedTranscriptStore(_RecordingTranscriptStore):
    def __init__(self) -> None:
        super().__init__()
        self.entered = anyio.Event()
        self.release = anyio.Event()

    async def append(self, record: TurnRecord) -> bool:
        self.attempts.append(record)
        self.entered.set()
        await self.release.wait()
        self.turns.append(record)
        return record.harness_replay is not None


class _CapacityTranscriptStore(_RecordingTranscriptStore):
    async def append(self, record: TurnRecord) -> bool:
        from curie_runner.history import HistoryCapacityError

        self.attempts.append(record)
        raise HistoryCapacityError(413)


class _FirstBlockedTranscriptStore(_RecordingTranscriptStore):
    def __init__(self) -> None:
        super().__init__()
        self.entered = anyio.Event()
        self.cancelled = anyio.Event()

    async def append(self, record: TurnRecord) -> bool:
        self.attempts.append(record)
        if len(self.attempts) == 1:
            self.entered.set()
            try:
                await anyio.sleep_forever()
            finally:
                self.cancelled.set()
        self.turns.append(record)
        return record.harness_replay is not None


class _ReplayExportSession(FakeModelSession):
    def __init__(self, mode: str) -> None:
        super().__init__(default_turn)
        self.mode = mode

    async def export_replay_state(self) -> HarnessReplayState:
        if self.mode == "error":
            raise RuntimeError("private replay token-PLACEHOLDER")
        await anyio.sleep_forever()
        raise AssertionError("unreachable")


def _runner_with_history(
    store,
    *,
    script_factory=default_turn,
    tracer: RunTracer | None = None,
    session: FakeModelSession | None = None,
) -> tuple[SessionRunner, FakeModelSession]:
    fake = session or FakeModelSession(script_factory)
    return (
        SessionRunner(
            session_factory=lambda: fake,
            ceiling=0,
            tracer=tracer or RunTracer(None),
            classifier=SideEffectClassifier(),
            trace_name="history-test",
            session_id="session-PLACEHOLDER",
            history_store=store,
        ),
        fake,
    )


def _drain(runner: SessionRunner, frame) -> list:
    lines: list[str] = []

    async def go() -> None:
        await runner.start()
        async for line in runner.run_inbound(frame):
            lines.append(line)

    anyio.run(go)
    return parse_ndjson("".join(lines))


def test_happy_turn_stream_shape() -> None:
    runner, fake = _runner()
    events = _drain(runner, Event(type="message", text="go", user="U", ts="1"))
    types = [e.type for e in events]
    assert types[0] == "text_delta"
    assert "tool_note" in types
    assert "side_effect_flag" in types
    assert types[-1] == "final"
    assert events[-1].status == SessionStatus.DONE
    assert fake.queries == ["go"]  # the event text was pushed into the session
    assert runner.status == SessionStatus.DONE


def test_transcript_capacity_failure_precedes_terminal_final(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sensitive_body = (
        "https://state.example.com/agents/A/state/transcript/private-key "
        "private transcript text token-PLACEHOLDER"
    )
    append_attempts: list[dict[str, object]] = []
    app = web.Application()

    async def reject_append(request: web.Request) -> web.Response:
        append_attempts.append(await request.json())
        return web.Response(status=413, text=sensitive_body)

    async def get_history(_request: web.Request) -> web.Response:
        return web.json_response(
            {"detail": "not found"},
            status=404,
            headers={"X-Curie-Transcript-Max-Bytes": "65536"},
        )

    app.router.add_get("/agents/A/state/transcript/t1", get_history)
    app.router.add_post("/agents/A/state/transcript/t1/append", reject_append)
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    metrics: list[tuple[str, dict[str, object]]] = []

    def capture_metric(
        name: str,
        _value: float = 1,
        *,
        attributes: dict[str, object],
    ) -> None:
        metrics.append((name, dict(attributes)))

    monkeypatch.setattr(session_module, "record_metric", capture_metric)

    async def go() -> tuple[list, SessionRunner]:
        async with TestServer(app) as server:
            store = StateApiTranscriptStore(
                str(server.make_url("/agents/A/state/transcript/t1")), token=None
            )
            assert await store.load() == []
            runner, _fake = _runner_with_history(store, tracer=RunTracer(provider))
            await runner.start()
            lines = [
                line
                async for line in runner.run_turn(
                    Event(type="message", text="question", user="U", ts="1")
                )
            ]
            await runner.close()
            return parse_ndjson("".join(lines)), runner

    events, runner = anyio.run(go)
    errors = [event for event in events if isinstance(event, ErrorEvent)]
    finals = [event for event in events if isinstance(event, Final)]
    assert len(errors) == 1
    assert errors[0].classification == "history-persistence-error"
    assert sensitive_body not in errors[0].message
    assert len(finals) == 1
    assert finals[0].status is SessionStatus.CLASSIFIED_FAILURE
    assert finals[0].text != "all done"
    assert finals[0].approval_summary is None
    assert finals[0].approval_route is None
    assert events[-2:] == [errors[0], finals[0]]
    assert len(append_attempts) == 1
    assert runner.history_durable is False

    roots = _span_named(list(exporter.get_finished_spans()), "agent.run")
    assert len(roots) == 1
    assert roots[0].attributes["curie.terminal.cause"] == "classified_failure"
    assert roots[0].attributes["curie.terminal.status"] == "failed"
    assert roots[0].status.status_code is StatusCode.ERROR
    completed = [
        attributes
        for name, attributes in metrics
        if name == "curie.turn.completed"
    ]
    assert completed == [
        {
            "service.name": "curie-runner",
            "source": "runner",
            "outcome": "classified_failure",
        }
    ]


@pytest.mark.parametrize(
    ("script_factory", "expected_status"),
    (
        (default_turn, SessionStatus.DONE),
        (lambda: approval_turn("Approve the action"), SessionStatus.AWAITING_APPROVAL),
        (
            lambda: approval_turn("Approve the action")[:-1],
            SessionStatus.AWAITING_APPROVAL,
        ),
    ),
)
def test_persistable_terminal_waits_for_append_and_closes_control_window(
    script_factory,
    expected_status: SessionStatus,
) -> None:
    store = _BlockedTranscriptStore()
    runner, fake = _runner_with_history(store, script_factory=script_factory)
    epoch = "P" * 43
    lines: list[str] = []

    async def go() -> None:
        await runner.start()

        async def consume() -> None:
            async for line in runner.run_turn(
                Event(type="message", text="question", user="U", ts="1"),
                turn_epoch=epoch,
            ):
                lines.append(line)

        async with anyio.create_task_group() as tasks:
            tasks.start_soon(consume)
            await store.entered.wait()
            observed = parse_ndjson("".join(lines))
            assert not any(isinstance(event, Final) for event in observed)
            assert runner.turn_active is False
            with anyio.fail_after(0.2):
                assert await runner.timeout(epoch) is False
            with anyio.fail_after(0.2):
                assert await runner.steer("too late") is False
            interrupts_before = fake.interrupts
            with anyio.fail_after(0.2):
                await runner.interrupt("late stop")
            assert fake.interrupts == interrupts_before
            store.release.set()

        events = parse_ndjson("".join(lines))
        assert events[-1].status is expected_status
        assert len(store.turns) == 1
        if expected_status is SessionStatus.AWAITING_APPROVAL:
            assert store.turns[0].approval is not None
            assert store.turns[0].approval.summary == "Approve the action"
        assert all(
            not (
                message.role == "user"
                and message.content == "too late"
            )
            for message in store.turns[0].messages
        )

        interrupts_before_idle = fake.interrupts
        with anyio.fail_after(0.2):
            await runner.interrupt("idle lease fence")
        assert fake.interrupts == interrupts_before_idle + 1
        await runner.close()

    anyio.run(go)


def test_closing_stream_after_success_final_cannot_skip_append() -> None:
    store = _RecordingTranscriptStore()
    runner, _fake = _runner_with_history(store)

    async def go() -> None:
        await runner.start()
        stream = runner.run_turn(
            Event(type="message", text="question", user="U", ts="1")
        )
        async for line in stream:
            event = parse_ndjson(line)[0]
            if isinstance(event, Final):
                assert event.status is SessionStatus.DONE
                break
        await stream.aclose()

    anyio.run(go)
    assert len(store.turns) == 1
    assert store.turns[0].user == "question"
    assert runner.history_durable is True


@pytest.mark.parametrize(
    "script_factory",
    (
        lambda: approval_turn("Approve the action"),
        lambda: approval_turn("Approve the action")[:-1],
    ),
)
def test_approval_capacity_failure_clears_pending_approval(script_factory) -> None:
    store = _CapacityTranscriptStore()
    runner, _fake = _runner_with_history(store, script_factory=script_factory)

    async def go() -> list:
        await runner.start()
        lines = [
            line
            async for line in runner.run_turn(
                Event(type="message", text="question", user="U", ts="1")
            )
        ]
        return parse_ndjson("".join(lines))

    events = anyio.run(go)
    errors = [event for event in events if isinstance(event, ErrorEvent)]
    finals = [event for event in events if isinstance(event, Final)]
    assert len(errors) == 1
    assert errors[0].classification == "history-persistence-error"
    assert len(finals) == 1
    assert finals[0].status is SessionStatus.CLASSIFIED_FAILURE
    assert finals[0].approval_summary is None
    assert finals[0].approval_route is None
    assert events[-2:] == [errors[0], finals[0]]
    assert len(store.attempts) == 1
    assert store.turns == []
    assert runner.history_durable is False


def test_capacity_loss_stays_sticky_after_a_later_successful_append() -> None:
    class RecoveringStore(_RecordingTranscriptStore):
        async def append(self, record: TurnRecord) -> bool:
            from curie_runner.history import HistoryCapacityError

            self.attempts.append(record)
            if len(self.attempts) == 1:
                raise HistoryCapacityError(413)
            self.turns.append(record)
            return record.harness_replay is not None

    store = RecoveringStore()
    runner, _fake = _runner_with_history(store)

    async def go() -> list[list]:
        await runner.start()
        turns: list[list[object]] = []
        for text, ts in (("first", "1"), ("second", "2")):
            lines = [
                line
                async for line in runner.run_turn(
                    Event(type="message", text=text, user="U", ts=ts)
                )
            ]
            turns.append(parse_ndjson("".join(lines)))
        return turns

    first, second = anyio.run(go)
    assert first[-1].status is SessionStatus.CLASSIFIED_FAILURE
    assert second[-1].status is SessionStatus.DONE
    assert [record.user for record in store.turns] == ["second"]
    assert runner.history_durable is False


@pytest.mark.parametrize("mode", ("error", "timeout"))
def test_optional_replay_export_failure_still_appends_portable_history(
    mode: str,
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    if mode == "timeout":
        monkeypatch.setattr(
            session_module,
            "_HISTORY_REPLAY_EXPORT_BUDGET_SECONDS",
            0.01,
        )
    store = _RecordingTranscriptStore()
    session = _ReplayExportSession(mode)
    runner, _fake = _runner_with_history(store, session=session)

    with caplog.at_level(logging.WARNING, logger="curie_runner.session"):
        events = _drain(
            runner,
            Event(type="message", text="question", user="U", ts="1"),
        )

    assert events[-1].status is SessionStatus.DONE
    assert len(store.turns) == 1
    assert store.turns[0].harness_replay is None
    assert runner.history_durable is True
    messages = [record.getMessage() for record in caplog.records]
    assert any("harness replay export failed" in message for message in messages)
    assert all("private replay token-PLACEHOLDER" not in message for message in messages)


def test_persistence_stage_timeout_is_bounded_and_loss_stays_sticky(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    assert session_module._HISTORY_PERSISTENCE_BUDGET_SECONDS == 15.0
    monkeypatch.setattr(
        session_module,
        "_HISTORY_PERSISTENCE_BUDGET_SECONDS",
        0.05,
    )
    store = _FirstBlockedTranscriptStore()
    runner, _fake = _runner_with_history(store)

    async def go() -> tuple[list, list]:
        await runner.start()
        with anyio.fail_after(1):
            first_lines = [
                line
                async for line in runner.run_turn(
                    Event(type="message", text="first", user="U", ts="1")
                )
            ]
        await store.cancelled.wait()
        second_lines = [
            line
            async for line in runner.run_turn(
                Event(type="message", text="second", user="U", ts="2")
            )
        ]
        return parse_ndjson("".join(first_lines)), parse_ndjson("".join(second_lines))

    first, second = anyio.run(go)
    assert first[-1].status is SessionStatus.DONE
    assert second[-1].status is SessionStatus.DONE
    assert [record.user for record in store.turns] == ["second"]
    assert runner.turn_active is False
    assert runner.history_durable is False


def test_cancellation_during_persistence_marks_loss_without_a_terminal() -> None:
    store = _FirstBlockedTranscriptStore()
    runner, fake = _runner_with_history(store)
    first_lines: list[str] = []

    async def go() -> list:
        await runner.start()
        cancelled = anyio.Event()
        scope_holder: list[anyio.CancelScope] = []

        async def consume_first() -> None:
            with anyio.CancelScope() as scope:
                scope_holder.append(scope)
                async for line in runner.run_turn(
                    Event(type="message", text="first", user="U", ts="1")
                ):
                    first_lines.append(line)
            cancelled.set()

        async with anyio.create_task_group() as tasks:
            tasks.start_soon(consume_first)
            await store.entered.wait()
            scope_holder[0].cancel()
            await cancelled.wait()
        await store.cancelled.wait()
        second_lines = [
            line
            async for line in runner.run_turn(
                Event(type="message", text="second", user="U", ts="2")
            )
        ]
        return parse_ndjson("".join(second_lines))

    second = anyio.run(go)
    first = parse_ndjson("".join(first_lines))
    assert not any(isinstance(event, Final) for event in first)
    assert second[-1].status is SessionStatus.DONE
    assert [record.user for record in store.turns] == ["second"]
    assert fake.interrupts == 0
    assert runner.turn_active is False
    assert runner.history_durable is False


def test_first_resumed_turn_records_cache_read_metric_once(monkeypatch) -> None:
    calls: list[tuple[str, int | float | None, dict[str, object] | None]] = []

    def record(name, value=None, *, attributes=None):
        calls.append((name, value, attributes))

    monkeypatch.setattr("curie_runner.session.record_metric", record)

    def turn():
        return [
            AssistantMessage(content=[TextBlock(text="ok")], model="fake"),
            ResultMessage(
                subtype="success",
                duration_ms=1,
                duration_api_ms=1,
                is_error=False,
                num_turns=1,
                session_id="sdk-session",
                usage={
                    "input_tokens": 1,
                    "output_tokens": 1,
                    "cache_read_input_tokens": 321,
                    "cache_creation_input_tokens": 0,
                },
                result="ok",
            ),
        ]

    runner = SessionRunner(
        session_factory=lambda: FakeModelSession(turn),
        ceiling=0,
        tracer=RunTracer(None),
        classifier=SideEffectClassifier(),
        trace_name="resume-cache",
        history_resumed=True,
    )

    async def go() -> None:
        await runner.start()
        for ts in ("1", "2"):
            async for _ in runner.run_turn(
                Event(type="message", text="continue", user="U", ts=ts)
            ):
                pass

    anyio.run(go)

    cache_calls = [call for call in calls if call[0] == "curie.history.resume.cache_read"]
    assert cache_calls == [
        (
            "curie.history.resume.cache_read",
            321,
            {"service.name": "curie-runner", "source": "runner", "cache_hit": "true"},
        )
    ]


@pytest.mark.parametrize(
    ("session_type", "expected_phase", "expect_tool"),
    (
        (_ProviderStallSession, "provider_wait", False),
        (_ToolStallSession, "tool_wait", True),
    ),
)
def test_interrupting_a_stalled_phase_preserves_phase_and_cancels_cleanly(
    session_type,
    expected_phase: str,
    expect_tool: bool,
) -> None:
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))

    async def go() -> tuple[list[str], _ProviderStallSession]:
        entered = anyio.Event()
        release = anyio.Event()
        session = session_type(entered, release)
        runner = SessionRunner(
            session_factory=lambda: session,
            ceiling=0,
            tracer=RunTracer(provider),
            classifier=SideEffectClassifier(),
            trace_name="t",
        )
        lines: list[str] = []

        async def consume() -> None:
            async for line in runner.run_turn(
                Event(type="message", text="go", user="U", ts="1")
            ):
                lines.append(line)

        await runner.start()
        async with anyio.create_task_group() as tasks:
            tasks.start_soon(consume)
            await entered.wait()
            await runner.interrupt("operator stop")
        await runner.close()
        return lines, session

    lines, session = anyio.run(go)
    events = parse_ndjson("".join(lines))
    assert events[-1].status is SessionStatus.IDLE_AWAITING_INPUT
    assert session.interrupts >= 1
    spans = list(exporter.get_finished_spans())
    root = _span_named(spans, "agent.run")[0]
    assert root.attributes["curie.phase"] == expected_phase
    assert root.attributes["curie.terminal.cause"] == "interrupt_requested"
    assert root.attributes["curie.terminal.status"] == "cancelled"
    assert root.status.status_code is StatusCode.OK
    tools = _span_named(spans, "execute_tool")
    assert bool(tools) is expect_tool
    if tools:
        assert tools[0].attributes["curie.tool.outcome"] == "cancelled"
        assert "internal-stalled-call-PLACEHOLDER" not in repr(tools[0].attributes)


@pytest.mark.parametrize(
    ("session_type", "expected_phase", "expect_tool"),
    (
        (_ProviderInterruptErrorSession, "provider_wait", False),
        (_ToolInterruptErrorSession, "tool_wait", True),
    ),
)
def test_interrupt_precedes_iterator_exception_and_preserves_cancelled_terminal(
    session_type,
    expected_phase: str,
    expect_tool: bool,
) -> None:
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))

    async def go() -> tuple[list[str], _ProviderStallSession]:
        entered = anyio.Event()
        release = anyio.Event()
        session = session_type(entered, release)
        runner = SessionRunner(
            session_factory=lambda: session,
            ceiling=0,
            tracer=RunTracer(provider),
            classifier=SideEffectClassifier(),
            trace_name="t",
        )
        lines: list[str] = []

        async def consume() -> None:
            async for line in runner.run_turn(
                Event(type="message", text="go", user="U", ts="1")
            ):
                lines.append(line)

        await runner.start()
        async with anyio.create_task_group() as tasks:
            tasks.start_soon(consume)
            await entered.wait()
            await runner.interrupt("operator stop")
        await runner.close()
        return lines, session

    lines, session = anyio.run(go)
    events = parse_ndjson("".join(lines))
    assert not any(isinstance(event, ErrorEvent) for event in events)
    assert events[-1].status is SessionStatus.IDLE_AWAITING_INPUT
    assert session.interrupts >= 1

    spans = list(exporter.get_finished_spans())
    root = _span_named(spans, "agent.run")[0]
    assert root.attributes["curie.phase"] == expected_phase
    assert root.attributes["curie.terminal.cause"] == "interrupt_requested"
    assert root.attributes["curie.terminal.status"] == "cancelled"
    assert root.status.status_code is StatusCode.OK
    tools = _span_named(spans, "execute_tool")
    assert bool(tools) is expect_tool
    if tools:
        assert tools[0].status.status_code is StatusCode.OK
        assert tools[0].attributes["curie.phase.end_kind"] == "terminal_inferred"
        assert tools[0].attributes["curie.tool.outcome"] == "cancelled"
        assert "internal-interrupt-error-call-PLACEHOLDER" not in repr(
            tools[0].attributes
        )


def test_timeout_terminalizes_before_generator_close_and_emits_one_metric(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Closing at a suspended yield must not let abandonment store first."""

    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    fake = FakeModelSession()
    store = _RecordingTranscriptStore()
    runner = SessionRunner(
        session_factory=lambda: fake,
        ceiling=0,
        tracer=RunTracer(provider),
        classifier=SideEffectClassifier(),
        trace_name="t",
        history_store=store,
    )
    metrics: list[tuple[str, dict[str, str]]] = []

    def capture_metric(
        name: str,
        _value: float = 1,
        *,
        attributes: dict[str, str],
    ) -> None:
        metrics.append((name, dict(attributes)))

    monkeypatch.setattr(session_module, "record_metric", capture_metric)
    epoch = "A" * 43

    async def go() -> None:
        await runner.start()
        turn = runner.run_turn(
            Event(type="message", text="go", user="U0EXAMPLE1", ts="1"),
            turn_epoch=epoch,
        )
        await anext(turn)
        assert await runner.timeout(epoch) is True
        # No further yield is requested. GeneratorExit reaches run_turn's
        # no-yield finally, which must store timeout before RunTracer can infer
        # generic abandonment.
        await turn.aclose()
        await runner.close()

    anyio.run(go)
    spans = list(exporter.get_finished_spans())
    root = _span_named(spans, "agent.run")[0]
    assert root.attributes["curie.terminal.cause"] == "runner_timeout"
    assert root.attributes["curie.terminal.status"] == "failed"
    assert root.status.status_code is StatusCode.ERROR
    completed = [
        attributes
        for name, attributes in metrics
        if name == "curie.turn.completed"
    ]
    assert completed == [
        {
            "service.name": "curie-runner",
            "source": "runner",
            "outcome": "classified_failure",
        }
    ]
    assert store.attempts == []


def test_timeout_epoch_is_isolated_from_stale_spoofed_and_replayed_turns() -> None:
    runner, fake = _runner()
    first_epoch = "B" * 43
    second_epoch = "C" * 43
    spoofed_epoch = "D" * 43

    async def go() -> None:
        await runner.start()
        first = runner.run_turn(
            Event(type="message", text="first", user="U0EXAMPLE1", ts="1"),
            turn_epoch=first_epoch,
        )
        async for _ in first:
            pass

        interrupts_after_first = fake.interrupts
        assert await runner.timeout(first_epoch) is False
        assert fake.interrupts == interrupts_after_first

        second = runner.run_turn(
            Event(type="message", text="second", user="U0EXAMPLE1", ts="2"),
            turn_epoch=second_epoch,
        )
        await anext(second)
        assert await runner.timeout(first_epoch) is False
        assert await runner.timeout(spoofed_epoch) is False
        assert fake.interrupts == interrupts_after_first

        assert await runner.timeout(second_epoch) is True
        async for _ in second:
            pass
        interrupts_after_timeout = fake.interrupts
        assert await runner.timeout(second_epoch) is False
        assert fake.interrupts == interrupts_after_timeout
        await runner.close()

    anyio.run(go)


def test_timeout_during_abandonment_cleanup_cannot_own_next_turn_stop() -> None:
    class CleanupRaceSession:
        def __init__(self) -> None:
            self.queries: list[str] = []
            self.interrupt_lock = anyio.Lock()
            self.interrupt_roles: list[str] = []
            self.first_interrupt_entered = anyio.Event()
            self.release_first_interrupt = anyio.Event()
            self.second_interrupt_entered = anyio.Event()
            self.release_second_interrupt = anyio.Event()
            self.turn_b_cleanup_started = False

        async def connect(self) -> None: ...

        async def query(self, text: str) -> None:
            self.queries.append(text)

        async def receive_turn(self):
            yield AssistantMessage(
                content=[TextBlock(text=f"working-{len(self.queries)}")],
                model="fake-model",
            )

        async def interrupt(self) -> None:
            async with self.interrupt_lock:
                self.interrupt_roles.append(
                    "turn_b_cleanup"
                    if self.turn_b_cleanup_started
                    else "before_turn_b_cleanup"
                )
                attempt = len(self.interrupt_roles)
                if attempt == 1:
                    self.first_interrupt_entered.set()
                    await self.release_first_interrupt.wait()
                elif attempt == 2:
                    self.second_interrupt_entered.set()
                    await self.release_second_interrupt.wait()

        async def close(self) -> None:
            self.release_first_interrupt.set()
            self.release_second_interrupt.set()

    session = CleanupRaceSession()
    runner = SessionRunner(
        session_factory=lambda: session,
        ceiling=0,
        tracer=RunTracer(None),
        classifier=SideEffectClassifier(),
        trace_name="t",
    )
    first_epoch = "J" * 43
    second_epoch = "K" * 43

    async def go() -> None:
        first_close_done = anyio.Event()
        timeout_started = anyio.Event()
        timeout_done = anyio.Event()
        timeout_accepted: list[bool] = []

        async def close_first() -> None:
            await first.aclose()
            first_close_done.set()

        async def request_late_timeout() -> None:
            timeout_started.set()
            timeout_accepted.append(await runner.timeout(first_epoch))
            timeout_done.set()

        await runner.start()
        first = runner.run_turn(
            Event(type="message", text="first", user="U0EXAMPLE1", ts="1"),
            turn_epoch=first_epoch,
        )
        await anext(first)

        async with anyio.create_task_group() as tasks:
            tasks.start_soon(close_first)
            await session.first_interrupt_entered.wait()
            tasks.start_soon(request_late_timeout)
            await timeout_started.wait()
            # Let the control request either reject the retired epoch or queue
            # behind turn A's cleanup stop. No wall-clock timing is involved.
            await anyio.lowlevel.checkpoint()
            await anyio.lowlevel.checkpoint()
            session.release_first_interrupt.set()
            await first_close_done.wait()

            second = runner.run_turn(
                Event(type="message", text="second", user="U0EXAMPLE1", ts="2"),
                turn_epoch=second_epoch,
            )
            await anext(second)

            if not timeout_done.is_set():
                # Current buggy behavior reaches here: the timeout stop was
                # accepted for A but its acknowledgement lands during B.
                await session.second_interrupt_entered.wait()
                session.release_second_interrupt.set()
                await timeout_done.wait()

            session.turn_b_cleanup_started = True
            second_close_done = anyio.Event()

            async def close_second() -> None:
                await second.aclose()
                second_close_done.set()

            tasks.start_soon(close_second)
            if not second_close_done.is_set():
                await session.second_interrupt_entered.wait()
                session.release_second_interrupt.set()
            await second_close_done.wait()

        assert timeout_accepted == [False]
        assert session.interrupt_roles == [
            "before_turn_b_cleanup",
            "turn_b_cleanup",
        ]
        await runner.close()

    anyio.run(go)


def test_next_turn_waits_for_timeout_interrupt_to_settle() -> None:
    class SlowTimeoutInterruptSession:
        def __init__(self) -> None:
            self.queries: list[str] = []
            self.interrupts = 0
            self.first_receive_entered = anyio.Event()
            self.end_first_receive = anyio.Event()
            self.interrupt_entered = anyio.Event()
            self.release_interrupt = anyio.Event()
            self.interrupt_settled = anyio.Event()
            self.second_query_started = anyio.Event()
            self.second_query_started_early = False

        async def connect(self) -> None: ...

        async def query(self, text: str) -> None:
            self.queries.append(text)
            if len(self.queries) == 2:
                self.second_query_started_early = not self.interrupt_settled.is_set()
                self.second_query_started.set()

        async def receive_turn(self):
            if len(self.queries) == 1:
                self.first_receive_entered.set()
                await self.end_first_receive.wait()
                return
            yield ResultMessage(
                subtype="success",
                duration_ms=1,
                duration_api_ms=1,
                is_error=False,
                num_turns=1,
                session_id="sdk-session-PLACEHOLDER",
                result="healthy",
            )

        async def interrupt(self) -> None:
            self.interrupts += 1
            self.end_first_receive.set()
            self.interrupt_entered.set()
            await self.release_interrupt.wait()
            self.interrupt_settled.set()

        async def close(self) -> None: ...

    session = SlowTimeoutInterruptSession()
    runner = SessionRunner(
        session_factory=lambda: session,
        ceiling=0,
        tracer=RunTracer(None),
        classifier=SideEffectClassifier(),
        trace_name="t",
    )
    first_epoch = "F" * 43
    second_epoch = "G" * 43
    first_lines: list[str] = []
    second_lines: list[str] = []

    async def go() -> None:
        first_done = anyio.Event()
        second_attempted = anyio.Event()
        second_done = anyio.Event()
        timeout_done = anyio.Event()
        timeout_accepted: list[bool] = []

        async def consume_first() -> None:
            async for line in runner.run_turn(
                Event(type="message", text="first", user="U0EXAMPLE1", ts="1"),
                turn_epoch=first_epoch,
            ):
                first_lines.append(line)
            first_done.set()

        async def request_timeout() -> None:
            timeout_accepted.append(await runner.timeout(first_epoch))
            timeout_done.set()

        async def consume_second() -> None:
            second_attempted.set()
            async for line in runner.run_turn(
                Event(type="message", text="second", user="U0EXAMPLE1", ts="2"),
                turn_epoch=second_epoch,
            ):
                second_lines.append(line)
            second_done.set()

        await runner.start()
        async with anyio.create_task_group() as tasks:
            tasks.start_soon(consume_first)
            await session.first_receive_entered.wait()
            tasks.start_soon(request_timeout)
            await session.interrupt_entered.wait()
            tasks.start_soon(consume_second)
            await second_attempted.wait()
            # Let the timeout, first consumer, and already-queued second
            # consumer advance to their ownership barriers. No clock delay is
            # involved: checkpoints only give each runnable task a turn.
            await anyio.lowlevel.checkpoint()
            await anyio.lowlevel.checkpoint()

            assert not first_done.is_set()
            assert not session.second_query_started.is_set()
            assert not timeout_done.is_set()

            session.release_interrupt.set()
            await timeout_done.wait()
            await first_done.wait()
            await session.second_query_started.wait()
            await second_done.wait()

        assert timeout_accepted == [True]
        await runner.close()

    anyio.run(go)

    first_events = parse_ndjson("".join(first_lines))
    second_events = parse_ndjson("".join(second_lines))
    assert first_events[-1].status is SessionStatus.CLASSIFIED_FAILURE
    assert second_events[-1].status is SessionStatus.DONE
    assert session.queries == ["first", "second"]
    assert session.interrupts == 1
    assert session.interrupt_settled.is_set()
    assert not session.second_query_started_early


class _OrderedTimeoutWireSession:
    """Event-gated SDK double exposing stop/query order without clock sleeps."""

    def __init__(self, failure: str) -> None:
        self.failure = failure
        self.wire: list[tuple[str, str | int]] = []
        self.queries = 0
        self.interrupts = 0
        self.first_receive_entered = anyio.Event()
        self.end_first_receive = anyio.Event()
        self.first_interrupt_entered = anyio.Event()
        self.cleanup_interrupt_written = anyio.Event()
        self.release_ack = anyio.Event()
        self.second_query_started = anyio.Event()

    async def connect(self) -> None: ...

    async def query(self, text: str) -> None:
        self.queries += 1
        self.wire.append(("query", text))
        if self.queries == 2:
            self.second_query_started.set()

    async def receive_turn(self):
        if self.queries == 1:
            self.first_receive_entered.set()
            await self.end_first_receive.wait()
            return
        yield ResultMessage(
            subtype="success",
            duration_ms=1,
            duration_api_ms=1,
            is_error=False,
            num_turns=1,
            session_id="sdk-session-PLACEHOLDER",
            result="healthy",
        )

    async def interrupt(self) -> None:
        self.interrupts += 1
        attempt = self.interrupts
        self.first_interrupt_entered.set()
        if self.failure == "cancel-before-write" and attempt == 1:
            await self.release_ack.wait()
            return
        self.wire.append(("interrupt", attempt))
        if attempt > 1:
            self.cleanup_interrupt_written.set()
        if self.failure == "ack-failure" and attempt == 1:
            self.end_first_receive.set()
            raise RuntimeError("Control request timeout: interrupt")
        await self.release_ack.wait()
        self.end_first_receive.set()

    async def close(self) -> None:
        self.release_ack.set()
        self.end_first_receive.set()


def _exercise_timeout_interrupt_failure(
    failure: str,
) -> tuple[
    _OrderedTimeoutWireSession,
    list[str],
    list[str],
    list[BaseException],
    list[ReadableSpan],
]:
    session = _OrderedTimeoutWireSession(failure)
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    runner = SessionRunner(
        session_factory=lambda: session,
        ceiling=0,
        tracer=RunTracer(provider),
        classifier=SideEffectClassifier(),
        trace_name="t",
    )
    first_epoch = "H" * 43
    second_epoch = "I" * 43
    first_lines: list[str] = []
    second_lines: list[str] = []
    timeout_errors: list[BaseException] = []

    async def go() -> None:
        first_done = anyio.Event()
        second_done = anyio.Event()
        timeout_done = anyio.Event()
        first_scope = anyio.CancelScope()
        timeout_scope = anyio.CancelScope()

        async def consume_first() -> None:
            with first_scope:
                try:
                    async for line in runner.run_turn(
                        Event(
                            type="message",
                            text="first",
                            user="U0EXAMPLE1",
                            ts="1",
                        ),
                        turn_epoch=first_epoch,
                    ):
                        first_lines.append(line)
                except anyio.get_cancelled_exc_class():
                    pass
            first_done.set()

        async def request_timeout() -> None:
            with timeout_scope:
                try:
                    await runner.timeout(first_epoch)
                except BaseException as exc:
                    timeout_errors.append(exc)
            timeout_done.set()

        async def consume_second() -> None:
            async for line in runner.run_turn(
                Event(type="message", text="second", user="U0EXAMPLE1", ts="2"),
                turn_epoch=second_epoch,
            ):
                second_lines.append(line)
            second_done.set()

        await runner.start()
        async with anyio.create_task_group() as tasks:
            tasks.start_soon(consume_first)
            await session.first_receive_entered.wait()
            tasks.start_soon(request_timeout)
            await session.first_interrupt_entered.wait()

            if failure.startswith("cancel-"):
                timeout_scope.cancel()
                await timeout_done.wait()
                # Abandon the blocked consumer. Its run_turn finalizer must see
                # the still-open turn and send the safety-net stop; ending the
                # fake iterator normally would set _turn_open=False first and
                # would not exercise this cancellation shape.
                first_scope.cancel()
                await session.cleanup_interrupt_written.wait()
                session.release_ack.set()
                await first_done.wait()
                tasks.start_soon(consume_second)
            else:
                await timeout_done.wait()
                tasks.start_soon(consume_second)

            await first_done.wait()
            await session.second_query_started.wait()
            await second_done.wait()

        await runner.close()

    anyio.run(go)
    return session, first_lines, second_lines, timeout_errors, list(
        exporter.get_finished_spans()
    )


def _assert_timeout_then_healthy(
    first_lines: list[str],
    second_lines: list[str],
    spans: list[ReadableSpan],
    *,
    first_was_abandoned: bool = False,
) -> None:
    second_events = parse_ndjson("".join(second_lines))
    if first_was_abandoned:
        assert not first_lines
    else:
        first_events = parse_ndjson("".join(first_lines))
        assert first_events[-1].status is SessionStatus.CLASSIFIED_FAILURE
    assert second_events[-1].status is SessionStatus.DONE
    first_root = _span_named(spans, "agent.run")[0]
    assert first_root.attributes["curie.terminal.cause"] == "runner_timeout"
    assert first_root.attributes["curie.terminal.status"] == "failed"
    assert first_root.status.status_code is StatusCode.ERROR


def test_timeout_cancelled_after_stop_write_cleans_up_before_next_query() -> None:
    session, first, second, errors, spans = _exercise_timeout_interrupt_failure(
        "cancel-after-write"
    )
    assert len(errors) == 1
    assert isinstance(errors[0], asyncio.CancelledError)
    assert session.wire == [
        ("query", "first"),
        ("interrupt", 1),
        ("interrupt", 2),
        ("query", "second"),
    ]
    _assert_timeout_then_healthy(first, second, spans, first_was_abandoned=True)


def test_timeout_ack_failure_releases_only_after_recorded_stop() -> None:
    session, first, second, errors, spans = _exercise_timeout_interrupt_failure(
        "ack-failure"
    )
    assert [type(error) for error in errors] == [RuntimeError]
    assert session.wire == [
        ("query", "first"),
        ("interrupt", 1),
        ("query", "second"),
    ]
    _assert_timeout_then_healthy(first, second, spans)


def test_timeout_cancelled_before_stop_write_uses_cleanup_before_next_query() -> None:
    session, first, second, errors, spans = _exercise_timeout_interrupt_failure(
        "cancel-before-write"
    )
    assert len(errors) == 1
    assert isinstance(errors[0], asyncio.CancelledError)
    assert session.wire == [
        ("query", "first"),
        ("interrupt", 2),
        ("query", "second"),
    ]
    _assert_timeout_then_healthy(first, second, spans, first_was_abandoned=True)


def test_timeout_precedes_an_operator_interrupt_on_the_same_turn() -> None:
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    script = [
        AssistantMessage(content=[TextBlock(text="working")], model="fake-model"),
        ResultMessage(
            subtype="error_during_execution",
            duration_ms=1,
            duration_api_ms=1,
            is_error=True,
            num_turns=1,
            session_id="sdk-session-PLACEHOLDER",
            result="stopped",
        ),
    ]
    fake = FakeModelSession(lambda: script, truncate_on_interrupt=False)
    runner = SessionRunner(
        session_factory=lambda: fake,
        ceiling=0,
        tracer=RunTracer(provider),
        classifier=SideEffectClassifier(),
        trace_name="t",
    )
    epoch = "E" * 43
    lines: list[str] = []

    async def go() -> None:
        await runner.start()
        turn = runner.run_turn(
            Event(type="message", text="go", user="U0EXAMPLE1", ts="1"),
            turn_epoch=epoch,
        )
        lines.append(await anext(turn))
        await runner.interrupt("operator stop")
        assert await runner.timeout(epoch) is True
        async for line in turn:
            lines.append(line)
        await runner.close()

    anyio.run(go)
    events = parse_ndjson("".join(lines))
    assert events[-1].status is SessionStatus.CLASSIFIED_FAILURE
    root = _span_named(list(exporter.get_finished_spans()), "agent.run")[0]
    assert root.attributes["curie.terminal.cause"] == "runner_timeout"
    assert root.attributes["curie.terminal.status"] == "failed"
    assert root.status.status_code is StatusCode.ERROR


@pytest.mark.parametrize(
    ("session_type", "expected_phase", "expect_tool"),
    (
        (_ProviderStallSession, "provider_wait", False),
        (_ToolStallSession, "tool_wait", True),
    ),
)
def test_abandoning_a_stalled_phase_is_error_not_intentional_cancellation(
    session_type,
    expected_phase: str,
    expect_tool: bool,
    caplog,
) -> None:
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))

    async def go() -> None:
        entered = anyio.Event()
        release = anyio.Event()
        session = session_type(entered, release)
        runner = SessionRunner(
            session_factory=lambda: session,
            ceiling=0,
            tracer=RunTracer(provider),
            classifier=SideEffectClassifier(),
            trace_name="t",
        )
        scope_ready = anyio.Event()
        scope_holder: list[anyio.CancelScope] = []

        async def consume() -> None:
            with anyio.CancelScope() as scope:
                scope_holder.append(scope)
                scope_ready.set()
                async for _ in runner.run_turn(
                    Event(type="message", text="go", user="U", ts="1")
                ):
                    pass

        await runner.start()
        async with anyio.create_task_group() as tasks:
            tasks.start_soon(consume)
            await scope_ready.wait()
            await entered.wait()
            scope_holder[0].cancel()
        await runner.close()

    with caplog.at_level(logging.INFO, logger="curie_runner.session"):
        anyio.run(go)
    assert not any("turn end" in record.getMessage() for record in caplog.records)
    spans = list(exporter.get_finished_spans())
    root = _span_named(spans, "agent.run")[0]
    assert root.attributes["curie.phase"] == expected_phase
    assert root.attributes["curie.terminal.cause"] == "abandoned"
    assert root.attributes["curie.terminal.status"] == "abandoned"
    assert root.status.status_code is StatusCode.ERROR
    tools = _span_named(spans, "execute_tool")
    assert bool(tools) is expect_tool
    if tools:
        assert tools[0].attributes["curie.tool.outcome"] == "cancelled"


def test_turn_lifecycle_logged(caplog) -> None:
    runner, _ = _runner()
    event = Event(type="message", text="go", user="U-log", ts="1")

    with caplog.at_level(logging.INFO, logger="curie_runner.session"):
        events = _drain(runner, event)

    messages = [record.getMessage() for record in caplog.records]
    assert events[-1].status == SessionStatus.DONE
    assert any("turn start" in message and "user=U-log" in message for message in messages)
    assert any("turn end" in message and "status=done" in message for message in messages)
    assert any("tool call" in message and "tool=Bash" in message for message in messages)


def test_bare_interrupt_yields_idle_final() -> None:
    runner, _ = _runner()
    events = _drain(runner, Interrupt(reason="stop"))
    assert [e.type for e in events] == ["final"]
    assert events[0].status == SessionStatus.IDLE_AWAITING_INPUT


def test_midturn_interrupt_reclassifies_final_to_idle() -> None:
    # Interrupt delivered while the turn is live: the fake truncates its replay
    # (as the SDK's native interrupt would), the turn ends without a model result,
    # and the fallback final is idle-awaiting-input rather than done.
    runner, fake = _runner()  # default_turn: several messages before the result

    lines: list[str] = []

    async def go() -> None:
        await runner.start()
        gen = runner.run_turn(Event(type="message", text="go", user="U", ts="1"))
        lines.append(await gen.__anext__())  # consume the first outbound event
        assert runner.turn_active
        await runner.interrupt("user stop")  # side-channel interrupt mid-turn
        async for line in gen:
            lines.append(line)

    anyio.run(go)
    events = parse_ndjson("".join(lines))
    assert events[-1].type == "final"
    assert events[-1].status == SessionStatus.IDLE_AWAITING_INPUT
    assert fake.interrupts >= 1


def test_interrupt_reclassifies_error_result_to_idle() -> None:
    # The other real interrupt shape: the SDK still delivers a terminal *error*
    # result after the interrupt. An intentional stop must read as idle, not a
    # classified failure.
    script = [
        AssistantMessage(content=[TextBlock(text="working")], model="m"),
        ResultMessage(
            subtype="error_during_execution", duration_ms=1, duration_api_ms=1,
            is_error=True, num_turns=1, session_id="s", result="aborted",
        ),
    ]
    fake = FakeModelSession(lambda: script, truncate_on_interrupt=False)
    runner = SessionRunner(
        session_factory=lambda: fake, ceiling=0, tracer=RunTracer(None),
        classifier=SideEffectClassifier(), trace_name="t",
    )

    lines: list[str] = []

    async def go() -> None:
        await runner.start()
        gen = runner.run_turn(Event(type="message", text="go", user="U", ts="1"))
        lines.append(await gen.__anext__())
        await runner.interrupt("user stop")
        async for line in gen:
            lines.append(line)

    anyio.run(go)
    events = parse_ndjson("".join(lines))
    assert events[-1].type == "final"
    assert events[-1].status == SessionStatus.IDLE_AWAITING_INPUT


def test_sdk_exception_still_terminates_in_final() -> None:
    # If the model session raises mid-turn, the ACI stream must still end in a
    # classified-failure final (never a truncated, final-less stream).
    class RaisingSession:
        async def connect(self) -> None: ...
        async def query(self, text: str) -> None:
            raise RuntimeError("cli disconnected")
        async def receive_turn(self):  # pragma: no cover - never reached
            if False:
                yield None
        async def interrupt(self) -> None: ...
        async def close(self) -> None: ...

    runner = SessionRunner(
        session_factory=RaisingSession, ceiling=0, tracer=RunTracer(None),
        classifier=SideEffectClassifier(), trace_name="t",
    )
    events = _drain(runner, Event(type="message", text="go", user="U", ts="1"))
    assert [e.type for e in events] == ["error", "final"]
    assert events[0].classification == "runner-error"
    assert events[-1].status == SessionStatus.CLASSIFIED_FAILURE
    assert runner.status == SessionStatus.CLASSIFIED_FAILURE


def test_sdk_exception_logs_turn_failure(caplog) -> None:
    class RaisingSession:
        async def connect(self) -> None: ...

        async def query(self, text: str) -> None:
            raise RuntimeError("authentication_failed")

        async def receive_turn(self):  # pragma: no cover - never reached
            if False:
                yield None

        async def interrupt(self) -> None: ...

        async def close(self) -> None: ...

    runner = SessionRunner(
        session_factory=RaisingSession,
        ceiling=0,
        tracer=RunTracer(None),
        classifier=SideEffectClassifier(),
        trace_name="t",
    )
    with caplog.at_level(logging.INFO, logger="curie_runner.session"):
        events = _drain(runner, Event(type="message", text="go", user="U", ts="1"))

    messages = [record.getMessage() for record in caplog.records]
    assert [e.type for e in events] == ["error", "final"]
    assert events[-1].status == SessionStatus.CLASSIFIED_FAILURE
    assert any(
        "turn end" in message and "status=classified-failure" in message
        for message in messages
    )
    assert any(
        record.levelno == logging.ERROR
        and "turn failed" in record.getMessage()
        and "RuntimeError" in record.getMessage()
        for record in caplog.records
    )
    assert any("authentication_failed" in message for message in messages)


def test_auth_rejection_fails_fast_not_retried(caplog) -> None:
    # A rejected model credential (provider 401/403 -> AssistantMessage.error
    # "authentication_failed") must surface a DISTINCT, immediate classified
    # failure -- not a generic error streamed while the SDK/CLI retries with
    # backoff to the ~2min wall. The script places messages AFTER the auth error
    # that must never be consumed (proving the turn aborted at the rejection and
    # did not keep driving the session).
    sentinel = "SHOULD-NOT-APPEAR-AFTER-AUTH-FAIL"
    script = [
        AssistantMessage(content=[], model="m", error="authentication_failed"),
        AssistantMessage(content=[TextBlock(text=sentinel)], model="m"),
        ResultMessage(
            subtype="success", duration_ms=1, duration_api_ms=1,
            is_error=False, num_turns=1, session_id="s", result=sentinel,
        ),
    ]
    runner, fake = _runner(lambda: script)

    with caplog.at_level(logging.ERROR, logger="curie_runner.session"):
        events = _drain(runner, Event(type="message", text="go", user="U", ts="1"))

    # Distinct, terminal, credential-rejected classification (not "runner-error",
    # not "budget-exceeded", not the raw SDK "authentication_failed").
    assert [e.type for e in events] == ["error", "final"]
    assert events[0].classification == "model-credential-rejected"
    assert "CURIE_CREDENTIALS" in events[0].message
    assert events[-1].status == SessionStatus.CLASSIFIED_FAILURE
    assert runner.status == SessionStatus.CLASSIFIED_FAILURE
    # Fast-fail: aborted at the rejection, never consuming later messages, and
    # interrupted the live session so the CLI stops retrying.
    assert all(sentinel not in getattr(e, "text", "") for e in events)
    assert fake.interrupts >= 1
    assert any(
        record.levelno == logging.ERROR and "auth failure" in record.getMessage()
        for record in caplog.records
    )


def test_auth_fast_fail_survives_a_wedged_interrupt(caplog) -> None:
    # Hardening for the fast-fail: if interrupt() itself RAISES (a wedged
    # transport -- the very state a bad credential can cause), the exception must
    # NOT propagate to the generic *retryable* runner-error handler. The turn must
    # still surface the terminal model-credential-rejected classification so the
    # auth failure is never retried back into the ~2min hang.
    class WedgedInterruptSession(FakeModelSession):
        async def interrupt(self) -> None:
            self.interrupts += 1
            raise RuntimeError("transport wedged")

    script = [AssistantMessage(content=[], model="m", error="authentication_failed")]
    fake = WedgedInterruptSession(lambda: script)
    runner = SessionRunner(
        session_factory=lambda: fake,
        ceiling=0,
        tracer=RunTracer(None),
        classifier=SideEffectClassifier(),
        trace_name="t",
    )

    with caplog.at_level(logging.ERROR, logger="curie_runner.session"):
        events = _drain(runner, Event(type="message", text="go", user="U", ts="1"))

    # Still the terminal credential-rejected classification -- NOT the retryable
    # generic "runner-error", even though interrupt() raised.
    assert [e.type for e in events] == ["error", "final"]
    assert events[0].classification == "model-credential-rejected"
    assert "runner-error" not in [getattr(e, "classification", None) for e in events]
    assert events[-1].status == SessionStatus.CLASSIFIED_FAILURE
    assert runner.status == SessionStatus.CLASSIFIED_FAILURE
    assert fake.interrupts >= 1  # the interrupt was attempted (and swallowed)


def test_transient_model_error_is_not_fast_failed() -> None:
    # A transient AssistantMessage.error (e.g. a hard rate-limit) is NOT a
    # credential rejection: it must not credential-reject; must reach DONE.
    # The SDK token is constrained to unclassified rather than passed through.
    script = [
        AssistantMessage(content=[], model="m", error="rate_limit"),
        ResultMessage(
            subtype="success", duration_ms=1, duration_api_ms=1,
            is_error=False, num_turns=1, session_id="s", result="recovered",
        ),
    ]
    runner, fake = _runner(lambda: script)
    events = _drain(runner, Event(type="message", text="go", user="U", ts="1"))

    classifications = [getattr(e, "classification", None) for e in events]
    assert "model-credential-rejected" not in classifications
    assert "unclassified" in classifications
    assert "rate_limit" not in classifications
    assert events[-1].status == SessionStatus.DONE  # reached the model's result
    assert fake.interrupts == 0  # not aborted


def test_budget_halt_logged(caplog) -> None:
    script = [
        AssistantMessage(
            content=[TextBlock(text="thinking hard")],
            model="fake",
            usage={"output_tokens": 500},
        ),
        ResultMessage(
            subtype="success",
            duration_ms=1,
            duration_api_ms=1,
            is_error=False,
            num_turns=1,
            session_id="s",
            result="done",
            usage={"output_tokens": 500},
        ),
    ]
    runner, _ = _runner(lambda: script, ceiling=10)

    with caplog.at_level(logging.INFO, logger="curie_runner.session"):
        events = _drain(runner, Event(type="message", text="go", user="U", ts="1"))

    assert events[-1].status == SessionStatus.CLASSIFIED_FAILURE
    assert any(
        "turn end" in record.getMessage()
        and "status=classified-failure" in record.getMessage()
        for record in caplog.records
    )
    assert any(
        record.levelno == logging.WARNING and "budget halt" in record.getMessage()
        for record in caplog.records
    )


def test_error_result_body_not_logged(caplog) -> None:
    # An error *result* turn builds ErrorEvent(message=result); the "model error"
    # ERROR record must log only the structural classification, never the result body
    # (which is the model output / Final.text). No prior interrupt, so the turn is
    # a plain classified failure and translate takes the ErrorEvent(message=text)
    # branch.
    sentinel = "SENTINEL-RESULT-BODY-7c2e"
    script = [
        AssistantMessage(content=[TextBlock(text="working")], model="m"),
        ResultMessage(
            subtype="error_during_execution", duration_ms=1, duration_api_ms=1,
            is_error=True, num_turns=1, session_id="s", result=sentinel,
        ),
    ]
    runner, _ = _runner(lambda: script)

    with caplog.at_level(logging.WARNING, logger="curie_runner.session"):
        events = _drain(runner, Event(type="message", text="go", user="U", ts="1"))

    assert events[-1].status == SessionStatus.CLASSIFIED_FAILURE
    assert any(
        record.levelno == logging.ERROR and "model error" in record.getMessage()
        for record in caplog.records
    )
    assert all(sentinel not in record.getMessage() for record in caplog.records)


def test_turn_logging_does_not_include_message_body(caplog) -> None:
    runner, _ = _runner()
    sentinel = "SENTINEL-SECRET-BODY-9f3a"

    with caplog.at_level(logging.INFO, logger="curie_runner.session"):
        _drain(runner, Event(type="message", text=sentinel, user="U", ts="1"))

    assert all(sentinel not in record.getMessage() for record in caplog.records)


def test_steer_rejected_once_final_is_produced() -> None:
    # Finish-race guard: the moment the terminal final is produced, the turn no
    # longer accepts steers -- even though the generator has not fully closed.
    runner, fake = _runner()

    async def go() -> None:
        await runner.start()
        gen = runner.run_turn(Event(type="message", text="go", user="U", ts="1"))
        async for line in gen:
            if parse_ndjson(line)[0].type == "final":
                assert runner.turn_active is False
                assert await runner.steer("too late") is False

    anyio.run(go)
    assert fake.queries == ["go"]  # the late steer never reached the session


def test_abandoned_stream_interrupts_the_sdk() -> None:
    # Consumer disconnect mid-turn (GeneratorExit via aclose) must stop the SDK so
    # it does not keep running tools past the released turn.
    runner, fake = _runner()

    async def go() -> None:
        await runner.start()
        gen = runner.run_turn(Event(type="message", text="go", user="U", ts="1"))
        await gen.__anext__()  # turn live, mid-run
        assert runner.turn_active
        await gen.aclose()  # consumer walks away before the terminal final
        assert runner.turn_active is False

    anyio.run(go)
    assert fake.interrupts >= 1


def test_cross_task_abandon_does_not_wedge_turn_lock() -> None:
    # Client-disconnect mid-stream (#679): the server drives run_turn and, when
    # response.write() raises on disconnect, the suspended generator is finalized
    # by the asyncgen GC on a DIFFERENT task than the one that opened it. The turn
    # lock must survive that cross-task teardown. An anyio.Lock released off-owner
    # raises "current task is not holding this lock" AND leaves itself held,
    # wedging every future turn forever; a Semaphore's owner-agnostic release does
    # not. This closes the generator from a child task, then asserts a fresh turn
    # still acquires the lock and runs to a terminal final.
    runner, fake = _runner()

    async def go() -> None:
        await runner.start()
        gen = runner.run_turn(Event(type="message", text="first", user="U", ts="1"))
        await gen.__anext__()  # turn live; the lock is held by THIS task
        assert runner.turn_active

        # Finalize the abandoned generator on a different task, mimicking the
        # asyncgen GC finalizer running off the driving frame. With the old
        # anyio.Lock this raises and wedges the lock; the task group would then
        # propagate the RuntimeError and fail the test here.
        async with anyio.create_task_group() as tg:
            tg.start_soon(gen.aclose)
        assert runner.turn_active is False

        # The discriminator: a subsequent turn must acquire the lock and finish.
        # fail_after turns a permanent wedge into a failure instead of a hang.
        lines: list[str] = []
        with anyio.fail_after(5):
            async for line in runner.run_turn(
                Event(type="message", text="second", user="U", ts="2")
            ):
                lines.append(line)

        events = parse_ndjson("".join(lines))
        assert events[-1].type == "final"
        assert events[-1].status == SessionStatus.DONE

    anyio.run(go)
    assert fake.queries[-1] == "second"  # the second turn actually ran


def test_build_options_carries_resume_ref() -> None:
    # Rehydrate-from-history (ADR-0003): a history ref becomes the SDK resume id.
    options = build_options(
        plugins=[],
        model="claude-opus-4-8",
        system_prompt=None,
        max_turns=20,
        max_budget_usd=5.0,
        resume="s3://history/thread-42",
    )
    assert options.resume == "s3://history/thread-42"
    assert options.max_budget_usd == 5.0
    assert options.permission_mode == "bypassPermissions"


def test_build_options_no_history_ref_is_none() -> None:
    options = build_options(
        plugins=[], model=None, system_prompt=None, max_turns=20,
        max_budget_usd=1.0, resume=None,
    )
    assert options.resume is None
    assert options.task_budget is None


def test_build_options_requests_payload_stripped_partial_boundaries() -> None:
    options = build_options(
        plugins=[],
        model=None,
        system_prompt=None,
        max_turns=20,
        max_budget_usd=1.0,
        resume=None,
    )
    assert options.include_partial_messages is True


def test_build_options_carries_task_budget_hint() -> None:
    # The ACI task_budget_hint (soft pacing) becomes the SDK task_budget.
    options = build_options(
        plugins=[], model=None, system_prompt=None, max_turns=20,
        max_budget_usd=1.0, resume=None, task_budget_hint=64000,
    )
    assert options.task_budget == {"total": 64000}


def test_steer_reaches_live_session() -> None:
    # A steer injects into the live turn: its text lands on the session as a
    # second query while the turn's stream is still open.
    runner, fake = _runner()

    async def go() -> None:
        await runner.start()
        gen = runner.run_turn(Event(type="message", text="first", user="U", ts="1"))
        await gen.__anext__()  # turn is now live
        assert await runner.steer("steered follow-up") is True
        async for _ in gen:
            pass

    anyio.run(go)
    assert fake.queries == ["first", "steered follow-up"]


def _connector_failure() -> ConnectorCapabilityFailure:
    return ConnectorCapabilityFailure(
        connector="github",
        credential_names=("GITHUB_TOKEN",),
        reason="empty_expansion",
    )


def _connector_runner(
    tracer: RunTracer | None = None, **kwargs
) -> tuple[SessionRunner, FakeModelSession]:
    fake = FakeModelSession(default_turn)
    runner = SessionRunner(
        session_factory=lambda: fake,
        ceiling=0,
        tracer=tracer or RunTracer(None),
        classifier=SideEffectClassifier(),
        trace_name="t",
        **kwargs,
    )
    return runner, fake


def _two_turns(runner: SessionRunner) -> tuple[list, list]:
    async def go() -> tuple[list, list]:
        await runner.start()
        turns = []
        for text, ts in (("go", "1"), ("again", "2")):
            turns.append(
                parse_ndjson(
                    "".join(
                        [
                            line
                            async for line in runner.run_inbound(
                                Event(type="message", text=text, user="U", ts=ts)
                            )
                        ]
                    )
                )
            )
        return turns[0], turns[1]

    return anyio.run(go)


def _exclusion_callback(availability: ConnectorAvailability):
    hooks = build_gated_pre_tool_use_hooks(None, availability)
    assert hooks is not None
    return hooks["PreToolUse"][0].hooks[0]


def _hook_decision(availability: ConnectorAvailability, tool_name: str) -> dict:
    callback = _exclusion_callback(availability)
    return anyio.run(callback, {"tool_name": tool_name}, None, None)


def test_declared_connector_failure_runs_the_model_with_a_notice(
    caplog: pytest.LogCaptureFixture,
) -> None:
    # #2634: a persistent expansion failure no longer halts the turn. The model
    # is queried on every turn and the DONE final leads with the notice. No
    # ErrorEvent: the worker would keep its classification for the whole turn.
    planted = "ghp-must-not-appear-PLACEHOLDER"
    notice = _connector_failure().caller_message()
    runner, fake = _connector_runner(connector_failures=(_connector_failure(),))

    with caplog.at_level(logging.ERROR, logger="curie_runner.session"):
        first, second = _two_turns(runner)

    assert fake.queries == ["go", "again"]
    for events in (first, second):
        assert not any(isinstance(e, ErrorEvent) for e in events)
        assert "text_delta" in [e.type for e in events]
        assert events[-1].type == "final"
        assert events[-1].status == SessionStatus.DONE
        assert events[-1].text.startswith(notice)
        assert events[-1].text.endswith("all done")
        assert planted not in events[-1].text
    assert any(
        record.levelno == logging.ERROR
        and "github" in record.getMessage()
        and "GITHUB_TOKEN" in record.getMessage()
        and planted not in record.getMessage()
        for record in caplog.records
    )


def test_declared_connector_failure_does_not_abandon_the_otel_span() -> None:
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    runner, fake = _connector_runner(
        tracer=RunTracer(provider), connector_failures=(_connector_failure(),)
    )

    async def go() -> None:
        await runner.start()
        async for _line in runner.run_inbound(Event(type="message", text="go", user="U", ts="1")):
            pass
        await runner.close()

    anyio.run(go)
    root = _span_named(list(exporter.get_finished_spans()), "agent.run")[0]
    assert root.attributes["curie.terminal.cause"] == "completed"
    assert root.attributes["curie.terminal.status"] == "succeeded"
    assert fake.queries == ["go"]


def test_reprobe_that_raises_keeps_prior_failures_and_still_queries() -> None:
    async def broken(_failures):
        raise RuntimeError("reprobe exploded")

    availability = ConnectorAvailability()
    runner, fake = _connector_runner(
        connector_failures=(_connector_failure(),),
        connector_reprobe=broken,
        connector_availability=availability,
    )
    events = _drain(runner, Event(type="message", text="go", user="U", ts="1"))

    assert fake.queries == ["go"]
    assert availability.failures == (_connector_failure(),)
    assert not any(isinstance(e, ErrorEvent) for e in events)
    assert events[-1].status == SessionStatus.DONE
    assert events[-1].text.startswith(_connector_failure().caller_message())
    assert _hook_decision(availability, "mcp__github__search")["hookSpecificOutput"][
        "permissionDecision"
    ] == "deny"


class _ReconnectingFakeSession(FakeModelSession):
    """A fake session that also answers the MCP reconnector protocol."""

    def __init__(self, answers: list[bool]) -> None:
        super().__init__(default_turn)
        self.answers = answers
        self.ensured: list[str] = []

    async def ensure_mcp_server(self, name: str) -> bool:
        self.ensured.append(name)
        return self.answers.pop(0)


def _probe_failed() -> ConnectorCapabilityFailure:
    return ConnectorCapabilityFailure(
        connector="github", credential_names=("GITHUB_TOKEN",), reason="probe_failed"
    )


def _reconnect_runner(
    answers: list[bool],
) -> tuple[SessionRunner, _ReconnectingFakeSession, ConnectorAvailability, list[int]]:
    session = _ReconnectingFakeSession(answers)
    availability = ConnectorAvailability((_probe_failed(),))
    reprobes: list[int] = []

    async def reprobe(failures):
        # Turn 1 the side probe still fails; turn 2 it succeeds.
        reprobes.append(len(failures))
        return failures if len(reprobes) == 1 else ()

    runner = SessionRunner(
        session_factory=lambda: session,
        ceiling=0,
        tracer=RunTracer(None),
        classifier=SideEffectClassifier(),
        trace_name="t",
        connector_failures=(_probe_failed(),),
        connector_reprobe=reprobe,
        connector_availability=availability,
    )
    return runner, session, availability, reprobes


def test_side_probe_recovery_is_kept_when_the_session_is_not_connected() -> None:
    runner, session, availability, _ = _reconnect_runner([False])
    notice = _probe_failed().caller_message()

    first, second = _two_turns(runner)

    assert session.queries == ["go", "again"]
    assert session.ensured == ["github"]
    for events in (first, second):
        assert events[-1].status == SessionStatus.DONE
        assert events[-1].text.startswith(notice)
    assert availability.failures == (_probe_failed(),)
    assert _hook_decision(availability, "mcp__github__search")["hookSpecificOutput"][
        "permissionDecision"
    ] == "deny"


def test_side_probe_recovery_clears_when_the_session_is_connected() -> None:
    runner, session, availability, _ = _reconnect_runner([True])

    first, second = _two_turns(runner)

    assert session.ensured == ["github"]
    assert first[-1].text.startswith(_probe_failed().caller_message())
    assert second[-1].text == "all done"
    assert availability.failures == ()
    assert _hook_decision(availability, "mcp__github__search") == {}


class _StubMcpClient:
    def __init__(self, statuses: list[str], *, raises: bool = False) -> None:
        self.statuses = statuses
        self.raises = raises
        self.reconnected: list[str] = []

    async def get_mcp_status(self) -> dict:
        if self.raises:
            raise RuntimeError("status failed with Bearer ghp-must-not-log-PLACEHOLDER")
        return {
            "mcpServers": [
                {"name": "linear", "status": "connected"},
                {"name": "github", "status": self.statuses.pop(0)},
            ]
        }

    async def reconnect_mcp_server(self, name: str) -> None:
        self.reconnected.append(name)


@pytest.mark.parametrize(
    ("statuses", "raises", "expected", "reconnected"),
    (
        (["connected"], False, True, []),
        (["failed", "connected"], False, True, ["github"]),
        (["failed", "failed"], False, False, ["github"]),
        ([], True, False, []),
    ),
)
def test_claude_agent_session_ensure_mcp_server(
    caplog: pytest.LogCaptureFixture,
    statuses: list[str],
    raises: bool,
    expected: bool,
    reconnected: list[str],
) -> None:
    client = _StubMcpClient(statuses, raises=raises)
    session = ClaudeAgentSession.__new__(ClaudeAgentSession)
    session._client = client  # type: ignore[assignment]

    assert isinstance(session, McpServerReconnector)
    with caplog.at_level(logging.WARNING, logger="curie_runner.adapter"):
        assert anyio.run(session.ensure_mcp_server, "github") is expected
    assert client.reconnected == reconnected
    for record in caplog.records:
        assert "ghp-must-not-log-PLACEHOLDER" not in record.getMessage()
    if raises:
        assert any("error_class=RuntimeError" in r.getMessage() for r in caplog.records)


def test_fake_session_is_not_an_mcp_reconnector() -> None:
    assert not isinstance(FakeModelSession(default_turn), McpServerReconnector)


def _blocking_reprobe_runner(
    release: anyio.Event, entered: anyio.Event
) -> tuple[SessionRunner, FakeModelSession]:
    async def reprobe(failures):
        entered.set()
        await release.wait()
        return failures

    return _connector_runner(
        connector_failures=(_probe_failed(),), connector_reprobe=reprobe
    )


async def _during_recovery(
    runner: SessionRunner,
    release: anyio.Event,
    entered: anyio.Event,
    action,
    *,
    turn_epoch: str | None = None,
) -> list:
    """Drive one turn, run ``action`` while recovery is blocked, then release."""

    lines: list[str] = []
    await runner.start()

    async def drive() -> None:
        async for line in runner.run_turn(
            Event(type="message", text="go", user="U", ts="1"), turn_epoch=turn_epoch
        ):
            lines.append(line)

    async with anyio.create_task_group() as tg:
        tg.start_soon(drive)
        await entered.wait()
        await action()
        release.set()
    return parse_ndjson("".join(lines))


def test_interrupt_during_connector_recovery_never_queries_the_model() -> None:
    release, entered = anyio.Event(), anyio.Event()
    runner, fake = _blocking_reprobe_runner(release, entered)

    async def stop() -> None:
        assert runner.turn_active is True
        await runner.interrupt()

    events = anyio.run(_during_recovery, runner, release, entered, stop)

    assert fake.queries == []
    assert fake.interrupts == 0
    assert [e.type for e in events] == ["final"]
    assert events[-1].status == SessionStatus.IDLE_AWAITING_INPUT
    assert events[-1].text == "run interrupted"
    assert runner.status == SessionStatus.IDLE_AWAITING_INPUT
    assert runner.turn_active is False


def test_timeout_during_connector_recovery_is_accepted_and_never_queries() -> None:
    release, entered = anyio.Event(), anyio.Event()
    runner, fake = _blocking_reprobe_runner(release, entered)
    accepted: list[bool] = []

    async def stop() -> None:
        accepted.append(await runner.timeout("epoch-1"))

    events = anyio.run(
        functools.partial(
            _during_recovery, runner, release, entered, stop, turn_epoch="epoch-1"
        )
    )

    assert accepted == [True]
    assert fake.queries == []
    assert fake.interrupts == 0
    assert [e.type for e in events] == ["final"]
    assert events[-1].status == SessionStatus.CLASSIFIED_FAILURE
    assert events[-1].text == "run timed out"
    assert runner.status == SessionStatus.CLASSIFIED_FAILURE


def test_status_reports_turn_active_during_connector_recovery() -> None:
    release, entered = anyio.Event(), anyio.Event()
    runner, fake = _blocking_reprobe_runner(release, entered)
    observed: list[bool] = []

    async def probe_status() -> None:
        async with TestClient(TestServer(create_app(runner))) as client:
            resp = await client.get("/status")
            observed.append((await resp.json())["turn_active"])

    events = anyio.run(_during_recovery, runner, release, entered, probe_status)

    assert observed == [True]
    assert fake.queries == ["go"]
    assert events[-1].status == SessionStatus.DONE


def test_steer_during_connector_recovery_is_refused_and_original_goes_first() -> None:
    release, entered = anyio.Event(), anyio.Event()
    runner, fake = _blocking_reprobe_runner(release, entered)
    steered: list[bool] = []

    async def steer() -> None:
        steered.append(await runner.steer("followup"))
        assert fake.queries == []

    events = anyio.run(_during_recovery, runner, release, entered, steer)

    assert steered == [False]
    assert fake.queries == ["go"]
    assert events[-1].status == SessionStatus.DONE
    assert events[-1].text.startswith(_probe_failed().caller_message())


def test_slow_session_confirmation_is_not_cancelled_by_the_budget(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(session_module, "_CONNECTOR_RECOVERY_BUDGET_SECONDS", 0.05)
    completed: list[str] = []

    class _SlowConfirmSession(FakeModelSession):
        async def ensure_mcp_server(self, name: str) -> bool:
            await anyio.sleep(0.3)
            completed.append(name)
            return True

    session = _SlowConfirmSession(default_turn)
    availability = ConnectorAvailability()

    async def recovered(_failures):
        return ()

    runner = SessionRunner(
        session_factory=lambda: session,
        ceiling=0,
        tracer=RunTracer(None),
        classifier=SideEffectClassifier(),
        trace_name="t",
        connector_failures=(_probe_failed(),),
        connector_reprobe=recovered,
        connector_availability=availability,
    )
    events = _drain(runner, Event(type="message", text="go", user="U", ts="1"))

    assert completed == ["github"]
    assert availability.failures == ()
    assert session.queries == ["go"]
    assert events[-1].text == "all done"


def test_connector_recovery_over_budget_keeps_failure_and_runs_the_turn(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(session_module, "_CONNECTOR_RECOVERY_BUDGET_SECONDS", 0.05)
    availability = ConnectorAvailability()

    async def slow(_failures):
        await anyio.sleep(5)
        return ()

    runner, fake = _connector_runner(
        connector_failures=(_probe_failed(),),
        connector_reprobe=slow,
        connector_availability=availability,
    )
    events = _drain(runner, Event(type="message", text="go", user="U", ts="1"))

    assert fake.queries == ["go"]
    assert availability.failures == (_probe_failed(),)
    assert events[-1].status == SessionStatus.DONE
    assert events[-1].text.startswith(_probe_failed().caller_message())


def test_failed_model_turn_with_failed_connector_is_not_prefixed() -> None:
    # The notice leads only a DONE final. A classified failure keeps its own
    # text, and nothing on the stream carries a connector classification that
    # could mask the turn's real (possibly retryable) failure in the worker.
    def failing_turn() -> list:
        return [
            AssistantMessage(content=[TextBlock(text="trying")], model="fake-model"),
            ResultMessage(
                subtype="error_during_execution",
                duration_ms=1,
                duration_api_ms=1,
                is_error=True,
                num_turns=1,
                session_id="fake-session",
                result="model blew up",
            ),
        ]

    fake = FakeModelSession(failing_turn)
    runner = SessionRunner(
        session_factory=lambda: fake,
        ceiling=0,
        tracer=RunTracer(None),
        classifier=SideEffectClassifier(),
        trace_name="t",
        connector_failures=(_connector_failure(),),
    )
    events = _drain(runner, Event(type="message", text="go", user="U", ts="1"))

    assert fake.queries == ["go"]
    final = events[-1]
    assert final.status == SessionStatus.CLASSIFIED_FAILURE
    assert not final.text.startswith(_connector_failure().caller_message())
    assert _connector_failure().caller_message() not in final.text
    assert not any(
        isinstance(e, ErrorEvent) and "connector" in (e.classification or "")
        for e in events
    )


def test_healthy_connector_still_queries_the_model() -> None:
    runner, fake = _runner()
    events = _drain(runner, Event(type="message", text="go", user="U", ts="1"))
    assert fake.queries == ["go"]
    assert events[-1].status == SessionStatus.DONE
