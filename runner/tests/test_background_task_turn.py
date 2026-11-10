"""A live SDK background task keeps the factory turn open past the first result.

The fake uses one shared queue, the same shape as ``_SharedQueueSession``:
each ``receive_turn`` yields through the next ``ResultMessage`` and then
returns, and ``interrupt`` enqueues the interrupted turn's result.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Any

import anyio
from aci_protocol import Event, Final, SessionStatus, parse_ndjson_line
from claude_agent_sdk import (
    AssistantMessage,
    ResultMessage,
    SystemMessage,
    TaskNotificationMessage,
    TaskUpdatedMessage,
    TextBlock,
)
from curie_runner import RunTracer, SideEffectClassifier
from curie_runner.session import SessionRunner
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.trace import StatusCode


def _reply(text: str) -> AssistantMessage:
    return AssistantMessage(content=[TextBlock(text=text)], model="stub-model")


def _result(
    text: str,
    *,
    is_error: bool = False,
    subtype: str = "success",
    usage: dict[str, Any] | None = None,
) -> ResultMessage:
    return ResultMessage(
        subtype=subtype,
        duration_ms=1,
        duration_api_ms=1,
        is_error=is_error,
        num_turns=1,
        session_id="sdk-session",
        result=text,
        usage=usage,
    )


def _backgrounded(task_id: str) -> TaskUpdatedMessage:
    return TaskUpdatedMessage(
        subtype="task_updated",
        data={},
        task_id=task_id,
        patch={"is_backgrounded": True},
    )


def _notification(task_id: str) -> TaskNotificationMessage:
    return TaskNotificationMessage(
        subtype="task_notification",
        data={},
        task_id=task_id,
        status="completed",
        output_file="output",
        summary="completed",
        uuid="task-note",
        session_id="sdk-session",
    )


def _tasks_changed(tasks: list[dict[str, Any]]) -> SystemMessage:
    return SystemMessage(subtype="background_tasks_changed", data={"tasks": tasks})


class _ScriptedBackgroundSession:
    """One queue across ``receive_turn`` calls, scripted before ``query``."""

    def __init__(self, messages: list[Any]) -> None:
        self.queue: list[Any] = list(messages)
        self.arrived = anyio.Event()
        if self.queue:
            self.arrived.set()
        self.queries: list[str] = []
        self.interrupts = 0
        self.waiting = anyio.Event()

    async def connect(self) -> None:
        return None

    async def close(self) -> None:
        return None

    def _put(self, message: Any) -> None:
        self.queue.append(message)
        self.arrived.set()

    async def query(self, text: str) -> None:
        self.queries.append(text)

    async def interrupt(self) -> None:
        self.interrupts += 1
        self._put(_result("", is_error=True, subtype="error_during_execution"))

    def receive_turn(self) -> AsyncIterator[Any]:
        async def _gen() -> AsyncIterator[Any]:
            while True:
                while not self.queue:
                    self.waiting.set()
                    self.arrived = anyio.Event()
                    await self.arrived.wait()
                message = self.queue.pop(0)
                yield message
                if isinstance(message, ResultMessage):
                    return

        return _gen()


def _runner(
    session: _ScriptedBackgroundSession,
    tracer: RunTracer | None = None,
    *,
    ceiling: int = 0,
) -> SessionRunner:
    return SessionRunner(
        max_usd_per_day=None,
        held_secrets=frozenset(),
        session_factory=lambda: session,
        ceiling=ceiling,
        tracer=tracer or RunTracer(None),
        classifier=SideEffectClassifier(),
        trace_name="t",
    )


async def _finals(runner: SessionRunner, text: str, *, epoch: str | None = None) -> list[Final]:
    finals: list[Final] = []
    async for line in runner.run_turn(
        Event(type="message", text=text, user="U1", ts="1"),
        turn_epoch=epoch,
    ):
        parsed = parse_ndjson_line(line)
        if isinstance(parsed, Final):
            finals.append(parsed)
    return finals


def test_result_while_a_background_task_is_live_waits_for_the_next_result() -> None:
    session = _ScriptedBackgroundSession(
        [
            _reply("reply A"),
            _backgrounded("b1"),
            _result("reply A"),
            _notification("b1"),
            _tasks_changed([]),
            _reply("reply B"),
            _result("reply B"),
        ]
    )
    runner = _runner(session)

    async def go() -> list[Final]:
        await runner.start()
        try:
            with anyio.fail_after(5):
                return await _finals(runner, "go")
        finally:
            await runner.close()

    finals = anyio.run(go)
    assert len(finals) == 1
    assert finals[0].text == "reply B"
    assert finals[0].status is SessionStatus.DONE
    assert len(session.queries) == 1


def test_turn_without_background_tasks_is_unchanged() -> None:
    session = _ScriptedBackgroundSession([_reply("reply"), _result("reply")])
    runner = _runner(session)

    async def go() -> list[Final]:
        await runner.start()
        try:
            with anyio.fail_after(5):
                return await _finals(runner, "go")
        finally:
            await runner.close()

    finals = anyio.run(go)
    assert len(finals) == 1
    assert finals[0].text == "reply"
    assert finals[0].status is SessionStatus.DONE
    assert len(session.queries) == 1


def test_deadline_with_a_live_background_task_ends_as_timeout() -> None:
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    session = _ScriptedBackgroundSession([_backgrounded("b1"), _result("reply A")])
    runner = _runner(session, RunTracer(provider))
    epoch = "A" * 43

    async def go() -> list[Final]:
        await runner.start()
        try:
            finals: list[Final] = []

            async def drive() -> None:
                with anyio.fail_after(5):
                    finals.extend(await _finals(runner, "go", epoch=epoch))

            async with anyio.create_task_group() as tg:
                tg.start_soon(drive)
                with anyio.fail_after(5):
                    await session.waiting.wait()
                assert await runner.timeout(epoch) is True
            return finals
        finally:
            await runner.close()

    finals = anyio.run(go)
    assert len(finals) == 1
    assert finals[0].status is SessionStatus.CLASSIFIED_FAILURE
    root = next(span for span in exporter.get_finished_spans() if span.name == "agent.run")
    assert root.attributes is not None
    assert root.attributes["curie.terminal.cause"] == "runner_timeout"
    assert root.attributes["curie.terminal.status"] == "failed"
    assert root.status.status_code is StatusCode.ERROR


def test_output_ceiling_with_a_live_background_task_interrupts() -> None:
    session = _ScriptedBackgroundSession(
        [
            _backgrounded("b1"),
            _result("reply A", usage={"output_tokens": 50}),
        ]
    )
    runner = _runner(session, ceiling=1)

    async def go() -> list[Final]:
        await runner.start()
        try:
            with anyio.fail_after(5):
                return await _finals(runner, "go")
        finally:
            await runner.close()

    finals = anyio.run(go)
    assert len(finals) == 1
    assert finals[0].status is SessionStatus.CLASSIFIED_FAILURE
    assert session.interrupts == 1


def test_usd_budget_result_ends_turn_while_a_background_task_is_live() -> None:
    session = _ScriptedBackgroundSession(
        [
            _backgrounded("b1"),
            _result("over", is_error=True, subtype="error_max_budget_usd"),
        ]
    )
    runner = _runner(session)

    async def go() -> list[Final]:
        await runner.start()
        try:
            with anyio.fail_after(5):
                return await _finals(runner, "go")
        finally:
            await runner.close()

    finals = anyio.run(go)
    assert len(finals) == 1
    assert finals[0].status is SessionStatus.CLASSIFIED_FAILURE
    assert len(session.queries) == 1
