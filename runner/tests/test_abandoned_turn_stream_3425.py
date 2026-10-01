"""#3425: a turn abandoned mid-run must not leak its output into the next turn.

The real ``ClaudeSDKClient`` delivers every turn from ONE shared message queue,
and ``receive_response`` reads it up to the next ``ResultMessage``. When the
worker that owned a turn dies mid-tool, the runner's stream is closed, the
runner interrupts the SDK, and the interrupted turn's terminal ``ResultMessage``
is still queued. Without a resynchronization step the next turn reads THAT
result as its own (a failed turn), and every later turn reads its predecessor's
answer, permanently one prompt late.

``_SharedQueueSession`` models that queue faithfully instead of the per-turn
script ``FakeModelSession`` replays, which cannot express cross-turn leakage.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Any

import anyio
import pytest
from aci_protocol import Event, Final, SessionStatus, parse_ndjson_line
from claude_agent_sdk import (
    AssistantMessage,
    ResultMessage,
    TextBlock,
    ToolUseBlock,
)
from curie_runner import RunTracer, SideEffectClassifier
from curie_runner import session as session_module
from curie_runner.session import SessionRunner


def _result(text: str, *, is_error: bool = False, subtype: str = "success") -> ResultMessage:
    return ResultMessage(
        subtype=subtype,
        duration_ms=1,
        duration_api_ms=1,
        is_error=is_error,
        num_turns=1,
        session_id="sdk-session",
        result=text,
    )


def _reply(text: str) -> AssistantMessage:
    return AssistantMessage(content=[TextBlock(text=text)], model="stub-model")


class _SharedQueueSession:
    """One message queue across turns, like the SDK's streaming-input client.

    ``query`` answers a prompt by enqueueing its reply and result, unless the
    prompt is ``slow``: then only the tool call is enqueued and the turn stays
    mid-tool until ``interrupt`` enqueues the CLI's interrupted-turn result.
    ``interrupt_emits_result=False`` models a wedged CLI that never answers the
    stop, so the runner must recycle the session instead of waiting forever.
    """

    def __init__(self, *, interrupt_emits_result: bool = True) -> None:
        self.queue: list[Any] = []
        self.arrived = anyio.Event()
        self.queries: list[str] = []
        self.interrupts = 0
        self.closed = False
        self._mid_tool = False
        self._interrupt_emits_result = interrupt_emits_result

    async def connect(self) -> None:
        return None

    async def close(self) -> None:
        self.closed = True

    def _put(self, message: Any) -> None:
        self.queue.append(message)
        self.arrived.set()

    async def query(self, text: str) -> None:
        self.queries.append(text)
        if text == "slow":
            self._mid_tool = True
            self._put(
                AssistantMessage(
                    content=[
                        ToolUseBlock(id="call_slow", name="Bash", input={"command": "sleep 90"})
                    ],
                    model="stub-model",
                )
            )
            return
        self._put(_reply(f"answer to {text}"))
        self._put(_result(f"answer to {text}"))

    async def interrupt(self) -> None:
        self.interrupts += 1
        if self._mid_tool and self._interrupt_emits_result:
            self._mid_tool = False
            self._put(_result("", is_error=True, subtype="error_during_execution"))

    def receive_turn(self) -> AsyncIterator[Any]:
        async def _gen() -> AsyncIterator[Any]:
            while True:
                while not self.queue:
                    self.arrived = anyio.Event()
                    await self.arrived.wait()
                message = self.queue.pop(0)
                yield message
                if isinstance(message, ResultMessage):
                    return

        return _gen()


def _runner(session: _SharedQueueSession, factory_calls: list[int] | None = None) -> SessionRunner:
    def factory() -> _SharedQueueSession:
        if factory_calls is not None:
            factory_calls.append(1)
        return session

    return SessionRunner(
        held_secrets=frozenset(),
        session_factory=factory,
        ceiling=0,
        tracer=RunTracer(None),
        classifier=SideEffectClassifier(),
        trace_name="t",
    )


async def _final(runner: SessionRunner, text: str) -> Final:
    finals: list[Final] = []
    with anyio.fail_after(5):
        async for line in runner.run_turn(Event(type="message", text=text, user="U1", ts="1")):
            parsed = parse_ndjson_line(line)
            if isinstance(parsed, Final):
                finals.append(parsed)
    assert len(finals) == 1
    return finals[0]


async def _abandon_mid_tool(runner: SessionRunner) -> None:
    """Consume the slow turn up to its tool call, then drop the stream.

    Closing the generator is what the server does when the worker holding the
    NDJSON response disappears (SIGKILL, OOM, node drain).
    """

    stream = runner.run_turn(Event(type="message", text="slow", user="U1", ts="1"))
    with anyio.fail_after(5):
        async for line in stream:
            if '"tool"' in line or "Bash" in line:
                break
    await stream.aclose()


@pytest.mark.anyio
async def test_turn_after_abandoned_turn_answers_its_own_prompt() -> None:
    session = _SharedQueueSession()
    runner = _runner(session)
    await runner.start()

    await _abandon_mid_tool(runner)
    assert session.interrupts == 1

    second = await _final(runner, "second")
    assert second.status is SessionStatus.DONE
    assert second.text == "answer to second"

    # The shift must not persist: every later turn answers its own prompt.
    third = await _final(runner, "third")
    assert third.status is SessionStatus.DONE
    assert third.text == "answer to third"
    assert session.queue == []


@pytest.mark.anyio
async def test_unfinished_abandoned_turn_fails_next_turn_without_querying(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A still-running old turn fails the new one; it never answers late.

    The session is kept (no recycle), so the conversation accumulated in this
    process survives, and the turn after the old one finally ends is correct.
    """

    monkeypatch.setattr(session_module, "_ABANDONED_TURN_DRAIN_TIMEOUT_S", 0.05)
    session = _SharedQueueSession(interrupt_emits_result=False)
    calls: list[int] = []
    runner = _runner(session, calls)
    await runner.start()

    await _abandon_mid_tool(runner)

    second = await _final(runner, "second")
    assert second.status is SessionStatus.CLASSIFIED_FAILURE
    assert session.queries == ["slow"]
    assert not session.closed
    assert calls == [1]

    # The old turn's tool finally returns and the CLI ends that turn.
    session._put(_reply("DONE"))
    session._put(_result("DONE"))

    third = await _final(runner, "third")
    assert third.status is SessionStatus.DONE
    assert third.text == "answer to third"
    assert session.queries == ["slow", "third"]


@pytest.mark.anyio
async def test_steer_is_refused_while_the_old_turn_drains(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Nothing may be queued ahead of the new turn's own prompt during a drain."""

    session = _SharedQueueSession(interrupt_emits_result=False)
    runner = _runner(session)
    await runner.start()
    await _abandon_mid_tool(runner)

    steer_results: list[bool] = []

    async def steer_then_finish_old_turn() -> None:
        # The drain is waiting on the old turn's result: the new turn is open
        # but must not accept a steer yet.
        with anyio.fail_after(5):
            while not runner._turn_open:
                await anyio.sleep(0.001)
        steer_results.append(await runner.steer("steered"))
        session._put(_result("DONE"))

    async with anyio.create_task_group() as tg:
        tg.start_soon(steer_then_finish_old_turn)
        second = await _final(runner, "second")

    assert steer_results == [False]
    assert "steered" not in session.queries
    assert second.status is SessionStatus.DONE
    assert second.text == "answer to second"


@pytest.mark.anyio
async def test_completed_turns_do_not_drain_or_recycle() -> None:
    """The resync is only for an unconsumed result; normal turns are untouched."""

    session = _SharedQueueSession()
    calls: list[int] = []
    runner = _runner(session, calls)
    await runner.start()

    assert (await _final(runner, "one")).text == "answer to one"
    assert (await _final(runner, "two")).text == "answer to two"
    assert calls == [1]
    assert session.interrupts == 0
