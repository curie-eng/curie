"""False-completion check (#517): observe-only, opt-in warning that a turn
declared done with a substantive answer but no tool-call evidence.

Drives the fake session with hand-built scripts so the tool-call count is exact,
and asserts the warning fires only when opted in, keys on tool_call_count (not
side_effect_emitted), and never changes the terminal final's status.
"""

from __future__ import annotations

import json

import anyio
from aci_protocol import Event
from claude_agent_sdk import AssistantMessage, ResultMessage, TextBlock, ToolUseBlock
from curie_runner.fake import FakeModelSession
from curie_runner.otel import RunTracer
from curie_runner.session import FALSE_COMPLETION_CLASSIFICATION, SessionRunner
from curie_runner.side_effects import SideEffectClassifier


def _assistant(*blocks: object) -> AssistantMessage:
    return AssistantMessage(content=list(blocks), model="fake-model", usage=None)


def _result(text: str) -> ResultMessage:
    return ResultMessage(
        subtype="success",
        duration_ms=1,
        duration_api_ms=1,
        is_error=False,
        num_turns=1,
        session_id="fake-session",
        result=text,
        usage={"input_tokens": 5, "output_tokens": 3},
    )


def _runner(script: list[object], *, check: bool) -> SessionRunner:
    session = FakeModelSession(lambda: script)
    return SessionRunner(
        held_secrets=frozenset(),
        session_factory=lambda: session,
        ceiling=10_000,
        tracer=RunTracer(None),
        classifier=SideEffectClassifier(),
        trace_name="test",
        session_id="s-1",
        false_completion_check=check,
    )


def _drain(runner: SessionRunner) -> list[dict[str, object]]:
    async def go() -> list[dict[str, object]]:
        await runner.start()
        event = Event(type="message", text="q", user="U", ts="0")
        lines = [line async for line in runner.run_turn(event)]
        return [json.loads(line) for line in lines]

    return anyio.run(go)


def _classifications(frames: list[dict[str, object]]) -> list[object]:
    return [f.get("classification") for f in frames if f.get("type") == "error"]


_ANSWER = "All set, nothing else to do."
_TEXT_ONLY = [_assistant(TextBlock(text=_ANSWER)), _result(_ANSWER)]


def test_fires_on_done_with_answer_but_no_tool_call() -> None:
    frames = _drain(_runner(_TEXT_ONLY, check=True))
    final = frames[-1]
    assert final["type"] == "final"
    # Observe-only: the final stays DONE, and a single warning frame precedes it.
    assert final["status"] == "done"
    assert FALSE_COMPLETION_CLASSIFICATION in _classifications(frames)


def test_off_by_default_no_warning() -> None:
    frames = _drain(_runner(_TEXT_ONLY, check=False))
    assert FALSE_COMPLETION_CLASSIFICATION not in _classifications(frames)
    assert frames[-1]["status"] == "done"


def test_no_false_alarm_when_a_read_only_tool_ran() -> None:
    # A read-only Read is tool-call evidence even though it sets no side-effect
    # flag; the check keys on tool_call_count, so it must NOT warn here.
    script = [
        _assistant(ToolUseBlock(id="t1", name="Read", input={"path": "/x"})),
        _assistant(TextBlock(text="Here is the answer from the file.")),
        _result("Here is the answer from the file."),
    ]
    frames = _drain(_runner(script, check=True))
    assert FALSE_COMPLETION_CLASSIFICATION not in _classifications(frames)


def test_no_warning_on_empty_answer() -> None:
    # A DONE turn with no substantive text is not a false completion.
    script = [_assistant(TextBlock(text="")), _result("")]
    frames = _drain(_runner(script, check=True))
    assert FALSE_COMPLETION_CLASSIFICATION not in _classifications(frames)
