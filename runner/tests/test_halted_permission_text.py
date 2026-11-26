"""The text of a permission pause the runner halted at its blocked call (ADR 0296).

Only that pause takes the blocked call's own model response, and only that
pause drops the aborted CLI result's failure text. Every other final keeps its
text: real failures, late stops, a turn that continued after the halt,
publication and policy pauses, final-answer bundles, a record whose call was
never observed, and turns the runner did not halt.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Any

import anyio
import pytest
from aci_protocol import Event, Final, SessionStatus, parse_ndjson
from claude_agent_sdk import AssistantMessage, ResultMessage, TextBlock, ToolUseBlock
from curie_runner.approval import ApprovalGate
from curie_runner.fake import FakeModelSession
from curie_runner.final_answer import FinalAnswerPolicy, FinalAnswerValidation
from curie_runner.harness.claude.approval import build_can_use_tool
from curie_runner.otel import RunTracer
from curie_runner.session import SessionRunner, _apply_approval_override, _halted_pause_text
from curie_runner.side_effects import SideEffectClassifier
from curie_runner.translate import TurnState
from plugin_format import PLATFORM_PUBLISH_TOOL_NAME

_GATED = "mcp__acme-access__grant_access"
_ARGS = {"resource": "sample-reports", "level": "read"}
_INTENT = "Read access to sample-reports needs an administrator's approval first."
_LATER = "A background task finished after the pause."


def _assistant(*blocks: Any, message_id: str | None = None, error: Any = None) -> AssistantMessage:
    return AssistantMessage(
        content=list(blocks), model="example-model", message_id=message_id, error=error
    )


def _result(text: str = "", *, is_error: bool = False) -> ResultMessage:
    return ResultMessage(
        subtype="error_during_execution" if is_error else "success",
        duration_ms=1,
        duration_api_ms=1,
        is_error=is_error,
        num_turns=1,
        session_id="example-session",
        result=text or None,
    )


class _Continuing(FakeModelSession):
    """After the halt, a later response arrives, as a held background result allows."""

    def __init__(self, later: list[Any], **kwargs: Any) -> None:
        super().__init__(lambda: [_blocked_text(), _blocked_call()], **kwargs)
        self._later = later

    async def receive_turn(self) -> AsyncIterator[Any]:
        async for message in super().receive_turn():
            if not isinstance(message, ResultMessage):
                yield message
        for message in self._later:
            yield message


class _LateTimeout:
    """A usage reporter during whose report the turn's timeout lands."""

    def __init__(self) -> None:
        self.runner: SessionRunner | None = None

    def observe(self, _message: object) -> None:
        return None

    async def report(self, _message: object, _model: object) -> None:
        assert self.runner is not None
        self.runner._timeout_requested = True


def _blocked_text() -> AssistantMessage:
    return _assistant(TextBlock(text=_INTENT), message_id="msg-1")


def _blocked_call() -> AssistantMessage:
    return _assistant(ToolUseBlock(id="t1", name=_GATED, input=dict(_ARGS)), message_id="msg-1")


async def _final(
    script: list[Any] | None = None, *, later: list[Any] | None = None, late_timeout: bool = False
) -> Any:
    gate = ApprovalGate(required=frozenset({_GATED}))
    permission = build_can_use_tool(gate)
    session: FakeModelSession = (
        _Continuing(later, can_use_tool=permission)
        if later is not None
        else FakeModelSession(lambda: list(script or []), can_use_tool=permission)
    )
    reporter = _LateTimeout() if late_timeout else None
    runner = SessionRunner(
        session_factory=lambda: session,
        tracer=RunTracer(None),
        classifier=SideEffectClassifier(),
        trace_name="halted-permission-text",
        ceiling=0,
        max_usd_per_day=None,
        held_secrets=frozenset(),
        approval_gate=gate,
        usage_reporter=reporter,  # type: ignore[arg-type]
    )
    if reporter is not None:
        reporter.runner = runner
    await runner.start()
    lines = [
        line
        async for line in runner.run_turn(Event(type="message", text="go", user="U1", ts="1.1"))
    ]
    return parse_ndjson("".join(lines))[-1]


def _run(script: list[Any] | None = None, **kwargs: Any) -> Any:
    return anyio.run(lambda: _final(script, **kwargs))


def test_the_halted_pause_takes_the_blocked_response_text() -> None:
    final = _run([_blocked_text(), _blocked_call()])
    assert final.status is SessionStatus.AWAITING_APPROVAL
    assert final.text == _INTENT


def test_a_provider_failure_on_a_gated_turn_keeps_its_failure_text() -> None:
    # An errored message classifies the turn as failed, which outranks the
    # halt: the failure text stays and no approval text is taken.
    final = _run(
        [
            _blocked_text(),
            _assistant(TextBlock(text="upstream error"), message_id="msg-1", error="server_error"),
            _blocked_call(),
        ]
    )
    assert final.status is SessionStatus.CLASSIFIED_FAILURE
    assert final.text == "run failed"


def test_a_timeout_landing_after_the_result_keeps_its_failure_text() -> None:
    final = _run([_blocked_text(), _blocked_call()], late_timeout=True)
    assert final.status is SessionStatus.CLASSIFIED_FAILURE
    assert final.text == "run failed"


@pytest.mark.parametrize(
    ("later", "expected"),
    [
        ([_assistant(TextBlock(text=_LATER), message_id="msg-2"), _result()], _LATER),
        (
            [_assistant(TextBlock(text=_LATER), message_id="msg-2"), _result(is_error=True)],
            "run failed",
        ),
    ],
    ids=["later-answer", "later-failure"],
)
def test_a_turn_that_continued_after_the_halt_keeps_its_text(
    later: list[Any], expected: str
) -> None:
    final = _run(later=later)
    assert final.status is SessionStatus.AWAITING_APPROVAL
    assert final.text == expected


def test_text_after_the_blocked_call_never_replaces_the_response() -> None:
    # The same response continues after the blocked call and the provider
    # result is empty: the fallback holds only the later block, so the whole
    # response is used instead.
    after = "The administrator will see the request in the access channel."
    final = _run(later=[_assistant(TextBlock(text=after), message_id="msg-1"), _result()])
    assert final.status is SessionStatus.AWAITING_APPROVAL
    assert final.text == f"{_INTENT}\n\n{after}"


def test_messages_without_an_id_are_never_grouped() -> None:
    # No message id proves no shared response, so a separate message's text
    # stays provisional and the halted CLI's failure text is still dropped.
    final = _run(
        [
            _assistant(TextBlock(text=_INTENT)),
            _assistant(ToolUseBlock(id="t1", name=_GATED, input=dict(_ARGS))),
        ]
    )
    assert final.status is SessionStatus.AWAITING_APPROVAL
    assert final.text == ""


def test_text_in_the_blocked_call_message_itself_counts_without_an_id() -> None:
    final = _run(
        [_assistant(TextBlock(text=_INTENT), ToolUseBlock(id="t1", name=_GATED, input=_ARGS))]
    )
    assert final.status is SessionStatus.AWAITING_APPROVAL
    assert final.text == _INTENT


def test_an_ungated_error_result_keeps_its_failure_text() -> None:
    final = _run(
        [
            _blocked_text(),
            _assistant(
                ToolUseBlock(id="t1", name="Read", input={"file_path": "x"}), message_id="msg-1"
            ),
            _result(is_error=True),
        ]
    )
    assert final.status is SessionStatus.CLASSIFIED_FAILURE
    assert final.text == "run failed"


def _halted_state(**overrides: Any) -> TurnState:
    state = TurnState(
        approval_summary="Tool call awaiting approval: x {}",
        approval_gate_kind="permission",
        approval_granted_tool=_GATED,
        approval_halt_requested=True,
        approval_call_id="t1",
        approval_flipped_failure=True,
        last_response="id:msg-1",
    )
    state.call_responses["t1"] = "id:msg-1"
    state.response_text["id:msg-1"] = _INTENT
    for key, value in overrides.items():
        setattr(state, key, value)
    return state


def _awaiting(text: str) -> Final:
    return Final(text=text, status=SessionStatus.AWAITING_APPROVAL, approval_summary="s")


def test_the_override_keeps_the_text_and_records_the_failure_path() -> None:
    state = _halted_state(approval_flipped_failure=False)
    failed = Final(text="run failed", status=SessionStatus.CLASSIFIED_FAILURE)
    flipped = _apply_approval_override(failed, state)
    assert flipped.status is SessionStatus.AWAITING_APPROVAL
    assert flipped.text == "run failed"
    assert state.approval_flipped_failure is True
    _apply_approval_override(Final(text="", status=SessionStatus.DONE), state)
    assert state.approval_flipped_failure is False


def test_the_halted_permission_pause_takes_the_blocked_response() -> None:
    assert _halted_pause_text(_awaiting("run failed"), _halted_state()).text == _INTENT
    done = _halted_state(approval_flipped_failure=False)
    assert _halted_pause_text(_awaiting(""), done).text == _INTENT
    assert _halted_pause_text(_awaiting("Waiting."), done).text == "Waiting."
    fallback = _halted_state(approval_flipped_failure=False, provider_result_empty=True)
    assert _halted_pause_text(_awaiting("Later block."), fallback).text == _INTENT


@pytest.mark.parametrize(
    "overrides",
    [
        {"approval_granted_tool": PLATFORM_PUBLISH_TOOL_NAME},
        {"approval_gate_kind": "policy", "approval_granted_tool": None},
        {"approval_call_id": None},
        {"approval_call_id": "t-unseen"},
        {"last_response": "id:msg-2"},
        {"approval_halt_requested": False},
        {"error_classification": "server-error"},
        {"final_answer": FinalAnswerValidation(FinalAnswerPolicy(tool="mcp__checks__finalize"))},
    ],
    ids=[
        "publication",
        "policy",
        "no-call-id",
        "unobserved-call",
        "later-response",
        "not-halted",
        "classified-error",
        "final-answer-policy",
    ],
)
def test_every_other_pause_keeps_its_text(overrides: dict[str, Any]) -> None:
    state = _halted_state(**overrides)
    assert _halted_pause_text(_awaiting("run failed"), state).text == "run failed"
    assert _halted_pause_text(_awaiting(""), state).text == ""


@pytest.mark.parametrize(
    "status", [SessionStatus.CLASSIFIED_FAILURE, SessionStatus.IDLE_AWAITING_INPUT]
)
def test_a_final_that_is_not_awaiting_approval_is_unchanged(status: SessionStatus) -> None:
    final = Final(text="run failed", status=status)
    assert _halted_pause_text(final, _halted_state()) == final


def test_only_the_call_that_wrote_the_record_is_named() -> None:
    gate = ApprovalGate(required=frozenset({_GATED}))
    gate.block(_GATED, {"n": 1}, call_id="t1")
    gate.block(_GATED, {"n": 2}, call_id="t2")
    assert gate.pending_call_id == "t1"
    gate.reset()
    assert gate.pending_call_id is None
