"""The text of a permission pause the runner halted at its blocked call (ADR 0296).

Only that pause takes the blocked call's own model response, and only that
pause drops the aborted CLI result's failure text. Every other final keeps its
text: real failures, publication and policy pauses, final-answer bundles, a
record whose call was never observed, and turns the runner did not halt.
"""

from __future__ import annotations

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
from curie_runner.session import SessionRunner, _apply_approval_override
from curie_runner.side_effects import SideEffectClassifier
from curie_runner.translate import TurnState
from plugin_format import PLATFORM_PUBLISH_TOOL_NAME

_GATED = "mcp__acme-access__grant_access"
_ARGS = {"resource": "sample-reports", "level": "read"}
_INTENT = "Read access to sample-reports needs an administrator's approval first."


def _assistant(*blocks: Any, message_id: str | None = None, error: Any = None) -> AssistantMessage:
    return AssistantMessage(
        content=list(blocks), model="example-model", message_id=message_id, error=error
    )


async def _final(script: list[Any]) -> Any:
    gate = ApprovalGate(required=frozenset({_GATED}))
    session = FakeModelSession(lambda: list(script), can_use_tool=build_can_use_tool(gate))
    runner = SessionRunner(
        session_factory=lambda: session,
        tracer=RunTracer(None),
        classifier=SideEffectClassifier(),
        trace_name="halted-permission-text",
        ceiling=0,
        max_usd_per_day=None,
        held_secrets=frozenset(),
        approval_gate=gate,
    )
    await runner.start()
    lines = [
        line
        async for line in runner.run_turn(Event(type="message", text="go", user="U1", ts="1.1"))
    ]
    return parse_ndjson("".join(lines))[-1]


def _run(script: list[Any]) -> Any:
    return anyio.run(_final, script)


def test_a_provider_failure_on_a_gated_turn_keeps_its_failure_text() -> None:
    # A classified provider error outranks the halt: the turn failed, so the
    # failure text stays and no approval text is taken from the response.
    final = _run(
        [
            _assistant(TextBlock(text=_INTENT), message_id="msg-1"),
            _assistant(TextBlock(text="upstream error"), message_id="msg-1", error="server_error"),
            _assistant(ToolUseBlock(id="t1", name=_GATED, input=dict(_ARGS)), message_id="msg-1"),
        ]
    )
    assert final.status is SessionStatus.CLASSIFIED_FAILURE
    assert final.text == "run failed"


def test_the_blocked_response_text_excludes_errored_messages() -> None:
    final = _run(
        [
            _assistant(TextBlock(text=_INTENT), message_id="msg-1"),
            _assistant(ToolUseBlock(id="t1", name=_GATED, input=dict(_ARGS)), message_id="msg-1"),
        ]
    )
    assert final.status is SessionStatus.AWAITING_APPROVAL
    assert final.text == _INTENT


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
            _assistant(TextBlock(text=_INTENT), message_id="msg-1"),
            _assistant(
                ToolUseBlock(id="t1", name="Read", input={"file_path": "x"}), message_id="msg-1"
            ),
            ResultMessage(
                subtype="error_during_execution",
                duration_ms=1,
                duration_api_ms=1,
                is_error=True,
                num_turns=1,
                session_id="example-session",
            ),
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
    )
    state.call_responses["t1"] = "id:msg-1"
    state.response_text["id:msg-1"] = _INTENT
    for key, value in overrides.items():
        setattr(state, key, value)
    return state


_FAILED = Final(text="run failed", status=SessionStatus.CLASSIFIED_FAILURE)


def test_the_halted_permission_pause_takes_the_blocked_response() -> None:
    final = _apply_approval_override(_FAILED, _halted_state())
    assert final.status is SessionStatus.AWAITING_APPROVAL
    assert final.text == _INTENT
    done = _apply_approval_override(Final(text="", status=SessionStatus.DONE), _halted_state())
    assert done.text == _INTENT
    kept = _apply_approval_override(
        Final(text="Waiting.", status=SessionStatus.DONE), _halted_state()
    )
    assert kept.text == "Waiting."


@pytest.mark.parametrize(
    "overrides",
    [
        {"approval_granted_tool": PLATFORM_PUBLISH_TOOL_NAME},
        {"approval_gate_kind": "policy", "approval_granted_tool": None},
        {"approval_call_id": None},
        {"approval_call_id": "t-unseen"},
        {"final_answer": FinalAnswerValidation(FinalAnswerPolicy(tool="mcp__checks__finalize"))},
    ],
    ids=["publication", "policy", "no-call-id", "unobserved-call", "final-answer-policy"],
)
def test_every_other_pause_keeps_its_text(overrides: dict[str, Any]) -> None:
    final = _apply_approval_override(_FAILED, _halted_state(**overrides))
    assert final.status is SessionStatus.AWAITING_APPROVAL
    assert final.text == "run failed"
    done = _apply_approval_override(
        Final(text="", status=SessionStatus.DONE), _halted_state(**overrides)
    )
    assert done.text == ""


def test_a_turn_the_runner_did_not_halt_keeps_its_text() -> None:
    state = _halted_state(approval_halt_requested=False)
    assert _apply_approval_override(_FAILED, state) == _FAILED
    done = _apply_approval_override(Final(text="", status=SessionStatus.DONE), state)
    assert done.status is SessionStatus.AWAITING_APPROVAL and done.text == ""
    failed = _halted_state(error_classification="server-error")
    assert _apply_approval_override(_FAILED, failed) == _FAILED


def test_only_the_call_that_wrote_the_record_is_named() -> None:
    gate = ApprovalGate(required=frozenset({_GATED}))
    gate.block(_GATED, {"n": 1}, call_id="t1")
    gate.block(_GATED, {"n": 2}, call_id="t2")
    assert gate.pending_call_id == "t1"
    gate.reset()
    assert gate.pending_call_id is None
