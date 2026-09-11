"""SDK-message to ACI-outbound-event translation."""

import json

import pytest
from aci_protocol import ErrorEvent, Final, SessionStatus
from claude_agent_sdk import (
    AssistantMessage,
    RateLimitEvent,
    ResultMessage,
    TextBlock,
    ThinkingBlock,
    ToolResultBlock,
    ToolUseBlock,
    UserMessage,
)
from claude_agent_sdk.types import RateLimitInfo
from curie_runner import SideEffectClassifier
from curie_runner.translate import RESULT_MAX_BYTES, TurnState, translate_message


def _translate(message: object, state: TurnState | None = None) -> list:
    return translate_message(message, state or TurnState(), SideEffectClassifier(), None)


def test_text_block_becomes_text_delta() -> None:
    msg = AssistantMessage(content=[TextBlock(text="hi there")], model="m")
    events = _translate(msg)
    assert [e.type for e in events] == ["text_delta"]
    assert events[0].text == "hi there"


def test_tool_use_emits_a_note_and_a_flag_per_side_effecting_call() -> None:
    state = TurnState()
    msg = AssistantMessage(
        content=[
            ToolUseBlock(id="1", name="Bash", input={}),
            ToolUseBlock(id="2", name="Write", input={}),
        ],
        model="m",
    )
    events = _translate(msg, state)
    types = [e.type for e in events]
    # A note and a flag for each call. The flag was capped at once per run while
    # its only consumer was a boolean; the action is the unit now (ADR-0117), and
    # ``side_effect_emitted`` still latches for the consumers that read presence.
    assert types.count("tool_note") == 2
    assert types.count("side_effect_flag") == 2
    assert state.side_effect_emitted


def test_read_only_tool_notes_without_flag() -> None:
    msg = AssistantMessage(content=[ToolUseBlock(id="1", name="Read", input={})], model="m")
    events = _translate(msg)
    assert [e.type for e in events] == ["tool_note"]


def test_tool_search_notes_without_flag() -> None:
    """#2130: Claude's tool-discovery read is not a receipt mutation."""

    msg = AssistantMessage(
        content=[ToolUseBlock(id="1", name="ToolSearch", input={"query": "resources"})],
        model="m",
    )

    events = _translate(msg)

    assert [event.type for event in events] == ["tool_note"]


def test_result_success_is_final_done() -> None:
    msg = ResultMessage(
        subtype="success", duration_ms=1, duration_api_ms=1, is_error=False,
        num_turns=1, session_id="s", result="answer",
    )
    events = _translate(msg)
    assert [e.type for e in events] == ["final"]
    assert events[0].status == SessionStatus.DONE
    assert events[0].text == "answer"


def test_success_final_carries_token_usage() -> None:
    # #390: usage from the SDK result rides the successful final so a consumer
    # can attribute a dollar cost to the turn.
    msg = ResultMessage(
        subtype="success", duration_ms=1, duration_api_ms=1, is_error=False,
        num_turns=1, session_id="s", result="answer",
        usage={"input_tokens": 1200, "output_tokens": 88},
    )
    events = _translate(msg)
    assert events[0].input_tokens == 1200
    assert events[0].output_tokens == 88


def test_success_final_has_no_usage_when_result_reports_none() -> None:
    # A result with no usage block leaves the wire counts None (never a
    # fabricated zero), so a consumer reads "cost unknown".
    msg = ResultMessage(
        subtype="success", duration_ms=1, duration_api_ms=1, is_error=False,
        num_turns=1, session_id="s", result="answer", usage=None,
    )
    events = _translate(msg)
    assert events[0].input_tokens is None
    assert events[0].output_tokens is None


def test_failure_final_carries_no_usage() -> None:
    # A classified-failure final is never graded, so it carries no cost signal.
    msg = ResultMessage(
        subtype="error", duration_ms=1, duration_api_ms=1, is_error=True,
        num_turns=1, session_id="s", result="boom",
        usage={"input_tokens": 10, "output_tokens": 2},
    )
    events = _translate(msg)
    final = next(e for e in events if e.type == "final")
    assert final.input_tokens is None
    assert final.output_tokens is None


def test_reasoning_model_empty_result_falls_back_to_assistant_text() -> None:
    # A reasoning model routed through OpenRouter (e.g. z-ai/glm-5.2) streams the
    # answer as a TextBlock but the terminal ResultMessage reports success with an
    # EMPTY result (the empty-signature thinking block trips result extraction).
    # The delivered Final must carry the assistant text, not "".
    state = TurnState()
    assistant = AssistantMessage(
        content=[
            TextBlock(text="The sky is blue on a clear day."),
            ThinkingBlock(thinking="the user asked...", signature=""),
        ],
        model="z-ai/glm-5.2",
    )
    _translate(assistant, state)  # accumulates text into state
    result = ResultMessage(
        subtype="success", duration_ms=1, duration_api_ms=1, is_error=False,
        num_turns=1, session_id="s", result="",
    )
    events = _translate(result, state)
    assert [e.type for e in events] == ["final"]
    assert events[0].status == SessionStatus.DONE
    assert events[0].text == "The sky is blue on a clear day."


def test_result_with_own_text_ignores_accumulated_fallback() -> None:
    # When the ResultMessage carries its own result, it wins over accumulated text
    # (non-reasoning models are unaffected by the empty-result fallback).
    state = TurnState()
    _translate(AssistantMessage(content=[TextBlock(text="streamed")], model="m"), state)
    result = ResultMessage(
        subtype="success", duration_ms=1, duration_api_ms=1, is_error=False,
        num_turns=1, session_id="s", result="authoritative",
    )
    events = _translate(result, state)
    assert [e.type for e in events] == ["final"]
    assert events[0].text == "authoritative"


def test_result_error_is_error_then_classified_final() -> None:
    msg = ResultMessage(
        subtype="error_during_execution", duration_ms=1, duration_api_ms=1, is_error=True,
        num_turns=1, session_id="s", result="boom",
    )
    events = _translate(msg)
    assert [e.type for e in events] == ["error", "final"]
    assert events[-1].status == SessionStatus.CLASSIFIED_FAILURE


@pytest.mark.parametrize("terminal_reason", ("aborted_streaming", "aborted_tools"))
def test_sdk_abort_result_is_error_then_classified_final(
    terminal_reason: str,
) -> None:
    msg = ResultMessage(
        subtype="error_during_execution",
        duration_ms=1,
        duration_api_ms=1,
        is_error=True,
        num_turns=1,
        session_id="s",
        result="run failed",
        terminal_reason=terminal_reason,
    )

    events = _translate(msg)

    assert [event.type for event in events] == ["error", "final"]
    assert isinstance(events[0], ErrorEvent)
    assert events[0].classification == "unclassified"
    assert events[0].classification != "error_during_execution"
    # Allowlist-constrain must not drop the raw subtype from the message.
    assert "error_during_execution" in events[0].message
    assert "run failed" in events[0].message
    assert isinstance(events[1], Final)
    assert events[1].status is SessionStatus.CLASSIFIED_FAILURE


def test_assistant_error_field_emits_error_event() -> None:
    msg = AssistantMessage(content=[], model="m", error="rate_limit")
    events = _translate(msg)
    assert [e.type for e in events] == ["error"]
    assert events[0].classification == "unclassified"
    assert events[0].classification != "rate_limit"
    assert "rate_limit" in events[0].message


def test_assistant_unknown_error_is_unclassified_not_passthrough() -> None:
    msg = AssistantMessage(content=[], model="m", error="unknown")
    events = _translate(msg)
    assert [e.type for e in events] == ["error"]
    assert isinstance(events[0], ErrorEvent)
    assert events[0].classification == "unclassified"
    assert "unknown" in events[0].message
    assert events[0].classification != "unknown"


def test_rate_limit_rejected_maps_to_error() -> None:
    info = RateLimitInfo(status="rejected")
    events = _translate(RateLimitEvent(rate_limit_info=info, uuid="u", session_id="s"))
    assert [e.type for e in events] == ["error"]
    assert events[0].classification == "rate-limit"


def test_rate_limit_warning_is_dropped() -> None:
    # allowed / allowed_warning are advisory; the run is still allowed to
    # continue, so no failure event is injected.
    for status in ("allowed", "allowed_warning"):
        info = RateLimitInfo(status=status)
        events = _translate(RateLimitEvent(rate_limit_info=info, uuid="u", session_id="s"))
        assert events == []


# --- One frame per call, and what each carries (ADR-0117) ----------------------


def _tool_result(
    tool_use_id: str,
    content: object,
    *,
    is_error: bool | None = None,
) -> UserMessage:
    return UserMessage(
        content=[
            ToolResultBlock(tool_use_id=tool_use_id, content=content, is_error=is_error)
        ]
    )


def test_each_side_effecting_call_gets_its_own_flag() -> None:
    """The cap was once-per-turn because a boolean cannot be set twice.

    A turn that calls three mutating tools reported one. The record, the receipt
    line and the undo are all per action, so the frame is too.
    """

    state = TurnState()
    msg = AssistantMessage(
        content=[
            ToolUseBlock(id="1", name="Bash", input={"command": "ls"}),
            ToolUseBlock(id="2", name="Write", input={"file_path": "/tmp/x"}),
        ],
        model="m",
    )
    flags = [e for e in _translate(msg, state) if e.type == "side_effect_flag"]
    assert [f.call_id for f in flags] == ["1", "2"]
    assert [f.arguments for f in flags] == [{"command": "ls"}, {"file_path": "/tmp/x"}]


def test_the_no_retry_signal_still_latches() -> None:
    """ADR-0013's rule reads presence, not count, and kernel.py is sacred."""

    state = TurnState()
    msg = AssistantMessage(content=[ToolUseBlock(id="1", name="Bash", input={})], model="m")
    _translate(msg, state)
    assert state.side_effect_emitted


def test_a_side_effecting_result_closes_its_call() -> None:
    state = TurnState()
    _translate(
        AssistantMessage(content=[ToolUseBlock(id="1", name="Bash", input={})], model="m"),
        state,
    )
    events = _translate(_tool_result("1", '{"ok": true, "prior": {"replicas": 3}}'), state)
    assert [e.type for e in events] == ["side_effect_flag"]
    assert events[0].call_id == "1"
    assert events[0].result == {"ok": True, "prior": {"replicas": 3}}
    assert events[0].failed is False


def test_a_read_only_result_is_still_dropped_whole() -> None:
    """File contents are the model's working material, not wire traffic."""

    state = TurnState()
    _translate(
        AssistantMessage(content=[ToolUseBlock(id="1", name="Read", input={})], model="m"),
        state,
    )
    assert _translate(_tool_result("1", "the whole file"), state) == []


def test_a_prose_reply_carries_no_structured_result() -> None:
    """Guessing structure out of a sentence is how a restore acts on a guess."""

    state = TurnState()
    _translate(
        AssistantMessage(content=[ToolUseBlock(id="1", name="Bash", input={})], model="m"),
        state,
    )
    events = _translate(_tool_result("1", "restarted the deployment"), state)
    assert events[0].result is None


def test_a_failed_call_says_so() -> None:
    """A record is undoable only on a successful outcome, so the outcome travels."""

    state = TurnState()
    _translate(
        AssistantMessage(content=[ToolUseBlock(id="1", name="Bash", input={})], model="m"),
        state,
    )
    events = _translate(_tool_result("1", '{"ok": false}', is_error=True), state)
    assert events[0].failed is True


def test_an_oversized_result_is_dropped_not_truncated() -> None:
    """A truncated JSON object is a lie a restore would act on."""

    state = TurnState()
    _translate(
        AssistantMessage(content=[ToolUseBlock(id="1", name="Bash", input={})], model="m"),
        state,
    )
    huge = json.dumps({"prior": {"blob": "x" * (RESULT_MAX_BYTES + 1)}})
    events = _translate(_tool_result("1", huge), state)
    assert events[0].result is None
    assert events[0].detail == "tool result too large to record"


def test_a_result_for_an_unknown_call_is_ignored() -> None:
    state = TurnState()
    assert _translate(_tool_result("nope", '{"ok": true}'), state) == []


def test_a_call_is_closed_exactly_once() -> None:
    """A duplicate result must not mint a second record for one call."""

    state = TurnState()
    _translate(
        AssistantMessage(content=[ToolUseBlock(id="1", name="Bash", input={})], model="m"),
        state,
    )
    assert len(_translate(_tool_result("1", '{"ok": true}'), state)) == 1
    assert _translate(_tool_result("1", '{"ok": true}'), state) == []


def test_a_string_user_message_is_not_iterated_as_characters() -> None:
    """UserMessage.content is str OR a block list; the str case has no results."""

    assert _translate(UserMessage(content="just text"), TurnState()) == []


def test_a_call_whose_result_never_arrives_leaves_its_opening_frame() -> None:
    """A turn that died mid-call still reported that something mutated.

    The opening frame is the honest record of an attempt: arguments, no result,
    and therefore not undoable downstream.
    """

    state = TurnState()
    events = _translate(
        AssistantMessage(
            content=[ToolUseBlock(id="1", name="Bash", input={"command": "rm"})], model="m"
        ),
        state,
    )
    flags = [e for e in events if e.type == "side_effect_flag"]
    assert len(flags) == 1
    assert flags[0].result is None
    assert state.pending_actions == {"1": "Bash"}
