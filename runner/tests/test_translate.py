"""SDK-message to ACI-outbound-event translation."""

from aci_protocol import SessionStatus
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
from curie_runner.translate import TurnState, translate_message


def _translate(message: object, state: TurnState | None = None) -> list:
    return translate_message(message, state or TurnState(), SideEffectClassifier(), None)


def test_text_block_becomes_text_delta() -> None:
    msg = AssistantMessage(content=[TextBlock(text="hi there")], model="m")
    events = _translate(msg)
    assert [e.type for e in events] == ["text_delta"]
    assert events[0].text == "hi there"


def test_every_side_effecting_call_emits_its_own_flag() -> None:
    """One flag per CALL, not per run.

    This used to assert one flag per run, because the only consumer was the
    kernel's no-retry rule and a boolean cannot be more than set. A consumer that
    records what a turn did to the world needs each call, so the dedup is gone
    while ``side_effect_emitted`` still latches for the rule that reads it.
    """

    state = TurnState()
    msg = AssistantMessage(
        content=[
            ToolUseBlock(id="1", name="Bash", input={"command": "ls"}),
            ToolUseBlock(id="2", name="Write", input={"path": "a"}),
        ],
        model="m",
    )
    events = _translate(msg, state)
    types = [e.type for e in events]
    assert types.count("tool_note") == 2
    assert types.count("side_effect_flag") == 2
    assert state.side_effect_emitted
    flags = [e for e in events if e.type == "side_effect_flag"]
    assert [f.tool for f in flags] == ["Bash", "Write"]
    assert flags[0].arguments == {"command": "ls"}


def test_a_read_only_call_records_nothing_to_attribute_a_result_to() -> None:
    state = TurnState()
    _translate(
        AssistantMessage(
            content=[ToolUseBlock(id="1", name="Read", input={"path": "a"})], model="m"
        ),
        state,
    )
    assert state.pending_actions == {}


def test_a_side_effecting_result_is_attributed_to_its_call() -> None:
    """The result arrives on a later UserMessage, which used to be dropped whole."""

    state = TurnState()
    _translate(
        AssistantMessage(
            content=[ToolUseBlock(id="call-1", name="Write", input={"path": "a"})], model="m"
        ),
        state,
    )
    events = _translate(
        UserMessage(content=[ToolResultBlock(tool_use_id="call-1", content='{"ok": true}')]),
        state,
    )
    assert [e.type for e in events] == ["side_effect_flag"]
    assert events[0].tool == "Write"
    assert events[0].result == {"ok": True}
    assert state.pending_actions == {}


def test_a_prose_result_carries_no_structured_payload() -> None:
    """Guessing structure out of a sentence is how a restore acts on a guess."""

    state = TurnState()
    _translate(
        AssistantMessage(
            content=[ToolUseBlock(id="call-1", name="Write", input={})], model="m"
        ),
        state,
    )
    events = _translate(
        UserMessage(content=[ToolResultBlock(tool_use_id="call-1", content="restart triggered")]),
        state,
    )
    assert events[0].result is None


def test_a_read_only_result_is_still_dropped() -> None:
    """File contents are the model's working material, not wire traffic."""

    state = TurnState()
    _translate(
        AssistantMessage(content=[ToolUseBlock(id="r1", name="Read", input={})], model="m"),
        state,
    )
    events = _translate(
        UserMessage(content=[ToolResultBlock(tool_use_id="r1", content="the file body")]),
        state,
    )
    assert events == []


def test_read_only_tool_notes_without_flag() -> None:
    msg = AssistantMessage(content=[ToolUseBlock(id="1", name="Read", input={})], model="m")
    events = _translate(msg)
    assert [e.type for e in events] == ["tool_note"]


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


def test_assistant_error_field_emits_error_event() -> None:
    msg = AssistantMessage(content=[], model="m", error="rate_limit")
    events = _translate(msg)
    assert [e.type for e in events] == ["error"]
    assert events[0].classification == "rate_limit"


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
