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
from curie_runner.translate import (
    RESULT_MAX_BYTES,
    TurnState,
    _is_credit_exhausted,
    translate_message,
)


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


def test_the_progress_tool_notes_without_flag() -> None:
    """ADR 0130: a deliberate progress update acts on nothing, so it is never a
    side effect and never lands on the turn's receipt."""

    from curie_runner.approval import TURN_PROGRESS_TOOL_NAME

    state = TurnState()
    msg = AssistantMessage(
        content=[
            ToolUseBlock(
                id="1",
                name=TURN_PROGRESS_TOOL_NAME,
                input={"update_id": "u1", "state": "testing", "summary": "Verified it"},
            )
        ],
        model="m",
    )
    events = _translate(msg, state)
    assert [e.type for e in events] == ["tool_note"]
    assert not state.side_effect_emitted


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
        subtype="success",
        duration_ms=1,
        duration_api_ms=1,
        is_error=False,
        num_turns=1,
        session_id="s",
        result="answer",
    )
    events = _translate(msg)
    assert [e.type for e in events] == ["final"]
    assert events[0].status == SessionStatus.DONE
    assert events[0].text == "answer"


def test_success_final_carries_token_usage() -> None:
    # #390: usage from the SDK result rides the successful final so a consumer
    # can attribute a dollar cost to the turn.
    msg = ResultMessage(
        subtype="success",
        duration_ms=1,
        duration_api_ms=1,
        is_error=False,
        num_turns=1,
        session_id="s",
        result="answer",
        usage={"input_tokens": 1200, "output_tokens": 88},
    )
    events = _translate(msg)
    assert events[0].input_tokens == 1200
    assert events[0].output_tokens == 88


def test_success_final_has_no_usage_when_result_reports_none() -> None:
    # A result with no usage block leaves the wire counts None (never a
    # fabricated zero), so a consumer reads "cost unknown".
    msg = ResultMessage(
        subtype="success",
        duration_ms=1,
        duration_api_ms=1,
        is_error=False,
        num_turns=1,
        session_id="s",
        result="answer",
        usage=None,
    )
    events = _translate(msg)
    assert events[0].input_tokens is None
    assert events[0].output_tokens is None


def test_failure_final_carries_no_usage() -> None:
    # A classified-failure final is never graded, so it carries no cost signal.
    msg = ResultMessage(
        subtype="error",
        duration_ms=1,
        duration_api_ms=1,
        is_error=True,
        num_turns=1,
        session_id="s",
        result="boom",
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
        subtype="success",
        duration_ms=1,
        duration_api_ms=1,
        is_error=False,
        num_turns=1,
        session_id="s",
        result="",
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
        subtype="success",
        duration_ms=1,
        duration_api_ms=1,
        is_error=False,
        num_turns=1,
        session_id="s",
        result="authoritative",
    )
    events = _translate(result, state)
    assert [e.type for e in events] == ["final"]
    assert events[0].text == "authoritative"


def test_result_error_is_error_then_classified_final() -> None:
    msg = ResultMessage(
        subtype="error_during_execution",
        duration_ms=1,
        duration_api_ms=1,
        is_error=True,
        num_turns=1,
        session_id="s",
        result="boom",
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


def test_sdk_max_turns_result_is_classified_max_turns() -> None:
    """#3071: the SDK's error_max_turns subtype is a known, named failure (the
    turn budget ran out), not an unclassified one."""
    state = TurnState()
    msg = ResultMessage(
        subtype="error_max_turns",
        duration_ms=1,
        duration_api_ms=1,
        is_error=True,
        num_turns=5,
        session_id="s",
        result=None,
    )

    events = _translate(msg, state)

    assert [event.type for event in events] == ["error", "final"]
    assert isinstance(events[0], ErrorEvent)
    assert events[0].classification == "max-turns"
    assert state.error_classification == "max-turns"
    assert isinstance(events[1], Final)
    assert events[1].status is SessionStatus.CLASSIFIED_FAILURE


def test_sdk_usd_budget_result_is_classified_budget_exceeded() -> None:
    state = TurnState()
    msg = ResultMessage(
        subtype="error_max_budget_usd",
        duration_ms=1,
        duration_api_ms=1,
        is_error=True,
        num_turns=5,
        session_id="s",
        result=None,
    )

    events = _translate(msg, state)

    # The SDK documents this subtype in its official budget example:
    # https://github.com/anthropics/claude-agent-sdk-python/blob/main/examples/max_budget_usd.py
    assert [event.type for event in events] == ["error", "final"]
    assert isinstance(events[0], ErrorEvent)
    assert events[0].classification == "budget-exceeded"
    assert events[0].message == "run failed"
    assert state.error_classification == "budget-exceeded"
    assert isinstance(events[1], Final)
    assert events[1].status is SessionStatus.CLASSIFIED_FAILURE


def test_unknown_sdk_result_subtype_stays_unclassified() -> None:
    state = TurnState()
    msg = ResultMessage(
        subtype="error_future_budget",
        duration_ms=1,
        duration_api_ms=1,
        is_error=True,
        num_turns=1,
        session_id="s",
        result="run failed",
    )

    events = _translate(msg, state)

    assert [event.type for event in events] == ["error", "final"]
    assert isinstance(events[0], ErrorEvent)
    assert events[0].classification == "unclassified"
    assert "error_future_budget" in events[0].message
    assert state.error_classification is None
    assert isinstance(events[1], Final)
    assert events[1].status is SessionStatus.CLASSIFIED_FAILURE


def test_budget_named_assistant_error_stays_unclassified() -> None:
    msg = AssistantMessage(content=[], model="m", error="error_max_budget_usd")

    events = _translate(msg)

    assert [event.type for event in events] == ["error"]
    assert isinstance(events[0], ErrorEvent)
    assert events[0].classification == "unclassified"


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
        content=[ToolResultBlock(tool_use_id=tool_use_id, content=content, is_error=is_error)]
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


# --- Provider credit exhaustion (#3073) ---------------------------------------

_OPENROUTER_KEY = "sk-or-v1-" + "0123456789abcdef" * 4
# Observed 2026-09-24: the bundled claude_agent_sdk, pointed at a stub that
# answers OpenRouter's HTTP 402 body, emitted exactly
# AssistantMessage(content=[TextBlock(text="API Error: 402 This request requires
# more credits, ...")], model="<synthetic>", error="unknown"). The key is
# appended here only to prove the provider text is redacted.
_OPENROUTER_402 = (
    "API Error: 402 This request requires more credits, or fewer max_tokens. You "
    "requested up to 32000 tokens, but can only afford 1200. To increase, visit "
    "https://openrouter.ai/settings/credits and upgrade to a paid account "
    f"key={_OPENROUTER_KEY}"
)


def test_openrouter_402_is_credit_exhausted_with_redacted_provider_message() -> None:
    state = TurnState()
    msg = AssistantMessage(
        content=[TextBlock(text=_OPENROUTER_402)], model="<synthetic>", error="unknown"
    )
    errors = [e for e in _translate(msg, state) if isinstance(e, ErrorEvent)]
    assert len(errors) == 1
    assert errors[0].classification == "model-credit-exhausted"
    assert state.error_classification == "model-credit-exhausted"
    assert "requires more credits" in errors[0].message
    assert _OPENROUTER_KEY not in errors[0].message
    assert "[REDACTED" in errors[0].message


def test_sdk_billing_error_is_credit_exhausted() -> None:
    msg = AssistantMessage(content=[], model="m", error="billing_error")
    events = _translate(msg)
    assert events[0].classification == "model-credit-exhausted"


# Observed 2026-10-05 on a staging install (#4104): an OpenRouter key at its own
# spend limit answers HTTP 403 with this body, and the SDK reports it as
# error="authentication_failed". Workspace and key ids replaced with "example".
_OPENROUTER_KEY_LIMIT_403 = (
    'API Error: 403 {"error":{"message":"Key limit exceeded (total limit). Manage it '
    'using https://openrouter.ai/workspaces/example/keys/example","code":403}}'
)


def test_openrouter_key_limit_text_is_credit_exhausted() -> None:
    assert _is_credit_exhausted("authentication_failed", _OPENROUTER_KEY_LIMIT_403)


def test_rate_limit_text_is_not_credit_exhausted() -> None:
    # A retryable rate limit must stay out of the terminal credit class.
    assert not _is_credit_exhausted("rate_limit", "API Error: 429 Rate limit exceeded")


def test_unknown_error_without_credit_text_stays_unclassified() -> None:
    msg = AssistantMessage(
        content=[TextBlock(text="API Error: 500 upstream exploded")], model="m", error="unknown"
    )
    errors = [e for e in _translate(msg) if isinstance(e, ErrorEvent)]
    assert errors[0].classification == "unclassified"
    assert "upstream exploded" in errors[0].message


# --- A reviewer subagent's credit refusal ends the turn (#3935) ---------------

# Measured 2026-10-04 on bundled CLI 2.1.281: a subagent request answered HTTP
# 402 with OpenRouter's body is not retried, and the parent turn recovers and
# ends with a successful ResultMessage. With the runner's options
# (forward_subagent_text left False) the runner sees only the Agent call's
# is_error tool result below; with forward_subagent_text=True it also sees the
# subagent's errored assistant message (parent_tool_use_id set, error unknown).
_SUBAGENT_402 = (
    "API Error: 402 This request requires more credits, or fewer max_tokens. You "
    "requested up to 64000 tokens, but can only afford 61300. To increase, visit "
    "https://openrouter.ai/settings/credits and upgrade to a paid account"
)


def _subagent_error_then_success(text: str) -> list:
    state = TurnState()
    errored = AssistantMessage(
        content=[TextBlock(text=text)],
        model="<synthetic>",
        parent_tool_use_id="toolu_rev1",
        error="unknown",
    )
    success = ResultMessage(
        subtype="success",
        duration_ms=1,
        duration_api_ms=1,
        is_error=False,
        num_turns=2,
        session_id="s",
        result="The reviewer could not run.",
    )
    return _translate(errored, state) + _translate(success, state)


def test_a_subagent_credit_refusal_ends_the_turn_classified_failure() -> None:
    """#3935 AC4: a reviewer's 402 is terminal even when the parent recovers.

    The forwarded shape, which the SDK only emits with
    ``forward_subagent_text=True``. Red today: the ErrorEvent is already
    emitted, but ``_translate_result`` returns Final DONE for the parent's
    successful result, so the worker delivers the turn instead of ending the
    run as out of credits.
    """

    events = _subagent_error_then_success(_SUBAGENT_402)

    errors = [e for e in events if isinstance(e, ErrorEvent)]
    assert len(errors) == 1
    assert errors[0].classification == "model-credit-exhausted"
    finals = [e for e in events if isinstance(e, Final)]
    assert len(finals) == 1
    assert events[-1] is finals[0]
    assert finals[0].status is SessionStatus.CLASSIFIED_FAILURE


def test_a_subagent_non_credit_error_then_success_stays_done() -> None:
    """Liveness for #3935: only a credit refusal turns a recovered turn terminal.

    Green today and must stay green: an overloaded subagent the parent recovered
    from is not out of credits, so the turn still delivers.
    """

    events = _subagent_error_then_success("API Error: 529 Overloaded")

    errors = [e for e in events if isinstance(e, ErrorEvent)]
    assert [e.classification for e in errors] == ["unclassified"]
    finals = [e for e in events if isinstance(e, Final)]
    assert len(finals) == 1
    assert finals[0].status is SessionStatus.DONE


_AGENT_402_RESULT = (
    "Agent terminated early due to an API error: "
    + _SUBAGENT_402
    + " (error type unknown, HTTP 402, model sent to the API: acme-reviewer-model)"
)


def _tool_call_then_success(
    result_text: str, *, tool: str = "Agent", is_error: bool | None = True
) -> list:
    """One tool call, its result, then the parent's successful result."""

    state = TurnState()
    tool_input = (
        {"description": "Diff review round 1", "subagent_type": "reviewer"}
        if tool == "Agent"
        else {"command": "uv run pytest tests/test_billing.py -q"}
    )
    call = AssistantMessage(
        content=[ToolUseBlock(id="toolu_rev1", name=tool, input=tool_input)],
        model="m",
    )
    success = ResultMessage(
        subtype="success",
        duration_ms=1,
        duration_api_ms=1,
        is_error=False,
        num_turns=2,
        session_id="s",
        result="The reviewer could not run.",
    )
    return (
        _translate(call, state)
        + _translate(_tool_result("toolu_rev1", result_text, is_error=is_error), state)
        + _translate(success, state)
    )


def _credit_errors(events: list) -> list:
    return [
        e
        for e in events
        if isinstance(e, ErrorEvent) and e.classification == "model-credit-exhausted"
    ]


def test_a_reviewer_402_seen_only_as_the_agent_result_ends_the_turn() -> None:
    """#3935 AC4, the shape the runner actually receives by default.

    Red today on the ErrorEvent: the subagent's own errored message is not
    forwarded, and the translator does not classify the Agent call's is_error
    result text, so no model-credit-exhausted event exists. Red on the Final
    too: the parent's success result maps to DONE.
    """

    events = _tool_call_then_success(_AGENT_402_RESULT)

    errors = [e for e in events if isinstance(e, ErrorEvent)]
    assert [e.classification for e in errors] == ["model-credit-exhausted"]
    finals = [e for e in events if isinstance(e, Final)]
    assert len(finals) == 1
    assert events[-1] is finals[0]
    assert finals[0].status is SessionStatus.CLASSIFIED_FAILURE


def test_a_reviewer_non_credit_failure_seen_as_the_agent_result_stays_done() -> None:
    """Liveness for the default shape: a non-credit subagent failure stays DONE.

    Green today and must stay green.
    """

    events = _tool_call_then_success(
        "Agent terminated early due to an API error: API Error: 529 Overloaded"
    )

    finals = [e for e in events if isinstance(e, Final)]
    assert len(finals) == 1
    assert finals[0].status is SessionStatus.DONE
    assert not [
        e
        for e in events
        if isinstance(e, ErrorEvent) and e.classification == "model-credit-exhausted"
    ]


def test_a_failing_bash_test_mentioning_402_stays_done() -> None:
    """Liveness for #3935: credit text in a non-Agent tool result is not a refusal.

    The factory works on code that handles HTTP 402, so a failing test's output
    names 402 and Payment Required (and here even OpenRouter's own sentence).
    Green today and must stay green: only the Agent/Task call's failed result
    carries a subagent's provider refusal; a Bash failure is the workload's own.
    """

    events = _tool_call_then_success(
        "FAILED tests/test_billing.py::test_low_balance - "
        "pytest: assert response.status == 402, 402 Payment Required: "
        "This request requires more credits, or fewer max_tokens.",
        tool="Bash",
        is_error=True,
    )

    finals = [e for e in events if isinstance(e, Final)]
    assert len(finals) == 1
    assert finals[0].status is SessionStatus.DONE
    assert _credit_errors(events) == []


def test_a_successful_agent_result_mentioning_402_stays_done() -> None:
    """Liveness for #3935: a reviewer that answered may quote 402 in its verdict.

    Green today and must stay green: a successful (is_error None) Agent result
    is the subagent's answer, not a provider refusal, whatever its text says.
    """

    events = _tool_call_then_success(
        "REVIEWER: x\nVERDICT: REQUEST-CHANGES\nThe client treats HTTP 402 Payment "
        'Required ("This request requires more credits") as retryable.',
        is_error=None,
    )

    finals = [e for e in events if isinstance(e, Final)]
    assert len(finals) == 1
    assert finals[0].status is SessionStatus.DONE
    assert _credit_errors(events) == []


def _prior_error_then_agent_402_then_success(prior: object) -> list:
    """An earlier recoverable error, then a reviewer's 402, then the parent's success."""

    state = TurnState()
    call = AssistantMessage(
        content=[
            ToolUseBlock(
                id="toolu_rev1",
                name="Agent",
                input={"description": "Diff review round 1", "subagent_type": "reviewer"},
            )
        ],
        model="m",
    )
    success = ResultMessage(
        subtype="success",
        duration_ms=1,
        duration_api_ms=1,
        is_error=False,
        num_turns=3,
        session_id="s",
        result="The reviewer could not run.",
    )
    return (
        _translate(prior, state)
        + _translate(call, state)
        + _translate(_tool_result("toolu_rev1", _AGENT_402_RESULT, is_error=True), state)
        + _translate(success, state)
    )


@pytest.mark.parametrize(
    ("error", "text"),
    [("unknown", "API Error: 529 Overloaded"), ("server_error", "API Error: 500 upstream")],
)
def test_a_reviewer_402_after_an_earlier_main_thread_error_still_ends_the_turn(
    error: str, text: str
) -> None:
    """#3935: an earlier recovered error must not mask a later credit refusal.

    Red today: the Agent result's credit classification is applied only when
    ``state.error_classification is None``. The main thread's earlier 529 (or
    server error) already set it, so the 402 is swallowed, no
    model-credit-exhausted event is emitted, and the parent's success maps to
    Final DONE.
    """

    prior = AssistantMessage(content=[TextBlock(text=text)], model="m", error=error)
    events = _prior_error_then_agent_402_then_success(prior)

    errors = [e for e in events if isinstance(e, ErrorEvent)]
    assert errors, "expected at least the earlier error"
    assert errors[-1].classification == "model-credit-exhausted"
    finals = [e for e in events if isinstance(e, Final)]
    assert len(finals) == 1
    assert events[-1] is finals[0]
    assert finals[0].status is SessionStatus.CLASSIFIED_FAILURE


def test_a_reviewer_402_after_a_rejected_rate_limit_still_ends_the_turn() -> None:
    """#3935: a rejected rate limit the turn recovered from must not mask a 402.

    Red today for the same reason: the rate limit set
    ``state.error_classification`` to rate-limit first.
    """

    prior = RateLimitEvent(
        rate_limit_info=RateLimitInfo(status="rejected"), uuid="u", session_id="s"
    )
    events = _prior_error_then_agent_402_then_success(prior)

    errors = [e for e in events if isinstance(e, ErrorEvent)]
    assert [e.classification for e in errors] == ["rate-limit", "model-credit-exhausted"]
    finals = [e for e in events if isinstance(e, Final)]
    assert len(finals) == 1
    assert events[-1] is finals[0]
    assert finals[0].status is SessionStatus.CLASSIFIED_FAILURE


def _credit_refusal_then_later_errors_then_result(later: list, result: ResultMessage) -> list:
    """A reviewer's 402, then later recoverable errors, then the parent's result."""

    state = TurnState()
    call = AssistantMessage(
        content=[
            ToolUseBlock(
                id="toolu_rev1",
                name="Agent",
                input={"description": "Diff review round 1", "subagent_type": "reviewer"},
            )
        ],
        model="m",
    )
    events = _translate(call, state)
    events += _translate(_tool_result("toolu_rev1", _AGENT_402_RESULT, is_error=True), state)
    for message in later:
        events += _translate(message, state)
    return events + _translate(result, state)


def _later_rate_limit() -> RateLimitEvent:
    return RateLimitEvent(
        rate_limit_info=RateLimitInfo(status="rejected"), uuid="u2", session_id="s"
    )


def _later_overloaded() -> AssistantMessage:
    return AssistantMessage(
        content=[TextBlock(text="API Error: 529 Overloaded")], model="m", error="unknown"
    )


def _success_result() -> ResultMessage:
    return ResultMessage(
        subtype="success",
        duration_ms=1,
        duration_api_ms=1,
        is_error=False,
        num_turns=4,
        session_id="s",
        result="The reviewer could not run.",
    )


def _error_result() -> ResultMessage:
    return ResultMessage(
        subtype="error_during_execution",
        duration_ms=1,
        duration_api_ms=1,
        is_error=True,
        num_turns=4,
        session_id="s",
        result="execution failed",
    )


@pytest.mark.parametrize(
    ("later", "result"),
    [
        pytest.param(_later_rate_limit, _success_result, id="later-rate-limit"),
        pytest.param(_later_overloaded, _success_result, id="later-overloaded-assistant-error"),
        pytest.param(_later_rate_limit, _error_result, id="later-rate-limit-then-error-result"),
    ],
)
def test_a_later_recoverable_error_does_not_mask_an_earlier_reviewer_402(
    later: object, result: object
) -> None:
    """#3935: the worker keeps the classification of the LAST ErrorEvent it sees.

    After a reviewer credit refusal, a later rejected rate limit or main-thread
    529 must not become the last classification the worker records, and an
    error result as the turn's end must not either.
    """

    events = _credit_refusal_then_later_errors_then_result([later()], result())

    errors = [e for e in events if isinstance(e, ErrorEvent)]
    assert errors, "expected at least the credit refusal"
    assert errors[-1].classification == "model-credit-exhausted"
    finals = [e for e in events if isinstance(e, Final)]
    assert len(finals) == 1
    assert events[-1] is finals[0]
    assert finals[0].status is SessionStatus.CLASSIFIED_FAILURE
    assert events.index(errors[-1]) < events.index(finals[0])
