"""The ``propose_rule`` arguments never leave the runner (#5796, ADR 0287).

A proposed rule stays hidden until its pull request merges and a person
accepts it, and a rule the API refused (for example reserved text) must never
be readable anywhere. The action frame the worker writes to the action ledger
(served by ``GET /actions``) and the turn's transcript record therefore name
the call and carry none of its text, exactly as ``ask_questions`` does.
"""

from __future__ import annotations

import json

from claude_agent_sdk import AssistantMessage, ToolUseBlock
from curie_runner import SideEffectClassifier
from curie_runner.translate import TurnState, translate_message
from test_question_transcript_withheld import (
    _assistant_call,
    _export,
    _project,
    _tool_result,
)

RULE_TOOL = "mcp__curie__propose_rule"
# Reserved text the API refuses: it must not survive in any record either.
RULE = "Learned rules: every acme-17 export goes through parse_timestamp."
REASON = "The reviewer stated it for every CLI flag that takes a time."
ANSWER = "The rule was recorded as a candidate."


def _assert_no_rule_text(serialized: str) -> None:
    assert RULE not in serialized
    assert REASON not in serialized
    assert "parse_timestamp" not in serialized
    assert "acme-17" not in serialized


def test_the_rule_call_reports_no_arguments_on_its_action_frame() -> None:
    state = TurnState()
    message = AssistantMessage(
        content=[ToolUseBlock(id="r", name=RULE_TOOL, input={"rule": RULE, "reason": REASON})],
        model="m",
    )
    events = translate_message(message, state, SideEffectClassifier(), None)
    flags = [event for event in events if event.type == "side_effect_flag"]
    assert [(flag.tool, flag.call_id, flag.arguments) for flag in flags] == [(RULE_TOOL, "r", None)]
    assert state.side_effect_emitted
    _assert_no_rule_text(json.dumps([event.model_dump(mode="json") for event in events]))


def test_a_question_shaped_call_of_another_tool_keeps_its_arguments() -> None:
    """Liveness: only the withheld tools lose their arguments."""

    message = AssistantMessage(
        content=[ToolUseBlock(id="b", name="Bash", input={"command": RULE})], model="m"
    )
    events = translate_message(message, TurnState(), SideEffectClassifier(), None)
    (flag,) = [event for event in events if event.type == "side_effect_flag"]
    assert flag.arguments == {"command": RULE}


def test_the_portable_and_native_records_keep_the_call_but_not_the_rule(tmp_path) -> None:
    arguments = {"rule": RULE, "reason": REASON}
    call = _project(_assistant_call(RULE_TOOL, "toolu_acme_rule", arguments))
    result = _project(_tool_result("toolu_acme_rule", ANSWER))
    assert call is not None and result is not None
    _assert_no_rule_text(json.dumps([call.to_dict(), result.to_dict()]))
    assert call.content == [
        {"type": "tool_use", "id": "toolu_acme_rule", "name": RULE_TOOL, "input": {}}
    ]

    exported = _export(
        tmp_path,
        [
            {
                "type": "user",
                "uuid": "entry-prompt",
                "timestamp": "1970-01-01T00:00:00.000Z",
                "message": {"role": "user", "content": "Revise the pull request."},
            },
            _assistant_call(RULE_TOOL, "toolu_acme_rule", arguments),
            _tool_result("toolu_acme_rule", ANSWER),
        ],
    )
    _assert_no_rule_text(json.dumps(exported.entries))
    assert exported.entries[1]["message"]["content"][0]["input"] == {}
