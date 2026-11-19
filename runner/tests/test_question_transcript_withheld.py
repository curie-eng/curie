"""The ``ask_questions`` input never reaches the turn's transcript record (#5785).

ADR 0286 decision 4 keeps question text on the tracker only. The action frame
already withholds the call's arguments; the transcript record the runner
appends carries the same call twice more, in its portable messages and in the
native checkpoint the Claude harness mirrored. Both keep the call (its name and
id, so the tool result still pairs) and drop what it asked. Entries are shaped
like what claude-agent-sdk 0.2.159 (bundled CLI 2.1.281) mirrors for an
in-process MCP tool call and its text result.
"""

from __future__ import annotations

import json
from typing import Any

import anyio
from claude_agent_sdk._internal.message_parser import parse_message
from curie_runner.adapter import (
    ClaudeAgentSession,
    build_options,
    build_structured_resume,
    model_message_to_conversation,
)
from curie_runner.history import HarnessReplayState

QUESTION_TOOL = "mcp__curie__ask_questions"
SUMMARY = "The export drops rows whose region code is lowercase; reproduced with acme-17."
QUESTIONS = [
    "Should lowercase region codes be normalized or rejected?",
    "Is the acme-17 fixture representative of production exports?",
]
POSTED = (
    "The questions are posted on the ticket as round 1 of 3. This session ends now: "
    "stop working, make no further changes and end the turn without opening a pull request."
)


def _assistant_call(name: str, call_id: str, tool_input: dict[str, Any]) -> dict[str, Any]:
    return {
        "type": "assistant",
        "uuid": f"entry-{call_id}",
        "timestamp": "1970-01-01T00:00:00.000Z",
        "message": {
            "id": f"msg_acme_{call_id}",
            "type": "message",
            "role": "assistant",
            "model": "acme-model",
            "content": [
                {
                    "type": "tool_use",
                    "id": call_id,
                    "name": name,
                    "input": tool_input,
                    "caller": {"type": "direct"},
                }
            ],
        },
    }


def _tool_result(call_id: str, text: str) -> dict[str, Any]:
    return {
        "type": "user",
        "uuid": f"entry-result-{call_id}",
        "timestamp": "1970-01-01T00:00:00.000Z",
        "message": {
            "role": "user",
            "content": [
                {
                    "type": "tool_result",
                    "tool_use_id": call_id,
                    "content": [{"type": "text", "text": text}],
                }
            ],
        },
        "toolUseResult": [{"type": "text", "text": text}],
    }


def _question_turn() -> list[dict[str, Any]]:
    return [
        {
            "type": "user",
            "uuid": "entry-prompt",
            "timestamp": "1970-01-01T00:00:00.000Z",
            "message": {"role": "user", "content": "Build the ticket."},
        },
        _assistant_call(
            QUESTION_TOOL, "toolu_acme_ask", {"summary": SUMMARY, "questions": QUESTIONS}
        ),
        _tool_result("toolu_acme_ask", POSTED),
    ]


def _project(entry: dict[str, Any]):
    return model_message_to_conversation(
        parse_message({"type": entry["type"], "message": entry["message"]})
    )


def _export(tmp_path, entries: list[dict[str, Any]]) -> HarnessReplayState:
    resume = build_structured_resume((), curie_session_id="acme-thread", cwd=str(tmp_path))
    assert resume.session_store is not None
    session = ClaudeAgentSession(
        build_options(
            plugins=[],
            model=None,
            system_prompt=None,
            max_turns=2,
            max_budget_usd=1.0,
            resume=resume.resume,
            session_id=resume.session_id,
            session_store=resume.session_store,
        )
    )
    anyio.run(resume.session_store.append, resume.session_key, entries)
    exported = anyio.run(session.export_replay_state)
    assert exported is not None
    return exported


def _assert_no_question_text(serialized: str) -> None:
    assert SUMMARY not in serialized
    for question in QUESTIONS:
        assert question not in serialized
    # No fragment survives either, such as an escaped or split copy.
    assert "acme-17" not in serialized
    assert "lowercase" not in serialized


def test_portable_projection_keeps_the_question_call_but_not_its_text() -> None:
    question_input = {"summary": SUMMARY, "questions": QUESTIONS}
    call = _project(_assistant_call(QUESTION_TOOL, "toolu_acme_ask", question_input))
    result = _project(_tool_result("toolu_acme_ask", POSTED))
    assert call is not None and result is not None

    _assert_no_question_text(json.dumps([call.to_dict(), result.to_dict()]))
    assert call.content == [
        {"type": "tool_use", "id": "toolu_acme_ask", "name": QUESTION_TOOL, "input": {}}
    ]
    assert result.content[0]["tool_use_id"] == "toolu_acme_ask"


def test_native_checkpoint_export_keeps_the_question_call_but_not_its_text(tmp_path) -> None:
    exported = _export(tmp_path, _question_turn())

    assert exported.kind == "checkpoint"
    _assert_no_question_text(json.dumps(exported.entries))
    block = exported.entries[1]["message"]["content"][0]
    assert block == {
        "type": "tool_use",
        "id": "toolu_acme_ask",
        "name": QUESTION_TOOL,
        "input": {},
        "caller": {"type": "direct"},
    }
    assert exported.entries[2]["message"]["content"][0]["tool_use_id"] == "toolu_acme_ask"


def test_withheld_native_and_portable_still_replay_the_native_checkpoint(tmp_path) -> None:
    # Both records withhold the same input, so the exact correspondence the
    # native replay requires still holds and the checkpoint is not set aside.
    native = _question_turn()
    portable = tuple(message for entry in native if (message := _project(entry)) is not None)
    exported = _export(tmp_path, native)

    resume = build_structured_resume(
        portable, curie_session_id="acme-thread", cwd=str(tmp_path), harness_replay=exported
    )
    assert resume.session_store is not None
    restored = anyio.run(resume.session_store.load, resume.session_key)

    assert restored == list(exported.entries)
    _assert_no_question_text(json.dumps(restored))


def test_other_tool_input_stays_verbatim_in_both_records(tmp_path) -> None:
    tool_input = {"command": "pytest -q tests/test_export.py", "description": "Run the tests"}
    call = _assistant_call("Bash", "toolu_acme_bash", tool_input)

    projected = _project(call)
    assert projected is not None
    assert projected.content[0]["input"] == tool_input

    exported = _export(tmp_path, [call, _tool_result("toolu_acme_bash", "3 passed")])
    assert exported.entries[0]["message"]["content"][0]["input"] == tool_input


def test_other_tool_payloads_holding_a_question_shaped_value_stay_verbatim(tmp_path) -> None:
    # Only the question call itself is withheld. Another tool's input, result
    # and structured result keep a value shaped like a question call (a test
    # fixture written to disk, a log line read back) byte for byte.
    nested = {
        "type": "tool_use",
        "id": "toolu_fixture",
        "name": QUESTION_TOOL,
        "input": {"summary": SUMMARY, "questions": QUESTIONS},
    }
    tool_input = {"file_path": "tests/fixture.json", "content": nested}
    call = _assistant_call("Write", "toolu_acme_write", tool_input)
    result = _tool_result("toolu_acme_write", "written")
    result["toolUseResult"] = {
        "type": "create",
        "filePath": "tests/fixture.json",
        "content": nested,
    }
    result["message"]["content"][0]["content"] = [nested]

    exported = _export(tmp_path, [call, result])

    assert exported.entries[0]["message"]["content"][0]["input"] == tool_input
    assert exported.entries[1]["toolUseResult"]["content"] == nested
    assert exported.entries[1]["message"]["content"][0]["content"] == [nested]
