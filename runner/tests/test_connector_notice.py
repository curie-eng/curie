"""The connector-unavailable notice on a DONE reply.

A declared connector that is still unavailable at turn start leads the reply
with one plain notice, but only when that turn called one of the unavailable
connector's tools. Several unavailable connectors never stack several notices,
and a reply that never needed a connector carries none: the failure is logged
for operators instead.
"""

from __future__ import annotations

import json
import logging

import anyio
from aci_protocol import Event
from claude_agent_sdk import AssistantMessage, ToolResultBlock, ToolUseBlock, UserMessage
from curie_runner import RunTracer, SideEffectClassifier
from curie_runner.fake import FakeModelSession, _result
from curie_runner.mcp_tool_capability import ConnectorCapabilityFailure
from curie_runner.session import SessionRunner

EVENT = Event(type="message", text="question", user="U", ts="1")
ANSWER = "Here is the answer."


def _failure(connector: str, reason: str = "probe_failed") -> ConnectorCapabilityFailure:
    return ConnectorCapabilityFailure(
        connector=connector, credential_names=(), reason=reason, attempts=3
    )


def _tool_call(call_id: str, name: str) -> list[object]:
    return [
        AssistantMessage(
            content=[ToolUseBlock(id=call_id, name=name, input={})], model="synthetic"
        ),
        UserMessage(
            content=[ToolResultBlock(tool_use_id=call_id, content="unavailable", is_error=True)]
        ),
    ]


def _reply(script: list[object], failures: tuple[ConnectorCapabilityFailure, ...]) -> str:
    async def exercise() -> str:
        fake = FakeModelSession(lambda: list(script), truncate_on_interrupt=False)
        runner = SessionRunner(
            session_factory=lambda: fake,
            ceiling=0,
            max_usd_per_day=None,
            tracer=RunTracer(None),
            classifier=SideEffectClassifier(),
            trace_name="connector-notice-test",
            held_secrets=frozenset(),
            connector_failures=failures,
        )
        await runner.start()
        try:
            frames = [json.loads(line) async for line in runner.run_turn(EVENT)]
        finally:
            await runner.close()
        assert frames[-1]["status"] == "done"
        text: str = frames[-1]["text"]
        return text

    return anyio.run(exercise)


def test_reply_that_used_no_connector_carries_no_notice(caplog):
    failures = (_failure("github"), _failure("tracker"))

    with caplog.at_level(logging.ERROR):
        text = _reply([_result(text=ANSWER)], failures)

    assert text == ANSWER
    # The operator still sees which connectors are unavailable.
    assert "declared connector capability failed" in caplog.text
    assert "github,tracker" in caplog.text


def test_reply_that_called_unavailable_connectors_carries_one_notice():
    failures = (_failure("github"), _failure("tracker"), _failure("wiki"))
    script = [
        *_tool_call("one", "mcp__github__search"),
        *_tool_call("two", "mcp__tracker__list_items"),
        _result(text=ANSWER),
    ]

    text = _reply(script, failures)

    notice = failures[0].caller_message()
    assert text == f"{notice}\n\n{ANSWER}"
    assert text.count("Connected service features are unavailable") == 1


def test_notice_names_the_failure_of_the_connector_the_turn_called():
    failures = (_failure("github", reason="missing_credential"), _failure("tracker"))
    script = [*_tool_call("one", "mcp__tracker__list_items"), _result(text=ANSWER)]

    text = _reply(script, failures)

    assert text == f"{failures[1].caller_message()}\n\n{ANSWER}"


def test_call_to_an_available_connector_carries_no_notice():
    script = [*_tool_call("one", "mcp__wiki__read_page"), _result(text=ANSWER)]

    assert _reply(script, (_failure("github"),)) == ANSWER
