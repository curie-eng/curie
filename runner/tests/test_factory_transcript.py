"""Factory capture sees provider messages replay history intentionally omits (#5608)."""

import anyio
from aci_protocol import Event, Final, parse_ndjson_line
from claude_agent_sdk import (
    AssistantMessage,
    ResultMessage,
    TextBlock,
    ToolResultBlock,
    ToolUseBlock,
    UserMessage,
)
from curie_runner import RunTracer, SideEffectClassifier
from curie_runner.factory_transcript import _event_parts, visible_message
from curie_runner.fake import FakeModelSession, default_turn
from curie_runner.session import SessionRunner


def test_archive_projection_preserves_subagent_tool_calls_and_results() -> None:
    call = AssistantMessage(
        content=[ToolUseBlock(id="call-1", name="Bash", input={"command": "cargo test"})],
        model="test",
        parent_tool_use_id="parent-1",
    )
    result = UserMessage(
        content=[ToolResultBlock(tool_use_id="call-1", content="3 passed", is_error=False)],
        parent_tool_use_id="parent-1",
    )
    final = AssistantMessage(content=[TextBlock(text="done")], model="test")
    terminal = ResultMessage(
        subtype="success",
        duration_ms=1,
        duration_api_ms=1,
        is_error=False,
        num_turns=1,
        session_id="test",
        result="done",
        usage={},
    )

    assert visible_message(call) == (
        "message",
        {
            "role": "assistant",
            "content": [
                {
                    "type": "tool_use",
                    "id": "call-1",
                    "name": "Bash",
                    "input": {"command": "cargo test"},
                }
            ],
            "parent_tool_use_id": "parent-1",
        },
    )
    assert visible_message(result)[1]["content"][0]["content"] == "3 passed"
    assert visible_message(final)[1]["content"] == [{"type": "text", "text": "done"}]
    assert visible_message(terminal)[1]["is_error"] is False


def test_oversized_event_is_losslessly_chunked() -> None:
    import json

    original = {"content": "💡" * 900_000}
    parts = _event_parts("message", original)
    assert len(parts) > 1
    assert all(kind == "chunk" for kind, _ in parts)
    assert [part["index"] for _, part in parts] == list(range(len(parts)))
    assert all(part["count"] == len(parts) for _, part in parts)
    restored = json.loads("".join(part["json_fragment"] for _, part in parts))
    assert restored == original


def test_runner_captures_prompt_sdk_messages_and_terminal_frame() -> None:
    class Recorder:
        def __init__(self) -> None:
            self.events: list[tuple[str, dict]] = []

        async def record(self, kind: str, content: dict) -> None:
            self.events.append((kind, content))

    recorder = Recorder()
    runner = SessionRunner(
        session_factory=lambda: FakeModelSession(default_turn),
        ceiling=0,
        max_usd_per_day=None,
        tracer=RunTracer(None),
        classifier=SideEffectClassifier(),
        trace_name="factory-transcript-test",
        held_secrets=frozenset(),
        factory_transcript=recorder,  # type: ignore[arg-type]
    )

    async def go() -> Final:
        await runner.start()
        lines = [
            line
            async for line in runner.run_inbound(
                Event(type="message", text="run tests", user="U", ts="1")
            )
        ]
        final = parse_ndjson_line(lines[-1])
        assert isinstance(final, Final)
        return final

    anyio.run(go)
    kinds = [kind for kind, _ in recorder.events]
    assert kinds[0] == "prompt"
    assert "message" in kinds
    assert "result" in kinds
    assert kinds[-1] == "capture_complete"
