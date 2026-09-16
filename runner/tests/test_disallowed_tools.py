"""CURIE_DISALLOWED_TOOLS is the deployed deny list for persistent writes (#2429).

The knob is runner-local, not a BootEnv contract field. Unset preserves the
historical empty SDK list. When set, the names are handed to
ClaudeAgentOptions.disallowed_tools and the fake session refuses the same names
so an attempted write cannot execute offline.
"""

from __future__ import annotations

import json
from pathlib import Path

import anyio
from claude_agent_sdk import AssistantMessage, ToolUseBlock, UserMessage
from curie_runner.__main__ import build_runner
from curie_runner.adapter import build_options
from curie_runner.config import RunnerConfig
from curie_runner.fake import FakeModelSession

_BUDGET = '{"max_output_tokens_per_run": 10000, "max_usd_per_day": 1.0}'
_BASE = {
    "CURIE_PLUGIN_DIR": "/bundle",
    "CURIE_SESSION_ID": "sess-deny",
    "CURIE_SANDBOX_ID": "sbx-deny",
    "CURIE_BUDGET": _BUDGET,
}


def test_disallowed_tools_unset_is_empty() -> None:
    assert RunnerConfig.from_env(dict(_BASE)).disallowed_tools == ()


def test_disallowed_tools_parses_comma_list_and_drops_blanks() -> None:
    config = RunnerConfig.from_env(
        {
            **_BASE,
            "CURIE_DISALLOWED_TOOLS": (
                "mcp__curie-state__append, mcp__curie-state__set ,"
                "mcp__curie-state__delete,Write,Edit,"
            ),
        }
    )
    assert config.disallowed_tools == (
        "mcp__curie-state__append",
        "mcp__curie-state__set",
        "mcp__curie-state__delete",
        "Write",
        "Edit",
    )


def test_disallowed_tools_whitespace_only_is_empty() -> None:
    config = RunnerConfig.from_env({**_BASE, "CURIE_DISALLOWED_TOOLS": "  , , "})
    assert config.disallowed_tools == ()


def test_build_options_carries_disallowed_tools() -> None:
    denied = ["mcp__curie-state__append", "Write"]
    options = build_options(
        plugins=[],
        model=None,
        system_prompt=None,
        max_turns=20,
        max_budget_usd=1.0,
        resume=None,
        policy_disallowed_tools=("Write", "mcp__z-policy__deny", "mcp__a-policy__deny"),
        disallowed_tools=denied,
    )
    assert options.disallowed_tools == [
        *denied,
        "mcp__a-policy__deny",
        "mcp__z-policy__deny",
    ]
    empty = build_options(
        plugins=[],
        model=None,
        system_prompt=None,
        max_turns=20,
        max_budget_usd=1.0,
        resume=None,
    )
    assert empty.disallowed_tools == []


def test_fake_session_refuses_a_disallowed_write_and_does_not_execute() -> None:
    def script() -> list[object]:
        return [
            AssistantMessage(
                content=[
                    ToolUseBlock(
                        id="append-1",
                        name="mcp__curie-state__append",
                        input={"namespace": "notes", "key": "log", "item": "x"},
                    )
                ],
                model="fake-model",
            ),
            UserMessage(
                content="appended",
            ),
        ]

    session = FakeModelSession(
        script_factory=script,
        disallowed_tools=("mcp__curie-state__append", "Write"),
    )

    async def go() -> list[object]:
        await session.query("append persistent state")
        return [message async for message in session.receive_turn()]

    messages = anyio.run(go)
    assert len(messages) == 1
    assert isinstance(messages[0], AssistantMessage)
    block = messages[0].content[0]
    assert isinstance(block, ToolUseBlock)
    assert block.name == "mcp__curie-state__append"


def test_fake_session_still_executes_tools_outside_the_deny_list() -> None:
    session = FakeModelSession(
        disallowed_tools=("mcp__curie-state__append", "Write"),
    )

    async def go() -> list[object]:
        await session.query("hello")
        return [message async for message in session.receive_turn()]

    messages = anyio.run(go)
    names = [
        block.name
        for message in messages
        if isinstance(message, AssistantMessage)
        for block in message.content
        if isinstance(block, ToolUseBlock)
    ]
    assert names == ["Bash"]
    assert any(isinstance(message, UserMessage) for message in messages)


def test_build_runner_wires_disallowed_tools_onto_the_fake_session(
    tmp_path: Path,
) -> None:
    plugin = tmp_path / ".claude-plugin"
    plugin.mkdir(parents=True)
    (plugin / "plugin.json").write_text(json.dumps({"name": "deny-writes"}))
    config = RunnerConfig.from_env(
        {
            "CURIE_PLUGIN_DIR": str(tmp_path),
            "CURIE_SESSION_ID": "s-deny",
            "CURIE_SANDBOX_ID": "b-deny",
            "CURIE_BUDGET": _BUDGET,
            "CURIE_DISALLOWED_TOOLS": "mcp__curie-state__append,Write",
        }
    )
    runner = build_runner(config, fake_model=True)
    session = runner._factory()
    assert isinstance(session, FakeModelSession)
    assert session._disallowed_tools == ("mcp__curie-state__append", "Write")
