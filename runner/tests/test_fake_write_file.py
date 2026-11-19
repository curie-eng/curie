"""``[fake:write-file <name> <content>]`` (#5571).

The directive makes the default script emit one built-in ``Write`` call for
``<name>`` in the working directory, with an absolute path as the real CLI
does, and perform the write only when the permission gate let the call run.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import anyio
import pytest
from claude_agent_sdk import ToolResultBlock, ToolUseBlock, UserMessage
from curie_runner.approval import ApprovalGate
from curie_runner.fake import FakeModelSession
from curie_runner.harness.claude.approval import build_can_use_tool

_DIRECTIVE = "[fake:write-file note.txt hello]"


def _turn(gate: ApprovalGate | None, prompt: str = _DIRECTIVE) -> list[Any]:
    fake = FakeModelSession(can_use_tool=None if gate is None else build_can_use_tool(gate))

    async def run() -> list[Any]:
        await fake.query(prompt)
        return [message async for message in fake.receive_turn()]

    return anyio.run(run)


def _write_input(messages: list[Any]) -> dict[str, Any]:
    calls = [
        block
        for message in messages
        for block in getattr(message, "content", []) or []
        if isinstance(block, ToolUseBlock) and block.name == "Write"
    ]
    assert len(calls) == 1
    return dict(calls[0].input)


def _results(messages: list[Any]) -> list[ToolResultBlock]:
    return [
        block
        for message in messages
        if isinstance(message, UserMessage) and isinstance(message.content, list)
        for block in message.content
        if isinstance(block, ToolResultBlock)
    ]


@pytest.fixture
def cwd(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.chdir(tmp_path)
    return tmp_path


def test_an_ungated_write_creates_the_file_once_at_an_absolute_path(cwd: Path) -> None:
    messages = _turn(None)
    assert _write_input(messages) == {"file_path": str(cwd / "note.txt"), "content": "hello"}
    assert (cwd / "note.txt").read_text() == "hello"
    assert [result.is_error for result in _results(messages)] == [False]


def test_a_gated_write_is_held_and_writes_nothing(cwd: Path) -> None:
    gate = ApprovalGate(required=frozenset({"Write"}))
    messages = _turn(gate)
    assert gate.pending_granted_tool == "Write"
    assert gate.pending_granted_arguments == _write_input(messages)
    assert not (cwd / "note.txt").exists()


def test_a_grant_for_the_exact_arguments_writes_once(cwd: Path) -> None:
    arguments = {"file_path": str(cwd / "note.txt"), "content": "hello"}
    gate = ApprovalGate(
        required=frozenset({"Write"}), grant_tool="Write", grant_arguments=arguments
    )
    _turn(gate)
    assert (cwd / "note.txt").read_text() == "hello"
    (cwd / "note.txt").unlink()
    gate.reset()
    _turn(gate)
    assert gate.pending_granted_tool == "Write"
    assert not (cwd / "note.txt").exists()


def test_a_grant_for_another_path_writes_nothing(cwd: Path) -> None:
    elsewhere = {"file_path": str(cwd / "other" / "note.txt"), "content": "hello"}
    gate = ApprovalGate(
        required=frozenset({"Write"}), grant_tool="Write", grant_arguments=elsewhere
    )
    messages = _turn(gate)
    assert not (cwd / "note.txt").exists()
    assert [result.is_error for result in _results(messages)] == [True]


@pytest.mark.parametrize(
    "directive",
    ["[fake:write-file ../escape.txt x]", "[fake:write-file .. x]", "[fake:write-file a/b x]"],
)
def test_a_directive_naming_anything_but_one_file_writes_nothing(cwd: Path, directive: str) -> None:
    messages = _turn(None, directive)
    assert not any(
        isinstance(block, ToolUseBlock) and block.name == "Write"
        for message in messages
        for block in getattr(message, "content", []) or []
    )
    assert sorted(cwd.iterdir()) == []
