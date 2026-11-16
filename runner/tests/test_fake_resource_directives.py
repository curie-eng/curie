"""``[fake:hold-memory <MiB>]`` and ``[fake:fill-scratch <MiB>]`` (#5427).

Both directives exhaust a runner resource offline: memory inside the runner
process, scratch as one file in the temporary directory (the sandbox's /tmp
scratch volume; the runner's root filesystem is read-only). Neither emits a tool
call, so the turn carries no side effect, and both release what they took.
"""

from __future__ import annotations

import tempfile
from pathlib import Path
from typing import Any

import anyio
import pytest
from claude_agent_sdk import AssistantMessage, ResultMessage, TextBlock, ToolUseBlock
from curie_runner import fake
from curie_runner.fake import FakeModelSession


def _turn(prompt: str) -> list[Any]:
    session = FakeModelSession()

    async def run() -> list[Any]:
        await session.query(prompt)
        return [message async for message in session.receive_turn()]

    return anyio.run(run)


def _texts(messages: list[Any]) -> list[str]:
    return [
        block.text
        for message in messages
        if isinstance(message, AssistantMessage)
        for block in message.content
        if isinstance(block, TextBlock)
    ]


def _tool_calls(messages: list[Any]) -> list[ToolUseBlock]:
    return [
        block
        for message in messages
        if isinstance(message, AssistantMessage)
        for block in message.content
        if isinstance(block, ToolUseBlock)
    ]


@pytest.fixture(autouse=True)
def short_holds(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(fake, "HOLD_MEMORY_SECONDS", 0.0)
    monkeypatch.setattr(fake, "FILL_SCRATCH_SECONDS", 0.0)
    monkeypatch.delenv(fake.STANDING_DIRECTIVE_ENV, raising=False)


@pytest.fixture
def cwd(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    scratch = tmp_path / "tmp"
    scratch.mkdir()
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(tempfile, "tempdir", str(scratch))
    return scratch


def test_hold_memory_touches_the_requested_size_and_answers_without_a_tool(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    held: list[int] = []
    touch = fake._touch_memory

    def recording(mib: int) -> bytearray:
        buffer = touch(mib)
        held.append(len(buffer))
        assert all(buffer[offset] == 1 for offset in range(0, len(buffer), fake._PAGE))
        return buffer

    monkeypatch.setattr(fake, "_touch_memory", recording)
    messages = _turn("[fake:hold-memory 2]")
    assert held == [2 * 1024 * 1024]
    assert _tool_calls(messages) == []
    assert _texts(messages) == ["Holding memory", "all done"]
    assert isinstance(messages[-1], ResultMessage)
    assert not messages[-1].is_error


def test_fill_scratch_writes_one_file_in_the_temporary_directory_then_deletes_it(
    cwd: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    seen: list[int] = []

    async def observe(seconds: float) -> None:
        path = cwd / fake.FILL_SCRATCH_FILE
        seen.append(path.stat().st_size)

    monkeypatch.setattr(fake.anyio, "sleep", observe)
    messages = _turn("[fake:fill-scratch 3]")
    assert seen == [3 * 1024 * 1024]
    assert list(cwd.iterdir()) == []
    assert _tool_calls(messages) == []
    assert _texts(messages) == ["Filling scratch", "all done"]
    assert isinstance(messages[-1], ResultMessage)


def test_fill_scratch_deletes_its_file_when_the_turn_is_cancelled(
    cwd: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(fake, "FILL_SCRATCH_SECONDS", 60.0)
    session = FakeModelSession()
    observed: list[bool] = []

    async def run() -> None:
        await session.query("[fake:fill-scratch 1]")
        with anyio.move_on_after(1.0):
            async for _ in session.receive_turn():
                pass
        observed.append((cwd / fake.FILL_SCRATCH_FILE).exists())

    anyio.run(run)
    assert observed == [False]


@pytest.mark.parametrize(
    "directive", ["[fake:hold-memory 0]", "[fake:hold-memory 16385]", "[fake:fill-scratch x]"]
)
def test_an_out_of_range_or_unparsable_size_falls_back_to_the_default_turn(
    cwd: Path, directive: str
) -> None:
    messages = _turn(directive)
    assert [call.name for call in _tool_calls(messages)] == ["Bash"]
    assert list(cwd.iterdir()) == []


def test_the_standing_directive_applies_to_a_turn_whose_text_is_only_a_link(
    cwd: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv(fake.STANDING_DIRECTIVE_ENV, "[fake:fill-scratch 1]")
    messages = _turn("https://forge.example/acme/widgets/issues/7")
    assert _texts(messages) == ["Filling scratch", "all done"]
    assert list(cwd.iterdir()) == []


def test_without_a_standing_directive_a_link_gets_the_default_turn() -> None:
    messages = _turn("https://forge.example/acme/widgets/issues/7")
    assert [call.name for call in _tool_calls(messages)] == ["Bash"]
