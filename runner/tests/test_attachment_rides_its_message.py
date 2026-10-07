"""A turn's attachments are announced on the message that carried them (#3691).

The system prompt cannot say which message brought a file. A person who
re-attaches a revised file under the same name composes the system prompt the
thread's first turn already ran under, so a resume that keeps that prompt
leaves the model with nothing new: the same notice it has read since the first
turn, and a new user message with no mention of a file. It answers that no file
is attached.

So the runner names this boot's files on the first prompt it sends, which is the
message the worker booted the sandbox for, and records that prompt as the turn's
user message. A later prompt to the same runner carried no file and names none.
"""

from __future__ import annotations

import json
import typing
from pathlib import Path
from typing import Any

import anyio
import pytest
from aci_protocol import Event, Final, parse_ndjson_line
from curie_runner import __main__ as boot
from curie_runner.__main__ import build_runner
from curie_runner.config import RunnerConfig
from curie_runner.history import TurnRecord

_BUDGET = '{"max_output_tokens_per_run": 10000, "max_usd_per_day": 1.0}'


class _RecordingStore:
    def __init__(self) -> None:
        self.turns: list[TurnRecord] = []

    async def load(self) -> list[TurnRecord]:
        return list(self.turns)

    async def append(self, record: TurnRecord) -> bool:
        self.turns.append(record)
        return record.harness_replay is not None


class _CapturedSession:
    """Stands in for ClaudeAgentSession so the composed options can be read."""

    def __init__(self, options: Any) -> None:
        self.options = options

    async def connect(self) -> None:
        return None

    async def query(self, _text: str) -> None:
        return None

    async def interrupt(self) -> None:
        return None

    async def close(self) -> None:
        return None

    async def receive_turn(self) -> typing.AsyncIterator[Any]:
        if False:
            yield None


def _config(tmp_path: Path) -> RunnerConfig:
    plugin = tmp_path / "plugin"
    (plugin / ".claude-plugin").mkdir(parents=True)
    (plugin / ".claude-plugin" / "plugin.json").write_text(
        json.dumps({"name": "attachment-message-demo", "version": "0.1.0"}),
        encoding="utf-8",
    )
    return RunnerConfig.from_env(
        {
            "CURIE_PLUGIN_DIR": str(plugin),
            "CURIE_SESSION_ID": "s-3691",
            "CURIE_SANDBOX_ID": "b-3691",
            "CURIE_BUDGET": _BUDGET,
        }
    )


def _mount(tmp_path: Path, files: dict[str, bytes]) -> Path:
    mount = tmp_path / "attachments"
    mount.mkdir(parents=True, exist_ok=True)
    for name, payload in files.items():
        (mount / name).write_bytes(payload)
    return mount


def _serve(runner: Any, texts: list[str]) -> list[str]:
    """Drive each text as one turn through the runner; return what it queried."""

    async def go() -> list[str]:
        await runner.start()
        for index, text in enumerate(texts):
            lines = [
                line
                async for line in runner.run_inbound(
                    Event(type="message", text=text, user="U", ts=str(index + 1))
                )
            ]
            assert isinstance(parse_ndjson_line(lines[-1]), Final)
        return list(runner._session.queries)  # noqa: SLF001 -- the sent prompt is the subject

    return anyio.run(go)


def test_the_message_a_file_arrived_with_names_it_by_its_absolute_path(
    tmp_path: Path,
) -> None:
    # revert: announce the files in the system prompt alone -> a revised file
    # re-attached under the same name reaches the model as a bare message, and
    # the agent answers that nothing is attached.
    mount = _mount(tmp_path, {"notes.md": b"# revised\n"})
    store = _RecordingStore()
    runner = build_runner(
        _config(tmp_path),
        fake_model=True,
        history_store=store,
        attachments_path=mount,
    )

    text = "file the revised notes"
    queries = _serve(runner, [text])

    sent = queries[0]
    assert "[platform-sender" in sent, "the query has no platform sender header"
    user_at = sent.index("[user-message")
    end_at = sent.index("[end-user-message")
    assert text in sent[user_at:end_at], "the person's own words must sit inside the user fence"
    attachment = str(mount / "notes.md")
    assert attachment in sent[end_at:], (
        "the message that carried notes.md does not name it after the user "
        "fence, so the model cannot tell this message brought a file"
    )
    (turn,) = store.turns
    assert turn.messages[0].role == "user"
    assert turn.messages[0].content == sent, (
        "the recorded user message is not the prompt the model saw, so a later "
        "replay loses which message carried the file"
    )
    assert turn.user == text


def test_a_later_message_to_the_same_runner_names_no_file(tmp_path: Path) -> None:
    # A warm sandbox keeps its /attachments for the turns after the one it was
    # booted for. Those messages carried nothing, and must not say otherwise.
    mount = _mount(tmp_path, {"notes.md": b"# revised\n"})
    store = _RecordingStore()
    runner = build_runner(
        _config(tmp_path),
        fake_model=True,
        history_store=store,
        attachments_path=mount,
    )

    queries = _serve(runner, ["file the revised notes", "thanks, that is all"])

    later = queries[1]
    user_at = later.index("[user-message")
    end_at = later.index("[end-user-message")
    assert "thanks, that is all" in later[user_at:end_at]
    assert str(mount / "notes.md") not in later
    assert store.turns[1].messages[0].content == later


def test_a_boot_with_no_files_sends_the_message_unchanged(tmp_path: Path) -> None:
    # The overwhelming majority of turns: the chart mounts the emptyDir
    # unconditionally, and an empty mount must leave the prompt byte for byte.
    runner = build_runner(
        _config(tmp_path),
        fake_model=True,
        history_store=_RecordingStore(),
        attachments_path=_mount(tmp_path, {}),
    )

    sent = _serve(runner, ["what can you do?"])[0]
    user_at = sent.index("[user-message")
    end_at = sent.index("[end-user-message")
    assert "what can you do?" in sent[user_at:end_at]
    assert "absolute paths" not in sent


def test_the_system_prompt_does_not_say_the_current_message_carried_them(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # The system prompt stays in force for every turn this sandbox serves, so it
    # may say what is on disk but not which message brought it.
    monkeypatch.setattr(boot, "ClaudeAgentSession", _CapturedSession)
    mount = _mount(tmp_path, {"notes.md": b"# revised\n"})
    runner = build_runner(_config(tmp_path), attachments_path=mount)
    session = runner._factory()  # noqa: SLF001 -- the boot wiring is the subject
    assert isinstance(session, _CapturedSession)

    prompt = session.options.system_prompt or ""
    assert str(mount / "notes.md") in prompt
    assert "message you are answering" not in prompt
