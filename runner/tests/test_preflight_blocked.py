"""A blocked declared check stops a factory run before any model round (#3873).

A declared check whose probe is unavailable and that declares no
``delegated_to`` is blocked. The runner then never builds the model session:
every turn, including the worker's early-stop continuation (#3128), answers
with the same ``Could not complete:`` explanation, makes no tool call, and
reports a zero-token implementer usage row for the configured model.

These boot the real ``build_runner`` with real subprocess probes and drive the
turns through ``SessionRunner.run_turn``. The progress server records both the
``/verification`` and the ``/usage`` POSTs.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import anyio
import pytest
from claude_agent_sdk import AssistantMessage, ResultMessage, TextBlock, ToolUseBlock

from .test_factory_preflight import (
    _BLOCKED_PREFIX,
    _EARLY_STOP_CONTINUATION,
    _ISSUE_TEXT,
    _MODEL,
    _TOKEN,
    _assert_blocked_turn,
    _blocked_first_turn,
    _Boot,
    _booted,
    _executable,
    _names,
    _record,
    _turn,
    _zero_usage_entry,
)

_UNITS_RS: dict[str, Any] = {
    "id": "units_rs",
    "paths": ["**/*.rs", "Cargo.lock"],
    "command": ["cargo", "test", "--locked"],
    "install": ["cargo", "fetch", "--locked"],
}
_UNITS_PY: dict[str, Any] = {
    "id": "units_py",
    "paths": ["unitconv/**/*.py"],
    "command": ["unitconv-check", "--fast"],
}


def _cargo_without_registry(bindir: Path, order: Path) -> Path:
    """A ``cargo`` whose locked fetch cannot reach the package registry."""

    return _executable(
        bindir,
        "cargo",
        f"printf '%s\\n' \"$*\" >> '{order}'\n"
        'if [ "$1" = "fetch" ]; then\n'
        "  printf '%s\\n' 'error: failed to download `serde v1.0.210`' >&2\n"
        "  exit 101\n"
        "fi\n"
        "exit 0\n",
    )


# --- R-T1: the locked cargo fetch cannot reach the registry ---------------------------


def test_locked_fetch_without_registry_answers_every_turn_without_a_model(
    tmp_path: Path, monkeypatch: Any
) -> None:
    order = tmp_path / "order"
    bindir = _cargo_without_registry(tmp_path / "bin", order)
    bundle = {"lockfile_installs": True, "checks": [_UNITS_RS]}

    async def run() -> tuple[_Boot, Any, Any]:
        async with _booted(
            tmp_path, monkeypatch, path=str(bindir), bundle=bundle, model=_MODEL
        ) as booted:
            first = _assert_blocked_turn(await _turn(booted.runner, _ISSUE_TEXT))
            assert "model_started" not in booted.events
            second = _assert_blocked_turn(await _turn(booted.runner, _EARLY_STOP_CONTINUATION))
            assert "model_started" not in booted.events
            assert booted.runner is not None
            await booted.runner.reset()
            assert "model_started" not in booted.events
            return booted, first, second

    booted, first, second = anyio.run(run)

    # The declared locked install really ran, and the check command never did.
    assert order.read_text(encoding="utf-8").splitlines() == ["fetch --locked"]
    assert booted.received == [
        (
            _record(
                check="units_rs",
                command="cargo test --locked",
                outcome="unavailable",
                exit_status=None,
                blocked=["package_registry"],
            ),
            _TOKEN,
        )
    ]
    assert "model_started" not in booted.events
    assert booted.events == ["verification_posted", "usage_posted", "usage_posted"]

    text = first.text
    assert text.startswith(_BLOCKED_PREFIX)
    assert _names(text, "units_rs")
    assert _names(text, "package_registry")
    assert "declared install did not complete" in text
    assert "delegated_to" in text
    assert len(text) <= 4000
    # No process output, declared command, or install argv leaks into the text.
    for leaked in ("serde", "failed to download", "cargo test", "cargo fetch", "**/*.rs"):
        assert leaked not in text
    assert second.text == text

    # One zero implementer row per turn for the configured model.
    assert len(booted.usage) == 2
    for body, token in booted.usage:
        assert token == _TOKEN
        assert body["primary_model"] == _MODEL
        assert body["models"] == [_zero_usage_entry(_MODEL)]
    turn_ids = [body["turn_id"] for body, _ in booted.usage]
    assert len(set(turn_ids)) == 2 and all(turn_ids)


# --- R-T2: a declared program is missing ----------------------------------------------


def test_missing_declared_program_stops_before_the_model_without_naming_it(
    tmp_path: Path, monkeypatch: Any
) -> None:
    empty = tmp_path / "empty-bin"
    empty.mkdir()

    booted, final = _blocked_first_turn(
        tmp_path,
        monkeypatch,
        path=str(empty),
        bundle={"checks": [_UNITS_PY]},
        model=_MODEL,
    )

    assert [body for body, _ in booted.received] == [
        _record(
            check="units_py",
            command="unitconv-check --fast",
            outcome="unavailable",
            exit_status=None,
            missing=["unitconv-check"],
        )
    ]
    assert "a program declared by check units_py" in final.text
    assert "unitconv-check" not in final.text
    assert "--fast" not in final.text
    assert "unitconv/**" not in final.text
    assert [body["models"] for body, _ in booted.usage] == [[_zero_usage_entry(_MODEL)]]


# --- R-T6: one passed check does not rescue a blocked one ------------------------------


def test_passed_check_beside_a_blocked_check_still_stops_and_names_only_the_blocked(
    tmp_path: Path, monkeypatch: Any
) -> None:
    bindir = _executable(tmp_path / "bin", "units-check", "exit 0\n")
    passing = {"id": "units_ok", "paths": ["unitconv/**/*.py"], "command": ["units-check"]}
    blocked = {"id": "units_rs", "paths": ["**/*.rs"], "command": ["cargo", "test"]}

    booted, final = _blocked_first_turn(
        tmp_path,
        monkeypatch,
        path=str(bindir),
        bundle={"checks": [passing, blocked]},
        model=_MODEL,
    )

    assert [body["outcome"] for body, _ in booted.received] == ["passed", "unavailable"]
    assert _names(final.text, "units_rs")
    assert _names(final.text, "cargo")
    assert not _names(final.text, "units_ok")


def test_every_blocked_check_is_listed(tmp_path: Path, monkeypatch: Any) -> None:
    empty = tmp_path / "empty-bin"
    empty.mkdir()
    rust = {"id": "units_rs", "paths": ["**/*.rs"], "command": ["cargo", "test"]}

    booted, final = _blocked_first_turn(
        tmp_path,
        monkeypatch,
        path=str(empty),
        bundle={"checks": [_UNITS_PY, rust]},
        model=_MODEL,
    )

    assert [body["outcome"] for body, _ in booted.received] == ["unavailable", "unavailable"]
    assert _names(final.text, "units_py")
    assert _names(final.text, "units_rs")
    assert final.text.count(_BLOCKED_PREFIX) == 1


def test_a_delegated_check_beside_a_blocked_check_does_not_rescue_the_run(
    tmp_path: Path, monkeypatch: Any
) -> None:
    empty = tmp_path / "empty-bin"
    empty.mkdir()
    delegated = {
        "id": "integration",
        "paths": ["unitconv/**"],
        "command": ["units-integration"],
        "delegated_to": "integration-tests",
    }

    booted, final = _blocked_first_turn(
        tmp_path,
        monkeypatch,
        path=str(empty),
        bundle={"checks": [delegated, _UNITS_PY]},
        model=_MODEL,
    )

    assert [body.get("delegated_to") for body, _ in booted.received] == [
        "integration-tests",
        None,
    ]
    assert _names(final.text, "units_py")
    assert not _names(final.text, "integration")
    assert "integration-tests" not in final.text


# --- R-T7: a rejected report still fails boot closed ----------------------------------


def test_rejected_report_of_a_blocked_check_fails_boot_before_any_session(
    tmp_path: Path, monkeypatch: Any
) -> None:
    empty = tmp_path / "empty-bin"
    empty.mkdir()
    observed = _Boot()

    async def run() -> None:
        async with _booted(
            tmp_path,
            monkeypatch,
            path=str(empty),
            bundle={"checks": [_UNITS_PY]},
            report_status=409,
            model=_MODEL,
            into=observed,
        ):
            pytest.fail("boot must not complete after a rejected report")

    with pytest.raises(RuntimeError, match="preflight report was not accepted"):
        anyio.run(run)

    assert observed.runner is None
    assert observed.events == ["verification_posted"]
    assert observed.usage == []


# --- R-T9: no configured model ---------------------------------------------------------


def test_without_a_configured_model_the_explanation_is_delivered_and_no_usage_posted(
    tmp_path: Path, monkeypatch: Any
) -> None:
    empty = tmp_path / "empty-bin"
    empty.mkdir()

    booted, final = _blocked_first_turn(
        tmp_path, monkeypatch, path=str(empty), bundle={"checks": [_UNITS_PY]}
    )

    assert _names(final.text, "units_py")
    # Honest "not reported": no model name, so no usage row is invented.
    assert booted.usage == []
    assert "usage_posted" not in booted.events


# --- the offline session itself ---------------------------------------------------------


async def _replay(session: Any, prompt: str) -> list[Any]:
    await session.query(prompt)
    return [message async for message in session.receive_turn()]


def test_blocked_session_answers_each_turn_with_the_text_and_zero_usage() -> None:
    from curie_runner.harness.claude.preflight_blocked import PreflightBlockedSession

    text = f"{_BLOCKED_PREFIX} Check units_rs is blocked."

    async def run() -> list[list[Any]]:
        session = PreflightBlockedSession(text, model=_MODEL)
        await session.connect()
        turns = [
            await _replay(session, _ISSUE_TEXT),
            await _replay(session, _EARLY_STOP_CONTINUATION),
        ]
        await session.close()
        return turns

    turns = anyio.run(run)

    zero = {"input_tokens": 0, "output_tokens": 0}
    result_ids = []
    for messages in turns:
        assert [type(message) for message in messages] == [AssistantMessage, ResultMessage]
        assistant, result = messages
        assert assistant.content == [TextBlock(text=text)]
        assert not any(isinstance(block, ToolUseBlock) for block in assistant.content)
        assert assistant.model == _MODEL
        assert assistant.usage == zero
        assert result.subtype == "success"
        assert result.is_error is False
        assert result.result == text
        assert result.usage == zero
        assert not result.model_usage
        result_ids.append(result.uuid)
    assert len(set(result_ids)) == 2 and all(result_ids)


def test_blocked_session_has_no_model_options() -> None:
    from curie_runner.harness.claude.preflight_blocked import PreflightBlockedSession

    session = PreflightBlockedSession(_BLOCKED_PREFIX, model=_MODEL)

    assert getattr(session, "options", None) is None


def test_blocked_session_without_a_model_names_no_model() -> None:
    from curie_runner.harness.claude.preflight_blocked import PreflightBlockedSession

    async def run() -> list[Any]:
        session = PreflightBlockedSession(_BLOCKED_PREFIX, model=None)
        await session.connect()
        return await _replay(session, _ISSUE_TEXT)

    assistant = anyio.run(run)[0]

    assert isinstance(assistant, AssistantMessage)
    assert assistant.model == ""


def test_interrupt_empties_the_next_replay() -> None:
    from curie_runner.harness.claude.preflight_blocked import PreflightBlockedSession

    async def run() -> list[Any]:
        session = PreflightBlockedSession(_BLOCKED_PREFIX, model=_MODEL)
        await session.connect()
        await session.query(_ISSUE_TEXT)
        await session.interrupt()
        return [message async for message in session.receive_turn()]

    messages = anyio.run(run)

    assert not any(isinstance(message, AssistantMessage) for message in messages)
