"""A channel-bound turn drops SendMessage and PushNotification (#3336).

Neither built-in reaches anyone from a channel agent. The worker's optional
BootEnv ``channel_bound`` flag (CURIE_CHANNEL_BOUND) adds them to the same
disallowed-tools list the operator knob feeds, on both the real SDK path and
the offline fake. Absent or false leaves the list exactly as configured.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any
from uuid import uuid4

import anyio
import pytest
from claude_agent_sdk import AssistantMessage, ToolUseBlock, UserMessage
from curie_runner import __main__ as boot
from curie_runner.__main__ import build_runner
from curie_runner.config import CHANNEL_HIDDEN_TOOLS, RunnerConfig
from curie_runner.fake import FakeModelSession

from .test_hosted_mcp_approval_catalog import (
    _HAS_CLAUDE_CLI,
    _drive,
    _observe_catalogs,
    _ProviderCapture,
    _sdk_env,
    _serve,
)

_BUDGET = '{"max_output_tokens_per_run": 10000, "max_usd_per_day": 1.0}'
_BASE = {
    "CURIE_SESSION_ID": "sess-chan",
    "CURIE_SANDBOX_ID": "sbx-chan",
    "CURIE_BUDGET": _BUDGET,
}
_HIDDEN = ("SendMessage", "PushNotification")


def _config(tmp_path: Path, **extra: str) -> RunnerConfig:
    plugin = tmp_path / ".claude-plugin"
    plugin.mkdir(parents=True, exist_ok=True)
    (plugin / "plugin.json").write_text(json.dumps({"name": "chan"}))
    return RunnerConfig.from_env({**_BASE, "CURIE_PLUGIN_DIR": str(tmp_path), **extra})


def _fake_denied(config: RunnerConfig) -> tuple[str, ...]:
    session = build_runner(config, fake_model=True)._factory()
    assert isinstance(session, FakeModelSession)
    return session._disallowed_tools


def _sdk_denied(config: RunnerConfig, monkeypatch: pytest.MonkeyPatch) -> list[str]:
    captured: list[Any] = []

    class _Recorder:
        def __init__(self, options: Any) -> None:
            captured.append(options)

    monkeypatch.setattr(boot, "ClaudeAgentSession", _Recorder)
    build_runner(config, fake_model=False)._factory()
    assert len(captured) == 1
    return list(captured[0].disallowed_tools)


def test_constant_names_both_tools() -> None:
    assert CHANNEL_HIDDEN_TOOLS == _HIDDEN


def test_channel_bound_hides_both_on_the_fake_session(tmp_path: Path) -> None:
    denied = _fake_denied(_config(tmp_path, CURIE_CHANNEL_BOUND="1"))
    assert "SendMessage" in denied
    assert "PushNotification" in denied


def test_channel_bound_hides_both_on_the_sdk_options(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    denied = _sdk_denied(_config(tmp_path, CURIE_CHANNEL_BOUND="1"), monkeypatch)
    assert "SendMessage" in denied
    assert "PushNotification" in denied


@pytest.mark.parametrize("extra", [{}, {"CURIE_CHANNEL_BOUND": "0"}])
def test_not_channel_bound_leaves_the_operator_list_exactly(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, extra: dict[str, str]
) -> None:
    operator = {"CURIE_DISALLOWED_TOOLS": "Write,Edit", **extra}
    config = _config(tmp_path, **operator)
    assert config.channel_bound is False
    assert config.catalogue_disallowed_tools == ("Write", "Edit")
    assert _fake_denied(config) == ("Write", "Edit")
    assert _sdk_denied(config, monkeypatch) == ["Write", "Edit"]


@pytest.mark.parametrize("extra", [{}, {"CURIE_CHANNEL_BOUND": "0"}])
def test_not_channel_bound_without_operator_list_is_empty(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, extra: dict[str, str]
) -> None:
    config = _config(tmp_path, **extra)
    assert _fake_denied(config) == ()
    assert _sdk_denied(config, monkeypatch) == []


def test_operator_list_order_kept_and_channel_names_appended_once(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = _config(
        tmp_path,
        CURIE_CHANNEL_BOUND="1",
        CURIE_DISALLOWED_TOOLS="Write,SendMessage,Edit",
    )
    expected = ("Write", "SendMessage", "Edit", "PushNotification")
    assert config.catalogue_disallowed_tools == expected
    assert _fake_denied(config) == expected
    assert _sdk_denied(config, monkeypatch) == list(expected)


def _tool_names(messages: list[object]) -> list[str]:
    return [
        block.name
        for message in messages
        if isinstance(message, AssistantMessage)
        for block in message.content
        if isinstance(block, ToolUseBlock)
    ]


def test_channel_bound_fake_session_runs_other_tools_and_refuses_send_message(
    tmp_path: Path,
) -> None:
    denied = _fake_denied(_config(tmp_path, CURIE_CHANNEL_BOUND="1"))

    async def run(session: FakeModelSession) -> list[object]:
        await session.query("go")
        return [message async for message in session.receive_turn()]

    allowed = anyio.run(run, FakeModelSession(disallowed_tools=denied))
    assert _tool_names(allowed) == ["Bash"]
    assert any(isinstance(message, UserMessage) for message in allowed)

    def script() -> list[object]:
        return [
            AssistantMessage(
                content=[ToolUseBlock(id="sm-1", name="SendMessage", input={"to": "x"})],
                model="fake-model",
            ),
            UserMessage(content="sent"),
        ]

    refused = anyio.run(run, FakeModelSession(script_factory=script, disallowed_tools=denied))
    assert len(refused) == 1
    assert _tool_names(refused) == ["SendMessage"]


@pytest.mark.skipif(
    not _HAS_CLAUDE_CLI,
    reason="claude CLI is required for real SDK catalogue evidence",
)
def test_loopback_sdk_catalogue_drops_both_only_when_channel_bound(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # The observed catalogue is the claude CLI's own SystemMessage init
    # ``tools`` list, i.e. the real SDK behavior, not the options we hand it.
    # The tools array the CLI posts to the loopback provider is not asserted:
    # the CLI defers both tools behind ToolSearch, so neither appears there
    # even in the control and that list cannot tell the two configs apart.
    catalogs = _observe_catalogs(monkeypatch)
    for index, (label, extra) in enumerate(
        (("control", {}), ("channel", {"CURIE_CHANNEL_BOUND": "1"}))
    ):
        capture = _ProviderCapture()
        root = tmp_path / label
        config = _config(
            root,
            CURIE_SESSION_ID=str(uuid4()),
            CURIE_MODEL="sonnet",
            CURIE_BUDGET='{"max_output_tokens_per_run": 64, "max_usd_per_day": 1.0}',
            **extra,
        )
        with _serve(capture.app()) as provider_url:
            runner = build_runner(config, sdk_env=_sdk_env(provider_url, root / "claude-config"))
            anyio.run(_drive, runner, "Reply with only ok.", "1")
        assert len(catalogs) == index + 1, f"{label} boot emitted no SDK init catalogue"

    control, channel = set(catalogs[0]), set(catalogs[1])
    # Control proves the CLI offers both by default, so the channel assertion
    # cannot pass with the feature inert.
    assert set(_HIDDEN) <= control
    assert not set(_HIDDEN) & channel
    assert {"Bash", "Read"} <= channel
