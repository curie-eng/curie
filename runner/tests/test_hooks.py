"""Bundle PreToolUse hook consumption (#272): manifest -> SDK HookMatcher."""

from __future__ import annotations

import asyncio
import json
from pathlib import Path

import anyio
from curie_runner import hooks, load_bundle_hooks
from curie_runner.approval import ApprovalGate, build_approval_hook
from curie_runner.mcp_tool_capability import ConnectorAvailability, ConnectorCapabilityFailure


def _bundle(tmp_path: Path, hooks: object) -> str:
    (tmp_path / ".claude-plugin").mkdir()
    (tmp_path / ".claude-plugin" / "plugin.json").write_text(
        json.dumps({"name": "demo", "hooks": hooks}), encoding="utf-8"
    )
    return str(tmp_path)


def test_no_bundle_or_no_hooks_is_none(tmp_path: Path) -> None:
    assert load_bundle_hooks(None) is None
    assert load_bundle_hooks("") is None
    # A manifest without hooks.
    (tmp_path / ".claude-plugin").mkdir()
    (tmp_path / ".claude-plugin" / "plugin.json").write_text(
        '{"name": "demo"}', encoding="utf-8"
    )
    assert load_bundle_hooks(str(tmp_path)) is None


def test_pretooluse_command_hook_becomes_matcher(tmp_path: Path) -> None:
    plugin_dir = _bundle(
        tmp_path,
        {"PreToolUse": [{"matcher": "Bash", "hooks": [{"type": "command", "command": "true"}]}]},
    )
    hooks = load_bundle_hooks(plugin_dir)
    assert hooks is not None
    matchers = hooks["PreToolUse"]
    assert len(matchers) == 1
    assert matchers[0].matcher == "Bash"
    assert len(matchers[0].hooks) == 1  # one callback wrapping the command(s)


def test_non_pretooluse_events_are_ignored(tmp_path: Path) -> None:
    plugin_dir = _bundle(
        tmp_path,
        {"PostToolUse": [{"hooks": [{"type": "command", "command": "true"}]}]},
    )
    assert load_bundle_hooks(plugin_dir) is None


def test_non_command_hook_actions_are_skipped(tmp_path: Path) -> None:
    plugin_dir = _bundle(
        tmp_path,
        {"PreToolUse": [{"matcher": "Bash", "hooks": [{"type": "someFutureType"}]}]},
    )
    assert load_bundle_hooks(plugin_dir) is None


def test_hook_callback_denies_on_exit_2(tmp_path: Path) -> None:
    plugin_dir = _bundle(
        tmp_path,
        {
            "PreToolUse": [
                {"hooks": [{"type": "command", "command": "echo blocked 1>&2; exit 2"}]}
            ]
        },
    )
    hooks = load_bundle_hooks(plugin_dir)
    assert hooks is not None
    callback = hooks["PreToolUse"][0].hooks[0]

    async def go() -> dict:
        return await callback({"tool_name": "Bash", "tool_input": {}}, "tuid", {})

    out = anyio.run(go)
    decision = out["hookSpecificOutput"]
    assert decision["hookEventName"] == "PreToolUse"
    assert decision["permissionDecision"] == "deny"
    assert "blocked" in decision["permissionDecisionReason"]


def test_hook_callback_allows_on_exit_0(tmp_path: Path) -> None:
    plugin_dir = _bundle(
        tmp_path,
        {"PreToolUse": [{"hooks": [{"type": "command", "command": "exit 0"}]}]},
    )
    hooks = load_bundle_hooks(plugin_dir)
    assert hooks is not None
    callback = hooks["PreToolUse"][0].hooks[0]

    async def go() -> dict:
        return await callback({"tool_name": "Bash", "tool_input": {}}, "tuid", {})

    assert anyio.run(go) == {}


def test_first_denying_command_short_circuits(tmp_path: Path) -> None:
    plugin_dir = _bundle(
        tmp_path,
        {
            "PreToolUse": [
                {
                    "hooks": [
                        {"type": "command", "command": "exit 0"},
                        {"type": "command", "command": "echo no 1>&2; exit 2"},
                    ]
                }
            ]
        },
    )
    hooks = load_bundle_hooks(plugin_dir)
    assert hooks is not None
    callback = hooks["PreToolUse"][0].hooks[0]

    async def go() -> dict:
        return await callback({"tool_name": "Bash", "tool_input": {}}, "tuid", {})

    out = anyio.run(go)
    assert out["hookSpecificOutput"]["permissionDecision"] == "deny"


def test_command_hook_kills_child_on_timeout(monkeypatch) -> None:
    """A hook command that outlives the timeout must not orphan its shell child."""

    monkeypatch.setattr(hooks, "_HOOK_TIMEOUT_S", 0.2)

    spawned: list[asyncio.subprocess.Process] = []
    real_create_subprocess_exec = asyncio.create_subprocess_exec

    async def spy_create_subprocess_exec(*args, **kwargs):
        proc = await real_create_subprocess_exec(*args, **kwargs)
        spawned.append(proc)
        return proc

    monkeypatch.setattr(hooks.asyncio, "create_subprocess_exec", spy_create_subprocess_exec)

    async def go() -> dict:
        return await hooks._run_command_hook("sleep 30", {"tool_name": "Bash", "tool_input": {}})

    result = anyio.run(go)

    output = result["hookSpecificOutput"]
    assert "permissionDecision" not in output
    assert "failed to run" in output["additionalContext"]

    assert len(spawned) == 1
    proc = spawned[0]
    assert proc.returncode is not None, "timed-out hook child was left running (orphaned)"


def _gated_callback(gate: ApprovalGate, availability: ConnectorAvailability):
    """The one PreToolUse callback, composed exactly as ``build_runner`` does."""

    composed = hooks.build_gated_pre_tool_use_hooks(build_approval_hook(gate), availability)
    assert composed is not None
    (matcher,) = composed["PreToolUse"]
    assert matcher.matcher is None
    (callback,) = matcher.hooks
    return callback


def _decide(callback, tool_name: str) -> dict:
    return anyio.run(callback, {"tool_name": tool_name, "tool_input": {}}, "tuid", None)


_GITHUB_FAILURE = ConnectorCapabilityFailure(
    connector="github", credential_names=("GITHUB_TOKEN",), reason="probe_failed"
)
_GATED = "mcp__github__create_issue"


def test_excluded_gated_connector_tool_is_denied_before_approval_state() -> None:
    # #2634: the exclusion fronts the approval hook, so an excluded gated tool
    # records no pending approval, requests no stop, and spends no grant.
    gate = ApprovalGate(required=frozenset({_GATED}), grant_tool=_GATED)
    availability = ConnectorAvailability((_GITHUB_FAILURE,))
    callback = _gated_callback(gate, availability)

    denied = _decide(callback, _GATED)

    assert denied == {
        "hookSpecificOutput": {
            "hookEventName": "PreToolUse",
            "permissionDecision": "deny",
            "permissionDecisionReason": _GITHUB_FAILURE.caller_message(),
        }
    }
    assert "continue_" not in denied
    assert gate.pending_summary is None
    assert gate.pending_halt is False
    assert gate.grant_tool == _GATED


def test_recovered_gated_connector_tool_takes_the_normal_approval_path() -> None:
    availability = ConnectorAvailability((_GITHUB_FAILURE,))

    granted_gate = ApprovalGate(required=frozenset({_GATED}), grant_tool=_GATED)
    granted = _gated_callback(granted_gate, availability)
    assert _decide(granted, _GATED)["hookSpecificOutput"]["permissionDecision"] == "deny"
    availability.failures = ()
    allowed = _decide(granted, _GATED)
    assert allowed["hookSpecificOutput"]["permissionDecision"] == "allow"
    assert granted_gate.grant_tool is None

    blocked_gate = ApprovalGate(required=frozenset({_GATED}))
    blocked = _decide(_gated_callback(blocked_gate, availability), _GATED)
    assert blocked["hookSpecificOutput"]["permissionDecision"] == "deny"
    assert blocked["continue_"] is False
    assert blocked_gate.pending_halt is True
    assert blocked_gate.pending_granted_tool == _GATED


def test_ungated_tools_pass_through_the_composed_hook() -> None:
    gate = ApprovalGate(required=frozenset({_GATED}))
    availability = ConnectorAvailability((_GITHUB_FAILURE,))
    callback = _gated_callback(gate, availability)

    assert _decide(callback, "Bash") == {}
    assert _decide(callback, "mcp__githubenterprise__search") == {}
    assert _decide(callback, "mcp__linear__search") == {}
    assert gate.pending_summary is None
