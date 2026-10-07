"""Bundle PreToolUse hook consumption (#272): manifest -> SDK HookMatcher."""

from __future__ import annotations

import json
import os
import shlex
import signal
import time
from pathlib import Path

import anyio
import pytest
from curie_runner import hooks, load_bundle_hooks
from curie_runner.__main__ import build_runner
from curie_runner.approval import ApprovalGate
from curie_runner.config import RunnerConfig
from curie_runner.harness.claude.approval import build_approval_hook
from curie_runner.mcp_tool_capability import (
    ConnectorAvailability,
    ConnectorCapabilityFailure,
    McpToolCapabilityProbe,
)
from curie_runner.progress import PROGRESS_TOKEN_ENV, PROGRESS_URL_ENV


def _factory_session_options(tmp_path, monkeypatch, *, progress_url=None, progress_token=None):
    monkeypatch.delenv(PROGRESS_URL_ENV, raising=False)
    monkeypatch.delenv(PROGRESS_TOKEN_ENV, raising=False)
    if progress_url is not None:
        monkeypatch.setenv(PROGRESS_URL_ENV, progress_url)
    if progress_token is not None:
        monkeypatch.setenv(PROGRESS_TOKEN_ENV, progress_token)

    plugin = tmp_path / ".claude-plugin"
    plugin.mkdir()
    (plugin / "plugin.json").write_text(
        json.dumps({"name": "factory-hook-wiring"}), encoding="utf-8"
    )
    config = RunnerConfig.from_env(
        {
            "CURIE_PLUGIN_DIR": str(tmp_path),
            "CURIE_SESSION_ID": "s-factory-hooks",
            "CURIE_SANDBOX_ID": "b-factory-hooks",
            "CURIE_BUDGET": '{"max_output_tokens_per_run": 10000, "max_usd_per_day": 1.0}',
        }
    )
    runner = build_runner(
        config,
        mcp_capability=McpToolCapabilityProbe(
            complete=True,
            has_potential_write_tool=False,
            tool_count=0,
        ),
    )
    return runner._factory()._options


def test_factory_progress_credentials_wire_foreground_guard_into_session_options(
    tmp_path, monkeypatch
) -> None:
    options = _factory_session_options(
        tmp_path,
        monkeypatch,
        progress_url="http://progress.example/v1/work-item-progress/test",
        progress_token="test-progress-token",
    )

    assert options.hooks is not None
    matcher = next(
        matcher
        for matcher in options.hooks["PreToolUse"]
        if matcher.matcher == "Bash|Agent|Task"
    )
    (callback,) = matcher.hooks
    denied = anyio.run(
        callback,
        {"tool_name": "Bash", "tool_input": {"command": "cargo build", "run_in_background": True}},
        "tuid",
        None,
    )
    assert denied["hookSpecificOutput"]["permissionDecision"] == "deny"


@pytest.mark.parametrize(
    ("progress_url", "progress_token"),
    [
        pytest.param(None, "test-progress-token", id="missing-url"),
        pytest.param(
            "http://progress.example/v1/work-item-progress/test", None, id="missing-token"
        ),
    ],
)
def test_factory_foreground_guard_is_absent_without_both_progress_credentials(
    tmp_path, monkeypatch, progress_url, progress_token
) -> None:
    options = _factory_session_options(
        tmp_path,
        monkeypatch,
        progress_url=progress_url,
        progress_token=progress_token,
    )

    # Only the per-turn tool access front (RUNNER-TOOL-ACCESS-2), which every
    # session carries; no factory foreground guard.
    assert options.hooks is not None
    (matcher,) = options.hooks["PreToolUse"]
    assert matcher.matcher is None
    assert [callback.__qualname__ for callback in matcher.hooks] == [
        "front_pre_tool_use_hooks.<locals>.front"
    ]


@pytest.mark.parametrize(
    ("tool_name", "run_in_background", "expected"),
    [
        pytest.param("Bash", True, "deny", id="background-bash"),
        pytest.param("Bash", False, "allow", id="foreground-bash"),
        pytest.param("Bash", None, "allow", id="bash-defaults-to-foreground"),
        pytest.param("Agent", True, "deny", id="background-agent"),
        pytest.param("Agent", None, "deny", id="agent-background-default"),
        pytest.param("Agent", False, "allow", id="foreground-agent"),
        pytest.param("Task", True, "deny", id="background-task"),
        pytest.param("Task", None, "deny", id="task-background-default"),
        pytest.param("Task", False, "allow", id="foreground-task"),
    ],
)
def test_factory_foreground_guard_decides_from_explicit_background_setting(
    tool_name: str, run_in_background: bool | None, expected: str
) -> None:
    (matcher,) = hooks.build_factory_foreground_hooks()["PreToolUse"]
    assert matcher.matcher == "Bash|Agent|Task"
    (callback,) = matcher.hooks
    tool_input = {"command": "cargo build --locked"}
    if run_in_background is not None:
        tool_input["run_in_background"] = run_in_background

    out = anyio.run(
        callback,
        {"tool_name": tool_name, "tool_input": tool_input},
        "tuid",
        None,
    )

    decision = out.get("hookSpecificOutput", {}).get("permissionDecision", "allow")
    assert decision == expected
    if expected == "deny":
        reason = out["hookSpecificOutput"]["permissionDecisionReason"].lower()
        assert "foreground" in reason
        assert "end your turn" in reason


def _bundle(tmp_path: Path, hooks: object) -> str:
    (tmp_path / ".claude-plugin").mkdir(parents=True)
    (tmp_path / ".claude-plugin" / "plugin.json").write_text(
        json.dumps({"name": "demo", "hooks": hooks}), encoding="utf-8"
    )
    return str(tmp_path)


def test_no_bundle_or_no_hooks_is_none(tmp_path: Path) -> None:
    assert load_bundle_hooks(None) is None
    assert load_bundle_hooks("") is None
    # A manifest without hooks.
    (tmp_path / ".claude-plugin").mkdir()
    (tmp_path / ".claude-plugin" / "plugin.json").write_text('{"name": "demo"}', encoding="utf-8")
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
        {"PreToolUse": [{"hooks": [{"type": "command", "command": "echo blocked 1>&2; exit 2"}]}]},
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


def test_command_hook_env_omits_platform_credentials(monkeypatch, tmp_path) -> None:
    """A bundle hook subprocess must not see platform credentials.

    The runner process still holds them (the parent env is unchanged). The
    hook's own environment does not, and it still receives ordinary config
    plus CLAUDE_PLUGIN_ROOT.
    """

    monkeypatch.setenv("CURIE_RUNNER_TOKEN", "runner-sentinel")
    monkeypatch.setenv("CURIE_STATE_TOKEN", "sbx.example-state")
    monkeypatch.setenv("CURIE_MEMORY_TOKEN", "sbx.example-memory")
    monkeypatch.setenv("CURIE_CONNECTOR_CALLER_TOKEN", "cct.example-caller")
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-ant-PLACEHOLDER")
    monkeypatch.setenv("CURIE_MODEL", "claude-sonnet-5")
    captured = tmp_path / "hook-env.txt"

    async def go() -> dict:
        return await hooks._run_command_hook(
            f"env > {captured}",
            {"tool_name": "Bash", "tool_input": {}},
            tmp_path,
        )

    anyio.run(go)
    rendered = captured.read_text(encoding="utf-8")
    assert "runner-sentinel" not in rendered
    assert "sbx.example-state" not in rendered
    assert "sbx.example-memory" not in rendered
    assert "cct.example-caller" not in rendered
    assert "sk-ant-PLACEHOLDER" not in rendered
    assert "CURIE_MODEL=claude-sonnet-5" in rendered
    assert f"CLAUDE_PLUGIN_ROOT={tmp_path}" in rendered
    assert os.environ["CURIE_RUNNER_TOKEN"] == "runner-sentinel"


def test_command_hook_kills_child_on_timeout(monkeypatch, tmp_path) -> None:
    """A hook command that outlives the timeout must not orphan its children.

    The hook runs under ``/bin/sh -c``; killing only the shell leaves the
    command it started (here a backgrounded ``sleep``) running as an orphan.
    """

    monkeypatch.setattr(hooks, "_HOOK_TIMEOUT_S", 1.0)
    pid_file = tmp_path / "grandchild.pid"
    real_wait_for = hooks.asyncio.wait_for

    async def timeout_after_child_starts(awaitable, *, timeout):
        await _wait_for_hook_child(pid_file)
        return await real_wait_for(awaitable, timeout=timeout)

    monkeypatch.setattr(hooks.asyncio, "wait_for", timeout_after_child_starts)

    async def go() -> dict:
        return await hooks._run_command_hook(
            _hook_child_command(pid_file),
            {"tool_name": "Bash", "tool_input": {}},
            Path.cwd(),
        )

    started = time.monotonic()
    result = anyio.run(go)
    elapsed = time.monotonic() - started

    output = result["hookSpecificOutput"]
    assert "permissionDecision" not in output
    assert "failed to run" in output["additionalContext"]

    grandchild = int(pid_file.read_text())
    _wait_for_hook_child_exit(grandchild)
    alive = _process_alive(grandchild)
    if alive:
        os.kill(grandchild, signal.SIGKILL)
    assert not alive, "timed-out hook left its grandchild running (orphaned)"
    assert elapsed < 10, f"hook timeout took {elapsed:.1f}s to return"


def test_command_hook_kills_its_group_when_cancelled(monkeypatch, tmp_path) -> None:
    """An aborted turn cancels the hook callback; its process group must die too.

    The hook runs in its own session, so nothing else would signal it.
    """

    monkeypatch.setattr(hooks, "_HOOK_TIMEOUT_S", 30)
    pid_file = tmp_path / "grandchild.pid"
    real_wait_for = hooks.asyncio.wait_for

    async def go() -> None:
        child_ready = anyio.Event()

        async def wait_after_subprocess_owned(awaitable, *, timeout):
            await _wait_for_hook_child(pid_file)
            child_ready.set()
            return await real_wait_for(awaitable, timeout=timeout)

        monkeypatch.setattr(hooks.asyncio, "wait_for", wait_after_subprocess_owned)
        async with anyio.create_task_group() as group:
            group.start_soon(
                hooks._run_command_hook,
                _hook_child_command(pid_file),
                {"tool_name": "Bash", "tool_input": {}},
                Path.cwd(),
            )
            # The child can start before create_subprocess_exec returns. Wait
            # until the hook owns its process handle before cancelling it.
            with anyio.fail_after(10):
                await child_ready.wait()
            group.cancel_scope.cancel()

    anyio.run(go)

    grandchild = int(pid_file.read_text())
    _wait_for_hook_child_exit(grandchild)
    alive = _process_alive(grandchild)
    if alive:
        try:
            os.kill(grandchild, signal.SIGKILL)
        except ProcessLookupError:
            pass
    assert not alive, "a cancelled hook left its grandchild running (orphaned)"


def _hook_child_command(pid_file: Path) -> str:
    child = f"echo $$ > {shlex.quote(str(pid_file))}; exec sleep 30"
    return f"/bin/sh -c {shlex.quote(child)} & wait"


async def _wait_for_hook_child(pid_file: Path) -> None:
    with anyio.fail_after(10):
        while True:
            try:
                pid = pid_file.read_text().strip()
            except FileNotFoundError:
                pid = ""
            if pid:
                assert _process_alive(int(pid)), "hook child exited before the test stimulus"
                return
            await anyio.sleep(0.01)


def _wait_for_hook_child_exit(pid: int) -> None:
    deadline = time.monotonic() + 10
    while _process_alive(pid) and time.monotonic() < deadline:
        time.sleep(0.05)


def _process_alive(pid: int) -> bool:
    """True while ``pid`` exists and is not a zombie awaiting reaping."""

    try:
        stat = Path(f"/proc/{pid}/stat").read_text()
    except (FileNotFoundError, ProcessLookupError):
        return False
    return stat.rsplit(")", 1)[1].split()[0] != "Z"


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


# #2946: the rest of the documented PreToolUse contract, driven through the
# public callback with real /bin/sh hook processes.
#
# Source for the decision shapes: Claude Code hooks reference,
# https://code.claude.com/docs/en/hooks ("Exit code output", "JSON output",
# "PreToolUse decision control"). Exit 2 denies with stderr as the reason. Exit 0
# with stdout JSON is parsed: ``hookSpecificOutput.permissionDecision: "deny"``
# denies with ``permissionDecisionReason``, and the older top level
# ``decision: "block"`` with ``reason`` is still honored as a deny; ``"ask"``
# requests a permission prompt, and ``continue: false`` with ``stopReason`` stops
# (the Python SDK spells it ``continue_``, claude_agent_sdk/types.py). Exit 0 stdout
# that is not JSON is not a decision. Any other exit code is a non-blocking error.
# Plugin hooks see ``CLAUDE_PLUGIN_ROOT`` set to the plugin directory.


def _write_script(root: Path, rel: str, body: str) -> None:
    path = root / rel
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("#!/bin/sh\n" + body, encoding="utf-8")
    path.chmod(0o755)


def _callback_for(plugin_dir: str):
    loaded = load_bundle_hooks(plugin_dir)
    assert loaded is not None
    (matcher,) = loaded["PreToolUse"]
    (callback,) = matcher.hooks
    return callback


def _single(tmp_path: Path, *commands: str):
    plugin_dir = _bundle(
        tmp_path,
        {"PreToolUse": [{"hooks": [{"type": "command", "command": c} for c in commands]}]},
    )
    return _callback_for(plugin_dir)


def _call(callback) -> dict:
    return anyio.run(callback, {"tool_name": "Bash", "tool_input": {}}, "tuid", {})


def test_exit_0_stdout_permission_decision_deny_denies(tmp_path: Path) -> None:
    body = (
        '{"hookSpecificOutput": {"hookEventName": "PreToolUse",'
        ' "permissionDecision": "deny", "permissionDecisionReason": "no shell"}}'
    )
    out = _call(_single(tmp_path, f"echo '{body}'; exit 0"))
    assert out == {
        "hookSpecificOutput": {
            "hookEventName": "PreToolUse",
            "permissionDecision": "deny",
            "permissionDecisionReason": "no shell",
        }
    }


def test_exit_0_stdout_legacy_decision_block_denies(tmp_path: Path) -> None:
    out = _call(_single(tmp_path, """echo '{"decision": "block", "reason": "legacy no"}'"""))
    assert out["hookSpecificOutput"]["permissionDecision"] == "deny"
    assert out["hookSpecificOutput"]["permissionDecisionReason"] == "legacy no"


def test_exit_0_stdout_allow_decision_is_ordinary_allow(tmp_path: Path) -> None:
    body = '{"hookSpecificOutput": {"hookEventName": "PreToolUse", "permissionDecision": "allow"}}'
    assert _call(_single(tmp_path, f"echo '{body}'")) == {}


def test_exit_0_malformed_stdout_is_not_a_decision(tmp_path: Path) -> None:
    assert _call(_single(tmp_path / "a", "echo 'deny {not json'")) == {}
    assert _call(_single(tmp_path / "b", "echo '[\"deny\"]'")) == {}


def test_other_exit_code_with_deny_json_is_non_blocking(tmp_path: Path) -> None:
    body = '{"hookSpecificOutput": {"permissionDecision": "deny"}}'
    # Non-blocking error: the tool call proceeds, exactly as it did before #2946.
    assert _call(_single(tmp_path, f"echo '{body}'; exit 1")) == {}
    assert _call(_single(tmp_path / "c", f"echo '{body}'; exit 127")) == {}


def test_stdout_deny_short_circuits_later_hooks(tmp_path: Path) -> None:
    marker = tmp_path / "later-ran"
    body = '{"hookSpecificOutput": {"permissionDecision": "deny", "permissionDecisionReason": "x"}}'
    out = _call(_single(tmp_path, f"echo '{body}'", f"touch {marker}"))
    assert out["hookSpecificOutput"]["permissionDecision"] == "deny"
    assert not marker.exists()


def test_exit_2_deny_short_circuits_later_hooks(tmp_path: Path) -> None:
    marker = tmp_path / "later-ran"
    out = _call(_single(tmp_path, "echo stop 1>&2; exit 2", f"touch {marker}"))
    assert out["hookSpecificOutput"]["permissionDecisionReason"] == "stop"
    assert not marker.exists()


def test_claude_plugin_root_script_runs_and_denies(tmp_path: Path) -> None:
    _write_script(
        tmp_path, "hooks/deny.sh", 'echo "denied from $CLAUDE_PLUGIN_ROOT" 1>&2\nexit 2\n'
    )
    out = _call(_single(tmp_path, "${CLAUDE_PLUGIN_ROOT}/hooks/deny.sh"))
    decision = out["hookSpecificOutput"]
    assert decision["permissionDecision"] == "deny"
    assert decision["permissionDecisionReason"] == f"denied from {tmp_path}"


def test_relative_path_resolves_against_the_bundle(tmp_path: Path, monkeypatch) -> None:
    elsewhere = tmp_path / "elsewhere"
    bundle = tmp_path / "bundle"
    elsewhere.mkdir()
    bundle.mkdir()
    monkeypatch.chdir(elsewhere)
    _write_script(bundle, "hooks/deny.sh", 'echo "cwd=$(pwd)" 1>&2\nexit 2\n')
    out = _call(_single(bundle, "./hooks/deny.sh"))
    assert out["hookSpecificOutput"]["permissionDecisionReason"] == f"cwd={bundle}"


def test_exit_0_stdout_ask_is_carried_not_allowed(tmp_path: Path) -> None:
    marker = tmp_path / "later-ran"
    body = (
        '{"hookSpecificOutput": {"hookEventName": "PreToolUse",'
        ' "permissionDecision": "ask", "permissionDecisionReason": "confirm"}}'
    )
    out = _call(_single(tmp_path, f"echo '{body}'", f"touch {marker}"))
    assert out["hookSpecificOutput"]["permissionDecision"] == "ask"
    assert out["hookSpecificOutput"]["permissionDecisionReason"] == "confirm"
    assert not marker.exists()


def test_exit_0_stdout_continue_false_stops(tmp_path: Path) -> None:
    marker = tmp_path / "later-ran"
    body = '{"continue": false, "stopReason": "halt now"}'
    out = _call(_single(tmp_path, f"echo '{body}'", f"touch {marker}"))
    assert out == {"continue_": False, "stopReason": "halt now"}
    assert not marker.exists()


def test_hooks_declared_as_a_file_path_still_deny(tmp_path: Path) -> None:
    (tmp_path / "hooks").mkdir()
    (tmp_path / "hooks" / "hooks.json").write_text(
        json.dumps(
            {"PreToolUse": [{"hooks": [{"type": "command", "command": "echo f 1>&2; exit 2"}]}]}
        ),
        encoding="utf-8",
    )
    plugin_dir = _bundle(tmp_path, "hooks/hooks.json")
    out = _call(_callback_for(plugin_dir))
    assert out["hookSpecificOutput"]["permissionDecision"] == "deny"
