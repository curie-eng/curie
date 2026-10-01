"""Consume the bundle manifest ``hooks`` field as SDK PreToolUse guardrails.

The plugin-format ``hooks`` declaration (validated at deploy by
``plugin_format.validate_bundle``) gives builders deterministic, in-bundle
guardrails that complement platform-level gates. This module reads that
declaration from the mounted bundle and translates its **PreToolUse** entries
into ``claude_agent_sdk`` ``HookMatcher`` callbacks, so a declared command runs
before a matching tool call and can block it.

Only ``PreToolUse`` is consumed here (the deterministic before-the-tool gate the
issue scopes); other events are validated by plugin-format but not yet wired.
Each ``type: "command"`` hook runs the command through ``/bin/sh -c`` with the
hook input JSON on stdin, cwd set to the mounted bundle and ``CLAUDE_PLUGIN_ROOT``
set to it, following the Claude Code convention: exit 2 denies the tool call
(stderr is the reason); exit 0 allows it unless stdout is a JSON decision object
that denies; any other non-zero is a non-blocking hook error that lets the call
proceed.
"""

from __future__ import annotations

import asyncio
import json
import os
import signal
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from claude_agent_sdk import HookMatcher
from plugin_format import HookMatcherConfig, PluginManifest, resolve_manifest
from pydantic import TypeAdapter, ValidationError

from .mcp_tool_capability import ConnectorAvailability
from .subprocess_env import shell_and_hook_env

_HOOKS_ADAPTER = TypeAdapter(dict[str, list[HookMatcherConfig]])

# Deny convention (Claude Code): a command hook that exits with this code blocks
# the tool call; its stderr becomes the reason surfaced to the model.
_DENY_EXIT_CODE = 2
_HOOK_TIMEOUT_S = 60.0


def _load_manifest_hooks(plugin_dir: str | None) -> dict[str, list[HookMatcherConfig]] | None:
    """Read + parse the manifest ``hooks`` declaration from the bundle.

    Returns the parsed hooks mapping, or ``None`` when there is no plugin dir, no
    manifest, or no ``hooks`` field. Best-effort: a bundle that fails to parse
    here is caught by ``load_plugins``/``validate_bundle`` at startup, the
    authoritative gate, so this stays quiet.
    """

    if not plugin_dir:
        return None
    root = Path(plugin_dir)
    manifest_path = resolve_manifest(root)
    if manifest_path is None:
        return None
    try:
        manifest = PluginManifest.model_validate(
            json.loads(manifest_path.read_text(encoding="utf-8"))
        )
    except (json.JSONDecodeError, ValueError, OSError):
        return None

    declared = manifest.hooks
    if declared is None:
        return None
    if isinstance(declared, str):
        hooks_path = root / declared
        if not hooks_path.is_file():
            return None
        try:
            data: object = json.loads(hooks_path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            return None
    else:
        data = declared

    try:
        return _HOOKS_ADAPTER.validate_python(data)
    except ValidationError:
        return None


class RefusalLedger:
    """Call IDs a runner-side PreToolUse callback denied since the last prompt.

    The approval gate records its own decisions and the per-turn tool access
    its own refusals. This ledger holds the rest of the runner's PreToolUse
    denials, so ``SessionRunner`` can count their error results as a refusal or
    an unavailable connector and not as the tool failing (#3580). One instance
    is shared by every callback that records into it and by the session, which
    clears it immediately before each prompt, as it does ``TurnToolAccess``.
    """

    def __init__(self) -> None:
        # Denied by the bundle's own PreToolUse command or the factory guard.
        self.refused_call_ids: set[str] = set()
        # Denied because the call's connector is in the exclusion set.
        self.unavailable_call_ids: set[str] = set()

    def begin(self) -> None:
        """Start the record for the prompt about to be sent."""

        self.refused_call_ids.clear()
        self.unavailable_call_ids.clear()


def _record_deny(
    ids: set[str] | None, result: dict[str, Any], tool_use_id: str | None
) -> dict[str, Any]:
    """``result``, remembering ``tool_use_id`` in ``ids`` when it denies the call.

    Only a deny is a refusal. ``ask`` asks the permission layer, which may
    still allow the call, and a stop without a deny ends the turn rather than
    refusing this call.
    """

    if (
        ids is not None
        and tool_use_id
        and isinstance(result.get("hookSpecificOutput"), Mapping)
        and result["hookSpecificOutput"].get("permissionDecision") == "deny"
    ):
        ids.add(tool_use_id)
    return result


def _decision(decision: str, reason: str) -> dict[str, Any]:
    return {
        "hookSpecificOutput": {
            "hookEventName": "PreToolUse",
            "permissionDecision": decision,
            "permissionDecisionReason": reason or "blocked by bundle PreToolUse hook",
        }
    }


def build_factory_foreground_hooks(
    ledger: RefusalLedger | None = None,
) -> dict[str, list[HookMatcher]]:
    """Keep factory commands and reviewer calls inside the active turn.

    A deny is recorded in ``ledger`` as a refusal (#3580).
    """

    refused = ledger.refused_call_ids if ledger is not None else None

    async def guard(hook_input: Any, tool_use_id: str | None, _ctx: Any) -> Any:
        return _record_deny(refused, _foreground_decision(hook_input), tool_use_id)

    return {"PreToolUse": [HookMatcher(matcher="Bash|Agent|Task", hooks=[guard])]}


def _foreground_decision(hook_input: Any) -> dict[str, Any]:
    """The foreground guard's decision for one call; ``{}`` abstains."""

    if not isinstance(hook_input, dict):
        return {}
    tool = hook_input.get("tool_name")
    tool_input = hook_input.get("tool_input")
    if not isinstance(tool_input, dict):
        return {}
    if tool == "Bash" and tool_input.get("run_in_background") is True:
        return _decision(
            "deny", "Run Bash in the foreground and wait for it before you end your turn."
        )
    if tool in ("Agent", "Task") and tool_input.get("run_in_background") is not False:
        return _decision(
            "deny",
            "Run the agent in the foreground with run_in_background false. "
            "Wait for it before you end your turn.",
        )
    return {}


def _stdout_decision(out: bytes) -> dict[str, Any]:
    """Read an exit 0 hook's stdout decision object (#2946).

    Claude Code parses exit 0 stdout as JSON: ``hookSpecificOutput``
    ``permissionDecision`` ``"deny"`` or ``"ask"`` carries through with
    ``permissionDecisionReason``, the older top level ``decision: "block"`` with
    ``reason`` denies, and ``continue: false`` stops with ``stopReason`` (spelled
    ``continue_`` for the SDK). ``"allow"`` and stdout that is not a JSON object
    are not a decision, so the call proceeds.
    """

    try:
        data = json.loads(out.decode("utf-8", "replace"))
    except ValueError:
        return {}
    if not isinstance(data, dict):
        return {}
    result: dict[str, Any] = {}
    specific = data.get("hookSpecificOutput")
    if isinstance(specific, dict) and specific.get("permissionDecision") in ("deny", "ask"):
        result = _decision(
            specific["permissionDecision"], str(specific.get("permissionDecisionReason") or "")
        )
    elif data.get("decision") == "block":
        result = _decision("deny", str(data.get("reason") or ""))
    if data.get("continue") is False:
        result["continue_"] = False
        if isinstance(data.get("stopReason"), str):
            result["stopReason"] = data["stopReason"]
    return result


async def _kill_hook_group(proc: asyncio.subprocess.Process) -> None:
    """SIGKILL a hook's whole process group and reap its shell.

    The hook starts in its own session, so its pid is the group id and the
    kill reaches every process the shell started, even after the shell itself
    exited. Best-effort: an already-empty group raising ProcessLookupError must
    not mask the caller's original error.
    """

    try:
        os.killpg(proc.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    await proc.wait()


async def _run_command_hook(command: str, hook_input: Any, plugin_root: Path) -> dict[str, Any]:
    """Run one command hook and map its result to a PreToolUse decision.

    The command runs in ``plugin_root`` with ``CLAUDE_PLUGIN_ROOT`` set to it, so
    ``${CLAUDE_PLUGIN_ROOT}/hooks/x.sh`` and ``./hooks/x.sh`` both name the
    bundle's script. exit ``_DENY_EXIT_CODE`` -> deny with stderr as the reason;
    exit 0 -> the stdout decision object, if any, else allow; any other exit ->
    non-blocking error.
    """

    try:
        payload = json.dumps(hook_input, default=str)
    except (TypeError, ValueError):
        payload = "{}"

    proc: asyncio.subprocess.Process | None = None
    try:
        proc = await asyncio.create_subprocess_exec(
            "/bin/sh",
            "-c",
            command,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            cwd=plugin_root,
            env=shell_and_hook_env(os.environ, extra={"CLAUDE_PLUGIN_ROOT": str(plugin_root)}),
            start_new_session=True,
        )
        out, err = await asyncio.wait_for(
            proc.communicate(input=payload.encode("utf-8")), timeout=_HOOK_TIMEOUT_S
        )
    except asyncio.CancelledError:
        # The hook runs in its own session, so a signal to the runner's process
        # group no longer reaches it: an aborted turn must reap it explicitly.
        if proc is not None:
            await _kill_hook_group(proc)
        raise
    except (TimeoutError, OSError) as exc:
        # A timed-out hook leaves its shell and everything the shell started
        # still running. Killing only the shell would orphan the grandchildren
        # (an OSError from create_subprocess_exec itself has no live proc).
        if proc is not None:
            await _kill_hook_group(proc)
        # A hook that cannot run is a non-blocking error: surface context but do
        # not silently deny the tool (deploy-time validation already rejected
        # malformed declarations).
        return {
            "hookSpecificOutput": {
                "hookEventName": "PreToolUse",
                "additionalContext": f"bundle hook failed to run: {exc}",
            }
        }

    if proc.returncode == _DENY_EXIT_CODE:
        return _decision("deny", err.decode("utf-8", "replace").strip())
    if proc.returncode == 0:
        return _stdout_decision(out)
    context = err.decode("utf-8", "replace").strip() or out.decode("utf-8", "replace").strip()
    return {
        "hookSpecificOutput": {
            "hookEventName": "PreToolUse",
            "additionalContext": f"bundle hook exited {proc.returncode}: {context}",
        }
    }


def _make_callback(
    commands: list[str], plugin_root: Path, ledger: RefusalLedger | None = None
) -> Any:
    """Build one SDK hook callback that runs each command hook in order.

    The first command to deny, ask, or stop wins (short-circuits); otherwise the
    tool proceeds. A deny is recorded in ``ledger`` as a refusal (#3580): the
    bundle's own guardrail refused the call, and its connector never saw it.
    """

    refused = ledger.refused_call_ids if ledger is not None else None

    async def _callback(hook_input: Any, tool_use_id: str | None, _ctx: Any) -> dict[str, Any]:
        for command in commands:
            result = await _run_command_hook(command, hook_input, plugin_root)
            decision = result.get("hookSpecificOutput", {}).get("permissionDecision")
            if decision in ("deny", "ask") or result.get("continue_") is False:
                return _record_deny(refused, result, tool_use_id)
        return {}

    return _callback


def load_bundle_hooks(
    plugin_dir: str | None, *, ledger: RefusalLedger | None = None
) -> dict[str, list[HookMatcher]] | None:
    """Translate the bundle's PreToolUse hooks into SDK ``HookMatcher`` config.

    Returns a ``{"PreToolUse": [HookMatcher, ...]}`` mapping for
    ``ClaudeAgentOptions.hooks``, or ``None`` when the bundle declares no usable
    PreToolUse command hooks. Non-command hook actions are skipped (only
    ``type: "command"`` is executable here); other events are ignored for now.
    Every deny is recorded in ``ledger`` when one is given.
    """

    parsed = _load_manifest_hooks(plugin_dir)
    if not parsed or not plugin_dir:
        return None

    pre_tool_use = parsed.get("PreToolUse")
    if not pre_tool_use:
        return None

    matchers: list[HookMatcher] = []
    for entry in pre_tool_use:
        commands = [
            h.command
            for h in entry.hooks
            if h.type == "command" and h.command and h.command.strip()
        ]
        if not commands:
            continue
        matchers.append(
            HookMatcher(
                matcher=entry.matcher,
                hooks=[_make_callback(commands, Path(plugin_dir), ledger)],
            )
        )

    if not matchers:
        return None
    return {"PreToolUse": matchers}


def build_gated_pre_tool_use_hooks(
    approval_hooks: dict[str, list[HookMatcher]] | None,
    availability: ConnectorAvailability | None,
    ledger: RefusalLedger | None = None,
) -> dict[str, list[HookMatcher]] | None:
    """Compose the connector exclusion (#2634) in FRONT of the approval hook.

    A failed declared connector no longer halts the turn: the model runs, and
    this keeps it from calling ``mcp__<connector>__*`` while the connector is
    in the runner-owned exclusion set, read at call time so a connector the
    turn-start re-probe recovers is callable again with no session rebuild.

    Composed, never merged as a sibling matcher: the CLI dispatches matchers on
    one event concurrently, so a sibling approval hook would still record a
    pending approval and stop the turn, or spend the one-shot grant, on a call
    the exclusion denies. Here an excluded tool returns the connector deny
    WITHOUT reaching the approval callbacks, so no gate state is touched; every
    other tool is delegated to them unchanged. The deny carries the caller-safe
    ``caller_message()`` (names only) and no ``continue_: False``: the turn
    should carry on and answer with the tools it still has.

    The exclusion deny is recorded in ``ledger`` as unavailable (#3580): the
    call's connector failed its startup probe, so the call never reached it.
    The delegated approval callbacks record their own decisions on the gate.

    ``availability`` None returns ``approval_hooks`` as-is, so an agent with no
    failed connector keeps its wiring byte-identical. The approval hooks must
    be ``matcher=None`` (every tool), which is what ``build_approval_hook``
    registers; a named matcher cannot be delegated to faithfully and raises.
    """

    if availability is None:
        return approval_hooks
    delegates: list[Any] = []
    for matcher in (approval_hooks or {}).get("PreToolUse", []):
        if matcher.matcher is not None:
            raise ValueError("connector exclusion can only front a matcher=None hook")
        delegates.extend(matcher.hooks)
    unavailable = ledger.unavailable_call_ids if ledger is not None else None

    async def connector_exclusion_hook(
        hook_input: Any,
        tool_use_id: str | None,
        context: Any,
    ) -> dict[str, Any]:
        tool_name = (
            hook_input.get("tool_name")
            if isinstance(hook_input, Mapping)
            else getattr(hook_input, "tool_name", None)
        )
        failure = (
            availability.failure_for_tool(tool_name)
            if isinstance(tool_name, str) and tool_name
            else None
        )
        if failure is not None:
            return _record_deny(
                unavailable,
                {
                    "hookSpecificOutput": {
                        "hookEventName": "PreToolUse",
                        "permissionDecision": "deny",
                        "permissionDecisionReason": failure.caller_message(),
                    }
                },
                tool_use_id,
            )
        for delegate in delegates:
            result: dict[str, Any] = await delegate(hook_input, tool_use_id, context)
            if result:
                return result
        return {}

    callback: Any = connector_exclusion_hook
    composed = {event: list(matchers) for event, matchers in (approval_hooks or {}).items()}
    composed["PreToolUse"] = [HookMatcher(matcher=None, hooks=[callback])]
    return composed


def _merge_pre_tool_use_hooks(
    approval_hooks: dict[str, list[HookMatcher]] | None,
    bundle_hooks: dict[str, list[HookMatcher]] | None,
) -> dict[str, list[HookMatcher]] | None:
    """Merge the approval gate's PreToolUse matcher with the bundle's own (#1852).

    Merge, never replace: dropping the bundle's declared PreToolUse guardrails
    (#272) would silently disarm them, and dropping the approval matcher leaves
    the gate bypassable by any permission rule -- the #1852 defect itself. The
    approval matcher is placed first for determinism of our own construction;
    the CLI dispatches matchers on one event CONCURRENTLY
    (claude_agent_sdk/types.py:1956-1961), so list position is not a runtime
    precedence guarantee and nothing here relies on it.

    Returns None when neither side contributes anything: ``ClaudeAgentOptions``
    takes ``hooks=None`` to mean "no hooks declared", which is not the same as
    an empty matcher list, and ``load_bundle_hooks`` returning None for a bundle
    with no hooks is the common case rather than an error.
    """

    merged: dict[str, list[HookMatcher]] = {}
    for source in (approval_hooks, bundle_hooks):
        if not source:
            continue
        for event, matchers in source.items():
            merged.setdefault(event, []).extend(matchers)
    return merged or None
