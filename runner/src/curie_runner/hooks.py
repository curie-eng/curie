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
hook input JSON on stdin, following the Claude Code convention: exit 0 allows the
tool call, exit 2 denies it (stderr is the reason), any other non-zero is a
non-blocking hook error that lets the call proceed.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from claude_agent_sdk import HookMatcher
from plugin_format import HookMatcherConfig, PluginManifest, resolve_manifest
from pydantic import TypeAdapter, ValidationError

from .mcp_tool_capability import ConnectorAvailability

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


async def _run_command_hook(command: str, hook_input: Any) -> dict[str, Any]:
    """Run one command hook and map its exit to a PreToolUse decision.

    exit 0 -> allow (empty output, proceed); exit ``_DENY_EXIT_CODE`` -> deny with
    the command's stderr as the reason; any other exit -> non-blocking error.
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
        )
        out, err = await asyncio.wait_for(
            proc.communicate(input=payload.encode("utf-8")), timeout=_HOOK_TIMEOUT_S
        )
    except (TimeoutError, OSError) as exc:
        # A timed-out hook leaves its shell child still running; kill it so it
        # doesn't orphan (an OSError from create_subprocess_exec itself has no
        # live proc to clean up). Best-effort: an already-dead child raising
        # ProcessLookupError must not mask the original timeout/OSError.
        if proc is not None and proc.returncode is None:
            try:
                proc.kill()
                await proc.wait()
            except ProcessLookupError:
                pass
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
        reason = err.decode("utf-8", "replace").strip() or "blocked by bundle PreToolUse hook"
        return {
            "hookSpecificOutput": {
                "hookEventName": "PreToolUse",
                "permissionDecision": "deny",
                "permissionDecisionReason": reason,
            }
        }
    if proc.returncode not in (0, _DENY_EXIT_CODE):
        context = err.decode("utf-8", "replace").strip() or out.decode("utf-8", "replace").strip()
        return {
            "hookSpecificOutput": {
                "hookEventName": "PreToolUse",
                "additionalContext": f"bundle hook exited {proc.returncode}: {context}",
            }
        }
    return {}


def _make_callback(commands: list[str]) -> Any:
    """Build one SDK hook callback that runs each command hook in order.

    The first command to deny wins (short-circuits); otherwise the tool proceeds.
    """

    async def _callback(hook_input: Any, _tool_use_id: str | None, _ctx: Any) -> dict[str, Any]:
        for command in commands:
            result = await _run_command_hook(command, hook_input)
            decision = result.get("hookSpecificOutput", {}).get("permissionDecision")
            if decision == "deny":
                return result
        return {}

    return _callback


def load_bundle_hooks(plugin_dir: str | None) -> dict[str, list[HookMatcher]] | None:
    """Translate the bundle's PreToolUse hooks into SDK ``HookMatcher`` config.

    Returns a ``{"PreToolUse": [HookMatcher, ...]}`` mapping for
    ``ClaudeAgentOptions.hooks``, or ``None`` when the bundle declares no usable
    PreToolUse command hooks. Non-command hook actions are skipped (only
    ``type: "command"`` is executable here); other events are ignored for now.
    """

    parsed = _load_manifest_hooks(plugin_dir)
    if not parsed:
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
        matchers.append(HookMatcher(matcher=entry.matcher, hooks=[_make_callback(commands)]))

    if not matchers:
        return None
    return {"PreToolUse": matchers}


def build_gated_pre_tool_use_hooks(
    approval_hooks: dict[str, list[HookMatcher]] | None,
    availability: ConnectorAvailability | None,
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
            return {
                "hookSpecificOutput": {
                    "hookEventName": "PreToolUse",
                    "permissionDecision": "deny",
                    "permissionDecisionReason": failure.caller_message(),
                }
            }
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
