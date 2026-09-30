"""Offline end-to-end of the #245 permission gate through the real boot path.

This drives the real boot wiring (``build_runner(..., fake_model=True)``) so the
approval gate is exercised together with config parsing, the session loop, and
NDJSON streaming -- not just its unit seam. The fake model replays a scripted Bash
tool call (``default_turn``); the gate must intercept it exactly as the SDK would,
with zero credential and zero network.

It fails against the pre-#413 fake factory, which builds a bare
``FakeModelSession()`` that ignores ``can_use_tool``: the gated turn ends ``done``
(not ``awaiting-approval``).

Bundle PreToolUse command hooks (#272) are deliberately NOT exercised in fake mode
-- they shell out, which would break the fake's offline no-op guarantee -- so their
bundle -> ``load_bundle_hooks`` -> deny wiring is covered offline in ``test_hooks``.
"""

from __future__ import annotations

import json

import anyio
from aci_protocol import Event
from claude_agent_sdk import PermissionResultAllow, PermissionResultDeny
from claude_agent_sdk.types import ToolPermissionContext
from curie_runner.__main__ import build_runner
from curie_runner.approval import (
    build_approval_hook,
    build_can_use_tool,
)
from curie_runner.config import RunnerConfig
from plugin_format import PLATFORM_PUBLISH_TOOL_NAME, validate_bundle

# A budget high enough that default_turn's 8 output tokens never trip the halt
# (a halt would outrank a pending approval and mask the gate under test).
_BUDGET = '{"max_output_tokens_per_run": 10000, "max_usd_per_day": 1.0}'


def _base_env(plugin_dir: str) -> dict[str, str]:
    return {
        "CURIE_PLUGIN_DIR": plugin_dir,
        "CURIE_SESSION_ID": "s-e2e",
        "CURIE_SANDBOX_ID": "b-e2e",
        "CURIE_BUDGET": _BUDGET,
    }


def _write_manifest(tmp_path, manifest: dict) -> str:
    """Write a minimal bundle manifest and return the plugin dir path."""
    plugin = tmp_path / ".claude-plugin"
    plugin.mkdir(parents=True)
    (plugin / "plugin.json").write_text(json.dumps(manifest))
    return str(tmp_path)


async def _drain(runner, text: str) -> list[dict[str, object]]:
    lines = [
        line
        async for line in runner.run_turn(
            Event(type="message", text=text, user="U", ts="1")
        )
    ]
    return [json.loads(line) for line in lines]


def test_gate_e2e_ends_awaiting_approval_through_build_runner(tmp_path) -> None:
    # A bundle-declared/env-configured approval-required tool (Bash) must end the
    # turn awaiting-approval on a fake runner built by the real boot path.
    plugin_dir = _write_manifest(tmp_path, {"name": "gated"})
    env = {**_base_env(plugin_dir), "CURIE_APPROVAL_REQUIRED_TOOLS": "Bash"}
    runner = build_runner(RunnerConfig.from_env(env), fake_model=True)

    async def go() -> None:
        await runner.start()
        frames = await _drain(runner, "run some bash")
        final = frames[-1]
        assert final["type"] == "final"
        assert final["status"] == "awaiting-approval"
        assert isinstance(final["approval_summary"], str)
        assert final["approval_summary"].startswith("Tool call awaiting approval: Bash")

    anyio.run(go)


def test_empty_policy_and_env_preserve_bypass_without_workspace_on_fake_boot_path(
    tmp_path,
) -> None:
    plugin_dir = _write_manifest(tmp_path, {"name": "publisher"})
    runner = build_runner(
        RunnerConfig.from_env(_base_env(plugin_dir)), fake_model=True
    )

    gate = runner._approval_gate  # noqa: SLF001 - boot wiring is the assertion
    assert gate is None


def test_empty_policy_and_env_preserve_bypass_without_workspace_on_real_boot_path(
    tmp_path,
) -> None:
    plugin_dir = _write_manifest(tmp_path, {"name": "publisher"})
    runner = build_runner(
        RunnerConfig.from_env(_base_env(plugin_dir)), fake_model=False
    )

    gate = runner._approval_gate  # noqa: SLF001 - boot wiring is the assertion
    assert gate is None


def test_tool_policy_alone_constructs_both_live_interceptors(tmp_path) -> None:
    """A policy is an enforcement source even when no legacy gate exists."""

    plugin_dir = _write_manifest(
        tmp_path,
        {
            "name": "acme-bot",
            "toolPolicy": {
                "enforcement": "curie/mcp-tool-policy@1",
                "allow": ["kubernetes/pods_list"],
                "approvalRequired": ["kubernetes/resources_scale"],
                "deny": ["kubernetes/resources_delete"],
            },
        },
    )
    (tmp_path / "connectors.yaml").write_text(
        "connectors:\n"
        "  kubernetes:\n"
        "    image: ghcr.io/example/kubernetes-mcp-server:0.0.1\n",
        encoding="utf-8",
    )
    runner = build_runner(
        RunnerConfig.from_env(_base_env(plugin_dir)), fake_model=True
    )

    gate = runner._approval_gate  # noqa: SLF001 - boot wiring is the assertion
    assert gate is not None
    hook = build_approval_hook(gate)["PreToolUse"][0].hooks[0]
    callback = build_can_use_tool(gate)

    async def go() -> None:
        allowed = "mcp__kubernetes__pods_list"
        approval_required = "mcp__kubernetes__resources_scale"
        denied = "mcp__kubernetes__resources_delete"
        unclassified = "mcp__kubernetes__configuration_view"

        assert await hook(
            {"tool_name": allowed, "tool_input": {}}, None, None
        ) == {}
        assert isinstance(
            await callback(allowed, {}, ToolPermissionContext()),
            PermissionResultAllow,
        )

        hook_refusal = await hook(
            {"tool_name": denied, "tool_input": {}}, None, None
        )
        assert hook_refusal["hookSpecificOutput"]["permissionDecision"] == "deny"
        callback_refusal = await callback(denied, {}, ToolPermissionContext())
        assert isinstance(callback_refusal, PermissionResultDeny)
        assert gate.pending_summary is None

        unclassified_refusal = await callback(
            unclassified, {}, ToolPermissionContext()
        )
        assert isinstance(unclassified_refusal, PermissionResultDeny)
        assert gate.pending_summary is None

        hook_block = await hook(
            {"tool_name": approval_required, "tool_input": {}}, None, None
        )
        assert hook_block["hookSpecificOutput"]["permissionDecision"] == "deny"
        callback_block = await callback(
            approval_required, {}, ToolPermissionContext()
        )
        assert isinstance(callback_block, PermissionResultDeny)
        assert gate.pending_summary is not None

    anyio.run(go)


def test_tool_policy_allow_preserves_a_separate_legacy_connector_gate(
    tmp_path,
) -> None:
    """An explicit policy allow cannot subtract an approvalPolicy gate."""

    tool = "mcp__self-upgrade__upgrade_platform"
    plugin_dir = _write_manifest(
        tmp_path,
        {
            "name": "acme-bot",
            "approvalPolicy": {
                "gates": [{"gate": tool, "route": "sre-approvals"}]
            },
            "toolPolicy": {
                "enforcement": "curie/mcp-tool-policy@1",
                "allow": ["self-upgrade/upgrade_platform"],
                "approvalRequired": [],
                "deny": [],
            },
        },
    )
    (tmp_path / "connectors.yaml").write_text(
        "connectors:\n"
        "  self-upgrade:\n"
        "    image: ghcr.io/example/self-upgrade:0.0.1\n",
        encoding="utf-8",
    )
    runner = build_runner(
        RunnerConfig.from_env(_base_env(plugin_dir)), fake_model=True
    )
    gate = runner._approval_gate  # noqa: SLF001 - boot wiring is the assertion
    assert gate is not None
    hook = build_approval_hook(gate)["PreToolUse"][0].hooks[0]
    callback = build_can_use_tool(gate)

    async def go() -> None:
        hook_result = await hook(
            {"tool_name": tool, "tool_input": {}}, None, None
        )
        assert hook_result["hookSpecificOutput"]["permissionDecision"] == "deny"
        callback_result = await callback(tool, {}, ToolPermissionContext())
        assert isinstance(callback_result, PermissionResultDeny)
        assert gate.pending_route == "sre-approvals"
        assert gate.pending_granted_tool == tool

    anyio.run(go)


def test_managed_workspace_arms_mandatory_publish_on_fake_boot_path(tmp_path) -> None:
    plugin_dir = _write_manifest(tmp_path / "plugin", {"name": "publisher"})
    workspace = tmp_path / "workspace"
    (workspace / ".git").mkdir(parents=True)
    runner = build_runner(
        RunnerConfig.from_env(_base_env(plugin_dir)),
        fake_model=True,
        workspace_path=workspace,
    )

    gate = runner._approval_gate  # noqa: SLF001 - boot wiring is the assertion
    assert gate is not None
    assert gate.required == frozenset({PLATFORM_PUBLISH_TOOL_NAME})


def _publication_bundle(tmp_path, gates: list[dict[str, str]]) -> str:
    manifest: dict[str, object] = {"name": "publisher"}
    if gates:
        manifest["approvalPolicy"] = {"gates": gates}
    return _write_manifest(tmp_path / "plugin", manifest)


def _block_publication_on_boot_path(tmp_path, plugin_dir: str):
    workspace = tmp_path / "workspace"
    (workspace / ".git").mkdir(parents=True)
    runner = build_runner(
        RunnerConfig.from_env(_base_env(plugin_dir)),
        fake_model=True,
        workspace_path=workspace,
    )
    gate = runner._approval_gate  # noqa: SLF001 - boot wiring is the assertion
    assert gate is not None
    gate.bind_publication_context(None)
    hook = build_approval_hook(gate)["PreToolUse"][0].hooks[0]

    async def go() -> None:
        result = await hook(
            {
                "tool_name": PLATFORM_PUBLISH_TOOL_NAME,
                "tool_input": {"title": "Fix the README", "body": ""},
            },
            "toolu_publication_test",
            None,
        )
        assert result["hookSpecificOutput"]["permissionDecision"] == "deny"

    anyio.run(go)
    assert gate.pending_granted_tool == PLATFORM_PUBLISH_TOOL_NAME
    return gate


def test_declared_publication_gate_route_reaches_the_blocked_publication(
    tmp_path,
) -> None:
    """#2719: a bundle that declares the publication gate with a route must
    validate and carry that route onto the blocked publication. Before #2776 the
    validator refused ``mcp__curie__publish_changes`` so no bundle could bind a
    route and #2705 was inert. The worker then passes ``pending_route`` as
    ``PublicationCreateRequest.route`` (pinned in the worker approval lifecycle
    suite) and the API stores it on the approval (test_publication_operator_route).
    """

    plugin_dir = _publication_bundle(
        tmp_path,
        [{"gate": PLATFORM_PUBLISH_TOOL_NAME, "route": "publish-approvers"}],
    )
    validation = validate_bundle(plugin_dir)
    assert validation.valid, validation.errors

    gate = _block_publication_on_boot_path(tmp_path, plugin_dir)
    assert gate.pending_route == "publish-approvers"


def test_undeclared_publication_gate_leaves_the_publication_route_empty(
    tmp_path,
) -> None:
    """Missing-route control: with no declared gate the publication still blocks
    but carries no route, which the API refuses to an operator principal."""

    plugin_dir = _publication_bundle(tmp_path, [])
    assert validate_bundle(plugin_dir).valid

    gate = _block_publication_on_boot_path(tmp_path, plugin_dir)
    assert gate.pending_route is None
