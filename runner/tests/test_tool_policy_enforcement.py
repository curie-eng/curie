"""`toolPolicy` decided at the two interception points, not just parsed.

The lane `plugin_format.tool_policy` was holding open: "No bundle may ship a real
policy until the runtime lane lands." These pin what landing it means.

The load-bearing test is `test_a_denied_tool_is_refused_by_the_hook_not_only_the_callback`.
`can_use_tool` is skipped whenever another permission rule already allows a call
-- a skill's `allowed-tools` frontmatter is such a rule -- so a decision that
lives only there is bypassable, which is #1852. A policy enforced only in the
callback would be a policy a bundle can walk around by declaring its own
permissions, and every other test here would still pass.
"""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import anyio
import pytest
from claude_agent_sdk.types import PermissionResultAllow, PermissionResultDeny
from curie_runner import mcp_tool_capability
from curie_runner.approval import (
    APPROVAL_TOOL_NAME,
    PUBLISH_TOOL_NAME,
    ApprovalGate,
    build_approval_gate,
    build_approval_hook,
    build_can_use_tool,
    policy_disallowed_tools,
    resolve_approval_policy,
)
from mcp import Tool
from mcp.types import ToolAnnotations
from plugin_format import ToolPolicy


def _policy(**collections: list[str]) -> ToolPolicy:
    return ToolPolicy(
        enforcement="curie/mcp-tool-policy@1",
        allow=collections.get("allow", []),
        approvalRequired=collections.get("approval_required", []),
        deny=collections.get("deny", []),
    )


def _gate(policy: ToolPolicy | None, **kwargs: Any) -> ApprovalGate:
    return ApprovalGate(
        tool_policy=policy,
        bundle_name="sre-bot",
        connector_servers={"k8s-write", "self-upgrade"},
        mcp_servers=set(),
        **kwargs,
    )


def _hook_call(
    gate: ApprovalGate,
    tool_name: str,
    tool_input: dict[str, Any] | None = None,
) -> dict[str, Any]:
    hooks = build_approval_hook(gate)
    matcher = hooks["PreToolUse"][0]
    callback = matcher.hooks[0]
    return anyio.run(
        callback, {"tool_name": tool_name, "tool_input": tool_input or {}}, None, None
    )


def _denied(result: dict[str, Any]) -> bool:
    decision = (result.get("hookSpecificOutput") or {}).get("permissionDecision")
    return decision == "deny"


def _reason(result: dict[str, Any]) -> str:
    return (result.get("hookSpecificOutput") or {}).get("permissionDecisionReason", "")


def _catalog_gate() -> ApprovalGate:
    return ApprovalGate(
        tool_policy=_policy(
            allow=["operations/read_allowed"],
            approval_required=["operations/write_approval"],
            deny=["operations/write_denied"],
        ),
        bundle_name="acme-bot",
        mcp_servers={"operations", "failed"},
        connector_servers={"operations"},
    )


def _production_sre_gate(*, managed_workspace: bool) -> ApprovalGate:
    """Build the gate from the exact production SRE bundle policy and skill."""

    bundle = Path(__file__).parents[2] / "examples" / "sre-bot"
    assert (bundle / "skills" / "sre-bot" / "SKILL.md").is_file()
    resolution = resolve_approval_policy(str(bundle))
    gate = build_approval_gate(
        operator_tools=None,
        policy_routes=resolution.route_by_tool,
        grantable_by_route=resolution.grantable_by_route,
        summary_by_tool=resolution.summary_by_tool,
        bundle_name=resolution.bundle_name,
        mcp_servers=resolution.mcp_servers,
        connector_servers=resolution.connector_servers,
        managed_workspace=managed_workspace,
        tool_policy=resolution.tool_policy,
    )
    assert gate is not None
    return gate


def _assert_no_approval_was_recorded(gate: ApprovalGate) -> None:
    assert gate.pending_summary is None
    assert gate.pending_route is None
    assert gate.pending_gate_kind is None
    assert gate.pending_granted_tool is None
    assert gate.policy_requested is False
    assert gate.policy_rejected is False
    assert gate.policy_route is None
    assert gate.pending_halt is False


def _probe_advertised_tools(
    monkeypatch: pytest.MonkeyPatch, names: list[str]
) -> mcp_tool_capability.McpToolCapabilityProbe:
    @asynccontextmanager
    async def server_streams(
        *_args: Any, **_kwargs: Any
    ) -> AsyncIterator[tuple[object, object]]:
        yield object(), object()

    class ToolListSession:
        def __init__(self, *_args: Any, **_kwargs: Any) -> None:
            pass

        async def __aenter__(self) -> "ToolListSession":
            return self

        async def __aexit__(self, *_args: Any) -> None:
            pass

        async def initialize(self) -> None:
            pass

        async def list_tools(self, *, params: Any | None) -> SimpleNamespace:
            assert params is not None and params.cursor is None
            # MCP recommends this restricted alphabet but its Tool model accepts
            # other strings. Exercise the raw server response at Curie's probe
            # boundary. https://modelcontextprotocol.io/specification/2025-11-25/server/tools#tool-names
            return SimpleNamespace(
                tools=[
                    Tool(
                        name=name,
                        description="Test-only MCP tool.",
                        inputSchema={"type": "object"},
                        annotations=ToolAnnotations(readOnlyHint=False),
                    )
                    for name in names
                ],
                next_cursor=None,
            )

    monkeypatch.setattr(mcp_tool_capability, "_server_streams", server_streams)
    monkeypatch.setattr(mcp_tool_capability, "ClientSession", ToolListSession)
    return anyio.run(
        mcp_tool_capability.probe_mcp_tool_capability,
        None,
        {"operations": {}},
    )


@pytest.mark.parametrize(
    "unsafe_name",
    [
        "write_denied,Bash",
        "write_denied Bash",
        "write_denied(Bash)",
        "write_denied*",
    ],
    ids=["comma", "space", "parentheses", "wildcard"],
)
def test_nonconforming_mcp_tool_name_fails_catalog_probe_closed(
    monkeypatch: pytest.MonkeyPatch, unsafe_name: str
) -> None:
    result = _probe_advertised_tools(monkeypatch, ["read_allowed", unsafe_name])

    assert not result.complete
    assert result.has_potential_write_tool
    assert result.failures == ("operations",)
    assert result.tool_count == 0
    assert result.observed_tools == frozenset()
    assert result.readonly_tools == frozenset()
    assert policy_disallowed_tools(_catalog_gate(), result.observed_tools) == ()

    live_name = f"mcp__operations__{unsafe_name}"
    hook_gate = _catalog_gate()
    assert _denied(_hook_call(hook_gate, live_name))
    _assert_no_approval_was_recorded(hook_gate)

    callback_gate = _catalog_gate()
    callback_result = anyio.run(
        build_can_use_tool(callback_gate), live_name, {}, None
    )
    assert isinstance(callback_result, PermissionResultDeny)
    _assert_no_approval_was_recorded(callback_gate)


def test_conforming_mcp_tool_name_punctuation_reaches_catalog_projection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    valid_names = ["write.denied", "write-denied", "write_denied"]
    result = _probe_advertised_tools(monkeypatch, valid_names)
    expected = {
        f"mcp__operations__{tool_name}" for tool_name in valid_names
    }

    assert result.complete
    assert result.has_potential_write_tool
    assert result.failures == ()
    assert result.tool_count == len(valid_names)
    assert result.observed_tools == frozenset(expected)
    assert set(policy_disallowed_tools(_catalog_gate(), result.observed_tools)) == expected


def test_policy_disallowed_tools_project_only_denied_observed_runtime_names() -> None:
    plugin_prefix = "mcp__plugin_acme-bot_operations__"
    connector_prefix = "mcp__operations__"
    suffixes = {"read_allowed", "write_approval", "write_denied", "write_unmatched"}
    observed = frozenset(
        {f"{plugin_prefix}{suffix}" for suffix in suffixes}
        | {f"{connector_prefix}{suffix}" for suffix in suffixes}
    )
    expected_hidden = {
        f"{plugin_prefix}write_denied",
        f"{plugin_prefix}write_unmatched",
        f"{connector_prefix}write_denied",
        f"{connector_prefix}write_unmatched",
    }

    hidden = set(policy_disallowed_tools(_catalog_gate(), observed))

    assert hidden == expected_hidden
    assert all(name.startswith("mcp__") for name in hidden)
    assert all("/" not in name and "*" not in name for name in hidden)
    assert f"{plugin_prefix}read_allowed" not in hidden
    assert f"{connector_prefix}read_allowed" not in hidden
    assert f"{plugin_prefix}write_approval" not in hidden
    assert f"{connector_prefix}write_approval" not in hidden

    # A failed sibling was never observed, so catalog projection must not invent
    # its exact SDK name. Authorization remains fail-closed independently.
    failed_sibling = "mcp__plugin_acme-bot_failed__write_unmatched"
    assert failed_sibling not in hidden
    for tool_name in expected_hidden | {failed_sibling}:
        hook_gate = _catalog_gate()
        hook_result = _hook_call(hook_gate, tool_name)
        assert _denied(hook_result)
        assert "denied by this agent's tool policy" in _reason(hook_result)
        _assert_no_approval_was_recorded(hook_gate)

        callback_gate = _catalog_gate()
        callback_result = anyio.run(
            build_can_use_tool(callback_gate), tool_name, {}, None
        )
        assert isinstance(callback_result, PermissionResultDeny)
        assert "denied by this agent's tool policy" in callback_result.message
        _assert_no_approval_was_recorded(callback_gate)


def test_a_denied_tool_is_refused_by_the_hook_not_only_the_callback() -> None:
    """The whole point, and the one that fails if enforcement lives in the
    callback alone: the hook is the only interception no permission rule can
    shadow."""
    gate = _gate(_policy(deny=["k8s-write/*"]))
    result = _hook_call(gate, "mcp__k8s-write__restart_deployment")
    assert _denied(result)
    assert "denied by this agent's tool policy" in _reason(result)
    # A refusal is not an approval request: nothing was recorded for a human.
    assert gate.pending_summary is None


def test_a_refusal_does_not_invite_an_approval() -> None:
    """A policy `deny` and a gate block are both denials on the wire and must not
    read the same to a person: one has an audience who can permit it, the other
    does not."""
    gate = _gate(_policy(deny=["k8s-write/restart_deployment"]))
    _hook_call(gate, "mcp__k8s-write__restart_deployment")
    assert gate.pending_summary is None
    assert gate.pending_route is None
    assert gate.pending_granted_tool is None


def test_an_unclassified_mcp_tool_is_refused() -> None:
    """The documented default, and what actually defends the surface: a tool the
    server begins advertising after the bundle was authored is not inherited."""
    gate = _gate(_policy(allow=["k8s-write/restart_deployment"]))
    assert _denied(_hook_call(gate, "mcp__k8s-write__something_new"))


def test_a_builtin_is_untouched_by_a_policy_about_connectors() -> None:
    """`Bash` cannot be written into a `<server>/<tool>` pattern, so reading "no
    pattern matched" as the deny default would revoke it from every bundle that
    ships a policy."""
    gate = _gate(_policy(deny=["k8s-write/*"]))
    assert _hook_call(gate, "Bash") == {}


@pytest.mark.parametrize("interceptor", ["hook", "callback"])
def test_platform_publish_reaches_approval_gate_under_production_sre_tool_policy(
    interceptor: str,
) -> None:
    """Bundle MCP policy must not swallow Curie's platform-owned publish gate."""

    gate = _production_sre_gate(managed_workspace=True)
    tool_input = {"title": "Workspace tool check", "body": "prior-turn marker"}
    if interceptor == "hook":
        result = _hook_call(gate, PUBLISH_TOOL_NAME, tool_input)
        assert _denied(result)
        reason = _reason(result)
    else:
        result = anyio.run(
            build_can_use_tool(gate), PUBLISH_TOOL_NAME, tool_input, None
        )
        assert isinstance(result, PermissionResultDeny)
        reason = result.message

    assert gate.pending_gate_kind == "permission"
    assert gate.pending_granted_tool == PUBLISH_TOOL_NAME
    assert gate.publication_title == "Workspace tool check"
    assert gate.publication_body == "prior-turn marker"
    assert "denied by this agent's tool policy" not in reason


@pytest.mark.parametrize("interceptor", ["hook", "callback"])
def test_platform_request_approval_is_outside_production_sre_tool_policy(
    interceptor: str,
) -> None:
    """The sibling Curie MCP tool remains governed by its in-process route logic."""

    gate = _production_sre_gate(managed_workspace=True)
    if interceptor == "hook":
        assert _hook_call(gate, APPROVAL_TOOL_NAME) == {}
    else:
        result = anyio.run(build_can_use_tool(gate), APPROVAL_TOOL_NAME, {}, None)
        assert isinstance(result, PermissionResultAllow)

    _assert_no_approval_was_recorded(gate)


def test_an_allowed_tool_falls_through_to_the_existing_gate() -> None:
    """A policy may only ADD restrictions (#520). An `allow` does not hollow out
    an operator gate on the same tool."""
    gate = _gate(
        _policy(allow=["k8s-write/restart_deployment"]),
        required=frozenset({"mcp__k8s-write__restart_deployment"}),
    )
    result = _hook_call(gate, "mcp__k8s-write__restart_deployment")
    assert _denied(result), "an operator-gated tool must stay gated"
    assert gate.pending_summary is not None, "and it must still ask a human"


def test_approval_required_gates_a_tool_the_operator_never_named() -> None:
    gate = _gate(_policy(approval_required=["self-upgrade/upgrade_platform"]))
    result = _hook_call(gate, "mcp__self-upgrade__upgrade_platform")
    assert _denied(result)
    # A block, not a refusal: this one has an audience.
    assert gate.pending_summary is not None
    assert "denied by this agent's tool policy" not in _reason(result)


def test_a_bundle_with_no_policy_is_unchanged() -> None:
    """Every bundle shipped to date. The hook keeps its fast path."""
    gate = _gate(None)
    assert _hook_call(gate, "mcp__k8s-write__restart_deployment") == {}
    assert _hook_call(gate, "Bash") == {}


@pytest.mark.parametrize(
    "tool",
    ["mcp__k8s-write__restart_deployment", "mcp__k8s-write__something_new"],
)
def test_the_callback_agrees_with_the_hook(tool: str) -> None:
    """Both interception points share `_decide_gate`, so they must not disagree
    -- the defect class #1852 closed for the two invocation contexts."""
    gate = _gate(_policy(deny=["k8s-write/restart_deployment"]))
    callback = build_can_use_tool(gate)
    result = anyio.run(callback, tool, {}, None)
    assert type(result).__name__ == "PermissionResultDeny"
