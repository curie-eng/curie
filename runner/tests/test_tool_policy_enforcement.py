"""`toolPolicy` decided at the two interception points, not just parsed.

The lane `plugin_format.tool_policy` was holding open: "No bundle may ship a real
policy until the runtime lane lands." These pin what landing it means.

The load-bearing test is `test_a_denied_tool_is_refused_by_the_hook_not_only_the_callback`.
`can_use_tool` is skipped whenever another permission rule already allows a call
-- a skill's `allowed-tools` frontmatter is such a rule -- so a decision that
lives only there is bypassable, which is #1852. A policy enforced only in the
callback would be a policy a bundle can walk around by declaring its own
permissions, and every other test here would still pass.

The second cluster here is the mirror-image failure, #2286. Curie mounts its own
MCP servers (`curie`, `curie-state`) that a bundle is forbidden from declaring,
so the bundle's policy has nothing to classify them against and the fail-closed
default refuses them. The agent then sees its own channel memory and its own
approval path answer "denied by this agent's tool policy, do not retry". That
is a refusal with no audience, over a capability the bundle never governed. These
tests pin the scope rule (ADR-0139): the tools those servers publish are outside
a bundle's `toolPolicy`, decided by EXACT published tool name and only for a
server this session actually mounted, and the deny-by-default that defends every
genuinely undeclared server stays. Matching the `mcp__<server>__` prefix instead
was the first attempt, and it exempted every tool of any server merely keyed
`curie__...`, which is the bypass `test_an_ambient_server_keyed_onto_a_platform_prefix_is_refused`
now holds shut.
"""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import anyio
import pytest
from claude_agent_sdk.types import (
    PermissionResultAllow,
    PermissionResultDeny,
    ToolPermissionContext,
)
from curie_runner import mcp_tool_capability
from curie_runner.approval import (
    APPROVAL_TOOL_NAME,
    ApprovalGate,
    build_approval_gate,
    build_approval_hook,
    build_can_use_tool,
    policy_disallowed_tools,
    resolve_approval_policy,
)
from mcp import Tool
from mcp.types import ToolAnnotations
from plugin_format import PLATFORM_PUBLISH_TOOL_NAME, ToolPolicy


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
        callback,
        {"tool_name": tool_name, "tool_input": tool_input or {}},
        "toolu_policy_test",
        None,
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


def _production_sre_gate(
    *, managed_workspace: bool, state_server_mounted: bool = True
) -> ApprovalGate:
    """Build the gate from the exact production SRE bundle policy and skill.

    `state_server_mounted` defaults True because that is the shape these tests
    are about: a session the worker gave a `CURIE_STATE_URL`, so the runner
    mounted `curie-state` and its five channel-memory tools genuinely exist.
    `__main__` stamps the same fact onto the gate from the expression that
    decides the mount; a session without a state URL publishes no
    `mcp__curie-state__*` tool and exempts none, which
    `test_the_state_tools_are_exempt_only_when_the_platform_mounted_them` pins.
    """

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
    gate.state_server_mounted = state_server_mounted
    return gate


def _interception_reason(
    gate: ApprovalGate,
    tool_name: str,
    interceptor: str,
    tool_input: dict[str, Any] | None = None,
) -> str:
    """The refusal one interception point produced, in its own words, or "".

    Both points share `_decide_gate` and must not disagree, so a test asking
    "was this refused, and by whom" should not also have to know which of the
    two result shapes the answer arrived in.
    """

    if interceptor == "hook":
        result = _hook_call(gate, tool_name, tool_input)
        return _reason(result) if _denied(result) else ""
    outcome = anyio.run(
        build_can_use_tool(gate),
        tool_name,
        tool_input or {},
        ToolPermissionContext(tool_use_id="toolu_policy_test"),
    )
    return outcome.message if isinstance(outcome, PermissionResultDeny) else ""


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
        build_can_use_tool(callback_gate),
        live_name,
        {},
        ToolPermissionContext(tool_use_id="toolu_policy_test"),
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
            build_can_use_tool(callback_gate),
            tool_name,
            {},
            ToolPermissionContext(tool_use_id="toolu_policy_test"),
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
        result = _hook_call(gate, PLATFORM_PUBLISH_TOOL_NAME, tool_input)
        assert _denied(result)
        reason = _reason(result)
    else:
        result = anyio.run(
            build_can_use_tool(gate),
            PLATFORM_PUBLISH_TOOL_NAME,
            tool_input,
            ToolPermissionContext(tool_use_id="toolu_policy_test"),
        )
        assert isinstance(result, PermissionResultDeny)
        reason = result.message

    assert gate.pending_gate_kind == "permission"
    assert gate.pending_granted_tool == PLATFORM_PUBLISH_TOOL_NAME
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
        result = anyio.run(
            build_can_use_tool(gate),
            APPROVAL_TOOL_NAME,
            {},
            ToolPermissionContext(tool_use_id="toolu_policy_test"),
        )
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
    result = anyio.run(
        callback, tool, {}, ToolPermissionContext(tool_use_id="toolu_policy_test")
    )
    assert type(result).__name__ == "PermissionResultDeny"


# --------------------------------------------------------------------------- #
# Platform-owned MCP servers are outside a bundle's toolPolicy (#2286)
#
# `curie` and `curie-state` are mounted by the runner, and a bundle cannot
# declare them (`plugin_format.connectors.RESERVED_CONNECTOR_NAMES` refuses the
# names at deploy). A bundle therefore cannot express a policy over them, so
# classifying them against one can only ever produce the unmatched default.
# Spelled as literal live names on purpose: these are the strings the SDK puts
# on the wire, and deriving them from the constants under test would make a
# rename of either server invisible here.

_CHANNEL_MEMORY_TOOLS = [
    "mcp__curie-state__get",
    "mcp__curie-state__set",
    "mcp__curie-state__append",
    "mcp__curie-state__list",
    "mcp__curie-state__delete",
]

# Publication refuses a request it cannot record, so its two fields ride every
# call to it. Nothing else here reads tool_input.
_PUBLISH_INPUT = {"title": "Workspace tool check", "body": "prior-turn marker"}


def _input_for(tool_name: str) -> dict[str, Any]:
    return dict(_PUBLISH_INPUT) if tool_name == PLATFORM_PUBLISH_TOOL_NAME else {}


@pytest.mark.parametrize("interceptor", ["hook", "callback"])
def test_a_tool_from_an_undeclared_server_is_refused_at_both_interception_points(
    interceptor: str,
) -> None:
    """The fail-closed default, pinned where the decision is taken rather than
    where the name is mapped.

    Mutation this exists to catch, and the one the implementer must run: in
    `_tool_policy_outcome`, change `if canonical is None: return
    ToolPolicyDecision.DENY` to `return None`. The whole suite stayed green
    under that mutation on the pre-fix base, because every other test reaches
    DENY through a pattern that matched. Widening the platform exemption is
    exactly the edit that invites a reader to relax this branch, so the
    exemption and its guard must land together.
    """

    gate = _gate(_policy(allow=["k8s-write/restart_deployment"]))
    reason = _interception_reason(gate, "mcp__not-declared__whatever", interceptor)

    assert "denied by this agent's tool policy" in reason
    # A refusal, not a request: an undeclared server has no audience to ask.
    _assert_no_approval_was_recorded(gate)


def test_the_shipped_sre_bundle_can_still_reach_its_channel_memory() -> None:
    """The #2286 fix pin: one test, one assertion, the shipped policy.

    Deliberately NOT parametrized, and deliberately the whole refused set in a
    single assertion. The fix-pin verifier reverses this change's product hunks,
    runs exactly this node, and requires the report to carry one testcase and
    one failure; a parametrized twin of this test produces ten failures and is
    refused as unattributable. The broader coverage lives in the parametrized
    tests below, which also drive the hook. This one exists to be the pin.

    Reversed, every name here comes back "denied by this agent's tool policy".
    """

    gate = _production_sre_gate(managed_workspace=False)

    refused = [
        tool_name
        for tool_name in _CHANNEL_MEMORY_TOOLS
        if "denied by this agent's tool policy"
        in _interception_reason(gate, tool_name, "callback")
    ]

    assert refused == [], (
        "a bundle's toolPolicy refused the platform's own channel-memory tools;"
        " the bundle cannot declare curie-state, so it can never allow them"
    )


@pytest.mark.parametrize("interceptor", ["hook", "callback"])
@pytest.mark.parametrize(
    "tool_name",
    [*_CHANNEL_MEMORY_TOOLS, APPROVAL_TOOL_NAME, PLATFORM_PUBLISH_TOOL_NAME],
)
def test_every_platform_owned_tool_escapes_the_production_sre_tool_policy(
    tool_name: str, interceptor: str
) -> None:
    """The defect, read from the shipped bundle rather than a fixture.

    The production SRE policy names only `kubernetes/*`, `grafana/*`, `tempo/*`
    and `self-upgrade/*`, which is the only policy it CAN name: the bundle is
    forbidden from declaring `curie` or `curie-state`. Every ADR-0095 channel
    memory tool therefore fell through to the deny default, and the agent was
    told its own memory was forbidden and not to retry. Loading the real bundle
    keeps this test tracking the policy that actually ships, so a future policy
    edit cannot quietly make the platform reachable for the wrong reason.

    `managed_workspace=True` throughout, so publication is gated exactly as the
    existing publication regression gates it; the channel-memory names are not
    in `gate.required` under either setting.
    """

    gate = _production_sre_gate(managed_workspace=True)

    reason = _interception_reason(gate, tool_name, interceptor, _input_for(tool_name))

    assert "denied by this agent's tool policy" not in reason
    if tool_name == PLATFORM_PUBLISH_TOOL_NAME:
        # Blocked, and rightly so: the platform permission gate still owns
        # publication. Outside policy scope is not permission to run.
        assert gate.pending_gate_kind == "permission"
        assert gate.pending_granted_tool == PLATFORM_PUBLISH_TOOL_NAME
    else:
        _assert_no_approval_was_recorded(gate)

    # A negative pin, not a restoration. The capability probe only ever observes
    # connector and bundle MCP servers, so a platform name never reaches this
    # projection in production and no catalog delta is expected from the fix.
    # Asserted anyway so the projection and the two interception points cannot
    # drift into disagreeing about the same name.
    assert policy_disallowed_tools(gate, [tool_name]) == ()


@pytest.mark.parametrize("interceptor", ["hook", "callback"])
def test_a_tool_the_platform_adds_later_is_exempt_without_a_second_list(
    interceptor: str,
) -> None:
    """The anti-drift property, carried by one source list rather than a prefix.

    Two exact names were exempted when publication landed, so every tool added
    to a platform server afterwards was denied on arrival; that is how channel
    memory broke, and re-enumerating names by hand would let it break again at
    the next tool. The first fix answered that by exempting the whole
    `mcp__<platform server>__` PREFIX, which handed the same bypass to any MCP
    server whose key merely begins `curie__` or `curie-state__`.

    So the exemption is now the exact set of names the platform publishes, and
    that set is RENDERED from `state._STATE_TOOL_SPECS` -- the one list
    `build_state_server` registers with the SDK. A sixth state tool is exempt
    the moment it is registered, and there is no second list to forget. Read
    out of the derivation here on purpose, then cross-checked against the
    literal names this file pins, so a rename that breaks the live wire format
    still reddens.
    """

    # Function-local on purpose. STATE_TOOL_NAMES does not exist on the base,
    # so importing it at module scope would make every test in this file fail to
    # COLLECT whenever the product hunks are reversed, and a collection error is
    # not attributable to any one test. The fix-pin verifier refuses that, and
    # so should a reader trying to tell which assertion actually bit.
    from curie_runner.state import STATE_TOOL_NAMES

    gate = _production_sre_gate(managed_workspace=False)

    assert set(_CHANNEL_MEMORY_TOOLS) == set(STATE_TOOL_NAMES), (
        "the live names build_state_server publishes and the literal names this"
        " file pins must be the same set, or one of the two is stale"
    )
    for tool_name in sorted(STATE_TOOL_NAMES):
        reason = _interception_reason(gate, tool_name, interceptor)
        assert "denied by this agent's tool policy" not in reason

    _assert_no_approval_was_recorded(gate)


@pytest.mark.parametrize("interceptor", ["hook", "callback"])
@pytest.mark.parametrize(
    "tool_name",
    ["mcp__curie__extra__foo", "mcp__curie-state__extra__bar"],
    ids=["curie", "state"],
)
def test_an_ambient_server_keyed_onto_a_platform_prefix_is_refused(
    tool_name: str, interceptor: str
) -> None:
    """The regression this fix closes, and it was reachable rather than theoretical.

    `ClaudeAgentOptions.strict_mcp_config` defaults to False and
    `adapter.build_options` never sets it, so the CLI loads project `.mcp.json`,
    user settings and plugin servers BESIDE the `--mcp-config` dict
    `build_mcp_servers` controls; `check.py::evaluate` already treats those
    ambient servers as real. A mounted workspace is the session cwd, it is
    writable, and it survives across sandboxes, so a `.mcp.json` planted there
    is bundle-influenced input. An ambient server keyed `curie__extra` publishes
    `mcp__curie__extra__foo`, which carries the `mcp__curie__` prefix while
    being no tool the platform ever mounted.

    A connector cannot do this -- a connector name may not contain `_` -- which
    is why the earlier negative table (hyphen and concatenation neighbours such
    as `mcp__curie-state-archive__get`) never tried these two shapes and a
    prefix check passed it. Exact membership refuses both, and the names stay on
    the unmatched-is-DENY default that defends every other undeclared server.
    """

    gate = _gate(_policy(allow=["k8s-write/restart_deployment"]), state_server_mounted=True)
    reason = _interception_reason(gate, tool_name, interceptor)

    assert "denied by this agent's tool policy" in reason
    # A refusal, not a request: an undeclared server has no audience to ask.
    _assert_no_approval_was_recorded(gate)


@pytest.mark.parametrize("interceptor", ["hook", "callback"])
@pytest.mark.parametrize("mounted", [False, True], ids=["unmounted", "mounted"])
def test_the_state_tools_are_exempt_only_when_the_platform_mounted_them(
    mounted: bool, interceptor: str
) -> None:
    """The exemption is scoped to what THIS session actually published.

    `curie-state` mounts only when `resolve_state_client` returns a client,
    which needs `CURIE_STATE_URL`. With no state URL the platform publishes no
    `mcp__curie-state__*` tool at all, so a call wearing that name came from an
    ambient server and must land on the deny default -- exempting it would grant
    a bundle-influenced server a policy bypass for a platform capability the
    session does not even have. With the server mounted the same name is Curie's
    own channel memory and stays outside policy scope, which is #2286's fix.

    Both rulings in one table because asserting either alone passes against a
    flag that is ignored.
    """

    gate = _gate(_policy(allow=["k8s-write/restart_deployment"]), state_server_mounted=mounted)
    reason = _interception_reason(gate, "mcp__curie-state__get", interceptor)

    assert ("denied by this agent's tool policy" in reason) is not mounted
    _assert_no_approval_was_recorded(gate)


@pytest.mark.parametrize("interceptor", ["hook", "callback"])
@pytest.mark.parametrize(
    ("pattern", "tool_name"),
    [
        ("*/get", "mcp__curie-state__get"),
        ("*/publish_changes", PLATFORM_PUBLISH_TOOL_NAME),
    ],
    ids=["channel-memory", "publication"],
)
def test_a_wildcarded_server_segment_cannot_reach_a_platform_tool(
    pattern: str, tool_name: str, interceptor: str
) -> None:
    """The deploy validator never cross-checks a wildcarded server segment, by
    design, so `*/get` is the one pattern shape that can be written against a
    server the bundle may not name.

    It is inert, and this is where that is true rather than merely intended: the
    exemption is taken before `classify_tool` is ever consulted, so the pattern
    has nothing to match. If the exemption were ever moved after classification,
    a bundle could fence its own approval path with one wildcard.
    """

    gate = _gate(_policy(deny=[pattern]), state_server_mounted=True)
    reason = _interception_reason(gate, tool_name, interceptor, _input_for(tool_name))

    assert reason == ""
    _assert_no_approval_was_recorded(gate)


@pytest.mark.parametrize("interceptor", ["hook", "callback"])
@pytest.mark.parametrize(
    "tool_name", ["mcp__curie-state__get", APPROVAL_TOOL_NAME], ids=["state", "curie"]
)
def test_each_platform_server_covers_its_own_tools(
    tool_name: str, interceptor: str
) -> None:
    """Both directions of the overlapping-name pair, in one table.

    `curie` is a prefix of `curie-state`, so a check that answered by walking
    server prefixes could attribute `mcp__curie-state__get` to `curie` and leave
    a remainder that is not a tool name. Exact membership in the union of what
    both servers publish has no ordering to get wrong, and asserting only one of
    these two names would pass against a set that knew about one server and not
    the other.
    """

    gate = _gate(_policy(deny=["*/*"]), state_server_mounted=True)
    reason = _interception_reason(gate, tool_name, interceptor)

    assert reason == ""


@pytest.mark.parametrize(
    "tool_name",
    [
        "mcp__curie-stat__get",
        "mcp__curie-state-archive__get",
        "mcp__curiestate__get",
        "mcp__curied__get",
    ],
)
def test_a_server_that_merely_shares_a_platform_prefix_is_still_refused(
    tool_name: str,
) -> None:
    """The companion negative: ownership is the whole `mcp__<server>__` prefix.

    A check written as a bare `startswith("mcp__curie")` would answer yes for
    every name here, and each one is a server the bundle never declared. Without
    this table, a prefix check and an exact-prefix check are indistinguishable.
    """

    gate = _gate(_policy(allow=["k8s-write/restart_deployment"]))
    assert "denied by this agent's tool policy" in _interception_reason(
        gate, tool_name, "hook"
    )


@pytest.mark.parametrize("interceptor", ["hook", "callback"])
def test_a_plugin_mounted_bundle_server_named_curie_stays_inside_policy_scope(
    interceptor: str,
) -> None:
    """The bypass a substring check would hand a bundle.

    Nothing stops a bundle from calling one of its own MCP servers `curie`: the
    reserved-name refusal covers connectors, and a plugin mount is namespaced
    instead, arriving as `mcp__plugin_<bundle>_curie__<tool>`. That name
    contains the platform server's name and is not the platform, so it must
    stay fully governed. An implementation matching on `"curie" in tool_name`
    would let a bundle mint an unpoliced server by naming it after the platform.
    """

    gate = ApprovalGate(
        tool_policy=_policy(allow=["curie/read_state"]),
        bundle_name="sre-bot",
        mcp_servers={"curie"},
        connector_servers=set(),
    )
    reason = _interception_reason(
        gate, "mcp__plugin_sre-bot_curie__delete_everything", interceptor
    )

    assert "denied by this agent's tool policy" in reason
    _assert_no_approval_was_recorded(gate)


@pytest.mark.parametrize("interceptor", ["hook", "callback"])
@pytest.mark.parametrize(
    ("policy", "refused"),
    [(_policy(deny=["curie-docs/search"]), True), (_policy(allow=["curie-docs/search"]), False)],
    ids=["denied", "allowed"],
)
def test_a_connector_named_after_the_platform_still_obeys_its_policy(
    policy: ToolPolicy, refused: bool, interceptor: str
) -> None:
    """`RESERVED_CONNECTOR_NAMES` fences two exact names, not the `curie-`
    prefix, so `curie-docs` is a connector a bundle may legitimately ship.

    Both rulings are asserted: exempting it would silently unpolice a real
    connector, and refusing it outright would make the deny rule unfalsifiable.
    """

    gate = ApprovalGate(
        tool_policy=policy,
        bundle_name="sre-bot",
        mcp_servers=set(),
        connector_servers={"curie-docs"},
    )
    reason = _interception_reason(gate, "mcp__curie-docs__search", interceptor)

    assert ("denied by this agent's tool policy" in reason) is refused


@pytest.mark.parametrize(
    "tool_name", ["mcp__curie__", "mcp__curie-state__"], ids=["curie", "state"]
)
def test_a_platform_prefix_with_no_tool_left_ends_at_the_deny_default(
    tool_name: str,
) -> None:
    """`mcp__curie__` names no tool, so nothing owns it and nothing may exempt it.

    `canonical_tool_name` already refuses this shape rather than inventing an
    empty tool, and the ownership check has to agree: treating a bare prefix as
    platform-owned would turn a malformed name into a pass.
    """

    gate = _gate(_policy(allow=["k8s-write/restart_deployment"]))
    assert "denied by this agent's tool policy" in _interception_reason(
        gate, tool_name, "hook"
    )


@pytest.mark.parametrize("interceptor", ["hook", "callback"])
def test_an_operator_gate_on_a_platform_tool_still_blocks(interceptor: str) -> None:
    """Outside policy scope is not permission to run (ADR-0139).

    The exemption answers one question only: whether the bundle's policy has
    anything to say about this call. The operator gate, the bundle's
    `approvalPolicy` and the platform's own gates all still apply afterwards. An
    implementation that returned early from the decision instead of only from
    the classification would hollow out the operator layer while every
    policy test here stayed green.
    """

    gate = _gate(
        _policy(allow=["k8s-write/restart_deployment"]),
        required=frozenset({APPROVAL_TOOL_NAME}),
    )
    reason = _interception_reason(gate, APPROVAL_TOOL_NAME, interceptor)

    assert reason != "", "an operator-gated platform tool must stay gated"
    assert "denied by this agent's tool policy" not in reason
    assert gate.pending_summary is not None, "and it must still ask a human"
