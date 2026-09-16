"""Live SDK tool name -> the canonical `"<server>/<tool>"` a policy speaks.

`plugin_format.tool_policy` states the split of work outright: "Mapping canonical
-> live SDK name is the runtime lane's job and is out of scope here." This is
that mapping run backwards, and these tests pin the two things that make it more
than a string split, plus the specificity rule for overlapping server names.

**Underscores.** Neither mount shape can be parsed by splitting on `__`, because
a server name may contain them. The known-server sets are what disambiguate, and
a test that only ever uses hyphenated names would pass with a naive split.

**Specificity.** When more than one declared server prefix matches, the longest
server name owns the tool. Otherwise a shorter server can shadow a more specific
policy.

**The refusal.** `None` for an `mcp__` name is not "ungoverned", it is "could not
attribute this to a declared server". Collapsing that with "not an MCP tool" is
the fail-open the unclassified-is-denied default exists to prevent, which is why
`is_mcp_tool` is asked separately.
"""

import anyio
from claude_agent_sdk.types import PermissionResultDeny, ToolPermissionContext
from curie_runner.approval import (
    ApprovalGate,
    build_approval_hook,
    build_can_use_tool,
    canonical_tool_name,
    is_mcp_tool,
    policy_disallowed_tools,
)
from plugin_format import ToolPolicy


def _canonical(live: str, **kwargs: object) -> str | None:
    defaults: dict[str, object] = {
        "bundle_name": "sre-bot",
        "mcp_servers": set(),
        "connector_servers": set(),
    }
    defaults.update(kwargs)
    return canonical_tool_name(live, **defaults)  # type: ignore[arg-type]


def test_a_builtin_is_not_an_mcp_tool() -> None:
    # A policy pattern is `<server>/<tool>`, so `Bash` cannot appear in one and
    # is not governed. Answering this separately is what stops a policy about
    # connectors from revoking every built-in.
    assert not is_mcp_tool("Bash")
    assert not is_mcp_tool("Read")
    assert is_mcp_tool("mcp__k8s-write__restart_deployment")


def test_a_connector_tool_maps_to_its_bare_server() -> None:
    assert (
        _canonical(
            "mcp__k8s-write__restart_deployment",
            connector_servers={"k8s-write"},
        )
        == "k8s-write/restart_deployment"
    )


def test_a_plugin_mounted_tool_carries_the_bundle_infix() -> None:
    assert (
        _canonical("mcp__plugin_sre-bot_probe__ping", mcp_servers={"probe"})
        == "probe/ping"
    )


def test_an_underscored_server_name_still_resolves() -> None:
    """The case a naive split on `__` gets wrong, in both shapes."""
    assert (
        _canonical("mcp__k8s_write__restart_deployment", connector_servers={"k8s_write"})
        == "k8s_write/restart_deployment"
    )
    assert (
        _canonical("mcp__plugin_sre-bot_my_probe__ping", mcp_servers={"my_probe"})
        == "my_probe/ping"
    )


def test_overlapping_plugin_servers_use_the_longest_policy_name_everywhere() -> None:
    live_short = "mcp__plugin_sre-bot_logs__read"
    live_long = "mcp__plugin_sre-bot_logs__audit__export"

    def gate() -> ApprovalGate:
        return ApprovalGate(
            tool_policy=ToolPolicy(
                enforcement="curie/mcp-tool-policy@1",
                approvalRequired=["logs__audit/export"],
                deny=["logs/*"],
            ),
            bundle_name="sre-bot",
            mcp_servers={"logs", "logs__audit"},
            connector_servers=set(),
        )

    assert (
        _canonical(live_long, mcp_servers={"logs", "logs__audit"})
        == "logs__audit/export"
    )
    assert policy_disallowed_tools(gate(), [live_short, live_long]) == (live_short,)

    hook_gate = gate()
    hook = build_approval_hook(hook_gate)["PreToolUse"][0].hooks[0]
    hook_result = anyio.run(
        hook, {"tool_name": live_long, "tool_input": {}}, None, None
    )
    assert hook_result["hookSpecificOutput"]["permissionDecision"] == "deny"
    assert "denied by this agent's tool policy" not in hook_result["stopReason"]
    assert hook_gate.pending_granted_tool == live_long

    callback_gate = gate()
    callback_result = anyio.run(
        build_can_use_tool(callback_gate),
        live_long,
        {},
        ToolPermissionContext(),
    )
    assert isinstance(callback_result, PermissionResultDeny)
    assert "denied by this agent's tool policy" not in callback_result.message
    assert callback_gate.pending_granted_tool == live_long


def test_an_underscored_tool_name_survives_intact() -> None:
    assert (
        _canonical(
            "mcp__self-upgrade__upgrade_platform",
            connector_servers={"self-upgrade"},
        )
        == "self-upgrade/upgrade_platform"
    )


def test_an_undeclared_server_is_a_refusal_not_a_pass() -> None:
    """The drift the unclassified default exists to catch.

    A connector image that starts advertising a tool from a server this bundle
    never declared must not read as "no policy applies to it".
    """
    assert _canonical("mcp__rogue__delete_everything") is None
    assert is_mcp_tool("mcp__rogue__delete_everything")


def test_an_unknowable_server_set_refuses_rather_than_guessing() -> None:
    """`None` is the `declared_mcp_server_names` poison: unknowable, not empty."""
    assert _canonical("mcp__probe__ping", mcp_servers=None) is None
    assert _canonical("mcp__probe__ping", connector_servers=None) is None


def test_a_connector_wins_a_name_readable_as_either_shape() -> None:
    """Deterministic, and stated in the helper: the bare form is preferred.

    A connector literally named `plugin_<bundle>_<server>` produces a live name
    that also parses as a plugin mount. Something has to decide, and silently
    depending on set iteration order would make the answer vary per process.
    """
    live = "mcp__plugin_sre-bot_probe__ping"
    assert (
        _canonical(
            live,
            connector_servers={"plugin_sre-bot_probe"},
            mcp_servers={"probe"},
        )
        == "plugin_sre-bot_probe/ping"
    )


def test_a_prefix_with_no_tool_left_is_a_refusal() -> None:
    assert _canonical("mcp__k8s-write__", connector_servers={"k8s-write"}) is None


def test_no_bundle_name_cannot_resolve_a_plugin_mount() -> None:
    """The plugin prefix is built from the bundle name; without one there is no
    prefix to match, and guessing would attribute a tool to a server on the
    strength of a substring."""
    assert (
        _canonical(
            "mcp__plugin_sre-bot_probe__ping", bundle_name=None, mcp_servers={"probe"}
        )
        is None
    )
