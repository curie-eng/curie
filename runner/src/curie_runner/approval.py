"""Approval gates in the runner: the policy-gate tool and the permission gate.

ADR-0010 (approval gates, epic #22) defines one approval primitive with two
trigger types, and this module is the runner half of both:

- **Policy gate** (#244): the agent's own logic decides something needs a
  human decision. An in-process SDK MCP tool
  (``mcp__curie__request_approval``) is carried when the live MCP surface has
  a potentially mutating tool or an explicit approval gate. A fully observed
  surface whose tools all declare ``readOnlyHint=true`` omits it, so a model
  cannot page a human for an action the session cannot perform. The call
  executes no real-world action; it only marks the turn, and the session emits
  its terminal ``final`` with ``status=awaiting-approval``.
- **Permission gate** (#245): configuration marks a tool as
  approval-required, and the runner intercepts the model-initiated call
  proactively through the SDK ``can_use_tool`` callback -- the replacement
  for the previously hardcoded ``permission_mode="bypassPermissions"``. The
  gated call is denied (never executed), the ``ApprovalGate`` records what
  was blocked, and the turn ends ``awaiting-approval`` exactly like a policy
  gate, so the durable-record/suspend/resume lifecycle (#244) is shared.

The durable ``Approval`` record, the authorizer, and the resume trigger all
live server-side with the worker/API (they must not be spoofable from inside
the sandbox -- see docs/interfaces/approval/INTERFACE.md); the runner's only
job is to end the turn in the awaiting-approval state.

Policy-gate detection is wire-level, not execution-level: ``translate.py``
captures the ``ToolUseBlock`` naming the request tool, so the fake-model path
(a scripted ``ToolUseBlock``, no tool execution, no network) exercises the
identical seam as a real model call.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, NamedTuple

import yaml
from claude_agent_sdk import HookMatcher, create_sdk_mcp_server, tool
from claude_agent_sdk.types import (
    CanUseTool,
    McpSdkServerConfig,
    PermissionResultAllow,
    PermissionResultDeny,
    ToolPermissionContext,
)
from plugin_format import (
    TOOL_POLICY_ENFORCEMENT,
    ApprovalPolicy,
    PluginManifest,
    ToolPolicy,
    ToolPolicyDecision,
    ToolPolicyInvalid,
    ToolPolicyUnenforceable,
    classify_tool,
    connector_server_names,
    connector_tool_prefix,
    declared_mcp_server_names,
    effective_operator_gates,
    effective_tool_prefix,
    grantable_routes,
    load_tool_policy,
    parse_allowed_tools,
    render_gate_summary,
    resolve_manifest,
)

logger = logging.getLogger(__name__)

# Best-effort only (#712): NOT authoritative, NOT used for any gating
# decision -- only to decide whether a bare operator gate name is worth a
# "did you mean mcp__<server>__<tool>?" warning. The SDK does not export an
# enumerable built-in tool list, and this set may drift from a future SDK
# version; a name missing from here is not an error, just an unwarned pass,
# and a name present here that isn't actually offered in a given session is
# similarly harmless (it just arms nothing, same as today).
#
# Provenance (#736): the names below are read out of the CLI the SDK actually
# ships, not guessed. runner/pyproject.toml pins claude-agent-sdk>=0.2.115; that
# version bundles Claude Code CLI 2.1.206 (claude_agent_sdk/_cli_version.py),
# binary at claude_agent_sdk/_bundled/claude. Two reproducible sources:
#
# 1. Tool-name string constants. Note the extraction pattern must NOT anchor on
#    "var ": the minified bundle chains declarators, e.g.
#    var gH="CronCreate",gU="CronDelete",CVt="CronList" , so a var-anchored
#    regex captures only the first name and silently drops its siblings (that
#    is how CronDelete/CronList went missing on the first pass). Use:
#      grep -ao '[A-Za-z_$][A-Za-z_$0-9]*="[A-Z][A-Za-z]*"[,;]' <binary> \
#        | sed 's/.*="//;s/"[,;]//' | sort -u
# 2. The alias-to-canonical map, reproducible with:
#      grep -ao '.\{300\}BashOutputTool:"TaskOutput".\{0,400\}' <binary>
#    which yields verbatim:
#      {Task:"Agent",KillShell:"TaskStop",KillBash:"TaskStop",
#       AgentOutputTool:"TaskOutput",BashOutputTool:"TaskOutput",
#       AgentOutput:"TaskOutput",BashOutput:"TaskOutput",
#       ListPeers:"ListAgents",Brief:"SendUserMessage",
#       ListMcpResources:"ListMcpResourcesTool",
#       ReadMcpResource:"ReadMcpResourceTool",
#       ReadMcpResourceDir:"ReadMcpResourceDirTool"}
#
# The bound: this set holds ONLY names the SDK ``can_use_tool`` callback can
# actually observe, which is the CANONICAL tool-name set. It is deliberately NOT
# the CLI's whole recognition surface, because the two differ on alias keys.
#
# Why the alias map above is EXCLUDED rather than included. The bundled CLI
# consumes that map through a resolver, ``R2(e){return Object.hasOwn(Hti,e)?
# Hti[e]:e}``, and calls it only from its permission-RULE string parser
# (``{toolName:R2(e)}``) -- the settings.json allow/deny path. Curie's operator
# approval gate does not go through that path at all: ``build_approval_gate``
# arms names verbatim into ``gate.required`` and ``build_can_use_tool`` compares
# the SDK-reported tool name by exact equality, and the SDK reports the CANONICAL
# name. So an operator gating an alias (``ListMcpResources``, canonical
# ``ListMcpResourcesTool``) arms a literal that never matches: the gate silently
# arms nothing. For this gate an alias name IS a probable no-op, so its warning
# is a TRUE positive, not the false positive #736 set out to fix, and suppressing
# it would be exactly the silent fail-open #712 built the warning to surface.
# ``Task``, ``BashOutput``, and ``KillShell`` were in the historical pre-#736 set
# and are dropped for this reason (they are alias keys for ``Agent``,
# ``TaskOutput``, and ``TaskStop``); the canonical names stay.
#
# The inclusion criterion (apply this, do not re-derive it). A name is included
# when the bundled CLI does either of:
#   (a) declares it as a tool-name string constant AND carries a tool marker
#       next to it: a tool description string, a userFacingName(), or an
#       aliases:[...] registry entry, AND it is the canonical name rather than an
#       alias key from the map above; or
#   (b) still recognizes it in a permission-matcher list (the MultiEdit /
#       NotebookRead case).
# A bare capitalized string constant with no tool marker does NOT qualify, and an
# alias key NEVER qualifies.
#
# Recorded exclusions, so the raw-sweep diff is pre-resolved:
#   SlashCommand      zero occurrences of the literal in the binary.
#   TestingPermission real registry entry, but isEnabled(){return!1}, so it is
#                     never offered.
#   ConnectGitHub     name constant with no tool marker next to it, fails (a).
#   everything else the raw sweep returns (error classes, HTTP verbs,
#   credential types, Zod types, syntax-highlighter language names) fails (a).
#
# MultiEdit and NotebookRead are retained without a registry entry in 2.1.206:
# they have no tool description or name constant, but the CLI's live permission
# matcher still lists them (filePatternTools includes NotebookRead, and the
# write-tool deny set includes MultiEdit), so they remain legacy names the CLI
# recognizes in permission rules and gating them is not an operator mistake.
_KNOWN_BUILTIN_TOOLS = frozenset(
    {
        # Canonical tool names: criterion (a).
        "Agent",
        "Artifact",
        "AskUserQuestion",
        "Bash",
        "Cd",
        "ClaudeDesign",
        "CronCreate",
        "CronDelete",
        "CronList",
        "DesignSync",
        "Edit",
        "EndConversation",
        "EnterPlanMode",
        "EnterWorktree",
        "ExitPlanMode",
        "ExitWorktree",
        "Glob",
        "Grep",
        "LSP",
        "ListAgents",
        "ListConnectors",
        "ListMcpResourcesTool",
        "Monitor",
        "NotebookEdit",
        "ObserverReport",
        "PowerShell",
        "Projects",
        "PushNotification",
        "REPL",
        "Read",
        "ReadMcpResourceDirTool",
        "ReadMcpResourceTool",
        "RemoteTrigger",
        "ReportFindings",
        "ScheduleWakeup",
        "SearchMcpRegistry",
        "SendMessage",
        "SendUserFile",
        "SendUserMessage",
        "ShareOnboardingGuide",
        "ShowOnboardingRolePicker",
        "Skill",
        "StructuredOutput",
        "SuggestConnectors",
        "TaskCreate",
        "TaskGet",
        "TaskList",
        "TaskOutput",
        "TaskStop",
        "TaskUpdate",
        "TodoWrite",
        "ToolSearch",
        "WaitForMcpServers",
        "WebFetch",
        "WebSearch",
        "Workflow",
        "Write",
        # Legacy names the permission matcher still recognizes: criterion (b).
        "MultiEdit",
        "NotebookRead",
    }
)

# The server key under ClaudeAgentOptions.mcp_servers; the SDK prefixes tool
# names as mcp__<server>__<tool>.
APPROVAL_SERVER_NAME = "curie"
_TOOL_NAME = "request_approval"
# The fully qualified tool identifier as it appears on ToolUseBlock.name.
APPROVAL_TOOL_NAME = f"mcp__{APPROVAL_SERVER_NAME}__{_TOOL_NAME}"

# Platform-owned remote-development publication gate.  This is deliberately
# mounted beside the policy tool rather than shipped by a bundle: a bundle is
# untrusted input and must not be able to remove, execute, or grant its own
# publication action.  The worker recognizes this exact runner-stamped
# permission-gate provenance before it captures a patch.
_PUBLISH_TOOL = "publish_changes"
PUBLISH_TOOL_NAME = f"mcp__{APPROVAL_SERVER_NAME}__{_PUBLISH_TOOL}"

_PUBLISH_DESCRIPTION = (
    "When a managed repository is mounted, work only in /workspace and preserve"
    " existing changes. Do not push with git. Before requesting publication,"
    " identify the repository's own documented test or check command for the"
    " area you changed, run it from /workspace, and report the exact command,"
    " its exit status, and a concise result in the session thread. If you"
    " cannot identify or run an appropriate command, report that and do not"
    " publish. If the command fails, report the failure and do not publish. If"
    " verification generates artifacts, do not publish unrequested artifacts:"
    " use the repository's documented cleanup procedure when one exists and"
    " remove only artifacts this verification created, never requested or"
    " unrelated work; otherwise report the generated artifacts in the session"
    " thread and do not publish. When the changes are ready, use"
    " this tool to request human approval for publication. The platform will"
    " capture the patch, ask for approval in the requesting thread, and publish"
    " it from a separate trusted job only after approval. This tool never"
    " publishes changes itself. After calling it, end your turn and tell the"
    " user the publication request is pending."
)
_PUBLISH_SCHEMA = {
    "type": "object",
    "properties": {
        "title": {
            "type": "string",
            "description": "Short proposed pull request title.",
            "minLength": 1,
            "maxLength": 240,
        },
        "body": {
            "type": "string",
            "description": "Optional proposed pull request description.",
            "maxLength": 65_536,
        },
    },
    "required": ["title"],
}

_TOOL_DESCRIPTION = (
    "Request human approval before proceeding. Call this when your"
    " instructions say a step needs sign-off (a discount, an invoice, a"
    " remediation). Pass a one-line summary of exactly what needs approval,"
    " and, when your instructions name an approval route for this kind of"
    " decision, pass it as route (the platform delivers the request to that"
    " route's channel). After calling it, end your turn and tell the user the"
    " request is pending; the platform pauses the session and resumes it with"
    " the decision once an authorized human resolves it."
)

# Full JSON schema (not the shorthand type map) so ``route`` is optional: a
# request without a route falls back to the requesting channel.
_TOOL_SCHEMA = {
    "type": "object",
    "properties": {
        "summary": {
            "type": "string",
            "description": "One line stating exactly what needs approval.",
        },
        "route": {
            "type": "string",
            "description": "Optional approval route name from your instructions.",
        },
    },
    "required": ["summary"],
}


def _approval_error(text: str) -> dict[str, Any]:
    return {"content": [{"type": "text", "text": text}], "is_error": True}


_APPROVAL_OK = {
    "content": [
        {
            "type": "text",
            "text": (
                "Approval requested. The session will pause awaiting a human"
                " decision; end your turn now and tell the user the request"
                " is pending."
            ),
        }
    ]
}


def _distinct_routes(gate: ApprovalGate | None) -> list[str]:
    """The distinct manifest routes the gate declares, normalized and sorted.

    The route names are compared case-sensitively (Decision B): they are
    operator-authored identifiers, and ``load_approval_policy`` plus the agent's
    ``approval_routes`` binding map are both exact-match dict lookups, so
    case-folding here alone would accept a route the binding map then misses.
    """

    if gate is None:
        return []
    return sorted({r for r in gate.route_by_tool.values() if r})


def build_approval_server(
    gate: ApprovalGate | None = None,
    *,
    managed_workspace: bool = False,
    include_request_approval: bool = True,
) -> McpSdkServerConfig:
    """Build the in-process MCP server carrying applicable approval tools.

    Per-gate (#544, Decision B): the tool closes over ``gate`` so it can
    validate the model-supplied ``route`` against the manifest routes the gate
    declares, refusing with an ``is_error`` result -- which reaches the model
    and names the valid routes so it can retry within the same turn -- when the
    route is ambiguous (omitted with >1 declared) or unknown. A refused request
    creates no approval, so nothing silently widens to the requesting channel.
    ``gate`` is optional so a server with no manifest routes stays a generic
    policy approval (ADR-0034's channel-membership default).

    The route string is normalized identically to ``load_approval_policy`` (a
    ``.strip()``) before comparison so a route that validates green at deploy
    can never fail to match at runtime (#453, the validator/runtime split that
    shipped two silent fail-opens).

    ``include_request_approval=False`` omits only the generic policy gate. A
    managed workspace still carries ``publish_changes``, whose separate
    permission gate corresponds to an action the platform can actually perform.
    """

    @tool(_TOOL_NAME, _TOOL_DESCRIPTION, _TOOL_SCHEMA)
    async def request_approval(args: dict[str, Any]) -> dict[str, Any]:
        return process_approval_request(gate, args)

    tools = [request_approval] if include_request_approval else []

    @tool(_PUBLISH_TOOL, _PUBLISH_DESCRIPTION, _PUBLISH_SCHEMA)
    async def publish_changes(_args: dict[str, Any]) -> dict[str, Any]:
        # Discovery is unconditional so every session carries the publication
        # protocol. Authority is still mount-keyed in ``build_approval_gate``:
        # an unmounted session cannot create a publication approval, and a
        # direct invocation fails without mutating gate state.
        if not managed_workspace:
            return _approval_error(
                "No managed repository workspace is mounted at /workspace; "
                "publication cannot be requested from this session."
            )
        # Defence in depth: the permission callback must deny the call before
        # execution. If a harness bypasses that callback, the in-process tool
        # still performs no action and grants no capability.
        return _approval_error(
            "Publication is performed only by the platform after human approval; "
            "this sandbox tool cannot execute it directly."
        )

    tools.append(publish_changes)

    return create_sdk_mcp_server(
        name=APPROVAL_SERVER_NAME,
        version="1.0.0",
        tools=tools,
    )


def resolve_policy_route(
    declared: list[str], raw_route: Any
) -> tuple[bool, str | None, str | None]:
    """The pure route-resolution decision table (#544 Decision B, #561).

    Returns ``(rejected, resolved_route, error_message)`` for a model-supplied
    ``raw_route`` against the gate's ``declared`` manifest routes:

    - no declared routes -> generic policy approval, ``(False, None, None)``;
    - omitted route, exactly one declared -> auto-bind it, unambiguous;
    - omitted route, more than one declared -> refuse (ambiguous);
    - a declared route -> accept it;
    - an unknown route -> refuse.

    The route is normalized identically to ``load_approval_policy`` (a
    ``.strip()``) so a route that validates green at deploy cannot fail to match
    at runtime (#453). This is the ONE decision table both the real SDK path (the
    ``request_approval`` tool) and the offline fake path share, so the fake tier
    cannot silently widen a card to the requesting channel (#561).
    """

    route = str(raw_route).strip() if raw_route is not None else ""
    if not declared:
        return (False, None, None)
    if not route:
        if len(declared) == 1:
            return (False, declared[0], None)
        return (
            True,
            None,
            "Rejected: this decision has more than one approval route;"
            f" pass route as one of: {', '.join(declared)}.",
        )
    if route in declared:
        return (False, route, None)
    return (
        True,
        None,
        f"Rejected: unknown approval route {route!r}; pass route as one of: {', '.join(declared)}.",
    )


def process_approval_request(gate: ApprovalGate | None, args: dict[str, Any]) -> dict[str, Any]:
    """Apply one ``request_approval`` call to ``gate`` and return the model result.

    The single source of truth for the approval-request outcome, invoked from
    BOTH the in-process MCP tool (real SDK path) and ``FakeModelSession`` (offline
    path), so the two tiers resolve routes identically (#561). It mutates the
    sticky policy flags on ``gate`` and returns the tool result the model sees.
    """

    if gate is not None:
        # policy_requested is sticky-True: any call this turn means a policy
        # gate was raised. The per-request outcome (rejected/route), though,
        # must be fully determined by THIS call so the last call in the turn
        # wins -- a retry with a valid route after an is_error refusal must
        # clear the prior rejection, or _merge_gate_block drops the approval
        # the retry created (#544, Decision B recovery path).
        gate.policy_requested = True
        gate.policy_rejected = False
        gate.policy_route = None
    summary = str(args.get("summary") or "").strip()
    if not summary:
        if gate is not None:
            gate.policy_rejected = True
        return _approval_error("Rejected: pass a non-empty summary of what needs approval.")
    rejected, route, error = resolve_policy_route(_distinct_routes(gate), args.get("route"))
    if rejected:
        if gate is not None:
            gate.policy_rejected = True
        assert error is not None
        return _approval_error(error)
    if route is not None and gate is not None:
        gate.policy_route = route
    return _APPROVAL_OK


# --- The permission gate (#245): canUseTool over approval-required tools --------

# Longest tool-input rendering carried into an approval summary. The summary is
# shown to a human approver and persisted on the Approval record; a multi-KB
# tool input would drown both, so it is truncated with an ellipsis marker.
_SUMMARY_INPUT_LIMIT = 300

# The permission-gate summary prefix, in one place so the worker can pin against
# it (#430, ADR-0035): the worker treats a persisted approval summary starting
# with this prefix as a permission-gate block eligible for the one-shot grant,
# and reads the approved tool name from it. Keep summarize_tool_call's output
# identical to today -- this only names the shared literal.
#
# This prefix is a RESERVED namespace owned by ``summarize_tool_call`` -- the
# runner is the single writer of a genuine ``can_use_tool`` denial summary. Only
# machine-generated permission-gate summaries may live in it. Model- and
# skill-authored (policy-gate) summaries come from the model's own
# ``request_approval(summary=...)`` argument and are guarded out of this
# namespace by ``guard_reserved_summary`` before they are stored, so the worker's
# "summary starts with the prefix" check is an authoritative provenance signal: a
# prompt-injected agent cannot forge a real permission-gate block by naming a tool
# in its summary and thereby mint an unreviewed one-shot bypass grant (#430,
# ADR-0035, security).
APPROVAL_SUMMARY_PREFIX = "Tool call awaiting approval: "

# Prepended to a model/skill-authored summary that would otherwise collide with
# the reserved permission-gate namespace, neutralizing the forgery while leaving
# the human-facing text readable.
_RESERVED_SUMMARY_MARKER = "[agent-requested] "


def guard_reserved_summary(summary: str) -> str:
    """Keep a policy-gate summary out of the reserved permission-gate namespace.

    ``APPROVAL_SUMMARY_PREFIX`` is reserved for machine-generated permission-gate
    summaries (``summarize_tool_call``), which the worker trusts as proof that a
    real ``can_use_tool`` denial occurred before minting a one-shot bypass grant.
    A model/skill-authored summary is attacker-influenced, so if it starts with
    the reserved prefix this prepends a neutralizing marker so the result no
    longer does; any other summary is returned unchanged (#430, ADR-0035).
    """

    if summary.startswith(APPROVAL_SUMMARY_PREFIX):
        return f"{_RESERVED_SUMMARY_MARKER}{summary}"
    return summary


# What the denied model is told. It must steer the model to end the turn (so
# the session can emit the awaiting-approval final and the platform can
# suspend), and to not spin on retries -- a retried call is denied identically.
_DENY_MESSAGE = (
    "This tool call requires human approval and was NOT executed. An approval"
    " request has been recorded; the session will pause for a human decision."
    " Do not retry the tool. End your turn now and tell the user exactly what"
    " is pending approval."
)


def summarize_tool_call(tool_name: str, tool_input: dict[str, Any]) -> str:
    """A one-line, human-readable statement of the blocked call.

    This becomes the ``approval_summary`` on the awaiting-approval final and
    therefore the durable record's summary an auditor reads. A bundle-authored
    ``summary`` template (#2565) may replace what a person sees on the card;
    this machine string stays on the record either way.
    """

    try:
        rendered = json.dumps(tool_input, sort_keys=True, default=str)
    except (TypeError, ValueError):
        rendered = str(tool_input)
    if len(rendered) > _SUMMARY_INPUT_LIMIT:
        rendered = rendered[:_SUMMARY_INPUT_LIMIT] + "... (truncated)"
    return f"{APPROVAL_SUMMARY_PREFIX}{tool_name} {rendered}"


@dataclass
class ApprovalGate:
    """Shared state between the ``can_use_tool`` callback and the session loop.

    ``required`` is the per-agent set of approval-required tool names: the
    union of the bundle manifest's ``approvalPolicy`` gates (#247, versioned
    with the agent) and the CURIE_APPROVAL_REQUIRED_TOOLS env override.
    ``route_by_tool`` maps a manifest-gated tool to its declared route name,
    so a blocked call carries the route the platform binds to a channel.
    ``pending_summary``/``pending_route`` are set by the callback when it
    blocks a call and consumed by the session at turn end to flip the final
    to awaiting-approval; ``reset()`` clears them at each turn start so one
    turn's block never leaks into the next. ``pending_halt`` records that the
    runner's own gate asked the CLI to stop the turn (#1852) -- it is
    runner-internal state, never a wire field, and the session reads it to tell
    "the turn ended badly because we stopped it for approval" apart from "the
    turn ended badly".

    The first blocked call of a turn wins: a model that retries the denied
    tool (against the deny message's instruction) does not overwrite the
    summary the human will resolve against.

    ``grant_tool`` is the one-shot post-approval allowance (#430, ADR-0035): the
    single tool name the worker injects from durable state at resume boot so a
    genuinely-approved permission-gate call completes exactly once.
    ``consume_grant`` spends it (one use, tool-name-scoped), and ``reset()``
    expires any unspent grant after the boot turn. The grant is deliberately
    boot-turn-only so it never leaks across turns: ``reset()`` runs at the start
    of every turn, so the FIRST reset (the boot turn) preserves a freshly
    injected grant and every later reset clears it.

    ``grantable_by_route`` is the #558 operator-opt-in map: for a manifest gate
    marked ``grantableViaPolicy``, it binds the gate's route to the MANIFEST tool
    that a policy approval on that route may grant. The session stamps
    ``approval_granted_tool`` from ``grantable_tool_for_route`` at turn end, so
    the granted tool comes from the manifest, never a model-supplied string; a
    route absent from this map resolves to None, preserving #544's no-grant
    default.
    """

    required: frozenset[str] = field(default_factory=frozenset)
    route_by_tool: dict[str, str] = field(default_factory=dict)
    pending_summary: str | None = None
    pending_display: str | None = None
    pending_route: str | None = None
    summary_by_tool: dict[str, str] = field(default_factory=dict)
    # Durable provenance (#544, Decision C), set by ``block()`` on a permission
    # gate: ``pending_gate_kind='permission'`` and ``pending_granted_tool`` is
    # the exact tool ``can_use_tool`` denied -- the trusted, runner-held value
    # the resume-turn grant binds to, never parsed from a string and never
    # model-supplied. A policy gate never sets these (it authorizes a business
    # decision, never a tool); its provenance is stamped in translate.py.
    pending_gate_kind: str | None = None
    pending_granted_tool: str | None = None
    # Policy-gate route reconciliation (#544, Decision B), set by the
    # request_approval tool when the model calls it: whether a request was made
    # this turn, whether it was refused (ambiguous/unknown route -> no approval
    # is created), and the RESOLVED route an accepted request carries onto the
    # final. The session reconciles these at turn end so the final carries the
    # manifest-resolved route rather than the raw model argument.
    policy_requested: bool = False
    policy_rejected: bool = False
    policy_route: str | None = None
    grant_tool: str | None = None
    grantable_by_route: dict[str, str] = field(default_factory=dict)
    publication_title: str | None = None
    publication_body: str | None = None
    # Set by ``block()`` whenever the gate refuses a call (#1852). Since the
    # refusal now carries the SDK's turn-stopping flags, the CLI aborts the turn
    # and its terminal result arrives shaped like a failure; this marker is how
    # ``SessionRunner`` knows the runner itself requested that stop. It is
    # runner-internal and is NEVER serialized onto the wire.
    pending_halt: bool = False
    # A declared tool policy plus the bundle identity needed to translate live
    # SDK MCP names back to the canonical "<server>/<tool>" policy surface.
    tool_policy: ToolPolicy | None = None
    bundle_name: str | None = None
    mcp_servers: set[str] | None = None
    connector_servers: set[str] | None = None
    _boot_turn_seen: bool = False

    def grantable_tool_for_route(self, route: str | None) -> str | None:
        """The manifest tool a policy approval on ``route`` may grant (#558).

        None when ``route`` is None or the operator did not mark a gate on this
        route ``grantableViaPolicy``, preserving #544's no-grant default. The
        value comes from the manifest, never a model-supplied string.
        """

        if route is None:
            return None
        return self.grantable_by_route.get(route)

    def reset(self) -> None:
        self.pending_summary = None
        self.pending_display = None
        self.pending_route = None
        self.pending_gate_kind = None
        self.pending_granted_tool = None
        self.policy_requested = False
        self.policy_rejected = False
        self.policy_route = None
        self.publication_title = None
        self.publication_body = None
        # Strictly per-turn, and cleared with the other pending state rather
        # than with the boot-turn grant below: a halt that leaked forward would
        # make every later errored turn finalize as awaiting-approval (#1852).
        self.pending_halt = False
        # Boot-turn-only grant: keep it on the first reset (the boot turn),
        # expire any unspent grant on the second and later resets so it never
        # leaks into a subsequent turn.
        if self._boot_turn_seen:
            self.grant_tool = None
        self._boot_turn_seen = True

    def consume_grant(self, tool_name: str) -> bool:
        """Spend the one-shot grant iff it names ``tool_name`` (single use)."""

        if self.grant_tool is not None and tool_name == self.grant_tool:
            self.grant_tool = None
            return True
        return False

    def block(self, tool_name: str, tool_input: dict[str, Any]) -> None:
        # Outside the first-block guard on purpose (#1852): the FIRST blocked
        # call still wins the summary the human resolves against, but a second
        # blocked call in the same turn must still assert that the runner asked
        # for a stop. Do not tidy this back inside the guard below.
        self.pending_halt = True
        self._record_pending(tool_name, tool_input)

    def _record_pending(self, tool_name: str, tool_input: dict[str, Any]) -> bool:
        """Write the first pending record of the turn; report whether it wrote.

        Extracted from ``block`` so the stream-side observer
        (``observe_publication``, #2294) writes the IDENTICAL record a denying
        gate layer writes -- the worker recognizes a publication by exactly this
        runner-stamped provenance pair (``pending_gate_kind='permission'`` plus
        ``pending_granted_tool``), so two spellings of it would be two
        behaviors. It does NOT touch ``pending_halt``: asking the CLI to stop is
        the caller's decision, not the record's.

        Returns False when a record already stands (first-block-wins), True when
        this call created it. Raises ``ValueError`` on a malformed publication
        proposal, before anything is recorded.
        """

        if self.pending_summary is not None:
            return False
        if tool_name == PUBLISH_TOOL_NAME:
            title = tool_input.get("title")
            body = tool_input.get("body", "")
            if not isinstance(title, str) or not title.strip() or len(title) > 240:
                raise ValueError("publication title must be 1-240 characters")
            if not isinstance(body, str) or len(body) > 65_536:
                raise ValueError("publication body must be at most 65536 characters")
            self.publication_title = title.strip()
            self.publication_body = body
        self.pending_summary = summarize_tool_call(tool_name, tool_input)
        template = self.summary_by_tool.get(tool_name)
        self.pending_display = (
            render_gate_summary(template, tool_input) if template else None
        )
        self.pending_route = self.route_by_tool.get(tool_name)
        # Provenance for the permission gate (#544, Decision C): the tool
        # name here is the value ``can_use_tool`` itself denied -- the
        # trusted grant target, never derived from the summary string.
        self.pending_gate_kind = "permission"
        self.pending_granted_tool = tool_name
        return True

    def observe_publication(self, tool_input: dict[str, Any]) -> bool:
        """Record a publication request the runner saw on the STREAM (#2294).

        The runner's own observer of a publication call, alongside the
        PreToolUse hook (#1852) and ``can_use_tool`` (#245) rather than behind
        them. Against the real SDK the stream reaches the session loop BEFORE
        the CLI dispatches PreToolUse (observed live, #2294), so this is
        normally the FIRST writer of the record and the hook's ``block`` then
        finds it standing and only adds the halt marker; the fake tier inverts
        that order, and there this call is the duplicate. It replaces neither
        layer -- both still DECIDE the call, which this never does.

        What it closes is the case where NEITHER layer recorded the call (a
        hook that raised is reported and the call proceeds; a concurrently
        dispatched bundle hook; an input shape a layer abstains on): the stream
        still carries the ``ToolUseBlock``, and without this the turn finalizes
        DONE with nothing for a human to approve.

        It can only ever RECORD or raise -- it never returns an allow decision
        and never mints a grant, so it cannot widen authority: ``safe_grant_tool``
        already refuses a sandbox grant for the publish tool, and a second call
        blocks again. It deliberately leaves ``pending_halt`` False: the runner
        did not ask the CLI to stop this turn, and claiming otherwise would
        relabel an unrelated failure as awaiting-approval.

        Returns True when it created the pending record, False when one already
        stands (an earlier call this turn, or a gate layer that got there first
        on the fake tier -- exactly one record per turn either way). Raises
        ``ValueError`` on a malformed title/body, identically to ``block``; the
        session turns that into a fail-closed classified failure.
        """

        return self._record_pending(PUBLISH_TOOL_NAME, tool_input)


class _GateDecision(NamedTuple):
    """The three-way approval-gate outcome shared by both interception points.

    ``build_can_use_tool`` (the SDK permission callback, backstop) and
    ``build_approval_hook`` (the PreToolUse hook, #1852, first line of
    defense) each apply the identical ungated/granted/blocked rule to a
    candidate tool call before rendering it into their own callback's return
    shape (an SDK ``PermissionResult`` vs a hook-output dict). ``_decide_gate``
    below is that one rule, so a future change to it lands in both call sites
    at once instead of risking the two drifting apart -- exactly the defect
    class #1852 closed for the two independent invocation contexts, applied
    here to the decision they share.

    ``blocked`` is the only field a caller branches on today; ``ungated`` is
    carried so a caller that wants to distinguish "nothing to do" from
    "allowed via grant" can, without re-deriving it from ``gate.required``.
    """

    blocked: bool
    ungated: bool
    refusal: str | None = None


def is_mcp_tool(live_tool_name: str) -> bool:
    """Return whether a live SDK name belongs to an MCP server."""

    return live_tool_name.startswith("mcp__")


def canonical_tool_name(
    live_tool_name: str,
    *,
    bundle_name: str | None,
    mcp_servers: set[str] | None,
    connector_servers: set[str] | None,
) -> str | None:
    """Map a live SDK MCP name to the canonical policy name, if declared."""

    if not is_mcp_tool(live_tool_name):
        return None
    for server in sorted(connector_servers or (), key=lambda name: (-len(name), name)):
        prefix = connector_tool_prefix(server)
        if live_tool_name.startswith(prefix):
            tool = live_tool_name[len(prefix) :]
            return f"{server}/{tool}" if tool else None
    if bundle_name:
        for server in sorted(mcp_servers or (), key=lambda name: (-len(name), name)):
            prefix = effective_tool_prefix(bundle_name, server)
            if live_tool_name.startswith(prefix):
                tool = live_tool_name[len(prefix) :]
                return f"{server}/{tool}" if tool else None
    return None


def _tool_policy_outcome(gate: ApprovalGate, tool_name: str) -> ToolPolicyDecision | None:
    """Classify one live tool, preserving built-ins outside MCP policy."""

    if gate.tool_policy is None or not is_mcp_tool(tool_name):
        return None
    # These exact tools belong to the platform-owned approval server, not to
    # the bundle or one of its connectors.  Leave them to their existing
    # permission/in-process gates; every other MCP name remains fail-closed.
    if tool_name == APPROVAL_TOOL_NAME or tool_name == PUBLISH_TOOL_NAME:
        return None
    canonical = canonical_tool_name(
        tool_name,
        bundle_name=gate.bundle_name,
        mcp_servers=gate.mcp_servers,
        connector_servers=gate.connector_servers,
    )
    if canonical is None:
        return ToolPolicyDecision.DENY
    return classify_tool(gate.tool_policy, canonical)


def policy_disallowed_tools(
    gate: ApprovalGate, observed_tools: Iterable[str]
) -> tuple[str, ...]:
    """Project observed policy refusals into exact SDK-visible tool names.

    Catalog visibility is not authorization. This projection therefore uses
    only the side-effect-free policy classification and never consumes a grant
    or records a pending approval on ``gate``.
    """

    return tuple(
        sorted(
            {
                tool_name
                for tool_name in observed_tools
                if _tool_policy_outcome(gate, tool_name) is ToolPolicyDecision.DENY
            }
        )
    )


def _decide_gate(gate: ApprovalGate, tool_name: str, tool_input: dict[str, Any]) -> _GateDecision:
    """Apply the gate rule to one candidate call, mutating ``gate`` as needed.

    Mirrors the pre-extraction ``can_use_tool`` logic verbatim: a tool outside
    ``gate.required`` is ungated (no state change, allow); a gated tool with an
    unspent one-shot grant is granted (the grant is spent here, allow); anything
    else is blocked (``gate.block`` records it, deny). The caller still owns
    rendering the outcome into its own return shape and any outcome-specific
    side effects (the hook's grant-spend log line, the SDK deny's ``interrupt``
    flag) -- this function decides, it does not render.
    """

    outcome = _tool_policy_outcome(gate, tool_name)
    if outcome is ToolPolicyDecision.DENY:
        return _GateDecision(
            blocked=False,
            ungated=False,
            refusal=(
                f"{tool_name} is denied by this agent's tool policy. This is not an "
                "approval you can request -- the policy forbids the call. Do not retry "
                "it; say what you were trying to do and stop."
            ),
        )
    # Policy gates are additive to legacy/operator gates. A policy allow never
    # removes a legacy gate, while approvalRequired joins the same one-shot path.
    if outcome is ToolPolicyDecision.APPROVAL_REQUIRED and tool_name not in gate.required:
        if gate.consume_grant(tool_name):
            return _GateDecision(blocked=False, ungated=False)
        gate.block(tool_name, tool_input)
        return _GateDecision(blocked=True, ungated=False)
    if tool_name not in gate.required:
        return _GateDecision(blocked=False, ungated=True)
    # Publication is completed outside the sandbox after approval, so an
    # injected or stale grant must never let the in-sandbox tool execute.
    if tool_name != PUBLISH_TOOL_NAME and gate.consume_grant(tool_name):
        return _GateDecision(blocked=False, ungated=False)
    gate.block(tool_name, tool_input)
    return _GateDecision(blocked=True, ungated=False)


def build_can_use_tool(gate: ApprovalGate) -> CanUseTool:
    """The SDK permission callback replacing the hardcoded bypass (#245).

    Approval-required tools are denied (the call never executes) and recorded
    on the gate; every other tool is allowed, preserving the pre-gate posture
    for unconfigured tools. The decision is proactive -- the call is blocked
    before execution -- unlike the reactive ``side_effect_flag`` classifier,
    which only reports after the fact.

    Since #1852 this is the **second** line of defense, not the first.
    ``build_approval_hook`` decides first on the real path, because the SDK
    documents on ``ClaudeAgentOptions.can_use_tool``
    (``claude_agent_sdk/types.py:1932-1948``) that this callback is *not*
    invoked for a call already permitted by ``allowed_tools``, ``permission_mode``
    or a settings ``permissions.allow`` rule -- and a skill's ``allowed-tools``
    frontmatter is exactly such a rule. Confirmed live (2026-08-29,
    claude-agent-sdk 0.2.135 + OpenRouter anthropic/claude-sonnet-4.5): with
    ``allowed_tools=["Bash"]`` and no PreToolUse hook, this callback was never
    invoked and Bash executed. It remains the decision on the fake tier, and the
    backstop for every tool the hook abstains on or if no hook is registered.
    """

    async def can_use_tool(
        tool_name: str,
        tool_input: dict[str, Any],
        _context: ToolPermissionContext,
    ) -> PermissionResultAllow | PermissionResultDeny:
        # The one-shot post-approval allowance (#430): a resume-boot grant for
        # exactly this tool lets one call through (no block recorded, the
        # approved action completes) and re-arms the gate. ``_decide_gate``
        # applies this rule (shared with the hook below).
        try:
            decision = _decide_gate(gate, tool_name, tool_input)
        except ValueError as exc:
            return PermissionResultDeny(
                message=f"Publication request was not recorded: {exc}. Correct it and retry.",
                interrupt=True,
            )
        if decision.refusal is not None:
            return PermissionResultDeny(message=decision.refusal, interrupt=True)
        if decision.blocked:
            # ``interrupt`` is the SDK-native "deny AND stop the turn" flag
            # (``PermissionResultDeny.interrupt``, claude_agent_sdk/types.py:247-252),
            # forwarded to the CLI as ``response_data["interrupt"]`` in
            # claude_agent_sdk/_internal/query.py:474-477. Before #1852 only
            # ``_DENY_MESSAGE``'s prose asked the model to end its turn, and
            # against a real OpenRouter-backed model it simply spun until the
            # caller timed out -- with the stream entry pending and no approval
            # record. Prose is not a halt mechanism; this flag is. It rides the
            # DENY only: an allow that carried it would kill every ungated call.
            return PermissionResultDeny(message=_DENY_MESSAGE, interrupt=True)
        return PermissionResultAllow()

    return can_use_tool


# What the operator sees when the hook stops the turn. Short by design: it is
# surfaced as the CLI's stop reason, not as the approval summary (which is
# ``pending_summary``, built by ``summarize_tool_call``).
_HOOK_STOP_REASON = "Paused for human approval: an approval-required tool call was denied."

# Why the hook allows a call outright. Only ever emitted after the one-shot
# post-approval grant (#430) has actually been spent, so it can state that.
_HOOK_GRANT_REASON = (
    "Approved by a human: the one-shot post-approval grant for this tool was spent"
    " on this call, and the gate is re-armed for any further call."
)


def _hook_field(hook_input: Any, key: str) -> Any:
    """Read one field from a hook input that may be a mapping or a dataclass.

    The SDK types the callback's first argument as ``HookInput`` (a union of
    TypedDicts, so a dict at runtime), but the CLI is the thing that actually
    constructs it and a future shape change must not turn the gate into a
    raising hook. Missing/odd shapes resolve to None and the caller abstains.
    """

    if isinstance(hook_input, Mapping):
        return hook_input.get(key)
    return getattr(hook_input, key, None)


def build_approval_hook(gate: ApprovalGate) -> dict[str, list[HookMatcher]]:
    """The ``PreToolUse`` hook that no permission rule can shadow (#1852).

    ``can_use_tool`` (#245) arms the gate but is skipped whenever some other
    permission rule already allows the call, and a skill's ``allowed-tools``
    frontmatter is such a rule -- so a bundle could arm a gate and then walk
    straight through it. The SDK names the fix on that very field: "To observe
    or gate *every* tool call regardless of permission rules, use a
    ``PreToolUse`` hook via ``hooks`` instead"
    (``claude_agent_sdk/types.py:1945-1947``). ``matcher=None`` matches every
    tool call, which is the whole claim.

    Returns the ``{"PreToolUse": [HookMatcher]}`` mapping ``__main__`` MERGES
    into the bundle's own hooks (#272). Note that the CLI dispatches every
    matcher on one event **concurrently** (``types.py:1956-1961``), so this hook
    must not assume it runs before a bundle's hook, and its position in the
    merged list is construction order, not precedence.

    Three outcomes, keyed on gate membership:

    - **Ungated tool** -> ``{}``. No decision, so the call falls through the
      CLI's normal precedence and on to ``can_use_tool``, preserving today's
      posture exactly. An ``allow`` here would silently widen authority for
      every non-gated tool AND skip the callback the policy lane (#544/#558)
      still relies on.
    - **Gated, with the one-shot grant available** -> an EXPLICIT ``allow``,
      after spending the grant here. This is load-bearing, not stylistic: the
      SDK documents (and it was observed live on 2026-08-29 against OpenRouter
      anthropic/claude-sonnet-4.5) that a hook ``allow`` also skips
      ``can_use_tool``. So the hook must be the thing that spends the grant --
      if it returned ``{}`` instead, ``can_use_tool`` would run, find the grant
      unspent, and either block the approved call or (if the hook had spent it
      and still returned ``{}``) let one approval buy unlimited executions.
      Either way #430's one-shot allowance breaks. Do not "simplify" this to a
      bare ``{}``.
    - **Gated, no grant** -> record the block and ``deny``, plus the
      turn-stopping control fields, so the run pauses rather than spinning.

    A bundle's own PreToolUse guardrail can still veto an approved call
    (#1852, accepted rather than fixed). The grant above is spent AT DECISION
    TIME -- before this hook returns -- but every matcher on one ``PreToolUse``
    event is dispatched CONCURRENTLY by the CLI (``claude_agent_sdk/types.py``,
    the ``ClaudeAgentOptions.hooks`` docstring), so a bundle's own hook for the
    same tool (see ``hooks.py``) resolves independently and can return
    ``deny`` even though this hook already returned ``allow`` and spent the
    grant. When that happens the approved call does not execute, but the
    one-shot grant is already gone, so recovery is a fresh approval -- there is
    no way to detect a concurrently-dispatched hook's outcome from inside this
    callback, so this cannot be fixed from here. This is deliberate defense in
    depth, not a bug: a bundle's own guardrail vetoing an approved call is a
    legitimate second opinion, and failing toward "did not run, needs
    re-approval" is the safe direction -- the alternative (letting a
    bundle-denied call through because it was separately approved) would be a
    real hole. ``consume_grant`` is called here anyway (see the WARNING log at
    the call site) precisely because NOT spending it would let ``can_use_tool``
    re-block the approved call, which is the #430 regression this explicit
    ``allow`` exists to prevent.
    """

    async def approval_hook(
        hook_input: Any,
        _tool_use_id: str | None,
        _context: Any,
    ) -> dict[str, Any]:
        tool_name = _hook_field(hook_input, "tool_name")
        if not isinstance(tool_name, str) or not tool_name:
            # Abstain rather than raise. A hook that raises is reported by the
            # CLI as a hook error and the call then PROCEEDS -- a crash in the
            # gate would become a fail-open. Abstaining leaves ``can_use_tool``
            # as the backstop, which is strictly the pre-#1852 posture.
            return {}
        # A policy-bearing bundle must classify every MCP call in this hook;
        # unlike can_use_tool, PreToolUse cannot be shadowed by another allow.
        if gate.tool_policy is None and tool_name not in gate.required:
            return {}

        raw_input = _hook_field(hook_input, "tool_input")
        tool_input: dict[str, Any] = raw_input if isinstance(raw_input, dict) else {}

        # ``_decide_gate`` applies the shared ungated/granted/blocked rule (also
        # used by ``build_can_use_tool`` above); the grant, if any, is spent as
        # a side effect of this call.
        try:
            decision = _decide_gate(gate, tool_name, tool_input)
        except ValueError as exc:
            reason = f"Publication request was not recorded: {exc}. Correct it and retry."
            return {
                "hookSpecificOutput": {
                    "hookEventName": "PreToolUse",
                    "permissionDecision": "deny",
                    "permissionDecisionReason": reason,
                },
                "continue_": False,
                "stopReason": reason,
            }
        if decision.refusal is not None:
            return {
                "hookSpecificOutput": {
                    "hookEventName": "PreToolUse",
                    "permissionDecision": "deny",
                    "permissionDecisionReason": decision.refusal,
                },
                "continue_": False,
                "stopReason": decision.refusal,
            }
        if decision.ungated:
            return {}
        if not decision.blocked:
            # Observability for #1852 (accepted, not fixed): the grant is spent
            # HERE, before the concurrently-dispatched bundle PreToolUse hook's
            # own outcome is known (see the docstring above). If a bundle hook
            # independently denies this same call, the call never executes but
            # the grant is already gone -- silently, from an operator's view.
            # This WARNING is the only way to correlate "approval granted" with
            # "the call may not have actually run" from pod logs.
            logger.warning("approval one-shot grant spent tool=%s", tool_name)
            return {
                "hookSpecificOutput": {
                    "hookEventName": "PreToolUse",
                    "permissionDecision": "allow",
                    "permissionDecisionReason": _HOOK_GRANT_REASON,
                }
            }

        # ``continue_`` and ``stopReason`` are ``SyncHookJSONOutput`` common
        # control fields (claude_agent_sdk/types.py:520-561): "Whether Claude
        # should proceed after hook execution" and "Message shown when continue
        # is False". Emit the Python spelling ``continue_`` -- the SDK rewrites
        # it to the wire's "continue" in
        # claude_agent_sdk/_internal/query.py::_convert_hook_output_for_cli, so
        # emitting the wire name directly would be passed through untouched and
        # silently ignored, and the turn would keep running after the deny.
        return {
            "hookSpecificOutput": {
                "hookEventName": "PreToolUse",
                "permissionDecision": "deny",
                "permissionDecisionReason": _DENY_MESSAGE,
            },
            "continue_": False,
            "stopReason": _HOOK_STOP_REASON,
        }

    # ``Any`` for the same reason ``hooks.py::_make_callback`` uses it: the SDK's
    # ``HookCallback`` alias is typed against its TypedDict union, and a plain
    # ``dict[str, Any]`` return is not assignable to it.
    callback: Any = approval_hook
    return {"PreToolUse": [HookMatcher(matcher=None, hooks=[callback])]}


# --- The manifest approval policy (#247): gates shipped in the bundle -----------


class ApprovalPolicyError(RuntimeError):
    """A declared approval policy cannot be armed exactly as declared.

    Raised instead of degrading to "nothing is gated". A bundle that declares
    a gate the runner cannot arm is a hard configuration error surfaced at
    startup -- the same posture ``load_plugins`` takes for an invalid bundle,
    and for the same reason: booting anyway answers with the wrong (here,
    empty) authority set.
    """


@dataclass(frozen=True)
class ApprovalPolicyResolution:
    """The single parse of a bundle's ``approvalPolicy`` (#544/#558).

    ``route_by_tool`` is the ``{tool: route}`` map every gated tool the runner
    intercepts binds to; ``grantable_by_route`` is the #558 opt-in map of routes
    an operator marked ``grantableViaPolicy`` to the MANIFEST tool a policy
    approval on that route may grant. An honest empty policy yields
    ``ApprovalPolicyResolution({}, {})``.

    ``bundle_name`` and ``mcp_servers`` carry the bundle identity
    ``build_approval_gate`` needs to normalize an operator gate name to its
    effective plugin-prefixed runtime form (#703). They are populated even when
    the bundle declares no ``approvalPolicy``, because an operator may still gate
    a bundle MCP tool by the ``CURIE_APPROVAL_REQUIRED_TOOLS`` shorthand.
    ``mcp_servers`` is ``None`` when the declared-server set is unknowable (the
    ``declared_mcp_server_names`` poison), which fails an ``mcp__`` shorthand
    closed. Both are ``None`` when there is no bundle/manifest to read.

    ``connector_servers`` is the ``connectors.yaml`` half of the same tool surface
    (#1495), carried separately because a connector's live tool name is
    ``mcp__<connector>__<tool>`` -- the bare form, since the runner mounts it on
    ``ClaudeAgentOptions.mcp_servers`` rather than loading it as a plugin. Same
    ``None`` poison meaning.
    """

    route_by_tool: dict[str, str]
    grantable_by_route: dict[str, str]
    bundle_name: str | None = None
    mcp_servers: set[str] | None = None
    connector_servers: set[str] | None = None
    tool_policy: ToolPolicy | None = None
    summary_by_tool: dict[str, str] = field(default_factory=dict)


def resolve_approval_policy(plugin_dir: str | None) -> ApprovalPolicyResolution:
    """Parse the bundle manifest's ``approvalPolicy`` gates once (#544/#558).

    A gate's ``gate`` field names the tool the runner intercepts (the tool
    class of the ADR-0010 permission gate); its ``route`` names the approval
    route the platform binds to a channel per deployment. Declared in the
    bundle so the policy is versioned and evaluable with the agent (#247);
    validated at deploy by ``plugin_format.validate_bundle``.

    **Enforcement intent is read from the DECLARED policy, never from the
    resolved map** (#520). The resolved map empties on a resolution error, so
    deciding "must I enforce?" from it fails OPEN exactly when parsing broke:
    ``__main__`` builds no gate at all from an empty map, restoring the
    hardcoded bypass. This reader therefore raises ``ApprovalPolicyError``
    once a policy is declared but cannot be armed as declared, and reserves
    the empty resolution for the honest cases: no dir, no manifest, no
    ``approvalPolicy``, or an explicitly empty ``gates`` list.

    Unlike the hooks/systemPrompt readers this one is NOT best-effort. Those
    degrade to a smaller capability set and ``load_plugins`` is a sufficient
    backstop; this one degrades to a wider authority set, and ``load_plugins``
    does not run on the fake tier at all (``__main__.factory`` returns first),
    so the backstop is absent precisely where it would be needed.

    The grantable map (#558) is computed from the SAME normalization the deploy
    validator uses (``plugin_format.grantable_routes``), so validator and loader
    agree on which routes are grantable by construction (#453). The loader
    IGNORES the ambiguous set as defense in depth: it arms no ambiguous grant but
    does NOT raise on it -- the ambiguous gates still arm their tools, so the
    policy is armable, and the deploy validator already rejects the ambiguity.
    """

    if not plugin_dir:
        return ApprovalPolicyResolution({}, {})
    root = Path(plugin_dir)
    manifest_path = resolve_manifest(root)
    if manifest_path is None:
        return ApprovalPolicyResolution({}, {})
    # Read raw first: a manifest that will not parse cannot prove it declares
    # no policy, so the fail-closed reading is to refuse rather than assume.
    try:
        raw = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError) as exc:
        raise ApprovalPolicyError(
            f"cannot read the bundle manifest at {manifest_path} to determine whether"
            f" it declares an approvalPolicy; refusing to boot ungated ({exc})"
        ) from exc
    # Bundle identity for operator gate-name normalization (#703): the runner must
    # resolve an operator mcp__<server>__<tool> shorthand to its effective
    # plugin-prefixed runtime name, so read the bundle name and declared MCP
    # servers even when the bundle declares no approvalPolicy of its own -- an
    # operator may still gate a bundle MCP tool via the env knob.
    name = raw.get("name") if isinstance(raw, dict) else None
    bundle_name = name if isinstance(name, str) else None
    mcp_servers = declared_mcp_server_names(root)
    connectors = connector_server_names(root)
    tool_policy: ToolPolicy | None = None
    if isinstance(raw, dict) and raw.get("toolPolicy") is not None:
        try:
            manifest_for_policy = PluginManifest.model_validate(raw)
            tool_policy = load_tool_policy(
                manifest_for_policy, enforces=TOOL_POLICY_ENFORCEMENT
            )
        except (ValueError, TypeError, ToolPolicyUnenforceable, ToolPolicyInvalid) as exc:
            raise ApprovalPolicyError(
                f"the bundle at {root} declares a toolPolicy this build cannot apply"
                f" as written; refusing to boot with its tool surface unclassified"
                f" ({exc})"
            ) from exc
    if not isinstance(raw, dict) or raw.get("approvalPolicy") is None:
        return ApprovalPolicyResolution(
            {},
            {},
            bundle_name=bundle_name,
            mcp_servers=mcp_servers,
            connector_servers=connectors,
            tool_policy=tool_policy,
        )
    # An approvalPolicy IS declared. From here every failure is fail-closed:
    # the intent is established and a parse error cannot revoke it.
    try:
        manifest = PluginManifest.model_validate(raw)
        policy = ApprovalPolicy.model_validate(manifest.approvalPolicy)
    except (ValueError, TypeError) as exc:
        raise ApprovalPolicyError(
            f"the bundle at {root} declares an approvalPolicy that does not parse;"
            f" refusing to boot with its gates unarmed ({exc})"
        ) from exc
    routes = {
        gate.gate.strip(): gate.route.strip()
        for gate in policy.gates
        if gate.gate and gate.gate.strip() and gate.route and gate.route.strip()
    }
    # Last complete declaration wins, including a later gate that OMITS
    # summary: an earlier template must not leak onto a no-template winner
    # (the same last-wins rule ``routes`` uses).
    summaries: dict[str, str] = {}
    for gate in policy.gates:
        if not (gate.gate and gate.gate.strip() and gate.route and gate.route.strip()):
            continue
        tool = gate.gate.strip()
        if gate.summary and gate.summary.strip():
            summaries[tool] = gate.summary.strip()
        else:
            summaries.pop(tool, None)
    # Compare DISTINCT declared names against armed names, not counts: two
    # entries for one tool are a last-wins duplicate that validate_bundle
    # accepts, and rejecting them here would crash-loop a deploy-valid bundle.
    declared_names = {gate.gate.strip() for gate in policy.gates if isinstance(gate.gate, str)}
    unarmed = declared_names - set(routes)
    if unarmed:
        raise ApprovalPolicyError(
            f"the bundle at {root} declares approvalPolicy gate(s)"
            f" {sorted(unarmed)!r} that arm no tool; refusing to boot with a"
            " partially armed policy"
        )
    # #558: derive the grantable route -> manifest tool map. The ambiguous set is
    # ignored here (arm no ambiguous grant); the deploy validator already rejects
    # it, so this is belt-and-braces, not the enforcement point.
    grantable_by_route, _ambiguous = grantable_routes(policy.gates)
    return ApprovalPolicyResolution(
        routes,
        grantable_by_route,
        bundle_name=bundle_name,
        mcp_servers=mcp_servers,
        connector_servers=connectors,
        tool_policy=tool_policy,
        summary_by_tool=summaries,
    )


def load_approval_policy(plugin_dir: str | None) -> dict[str, str]:
    """The bundle manifest's ``approvalPolicy`` gates as ``{tool: route}``.

    A thin wrapper over ``resolve_approval_policy`` returning only the
    ``route_by_tool`` map, preserved for callers that need just the gated-tool
    routes (and its existing fail-closed test suite). See
    ``resolve_approval_policy`` for the full fail-closed contract.
    """

    return resolve_approval_policy(plugin_dir).route_by_tool


def build_approval_gate(
    *,
    operator_tools: Sequence[str] | None,
    policy_routes: dict[str, str],
    grant_tool: str | None = None,
    grantable_by_route: dict[str, str] | None = None,
    summary_by_tool: dict[str, str] | None = None,
    bundle_name: str | None = None,
    mcp_servers: set[str] | None = None,
    connector_servers: set[str] | None = None,
    managed_workspace: bool = False,
    tool_policy: ToolPolicy | None = None,
) -> ApprovalGate | None:
    """Merge the operator's gated tools with the bundle's declared gates.

    Two sources name approval-required tools: ``CURIE_APPROVAL_REQUIRED_TOOLS``
    (operator/per-agent config, a bare list of names with no route) and the
    bundle manifest's ``approvalPolicy`` (versioned with the agent, each gate
    carrying its route). Neither naming a tool keeps the bypass posture.

    **The bundle may only ADD names, never remove an operator-set one** (#520,
    the anti-hollow-out property). The gated-tool set is a UNION, which is what
    enforces that: no bundle-supplied value is subtracted from it, so a bundle
    cannot keep a trusted name while emptying what it restricts. That invariant
    is load-bearing -- rebuilding this merge as anything but a union (a dict
    update, an override, a bundle-wins precedence) would silently break it.

    The residual surface the union does NOT close is which ROUTE governs a tool
    both sources name: the operator's list carries no routes, so the bundle's
    route rides an operator-gated name and picks the approving audience. It is
    bounded -- the bundle can only name routes the operator has itself bound to
    a channel in ``approval_routes``, and ADR-0046 refuses an unbound one
    outright -- so the tool stays gated and the audience stays operator-chosen.
    We log it rather than refusing: making it fatal is disproportionate to a
    bounded widening, and `curie <tier> approvals` reports the two sources as
    one unlabeled list while `--gate` writes a full replacement, so echoing the
    displayed set back is the documented way to CREATE this overlap. A boot
    raise would crash-loop that flow, and would strand an approved action whose
    resume re-derives the same config. See ADR-0050.
    """

    # Normalize each operator-supplied name to its effective runtime form BEFORE
    # the overlap computation (#703, decision A2). The SDK plugin-prefixes a
    # bundle MCP tool to mcp__plugin_<bundle>_<server>__<tool>, so an operator
    # shorthand mcp__<server>__<tool> must be rewritten or the gate arms a literal
    # the runtime name never matches -- a silent fail-open. Built-ins and
    # already-effective mcp__plugin_ names pass verbatim (the manifest-half
    # policy_routes are already deploy-validated to effective form and are NOT
    # re-normalized here). An mcp__-shaped name that resolves to no declared
    # server fails CLOSED (raise), matching resolve_approval_policy's #520 posture
    # -- it never degrades to ungated (the anti-hollow-out union still never
    # subtracts). Normalizing before ``redefined`` makes the ADR-0050 overlap warn
    # fire on effective names, so an operator shorthand and its manifest twin
    # collide correctly.
    normalized: list[str] = []
    for raw_name in operator_tools or ():
        name = raw_name.strip()
        if not name:
            continue
        effective = effective_operator_gates(
            bundle_name, mcp_servers, name, connector_servers=connector_servers
        )
        if effective is None:
            raise ApprovalPolicyError(
                f"operator approval gate {name!r} names an MCP tool that cannot be"
                " resolved to a declared bundle server; refusing to boot with a"
                " gate that would arm nothing. Namespace it to its live"
                " mcp__plugin_<bundle>_<server>__<tool> name (or, for a"
                " connectors.yaml connector, mcp__<connector>__<tool>), or check the"
                f" bundle declares that server (declared MCP servers: {mcp_servers},"
                f" declared connectors: {connector_servers}). A gate is also refused"
                " when it is ambiguous -- a declared connector name and a declared MCP"
                " server name collide, so the gate resolves to two different live tool"
                " names with no principled winner; rename one of them"
            )
        # A bare (non-mcp__) name is armed VERBATIM as a built-in tool name,
        # never rewritten or checked (#712): `effective_operator_gates` has no
        # way to tell a real built-in ("Bash") from an operator's mistaken bare
        # MCP tool name ("resolve_leak" meant as shorthand for an in-bundle MCP
        # tool). That second case silently arms a literal the SDK's
        # `can_use_tool` callback can never match -- a fail-open on a
        # security-relevant control, with no signal anywhere that the gate is
        # a no-op. We cannot fail closed here (no authoritative built-in tool
        # list is importable from this frozen-adjacent path, and a legitimate
        # built-in gate must keep working), but we can stop it being SILENT:
        # warn when the name isn't among the well-known Claude Code built-ins,
        # naming the mcp__<server>__<tool> form the operator likely meant.
        if (
            effective == frozenset({name})
            and not name.startswith("mcp__")
            and name not in _KNOWN_BUILTIN_TOOLS
        ):
            logger.warning(
                "operator approval gate %r does not match any well-known Claude Code"
                " built-in tool name. It will be armed VERBATIM and will silently"
                " gate nothing if it was meant as shorthand for an in-bundle MCP"
                " tool -- use the mcp__<server>__<tool> form instead (declared"
                " servers: %s). If %r really is a built-in tool, ignore this warning.",
                name,
                mcp_servers,
                name,
            )
        # One gate name can resolve to MORE than one live tool name when a declared
        # connector and a declared MCP server both match it and nothing says which
        # hosts the tool (#1564). Arm every returned form: over-arming costs an
        # extra approval card, under-arming is a silent fail-open.
        normalized.extend(effective)

    operator = frozenset(name for name in normalized if name != PUBLISH_TOOL_NAME)
    # The publication gate is platform-owned and requester-thread scoped. A
    # bundle may mention the name, but it cannot attach its own audience route
    # or cause publication to exist without a mounted managed workspace.
    policy_routes = {
        tool_name: route
        for tool_name, route in policy_routes.items()
        if tool_name != PUBLISH_TOOL_NAME
    }
    redefined = sorted(operator & set(policy_routes))
    if redefined:
        logger.warning(
            "bundle approvalPolicy declares route(s) for %r, which the operator"
            " also gated via CURIE_APPROVAL_REQUIRED_TOOLS; the tool stays"
            " gated and the bundle's route decides the approving audience",
            redefined,
        )
    # Publication is a mandatory platform gate only for a managed checkout. It
    # is additive to both operator and bundle policy, has no audience route of
    # its own (the request thread owns the card), and cannot consume a grant.
    gated_tools = operator | frozenset(policy_routes)
    if managed_workspace:
        gated_tools |= frozenset({PUBLISH_TOOL_NAME})
    if not gated_tools and tool_policy is None:
        return None
    safe_grant_tool = None if grant_tool == PUBLISH_TOOL_NAME else grant_tool
    return ApprovalGate(
        required=gated_tools,
        route_by_tool=policy_routes,
        grant_tool=safe_grant_tool,
        grantable_by_route=grantable_by_route or {},
        summary_by_tool=summary_by_tool or {},
        tool_policy=tool_policy,
        bundle_name=bundle_name,
        mcp_servers=mcp_servers,
        connector_servers=connector_servers,
    )


# --- Bundle permissions that would bypass a gate (#1852) ------------------------
#
# A skill's ``allowed-tools`` frontmatter becomes a Claude Code permission rule,
# and a permission rule is applied BEFORE the SDK consults ``can_use_tool``. A
# bundle that gates Bash and also ships a skill declaring ``allowed-tools: [Bash]``
# therefore arms a gate the callback never sees: the tool runs, no approval record
# is created, and ``curie <tier> approvals`` still reports the gate as active. A
# gate that reports itself armed while executing silently is worse than no gate,
# because it is the one an operator trusts.
#
# The SDK does warn about this, but only for entries in
# ``ClaudeAgentOptions.allowed_tools``. Skill frontmatter never appears there --
# the CLI reads it out of the bundle -- so the SDK's own warning is structurally
# unable to fire for this case, which is why it went unnoticed.


class ShadowedGate(NamedTuple):
    """One skill permission rule that preauthorizes an approval-gated tool.

    Attributes:
        skill: The SKILL.md path relative to the bundle root, so the message names
            the file to edit rather than only the conflict.
        entry: The verbatim ``allowed-tools`` entry, so the author can find the
            line instead of guessing which entry matched.
        tool: The gated runtime tool name the entry preauthorizes.
        whole: True when the entry allows the tool outright (``Bash``, ``Bash()``,
            ``Bash(*)``); False when it allows only matching invocations
            (``Bash(ls:*)``). Both defeat a gate, the second for exactly the calls
            it matches, and naming which one was found keeps the message concrete.
    """

    skill: str
    entry: str
    tool: str
    whole: bool


def _whole_tool_allowed(entry: str) -> str | None:
    """Return the tool an ``allowed-tools`` entry allows OUTRIGHT, else None.

    Mirrors the CLI rule parser, which is also what the SDK implements for its own
    shadowing warning: an entry allows a whole tool when it carries no ``(...)``
    specifier (``Read``), or when the specifier is empty or a lone wildcard
    (``Read()``, ``Read(*)``). A real specifier (``Bash(ls:*)``) allows only
    matching invocations. A malformed entry is read as a bare tool name by the
    CLI, so it matches nothing.

    A test pins this against the SDK's implementation, because the CLI applies the
    SDK's reading and not ours: a divergence in either direction is a fail-open or
    a false refusal.
    """

    if not entry.strip():
        return None
    open_index = entry.find("(")
    if open_index == -1:
        return entry
    if open_index == 0 or not entry.endswith(")"):
        return None
    return entry[:open_index] if entry[open_index + 1 : -1] in ("", "*") else None


def _entry_tool(entry: str) -> str | None:
    """Return the tool an entry names, specifier or not.

    ``_whole_tool_allowed`` answers "does this allow the whole tool"; a gate needs
    the broader "which tool is this about". Gating ``Bash`` means every Bash call
    is approval-required, so ``Bash(ls:*)`` still preauthorizes part of what the
    gate claims to cover.
    """

    if not entry.strip():
        return None
    open_index = entry.find("(")
    if open_index == -1:
        return entry
    if open_index == 0 or not entry.endswith(")"):
        return None
    return entry[:open_index]


def _skill_allowed_tools(root: Path) -> list[tuple[str, list[str]]]:
    """Read every skill's ``allowed-tools`` declaration from a bundle directory.

    Deliberately tolerant: a skill whose frontmatter is missing, unterminated,
    unparseable, or not a mapping contributes nothing. ``validate_bundle`` already
    reports those, and failing the gate check on a malformed skill would report
    the wrong defect and hide the real one.

    Normalization (list or comma/space-delimited string) goes through
    ``plugin_format.parse_allowed_tools``, the single shared boundary for this
    field. #1852's commit noted that with one implementation there was no
    second path to disagree with, so a shared-helper extraction did not yet
    apply -- the dual-profile validator now reads this same field too, so a
    second call site exists and the shared helper is the required form to keep
    both readings in agreement.

    Args:
        root: The bundle root directory.

    Returns:
        ``(skill_path_relative_to_root, entries)`` per skill that declares
        allowed-tools, sorted by path so output is deterministic.
    """

    skills_dir = root / "skills"
    if not skills_dir.is_dir():
        return []
    found: list[tuple[str, list[str]]] = []
    for skill_file in sorted(skills_dir.rglob("SKILL.md")):
        try:
            text = skill_file.read_text(encoding="utf-8")
        except OSError:
            continue
        if not text.startswith("---"):
            continue
        parts = text.split("---", 2)
        if len(parts) < 3:
            continue
        try:
            loaded = yaml.safe_load(parts[1])
        except yaml.YAMLError:
            continue
        if not isinstance(loaded, dict):
            continue
        entries = parse_allowed_tools(loaded.get("allowed-tools"))
        if not entries:
            continue
        found.append((str(skill_file.relative_to(root)), entries))
    return found


def _gated_names_for_entry(
    tool: str,
    required: frozenset[str],
    resolution: ApprovalPolicyResolution | None,
) -> str | None:
    """Return the gated runtime name a skill entry would preauthorize, else None.

    Direct equality answers the built-in case (``Bash`` gated, ``Bash`` allowed).
    It does NOT answer the MCP case, and that gap is a fail-open rather than a
    cosmetic one: ``build_approval_gate`` arms an MCP gate under its live name
    (``mcp__plugin_<bundle>_<server>__<tool>``), while a skill author writes the
    natural ``mcp__<server>__<tool>`` shorthand. Compared raw, those two strings
    never match and the conflict is missed entirely.

    So a non-matching entry is run through ``effective_operator_gates`` -- the SAME
    normalization ``build_approval_gate`` applies to an operator's shorthand -- and
    the result is intersected with the armed set. Reusing that function rather than
    writing a second shorthand parser is deliberate: two parsers for one naming
    rule is the defect #1495 and #1564 were both about.

    Args:
        tool: The tool name the entry names.
        required: The armed runtime tool names.
        resolution: The bundle's resolved identity, or None when unavailable, in
            which case only direct equality is possible.

    Returns:
        The armed name this entry preauthorizes, or None.
    """

    if tool in required:
        return tool
    if resolution is None:
        return None
    effective = effective_operator_gates(
        resolution.bundle_name,
        resolution.mcp_servers,
        tool,
        connector_servers=resolution.connector_servers,
    )
    if effective is None:
        return None
    overlap = sorted(effective & required)
    return overlap[0] if overlap else None


def shadowed_gates(
    plugin_dir: str | Path,
    required: frozenset[str],
    resolution: ApprovalPolicyResolution | None = None,
) -> tuple[ShadowedGate, ...]:
    """Find skill permission rules that preauthorize an approval-gated tool.

    Args:
        plugin_dir: The bundle root directory.
        required: The runtime tool names the gate arms.
        resolution: The bundle identity used to normalize an MCP shorthand.

    Returns:
        Every conflict found, ordered by skill path then entry so the message is
        stable across runs. Empty when the bundle preauthorizes nothing gated.
    """

    if not required:
        return ()
    conflicts: list[ShadowedGate] = []
    for skill, entries in _skill_allowed_tools(Path(plugin_dir)):
        for entry in entries:
            named = _entry_tool(entry)
            if named is None:
                continue
            gated = _gated_names_for_entry(named, required, resolution)
            if gated is None:
                continue
            conflicts.append(
                ShadowedGate(
                    skill=skill,
                    entry=entry,
                    tool=gated,
                    whole=_whole_tool_allowed(entry) is not None,
                )
            )
    return tuple(conflicts)


def describe_shadowed_gates(conflicts: Sequence[ShadowedGate]) -> str:
    """Render conflicts as an operator-facing message that names the fix.

    The message has to survive being read from a pod log by someone who did not
    write the bundle, so it names the file, the entry, the tool, and the edit --
    not merely that a conflict exists.
    """

    lines = [
        "approval gate defeated by the bundle's own skill permissions: a skill"
        " 'allowed-tools' entry preauthorizes a tool the approval policy gates, and a"
        " permission rule is applied before the approval callback runs, so the gate"
        " would be armed but never consulted.",
    ]
    for conflict in sorted(conflicts):
        how = "allows the whole tool" if conflict.whole else "allows matching calls"
        lines.append(
            f"  {conflict.skill}: allowed-tools entry {conflict.entry!r} {how}"
            f" {conflict.tool!r}, which is approval-required"
        )
    lines.append(
        "Remove those entries from the skill frontmatter, or stop gating the tool."
        " Narrowing an entry does not help: a narrowed rule still preauthorizes the"
        " calls it matches."
    )
    return "\n".join(lines)


def assert_gates_not_shadowed(
    plugin_dir: str | None,
    gate: ApprovalGate | None,
    resolution: ApprovalPolicyResolution | None = None,
) -> None:
    """Refuse to boot when the bundle preauthorizes a tool the gate arms (#1852).

    This runs at session boot rather than at deploy time on purpose. Deploy-time
    validation can only see gates the BUNDLE declares; an operator who arms a gate
    afterwards with ``curie cluster approvals --gate`` never re-runs it. Boot sees
    both halves of the union, so it is the only place the whole conflict is
    visible, which is also what the issue's acceptance criteria require.

    Args:
        plugin_dir: The mounted bundle root, or None when no bundle is mounted.
        gate: The assembled gate, or None when nothing is gated.
        resolution: The bundle identity used to normalize an MCP shorthand.

    Raises:
        ApprovalPolicyError: When any skill permission rule names a gated tool.
    """

    if gate is None or plugin_dir is None:
        return
    conflicts = shadowed_gates(plugin_dir, gate.required, resolution)
    if conflicts:
        raise ApprovalPolicyError(describe_shadowed_gates(conflicts))
