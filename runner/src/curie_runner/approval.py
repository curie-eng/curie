"""Approval gates in the runner: the policy-gate tool and the permission gate.

Claude SDK tools, callbacks and hooks live in ``harness/claude/approval.py``.
This module owns their shared policy and gate decisions without importing the SDK.

ADR-0010 (approval gates, epic #22) defines one approval primitive with two
trigger types, and this module is the runner half of both:

- **Policy gate** (#244): the agent's own logic decides something needs a
  human decision. An in-process SDK MCP tool
  (``mcp__curie__request_approval``) is carried when a route is
  ``grantableViaPolicy``, or when the live MCP surface has a potentially
  mutating tool and no permission gate already pages. A fully observed
  surface whose tools all declare ``readOnlyHint=true`` omits it, so a model
  cannot page a human for an action the session cannot perform. Mounting it
  beside a permission gate produces a second card for one gated action
  (#2657). The call executes no real-world action; it only marks the turn,
  and the session emits its terminal ``final`` with ``status=awaiting-approval``.
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
from collections.abc import Iterable, Sequence
from copy import deepcopy
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, NamedTuple

import yaml
from aci_protocol import PublicationContext
from plugin_format import (
    PLATFORM_PUBLISH_TOOL_NAME,
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

from .approval_wording import describe_approval, presentation_text
from .caller_feedback import action_label
from .memory_facts import MEMORY_TOOL_NAMES
from .publication_precheck import PublicationPrecheck
from .tool_names import PROGRESS_TOOL, STATE_TOOL_NAMES, TURN_PROGRESS_TOOL

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
# The live-status-card progress tool (#3077), mounted on the same server only
# when the worker injected a progress URL and token.
PROGRESS_TOOL_NAME = f"mcp__{APPROVAL_SERVER_NAME}__{PROGRESS_TOOL}"
# Deliberate progress (ADR 0130's ``curie_progress``), mounted on the same server
# whenever ``report_progress`` is not; see ``turn_progress.py``.
TURN_PROGRESS_TOOL_NAME = f"mcp__{APPROVAL_SERVER_NAME}__{TURN_PROGRESS_TOOL}"
# The GitHub factory's platform issue read (ADR 0187), mounted on the same
# server only when the worker injected the issue read route and capability.
ISSUE_TOOL_NAME = f"mcp__{APPROVAL_SERVER_NAME}__get_issue"

# The existing platform MCP servers ``curie`` and ``curie-state`` retain their
# exemption under #2286 and ADR 0139. Only their exact published platform tool
# names bypass toolPolicy. A bundle may plugin mount either name with the
# plugin infix; those tools remain governed by policy. Connectors may use neither
# name. The reserved identity set also includes the channel read capability,
# which receives no exemption and is not mounted by this prerequisite.
#
# Reservation does not imply mounting, publication or policy exemption. The
# runtime exemption is decided on exact live tool names, not a reserved server
# set: see ``is_platform_owned_tool``, where the first #2286 fix went wrong by
# matching the server prefix instead.

# The live tool names the ``curie`` server publishes, enumerated from the two
# constants that already own them rather than respelled: ``APPROVAL_TOOL_NAME``
# is built from ``_TOOL_NAME`` above and ``PLATFORM_PUBLISH_TOOL_NAME`` is
# plugin_format's, which is also the literal the worker recognizes. Spelling
# either again here is the #453/#1495 drift trap: a second copy can stop
# matching the name the SDK actually puts on the wire, and the exemption would
# then silently arm nothing (or, worse, arm something else).
#
# Both are always exempt even though ``request_approval`` is omitted when a
# permission gate already pages (#2657): an omitted tool is not callable, so
# exempting its name costs nothing, and making the exemption depend on the
# pager decision would add a second way for the two to disagree. The same
# reasoning covers ``report_progress`` (#3077), mounted only for a factory
# execution: it reports a phase and never acts, so it is never gated.
# This also covers ``progress`` (ADR 0130), mounted only for eligible human
# turns, and ``get_issue`` (ADR 0187), which reads only the execution's own issue.
_APPROVAL_SERVER_TOOL_NAMES: frozenset[str] = frozenset(
    {
        APPROVAL_TOOL_NAME,
        PLATFORM_PUBLISH_TOOL_NAME,
        PROGRESS_TOOL_NAME,
        TURN_PROGRESS_TOOL_NAME,
        ISSUE_TOOL_NAME,
    }
)

# The memory tools (#1461, ADR-0167), on the same ``curie`` server but mounted
# only when the worker set a channel memory ref. Rendered from the one tuple
# ``harness.claude.approval.build_memory_tools`` registers, so a fourth tool cannot
# be published without being exempted. Unlike ``report_progress`` these are
# exempt ONLY when mounted: they write, so a name the platform did not publish
# this session must fall to the fail-closed default.
MEMORY_TOOL_LIVE_NAMES: frozenset[str] = frozenset(
    f"mcp__{APPROVAL_SERVER_NAME}__{name}" for name in MEMORY_TOOL_NAMES
)

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


def has_permission_pager(gate: ApprovalGate | None) -> bool:
    """Whether ``gate`` already pauses a tool call for a human.

    True when an approvalPolicy / operator gate names a tool other than
    publication, or when ``toolPolicy.approvalRequired`` is non-empty.
    Publication has its own dedicated tool and must not keep the generic
    pager (#1444).
    """

    if gate is None:
        return False
    if gate.required - {PLATFORM_PUBLISH_TOOL_NAME}:
        return True
    policy = gate.tool_policy
    return policy is not None and bool(policy.approvalRequired)


def include_generic_policy_pager(
    gate: ApprovalGate | None,
    *,
    has_potential_write_tool: bool,
) -> bool:
    """Whether to mount ``mcp__curie__request_approval``.

    A permission gate is already a pager. Mounting the generic policy tool
    beside it produces a grantless policy card and then a second PreToolUse
    card for the same action (#2657), unless the operator opted a route into
    ``grantableViaPolicy`` (#558).
    """

    if gate is not None and gate.grantable_by_route:
        return True
    if has_permission_pager(gate):
        return False
    return has_potential_write_tool


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
    pending_granted_arguments: dict[str, Any] | None = None
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
    # The canonical arguments the approver saw (#3174), carried on the trusted
    # resume boot input (#3255). When set, the grant admits only a call whose
    # arguments are canonically equal; None (a policy grant, which authorizes a
    # business decision rather than one call, or an approval recorded before
    # arguments were carried) keeps the tool-name-only match.
    grant_arguments: dict[str, Any] | None = None
    grantable_by_route: dict[str, str] = field(default_factory=dict)
    publication_title: str | None = None
    publication_body: str | None = None
    publication_precheck: PublicationPrecheck | None = field(default=None, repr=False)
    _publication_pending_id: str | None = None
    # Set by ``block()`` whenever the gate refuses a call (#1852). Since the
    # refusal now carries the SDK's turn-stopping flags, the CLI aborts the turn
    # and its terminal result arrives shaped like a failure; this marker is how
    # ``SessionRunner`` knows the runner itself requested that stop. It is
    # runner-internal and is NEVER serialized onto the wire.
    pending_halt: bool = False
    # The metric observer must attribute a hold to the exact SDK call. A tool
    # name is insufficient when a granted call and a held sibling share it.
    held_call_ids: set[str] = field(default_factory=set)
    refused_call_ids: set[str] = field(default_factory=set)
    # A declared tool policy plus the bundle identity needed to translate live
    # SDK MCP names back to the canonical "<server>/<tool>" policy surface.
    tool_policy: ToolPolicy | None = None
    bundle_name: str | None = None
    mcp_servers: set[str] | None = None
    connector_servers: set[str] | None = None
    # Whether THIS session actually mounted the ``curie-state`` MCP server
    # (#2286 adversarial round). The runner mounts it only when
    # ``resolve_state_client`` returns a client, so the platform's own
    # ``mcp__curie-state__*`` tools exist only then, and only then may those
    # names be exempted from a bundle's toolPolicy. ``__main__`` sets this from
    # the same expression that decides the mount.
    #
    # Default False because that is the SAFE direction, not the historical one:
    # a gate built without stating the fact exempts nothing named after the
    # state server, so a forgotten wiring costs an over-refusal a human can see
    # rather than a silent bypass nobody can.
    state_server_mounted: bool = False
    # Whether THIS session mounted the memory tools on the ``curie`` server
    # (#1461). Same fact-not-default reasoning as ``state_server_mounted``:
    # ``__main__`` sets it from the expression that decides the mount.
    memory_tools_mounted: bool = False
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

    def requires_approval(self, tool_name: str) -> bool:
        """Whether a call to ``tool_name`` would need an approval, read-only.

        @spec RUNNER-TOOL-ACCESS-1: a read-only turn cannot request one, so the
        tool access check refuses these before the gate is consulted. The same
        membership ``_decide_gate`` blocks on, with no state touched.
        """

        return (
            tool_name in self.required
            or _tool_policy_outcome(self, tool_name) is ToolPolicyDecision.APPROVAL_REQUIRED
        )

    def reset(self, *, grant_eligible: bool = True) -> None:
        """Clear one turn's gate state before the next turn runs.

        ``grant_eligible`` False is a turn that may not spend the boot grant (a
        read-only turn, RUNNER-TOOL-ACCESS-10): it neither expires the grant
        nor counts as the boot turn, so the next eligible turn still can.
        """

        self.pending_summary = None
        self.pending_display = None
        self.pending_route = None
        self.pending_gate_kind = None
        self.pending_granted_tool = None
        self.pending_granted_arguments = None
        self.policy_requested = False
        self.policy_rejected = False
        self.policy_route = None
        self.publication_title = None
        self.publication_body = None
        self._publication_pending_id = None
        if self.publication_precheck is not None:
            self.publication_precheck.clear()
        # Strictly per-turn, and cleared with the other pending state rather
        # than with the boot-turn grant below: a halt that leaked forward would
        # make every later errored turn finalize as awaiting-approval (#1852).
        self.pending_halt = False
        self.held_call_ids.clear()
        self.refused_call_ids.clear()
        # Boot-turn-only grant: keep it on the first reset (the boot turn),
        # expire any unspent grant on the second and later resets so it never
        # leaks into a subsequent turn.
        if not grant_eligible:
            return
        if self._boot_turn_seen:
            self.grant_tool = None
            self.grant_arguments = None
        self._boot_turn_seen = True

    def bind_publication_context(self, context: PublicationContext | None) -> None:
        if self.publication_precheck is None and context is not None:
            self.publication_precheck = PublicationPrecheck(None, None, network_enabled=False)
        if self.publication_precheck is not None:
            self.publication_precheck.bind(context)

    def clear_publication_context(self) -> None:
        if self.publication_precheck is not None:
            self.publication_precheck.clear()

    def publication_refused(self, tool_use_id: str | None) -> bool:
        return (
            self.publication_precheck is not None
            and tool_use_id in self.publication_precheck.refused_ids
        )

    async def check_publication(
        self, tool_use_id: str | None, tool_input: dict[str, Any]
    ) -> str | None:
        if self.publication_precheck is None:
            return None
        refusal = await self.publication_precheck.decide(tool_use_id, tool_input)
        if (
            refusal is not None
            and self.pending_granted_tool == PLATFORM_PUBLISH_TOOL_NAME
            and self._publication_pending_id == tool_use_id
        ):
            # A conflicting observation invalidates this call's earlier record.
            # Other tools retain ownership of their existing approval slot.
            self.pending_summary = None
            self.pending_display = None
            self.pending_route = None
            self.pending_gate_kind = None
            self.pending_granted_tool = None
            self.pending_granted_arguments = None
            self.pending_halt = False
            self.publication_title = None
            self.publication_body = None
            self._publication_pending_id = None
        return refusal

    def grant_argument_mismatch(self, tool_name: str, tool_input: dict[str, Any]) -> bool:
        """Whether an unspent grant names ``tool_name`` but not these arguments (#3174)."""

        return (
            self.grant_tool is not None
            and tool_name == self.grant_tool
            and self.grant_arguments is not None
            and _canonical_arguments(tool_input) != _canonical_arguments(self.grant_arguments)
        )

    def consume_grant(self, tool_name: str, tool_input: dict[str, Any] | None = None) -> bool:
        """Spend the one-shot grant iff it names ``tool_name`` (single use).

        When the grant carries the approved arguments, the call's arguments
        must also be canonically equal to them (#3174). A mismatch leaves the
        grant unspent so the exact approved call can still run once.
        """

        if self.grant_tool is None or tool_name != self.grant_tool:
            return False
        if self.grant_arguments is not None and (
            tool_input is None
            or _canonical_arguments(tool_input) != _canonical_arguments(self.grant_arguments)
        ):
            return False
        self.grant_tool = None
        self.grant_arguments = None
        return True

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
        if tool_name == PLATFORM_PUBLISH_TOOL_NAME:
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
            render_gate_summary(presentation_text(template, tool_name), tool_input)
            if template
            else None
        ) or describe_approval(tool_name, tool_input)
        self.pending_route = self.route_by_tool.get(tool_name)
        # Provenance for the permission gate (#544, Decision C): the tool
        # name here is the value ``can_use_tool`` itself denied -- the
        # trusted grant target, never derived from the summary string.
        self.pending_gate_kind = "permission"
        self.pending_granted_tool = tool_name
        self.pending_granted_arguments = deepcopy(tool_input)
        return True

    async def observe_publication(
        self, tool_use_id: str | None, tool_input: dict[str, Any]
    ) -> bool | None:
        """Await the shared decision before recording a streamed publication.

        The real SDK may deliver the stream before either permission callback.
        Every observer uses the exact call ID, so only one comparison runs and
        a continuing refusal never creates pending state. An approval decision
        records the first pending proposal without setting the halt marker;
        only the SDK callbacks request that stop. This observer never grants
        permission to execute the publication tool.

        Return None for a continuing refusal, True for a new pending record,
        and False when a previous record already owns the slot. A malformed
        proposal raises ValueError for the existing terminal failure handling.
        """

        if await self.check_publication(tool_use_id, tool_input) is not None:
            return None
        recorded = self._record_pending(PLATFORM_PUBLISH_TOOL_NAME, tool_input)
        if recorded:
            self._publication_pending_id = tool_use_id
        return recorded


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
    continue_turn: bool = False


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


def platform_tool_names(
    *, state_server_mounted: bool, memory_tools_mounted: bool = False
) -> frozenset[str]:
    """The live tool names Curie's own in-process servers mounted THIS session.

    Always the two ``curie`` server tools; the five ``curie-state`` tools only
    when the runner actually mounted that server, which it does solely when
    ``resolve_state_client`` returned a client (``CURIE_STATE_URL`` is set). With
    no state URL the platform publishes no ``mcp__curie-state__*`` tool at all,
    so anything wearing that name came from somewhere else and must land on the
    fail-closed default rather than collect an exemption for a server this
    session never mounted.

    The state names come from ``tool_names.STATE_TOOL_NAMES``, rendered from
    the same ordered names ``build_state_server`` registers, so a sixth state
    tool cannot be published without being exempted (#2286).

    The three memory tools (#1461) join the set only when the runner mounted
    them, on the same reasoning as the state tools.
    """

    names = _APPROVAL_SERVER_TOOL_NAMES
    if state_server_mounted:
        names = names | STATE_TOOL_NAMES
    if memory_tools_mounted:
        names = names | MEMORY_TOOL_LIVE_NAMES
    return names


def is_platform_owned_tool(
    live_tool_name: str, *, state_server_mounted: bool, memory_tools_mounted: bool = False
) -> bool:
    """Whether a live SDK name is one Curie's own servers published (#2286).

    EXACT membership in ``platform_tool_names``, and the exactness is the whole
    point. The first #2286 fix asked whether the name carried the
    ``mcp__<platform server>__`` PREFIX, which also answers yes for every tool
    of a DIFFERENT server whose key merely begins ``curie__`` or
    ``curie-state__``: ``mcp__curie__extra__foo`` and
    ``mcp__curie-state__extra__bar`` both matched, and both were handed a bundle
    toolPolicy bypass. It was reachable while ``adapter.build_options`` left
    ``strict_mcp_config`` off: the CLI then loaded project ``.mcp.json``, user
    settings and plugin servers BESIDE the ``--mcp-config`` dict, and a mounted
    workspace is the session cwd, writable, and survives across sandboxes, so a
    ``.mcp.json`` sitting there was bundle-influenced input. A connector cannot
    do it (a connector name may not contain ``_``), an ambient project server
    could.

    The residual the exact match alone cannot close: when the platform HAS
    mounted a server, an impostor publishing a name-identical tool is exempt
    too, because the live name is the entire thing this gate sees. #2899 closes
    it one layer down: the runner now sets ``strict_mcp_config``, so no server
    outside the ``--mcp-config`` dict the runner builds can load, and this exact
    match stays as defence in depth. The
    class this closes is the one the prefix match invented: every name on a
    platform-shaped prefix that the platform does not itself publish.

    A bare ``mcp__curie__`` names no tool and is in no set, so it falls through
    to the fail-closed default, as does a bundle's own plugin-mounted server
    named ``curie`` (its live names carry the ``plugin_<bundle>_`` infix).
    """

    return live_tool_name in platform_tool_names(
        state_server_mounted=state_server_mounted, memory_tools_mounted=memory_tools_mounted
    )


def _tool_policy_outcome(gate: ApprovalGate, tool_name: str) -> ToolPolicyDecision | None:
    """Classify one live tool, preserving built-ins outside MCP policy."""

    if gate.tool_policy is None or not is_mcp_tool(tool_name):
        return None
    # The tools Curie's own platform-owned servers published THIS session are
    # outside toolPolicy scope entirely (ADR-0139, #2286): a bundle cannot
    # declare those servers, so it cannot express a policy over them, and per
    # that ADR bundle configuration may not hollow out a platform control. Leave
    # them to their existing permission/in-process gate. Taken BEFORE
    # classify_tool deliberately: a wildcarded server segment such as
    # `deny: ["*/get"]` is the one pattern shape the deploy validator never
    # cross-checks, and returning here is what makes it inert rather than a way
    # to fence the approval path.
    #
    # By exact published name, never by server prefix: a prefix match also
    # exempts an ambient MCP server keyed `curie__extra` or `curie-state__extra`,
    # which is the widening the adversarial round on #2286 found and proved.
    # `gate.state_server_mounted` is what keeps the state half honest -- with no
    # state URL the platform mounts nothing named `curie-state`, so nothing
    # wearing that name is ours.
    #
    # Outside policy scope is not permission to run. Returning None means the
    # policy has no opinion; `_decide_gate` still applies gate.required, the
    # operator gates, and the publication special case below.
    if is_platform_owned_tool(
        tool_name,
        state_server_mounted=gate.state_server_mounted,
        memory_tools_mounted=gate.memory_tools_mounted,
    ):
        return None
    canonical = canonical_tool_name(
        tool_name,
        bundle_name=gate.bundle_name,
        mcp_servers=gate.mcp_servers,
        connector_servers=gate.connector_servers,
    )
    # LOAD BEARING, do not relax (#2119, #453/#544). An MCP name that maps to no
    # declared server is refused, not waved through: this unmatched-is-DENY
    # default is the property that actually defends the surface, and the
    # exemption above widened what precedes it. Turning this into `return None`
    # is the fail-open, and it is what
    # test_a_tool_from_an_undeclared_server_is_refused_at_both_interception_points
    # exists to redden.
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


def _canonical_arguments(arguments: dict[str, Any]) -> str:
    """Canonical JSON for argument equality: sorted keys, no insignificant space."""

    return json.dumps(arguments, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _grant_mismatch_refusal(tool_name: str) -> str:
    return (
        f"The approval for {action_label(tool_name)} covers only the approved details, "
        "and this request has different details. It was not run. Use the approved "
        "details, or tell the user what changed so they can approve the new request."
    )


async def _decide_gate(
    gate: ApprovalGate,
    tool_name: str,
    tool_input: dict[str, Any],
    tool_use_id: str | None,
) -> _GateDecision:
    """Apply the gate rule to one candidate call, mutating ``gate`` as needed.

    Publication awaits its shared precheck before the ordinary rule: a tool outside
    ``gate.required`` is ungated (no state change, allow); a gated tool with an
    unspent one-shot grant is granted (the grant is spent here, allow); anything
    else is blocked (``gate.block`` records it, deny). The caller still owns
    rendering the outcome into its own return shape and any outcome-specific
    side effects (the hook's grant-spend log line, the SDK deny's ``interrupt``
    flag) -- this function decides, it does not render.
    """

    if tool_name == PLATFORM_PUBLISH_TOOL_NAME:
        refusal = await gate.check_publication(tool_use_id, tool_input)
        if refusal is not None:
            return _GateDecision(
                blocked=False, ungated=False, refusal=refusal, continue_turn=True
            )
    outcome = _tool_policy_outcome(gate, tool_name)
    if outcome is ToolPolicyDecision.DENY:
        if tool_use_id is not None:
            gate.refused_call_ids.add(tool_use_id)
        return _GateDecision(
            blocked=False,
            ungated=False,
            refusal=(
                f"The {action_label(tool_name)} action is not permitted for this agent. "
                "Approval cannot authorize it. It was not run. Do not retry; "
                "explain what you were trying to do and stop."
            ),
        )
    # Policy gates are additive to legacy/operator gates. A policy allow never
    # removes a legacy gate, while approvalRequired joins the same one-shot path.
    if outcome is ToolPolicyDecision.APPROVAL_REQUIRED and tool_name not in gate.required:
        if gate.grant_argument_mismatch(tool_name, tool_input):
            if tool_use_id is not None:
                gate.refused_call_ids.add(tool_use_id)
            return _GateDecision(
                blocked=False,
                ungated=False,
                refusal=_grant_mismatch_refusal(tool_name),
                continue_turn=True,
            )
        if gate.consume_grant(tool_name, tool_input):
            return _GateDecision(blocked=False, ungated=False)
        gate.block(tool_name, tool_input)
        if tool_use_id is not None:
            gate.held_call_ids.add(tool_use_id)
        return _GateDecision(blocked=True, ungated=False)
    if tool_name not in gate.required:
        return _GateDecision(blocked=False, ungated=True)
    # Publication is completed outside the sandbox after approval, so an
    # injected or stale grant must never let the in-sandbox tool execute.
    if tool_name != PLATFORM_PUBLISH_TOOL_NAME and gate.grant_argument_mismatch(
        tool_name, tool_input
    ):
        if tool_use_id is not None:
            gate.refused_call_ids.add(tool_use_id)
        return _GateDecision(
            blocked=False,
            ungated=False,
            refusal=_grant_mismatch_refusal(tool_name),
            continue_turn=True,
        )
    if tool_name != PLATFORM_PUBLISH_TOOL_NAME and gate.consume_grant(tool_name, tool_input):
        return _GateDecision(blocked=False, ungated=False)
    gate.block(tool_name, tool_input)
    if tool_use_id is not None:
        gate.held_call_ids.add(tool_use_id)
    if (
        tool_name == PLATFORM_PUBLISH_TOOL_NAME
        and gate.pending_granted_tool == PLATFORM_PUBLISH_TOOL_NAME
        and gate._publication_pending_id is None
    ):
        gate._publication_pending_id = tool_use_id
    return _GateDecision(blocked=True, ungated=False)


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
    grant_arguments: dict[str, Any] | None = None,
    resumed_kind: str | None = None,
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

    operator = frozenset(name for name in normalized if name != PLATFORM_PUBLISH_TOOL_NAME)
    # PLATFORM_PUBLISH_TOOL_NAME stays out of the operator-tools set so the platform
    # adds the gate via managed_workspace, not as an operator list entry. A
    # bundle may attach an audience route through policy_routes. The requester
    # thread owns the card; publication still cannot consume a grant.
    redefined = sorted(operator & set(policy_routes))
    if redefined:
        logger.warning(
            "bundle approvalPolicy declares route(s) for %r, which the operator"
            " also gated via CURIE_APPROVAL_REQUIRED_TOOLS; the tool stays"
            " gated and the bundle's route decides the approving audience",
            redefined,
        )
    # Publication is a mandatory platform gate only for a managed checkout. It
    # is additive to both operator and bundle policy. The requester thread
    # still owns the card. Publication cannot consume a grant.
    gated_tools = operator | frozenset(policy_routes)
    if managed_workspace:
        gated_tools |= frozenset({PLATFORM_PUBLISH_TOOL_NAME})
    if not gated_tools and tool_policy is None:
        return None
    safe_grant_tool = None if grant_tool == PLATFORM_PUBLISH_TOOL_NAME else grant_tool
    # #3174: only a policy grant may be admitted by name alone, and it must be
    # one on both counts: the resume is recorded as a policy approval AND the
    # tool is one some grantableViaPolicy route maps to. ``resumed_kind`` only
    # narrows here; it never creates a grant. A permission grant without the
    # approved arguments (an old row, or a malformed carrier) cannot prove what
    # the approver saw, so it is dropped and the call asks for a fresh approval.
    policy_grant = resumed_kind == "policy" and safe_grant_tool in set(
        (grantable_by_route or {}).values()
    )
    if safe_grant_tool is not None and grant_arguments is None and not policy_grant:
        logger.warning(
            "dropping resume grant for %s: no approved arguments were carried",
            safe_grant_tool,
        )
        safe_grant_tool = None
    return ApprovalGate(
        required=gated_tools,
        route_by_tool=policy_routes,
        grant_tool=safe_grant_tool,
        grant_arguments=grant_arguments if safe_grant_tool is not None else None,
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
