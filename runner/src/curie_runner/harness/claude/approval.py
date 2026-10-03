"""Claude SDK tools and callbacks backed by the provider neutral approval gate."""

from __future__ import annotations

import json
import logging
from collections.abc import Mapping, Sequence
from typing import Any

from claude_agent_sdk import HookMatcher, SdkMcpTool, create_sdk_mcp_server, tool
from claude_agent_sdk.types import (
    CanUseTool,
    McpSdkServerConfig,
    PermissionResultAllow,
    PermissionResultDeny,
    ToolPermissionContext,
)
from plugin_format import PLATFORM_PUBLISH_TOOL_NAME

from ...approval import (
    _DENY_MESSAGE,
    _TOOL_NAME,
    APPROVAL_SERVER_NAME,
    ApprovalGate,
    _approval_error,
    _decide_gate,
    process_approval_request,
)
from ...memory_facts import (
    FORGET_TOOL,
    MAX_STATEMENT_CHARS,
    REMEMBER_TOOL,
    UPDATE_TOOL,
    FactNotFound,
    MemoryFactsError,
    MemoryFactsStore,
    MemoryFull,
    MemoryRefused,
    MemoryTurn,
)

logger = logging.getLogger(__name__)


# Platform-owned remote-development publication gate.  This is deliberately
# mounted beside the policy tool rather than shipped by a bundle: a bundle is
# untrusted input and must not be able to remove, execute, or grant its own
# publication action.  The worker recognizes this exact runner-stamped
# permission-gate provenance before it captures a patch.
_PUBLISH_TOOL = PLATFORM_PUBLISH_TOOL_NAME.removeprefix(f"mcp__{APPROVAL_SERVER_NAME}__")

_PUBLISH_DESCRIPTION = (
    "When a managed repository is mounted, work only in /workspace and preserve"
    " existing changes. Do not push with git. Before requesting publication,"
    " identify the repository's own documented test or check command for the"
    " area you changed, run it from /workspace, and report the exact command,"
    " its exit status, and a concise result in the session thread. If you"
    " cannot identify an appropriate command, report that and do not publish."
    " If the command cannot run because a required binary or service is unavailable,"
    " report that cause and publish only through a declared required CI route that"
    " selects the changed paths. The pull request must state that in-sandbox"
    " verification was unavailable and CI is pending proof. If no matching route"
    " exists, do not publish. If the command runs and fails, report the failure"
    " and do not publish. If"
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


def build_approval_server(
    gate: ApprovalGate | None = None,
    *,
    managed_workspace: bool = False,
    include_request_approval: bool = True,
    progress_tool: SdkMcpTool[Any] | None = None,
    turn_progress_tool: SdkMcpTool[Any] | None = None,
    memory_tools: Sequence[SdkMcpTool[Any]] = (),
    issue_tool: SdkMcpTool[Any] | None = None,
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

    ``progress_tool`` (#3077) is the ``report_progress`` tool, appended when the
    runner resolved a progress URL, token and phase declaration.
    ``turn_progress_tool`` is the deliberate progress tool (ADR 0130), appended
    only when ``progress_tool`` is not: a factory execution keeps its own.

    ``memory_tools`` (#1461) are ``remember``/``update``/``forget``, passed only
    when the worker set a channel memory ref.

    ``issue_tool`` (ADR 0187) is the ``get_issue`` tool, appended only for an
    execution with a WorkItem, when the worker injected its read capability.
    """

    @tool(_TOOL_NAME, _TOOL_DESCRIPTION, _TOOL_SCHEMA)
    async def request_approval(args: dict[str, Any]) -> dict[str, Any]:
        return process_approval_request(gate, args)

    tools: list[SdkMcpTool[Any]] = [request_approval] if include_request_approval else []

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
    if progress_tool is not None:
        tools.append(progress_tool)
    elif turn_progress_tool is not None:
        tools.append(turn_progress_tool)
    tools.extend(memory_tools)
    if issue_tool is not None:
        tools.append(issue_tool)

    return create_sdk_mcp_server(
        name=APPROVAL_SERVER_NAME,
        version="1.0.0",
        tools=tools,
    )


# --- The memory tools (#1461, ADR-0167) ---------------------------------------

_MEMORY_PROPERTY = {
    "type": "string",
    "enum": ["agent", "channel"],
    "description": "Which memory: channel (this channel only) or agent (every channel).",
}
_STATEMENT_PROPERTY = {"type": "string", "description": "One fact, stated plainly."}
_ID_PROPERTY = {"type": "string", "description": "The fact's id, as shown in brackets."}

_REMEMBER_SCHEMA = {
    "type": "object",
    "properties": {"memory": _MEMORY_PROPERTY, "statement": _STATEMENT_PROPERTY},
    "required": ["memory", "statement"],
}
_UPDATE_SCHEMA = {
    "type": "object",
    "properties": {
        "memory": _MEMORY_PROPERTY,
        "id": _ID_PROPERTY,
        "statement": _STATEMENT_PROPERTY,
    },
    "required": ["memory", "id", "statement"],
}
_FORGET_SCHEMA = {
    "type": "object",
    "properties": {"memory": _MEMORY_PROPERTY, "id": _ID_PROPERTY},
    "required": ["memory", "id"],
}


def build_memory_tools(
    *,
    agent_store: MemoryFactsStore | None,
    channel_store: MemoryFactsStore,
    turn: MemoryTurn,
    session_id: str,
) -> list[SdkMcpTool[Any]]:
    """The ``remember``/``update``/``forget`` tools for the ``curie`` server.

    The author of every write is ``turn.author``, set by the SessionRunner from
    the turn's inbound event; no argument names an author, a channel or a URL,
    and an ``author`` the model passes anyway is ignored. Every failure (an
    unknown memory, an unknown id, a full memory, an unreachable store) is an
    ``is_error`` result the model reads.
    """

    def pick(args: Mapping[str, Any]) -> tuple[MemoryFactsStore | None, str | None]:
        memory = args.get("memory")
        if memory == "channel":
            return channel_store, None
        if memory == "agent":
            if agent_store is None:
                return None, "Agent memory is not available in this session."
            return agent_store, None
        return None, f"Unknown memory {memory!r}; pass memory as agent or channel."

    def statement_of(args: Mapping[str, Any]) -> str | None:
        statement = args.get("statement")
        if isinstance(statement, str) and statement.strip():
            return statement.strip()
        return None

    def bad_statement(statement: str | None) -> dict[str, Any] | None:
        if statement is None:
            return _approval_error("statement must be a non-empty string.")
        if len(statement) > MAX_STATEMENT_CHARS:
            return _approval_error(
                f"Refused: a statement may be at most {MAX_STATEMENT_CHARS} characters "
                f"(this one is {len(statement)}). Nothing was saved."
            )
        return None

    def failure(exc: MemoryFactsError, memory: object, fact_id: object = None) -> dict[str, Any]:
        if isinstance(exc, MemoryFull) and exc.limit == "value":
            return _approval_error(
                f"Refused: that fact is too large to store as one {memory} memory entry "
                f"({exc}). Nothing was saved; state it more briefly."
            )
        if isinstance(exc, MemoryFull) and exc.limit == "facts":
            return _approval_error(f"Refused: {memory} memory is full: {exc}. Nothing was saved.")
        if isinstance(exc, MemoryFull):
            return _approval_error(f"Refused: {memory} memory is full ({exc}). Nothing was saved.")
        if isinstance(exc, FactNotFound):
            return _approval_error(f"Not found: no fact with id {fact_id!r} in {memory} memory.")
        if isinstance(exc, MemoryRefused):
            # ADR-0188: the API refused this credential, which is not an outage.
            logger.warning("memory tool refused error_class=%s: %s", type(exc).__name__, exc)
            return _approval_error(
                "Refused: this memory cannot be written from this conversation. Nothing was saved."
            )
        logger.warning("memory tool failed error_class=%s: %s", type(exc).__name__, exc)
        return _approval_error(f"The {memory} memory could not be reached. Nothing changed.")

    def ok(payload: dict[str, Any]) -> dict[str, Any]:
        return {"content": [{"type": "text", "text": json.dumps(payload)}]}

    @tool(
        REMEMBER_TOOL,
        "Save one new fact to memory and return its id. This, or update for a fact "
        "that already exists, is the only way to keep something for a later "
        "conversation: a request to remember, note or make something stick, or to set "
        "a standing instruction, means calling one of them.",
        _REMEMBER_SCHEMA,
    )
    async def remember(args: dict[str, Any]) -> dict[str, Any]:
        store, problem = pick(args)
        if store is None:
            return _approval_error(problem or "Unknown memory.")
        statement = statement_of(args)
        refusal = bad_statement(statement)
        if refusal is not None:
            return refusal
        assert statement is not None
        try:
            fact_id = await store.add(
                statement=statement, author=turn.author, session_id=session_id
            )
        except MemoryFactsError as exc:
            return failure(exc, args.get("memory"))
        return ok({"id": fact_id})

    @tool(UPDATE_TOOL, "Replace the statement of a remembered fact, by id.", _UPDATE_SCHEMA)
    async def update(args: dict[str, Any]) -> dict[str, Any]:
        store, problem = pick(args)
        if store is None:
            return _approval_error(problem or "Unknown memory.")
        statement = statement_of(args)
        refusal = bad_statement(statement)
        if refusal is not None:
            return refusal
        assert statement is not None
        fact_id = args.get("id")
        try:
            await store.update(
                str(fact_id), statement=statement, author=turn.author, session_id=session_id
            )
        except MemoryFactsError as exc:
            return failure(exc, args.get("memory"), fact_id)
        return ok({"id": fact_id, "updated": True})

    @tool(FORGET_TOOL, "Remove a remembered fact, by id.", _FORGET_SCHEMA)
    async def forget(args: dict[str, Any]) -> dict[str, Any]:
        store, problem = pick(args)
        if store is None:
            return _approval_error(problem or "Unknown memory.")
        fact_id = args.get("id")
        try:
            await store.forget(str(fact_id))
        except MemoryFactsError as exc:
            return failure(exc, args.get("memory"), fact_id)
        return ok({"id": fact_id, "forgotten": True})

    return [remember, update, forget]


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
            decision = await _decide_gate(gate, tool_name, tool_input, _context.tool_use_id)
        except ValueError as exc:
            return PermissionResultDeny(
                message=f"Publication request was not recorded: {exc}. Correct it and retry.",
                interrupt=True,
            )
        if decision.refusal is not None:
            return PermissionResultDeny(
                message=decision.refusal, interrupt=not decision.continue_turn
            )
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
            decision = await _decide_gate(gate, tool_name, tool_input, _tool_use_id)
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
            if decision.continue_turn:
                return {
                    "hookSpecificOutput": {
                        "hookEventName": "PreToolUse",
                        "permissionDecision": "deny",
                        "permissionDecisionReason": decision.refusal,
                    },
                    "continue_": True,
                }
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
