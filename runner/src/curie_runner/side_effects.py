"""Classify a tool call as idempotent (safe) or side-effecting.

The ACI ``side_effect_flag`` gates the no-retry-after-side-effects rule (ACI
section 2b): a failed run that executed a non-idempotent tool escalates to a
human instead of retrying, because at-least-once delivery plus a mutating tool
means a blind retry risks a duplicate real-world action.

Design choice (documented for F1/K1): a **read-only allowlist**, deny-by-default.
A harness declares which of its tools only read state; every other tool the
harness executes is treated as potentially side-effecting and flags the run.
This fails safe: a new or unknown tool escalates rather than being silently
retried. The alternative, an explicit non-idempotent denylist, fails open (a new
mutating tool would be missed) and was rejected for that reason.

**The read-only set is harness-declared** (ADR-0060). Tool identifiers are
harness-specific: the Claude Code harness names its read-only tools in PascalCase
(``Read``, ``Grep``, ...), while a second harness such as OpenCode names the same
capabilities in lowercase (``read``, ``grep``, ``webfetch``). The classifier
itself stays harness-agnostic and deny-by-default; it is constructed with the
active harness's declared read-only set. The default is the Claude adapter's
declaration (``CLAUDE_READONLY_TOOLS``), so an unconfigured caller sees the
historical Claude behavior unchanged. A future OpenCode adapter constructs the
classifier with its own declaration instead. There is no env knob for this: the
env override that once advertised itself here was never wired to a consumer, and
an unwired widening knob on a deny-by-default classifier is worse than none -- it
reads as an escape hatch that silently does nothing (#488).
"""

from collections.abc import Iterable

from .state import STATE_READ_TOOL_NAMES

# The platform's in-process approval-request tool (ADR-0010) is idempotent under
# EVERY harness: it executes no real-world action (it only marks the turn
# awaiting-approval), and when applicable it is injected by the platform rather
# than shipped by the harness. It is therefore always treated as idempotent,
# independent of which harness's read-only declaration is in force -- flagging it
# would block the no-retry rule for the very turns approvals pause, and a harness
# declaration that happened to omit it must not be able to reintroduce that bug.
# The factory's ``report_progress`` (#3077) is idempotent for the same reason: it
# reports a phase to the status card and acts on nothing (#3464). So is the
# deliberate ``progress`` tool (ADR 0130): it reports task state to the
# platform and acts on nothing, so it is never a side effect and never on the
# turn's receipt. The factory ``get_issue`` (ADR 0187) only reads its own issue.
# The ``curie-state`` reads (``get`` and ``list``) only read the durable store;
# their names come from the server's own tool list (``state.STATE_READ_TOOL_NAMES``)
# so they cannot drift from what it registers. Its writes (``set``, ``append``,
# ``delete``) change the store and stay side-effecting.
PLATFORM_IDEMPOTENT_TOOLS: frozenset[str] = (
    frozenset(
        {
            "mcp__curie__request_approval",
            "mcp__curie__report_progress",
            "mcp__curie__progress",
            "mcp__curie__get_issue",
        }
    )
    | STATE_READ_TOOL_NAMES
)

# The Claude Code harness adapter's declared read-only tool set. Names match the
# claude-agent-sdk tool identifiers (PascalCase). Per ADR-0060 each harness
# declares its own read-only set; this is the Claude adapter's declaration and
# the classifier default. A second harness (e.g. OpenCode) uses different
# identifiers and constructs the classifier with its own declaration.
CLAUDE_READONLY_TOOLS: frozenset[str] = frozenset(
    {
        "Read",
        "Glob",
        "Grep",
        "LS",
        "NotebookRead",
        "WebFetch",
        "WebSearch",
        "ToolSearch",
        "TodoRead",
    }
)

# The effective idempotent set for an unconfigured (Claude) classifier: the Claude
# adapter's declared read-only tools plus the always-idempotent platform tools.
# Retained as the module default so existing callers and Claude behavior are
# unchanged.
DEFAULT_IDEMPOTENT_TOOLS: frozenset[str] = CLAUDE_READONLY_TOOLS | PLATFORM_IDEMPOTENT_TOOLS


class SideEffectClassifier:
    """Decide whether a tool name denotes a non-idempotent (side-effecting) call."""

    def __init__(self, readonly_tools: Iterable[str] | None = None) -> None:
        # The harness-declared read-only set (defaults to the Claude adapter's
        # declaration) is unioned with the always-idempotent platform tools. An
        # explicit set REPLACES the Claude declaration -- it does not inherit
        # Claude's PascalCase names -- but the platform tools are always present.
        declared = (
            frozenset(readonly_tools)
            if readonly_tools is not None
            else CLAUDE_READONLY_TOOLS
        )
        self._idempotent = declared | PLATFORM_IDEMPOTENT_TOOLS

    def is_side_effecting(self, tool_name: str) -> bool:
        """True if executing ``tool_name`` may cause a non-idempotent side effect."""

        return tool_name not in self._idempotent
