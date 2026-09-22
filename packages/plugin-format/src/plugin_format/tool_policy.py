"""Normalize and classify the manifest's vanilla MCP tool policy (`toolPolicy`).

This module is the SINGLE normalization shared by the deploy-time validator
(``plugin_format.validate._validate_tool_policy``) and the runtime loader that
enforces ``curie/mcp-tool-policy@1`` in the runner (#2119).
Sharing one helper makes the two paths identical *by construction* -- the
#453/#544 lesson that a validator and a runtime loader normalizing separately
silently disagree and ship a fail-open. ``approval_policy.py`` exists for exactly
that reason; an implementer who inlines the glob grammar or the precedence ladder
into ``validate.py`` "because it is only used once" reintroduces it.

**The canonical tool name is ``"<server>/<tool>"``.** It is server-qualified
because two servers may publish the same tool name, and it is deliberately NOT
the SDK's live tool name. A live name is namespaced two DIFFERENT ways depending
on how the server is mounted: ``mcp__plugin_<bundle>_<server>__<tool>`` for a
plugin-loaded ``mcpServers`` entry (``approval_policy.effective_tool_prefix``)
versus ``mcp__<connector>__<tool>`` for a ``connectors.yaml`` connector
(``approval_policy.connector_tool_prefix``, #1495). Making the author write the
live form would repeat the #453/#1495 authoring trap in a place where the failure
is a silent permission GRANT rather than a silent missed approval. Mapping
canonical -> live SDK name is the runtime lane's job and is out of scope here.

**Curie's own platform-owned servers are outside this policy's scope** (#2286,
ADR-0139). ``curie`` (approval and publication) and ``curie-state`` (ADR-0095
channel memory) are mounted by the runner, and ``connectors.py`` refuses those
names to a bundle connector (``RESERVED_CONNECTOR_NAMES``), so a canonical name
naming one can never be a rule this package would have anything to classify. The
runtime therefore exempts the tools those servers actually published before it
consults the precedence ladder (by exact live tool name, not by server prefix:
``runner/src/curie_runner/approval.py::is_platform_owned_tool``), and
``validate.py`` reports
``tool_policy.platform_server`` for such a pattern rather than the
declare-the-server advice it gives for a typo. The cost is stated rather than
hidden: a bundle cannot restrict its agent's use of those tools either, so
``deny: ["curie-state/delete"]`` is inexpressible and would be inert. ADR-0139
is the authority for accepting it, since bundle configuration may add
restrictions but may not hollow out operator or platform controls, and the two
directions are the same predicate. An operator-level need for platform-scope
patterns would be new semantics, so it belongs to a future ``@2`` contract id,
not to a relaxation of this one. Note that a bundle MAY still declare a
plugin-mounted ``mcpServers`` entry of its own called ``curie``: its live names
carry the ``plugin_<bundle>_`` infix, it is not the platform server, and it
stays fully in scope.

**Declared here, enforced by the runner** (#2119). This package still enforces
nothing by itself, and the handshake is what keeps that honest: ``load_tool_policy``
refuses to hand a policy to a caller that does not name ``TOOL_POLICY_ENFORCEMENT``,
and ``validate_bundle`` REJECTS a policy-bearing bundle unless its caller passes
the same id -- together those keep "declared" and "enforced" from drifting apart.
The runtime half lives in ``runner/src/curie_runner/approval.py::_decide_gate``,
which both of the runner's interception points share, and where an unmatched MCP
tool is DENIED.

Both paths run the SAME ``check_policy_patterns``: ``validate.py`` renders its
issues one ``ValidationIssue`` per pattern, ``load_tool_policy`` collapses them
into a ``ToolPolicyInvalid``. Neither owns a rule the other lacks, so a policy
the deploy validator refuses can never be loaded by a runtime.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from enum import StrEnum
from fnmatch import fnmatchcase

from .models import PluginManifest, ToolPolicy

# The enforcement contract a bundle must name and a runtime must implement. It is
# a wire constant, not an identifier: a bundle writes it verbatim in its manifest
# and every consumer compares it byte-for-byte. A future change of SEMANTICS is a
# NEW id (``@2``), which a v1 build refuses outright rather than reinterpreting
# under v1 rules -- the versioned-discriminator half of the fail-closed design.
TOOL_POLICY_ENFORCEMENT = "curie/mcp-tool-policy@1"

# The canonical-name / pattern grammar, stated ONCE: exactly one "/", both
# segments non-empty, each segment drawn from A-Za-z0-9_.- plus the wildcards "*"
# and "?". ``validate_pattern`` is the whole rule; this expression is its
# character half, written negatively so a rejection can name the offending
# characters. One mechanism deliberately, not two: a second anchored whole-string
# regex saying the same thing is a synchronization burden with no reachable
# check behind it.
#
# This is deliberately NARROWER than a protocol-legal MCP name. MCP's tool-name
# character rule is a SHOULD and its schema leaves ``name`` an unconstrained
# string, and ``McpConfig`` accepts an arbitrary string as a server key, so a name
# this grammar cannot express does exist (``@scope/github``, ``search:docs``).
# That narrowness is FAIL-CLOSED, not a hole: a name the policy cannot spell is
# matched by no pattern and therefore DENIED by ``classify_tool``'s unmatched
# default. A bundle that must reach such a server reaches it through a wildcard
# segment, and accepts the widening that implies.
_DISALLOWED_CHAR_RE = re.compile(r"[^A-Za-z0-9_.*?/-]")

# The collections, in PRECEDENCE order. Everything that iterates the policy walks
# this tuple, so adding a fourth collection cannot be half-implemented: the
# classifier, the pattern checker and the validator all pick it up at once.
_COLLECTIONS = ("deny", "approvalRequired", "allow")


class ToolPolicyDecision(StrEnum):
    """What the policy says about one canonical tool name.

    The values are wire strings: they are serialized into approval records and
    logs, so they are pinned by test rather than free to follow the member names.
    """

    ALLOW = "allow"
    APPROVAL_REQUIRED = "approval-required"
    DENY = "deny"


class ToolPolicyUnenforceable(Exception):
    """A ``toolPolicy`` is declared but this call path cannot enforce it.

    Raised rather than returned, deliberately. ``None`` would be
    indistinguishable from "no policy declared", and a ``(policy, enforced:
    bool)`` tuple is exactly the shape a caller drops on the floor -- the #520
    lesson that "cannot verify" must fail CLOSED and LOUDLY, never through.
    """


class ToolPolicyInvalid(Exception):
    """A ``toolPolicy`` is MALFORMED: one or more patterns fail the shared grammar.

    Deliberately NOT ``ToolPolicyUnenforceable``. "This bundle asks for semantics
    I do not implement" and "this policy is not well-formed" are different
    failures with different fixes, and a caller that catches only the first must
    not silently swallow the second.

    ``load_tool_policy`` raises this so no caller can obtain a ``ToolPolicy``
    whose patterns the deploy validator would have REFUSED. That gap is the
    #453/#544 drift in its most dangerous form: ``_matches`` hands whatever
    strings the collections carry to ``fnmatchcase``, so a pattern the grammar
    forbids still gets real matching semantics -- ``grafana/[a-z]*`` becomes a
    live character class that can ALLOW tools in a bundle ``validate_bundle``
    would have rejected.
    """


@dataclass(frozen=True)
class ToolPolicyPatternIssue:
    """One declaration-level defect found in a policy's pattern collections.

    ``code`` is the bare code (``pattern_invalid`` / ``pattern_duplicate`` /
    ``pattern_conflict``); the validator prefixes it with ``tool_policy.``.
    ``collection`` and ``index`` locate the offending entry so the validator can
    build ``plugin.json (toolPolicy.<collection>[<i>])`` without knowing anything
    about the grammar itself.
    """

    code: str
    collection: str
    index: int
    pattern: str
    message: str


def validate_pattern(pattern: str) -> str | None:
    """The reason ``pattern`` is not a legal canonical glob, or ``None`` if it is.

    ``None`` means "no reason to reject" -- the same no-news-is-good-news shape
    the validator's other shape helpers use. Every rejection carries a
    human-readable reason naming WHICH rule failed, because that string is what a
    bundle author sees at deploy; "unsupported character" for a ``**`` would send
    them hunting the wrong thing.

    The rules, in the order they are reported:

    - non-blank (an empty or whitespace-only pattern matches nothing and is
      always an editing accident);
    - no whitespace anywhere (a canonical tool name carries none, so a pattern
      containing one is an inert rule the author believes is live);
    - no ``**`` (a two-segment namespace has no recursive level for it to mean,
      so it is genuinely ambiguous rather than merely unsupported);
    - no ``[`` / ``]`` character classes (``fnmatch``'s classes are
      locale-sensitive, which would make a policy's meaning depend on the machine
      that evaluates it);
    - exactly one ``/``, with both segments non-empty;
    - every remaining character drawn from ``A-Za-z0-9_.-`` plus ``*`` and ``?``.

    Those rules ARE the grammar; there is no second whole-string regex behind
    them to keep in sync.
    """

    if not pattern or not pattern.strip():
        return "a pattern must be a non-blank '<server>/<tool>' glob"
    if any(ch.isspace() for ch in pattern):
        return "a pattern must not contain whitespace"
    if "**" in pattern:
        return (
            "'**' has no meaning in a two-segment '<server>/<tool>' name; use a "
            "single '*' in the segment you mean to widen"
        )
    if "[" in pattern or "]" in pattern:
        return (
            "character classes ('[...]') are not supported: they are "
            "locale-sensitive, so the policy's meaning would depend on the "
            "machine evaluating it"
        )
    if pattern.count("/") != 1:
        return (
            "a pattern must contain exactly one '/' separating the server from "
            "the tool (canonical names are '<server>/<tool>')"
        )
    server, tool = pattern.split("/", 1)
    if not server or not tool:
        return "both the server and the tool segment must be non-empty"
    bad = sorted({m.group() for m in _DISALLOWED_CHAR_RE.finditer(pattern)})
    if bad:
        joined = ", ".join(repr(ch) for ch in bad)
        return (
            f"unsupported character(s) {joined}: a segment may contain only "
            "letters, digits, '_', '.', '-' and the wildcards '*' and '?'"
        )
    return None


def literal_server_segment(pattern: str) -> str | None:
    """The pattern's server segment when it names ONE literal server, else ``None``.

    ``None`` for a malformed pattern (its own error already fired) and for a
    wildcarded segment (``*/search_*``), which is the deliberate escape hatch for
    a bundle whose servers are not statically declared and is therefore never
    cross-checked against the declared set. Living here rather than in
    ``validate.py`` keeps the validator from re-deriving where a segment ends.
    """

    if validate_pattern(pattern) is not None:
        return None
    server = pattern.split("/", 1)[0]
    if "*" in server or "?" in server:
        return None
    return server


def policy_patterns(policy: ToolPolicy) -> list[tuple[str, int, str]]:
    """``(collection, index, pattern)`` for every declared pattern, in precedence order.

    One list rather than three loops, so a caller cannot forget a collection --
    and so a fourth collection added to ``_COLLECTIONS`` reaches every consumer at
    once instead of silently going unchecked in one of them.
    """

    out: list[tuple[str, int, str]] = []
    for collection in _COLLECTIONS:
        for i, pattern in enumerate(getattr(policy, collection)):
            out.append((collection, i, pattern))
    return out


def check_policy_patterns(policy: ToolPolicy) -> list[ToolPolicyPatternIssue]:
    """Every declaration-level defect in ``policy``'s pattern collections.

    The SHARED half of the deploy check: grammar, within-collection duplicates,
    and cross-collection identical strings. ``validate.py`` renders these into
    ``ValidationIssue``s and adds only the bundle-scoped unknown-server check on
    top, so the future runtime loader reuses these exact rules rather than
    reimplementing them (#453/#544).

    Overlapping-but-DIFFERENT globs are legal and are NOT reported: overlap is
    the normal authoring idiom (``deny */pods_delete`` narrowing
    ``approvalRequired */pods_*``) and resolves deterministically by class
    precedence. The identical STRING in two collections carries no information
    the precedence rule can use -- it is a copy-paste error whose stated intent
    (both) would be silently reduced to one, the #558 "validates green while
    arming nothing" shape.

    The scan never returns early: an author who gets one defect per ``curie
    build`` round-trips once per typo.
    """

    issues: list[ToolPolicyPatternIssue] = []
    # Where each well-formed pattern has already been seen, in precedence order,
    # so a later collection can name the earlier ones it collides with.
    seen: dict[str, list[str]] = {}

    for collection in _COLLECTIONS:
        within: set[str] = set()
        for i, pattern in enumerate(getattr(policy, collection)):
            reason = validate_pattern(pattern)
            if reason is not None:
                issues.append(
                    ToolPolicyPatternIssue(
                        code="pattern_invalid",
                        collection=collection,
                        index=i,
                        pattern=pattern,
                        message=f"invalid tool pattern {pattern!r}: {reason}",
                    )
                )
                # A pattern that failed the grammar cannot be meaningfully
                # duplicate- or conflict-checked; reporting it three times would
                # bury the one message that says how to fix it.
                continue

            if pattern in within:
                issues.append(
                    ToolPolicyPatternIssue(
                        code="pattern_duplicate",
                        collection=collection,
                        index=i,
                        pattern=pattern,
                        message=(
                            f"tool pattern {pattern!r} is listed more than once in "
                            f"'{collection}'; the repeat classifies nothing extra, "
                            "so it is an editing accident. Remove the duplicate."
                        ),
                    )
                )
                continue
            within.add(pattern)

            earlier = seen.get(pattern)
            if earlier:
                named = ", ".join(f"'{c}'" for c in earlier)
                issues.append(
                    ToolPolicyPatternIssue(
                        code="pattern_conflict",
                        collection=collection,
                        index=i,
                        pattern=pattern,
                        message=(
                            f"tool pattern {pattern!r} is declared in '{collection}' "
                            f"and also in {named}. Precedence is deny > "
                            "approvalRequired > allow, so only the "
                            "highest-precedence collection would take effect and "
                            "the rest of the stated intent is silently dropped. "
                            "Overlapping but DIFFERENT globs are legal and resolve "
                            "by precedence; the same string in two collections is "
                            "a declaration error. Keep it in one collection."
                        ),
                    )
                )
            seen.setdefault(pattern, []).append(collection)

    return issues


def _matches(pattern: str, server: str, tool: str) -> bool:
    """Whether ``pattern`` matches the canonical name already split into its segments.

    Server-vs-server and tool-vs-tool are matched INDEPENDENTLY, never as one
    string: globbing the whole string would let ``*`` cross the ``/``, so
    ``grafana/*`` would reach ``grafana/x/y`` and a trailing ``*`` would widen
    across servers -- a silent, total widening of the allow list.

    ``fnmatchcase``, not ``fnmatch``: ``fnmatch`` normalizes case on some
    platforms, which would make every pattern case-INSENSITIVE on those machines
    and let ``K8s/Resources_Create`` slip an allow written for
    ``k8s/resources_create``. That is a platform-dependent permission widening,
    which is worse than a wrong rule because it is invisible where it is authored.

    The CALLER splits the canonical name (``classify_tool`` does it once per
    call, not once per pattern) and guarantees it had exactly two segments; a
    name that did not is refused there, which keeps the fail-closed default
    honest for a malformed runtime name. The pattern is still split here because
    the collections store plain strings -- pre-splitting or precompiling them
    would change ``ToolPolicy``, which is part of the manifest contract.
    """

    pattern_parts = pattern.split("/")
    if len(pattern_parts) != 2:
        return False
    return fnmatchcase(server, pattern_parts[0]) and fnmatchcase(tool, pattern_parts[1])


def classify_tool(policy: ToolPolicy, canonical_tool_name: str) -> ToolPolicyDecision:
    """Classify one canonical ``"<server>/<tool>"`` name against ``policy``.

    Precedence is by CLASS and never by pattern specificity: ``deny`` beats
    ``approvalRequired`` beats ``allow``, even when the losing pattern is the
    narrower one. "Most specific wins" is the tempting reading and the dangerous
    one -- a narrow ``allow`` sitting inside a broad ``approvalRequired`` would
    silently drop the human out of the operation the broad glob was written to
    gate.

    A name no collection matches is **DENIED**. That default, not the pattern
    lists, is what actually defends the capability: a tool a server begins
    advertising after the bundle was authored is refused rather than inherited,
    so server tool-surface drift fails closed. It also makes a policy with three
    empty collections coherent (it denies everything) rather than vacuous.

    Each collection is scanned in FULL before the ladder moves on; the ladder is
    a precedence order over classes, never "the first collection that happens to
    contain a match". The shape is adjacent to ``effective_operator_gates``'s
    union-of-rules (#1564), where an early return IS a fail-open -- here the early
    return is the precedence rule itself, so do not restructure the two to match.
    """

    # Split ONCE, here: this is the future runtime's per-tool, per-turn hot path,
    # and splitting inside ``_matches`` re-derived the same two segments for every
    # pattern in the policy.
    name_parts = canonical_tool_name.split("/")
    if len(name_parts) != 2:
        # A name that is not exactly two segments is spelled by no legal pattern,
        # so it matches nothing and lands on the same DENY default an unmatched
        # name gets. Stated here rather than discovered per pattern.
        return ToolPolicyDecision.DENY
    server, tool = name_parts

    if any(_matches(p, server, tool) for p in policy.deny):
        return ToolPolicyDecision.DENY
    if any(_matches(p, server, tool) for p in policy.approvalRequired):
        return ToolPolicyDecision.APPROVAL_REQUIRED
    if any(_matches(p, server, tool) for p in policy.allow):
        return ToolPolicyDecision.ALLOW
    return ToolPolicyDecision.DENY


def load_tool_policy(manifest: PluginManifest, *, enforces: str | None) -> ToolPolicy | None:
    """The manifest's parsed ``toolPolicy``, or ``None`` when it declares none.

    ``enforces`` is the caller's statement of which enforcement contract IT
    implements; ``None`` means "this code path does not enforce tool policy at
    all". The handshake is two-sided and both sides must name
    ``TOOL_POLICY_ENFORCEMENT``:

    - the CALLER's ``enforces`` must match, so a build with no enforcement path
      can never obtain a policy object it would then quietly not apply;
    - the BUNDLE's own ``enforcement`` must match too, even when the caller's id
      is right, because a bundle asking for ``@2`` semantics is asking for rules
      this build does not implement. Applying ``@1`` rules to it would hand the
      author a different policy than the one they wrote.

    Either mismatch raises ``ToolPolicyUnenforceable``. There is deliberately no
    return value that means "declared but not applied".

    A manifest with no ``toolPolicy`` returns ``None`` for EVERY ``enforces``
    value, including ``None``. That is the backward-compatible path every bundle
    shipped to date takes: absence is not a declaration, so it can never be
    unenforceable.

    The policy's PATTERNS are then checked with ``check_policy_patterns`` -- the
    same call ``validate.py`` makes -- and any defect raises
    ``ToolPolicyInvalid``. A caller can therefore never hold a ``ToolPolicy``
    whose patterns the deploy validator would have refused.

    The ORDER is deliberate and load-bearing in two places. The caller check runs
    BEFORE parsing, so "no enforcement -> no policy object, ever" is true by
    structure rather than by a branch someone can rearrange. And the whole
    enforcement handshake -- caller first, then the bundle's own id -- is checked
    BEFORE pattern validity: a consumer that cannot enforce this contract must be
    turned away whatever the policy's shape, and leading with a glob complaint
    would imply that fixing the glob makes the bundle loadable here.
    """

    declared = manifest.toolPolicy
    if declared is None:
        return None

    if not isinstance(declared, dict):
        raise ToolPolicyUnenforceable(
            "toolPolicy must be an object; a non-object declaration cannot be "
            "parsed into a policy, and ignoring it would run the agent unfenced"
        )

    if enforces != TOOL_POLICY_ENFORCEMENT:
        raise ToolPolicyUnenforceable(
            f"this bundle declares a toolPolicy, but the caller enforces {enforces!r} "
            f"rather than {TOOL_POLICY_ENFORCEMENT!r}. Refusing to hand back a policy "
            "no one would apply."
        )

    policy = ToolPolicy.model_validate(declared)

    # Compared EXACTLY -- deliberately NOT stripped. The id is a wire constant,
    # so " curie/mcp-tool-policy@1 " is simply not it, and refusing it loudly is
    # the fail-closed direction: a stray space in a manifest earns a pointed
    # error instead of being silently treated as v1. This differs on purpose from
    # ``approval_policy.py``, which strips GATE names so the validator and the
    # runtime loader agree on one tool name; a version discriminator is not a
    # tool name, and normalizing it would let two distinct wire strings claim the
    # same contract. Do not "fix" this back to a ``.strip()``.
    declared_enforcement = policy.enforcement
    if declared_enforcement != TOOL_POLICY_ENFORCEMENT:
        raise ToolPolicyUnenforceable(
            f"this bundle declares toolPolicy.enforcement {declared_enforcement!r}, "
            f"which this build does not implement (it implements "
            f"{TOOL_POLICY_ENFORCEMENT!r}). Applying this build's rules to it would "
            "silently give the author a different policy than the one they wrote."
        )

    # The SAME rules the deploy validator reports, from the SAME call:
    # ``validate._validate_tool_policy`` renders each issue into a
    # ``ValidationIssue`` (``tool_policy.<code>`` at
    # ``toolPolicy.<collection>[<i>]``) so an author fixes every typo in one
    # ``curie build``; here they collapse into a single refusal, because a
    # runtime loader has no per-issue reporting surface and a partially-applied
    # policy is not a thing. One source, two renderings -- re-deriving either
    # side is exactly the #453/#544 validator-vs-runtime drift this module
    # exists to prevent.
    defects = check_policy_patterns(policy)
    if defects:
        detail = "; ".join(defect.message for defect in defects)
        raise ToolPolicyInvalid(
            f"this bundle's toolPolicy cannot be applied: {detail}. Refusing to "
            "return a policy the deploy validator would have rejected: matching "
            "would otherwise give a forbidden pattern real fnmatch semantics "
            "(a character class, a stray '**') and could ALLOW tools."
        )

    return policy
