"""Commands the published verification contract names (#1041).

`docs/agents.md` tells an agent which command proves which outcome, and an
agent is told to trust it. Nothing recomputed those names, so a renamed or
dropped subcommand left the contract naming a command that no longer exists --
the same defect class `counts.py` names for prose numbers, one noun over.

Each `curie ...` invocation a gated command doc writes inside a code construct
is resolved against the committed `cli/command-manifest.json`, which
`cli/tests/command_surface.rs` already pins against the live clap grammar, so
the manifest cannot silently drift from the real CLI. There are five ways to
fail, all loud:

- a path token is not a subcommand of the node it follows -- the drift the
  class is named for, from either side (the CLI renamed a leaf, or the doc
  invented a verb);
- a flag is declared neither on the resolved node nor as a `global` on any of
  its ancestors, so `--jsn` misleads an agent exactly as badly as a dead verb;
- a token is an angle-bracket placeholder (`<tier>`), which names a shape rather
  than a command an agent can run. Only the angle-bracket spelling is detected,
  which is why the contract's own prose bans exactly that spelling: a bare
  `TIER` or `...` is indistinguishable from a positional value here;
- a required artifact -- the contract or the manifest -- is missing, or the
  manifest no longer parses;
- the contract names no command at all, which is the vacuity guard: a reword
  that drops the backticks would otherwise leave a green gate over an
  unverified contract, the defect one level up from the one being gated.

Only code constructs are scanned (inline spans and fenced-block lines), so
ordinary English about the `curie` binary is not mistaken for a command line.
A scanned unit is ONE inline code span, or ONE line of a fenced block -- never a
line of the rendered document. Two separately-backticked commands on one source
line are therefore two units holding one invocation each, which is how the
current contract is written. Every `curie` token inside a unit still opens a NEW
invocation rather than only the first, because a single unit can carry two: a
fenced line an agent copy-pastes verbatim, or one span joining two commands with
`&&`. A first-occurrence-only resolver leaves the second one unchecked.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .citation import find_code_spans, parse_markdown
from .finding import Finding

# Required command docs. A missing one is a finding, never a skip: the
# `_ROOT_DOCS` lesson is that filtering an absent required file out with
# `.is_file()` is what lets it be deleted for zero findings and exit 0.
CONTRACT_REL = "docs/agents.md"
DEMO_RUNBOOK_REL = "examples/sre-bot/DEMO.md"
MANIFEST_REL = "cli/command-manifest.json"

# The token that opens an invocation, and the wrapping punctuation a doc puts
# around one mid-sentence. The leading set carries the shell wrappers as well as
# the quotes: `$(curie ... | jq -r .url)` is how a contract shows an agent how to
# capture a value, and if the opener has to equal `curie` exactly then the whole
# invocation is silently unchecked. `-` is deliberately absent from both sets, or
# a flag stops looking like a flag.
_CURIE = "curie"
_STRIP_LEADING = "`\"'$([{"
_STRIP_TRAILING = "`\"'.,:;)]}"


@dataclass(frozen=True)
class _Unit:
    """One scanned code construct: an inline span, or one fenced-block line."""

    text: str
    line: int | None


def _scanned_units(text: str) -> list[_Unit]:
    """Every inline code span, plus every line of every fenced code block.

    Text outside a code construct is deliberately not scanned. That is what
    keeps the gate off ordinary prose ("curie publishes this contract"), and
    the contract states the backtick house rule in its own body so an editor
    knows why.
    """
    units = [_Unit(span.content, span.line) for span in find_code_spans(text)]

    for token in parse_markdown(text):
        if token.type != "fence" or token.map is None:
            continue
        # ``map[0]`` is the opening fence line (0-based), so the first content
        # line is one below it, and +1 again to report 1-based.
        first = token.map[0] + 2
        for offset, line in enumerate(token.content.splitlines()):
            units.append(_Unit(line, first + offset))

    return units


def _strip_token(token: str) -> str:
    """Drop the quoting, shell wrapping and sentence punctuation around a token."""
    return token.lstrip(_STRIP_LEADING).rstrip(_STRIP_TRAILING)


def _args(node: dict[str, Any]) -> list[dict[str, Any]]:
    """A node's declared arguments, or an empty list when it declares none."""
    args = node.get("args")
    return args if isinstance(args, list) else []


def _global_args(node: dict[str, Any]) -> list[dict[str, Any]]:
    """The arguments a node declares as ``global``, which its descendants inherit."""
    return [arg for arg in _args(node) if arg.get("global")]


def _subcommands(node: dict[str, Any]) -> list[dict[str, Any]]:
    """A node's child commands. Hidden children are kept, deliberately.

    ``"hidden": true`` means "not advertised in --help", never "does not
    exist": `curie schema` is hidden and the contract names it as the authority
    on what commands exist. Filtering hidden nodes out here would silently
    break exactly that line.
    """
    children = node.get("subcommands")
    return children if isinstance(children, list) else []


def _child(node: dict[str, Any], name: str) -> dict[str, Any] | None:
    for child in _subcommands(node):
        if child.get("name") == name:
            return child
    return None


def _lookup_flag(token: str, declared: list[dict[str, Any]]) -> dict[str, Any] | None:
    """Match a flag token against the arguments in scope, returning the argument.

    ``--flag=value`` matches on the part before the first ``=``; ``--long``
    matches an argument's ``long``, ``-x`` its ``short``.
    """
    name = token.split("=", 1)[0]
    is_long = name.startswith("--")
    wanted = name[2:] if is_long else name[1:]
    key = "long" if is_long else "short"
    for arg in declared:
        if arg.get(key) == wanted:
            return arg
    return None


def _takes_a_value(arg: dict[str, Any]) -> bool:
    """Whether a flag consumes the token after it, per the manifest.

    A boolean flag (clap's ``SetTrue``) is emitted with exactly
    ``["true", "false"]`` as its possible values; a value-taking flag is not
    (``--color`` carries ``["auto", "always", "never"]``). This is what lets the
    resolver keep walking the command path across a flag instead of swallowing
    everything after it.
    """
    return arg.get("possible_values") != ["true", "false"]


def _manifest_finding(message: str) -> Finding:
    """A ``Finding`` against the manifest artifact itself, e.g. missing or unparseable."""
    return Finding(CONTRACT_REL, MANIFEST_REL, message)


def _load_manifest(repo_root: Path) -> tuple[dict[str, Any] | None, Finding | None]:
    """Read the command manifest, reporting rather than raising on a bad one."""
    path = repo_root / MANIFEST_REL
    if not path.is_file():
        return None, _manifest_finding(
            f"the command manifest is missing, so every command {CONTRACT_REL} names is "
            "unverified. It is a required artifact, not an optional one: without it this "
            "gate goes vacuously green. Restore it with "
            "`cargo run -- schema > cli/command-manifest.json`",
        )

    try:
        manifest = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        return None, _manifest_finding(
            f"the command manifest does not parse as JSON ({exc}), so every command "
            f"{CONTRACT_REL} names is unverified. Regenerate it with "
            "`cargo run -- schema > cli/command-manifest.json`",
        )

    if not isinstance(manifest, dict):
        return None, _manifest_finding(
            "the command manifest is not a command tree (its top level is not an object), "
            f"so every command {CONTRACT_REL} names is unverified",
        )

    return manifest, None


def _resolve_invocation(
    tokens: list[str],
    start: int,
    root: dict[str, Any],
    unit: _Unit,
    doc_rel: str,
) -> tuple[int, list[Finding]]:
    """Resolve one invocation, returning where it ended and what it got wrong.

    ``start`` indexes the ``curie`` token that opens it. The invocation ends at
    the next ``curie`` token or at the end of the unit, and the returned index
    is that stopping point, so the caller resumes there rather than abandoning
    the rest of the unit. Every termination -- a bad subcommand, a bad flag, a
    placeholder -- is local to this invocation.
    """
    node = root
    # Reset per invocation: a flag declared on one leaf must not launder itself
    # onto a different command later in the same unit. Accumulated as the walk
    # descends, so it holds every ANCESTOR's globals; the current node's own
    # arguments are added at check time rather than stored here.
    in_scope: list[dict[str, Any]] = []
    words = [tokens[start]]
    findings: list[Finding] = []
    # Set when the previous token was a value-taking flag, so this token is its
    # value rather than a command-path element.
    is_flag_value = False
    failed = False
    index = start + 1

    while index < len(tokens):
        token = tokens[index]
        if token == _CURIE:
            break
        index += 1
        if failed:
            # This invocation is already reported; consume to the next opener
            # rather than piling second-order findings on the same command.
            continue
        words.append(token)
        cited = " ".join(words)

        if token.startswith("<"):
            findings.append(
                Finding(
                    doc_rel,
                    cited,
                    f"`{token}` is a placeholder, and placeholders are banned in a "
                    "gated command doc: an agent cannot run a shape. Write every "
                    "command out in full, all three tiers included",
                    line=unit.line,
                )
            )
            failed = True
            continue

        if is_flag_value:
            # Claimed by the flag before it. Checked for a placeholder above,
            # because `--cases <file>` is a shape an agent cannot run either.
            is_flag_value = False
            continue

        if token.startswith("-"):
            # A flag does NOT end the command path: clap accepts a global flag
            # before the subcommand path (`curie --json skill status`), so the
            # walk continues past it. Its own node's arguments plus every
            # ancestor's globals are what is in scope: `--json` is declared once
            # on the root, so a per-leaf check would false-fail every real line.
            arg = _lookup_flag(token, in_scope + _args(node))
            if arg is None:
                findings.append(
                    Finding(
                        doc_rel,
                        cited,
                        f"`{token}` is not an argument of `{' '.join(words[:-1])}` and is "
                        "not a global flag; every flag a gated command doc names "
                        f"must be declared in {MANIFEST_REL}",
                        line=unit.line,
                    )
                )
                failed = True
                continue
            if "=" not in token and _takes_a_value(arg):
                following = tokens[index] if index < len(tokens) else None
                # Guard an optional-value flag (`--local-model`, `num_args` 0..1)
                # and the invocation boundary: neither a flag nor a new `curie`
                # can be this flag's value.
                if following is not None and not following.startswith("-") and following != _CURIE:
                    is_flag_value = True
            continue

        children = _subcommands(node)
        if not children:
            continue  # the leaf is reached; this is a positional value

        child = _child(node, token)
        if child is None:
            findings.append(
                Finding(
                    doc_rel,
                    cited,
                    f"`{token}` is not a subcommand of `{' '.join(words[:-1])}`; every "
                    "command a gated command doc names must resolve in "
                    f"{MANIFEST_REL}",
                    line=unit.line,
                )
            )
            failed = True
            continue

        in_scope = in_scope + _global_args(node)
        node = child

    if not failed and node is root:
        findings.append(
            Finding(
                doc_rel,
                " ".join(words),
                "names no subcommand, so nothing about it is verified; write the command "
                f"out in full so it resolves in {MANIFEST_REL}",
                line=unit.line,
            )
        )

    return index, findings


def _check_unit(
    unit: _Unit, root: dict[str, Any], doc_rel: str
) -> tuple[list[Finding], int]:
    """Resolve every invocation in one unit; return the findings and how many there were."""
    stripped = (_strip_token(token) for token in unit.text.split())
    # A token that was pure punctuation (`$(` written detached) strips to
    # nothing, and an empty string resolves as no subcommand of anything.
    tokens = [token for token in stripped if token]
    findings: list[Finding] = []
    invocations = 0
    index = 0

    while index < len(tokens):
        if tokens[index] != _CURIE:
            index += 1
            continue
        invocations += 1
        index, invocation_findings = _resolve_invocation(
            tokens, index, root, unit, doc_rel
        )
        findings.extend(invocation_findings)

    return findings, invocations


def _check_command_doc(
    repo_root: Path,
    rel: str,
    manifest: dict[str, Any] | None,
    missing_citation: str,
    missing_reason: str,
    empty_reason: str,
) -> list[Finding]:
    """Resolve one gated command doc against the manifest, or report its absence."""
    path = repo_root / rel
    if not path.is_file():
        return [Finding(rel, missing_citation, missing_reason)]
    if manifest is None:
        return []

    text = path.read_text(encoding="utf-8")
    findings: list[Finding] = []
    invocations = 0
    for unit in _scanned_units(text):
        unit_findings, unit_invocations = _check_unit(unit, manifest, rel)
        findings.extend(unit_findings)
        invocations += unit_invocations

    if invocations == 0:
        findings.append(Finding(rel, missing_citation, empty_reason))
    return findings


def check_agent_contract(repo_root: Path) -> list[Finding]:
    """Assert every command the gated command docs name against the manifest (#1041, #2247).

    Args:
        repo_root: The repository root the docs and the manifest resolve under.

    Returns:
        One finding per offending invocation -- never deduplicated by line, so
        two bad commands on one line stay two readable findings -- plus a
        finding for a missing or unparseable required artifact and one for a
        doc that names no command at all. An empty list when every command
        resolves.

    Two limits are deliberate, and a reader should not over-trust the gate past
    them. Once the walk reaches a LEAF, every remaining non-flag token is taken
    as a positional value and accepted unconditionally, so
    `curie skill status bogusgarbage` passes: the manifest does not say which
    leaves take positionals, and rejecting them would false-fail every real
    argument the contract writes out. And a placeholder is only caught in its
    angle-bracket spelling, which is the spelling the contract's own prose bans;
    a bare `TIER` is indistinguishable from a positional value here.
    """
    findings: list[Finding] = []

    manifest, manifest_finding = _load_manifest(repo_root)
    if manifest_finding is not None:
        findings.append(manifest_finding)

    findings.extend(
        _check_command_doc(
            repo_root,
            CONTRACT_REL,
            manifest,
            "agent-contract",
            f"{CONTRACT_REL} is missing; it is a required artifact -- README.md, "
            "llms.txt and `curie guide` all point an agent at it, and its absence "
            "empties this gate rather than failing it. Restore the file, or retire "
            "the contract deliberately",
            "no `curie` command appears inside a code construct any more, so the "
            "contract is no longer verified against "
            f"{MANIFEST_REL}. Commands are only checked inside backticks or a fenced "
            "block; restore them, or retire the contract deliberately",
        )
    )
    findings.extend(
        _check_command_doc(
            repo_root,
            DEMO_RUNBOOK_REL,
            manifest,
            "sre-demo-runbook",
            f"{DEMO_RUNBOOK_REL} is missing; it is a required artifact -- the SRE "
            "demo names `curie` flags an operator is told to run, and its absence "
            "empties this gate rather than failing it. Restore the file, or retire "
            "the runbook deliberately",
            "no `curie` command appears inside a code construct any more, so the "
            "runbook is no longer verified against "
            f"{MANIFEST_REL}. Commands are only checked inside backticks or a fenced "
            "block; restore them, or retire the runbook deliberately",
        )
    )
    return findings
