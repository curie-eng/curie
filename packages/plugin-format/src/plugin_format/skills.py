"""The Agent Skills conformance vocabulary and the ``allowed-tools`` boundary.

Two things live here, both shared by the validator and (for
``parse_allowed_tools``) by the runner:

- **The profile ids.** ``validate_bundle`` runs one rule set under two profiles:
  ``claude-plugin`` is the ingestion contract and reports every specification
  divergence as a warning; ``agent-skills-strict`` is a publishability gate and
  reports the same findings as errors (ADR-0135).
- **``parse_allowed_tools``, the single normalization boundary (D1).** Everything
  a consumer knows about ``allowed-tools`` comes through it. That is not a style
  preference: ``runner/src/curie_runner/approval.py``'s #1852 gate-shadow check
  reads the field, and an entry this helper loses is an entry the boot check
  cannot see -- a bundle that reports its Bash gate as armed while executing Bash
  unapproved. A second reader of this field is how that fail-open comes back.

``allowed_tools_style`` and ``unserializable_entries`` are validator internals and
are deliberately NOT exported from the package root: they answer "how was this
authored" and "would this survive the canonical serialization", questions only a
conformance check has, while a runtime consumer only ever needs the entries.
"""

import re

# The two conformance profiles. `claude-plugin` is the DEFAULT everywhere and is
# permanently what runner boot (`runner/src/curie_runner/plugin.py`) and deploy
# ingestion (`apps/api/src/curie_api/bundles.py`) validate under.
PROFILE_CLAUDE_PLUGIN = "claude-plugin"
PROFILE_AGENT_SKILLS_STRICT = "agent-skills-strict"
PROFILES = (PROFILE_CLAUDE_PLUGIN, PROFILE_AGENT_SKILLS_STRICT)

# The Agent Skills specification's closed world, in spec order. This is an
# ALLOWLIST on purpose: a denylist of the known Claude Code extras
# (`disable-model-invocation`, `user-invocable`, `argument-hint`, ...) goes stale
# the next time Claude Code adds a field, and a publishability gate that silently
# passes an unknown field is a false PASS.
SPEC_FIELDS = ("name", "description", "license", "compatibility", "metadata", "allowed-tools")

# The specification's skill-name rule: lowercase alphanumerics with single
# internal hyphens. Deliberately a LOCAL constant rather than a reuse of
# ``validate._NAME_RE`` (the Claude Code plugin manifest's kebab rule): the two
# rules happen to coincide today, and a future change to the manifest rule must
# not silently move this one. Length (1-64) is checked separately so a finding
# can name which of the two rules failed.
SKILL_NAME_RE = re.compile(r"^[a-z0-9]+(-[a-z0-9]+)*$")

# The zero-indent ``allowed-tools:`` line, with whatever follows it on that line.
# Anchored at column 0 so a nested key of the same name is never mistaken for the
# top-level one.
_KEY_LINE_RE = re.compile(r"^allowed-tools:[ \t]*(.*)$")


def parse_allowed_tools(value: str | list[str] | None) -> list[str]:
    """The entries a skill's ``allowed-tools`` declaration names, normalized.

    The ONE reader of this field (D1). Accepts either authored shape -- the
    canonical space-separated string or a YAML list -- and never raises, whatever
    it is handed: ``_skill_allowed_tools`` is deliberately tolerant, and a bundle
    whose frontmatter is nonsense is already reported by ``validate_bundle``.
    Raising here would fail the gate-shadow boot check on the wrong defect and
    hide the real one, so any other input yields ``[]``.

    A **string** is split on whitespace or a comma at PAREN DEPTH 0 only (D4).
    ``re.split(r"[\\s,]+", value)`` is explicitly the wrong implementation:
    ``"Bash(git commit:*)"`` is a realistic Claude Code permission rule, and
    splitting it into ``Bash(git`` / ``commit:*)`` makes ``_entry_tool`` return
    ``None`` for one fragment and a garbage tool name for the other -- the entry
    disappears from gate-shadow detection while looking like it was read.

    A **list** is already itemized, so its ``str`` entries are kept VERBATIM
    (surrounding whitespace stripped, empties dropped) and never re-split. Non-str
    items are filtered rather than coerced.
    """

    if isinstance(value, str):
        return _split_at_depth_zero(value)
    if isinstance(value, list):
        return [entry.strip() for entry in value if isinstance(entry, str) and entry.strip()]
    return []


def _split_at_depth_zero(value: str) -> list[str]:
    """Cut ``value`` at whitespace or commas that are outside every paren group."""

    entries: list[str] = []
    current: list[str] = []
    depth = 0
    for char in value:
        if char == "(":
            depth += 1
        elif char == ")":
            # Floored at 0 so a stray close paren cannot drive the depth
            # negative and thereby suppress every separator after it.
            depth = max(0, depth - 1)
        elif depth == 0 and (char.isspace() or char == ","):
            if current:
                entries.append("".join(current))
                current = []
            continue
        current.append(char)
    if current:
        entries.append("".join(current))
    return entries


def unserializable_entries(entries: list[str]) -> list[str]:
    """The entries that cannot survive the canonical ``" ".join`` round-trip (D4).

    The invariant the canonical scalar rests on is a list-level identity:
    ``parse_allowed_tools(" ".join(entries)) == entries``. This is the equivalent
    PER-ENTRY predicate, so a finding can name the entry to fix rather than the
    whole list -- an entry that is empty, or that carries a comma or whitespace at
    paren depth 0, or whose parens are unbalanced.

    Unbalanced parens have to be named separately because a depth-aware splitter
    round-trips ``"Bash(git"`` cleanly ON ITS OWN; the loss appears only at the
    list level, where the open paren swallows the following entry
    (``["Bash(git", "Read"]`` joins to ``"Bash(git Read"``, which parses back as
    ONE entry). The list-level identity alone would therefore miss it in the
    single-entry case.
    """

    return [entry for entry in entries if _cannot_round_trip(entry)]


def _cannot_round_trip(entry: str) -> bool:
    if not entry:
        return True
    depth = 0
    for char in entry:
        if char == "(":
            depth += 1
        elif char == ")":
            if depth == 0:
                return True
            depth -= 1
        elif depth == 0 and (char.isspace() or char == ","):
            return True
    return depth != 0


def allowed_tools_style(raw_frontmatter: str, parsed_value: object) -> str:
    """How ``allowed-tools`` was AUTHORED: ``absent``/``string``/``block``/``flow``.

    pyyaml parses ``[Read, Bash]`` and a ``- Read`` block into the same list, so
    raw-text inspection is unavoidable to tell them apart. But raw text ALONE
    misreads ``allowed-tools: "[Read]"`` -- a quoted string that merely begins
    with ``[`` -- so the PARSED value decides absent-vs-string-vs-list first, and
    the raw text is consulted only once the value is known to be a list (D3).

    Within the list case the raw scan is a FORWARD scan, not a peek at the key's
    line. ``allowed-tools:\\n  [Read]`` is a syntactically valid multiline flow
    sequence; a single-line detector reads it as a block list, and since a block
    list is only a warning under the strict profile while a flow list is an error,
    that misread would be a false PASS on a publishability gate.

    ``block`` is the fallback because it is the SAFE one: it is a warning in both
    profiles. Falling back to ``flow`` would hard-fail a strict bundle on YAML the
    detector simply could not read (a false FAIL); falling back to ``string``
    would suppress the finding entirely (a false PASS). The fallback now survives
    only for exotic YAML with no literal sequence at the key to read -- an anchor,
    an alias, or a merge key resolving to a list -- recorded as a known limitation
    in ADR-0135.
    """

    # `allowed-tools: null` parses to None exactly as an absent key does, and
    # declares no tools either way, so it is reported as absent rather than as an
    # empty declaration. A value that is neither a string nor a list is already a
    # `skill.frontmatter_invalid` error from the model; there is no authored
    # SHAPE to report about it.
    if parsed_value is None or not isinstance(parsed_value, str | list):
        return "absent"
    if isinstance(parsed_value, str):
        return "string"

    # A `\r` left in place would be the remainder's first non-space character, so
    # a CRLF file would never read as flow.
    lines = raw_frontmatter.replace("\r\n", "\n").replace("\r", "\n").split("\n")
    for index, line in enumerate(lines):
        match = _KEY_LINE_RE.match(line)
        if match is None:
            continue
        remainder = _strip_inline_comment(match.group(1)).strip()
        if remainder:
            return "flow" if remainder.startswith("[") else "block"
        for following in lines[index + 1 :]:
            stripped = following.strip()
            if not stripped or stripped.startswith("#"):
                continue
            return "flow" if stripped.startswith("[") else "block"
        return "block"
    return "block"


def _strip_inline_comment(text: str) -> str:
    """Drop a YAML trailing comment: a ``#`` at the start or after whitespace."""

    if text.startswith("#"):
        return ""
    index = text.find("#")
    while index > 0:
        if text[index - 1] in " \t":
            return text[:index]
        index = text.find("#", index + 1)
    return text
