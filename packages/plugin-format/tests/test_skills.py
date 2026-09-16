"""The single normalization boundary for skill ``allowed-tools`` (D1/D3/D4).

Everything a consumer knows about ``allowed-tools`` comes through
``parse_allowed_tools``. That is not a style preference: ``approval.py``'s #1852
gate-shadow check reads the field, and an entry this helper loses is an entry the
boot check cannot see -- a bundle that reports its gate as armed while executing
the tool. So the coverage here is deliberately deeper than the field's surface
area suggests, and the paren-aware cases are the load-bearing ones.
"""

import pytest
from plugin_format import parse_allowed_tools
from plugin_format.skills import allowed_tools_style, unserializable_entries

# --- parse_allowed_tools: the string form ------------------------------------


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        # The canonical shape the Agent Skills specification names.
        ("Read Bash", ["Read", "Bash"]),
        # Claude Code documents "space- OR comma-separated", so both split.
        ("Read,Bash", ["Read", "Bash"]),
        ("Read, Bash", ["Read", "Bash"]),
        # Any whitespace run, not just a single space.
        ("Read\t Bash\n", ["Read", "Bash"]),
        ("  Read   Bash  ", ["Read", "Bash"]),
        # Separator-only and empty inputs yield nothing rather than an empty entry.
        ("", []),
        ("   ", []),
        (",", []),
        (" , , ", []),
        # A specifier is part of the entry, never re-split.
        ("Bash(git:*) Read", ["Bash(git:*)", "Read"]),
        # D4: whitespace INSIDE a specifier must not split. A paren-blind
        # splitter yields ["Bash(git", "commit:*)"], _entry_tool returns None for
        # the first and a garbage name for the second, and the gate shadow is
        # missed entirely. This is the fail-open in string form.
        ("Bash(git commit:*) Read", ["Bash(git commit:*)", "Read"]),
        ("Read Bash(git commit:*)", ["Read", "Bash(git commit:*)"]),
        # A comma inside a specifier is likewise part of the entry.
        ("Bash(a,b) Read", ["Bash(a,b)", "Read"]),
        # Nested parens keep the depth walk honest.
        ("Bash(a(b c)d) Read", ["Bash(a(b c)d)", "Read"]),
        # Unbalanced parens absorb to end of string. Never raises; the entry is
        # named later by unserializable_entries, not lost here.
        ("Bash(git", ["Bash(git"]),
        ("Bash(git Read", ["Bash(git Read"]),
        # A stray close paren at depth 0 must not drive depth negative and
        # thereby suppress the following separator.
        ("oops) Read", ["oops)", "Read"]),
    ],
)
def test_parse_allowed_tools_splits_a_string_at_paren_depth_zero(
    value: str, expected: list[str]
) -> None:
    assert parse_allowed_tools(value) == expected


# --- parse_allowed_tools: the list form --------------------------------------


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (["Read", "Bash"], ["Read", "Bash"]),
        # A list is already itemized: entries are kept VERBATIM and never
        # re-split, so a specifier survives whatever it contains.
        (["Bash(git:*)"], ["Bash(git:*)"]),
        (["Bash(git commit:*)"], ["Bash(git commit:*)"]),
        (["Read Write"], ["Read Write"]),
        # Non-str items are filtered rather than coerced: a tolerant consumer on
        # a malformed bundle must not raise.
        (["Read", 3, None, "Bash"], ["Read", "Bash"]),
        ([3, None, {"a": 1}], []),
        # Surrounding whitespace is stripped; empties are dropped.
        (["  Read  ", "", "   ", "Bash"], ["Read", "Bash"]),
        ([], []),
    ],
)
def test_parse_allowed_tools_keeps_list_entries_verbatim(
    value: list[object], expected: list[str]
) -> None:
    assert parse_allowed_tools(value) == expected


@pytest.mark.parametrize("value", [None, 3, 0, {"a": 1}, True, False, 1.5, ("Read",), object()])
def test_parse_allowed_tools_never_raises_on_a_malformed_value(value: object) -> None:
    """Defensive by contract: ``_skill_allowed_tools`` is deliberately tolerant.

    A bundle whose frontmatter is nonsense is already reported by
    ``validate_bundle``; raising here would make the gate-shadow boot check fail
    on the wrong defect and hide the real one.
    """
    assert parse_allowed_tools(value) == []


def test_the_helper_is_exported_from_the_package_root() -> None:
    """Consumers import the public name, not the module path (A7)."""
    import plugin_format

    assert "parse_allowed_tools" in plugin_format.__all__
    assert plugin_format.PROFILE_CLAUDE_PLUGIN == "claude-plugin"
    assert plugin_format.PROFILE_AGENT_SKILLS_STRICT == "agent-skills-strict"


# --- the D4 round-trip identity ----------------------------------------------


@pytest.mark.parametrize(
    "entries",
    [
        ["Read"],
        ["Read", "Bash"],
        ["Bash(git:*)", "Read"],
        ["Bash(git commit:*)", "Read"],
        ["Bash(a,b)", "Read"],
        ["mcp__crm__send", "Read"],
        [],
    ],
)
def test_a_serializable_list_survives_the_canonical_join(entries: list[str]) -> None:
    """The invariant the canonical scalar rests on: join then parse is identity.

    ``cli/src/scaffold.rs`` emits ``" ".join(entries)`` as one YAML scalar. If
    that does not parse back to the same list, the scaffold silently corrupts an
    author's permission rules.
    """
    assert parse_allowed_tools(" ".join(entries)) == entries
    assert unserializable_entries(entries) == []


def test_an_unbalanced_entry_breaks_the_identity_at_the_LIST_level() -> None:
    """Why unbalanced parens are a NAMED rejection and not caught by the join test.

    ``"Bash(git"`` round-trips cleanly on its own -- a depth-aware splitter
    returns it verbatim. The loss appears only once a following entry exists: the
    open paren swallows it, and two entries collapse into one.
    """
    entries = ["Bash(git", "Read"]
    assert parse_allowed_tools(" ".join(entries)) == ["Bash(git Read"]
    assert parse_allowed_tools(" ".join(entries)) != entries
    # ...which is exactly why the per-entry predicate names it independently.
    assert unserializable_entries(entries) == ["Bash(git"]


# --- unserializable_entries ---------------------------------------------------


@pytest.mark.parametrize(
    "entry",
    [
        "Read",
        "Bash",
        "Bash()",
        "Bash(*)",
        "Bash(git:*)",
        # The correction to an earlier draft: paren-awareness makes this
        # round-trip, so it is serializable and must NOT be flagged.
        "Bash(git commit:*)",
        "Bash(a,b)",
        "Bash(a(b c)d)",
        "mcp__crm__send",
    ],
)
def test_a_separator_free_entry_is_serializable(entry: str) -> None:
    assert unserializable_entries([entry]) == []


@pytest.mark.parametrize(
    "entry",
    [
        # Whitespace at depth 0.
        "Read Write",
        "Read\tWrite",
        # A comma at depth 0.
        "Read,Write",
        "Read , Write",
        # Unbalanced parens: depth never returns to 0...
        "Bash(git",
        "Bash(a(b)",
        # ...or a close paren appears at depth 0.
        "oops)",
        "Bash(a))",
    ],
)
def test_an_entry_that_cannot_round_trip_is_named(entry: str) -> None:
    assert unserializable_entries([entry]) == [entry]


def test_only_the_offending_entries_are_named() -> None:
    """The message has to point at the entry to fix, not at the whole list."""
    entries = ["Read", "Bash(git commit:*)", "Read Write", "Bash(git", "Bash(a,b)"]
    assert unserializable_entries(entries) == ["Read Write", "Bash(git"]


# --- allowed_tools_style (D3) -------------------------------------------------
#
# pyyaml parses `[Read, Bash]` and a `- Read` block identically, so raw text is
# unavoidable. But raw text ALONE misclassifies a quoted "[Read]" string, so the
# PARSED value decides string-vs-list first and raw text is consulted only for a
# list.


def _fm(body: str) -> str:
    """The raw frontmatter text ``_read_frontmatter`` hands back (``parts[1]``)."""
    return f"\nname: demo\ndescription: A demo skill.\n{body}"


@pytest.mark.parametrize(
    ("raw", "parsed", "expected"),
    [
        # Absent / null: the parsed value settles it before raw text is read.
        (_fm(""), None, "absent"),
        (_fm("allowed-tools:\n"), None, "absent"),
        # String forms.
        (_fm("allowed-tools: Read Bash\n"), "Read Bash", "string"),
        (_fm('allowed-tools: "Read,Bash"\n'), "Read,Bash", "string"),
        (_fm('allowed-tools: ""\n'), "", "string"),
        # The trap: a QUOTED string that begins with `[`. Raw text alone reads
        # this as a flow sequence; the parsed type is the authority.
        (_fm('allowed-tools: "[Read]"\n'), "[Read]", "string"),
        # Block list.
        (_fm("allowed-tools:\n  - Read\n  - Bash\n"), ["Read", "Bash"], "block"),
        (_fm("allowed-tools:\n- Read\n"), ["Read"], "block"),
        # Flow list on the key's own line.
        (_fm("allowed-tools: [Read, Bash]\n"), ["Read", "Bash"], "flow"),
        (_fm("allowed-tools: [ Read ]\n"), ["Read"], "flow"),
        # The empty flow list three runner fixtures ship.
        (_fm("allowed-tools: []\n"), [], "flow"),
        # A multi-line flow sequence that OPENS on the key line.
        (_fm("allowed-tools: [Read,\n  Bash]\n"), ["Read", "Bash"], "flow"),
        # F3: a multi-line flow sequence that opens on the NEXT line. A
        # single-line peek reads this as a block list, and because a block list
        # is only a warning under the strict profile while a flow list is an
        # error, that misread is a false PASS on a publishability gate.
        (_fm("allowed-tools:\n  [Read]\n"), ["Read"], "flow"),
        (_fm("allowed-tools:\n  [Read, Bash]\n"), ["Read", "Bash"], "flow"),
        # A trailing comment must not be read as the remainder.
        (_fm("allowed-tools:  # the tools\n  - Read\n"), ["Read"], "block"),
        (_fm("allowed-tools: # the tools\n  [Read]\n"), ["Read"], "flow"),
        (_fm("allowed-tools: [Read]  # the tools\n"), ["Read"], "flow"),
        # Blank and comment lines are skipped by the forward scan.
        (_fm("allowed-tools:\n\n  # why\n  [Read]\n"), ["Read"], "flow"),
        (_fm("allowed-tools:\n\n  # why\n  - Read\n"), ["Read"], "block"),
    ],
)
def test_allowed_tools_style_classifies_the_authored_shape(
    raw: str, parsed: object, expected: str
) -> None:
    assert allowed_tools_style(raw, parsed) == expected


@pytest.mark.parametrize(
    ("body", "parsed", "expected"),
    [
        ("allowed-tools:\r\n  - Read\r\n", ["Read"], "block"),
        ("allowed-tools: [Read]\r\n", ["Read"], "flow"),
        # The \r must be stripped BEFORE the scan: left in place it is the
        # remainder's first non-space character and nothing ever reads as flow.
        ("allowed-tools:\r\n  [Read]\r\n", ["Read"], "flow"),
    ],
)
def test_allowed_tools_style_handles_crlf_line_endings(
    body: str, parsed: object, expected: str
) -> None:
    raw = f"\r\nname: demo\r\ndescription: A demo skill.\r\n{body}"
    assert allowed_tools_style(raw, parsed) == expected


@pytest.mark.parametrize(
    ("raw", "parsed"),
    [
        # An alias: the value is a list, but no literal sequence is present at
        # the key to read. Recorded in ADR-0135 as a known limitation.
        ("\nbase: &base\n  - Read\nallowed-tools: *base\n", ["Read"]),
        # A key line the scan cannot find at all (indented under something else).
        ("\nname: demo\nnested:\n  allowed-tools:\n    - Read\n", ["Read"]),
    ],
)
def test_exotic_yaml_with_no_literal_sequence_falls_back_to_block(raw: str, parsed: object) -> None:
    """Block is the SAFE fallback: it is a warning in both profiles.

    Falling back to ``flow`` would hard-fail a strict-profile bundle on YAML the
    detector simply could not read, which is a false FAIL; falling back to
    ``string`` would suppress the finding entirely, which is a false PASS.
    """
    assert allowed_tools_style(raw, parsed) == "block"
