"""Every approval-controlled connector write has a permission-map entry.

The failure this exists to stop has now happened twice, the same way both times.

``examples/sre-bot/docs/PERMISSION-MAP.md`` opens with "Every write this bot can
perform". `scale_deployment` shipped and the document did not mention it
(curie#1952). Then `upgrade_self` shipped and the document did not mention that
either -- and `upgrade_self` is the verb that replaces the bot's own running
version, carrying the one grant in the bundle that RBAC cannot narrow.

Nothing reported either omission, because nothing could: a gate validates against
the connector that publishes it, never against prose. The document stays true by
someone remembering, and the thing being documented is exactly the thing a
reviewer reads the document instead of deriving. So a permission map that has
silently stopped being complete is worse than no permission map: it answers
"which writes can this bot do" with a confident, short, wrong list.

Scoped to CONNECTOR tools (``mcp__*``), which is what this document is about: a
verb reached through a connector carries a credential and an RBAC surface, and
those are the two things an entry has to state. A gate on a built-in like
``Bash`` is a different question with a different answer, and demanding a
permission-map entry for one would have this check failing on
``examples/compat-fixture`` -- a test fixture with no write path at all.

The check is deliberately shallow -- the gate's tool name must appear somewhere
in the document, as one complete identifier token (``_names_gate``). A longer
tool name that merely starts with the gate names a different tool and never
satisfies it. Even so, it cannot tell you the entry is accurate, or current, or
honest about what the grant permits. It tells you someone was made to write
one, which is the step that was skipped both times.
"""

import json
import re
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]
EXAMPLES = REPO / "examples"


def _names_gate(text: str, gate: str) -> bool:
    """Does this permission-map text name the gate as one complete token?

    Identifier boundaries are alphanumerics plus underscore, hyphen and dot,
    so a longer identifier sharing the gate's prefix -- ``get_status``,
    ``get-other``, ``get.status``, ``get__extra`` -- names a different tool
    and is not evidence for the gate. Backticks, spaces and parentheses are
    not identifier punctuation, so exact names in code spans, table cells and
    documented signatures such as ``mcp__acme__get()`` still count. A mention
    left bare under identifier punctuation (a sentence-final period, say) must
    be written as an explicit code span instead. The gate is escaped whole,
    double underscores included; no fragment is ever split off.
    """
    return re.search(rf"(?<![\w.-]){re.escape(gate)}(?![\w.-])", text) is not None


def _gated_bundles() -> list[tuple[str, str, Path]]:
    """``(bundle, gate, permission_map)`` for every gate in every bundle.

    A bundle with gates but no permission map is reported by
    ``test_a_bundle_with_gated_writes_has_a_permission_map`` rather than skipped
    here, so adding the first gated verb to a bundle cannot pass vacuously.
    """

    found: list[tuple[str, str, Path]] = []
    for manifest in sorted(EXAMPLES.glob("*/.claude-plugin/plugin.json")):
        bundle = manifest.parent.parent.name
        parsed = json.loads(manifest.read_text(encoding="utf-8"))
        gates = ((parsed.get("approvalPolicy") or {}).get("gates")) or []
        doc = manifest.parent.parent / "docs" / "PERMISSION-MAP.md"
        for gate in gates:
            name = gate.get("gate") if isinstance(gate, dict) else None
            # Connector tools only: see the module docstring.
            if name and str(name).startswith("mcp__"):
                found.append((bundle, str(name), doc))
        for canonical in ((parsed.get("toolPolicy") or {}).get("approvalRequired")) or []:
            connector, separator, tool = str(canonical).partition("/")
            # Exact connector/tool entries describe a concrete mutation. Wildcard
            # policy fixtures do not identify one documentable live action.
            if separator and connector and tool and "*" not in str(canonical):
                found.append((bundle, f"mcp__{connector}__{tool}", doc))
    return found


def test_the_bundles_and_their_gates_are_readable() -> None:
    # A guard that silently finds nothing passes vacuously, which is the failure
    # mode of every check that reads files by glob.
    assert _gated_bundles(), "no gated writes found: check the glob"


@pytest.mark.parametrize(
    "bundle,gate,doc",
    _gated_bundles(),
    ids=[f"{b}:{g}" for b, g, _ in _gated_bundles()],
)
def test_a_gated_write_is_named_in_the_permission_map(bundle: str, gate: str, doc: Path) -> None:
    assert doc.is_file(), (
        f"{bundle} declares the gated write '{gate}' but has no "
        f"docs/PERMISSION-MAP.md. A bundle that can write needs the document "
        f"that says which writes, permitted by whom, bounded by what."
    )
    # The gate has to appear as one complete identifier token; a longer tool
    # name that merely starts with the gate is evidence for a different tool.
    # Exact names inside documented signatures still count, so this catches
    # table rows, code spans and signature forms without parsing Markdown.
    assert _names_gate(doc.read_text(encoding="utf-8"), gate), (
        f"{bundle}'s permission map does not mention '{gate}'. That document "
        f"claims to list every write this bot can perform, so an unlisted gated "
        f"verb makes it confidently wrong rather than merely incomplete. Add an "
        f"entry saying what the grant actually permits, what scopes it, and what "
        f"is deliberately not granted -- or drop the gate and its connector."
    )


_TOKEN_BOUNDARY_GATE = "mcp__acme__get"


def _drive_the_reader(doc: Path, text: str) -> None:
    """Call the real reader above with an anonymous document, #2293 item 19.

    The reader is the gate, so the boundary cases drive it directly instead of
    re-deriving the check against a private helper: a matcher that passes these
    probes but that the reader does not use would prove nothing.
    """
    doc.write_text(text, encoding="utf-8")
    test_a_gated_write_is_named_in_the_permission_map("acme-bot", _TOKEN_BOUNDARY_GATE, doc)


@pytest.mark.parametrize(
    "suffix",
    ["_status", "__extra", "-other", ".status"],
    ids=["underscore", "double-underscore", "hyphen", "dot"],
)
def test_token_boundary_refuses_a_longer_prefix_only_name(suffix: str, tmp_path: Path) -> None:
    # A longer identifier sharing the gate's prefix names a different tool. On
    # the substring reader it counted as documentation for the shorter gate,
    # which is exactly the false green this class exists to stop.
    with pytest.raises(AssertionError, match="does not mention"):
        _drive_the_reader(
            tmp_path / "PERMISSION-MAP.md",
            f"Every write this bot can perform: mcp__acme__get{suffix}.\n",
        )


def test_token_boundary_refuses_a_bare_name_left_under_identifier_punctuation(
    tmp_path: Path,
) -> None:
    # A bare mention adjacent to identifier punctuation -- here a sentence-final
    # period -- is not a token mention; it must be written as an explicit code
    # span instead (the code-span form is accepted below).
    with pytest.raises(AssertionError, match="does not mention"):
        _drive_the_reader(tmp_path / "PERMISSION-MAP.md", "call mcp__acme__get.\n")


@pytest.mark.parametrize(
    "mention",
    [
        "The gated read is `mcp__acme__get`.",
        "| Tool | Effect |\n| --- | --- |\n| mcp__acme__get | Reads one value. |",
        "The connector exposes mcp__acme__get() with no arguments.",
        "This bot can call mcp__acme__get to read one value.",
    ],
    ids=["code-span", "table-cell", "signature", "prose"],
)
def test_token_boundary_accepts_the_exact_gate_in_code_table_and_signature_forms(
    mention: str, tmp_path: Path
) -> None:
    # The exact-name control: each documented form still counts, so the
    # boundary rule tightens the match without demanding new documentation
    # shapes. Double underscores stay part of the name throughout.
    _drive_the_reader(tmp_path / "PERMISSION-MAP.md", mention)
