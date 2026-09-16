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
in the document. It cannot tell you the entry is accurate, or current, or honest
about what the grant permits. It tells you someone was made to write one, which
is the step that was skipped both times.
"""

import json
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]
EXAMPLES = REPO / "examples"


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
def test_a_gated_write_is_named_in_the_permission_map(
    bundle: str, gate: str, doc: Path
) -> None:
    assert doc.is_file(), (
        f"{bundle} declares the gated write '{gate}' but has no "
        f"docs/PERMISSION-MAP.md. A bundle that can write needs the document "
        f"that says which writes, permitted by whom, bounded by what."
    )
    # The exact live tool name also appears inside any documented signature, so
    # this catches both table rows and prose forms without parsing Markdown.
    assert gate in doc.read_text(encoding="utf-8"), (
        f"{bundle}'s permission map does not mention '{gate}'. That document "
        f"claims to list every write this bot can perform, so an unlisted gated "
        f"verb makes it confidently wrong rather than merely incomplete. Add an "
        f"entry saying what the grant actually permits, what scopes it, and what "
        f"is deliberately not granted -- or drop the gate and its connector."
    )
