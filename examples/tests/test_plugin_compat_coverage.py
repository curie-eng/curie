"""The plugin-compat gate must exercise all six Curie authoring extensions.

`scripts/check-plugin-compat.sh` runs `claude plugin validate` over every bundle
under `examples/`, asserting our six extension fields (systemPrompt,
starterPrompts, secrets, triggers, approvalPolicy, toolPolicy) stay
warn-and-ignored rather than rejected by Claude Code. But the gate can only
exercise a field that some discovered bundle actually declares -- and before the
compat-fixture bundle, three of the then-five (systemPrompt, triggers,
approvalPolicy) appeared in NO example manifest, so the gate silently covered
only two of them (#538).

This test defends that coverage: every one of the six fields must appear in at
least one discovered example bundle, so a future edit that drops a field from the
fixture (leaving the gate to cover it vacuously) fails here.
"""

import json
from pathlib import Path

EXAMPLES = Path(__file__).resolve().parents[1]

# The six Curie authoring extensions on the plugin manifest (plugin_format
# models.py); each is unknown-to-Claude-Code by design and warned-and-ignored.
_EXTENSION_FIELDS = (
    "systemPrompt",
    "starterPrompts",
    "secrets",
    "triggers",
    "approvalPolicy",
    "toolPolicy",
)


def _discover_manifests() -> list[dict]:
    """Every example bundle's parsed plugin manifest (same glob the gate uses)."""
    manifests = []
    for child in EXAMPLES.iterdir():
        manifest = child / ".claude-plugin" / "plugin.json"
        if child.is_dir() and child.name != "tests" and manifest.is_file():
            manifests.append(json.loads(manifest.read_text()))
    return manifests


def test_every_extension_field_is_exercised_by_some_bundle() -> None:
    manifests = _discover_manifests()
    assert manifests, "no example bundles discovered; the gate would pass vacuously"
    uncovered = [
        field for field in _EXTENSION_FIELDS if not any(field in manifest for manifest in manifests)
    ]
    assert not uncovered, (
        "these Curie extension fields appear in NO example bundle, so the "
        f"plugin-compat gate never exercises them: {uncovered}. Add them to the "
        "compat-fixture bundle (examples/compat-fixture/.claude-plugin/plugin.json)."
    )
