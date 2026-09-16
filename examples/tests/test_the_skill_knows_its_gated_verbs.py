"""A bundle's skill names every gated verb the bundle ships.

The failure this exists to stop, from the install it happened on:

`upgrade_platform` shipped as a connector tool, was published, was gated, was
installed, and answered `tools/list` correctly. Asked to use it, the bot called
`Bash` once and replied "all done" -- having done nothing.

The cause was one stale sentence. The skill had been written when the verb did
not exist, and said so:

    **The platform: no.** Moving the Curie release from one version to another
    is a Helm operation ... and you have no tool for it by design.

That instruction outlived the fact. Nothing connected the two: the gate validates
against the connector that publishes it, the connector's own tests validate the
tool, and neither has any view of the prose that tells the model whether to reach
for it. A capability the skill talks the model out of is indistinguishable, from
every surface an operator watches, from one that was never built.

The check is deliberately shallow -- the tool's bare name must appear somewhere
in the bundle's skill. It cannot tell you the skill describes the verb correctly,
or that the description is current. It tells you the skill has heard of it, which
is the step that was skipped.

Naming: the gate `mcp__self-upgrade__upgrade_platform` is satisfied by the skill
mentioning `upgrade_platform`, because that is how a skill refers to a tool.
"""

import json
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]
EXAMPLES = REPO / "examples"


def _gated_verbs() -> list[tuple[str, str, Path]]:
    """``(bundle, tool name, skill file)`` for every gated connector verb.

    Scoped to ``mcp__`` gates for the same reason the permission-map guard is: a
    gate on a built-in like ``Bash`` is not a capability the bundle ships, and
    demanding the skill name it would fail on a fixture with no skill at all.
    """

    found: list[tuple[str, str, Path]] = []
    for manifest in sorted(EXAMPLES.glob("*/.claude-plugin/plugin.json")):
        bundle_dir = manifest.parent.parent
        parsed = json.loads(manifest.read_text(encoding="utf-8"))
        gates = ((parsed.get("approvalPolicy") or {}).get("gates")) or []
        for gate in gates:
            name = gate.get("gate") if isinstance(gate, dict) else None
            if not name or not str(name).startswith("mcp__"):
                continue
            # `mcp__<server>__<tool>` -> `<tool>`
            tool = str(name).rsplit("__", 1)[-1]
            for skill in sorted(bundle_dir.glob("skills/*/SKILL.md")):
                found.append((bundle_dir.name, tool, skill))
    return found


def test_the_bundles_and_their_skills_are_readable() -> None:
    # A guard that silently finds nothing passes vacuously, which is the failure
    # mode of every check that reads files by glob.
    assert _gated_verbs(), "no gated verbs with skills found: check the glob"


@pytest.mark.parametrize(
    "bundle,tool,skill",
    _gated_verbs(),
    ids=[f"{b}:{t}" for b, t, _ in _gated_verbs()],
)
def test_the_skill_names_the_gated_verb(bundle: str, tool: str, skill: Path) -> None:
    assert tool in skill.read_text(encoding="utf-8"), (
        f"{bundle} ships the gated verb '{tool}' and {skill.name} never mentions "
        f"it. The model reads that file to decide what it can do, so a verb the "
        f"skill is silent about -- or worse, one it says does not exist -- is a "
        f"capability the bot will not use and may deny having. Describe it there, "
        f"or drop the gate and its connector."
    )
