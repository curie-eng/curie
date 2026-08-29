"""Does the control agent actually reach every CLI feature? (ADR-0133)

The claim "every feature of the CLI is available in some screen" is worth
nothing as prose, because the CLI grows and prose does not. So it is a test over
``cli/command-manifest.json``, the clap-derived surface the CLI itself emits.

Two directions, both needed. A command with no entry fails (the CLI grew and the
agent silently cannot do it). An entry naming no command also fails (a screen
claims to answer something that no longer exists). Without the second direction
the map rots into a list of things that used to be true.

No database and no HTTP: this reads the manifest and the vocabulary module, so
it runs in milliseconds and fails on the machine of whoever added the command.
"""

from __future__ import annotations

import json
from collections.abc import Iterator
from pathlib import Path
from typing import Any

from curie_api import proposals, screenbuild, screens

REPO_ROOT = Path(__file__).resolve().parents[3]
MANIFEST = REPO_ROOT / "cli" / "command-manifest.json"


def _leaves(node: dict[str, Any], path: tuple[str, ...] = ()) -> Iterator[str]:
    """Every runnable command path. A node with subcommands is a namespace --
    `curie cluster` alone runs nothing -- so only leaves are commands."""

    for sub in node.get("subcommands") or []:
        here = path + (sub["name"],)
        if sub.get("subcommands"):
            yield from _leaves(sub, here)
        else:
            yield " ".join(here)


def _manifest_leaves() -> list[str]:
    return sorted(_leaves(json.loads(MANIFEST.read_text())))


def test_every_cli_command_has_a_screen_or_a_written_exemption() -> None:
    missing = [c for c in _manifest_leaves() if screens.screen_for(c) is None]
    assert not missing, (
        "these CLI commands are reachable from `curie` but from no screen, and "
        "are not exempted. Add each to screens.SCREEN_FOR_COMMAND -- either a "
        "screen id, or an Exempt reason saying why a chat surface cannot "
        "honestly offer it:\n  " + "\n  ".join(missing)
    )


def test_the_map_names_no_command_the_cli_does_not_have() -> None:
    known = set(_manifest_leaves())
    stale = sorted(c for c in screens.SCREEN_FOR_COMMAND if c not in known)
    assert not stale, (
        "these entries name commands the CLI no longer has; drop them so the "
        "coverage number stays honest:\n  " + "\n  ".join(stale)
    )


def test_every_named_screen_is_a_screen_that_can_be_built() -> None:
    """A map entry pointing at a screen id nobody builds would report coverage
    for a page that 404s."""

    named = {
        target
        for target in screens.SCREEN_FOR_COMMAND.values()
        if not isinstance(target, screens.Exempt)
    }
    assert named <= set(screenbuild.BUILDERS), named - set(screenbuild.BUILDERS)
    assert set(screenbuild.BUILDERS) == set(screens.SCREEN_IDS)


def test_coverage_is_reported_accurately() -> None:
    """The headline number matches the map, so a summary cannot drift from it."""

    leaves = _manifest_leaves()
    # Exempt is a StrEnum, so an isinstance(..., str) test would count every
    # exemption as covered and report 100%. Ask the narrow question first.
    exempt = [c for c in leaves if isinstance(screens.screen_for(c), screens.Exempt)]
    covered = [c for c in leaves if c not in set(exempt)]
    assert len(covered) + len(exempt) == len(leaves)
    # Not an arbitrary floor: these are the deployed-platform verbs, the ones a
    # chat surface can serve at all. If this drops, a screen stopped answering
    # something it used to.
    assert len(covered) >= 26


def test_exemptions_are_all_used() -> None:
    """An Exempt value nothing uses is a category we invented and did not need;
    it makes the boundary look more considered than it is."""

    used = {t for t in screens.SCREEN_FOR_COMMAND.values() if isinstance(t, screens.Exempt)}
    unused = sorted(str(e) for e in screens.Exempt if e not in used)
    assert not unused, f"unused exemption categories: {unused}"


def test_destructive_buttons_are_not_proposable_by_the_agent() -> None:
    """The control agent's vocabulary and the operator-only one stay disjoint.

    If ``delete_agent`` ever appeared in ``ACTIONS``, the agent could raise it as
    a proposal and the whole "a model cannot ask for this" claim would be false
    while every other test still passed.
    """

    assert not set(proposals.ACTIONS) & set(proposals.OPERATOR_ONLY_ACTIONS)
    assert "delete_agent" in proposals.OPERATOR_ONLY_ACTIONS
    assert "delete_agent" not in proposals.ACTIONS
