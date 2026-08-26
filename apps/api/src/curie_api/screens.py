"""Screens: the channel-neutral UI the control agent puts in front of a human.

ADR-0125. A screen is a titled page of blocks plus a row of buttons. It is
**semantic, not rendered**: nothing here knows Slack's Block Kit or Discord's
component JSON, because ADR-0020 makes rendering the channel adapter's job and
this is the port's side of that line. A screen that carried Slack JSON would
make every second channel a rewrite instead of an adapter.

Why screens at all, rather than the agent describing things in prose:

- **A button is a human action.** The model renders the screen; the click is
  authored by a person and authorized server-side against the operator set. So
  the interesting operations stop depending on the model's wording at all,
  which is the same reason ``proposals.py`` renders its own summaries.
- **A button cannot be ambiguous.** "roll it back" has to be resolved to an
  agent and a version by a model. A button carries the ids already resolved,
  and the label was written from the store.

Two kinds of button, and the difference is the whole safety story:

- ``navigate`` moves to another screen. Free, reversible, no authorization.
- ``invoke`` performs a fleet action. Requires a resolved operator identity
  (``routers/fleet.py::require_operator``) and writes an executed proposal row,
  so every mutation lands in the same audit trail a CLI-driven one does.

The catalog below is the complete set. Its coverage against the CLI is not a
claim in prose: ``SCREEN_FOR_COMMAND`` maps every leaf command in
``cli/command-manifest.json`` either to a screen or to a written exemption, and
``apps/api/tests/test_screen_coverage.py`` fails when a new command appears
under neither.
"""

from __future__ import annotations

import enum
import uuid
from dataclasses import dataclass, field
from typing import Any


class ButtonStyle(enum.StrEnum):
    """How prominent a button is. Advisory to the adapter -- Slack renders
    ``danger`` red, a plain-text channel might render it "[!]" -- so it is a
    semantic weight, never a color."""

    default = "default"
    primary = "primary"
    danger = "danger"


class BlockKind(enum.StrEnum):
    text = "text"
    fields = "fields"
    rows = "rows"
    divider = "divider"
    note = "note"


@dataclass(frozen=True)
class Block:
    """One piece of a screen's body.

    ``text`` is a paragraph. ``fields`` is label/value pairs (an adapter renders
    them as columns or as ``label: value`` lines). ``rows`` is a list of records
    with a shared column order -- a table where the channel can draw one.
    ``note`` is de-emphasized context. ``divider`` separates sections.
    """

    kind: BlockKind
    text: str | None = None
    fields: dict[str, str] = field(default_factory=dict)
    columns: list[str] = field(default_factory=list)
    rows: list[dict[str, str]] = field(default_factory=list)


@dataclass(frozen=True)
class Button:
    id: str
    label: str
    kind: str  # "navigate" | "invoke"
    style: ButtonStyle = ButtonStyle.default
    # For navigate: the screen id. For invoke: the action name in proposals.ACTIONS.
    target: str = ""
    params: dict[str, Any] = field(default_factory=dict)
    # Shown before an invoke runs. Required on every destructive button by
    # ``test_destructive_buttons_confirm``: a mis-tap must cost a dialog, not an
    # agent. Written from store facts, like a proposal summary, never by a model.
    confirm: str | None = None


@dataclass(frozen=True)
class Screen:
    id: str
    title: str
    blocks: list[Block] = field(default_factory=list)
    buttons: list[Button] = field(default_factory=list)
    # The screen a "back" control returns to, or None for the root.
    parent: str | None = None
    subtitle: str | None = None


def _agent_ref(agent_id: uuid.UUID | str) -> str:
    return str(agent_id)


# --- The catalog -------------------------------------------------------------
#
# Screen ids are stable strings: a button rendered into a Slack message last
# week still resolves after a redeploy, so they are treated as a wire contract
# and not renamed casually.

HOME = "home"
FLEET = "fleet"
AGENT = "agent"
VERSIONS = "versions"
BUDGET = "budget"
OVERRIDES = "overrides"
MEMORY = "memory"
APPROVALS = "approvals"
PROPOSALS = "proposals"
THREADS = "threads"
OBSERVABILITY = "observability"
EVALS = "evals"
SURFACES = "surfaces"
DANGER = "danger"

# The pseudo-action a proposals-screen button carries. Not a member of any
# action table: it means "run the pending row this button names", which goes
# through the ordinary execute path rather than composing a new action.
PROPOSAL_BUTTON_TARGET = "__proposal__"

# Every screen id the catalog can produce, for the coverage test and for the
# API's 404 on an unknown id.
SCREEN_IDS: frozenset[str] = frozenset(
    {
        HOME,
        FLEET,
        AGENT,
        VERSIONS,
        BUDGET,
        OVERRIDES,
        MEMORY,
        APPROVALS,
        PROPOSALS,
        THREADS,
        OBSERVABILITY,
        EVALS,
        SURFACES,
        DANGER,
    }
)


class Exempt(enum.StrEnum):
    """Why a CLI command has no screen.

    Each value is a REASON A CHAT SURFACE CANNOT HONESTLY OFFER IT, not a
    backlog note. A command exempted here is one that would have to lie about
    what it did if a button claimed to run it, so the categories are worth
    reading as a description of the boundary between a CLI and a chat agent.
    """

    # Operates on the operator's own machine: a working directory, a Docker
    # daemon, a local file. A chat agent has none of those; it runs in a
    # sandbox on a cluster, and a button that "scaffolds a bundle" would create
    # it somewhere the operator cannot reach.
    local_machine = "local-machine"
    # Needs a source checkout and dev toolchains (`curie dev ...`). Fenced off
    # from released binaries too -- the CLI itself refuses these outside a
    # checkout, so a chat surface offering them would be offering less than the
    # CLI does.
    source_checkout = "source-checkout"
    # Installs, upgrades, or destroys the platform the agent is running ON. An
    # agent cannot uninstall its own substrate and report the result; the turn
    # dies with the cluster. This is a correctness boundary, not caution.
    substrate_lifecycle = "substrate-lifecycle"
    # Takes a credential as input (a Slack token, a GitHub App key, a sealing
    # key). Credentials must not be typed into a chat message: the channel
    # stores it, indexes it, and shows it to everyone in the room.
    credential_entry = "credential-entry"
    # Prints the CLI's own machine-readable surface for a coding agent driving
    # it. The chat agent IS the consumer of that surface, so re-exposing it as a
    # screen is a mirror pointed at a mirror.
    cli_introspection = "cli-introspection"
    # The command whose whole purpose is to stand in for a chat channel when you
    # do not have one. You are in the channel.
    is_the_channel = "is-the-channel"


# Every LEAF command in cli/command-manifest.json maps to a screen id or an
# Exempt reason. The coverage test walks the manifest, so this dict cannot
# silently fall behind the CLI: a new leaf command fails the suite until it is
# named here.
#
# Keys are the full command path, space-joined, exactly as the manifest spells
# it ("cluster budget", not "budget").
SCREEN_FOR_COMMAND: dict[str, str | Exempt] = {
    # -- The deployed-platform verbs. These are the product of this feature. ---
    "cluster status": FLEET,
    "local status": FLEET,
    "cluster versions": VERSIONS,
    "local versions": VERSIONS,
    "cluster kill": AGENT,
    "local kill": AGENT,
    "cluster resume": AGENT,
    "local resume": AGENT,
    "cluster budget": BUDGET,
    "local budget": BUDGET,
    "cluster overrides": OVERRIDES,
    "local overrides": OVERRIDES,
    "cluster memory": MEMORY,
    "local memory": MEMORY,
    "cluster approvals": APPROVALS,
    "local approvals": APPROVALS,
    "cluster reset-thread": THREADS,
    "local reset-thread": THREADS,
    "cluster observability": OBSERVABILITY,
    "local observability": OBSERVABILITY,
    "cluster eval": EVALS,
    "local eval": EVALS,
    "cluster surfaces": SURFACES,
    "local surfaces": SURFACES,
    "cluster delete": DANGER,
    "local delete": DANGER,
    # -- Everything else, with the reason it is not a screen. -----------------
    #
    # This half is the honest half. A control agent that claimed the whole CLI
    # would be claiming it can scaffold files on your laptop and uninstall the
    # cluster it is running inside. It cannot, and the categories say why.
    #
    # The operator's own machine: a working directory, a Docker daemon, a file.
    "try": Exempt.local_machine,
    "init": Exempt.local_machine,
    "skill up": Exempt.local_machine,
    "skill check": Exempt.local_machine,
    "skill approvals": Exempt.local_machine,
    "skill versions": Exempt.local_machine,
    "skill memory": Exempt.local_machine,
    "skill down": Exempt.local_machine,
    "skill status": Exempt.local_machine,
    "skill message": Exempt.local_machine,
    "skill eval": Exempt.local_machine,
    "skill eval-init": Exempt.local_machine,
    "local up": Exempt.local_machine,
    "local rebuild": Exempt.local_machine,
    "local down": Exempt.local_machine,
    "list-agents": Exempt.local_machine,
    "deploy-local": Exempt.local_machine,
    "build": Exempt.local_machine,
    "install": Exempt.local_machine,
    "update": Exempt.local_machine,
    "interactive": Exempt.local_machine,
    "doctor": Exempt.local_machine,
    "example sre-bot install": Exempt.local_machine,
    # A deploy reads a bundle out of a working directory and uploads it. The
    # bytes live on the operator's disk, so there is nothing for a chat button
    # to send. Redeploying a version the platform ALREADY holds is a different
    # operation and does have a button: it is rollback, on the versions screen.
    "local deploy": Exempt.local_machine,
    "cluster deploy": Exempt.local_machine,
    # Installs, upgrades, or destroys the platform this agent runs on.
    "cluster up": Exempt.substrate_lifecycle,
    "cluster down": Exempt.substrate_lifecycle,
    "cluster migrate-store": Exempt.substrate_lifecycle,
    "apply": Exempt.substrate_lifecycle,
    "diff": Exempt.substrate_lifecycle,
    # Takes a credential as input. Not into a chat message.
    "local comms": Exempt.credential_entry,
    "cluster comms": Exempt.credential_entry,
    "cluster github-app": Exempt.credential_entry,
    "seal": Exempt.credential_entry,
    "secrets set": Exempt.credential_entry,
    "secrets list": Exempt.credential_entry,
    "secrets unset": Exempt.credential_entry,
    # Stands in for a chat channel when you have none.
    "local message": Exempt.is_the_channel,
    "cluster message": Exempt.is_the_channel,
    # The CLI describing itself to a coding agent.
    "schema": Exempt.cli_introspection,
    "schema-index": Exempt.cli_introspection,
    "guide": Exempt.cli_introspection,
    # Source checkout and dev toolchains; the CLI itself refuses these from a
    # released binary.
    "dev contracts": Exempt.source_checkout,
    "dev chart-check": Exempt.source_checkout,
    "dev verify-fix-pin": Exempt.source_checkout,
    "dev e2e": Exempt.source_checkout,
    "dev e2e-ladder": Exempt.source_checkout,
    "dev chart-runtime-e2e": Exempt.source_checkout,
    "dev docs-lint": Exempt.source_checkout,
    "dev plugin-compat": Exempt.source_checkout,
    "dev eval-falsifiability": Exempt.source_checkout,
    "dev field-parity": Exempt.source_checkout,
    "dev emit-parity": Exempt.source_checkout,
    "dev verb-parity": Exempt.source_checkout,
    "dev schema-baseline": Exempt.source_checkout,
    "dev netpol-check": Exempt.source_checkout,
    "dev version-check": Exempt.source_checkout,
    "dev wire-tolerance": Exempt.source_checkout,
    "dev bump-version": Exempt.source_checkout,
}


def screen_for(command_path: str) -> str | Exempt | None:
    return SCREEN_FOR_COMMAND.get(command_path)
