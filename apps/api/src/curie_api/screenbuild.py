"""Building each screen from the store (ADR-0125).

Kept apart from ``screens.py`` on purpose: that module is the vocabulary (what a
screen and a button ARE, and which CLI command each screen answers), this one is
the queries. The split is what lets the coverage test import the vocabulary
without a database.

Every label a human reads here is written from a value this module just read.
That is the same rule ``proposals.py`` follows for its summaries and it exists
for the same reason: the click is the authorization, so the text the click is
based on has to come from the platform rather than from a model.
"""

from __future__ import annotations

import uuid
from typing import Any

from sqlalchemy import select

from . import crud
from .config import get_settings
from .killswitch import KillSwitch
from .models import Agent, ApprovalStatus, ControlProposal, Environment, ProposalStatus
from .routers.memory import _get_log_entry, _provenance_of, _records_of
from .screens import (
    AGENT,
    APPROVALS,
    BUDGET,
    DANGER,
    EVALS,
    FLEET,
    HOME,
    MEMORY,
    OBSERVABILITY,
    OVERRIDES,
    PROPOSAL_BUTTON_TARGET,
    PROPOSALS,
    SURFACES,
    THREADS,
    VERSIONS,
    Block,
    BlockKind,
    Button,
    ButtonStyle,
    Screen,
)

# Budget presets. Chosen values rather than a free-text field because a chat
# button cannot ask for arbitrary input without a modal, and because the common
# operator move is "cap it hard, now" rather than "set it to $7.40".
_BUDGET_PRESETS = (5.0, 25.0, 100.0)
_THINKING_PRESETS = ("disabled", "low", "medium", "high", "adaptive")


def _nav(id_: str, label: str, target: str, **params: Any) -> Button:
    return Button(id=id_, label=label, kind="navigate", target=target, params=params)


def _invoke(
    id_: str,
    label: str,
    action: str,
    *,
    style: ButtonStyle = ButtonStyle.default,
    confirm: str | None = None,
    **params: Any,
) -> Button:
    return Button(
        id=id_,
        label=label,
        kind="invoke",
        target=action,
        style=style,
        confirm=confirm,
        params=params,
    )


async def _agent_or_none(session: Any, params: dict[str, Any]) -> Agent | None:
    raw = params.get("agent_id")
    if not raw:
        return None
    try:
        agent_id = uuid.UUID(str(raw))
    except ValueError:
        return None
    return await crud.get_agent(session, agent_id)


def _missing_agent(parent: str) -> Screen:
    return Screen(
        id=FLEET,
        title="Pick an agent",
        parent=parent,
        blocks=[Block(kind=BlockKind.text, text="That agent no longer exists.")],
        buttons=[_nav("to-fleet", "Back to the fleet", FLEET)],
    )


async def _labels_for(session: Any, agent: Agent) -> dict[str, str | None]:
    out: dict[str, str | None] = {}
    for env in (Environment.prod, Environment.dev):
        deployment = await crud.get_active_deployment(session, agent.id, env)
        if deployment is None:
            out[env.value] = None
            continue
        version = await crud.get_version(session, deployment.version_id)
        out[env.value] = version.version_label if version else None
    return out


# --- Screens -----------------------------------------------------------------


async def build_home(session: Any, kill_switch: KillSwitch, params: dict[str, Any]) -> Screen:
    agents = await crud.list_agents(session)
    killed = [a for a in agents if await kill_switch.is_killed(a.id)]
    pending = list(
        await session.scalars(
            select(ControlProposal).where(
                ControlProposal.status == ProposalStatus.pending.value
            )
        )
    )
    approvals = await crud.list_approvals(session, status=ApprovalStatus.pending.value)

    blocks = [
        Block(
            kind=BlockKind.fields,
            fields={
                "Agents": str(len(agents)),
                "Killed": str(len(killed)),
                "Pending approvals": str(len(approvals)),
                "Pending proposals": str(len(pending)),
            },
        )
    ]
    # Anything needing a human is said in words as well as counted: a number in
    # a field block is easy to skim past, and these are the two that mean
    # somebody is currently blocked.
    if approvals:
        blocks.append(
            Block(
                kind=BlockKind.text,
                text=f"{len(approvals)} approval(s) are waiting on a person.",
            )
        )
    if pending:
        blocks.append(
            Block(
                kind=BlockKind.text,
                text=f"{len(pending)} proposal(s) are waiting to be run or declined.",
            )
        )

    return Screen(
        id=HOME,
        title="Curie",
        subtitle="Everything running on this platform",
        blocks=blocks,
        buttons=[
            _nav("to-fleet", "Agents", FLEET),
            _nav("to-approvals", "Approvals", APPROVALS),
            _nav("to-proposals", "Proposals", PROPOSALS),
            _nav("to-observability", "Traces & cost", OBSERVABILITY),
        ],
    )


async def build_fleet(session: Any, kill_switch: KillSwitch, params: dict[str, Any]) -> Screen:
    """`curie <tier> status`, as a list you can tap into."""

    agents = await crud.list_agents(session)
    rows = []
    buttons = []
    for agent in agents:
        labels = await _labels_for(session, agent)
        killed = await kill_switch.is_killed(agent.id)
        rows.append(
            {
                "agent": agent.name,
                "state": "killed" if killed else "running",
                "prod": labels[Environment.prod.value] or "-",
                "dev": labels[Environment.dev.value] or "-",
            }
        )
        buttons.append(_nav(f"open-{agent.id}", agent.name, AGENT, agent_id=str(agent.id)))

    blocks = (
        [Block(kind=BlockKind.rows, columns=["agent", "state", "prod", "dev"], rows=rows)]
        if rows
        else [Block(kind=BlockKind.text, text="No agents are deployed yet.")]
    )
    return Screen(
        id=FLEET,
        title="Agents",
        parent=HOME,
        blocks=blocks,
        buttons=buttons,
    )


async def build_agent(session: Any, kill_switch: KillSwitch, params: dict[str, Any]) -> Screen:
    agent = await _agent_or_none(session, params)
    if agent is None:
        return _missing_agent(HOME)
    killed = await kill_switch.is_killed(agent.id)
    labels = await _labels_for(session, agent)
    ref = str(agent.id)

    blocks = [
        Block(
            kind=BlockKind.fields,
            fields={
                "State": "killed" if killed else "running",
                "Prod": labels[Environment.prod.value] or "nothing deployed",
                "Dev": labels[Environment.dev.value] or "nothing deployed",
                "Model": agent.model or "platform default",
                "Thinking": agent.thinking or "platform default",
                "Daily cap": (
                    "platform default"
                    if agent.max_usd_per_day is None
                    else f"${agent.max_usd_per_day:.2f}"
                ),
            },
        )
    ]

    # Kill and resume are one button, not two greyed ones: the screen already
    # knows which state it is in, so offering the inapplicable one is offering a
    # tap that can only fail.
    if killed:
        toggle = _invoke(
            "resume",
            "Resume",
            "resume",
            style=ButtonStyle.primary,
            agent_id=ref,
            confirm=f"Let '{agent.name}' start taking new turns again?",
        )
    else:
        toggle = _invoke(
            "kill",
            "Kill",
            "kill",
            style=ButtonStyle.danger,
            agent_id=ref,
            confirm=(
                f"Stop '{agent.name}' from starting new turns? In-flight turns "
                "finish; new messages go unanswered until you resume it."
            ),
        )

    return Screen(
        id=AGENT,
        title=agent.name,
        parent=FLEET,
        blocks=blocks,
        buttons=[
            toggle,
            _nav("to-versions", "Versions", VERSIONS, agent_id=ref),
            _nav("to-budget", "Budget", BUDGET, agent_id=ref),
            _nav("to-overrides", "Model", OVERRIDES, agent_id=ref),
            _nav("to-memory", "Memory", MEMORY, agent_id=ref),
            _nav("to-threads", "Threads", THREADS, agent_id=ref),
            _nav("to-evals", "Evals", EVALS, agent_id=ref),
            _nav("to-surfaces", "Surfaces", SURFACES, agent_id=ref),
            _nav("to-danger", "Danger zone", DANGER, agent_id=ref),
        ],
    )


async def build_versions(session: Any, kill_switch: KillSwitch, params: dict[str, Any]) -> Screen:
    """`curie <tier> versions`, with a rollback button beside each one."""

    agent = await _agent_or_none(session, params)
    if agent is None:
        return _missing_agent(FLEET)
    versions = await crud.list_versions(session, agent.id)

    active: dict[uuid.UUID, list[str]] = {}
    for env in (Environment.prod, Environment.dev):
        deployment = await crud.get_active_deployment(session, agent.id, env)
        if deployment is not None:
            active.setdefault(deployment.version_id, []).append(env.value)

    rows = [
        {
            "version": v.version_label,
            "active": ", ".join(active.get(v.id, [])) or "-",
            "commit": (v.commit_sha or "")[:8] or "-",
            "built": v.created_at.strftime("%Y-%m-%d %H:%M"),
        }
        for v in versions
    ]
    prod_label = next(
        (
            v.version_label
            for v in versions
            if Environment.prod.value in active.get(v.id, [])
        ),
        None,
    )
    buttons = [
        _invoke(
            f"rollback-{v.id}",
            f"Roll back to {v.version_label}",
            "rollback",
            style=ButtonStyle.danger,
            agent_id=str(agent.id),
            version_id=str(v.id),
            env=Environment.prod.value,
            confirm=(
                f"Move '{agent.name}' in prod from {prod_label} to "
                f"{v.version_label}? The bundle is redeployed as-is; nothing rebuilds."
            ),
        )
        # Only versions that are not already live, and only when something IS
        # live to roll back FROM -- the action refuses both cases anyway, so
        # rendering the button would just be a tap that returns an error.
        for v in versions
        if prod_label is not None and Environment.prod.value not in active.get(v.id, [])
    ]

    blocks = (
        [Block(kind=BlockKind.rows, columns=["version", "active", "commit", "built"], rows=rows)]
        if rows
        else [Block(kind=BlockKind.text, text="This agent has no versions yet.")]
    )
    if prod_label is None and rows:
        blocks.append(
            Block(
                kind=BlockKind.note,
                text="Nothing is deployed in prod, so there is nothing to roll back.",
            )
        )
    return Screen(
        id=VERSIONS,
        title=f"{agent.name} — versions",
        parent=AGENT,
        blocks=blocks,
        buttons=buttons,
    )


async def build_budget(session: Any, kill_switch: KillSwitch, params: dict[str, Any]) -> Screen:
    """`curie <tier> budget`."""

    agent = await _agent_or_none(session, params)
    if agent is None:
        return _missing_agent(FLEET)
    current = (
        "platform default"
        if agent.max_usd_per_day is None
        else f"${agent.max_usd_per_day:.2f}/day"
    )
    buttons = [
        _invoke(
            f"budget-{value:g}",
            f"${value:g}/day",
            "set_budget",
            agent_id=str(agent.id),
            max_usd_per_day=value,
            confirm=f"Cap '{agent.name}' at ${value:.2f}/day? It is currently {current}.",
        )
        for value in _BUDGET_PRESETS
        if agent.max_usd_per_day != value
    ]
    if agent.max_usd_per_day is not None:
        buttons.append(
            _invoke(
                "budget-default",
                "Platform default",
                "set_budget",
                agent_id=str(agent.id),
                max_usd_per_day=None,
                confirm=(
                    f"Drop '{agent.name}' back to the platform default cap, "
                    f"from {current}?"
                ),
            )
        )
    return Screen(
        id=BUDGET,
        title=f"{agent.name} — budget",
        parent=AGENT,
        blocks=[
            Block(kind=BlockKind.fields, fields={"Daily cap": current}),
            Block(
                kind=BlockKind.note,
                text=(
                    "The cap bounds model spend per day. Reaching it stops new "
                    "turns until the day rolls over."
                ),
            ),
        ],
        buttons=buttons,
    )


async def build_overrides(session: Any, kill_switch: KillSwitch, params: dict[str, Any]) -> Screen:
    """`curie <tier> overrides`: the model and thinking-depth pins."""

    agent = await _agent_or_none(session, params)
    if agent is None:
        return _missing_agent(FLEET)
    ref = str(agent.id)
    buttons = [
        _invoke(
            f"thinking-{level}",
            f"Thinking: {level}",
            "set_thinking",
            agent_id=ref,
            thinking=level,
            confirm=f"Set '{agent.name}' thinking depth to {level}?",
        )
        for level in _THINKING_PRESETS
        if agent.thinking != level
    ]
    if agent.thinking is not None:
        buttons.append(
            _invoke(
                "thinking-default",
                "Thinking: platform default",
                "set_thinking",
                agent_id=ref,
                thinking=None,
                confirm=f"Drop '{agent.name}' back to the platform thinking default?",
            )
        )
    return Screen(
        id=OVERRIDES,
        title=f"{agent.name} — model",
        parent=AGENT,
        blocks=[
            Block(
                kind=BlockKind.fields,
                fields={
                    "Model": agent.model or "platform default",
                    "Thinking": agent.thinking or "platform default",
                },
            ),
            Block(
                kind=BlockKind.note,
                text=(
                    "Changing the model is not a button: the legal values depend "
                    "on the provider this install points at, so there is no list "
                    "to render. Ask for it by name and a proposal is raised."
                ),
            ),
        ],
        buttons=buttons,
    )


async def build_memory(session: Any, kill_switch: KillSwitch, params: dict[str, Any]) -> Screen:
    """`curie <tier> memory`: what the agent has learned. Read-only."""

    agent = await _agent_or_none(session, params)
    if agent is None:
        return _missing_agent(FLEET)
    # Read through the memory router's own log accessors rather than a second
    # query: the log's shape (a records list under one reserved key) is that
    # module's business, and duplicating the read is how a malformed-entry
    # tolerance rule ends up implemented twice.
    entry = await _get_log_entry(session, agent.id)
    records = _records_of(entry)
    rows = [
        {
            "#": str(i),
            "learned": str(r.get("content", ""))[:120],
            "source": str(_provenance_of(r).get("trace_id") or "-")[:12],
        }
        for i, r in enumerate(records)
    ]
    blocks = (
        [Block(kind=BlockKind.rows, columns=["#", "learned", "source"], rows=rows)]
        if rows
        else [Block(kind=BlockKind.text, text="This agent has not recorded anything yet.")]
    )
    blocks.append(
        Block(
            kind=BlockKind.note,
            text=(
                "Editing and deleting memory is deliberately not a chat button: "
                "it rewrites what the agent believes, and doing that from a phone "
                "is how a fleet ends up with unexplainable behavior. Use the "
                "console or `curie <tier> memory`."
            ),
        )
    )
    return Screen(
        id=MEMORY,
        title=f"{agent.name} — memory",
        parent=AGENT,
        blocks=blocks,
    )


async def build_threads(session: Any, kill_switch: KillSwitch, params: dict[str, Any]) -> Screen:
    """`curie <tier> reset-thread`."""

    agent = await _agent_or_none(session, params)
    if agent is None:
        return _missing_agent(FLEET)
    thread_key = str(params.get("thread_key") or "").strip()
    buttons = []
    if thread_key:
        buttons.append(
            _invoke(
                "reset",
                f"Release {thread_key}",
                "reset_thread",
                style=ButtonStyle.danger,
                agent_id=str(agent.id),
                thread_key=thread_key,
                confirm=(
                    f"Release the sandbox for thread {thread_key}? The next "
                    "message starts a fresh one. The transcript is kept."
                ),
            )
        )
    return Screen(
        id=THREADS,
        title=f"{agent.name} — threads",
        parent=AGENT,
        blocks=[
            Block(
                kind=BlockKind.text,
                text=(
                    "A wedged thread keeps answering from a stale sandbox — an "
                    "old credential, a redeploy it never picked up. Releasing it "
                    "forces the next message to cold-start."
                ),
            ),
            Block(
                kind=BlockKind.note,
                text=(
                    "Name the thread to release and this screen grows the button. "
                    "There is no 'release all': a blanket reset drops every live "
                    "turn on the agent, including ones nobody complained about."
                ),
            ),
        ],
        buttons=buttons,
    )


async def build_evals(session: Any, kill_switch: KillSwitch, params: dict[str, Any]) -> Screen:
    """`curie <tier> eval`."""

    agent = await _agent_or_none(session, params)
    if agent is None:
        return _missing_agent(FLEET)
    deployment = await crud.get_active_deployment(session, agent.id, Environment.dev)
    version = (
        await crud.get_version(session, deployment.version_id) if deployment else None
    )
    label = version.version_label if version else None

    buttons = []
    if label is not None:
        buttons.append(
            _invoke(
                "run",
                f"Run evals on {label}",
                "run_eval",
                style=ButtonStyle.primary,
                agent_id=str(agent.id),
                suite=None,
                confirm=(
                    f"Run '{agent.name}' eval suite against dev {label}? "
                    "Every case calls the model, so this costs tokens."
                ),
            )
        )
    return Screen(
        id=EVALS,
        title=f"{agent.name} — evals",
        parent=AGENT,
        blocks=[
            Block(
                kind=BlockKind.fields,
                fields={"Dev version": label or "nothing deployed to dev"},
            ),
            Block(
                kind=BlockKind.note,
                text=(
                    "Runs the same suite the git-push gate runs. Results land in "
                    "the trace store; this screen enqueues the job and does not "
                    "wait for it."
                ),
            ),
        ],
        buttons=buttons,
    )


async def build_approvals(session: Any, kill_switch: KillSwitch, params: dict[str, Any]) -> Screen:
    """`curie <tier> approvals --list`. Read-only, and deliberately so.

    Resolving an approval already has a channel surface: the approval card
    (ADR-0078), which carries the authorizer, the self-approval block, and the
    audit trail. Growing a second resolve button here would be a second
    implementation of the most safety-critical path in the system, and the two
    would drift. So this screen reports, and points at the card.
    """

    pending = await crud.list_approvals(session, status=ApprovalStatus.pending.value)
    rows = [
        {
            "id": str(a.id)[:8],
            "asked by": a.author,
            "what": a.summary[:80],
            "at": a.created_at.strftime("%H:%M") if a.created_at else "-",
        }
        for a in pending
    ]
    blocks = (
        [Block(kind=BlockKind.rows, columns=["id", "asked by", "what", "at"], rows=rows)]
        if rows
        else [Block(kind=BlockKind.text, text="Nothing is waiting on an approval.")]
    )
    blocks.append(
        Block(
            kind=BlockKind.note,
            text=(
                "Approve or reject on the approval card in the channel where it "
                "was raised. That card enforces who may resolve and blocks "
                "self-approval; a second button here would be a second copy of "
                "that policy."
            ),
        )
    )
    return Screen(id=APPROVALS, title="Approvals", parent=HOME, blocks=blocks)


async def build_proposals(session: Any, kill_switch: KillSwitch, params: dict[str, Any]) -> Screen:
    """Proposals the control agent raised, waiting on a person."""

    rows_q = await session.scalars(
        select(ControlProposal)
        .where(ControlProposal.status == ProposalStatus.pending.value)
        .order_by(ControlProposal.created_at.desc())
    )
    pending = list(rows_q)
    names = {a.id: a.name for a in await crud.list_agents(session)}

    blocks: list[Block] = []
    buttons: list[Button] = []
    for proposal in pending:
        blocks.append(
            Block(
                kind=BlockKind.fields,
                fields={
                    "Proposal": str(proposal.id)[:8],
                    "Agent": names.get(proposal.target_agent_id, "?"),
                    "What": proposal.summary,
                    "Asked by": proposal.requested_by or "unrecorded",
                },
            )
        )
        buttons.append(
            Button(
                id=f"run-{proposal.id}",
                label=f"Run {str(proposal.id)[:8]}",
                kind="invoke",
                target=PROPOSAL_BUTTON_TARGET,
                style=ButtonStyle.danger,
                params={"proposal_id": str(proposal.id)},
                # The confirm is the API's own summary, verbatim. The agent that
                # raised this never wrote a word of it.
                confirm=proposal.summary,
            )
        )
    if not pending:
        blocks.append(Block(kind=BlockKind.text, text="No proposals are waiting."))
    return Screen(id=PROPOSALS, title="Proposals", parent=HOME, blocks=blocks, buttons=buttons)


async def build_observability(
    session: Any, kill_switch: KillSwitch, params: dict[str, Any]
) -> Screen:
    """`curie <tier> observability`: where to look, not a copy of what is there."""

    settings = get_settings()
    return Screen(
        id=OBSERVABILITY,
        title="Traces & cost",
        parent=HOME,
        blocks=[
            Block(
                kind=BlockKind.fields,
                fields={"Trace store": settings.langfuse_host or "not configured"},
            ),
            Block(
                kind=BlockKind.note,
                text=(
                    "Traces, token counts, and per-agent cost live in the trace "
                    "store and the console. This screen names where, rather than "
                    "pasting a chart into a chat message."
                ),
            ),
        ],
    )


async def build_surfaces(
    session: Any, kill_switch: KillSwitch, params: dict[str, Any]
) -> Screen:
    """`curie <tier> surfaces`: the channels this agent listens on.

    Removal is a button because it names a binding that already exists.
    ADDING one is not: it needs a channel id the platform cannot enumerate, and
    a chat button carries no free-text field. The screen says so rather than
    pretending the operation is missing.
    """

    agent = await _agent_or_none(session, params)
    if agent is None:
        return _missing_agent(FLEET)
    bindings = list(agent.channels)
    rows = [{"kind": b.kind, "address": b.address} for b in bindings]

    buttons = []
    # The last binding is deliberately not removable: an agent with none is
    # deployed, healthy-looking, and unable to receive a turn. Same rule the
    # CLI path enforces, applied here so the button never renders rather than
    # rendering and failing.
    if len(bindings) > 1:
        buttons = [
            _invoke(
                f"unbind-{b.kind}-{b.address}",
                f"Unbind {b.kind}:{b.address}",
                "remove_surface",
                style=ButtonStyle.danger,
                agent_id=str(agent.id),
                kind=b.kind,
                address=b.address,
                confirm=(
                    f"Stop '{agent.name}' answering on {b.kind}:{b.address}? "
                    f"It keeps its other {len(bindings) - 1} binding(s)."
                ),
            )
            for b in bindings
        ]

    blocks = (
        [Block(kind=BlockKind.rows, columns=["kind", "address"], rows=rows)]
        if rows
        else [Block(kind=BlockKind.text, text="This agent has no bindings.")]
    )
    if len(bindings) == 1:
        blocks.append(
            Block(
                kind=BlockKind.note,
                text=(
                    "This is the agent's only binding, so it cannot be removed: "
                    "an agent with none is deployed, healthy-looking, and unable "
                    "to receive a turn. Add another first."
                ),
            )
        )
    blocks.append(
        Block(
            kind=BlockKind.note,
            text=(
                "Adding a binding needs a channel id, which a button cannot "
                "carry and which the platform cannot enumerate for you. Use "
                "`curie <tier> surfaces --add` or the console."
            ),
        )
    )
    return Screen(
        id=SURFACES,
        title=f"{agent.name} — surfaces",
        parent=AGENT,
        blocks=blocks,
        buttons=buttons,
    )


async def build_danger(session: Any, kill_switch: KillSwitch, params: dict[str, Any]) -> Screen:
    """`curie <tier> delete`. Operator-only, behind a typed confirmation.

    Reached only by deliberately opening this screen, and the button carries
    ``confirm_agent_name``, so the tap alone is not the decision.
    """

    agent = await _agent_or_none(session, params)
    if agent is None:
        return _missing_agent(FLEET)
    versions = await crud.list_versions(session, agent.id)
    return Screen(
        id=DANGER,
        title=f"{agent.name} — danger zone",
        parent=AGENT,
        blocks=[
            Block(
                kind=BlockKind.text,
                text=(
                    f"Deleting '{agent.name}' removes it, its "
                    f"{len(versions)} version(s), and its deployment history. "
                    "There is no undo and no rollback afterwards."
                ),
            ),
            Block(
                kind=BlockKind.note,
                text=(
                    "The control agent cannot ask for this. It is not in the "
                    "vocabulary it may propose, so this button exists only for a "
                    "person who navigated here on purpose."
                ),
            ),
        ],
        buttons=[
            Button(
                id="delete",
                label=f"Delete {agent.name}",
                kind="invoke",
                target="delete_agent",
                style=ButtonStyle.danger,
                params={"agent_id": str(agent.id), "confirm_agent_name": agent.name},
                confirm=(
                    f"Type the agent's name ({agent.name}) to confirm. This "
                    "deletes it and its history permanently."
                ),
            )
        ],
    )


BUILDERS = {
    HOME: build_home,
    FLEET: build_fleet,
    AGENT: build_agent,
    VERSIONS: build_versions,
    BUDGET: build_budget,
    OVERRIDES: build_overrides,
    MEMORY: build_memory,
    THREADS: build_threads,
    EVALS: build_evals,
    SURFACES: build_surfaces,
    APPROVALS: build_approvals,
    PROPOSALS: build_proposals,
    OBSERVABILITY: build_observability,
    DANGER: build_danger,
}
