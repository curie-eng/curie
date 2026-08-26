"""The fleet control plane: what the control agent may read, and may only ask for.

ADR-0125. Every other router is platform-key-only (``require_api_key``) or
agent-self-scoped (``require_state_access``, ADR-0033). This one is the first
surface a *sandboxed agent* may call about agents other than itself, so the
whole file is organized around one question: what stops any other agent from
calling it too.

Three independent things do.

1. **The scope.** The token must carry scope ``control`` (``CONTROL_SCOPE``).
   Every other sandbox holds ``state`` / ``state.app`` tokens, which fail
   verification here -- the scope is part of the signed payload, so a state
   token cannot be re-presented as a control token.

2. **The name.** The token must be bound to the agent named by
   ``settings.control_agent``. Tokens are signed per-agent (ADR-0033), so
   another agent's control-scoped token -- if one could somehow be minted --
   still names the wrong agent and fails. With ``control_agent`` unset, which is
   the default, nothing verifies at all and the plane is platform-key-only.

3. **The mint site.** The worker only ever mints a control token for that same
   named agent (``binding.boot_env``), so no other sandbox is even handed one.
   Points 1 and 2 are what make that hold if the worker is wrong.

And one thing that is NOT a defense, stated plainly because it looks like one:
the control agent's own tool surface. The bundle ships read tools and a propose
tool and no execute tool, but a bundle is data and the model runs inside it, so
that arrangement is ergonomics, not a boundary. The boundary is
``execute_proposal`` below refusing every caller but the platform key --
server-side, where the sandbox cannot reach it.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta
from typing import Annotated, Any

from fastapi import APIRouter, Depends, Header, HTTPException, status
from sqlalchemy import select

from .. import crud, proposals, sandbox_token
from ..approvers import ExplicitUsers
from ..auth import verify_platform_key
from ..config import get_settings
from ..deps import EvalQueueDep, KillSwitchDep, SessionDep, ThreadResetRequestsDep
from ..killswitch import KillSwitch
from ..models import (
    Agent,
    ControlProposal,
    Environment,
    ProposalStatus,
)
from ..schemas import (
    CommandCoverageOut,
    CommandCoverageRow,
    FleetAgentSummary,
    FleetVersionSummary,
    ProposalActionInfo,
    ProposalCreate,
    ProposalExecute,
    ProposalOut,
    ScreenActionIn,
    ScreenActionOut,
    ScreenBlockOut,
    ScreenButtonOut,
    ScreenOut,
)
from ..screenbuild import BUILDERS
from ..screens import FLEET, PROPOSAL_BUTTON_TARGET, SCREEN_FOR_COMMAND, Exempt, Screen

# The scope claim a control token carries. Mirrored byte-identically at the
# worker mint site (``apps/worker/src/curie_worker/binding.py``), exactly as
# STATE_SCOPE / STATE_APP_SCOPE are -- the shared string IS the contract, and a
# committed test asserts both sides still agree.
CONTROL_SCOPE = "control"

# How long a proposal stays executable. Short on purpose: the summary a human
# reads describes the fleet as it was when the proposal was rendered, and the
# further that drifts the less the click means. Expiry is checked at execute
# time against this column, so a proposal does not need a sweeper to become
# unrunnable.
PROPOSAL_TTL = timedelta(minutes=30)


def _now() -> datetime:
    """UTC now, as a naive datetime.

    The columns are ``DateTime`` without a timezone (as every other timestamp in
    this schema is), so an aware value would raise on comparison against what
    Postgres returns. Computed from an explicitly UTC clock and stripped, rather
    than the deprecated ``utcnow``, so the two halves of that statement stay
    visible to the next reader."""

    return datetime.now(UTC).replace(tzinfo=None)


class ControlCaller:
    """Which credential authorized a fleet request.

    Mirrors ``StateCaller`` in the state router. Two values rather than a bool
    because the difference is load-bearing on exactly one route: PLATFORM may
    execute, CONTROL may not, and every route that mutates the fleet asks.
    """

    PLATFORM = "platform"
    CONTROL = "control"


async def require_control_access(
    session: SessionDep,
    x_api_key: Annotated[str | None, Header()] = None,
) -> str:
    """Fleet-plane auth: the platform key, or the control agent's scoped token.

    Returns which one, so the execute route can refuse the second. Order
    matters only for cost: the platform key is a constant-time compare, the
    token path costs a database lookup, so the cheap check goes first.
    """

    if verify_platform_key(x_api_key):
        return ControlCaller.PLATFORM

    settings = get_settings()
    # Unset control_agent is the off switch, and it is the default. Checked
    # before anything else on this path so an install that never opted in does
    # no lookup and has no code path where a token could verify.
    if not settings.control_agent or x_api_key is None:
        raise HTTPException(
            status.HTTP_401_UNAUTHORIZED, "missing or invalid API key"
        )

    agent = await crud.get_agent_by_name(session, settings.control_agent)
    if agent is None:
        # Named but not deployed yet. Refusing (rather than falling through to a
        # different check) keeps the failure legible: the plane is off until the
        # named agent actually exists.
        raise HTTPException(
            status.HTTP_401_UNAUTHORIZED, "missing or invalid API key"
        )

    if not sandbox_token.verify(
        x_api_key, settings.api_key, agent=str(agent.id), scope=CONTROL_SCOPE
    ):
        raise HTTPException(
            status.HTTP_401_UNAUTHORIZED, "missing or invalid API key"
        )
    return ControlCaller.CONTROL


ControlCallerDep = Annotated[str, Depends(require_control_access)]

router = APIRouter(prefix="/fleet", tags=["fleet"])


async def _control_agent_id(session: SessionDep) -> uuid.UUID | None:
    settings = get_settings()
    if not settings.control_agent:
        return None
    agent = await crud.get_agent_by_name(session, settings.control_agent)
    return agent.id if agent else None


async def _load_target(session: SessionDep, agent_id: uuid.UUID) -> Agent:
    agent = await crud.get_agent(session, agent_id)
    if agent is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "agent not found")
    return agent


async def _executable_target(session: SessionDep, proposal: ControlProposal) -> Agent:
    """The agent a pending proposal acts on, or a 409.

    A NULL ``target_agent_id`` means the agent was deleted after the proposal
    was raised. The row stays -- it is the audit trail -- but there is nothing
    left to act on, and saying so is more useful than a 404 that reads like the
    proposal is missing.
    """

    if proposal.target_agent_id is None:
        raise HTTPException(
            status.HTTP_409_CONFLICT,
            f"agent '{proposal.target_agent_name}' has been deleted; "
            "this proposal can no longer be run",
        )
    return await _load_target(session, proposal.target_agent_id)


@router.get("/agents", response_model=list[FleetAgentSummary])
async def list_fleet(
    session: SessionDep, kill_switch: KillSwitchDep, caller: ControlCallerDep
) -> list[FleetAgentSummary]:
    """Every agent, with what is deployed and whether it is killed.

    This is the read the whole feature exists to serve: a human asks "what is
    running" in a thread and gets an answer without opening the console.
    """

    agents = await crud.list_agents(session)
    summaries: list[FleetAgentSummary] = []
    for agent in agents:
        labels: dict[str, str | None] = {}
        for env in (Environment.prod, Environment.dev):
            deployment = await crud.get_active_deployment(session, agent.id, env)
            if deployment is None:
                labels[env.value] = None
                continue
            version = await crud.get_version(session, deployment.version_id)
            labels[env.value] = version.version_label if version else None
        summaries.append(
            FleetAgentSummary(
                id=agent.id,
                name=agent.name,
                killed=await kill_switch.is_killed(agent.id),
                model=agent.model,
                max_usd_per_day=agent.max_usd_per_day,
                prod_version_label=labels[Environment.prod.value],
                dev_version_label=labels[Environment.dev.value],
            )
        )
    return summaries


@router.get("/agents/{agent_id}/versions", response_model=list[FleetVersionSummary])
async def list_agent_versions(
    agent_id: uuid.UUID, session: SessionDep, caller: ControlCallerDep
) -> list[FleetVersionSummary]:
    """An agent's versions, marked with where each is currently active.

    The control agent needs this to propose a rollback at all: a rollback names
    a version id, and it has no other way to learn a legal one. Marking which
    are live is what lets it tell a human "back to the version that was running
    before this morning" instead of reciting UUIDs.
    """

    await _load_target(session, agent_id)
    versions = await crud.list_versions(session, agent_id)

    active: dict[uuid.UUID, list[str]] = {}
    for env in (Environment.prod, Environment.dev):
        deployment = await crud.get_active_deployment(session, agent_id, env)
        if deployment is not None:
            active.setdefault(deployment.version_id, []).append(env.value)

    return [
        FleetVersionSummary(
            id=version.id,
            label=version.version_label,
            commit_sha=version.commit_sha,
            created_at=version.created_at,
            active_in=active.get(version.id, []),
        )
        for version in versions
    ]


@router.get("/actions", response_model=list[ProposalActionInfo])
async def list_actions(caller: ControlCallerDep) -> list[ProposalActionInfo]:
    """The closed vocabulary of proposable actions.

    Served rather than documented so the control agent's own description of what
    it can ask for cannot drift from what the API will accept.
    """

    return [
        ProposalActionInfo(name=action.name, description=action.description)
        for action in sorted(proposals.ACTIONS.values(), key=lambda a: a.name)
    ]


def _out(proposal: ControlProposal) -> ProposalOut:
    """The row as written. The agent name is a stored column, not a join: a
    proposal whose agent has since been deleted must still say what it was
    about."""

    return ProposalOut.model_validate(proposal)


@router.post(
    "/proposals", response_model=ProposalOut, status_code=status.HTTP_201_CREATED
)
async def create_proposal(
    body: ProposalCreate, session: SessionDep, caller: ControlCallerDep
) -> ProposalOut:
    """Record a fleet action for a human to run. Executes nothing.

    The control agent is allowed here, and that is the point of the whole
    design: proposing is the most it can do. ``proposals.prepare`` validates the
    parameters against the closed vocabulary and renders the consequence line
    from the store, so what gets written is the API's description of the action,
    not the caller's.
    """

    target = await _load_target(session, body.target_agent_id)
    try:
        prepared = await proposals.prepare(session, target, body.action, body.params)
    except proposals.ProposalError as exc:
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_ENTITY, str(exc)) from exc

    proposal = ControlProposal(
        target_agent_id=target.id,
        target_agent_name=target.name,
        action=body.action,
        params=prepared.params,
        summary=prepared.summary,
        # Provenance, not authority: when the control agent proposed this, stamp
        # it; when the platform key did, leave it NULL so the two are
        # distinguishable in the record forever.
        proposed_by_agent_id=(
            await _control_agent_id(session) if caller == ControlCaller.CONTROL else None
        ),
        requested_by=body.requested_by,
        thread_key=body.thread_key,
        status=ProposalStatus.pending.value,
        expires_at=_now() + PROPOSAL_TTL,
    )
    session.add(proposal)
    await session.commit()
    await session.refresh(proposal)
    return _out(proposal)


@router.get("/proposals", response_model=list[ProposalOut])
async def list_proposals(
    session: SessionDep,
    caller: ControlCallerDep,
    status_filter: str | None = None,
) -> list[ProposalOut]:
    stmt = select(ControlProposal).order_by(ControlProposal.created_at.desc())
    if status_filter is not None:
        stmt = stmt.where(ControlProposal.status == status_filter)
    return [_out(row) for row in await session.scalars(stmt)]


@router.get("/proposals/{proposal_id}", response_model=ProposalOut)
async def get_proposal(
    proposal_id: uuid.UUID, session: SessionDep, caller: ControlCallerDep
) -> ProposalOut:
    proposal = await session.get(ControlProposal, proposal_id)
    if proposal is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "proposal not found")
    return _out(proposal)


@router.post("/proposals/{proposal_id}/execute", response_model=ProposalOut)
async def execute_proposal(
    proposal_id: uuid.UUID,
    body: ProposalExecute,
    session: SessionDep,
    kill_switch: KillSwitchDep,
    thread_resets: ThreadResetRequestsDep,
    eval_queue: EvalQueueDep,
    caller: ControlCallerDep,
) -> ProposalOut:
    """Run a pending proposal. **Platform key only.**

    This is the line the whole ADR is about. The control agent authenticates
    fine on this router -- it reads the fleet and writes proposals through the
    same dependency -- and is refused *here*, by caller kind, server-side. There
    is no configuration that grants it, and no tool it can reach that reaches
    this. A model's authority ends at the pending row.

    Execute-once is a compare-and-set on ``status``, the same shape as
    ``claim_approval_resolution``: the row moves out of ``pending`` in the same
    transaction that runs the action, so a double click cannot roll the same
    agent back twice.
    """

    if caller != ControlCaller.PLATFORM:
        raise HTTPException(
            status.HTTP_403_FORBIDDEN,
            "executing a proposal requires the platform key; the control agent "
            "may propose but never execute",
        )

    proposal = await session.get(ControlProposal, proposal_id)
    if proposal is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "proposal not found")
    if proposal.status != ProposalStatus.pending.value:
        raise HTTPException(
            status.HTTP_409_CONFLICT,
            f"proposal is already {proposal.status}",
        )
    if proposal.expires_at <= _now():
        # Marked, not just refused: an expired proposal should stop appearing as
        # something a human could still click.
        proposal.status = ProposalStatus.expired.value
        await session.commit()
        raise HTTPException(
            status.HTTP_409_CONFLICT,
            "proposal expired; ask the control agent to propose it again against "
            "the current fleet state",
        )

    target = await _executable_target(session, proposal)
    try:
        result = await proposals.execute(
            session,
            target,
            proposal.action,
            proposal.params,
            proposals.ExecutionContext(
                kill_switch=kill_switch,
                thread_resets=thread_resets,
                eval_queue=eval_queue,
                default_eval_suite=get_settings().eval_default_suite,
            ),
        )
    except proposals.ProposalError as exc:
        # The fleet moved under the proposal between rendering and clicking. The
        # row stays pending: the human may still want it once they know why.
        raise HTTPException(status.HTTP_409_CONFLICT, str(exc)) from exc

    proposal.status = ProposalStatus.executed.value
    proposal.executed_at = _now()
    proposal.executed_by = body.executed_by
    proposal.result = result
    await session.commit()
    await session.refresh(proposal)
    return _out(proposal)


@router.post("/proposals/{proposal_id}/reject", response_model=ProposalOut)
async def reject_proposal(
    proposal_id: uuid.UUID,
    body: ProposalExecute,
    session: SessionDep,
    caller: ControlCallerDep,
) -> ProposalOut:
    """Decline a proposal. Platform key only, for the same reason execute is:
    a model that could reject its own proposals could hide them."""

    if caller != ControlCaller.PLATFORM:
        raise HTTPException(
            status.HTTP_403_FORBIDDEN,
            "resolving a proposal requires the platform key",
        )
    proposal = await session.get(ControlProposal, proposal_id)
    if proposal is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "proposal not found")
    if proposal.status != ProposalStatus.pending.value:
        raise HTTPException(
            status.HTTP_409_CONFLICT, f"proposal is already {proposal.status}"
        )
    proposal.status = ProposalStatus.rejected.value
    proposal.executed_at = _now()
    proposal.executed_by = body.executed_by
    await session.commit()
    await session.refresh(proposal)
    return _out(proposal)


# -- Screens: the surface a human taps (ADR-0125) ------------------------------
#
# Reads are open to the same two callers as everything above -- the control
# agent renders screens into a channel, so it must be able to fetch them.
# Invokes are not: they require an operator identity the API resolves itself.


async def require_operator(
    body: ScreenActionIn,
    caller: ControlCallerDep,
) -> str:
    """Who may press a button.

    Two conditions, both server-side. The caller must hold the platform key --
    the control agent is refused here exactly as it is refused on execute, so
    the model cannot press its own buttons. And the actor the caller names must
    be in the operator set.

    The second condition is what makes the first one worth having. A
    platform-key caller is the dispatcher relaying a click; without the operator
    check it would relay anyone's click, and "the model cannot press buttons"
    would only mean "the model has to ask a stranger in the channel to press
    it".

    ``ExplicitUsers`` is the set, not a Slack usergroup: it needs no upstream
    lookup, so a button still resolves when Slack is down, and the same list
    authorizes a Discord click without a second policy (ADR-0020).
    """

    if caller != ControlCaller.PLATFORM:
        raise HTTPException(
            status.HTTP_403_FORBIDDEN,
            "pressing a control button requires an operator; the control agent "
            "renders screens but never presses them",
        )
    operators = get_settings().control_operator_ids()
    if not operators:
        raise HTTPException(
            status.HTTP_403_FORBIDDEN,
            "no control operators are configured, so no button can be pressed; "
            "set CONTROL_OPERATORS to the user ids allowed to operate the fleet",
        )
    verdict = await ExplicitUsers(operators).contains(body.actor, None)
    if not verdict.member:
        raise HTTPException(
            status.HTTP_403_FORBIDDEN,
            f"{body.actor} is not a Curie operator on this install",
        )
    return body.actor


def _screen_out(screen: Screen) -> ScreenOut:
    return ScreenOut(
        id=screen.id,
        title=screen.title,
        subtitle=screen.subtitle,
        parent=screen.parent,
        blocks=[
            ScreenBlockOut(
                kind=str(b.kind),
                text=b.text,
                fields=b.fields,
                columns=b.columns,
                rows=b.rows,
            )
            for b in screen.blocks
        ],
        buttons=[
            ScreenButtonOut(
                id=b.id,
                label=b.label,
                kind=b.kind,
                style=str(b.style),
                target=b.target,
                params=b.params,
                confirm=b.confirm,
            )
            for b in screen.buttons
        ],
    )


async def _render(
    screen_id: str, session: SessionDep, kill_switch: KillSwitch, params: dict[str, Any]
) -> Screen:
    builder = BUILDERS.get(screen_id)
    if builder is None:
        raise HTTPException(
            status.HTTP_404_NOT_FOUND,
            f"no screen {screen_id!r}; known screens are {sorted(BUILDERS)}",
        )
    built: Screen = await builder(session, kill_switch, params)
    return built


@router.get("/screens/{screen_id}", response_model=ScreenOut)
async def get_screen(
    screen_id: str,
    session: SessionDep,
    kill_switch: KillSwitchDep,
    caller: ControlCallerDep,
    agent_id: str | None = None,
    thread_key: str | None = None,
) -> ScreenOut:
    """Render one screen. A read, so the control agent may call it.

    Query parameters rather than a body because a screen is addressable: the
    same (id, params) pair renders the same page, which is what lets a button's
    ``navigate`` target be a plain reference the adapter can re-fetch.
    """

    params = {"agent_id": agent_id, "thread_key": thread_key}
    return _screen_out(await _render(screen_id, session, kill_switch, params))


@router.get("/coverage", response_model=CommandCoverageOut)
async def command_coverage(caller: ControlCallerDep) -> CommandCoverageOut:
    """Which CLI command each screen answers, and why the rest have none.

    Served rather than only tested so the agent can answer "can you do X?"
    honestly, from the same table the coverage gate checks, instead of guessing
    from its system prompt.
    """

    rows = []
    covered = exempt = 0
    for command, target in sorted(SCREEN_FOR_COMMAND.items()):
        if isinstance(target, Exempt):
            exempt += 1
            rows.append(CommandCoverageRow(command=command, exempt=str(target)))
        else:
            covered += 1
            rows.append(CommandCoverageRow(command=command, screen=target))
    return CommandCoverageOut(
        total=len(rows), covered=covered, exempt=exempt, rows=rows
    )


@router.post("/screens/actions", response_model=ScreenActionOut)
async def invoke_button(
    body: ScreenActionIn,
    session: SessionDep,
    kill_switch: KillSwitchDep,
    thread_resets: ThreadResetRequestsDep,
    eval_queue: EvalQueueDep,
    actor: Annotated[str, Depends(require_operator)],
) -> ScreenActionOut:
    """Press a button.

    **The caller names a screen and a button id, never an action.** The server
    re-renders that screen and looks the button up in what it just built, so the
    action and its parameters are the ones the platform authored a moment ago.
    A caller cannot compose ``{"action": "delete_agent"}`` and send it: there is
    no field to put it in, and a button that the current screen does not render
    does not exist.

    That is what makes the confirm text meaningful too. The operator approved a
    sentence the API wrote; the API then runs the parameters that sentence was
    rendered from, not parameters that arrived alongside it.

    A press runs immediately -- it is a human act, not a model's -- and still
    writes an executed ``ControlProposal`` row, so a change made from a phone
    and one made from the CLI land in the same audit trail rather than only in a
    chat log somebody can delete.
    """

    settings = get_settings()
    ctx = proposals.ExecutionContext(
        kill_switch=kill_switch,
        thread_resets=thread_resets,
        eval_queue=eval_queue,
        default_eval_suite=settings.eval_default_suite,
    )

    # Re-render the screen the operator was looking at and find their button in
    # it. A button that is gone (the agent was already killed by someone else,
    # the version is now live) is a stale tap, and refusing it is how two
    # operators racing on one message do not both act.
    context = dict(body.params)
    screen = await _render(body.screen, session, kill_switch, context)
    button = next((b for b in screen.buttons if b.id == body.button), None)
    if button is None:
        raise HTTPException(
            status.HTTP_409_CONFLICT,
            "that button is no longer on this screen -- someone may have acted "
            "already, or the agent has changed. Re-open the screen.",
        )
    if button.kind != "invoke":
        raise HTTPException(
            status.HTTP_422_UNPROCESSABLE_ENTITY,
            "that button only navigates; fetch the target screen instead",
        )

    params = dict(button.params)
    raw_agent = params.pop("agent_id", None)
    typed_target = params.pop("confirm_agent_name", None)

    # The proposals screen runs an EXISTING pending row, so it goes down the
    # same path a CLI execute does and inherits its expiry and execute-once
    # checks rather than re-implementing them.
    if button.target == PROPOSAL_BUTTON_TARGET:
        return await _run_pending_proposal(params, session, ctx, actor)

    try:
        agent_id = uuid.UUID(str(raw_agent))
    except (ValueError, TypeError):
        raise HTTPException(
            status.HTTP_422_UNPROCESSABLE_ENTITY, "this button names no agent"
        ) from None
    agent = await _load_target(session, agent_id)

    # A danger button carries the agent's name and the operator has to type it
    # back. Compared against the name just loaded, so a button rendered before a
    # rename cannot delete the renamed agent on a stale confirmation.
    if typed_target is not None and (body.typed_confirmation or "").strip() != agent.name:
        raise HTTPException(
            status.HTTP_422_UNPROCESSABLE_ENTITY,
            f"type the agent's name ({agent.name}) to confirm this",
        )

    is_operator_only = button.target in proposals.OPERATOR_ONLY_ACTIONS
    try:
        prepared = await proposals.prepare(
            session, agent, button.target, params, operator=True
        )
    except proposals.ProposalError as exc:
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_ENTITY, str(exc)) from exc

    proposal = ControlProposal(
        target_agent_id=agent.id,
        target_agent_name=agent.name,
        action=button.target,
        params=prepared.params,
        summary=prepared.summary,
        # No proposing agent: a button press has no model in its provenance, and
        # the NULL is what says so a year later.
        proposed_by_agent_id=None,
        requested_by=actor,
        status=ProposalStatus.pending.value,
        expires_at=_now() + PROPOSAL_TTL,
    )
    session.add(proposal)
    await session.commit()
    await session.refresh(proposal)

    try:
        result = await proposals.execute(session, agent, button.target, prepared.params, ctx)
    except proposals.ProposalError as exc:
        proposal.status = ProposalStatus.rejected.value
        proposal.executed_by = actor
        proposal.result = {"error": str(exc)}
        await session.commit()
        raise HTTPException(status.HTTP_409_CONFLICT, str(exc)) from exc

    proposal.status = ProposalStatus.executed.value
    proposal.executed_at = _now()
    proposal.executed_by = actor
    proposal.result = result
    await session.commit()

    # Deleting the agent invalidates the screen we came from, so the next screen
    # is the fleet rather than a detail page for something that is gone.
    next_id = FLEET if is_operator_only else body.screen
    next_params = {} if is_operator_only else context
    next_screen = await _render(next_id, session, kill_switch, next_params)

    return ScreenActionOut(
        ok=True,
        message=prepared.summary,
        proposal_id=proposal.id,
        result=result,
        screen=_screen_out(next_screen),
    )


async def _run_pending_proposal(
    params: dict[str, Any],
    session: SessionDep,
    ctx: proposals.ExecutionContext,
    actor: str,
) -> ScreenActionOut:
    try:
        proposal_id = uuid.UUID(str(params.get("proposal_id")))
    except (ValueError, TypeError):
        raise HTTPException(
            status.HTTP_422_UNPROCESSABLE_ENTITY, "this button names no proposal"
        ) from None
    proposal = await session.get(ControlProposal, proposal_id)
    if proposal is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "proposal not found")
    if proposal.status != ProposalStatus.pending.value:
        raise HTTPException(
            status.HTTP_409_CONFLICT, f"proposal is already {proposal.status}"
        )
    if proposal.expires_at <= _now():
        proposal.status = ProposalStatus.expired.value
        await session.commit()
        raise HTTPException(status.HTTP_409_CONFLICT, "proposal expired")

    agent = await _executable_target(session, proposal)
    try:
        result = await proposals.execute(
            session, agent, proposal.action, proposal.params, ctx
        )
    except proposals.ProposalError as exc:
        raise HTTPException(status.HTTP_409_CONFLICT, str(exc)) from exc

    proposal.status = ProposalStatus.executed.value
    proposal.executed_at = _now()
    proposal.executed_by = actor
    proposal.result = result
    await session.commit()
    await session.refresh(proposal)
    return ScreenActionOut(
        ok=True, message=proposal.summary, proposal_id=proposal.id, result=result
    )
