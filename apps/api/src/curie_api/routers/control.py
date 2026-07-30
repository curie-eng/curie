"""Budgets + kill switch control plane (L1, API half).

Kill/resume produce the Valkey signal the worker consumes (SET flag + PUBLISH
event); budgets persist per-agent config the worker passes through at sandbox
boot; cost composes the OB1 metrics module filtered to the agent.
"""

import uuid

from fastapi import APIRouter, Depends, HTTPException, status

from .. import crud
from .. import metrics as metrics_service
from ..auth import require_api_key
from ..config import get_settings
from ..deps import KillSwitchDep, LangfuseDep, SessionDep, ThreadResetRequestsDep
from ..models import Agent
from ..schemas import (
    BehaviorPacksConfig,
    BudgetConfig,
    CostReport,
    KillState,
    ThreadResetState,
    enforce_behavior_packs_size,
)

router = APIRouter(
    prefix="/agents/{agent_id}",
    tags=["control"],
    dependencies=[Depends(require_api_key)],
)


async def _load_agent(session: SessionDep, agent_id: uuid.UUID) -> Agent:
    agent = await crud.get_agent(session, agent_id)
    if agent is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "agent not found")
    return agent


@router.post("/kill", response_model=KillState)
async def kill_agent(
    agent_id: uuid.UUID, session: SessionDep, kill_switch: KillSwitchDep
) -> KillState:
    await _load_agent(session, agent_id)
    await kill_switch.kill(agent_id)
    return KillState(killed=True)


@router.post("/resume", response_model=KillState)
async def resume_agent(
    agent_id: uuid.UUID, session: SessionDep, kill_switch: KillSwitchDep
) -> KillState:
    await _load_agent(session, agent_id)
    await kill_switch.resume(agent_id)
    return KillState(killed=False)


@router.get("/kill", response_model=KillState)
async def get_kill_state(
    agent_id: uuid.UUID, session: SessionDep, kill_switch: KillSwitchDep
) -> KillState:
    await _load_agent(session, agent_id)
    return KillState(killed=await kill_switch.is_killed(agent_id))


@router.post("/threads/{thread_key}/reset", response_model=ThreadResetState)
async def reset_thread(
    agent_id: uuid.UUID,
    thread_key: str,
    session: SessionDep,
    thread_reset_requests: ThreadResetRequestsDep,
) -> ThreadResetState:
    """Force the thread's sandbox to be released (#713): the worker's next
    maintenance tick deletes its claim and route, so the NEXT message on this
    thread cold-creates a fresh sandbox instead of adopting one that may be
    running stale env (a rotated credential, an unpicked-up redeploy, a wedge
    from a partial local-stack upgrade). A live turn on the thread, if any, is
    interrupted first -- see ``Kernel.release_thread``. Does not delete
    conversation history: a fresh sandbox still rehydrates from the durable
    transcript on its next claim, same as any other cold-create.

    ``agent_id`` scopes the action to a specific agent's registration (matching
    every other verb on this router) but is not itself required to resolve the
    thread -- the release is purely thread-keyed, mirroring
    ``SandboxSubstrate.release``.
    """
    await _load_agent(session, agent_id)
    await thread_reset_requests.request(thread_key)
    return ThreadResetState(requested=True)


@router.get("/threads/{thread_key}/reset", response_model=ThreadResetState)
async def get_thread_reset_state(
    agent_id: uuid.UUID,
    thread_key: str,
    session: SessionDep,
    thread_reset_requests: ThreadResetRequestsDep,
) -> ThreadResetState:
    """Poll whether a forced reset (the POST above) is still outstanding for
    this thread (#735). ``requested`` is True from the moment the POST enqueues
    the request until the worker's maintenance tick drains it AND
    ``release_thread`` actually completes, then False.

    Why this exists: the POST returns as soon as the request is *queued*, but the
    release only happens on the worker's maintenance tick (up to
    ``reclaim_interval_s`` -- 30s by default -- later). Without a way to observe
    completion, the natural operator workflow "reset the thread, then send a
    message to confirm" adopts the still-live pre-reset sandbox and reads a
    stale answer, indistinguishable from "the reset did not work" (#735). A
    caller that must not adopt the pre-reset sandbox polls this until it reads
    False before sending the next message; the CLI ``reset-thread`` verb does
    exactly that on the operator's behalf. Mirrors ``GET .../kill``.

    The signal flips only after the release LANDS, not at claim time (#812, was
    #806 incomplete). The worker SPOPs the request to claim it, then moves it
    into an in-progress set that ``is_pending`` also reads, clearing it only once
    ``release_thread`` succeeds. So this stays True across the whole release, and
    a release that raises or times out leaves it True -- the CLI reports the
    reset as unconfirmed rather than a false "released". Reading only the request
    set would flip to done the instant the request was SPOPped, before -- and
    independent of whether -- the sandbox was actually released.
    """
    await _load_agent(session, agent_id)
    return ThreadResetState(requested=await thread_reset_requests.is_pending(thread_key))


@router.get("/budget", response_model=BudgetConfig)
async def get_budget(agent_id: uuid.UUID, session: SessionDep) -> BudgetConfig:
    agent = await _load_agent(session, agent_id)
    return BudgetConfig.model_validate(agent)


@router.put("/budget", response_model=BudgetConfig)
async def put_budget(
    agent_id: uuid.UUID, config: BudgetConfig, session: SessionDep
) -> BudgetConfig:
    agent = await _load_agent(session, agent_id)
    updated = await crud.update_budget(
        session,
        agent,
        config.max_usd_per_day,
        config.max_output_tokens_per_run,
    )
    return BudgetConfig.model_validate(updated)


@router.get("/behavior-packs", response_model=BehaviorPacksConfig)
async def get_behavior_packs(agent_id: uuid.UUID, session: SessionDep) -> BehaviorPacksConfig:
    agent = await _load_agent(session, agent_id)
    # NULL (no packs configured) reads as the all-off default.
    if agent.behavior_packs is None:
        return BehaviorPacksConfig()
    return BehaviorPacksConfig.model_validate(agent.behavior_packs)


@router.put("/behavior-packs", response_model=BehaviorPacksConfig)
async def put_behavior_packs(
    agent_id: uuid.UUID, config: BehaviorPacksConfig, session: SessionDep
) -> BehaviorPacksConfig:
    agent = await _load_agent(session, agent_id)
    enforce_behavior_packs_size(config)
    updated = await crud.update_behavior_packs(session, agent, config.model_dump())
    return BehaviorPacksConfig.model_validate(updated.behavior_packs)


@router.get("/cost", response_model=CostReport)
async def get_cost(
    agent_id: uuid.UUID,
    session: SessionDep,
    lf: LangfuseDep,
    start: str | None = None,
    end: str | None = None,
) -> CostReport:
    agent = await _load_agent(session, agent_id)
    window = get_settings().metrics_default_window_hours
    try:
        start_iso, end_iso = metrics_service.resolve_window(start, end, window)
    except ValueError as exc:
        raise HTTPException(
            status.HTTP_422_UNPROCESSABLE_ENTITY,
            f"start/end must be ISO 8601 timestamps: {exc}",
        ) from exc
    # Cost is filtered by the agent's trace-name token: the runner names traces
    # curie-run:agent-<id>-thread-<ts>, so we match traceName contains
    # `agent-<id>`. A fresh agent with no matching traces reads zero.
    agent_filter = metrics_service.agent_trace_filter(agent.id)
    series = await metrics_service.series(
        lf, "cost_usd", start_iso, end_iso, "day", None, agent_filter
    )
    # A total of 0 over a window with token usage is a missing Langfuse price row,
    # not a free agent -- flag it so the Cost view renders "unknown" not $0.00 (#547).
    known = await metrics_service.cost_known(lf, start_iso, end_iso, None, agent_filter)
    return CostReport(
        start=start_iso,
        end=end_iso,
        total_usd=sum(point.value for point in series.points),
        cost_known=known,
        points=series.points,
    )
