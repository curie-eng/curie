"""Agent-created schedules (ADR 0248): an agent's own recurring and one-shot runs.

The runner reads them with its boot ``state`` credential and changes them with
the per-turn credential the worker mints (``schedules: "write"``); the operator
reads and repairs them with the platform key. The bundle-facing app token is
refused on every route, and the raw ``agent_schedules`` state namespace is
refused to every sandbox credential (``routers.state``), so this router is the
only door.

Who may change a schedule: an agent turn with a person behind it while its
turn is open and the agent's toggle is on. A scheduled run (``scheduled``
claim true, a ``<no person>`` or ``cron:`` sender) may list and cancel but
never create or update, and neither may a credential whose ``scheduled``
claim is not an exact false. The operator always may cancel, pause and revert (repair
while off), and may create and update while the toggle is on.

Every change runs under the per (agent, name) advisory lock the worker's
admission takes, then the schedule row lock, and appends to the change log.
``revision`` moves only when the prompt, cron, run_at or timezone change, or
on revert; pause, resume and cancel are logged without moving it.
"""

import hashlib
import json
import uuid
from datetime import UTC, datetime, timedelta
from typing import Annotated, Any

from aci_protocol.turn import DEFAULT_IDENTITY, SLACK_KIND, route_identity
from fastapi import APIRouter, Depends, HTTPException, Query, Request, status
from fastapi.responses import JSONResponse
from sqlalchemy.exc import IntegrityError

from .. import agent_schedule_time as times
from ..crud import agent_schedules as store
from ..crud.agent_schedules import OPERATOR, Stamp
from ..deps import SessionDep
from ..models import Agent, AgentChannel, AgentSchedule
from ..schemas.agent_schedules import (
    AgentScheduleCancel,
    AgentScheduleCreate,
    AgentScheduleList,
    AgentScheduleRevert,
    AgentScheduleRevisionOut,
    AgentScheduleRevisions,
    AgentScheduleUpdate,
)
from ..schemas.schedules import AgentScheduleOut
from .schedules import apply_control
from .state import StateCaller, StatePrincipal, require_state_access, require_turn_open

router = APIRouter(prefix="/agents/{agent_id}/schedules", tags=["agent-schedules"])
Principal = Annotated[StatePrincipal, Depends(require_state_access)]

MAX_PROMPT_CHARS = 4000
RUN_AT_HORIZON = timedelta(days=366)
# The runner's ``memory_facts.NO_PERSON``: the sender of a turn no person started.
NO_PERSON = "<no person>"

OFF = "agent schedules are off for this agent"
SCHEDULED = "scheduled runs cannot create or change schedules"
NO_WRITE = "this credential cannot change schedules"
EXACTLY_ONE = "exactly one of cron or run_at is required"


def _forbidden(detail: str) -> HTTPException:
    return HTTPException(status.HTTP_403_FORBIDDEN, detail)


def _unprocessable(detail: str) -> HTTPException:
    return HTTPException(422, detail)


def _not_found() -> HTTPException:
    return HTTPException(status.HTTP_404_NOT_FOUND, "schedule not found")


def _platform(principal: StatePrincipal) -> bool:
    return principal.caller is StateCaller.PLATFORM


def _refuse_app(principal: StatePrincipal) -> None:
    if principal.caller is StateCaller.APP:
        raise _forbidden(NO_WRITE)


async def _agent(session: SessionDep, agent_id: uuid.UUID, *, share: bool = False) -> Agent:
    agent = (
        await store.agent_for_share(session, agent_id)
        if share
        else await session.get(Agent, agent_id)
    )
    if agent is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "agent not found")
    return agent


async def _writer(
    request: Request,
    agent: Agent,
    principal: StatePrincipal,
    *,
    repair: bool,
    configures: bool,
) -> Stamp:
    """Authorize a change. ``repair`` (cancel, pause, revert) lets the operator
    act while the toggle is off; ``configures`` (create, update, revert) needs
    an exact false ``scheduled`` claim and is refused to a scheduled run."""

    _refuse_app(principal)
    if _platform(principal):
        if not agent.agent_schedules and not repair:
            raise _forbidden(OFF)
        return OPERATOR
    if not agent.agent_schedules:
        raise _forbidden(OFF)
    if principal.schedules != "write":
        raise _forbidden(NO_WRITE)
    sender = principal.sender
    if not sender:
        raise _forbidden("this credential names no sender")
    if configures:
        # ADR 0248 section 11: only an exact false admits a configuring change.
        if principal.scheduled is None:
            raise _forbidden(NO_WRITE)
        if principal.scheduled or sender == NO_PERSON or sender.startswith("cron:"):
            raise _forbidden(SCHEDULED)
    if not principal.turn or not principal.conversation:
        raise _forbidden("this credential names no turn or conversation")
    await require_turn_open(principal, agent.id, request, f"schedules/{agent.id}")
    return Stamp(sender, "agent", principal.conversation, principal.turn)


def _check_revision(schedule: AgentSchedule, expected: int | None) -> None:
    if expected is not None and schedule.revision != expected:
        raise HTTPException(
            status.HTTP_409_CONFLICT,
            f"schedule is at revision {schedule.revision}, not {expected}",
        )


def _check_live(schedule: AgentSchedule) -> None:
    if schedule.state != store.ACTIVE:
        raise HTTPException(status.HTTP_409_CONFLICT, f"schedule is {schedule.state}")


def _zone(name: str) -> str:
    if not times.valid_zone(name):
        raise _unprocessable(f"timezone {name!r} is not an IANA zone")
    return name


def _prompt(prompt: str) -> str:
    if not prompt.strip() or len(prompt) > MAX_PROMPT_CHARS:
        raise _unprocessable(f"prompt must be 1 to {MAX_PROMPT_CHARS} characters")
    return prompt


def _cron(agent: Agent, cron: str, zone: str, now: datetime) -> str:
    """A five-field cron that fires at least the agent's minimum apart."""

    normalized = " ".join(cron.split())
    try:
        shortest = times.min_interval_minutes(normalized, zone, now)
    except ValueError as exc:
        raise _unprocessable(str(exc)) from None
    if times.next_run(normalized, None, zone, now) is None:
        raise _unprocessable("cron never fires within the next four years")
    floor = agent.schedule_min_interval_minutes
    if shortest is not None and shortest < floor:
        raise _unprocessable(
            f"scheduled runs must be at least {floor} minutes apart; "
            f"this cron fires {shortest} minutes apart"
        )
    return normalized


def _run_at(text: str, zone: str) -> datetime:
    try:
        return times.parse_run_at(text, zone)
    except ValueError as exc:
        raise _unprocessable(str(exc)) from None


def _check_run_at(run_at: datetime, now: datetime) -> None:
    if run_at <= now:
        raise _unprocessable("run_at must be in the future")
    if run_at > now + RUN_AT_HORIZON:
        raise _unprocessable("run_at must be within 366 days")


def _pick_binding(rows: list[AgentChannel]) -> AgentChannel | None:
    """ADR-0168 decision 3, as the cron loop reads a target: one row, or, when
    the address holds several Slack identities, the default one."""

    candidates = rows
    if len(candidates) > 1 and all(row.kind == SLACK_KIND for row in candidates):
        candidates = [
            row for row in candidates if route_identity(row.kind, row.adapter) == DEFAULT_IDENTITY
        ]
    return candidates[0] if len(candidates) == 1 else None


async def _destination(
    session: SessionDep, agent: Agent, principal: StatePrincipal, data: AgentScheduleCreate
) -> AgentChannel:
    if _platform(principal):
        if data.destination is None:
            raise _unprocessable("an operator schedule names its destination")
        kind, address = data.destination.kind, data.destination.address
        adapter = data.destination.adapter
    else:
        binding = principal.binding or ""
        kind, _, address = binding.partition(":")
        adapter = None
        if not kind or not address:
            raise _unprocessable("this turn has no channel to schedule on")
    rows = list(await store.destinations(session, agent.id, kind, address))
    if adapter is not None:
        rows = [row for row in rows if row.adapter == adapter]
    picked = _pick_binding(rows)
    if picked is None:
        raise _unprocessable(f"{kind}:{address} is not one binding of this agent")
    return picked


def _idempotency_key(
    principal: StatePrincipal,
    data: AgentScheduleCreate,
    cron: str | None,
    run_at: datetime | None,
    zone: str,
    prompt: str,
) -> str:
    """The sandbox key: the turn's delivery event (the claim with only its final
    ``#suffix`` stripped, stable across attempts), the normalized schedule,
    zone and prompt. The operator may name one; else every create is new."""

    if _platform(principal):
        return data.idempotency_key or f"operator:{uuid.uuid4().hex}"
    event = (principal.turn or "").rsplit("#", 1)[0]
    when = cron if cron is not None else (run_at.isoformat() if run_at is not None else "")
    material = json.dumps([event, when, zone, prompt], ensure_ascii=False)
    return hashlib.sha256(material.encode()).hexdigest()


async def _views(
    session: SessionDep, principal: StatePrincipal, rows: list[AgentSchedule]
) -> list[AgentScheduleOut]:
    return await store.views(session, rows, platform=_platform(principal))


async def _view(
    session: SessionDep, principal: StatePrincipal, schedule: AgentSchedule
) -> AgentScheduleOut:
    (out,) = await _views(session, principal, [schedule])
    return out


@router.get("", response_model=AgentScheduleList, response_model_exclude_unset=True)
async def list_agent_schedules(
    agent_id: uuid.UUID,
    session: SessionDep,
    principal: Principal,
    include_all: Annotated[bool, Query(alias="all")] = False,
) -> AgentScheduleList:
    """The agent's schedule switch, limits and schedules (active and paused;
    ``all`` adds cancelled and done). A sandbox credential sees none while
    the toggle is off."""

    _refuse_app(principal)
    agent = await _agent(session, agent_id)
    rows: list[AgentSchedule] = []
    if agent.agent_schedules or _platform(principal):
        rows = list(await store.listed(session, agent.id, include_all=include_all))
    return AgentScheduleList(
        enabled=agent.agent_schedules,
        timezone=agent.schedule_timezone,
        max_active=agent.schedule_max_active,
        min_interval_minutes=agent.schedule_min_interval_minutes,
        schedules=await _views(session, principal, rows),
    )


async def _visible(
    session: SessionDep, agent_id: uuid.UUID, schedule_id: uuid.UUID, principal: StatePrincipal
) -> AgentSchedule:
    _refuse_app(principal)
    agent = await _agent(session, agent_id)
    if not agent.agent_schedules and not _platform(principal):
        raise _not_found()
    schedule = await store.get(session, agent.id, schedule_id)
    if schedule is None:
        raise _not_found()
    return schedule


@router.get("/{schedule_id}", response_model=AgentScheduleOut, response_model_exclude_unset=True)
async def get_agent_schedule(
    agent_id: uuid.UUID, schedule_id: uuid.UUID, session: SessionDep, principal: Principal
) -> AgentScheduleOut:
    schedule = await _visible(session, agent_id, schedule_id, principal)
    return await _view(session, principal, schedule)


@router.get("/{schedule_id}/revisions", response_model=AgentScheduleRevisions)
async def list_agent_schedule_revisions(
    agent_id: uuid.UUID, schedule_id: uuid.UUID, session: SessionDep, principal: Principal
) -> AgentScheduleRevisions:
    """The schedule's change log, oldest first. Platform key only."""

    if not _platform(principal):
        raise _forbidden("the change log is for the operator")
    schedule = await _visible(session, agent_id, schedule_id, principal)
    return AgentScheduleRevisions(
        revisions=[
            AgentScheduleRevisionOut.model_validate(entry, from_attributes=True)
            for entry in await store.revisions(session, schedule.id)
        ]
    )


@router.post(
    "",
    response_model=AgentScheduleOut,
    response_model_exclude_unset=True,
    status_code=status.HTTP_201_CREATED,
    responses={200: {"model": AgentScheduleOut, "description": "Idempotent replay"}},
)
async def create_agent_schedule(
    agent_id: uuid.UUID,
    data: AgentScheduleCreate,
    request: Request,
    session: SessionDep,
    principal: Principal,
) -> Any:
    _refuse_app(principal)
    await store.lock_creates(session, agent_id)
    agent = await _agent(session, agent_id, share=True)
    stamp = await _writer(request, agent, principal, repair=False, configures=True)
    if not _platform(principal) and (
        data.destination is not None or data.idempotency_key is not None
    ):
        raise _forbidden("destination and idempotency_key are set by the operator only")
    now = datetime.now(UTC)
    prompt = _prompt(data.prompt)
    zone = _zone(data.timezone if data.timezone is not None else agent.schedule_timezone)
    if (data.cron is None) == (data.run_at is None):
        raise _unprocessable(EXACTLY_ONE)
    run_at = None if data.run_at is None else _run_at(data.run_at, zone)
    cron = None if data.cron is None else " ".join(data.cron.split())
    key = _idempotency_key(principal, data, cron, run_at, zone, prompt)
    existing = await store.by_key(session, agent.id, key)
    if existing is not None:
        # A retry of the same delivery: the row it made, before any limit.
        replay = await _view(session, principal, existing)
        await session.commit()
        return _replayed(replay)
    if cron is not None:
        cron = _cron(agent, cron, zone, now)
    if run_at is not None:
        _check_run_at(run_at, now)
    binding = await _destination(session, agent, principal, data)
    limit = agent.schedule_max_active
    if await store.active_count(session, agent.id) >= limit:
        raise _unprocessable(f"an agent holds at most {limit} active schedules")
    schedule = AgentSchedule(
        id=uuid.uuid4(),
        agent_id=agent.id,
        revision=1,
        state=store.ACTIVE,
        prompt=prompt,
        cron=cron,
        run_at=run_at,
        timezone=zone,
        destination_kind=binding.kind,
        destination_address=binding.address,
        destination_adapter=binding.adapter,
        created_by=stamp.author,
        created_source=stamp.source,
        created_conversation=stamp.conversation,
        created_turn=stamp.turn,
        idempotency_key=key,
        effective_from=now,
        created_at=now,
        updated_at=now,
        updated_by=stamp.author,
    )
    session.add(schedule)
    try:
        await session.flush()
    except IntegrityError:
        raise HTTPException(status.HTTP_409_CONFLICT, "schedule was created concurrently") from None
    await store.log(session, schedule, action="create", state="active", stamp=stamp)
    out = await _view(session, principal, schedule)
    await session.commit()
    return out


def _replayed(out: AgentScheduleOut) -> JSONResponse:
    return JSONResponse(out.model_dump(mode="json", exclude_unset=True), status_code=200)


async def _locked(
    session: SessionDep, agent_id: uuid.UUID, schedule_id: uuid.UUID, principal: StatePrincipal
) -> tuple[Agent, AgentSchedule]:
    """The advisory lock, the schedule row lock, then the agent share lock."""

    _refuse_app(principal)
    schedule = await store.lock_schedule(session, agent_id, schedule_id)
    agent = await _agent(session, agent_id, share=True)
    if schedule is None:
        raise _not_found()
    return agent, schedule


async def _set_paused(
    session: SessionDep, schedule: AgentSchedule, paused: bool, stamp: Stamp
) -> None:
    if await store.is_paused(session, schedule) == paused:
        return
    await apply_control(session, schedule.agent_id, store.schedule_name(schedule.id), pause=paused)
    await store.log(
        session,
        schedule,
        action="pause" if paused else "resume",
        state="paused" if paused else "active",
        stamp=stamp,
    )


async def _configure(
    session: SessionDep,
    schedule: AgentSchedule,
    stamp: Stamp,
    *,
    prompt: str,
    cron: str | None,
    run_at: datetime | None,
    zone: str,
    action: str,
) -> bool:
    """Move the schedule to a new configuration revision when anything changed.
    A schedule that has fired cannot change its timing when either the current
    or the new configuration is a one-shot: a fired one-shot keeps its time,
    and a fired recurring schedule cannot become a one-shot, since its earlier
    run would read as the one-shot's completion."""

    timing = (cron, run_at, zone) != (schedule.cron, schedule.run_at, schedule.timezone)
    if not timing and prompt == schedule.prompt:
        return False
    one_shot = schedule.run_at is not None or run_at is not None
    if timing and one_shot and await store.has_fired(session, schedule):
        detail = (
            "this one-shot schedule has fired; its time cannot change"
            if schedule.run_at is not None
            else "this schedule has fired; cancel it and create a one-shot instead"
        )
        raise HTTPException(status.HTTP_409_CONFLICT, detail)
    now = datetime.now(UTC)
    schedule.prompt = prompt
    schedule.cron = cron
    schedule.run_at = run_at
    schedule.timezone = zone
    schedule.revision += 1
    if timing:
        schedule.effective_from = now
    schedule.updated_at = now
    schedule.updated_by = stamp.author
    await session.flush()
    paused = await store.is_paused(session, schedule)
    await store.log(
        session, schedule, action=action, state=store.read_state(schedule, paused), stamp=stamp
    )
    return True


@router.patch("/{schedule_id}", response_model=AgentScheduleOut, response_model_exclude_unset=True)
async def update_agent_schedule(
    agent_id: uuid.UUID,
    schedule_id: uuid.UUID,
    data: AgentScheduleUpdate,
    request: Request,
    session: SessionDep,
    principal: Principal,
) -> AgentScheduleOut:
    agent, schedule = await _locked(session, agent_id, schedule_id, principal)
    configures = any(
        value is not None for value in (data.prompt, data.cron, data.run_at, data.timezone)
    )
    if not configures and data.paused is None:
        raise _unprocessable("an update changes prompt, cron, run_at, timezone or paused")
    stamp = await _writer(
        request, agent, principal, repair=not configures and _platform(principal), configures=True
    )
    _check_revision(schedule, data.expected_revision)
    _check_live(schedule)
    if configures:
        now = datetime.now(UTC)
        if data.cron is not None and data.run_at is not None:
            raise _unprocessable(EXACTLY_ONE)
        zone = _zone(data.timezone) if data.timezone is not None else schedule.timezone
        prompt = _prompt(data.prompt) if data.prompt is not None else schedule.prompt
        cron, run_at = schedule.cron, schedule.run_at
        if data.cron is not None:
            cron, run_at = data.cron, None
        elif data.run_at is not None:
            cron, run_at = None, _run_at(data.run_at, zone)
        if cron is not None and (data.cron is not None or zone != schedule.timezone):
            cron = _cron(agent, cron, zone, now)
        if run_at is not None and run_at != schedule.run_at:
            _check_run_at(run_at, now)
        await _configure(
            session,
            schedule,
            stamp,
            prompt=prompt,
            cron=cron,
            run_at=run_at,
            zone=zone,
            action="update",
        )
    if data.paused is not None:
        await _set_paused(session, schedule, data.paused, stamp)
    out = await _view(session, principal, schedule)
    await session.commit()
    return out


@router.post(
    "/{schedule_id}/cancel", response_model=AgentScheduleOut, response_model_exclude_unset=True
)
async def cancel_agent_schedule(
    agent_id: uuid.UUID,
    schedule_id: uuid.UUID,
    data: AgentScheduleCancel,
    request: Request,
    session: SessionDep,
    principal: Principal,
) -> AgentScheduleOut:
    """Tombstone the schedule and settle its deferred runs. A scheduled run
    may cancel. An active, paused or done schedule cancels the same way;
    cancelling a done one-shot revokes its run's pending approval
    continuation, which the worker resumes for a done schedule but never for
    a cancelled one. Cancelling a cancelled schedule is a no-op."""

    agent, schedule = await _locked(session, agent_id, schedule_id, principal)
    stamp = await _writer(request, agent, principal, repair=True, configures=False)
    _check_revision(schedule, data.expected_revision)
    if schedule.state != store.CANCELLED:
        now = datetime.now(UTC)
        schedule.state = store.CANCELLED
        schedule.cancelled_at = now
        schedule.cancelled_by = stamp.author
        schedule.updated_at = now
        schedule.updated_by = stamp.author
        await session.flush()
        await store.settle_deferred(session, schedule)
        await store.log(session, schedule, action="cancel", state="cancelled", stamp=stamp)
    out = await _view(session, principal, schedule)
    await session.commit()
    return out


@router.post(
    "/{schedule_id}/revert", response_model=AgentScheduleOut, response_model_exclude_unset=True
)
async def revert_agent_schedule(
    agent_id: uuid.UUID,
    schedule_id: uuid.UUID,
    data: AgentScheduleRevert,
    request: Request,
    session: SessionDep,
    principal: Principal,
) -> AgentScheduleOut:
    """Restore an earlier revision's configuration as a new revision. The
    operator, or a person through the agent; never a scheduled run."""

    agent, schedule = await _locked(session, agent_id, schedule_id, principal)
    stamp = await _writer(request, agent, principal, repair=True, configures=True)
    _check_revision(schedule, data.expected_revision)
    _check_live(schedule)
    number = schedule.revision - 1 if data.to_revision is None else data.to_revision
    target = (
        None
        if number < 1 or number >= schedule.revision
        else await store.revision_config(session, schedule.id, number)
    )
    if target is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, f"schedule has no earlier revision {number}")
    # A restored cron meets today's minimum interval, as a new one would.
    cron = (
        None
        if target.cron is None
        else _cron(agent, target.cron, target.timezone, datetime.now(UTC))
    )
    changed = await _configure(
        session,
        schedule,
        stamp,
        prompt=target.prompt,
        cron=cron,
        run_at=target.run_at,
        zone=target.timezone,
        action="revert",
    )
    if not changed:
        raise HTTPException(
            status.HTTP_409_CONFLICT, f"revision {number} is the current configuration"
        )
    out = await _view(session, principal, schedule)
    await session.commit()
    return out
