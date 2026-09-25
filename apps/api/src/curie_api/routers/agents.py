"""Agents and their versions."""

import functools
import tempfile
import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path
from typing import NoReturn

from aci_protocol.turn import route_identity
from fastapi import APIRouter, Depends, HTTPException, status
from plugin_format import connector_lock
from plugin_format.connector_render import AmbiguousObjectName
from sqlalchemy.exc import DBAPIError, IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from starlette.concurrency import run_in_threadpool

from .. import bundles, crud, deploy
from ..auth import require_api_key
from ..config import get_settings
from ..deps import SessionDep, StoreDep
from ..models import Agent, AgentChannel
from ..publication_policy import PublicationPolicyConflict
from ..runner_resources import quota_refusal
from ..schemas import (
    AgentCreate,
    AgentOut,
    AgentUpdate,
    BundleFile,
    BundleFiles,
    ChannelBindingPatch,
    ChannelBindingWrite,
    ConnectorManifests,
    VersionCreate,
    VersionOut,
    enforce_behavior_packs_size,
)

router = APIRouter(prefix="/agents", tags=["agents"], dependencies=[Depends(require_api_key)])

# Postgres SQLSTATE for a unique_violation. asyncpg exposes it (and the
# violated constraint's name) as plain attributes on the wrapped driver
# exception -- there is no psycopg-style `.diag` namespace.
_UNIQUE_VIOLATION = "23505"
# Postgres SQLSTATE for a check_violation.
_CHECK_VIOLATION = "23514"

# The real unique constraints an agent write can violate -- on `agents` and on
# its `agent_channels` binding (from the alembic migrations) -- mapped to the
# human message for each. Any other unique violation falls back to a generic
# message.
_UNIQUE_CONSTRAINT_MESSAGES = {
    "agents_name_key": "an agent with that name already exists",
    # ix_agents_repo_full_name stopped being unique in 0018 (ADR-0091): one
    # repository builds many agents now. The mapping is kept because a
    # pre-0018 database still has the unique index, and an operator hitting it
    # deserves the actionable message rather than the generic fallback. It
    # names the fix, since the constraint is no longer intended behaviour.
    "ix_agents_repo_full_name": (
        "an agent for that repository already exists. One repository may build "
        "several agents (ADR-0091) -- run `alembic upgrade head` to apply "
        "migration 0018, which drops this constraint"
    ),
    # #38: one agent per channel ROUTE, carried onto `agent_channels` by
    # migration 0021 and widened from the address alone to the `(kind, address)`
    # pair by 0023, once the worker's resolver started routing on the pair too
    # (ADR-0096 phase 2). Without this the create succeeded and the second agent
    # was silently shadowed by the resolver at runtime. Stated without the word
    # "Slack" since ADR-0096: the invariant, and the shadowing it prevents,
    # belong to every channel kind. The constraint is on the PAIR and fires
    # whatever identity the write names, so the message names the pair. The
    # contract migration for ADR-0168 decision 3 (#3100) widens it to the
    # `(kind, adapter, address)` route and adds that constraint's entry here.
    "agent_channels_kind_address_key": (
        "another agent is already bound to that channel kind and address; one "
        "agent per route (move or delete the other agent, or pick another "
        "address)"
    ),
    # No entry for `agent_channels_agent_id_key`: migration 0030 drops that
    # constraint (ADR-0118), so its message can never fire again, and it said
    # the opposite of what this API now does. A dead entry is worse than none --
    # it reads as a protection. The pair constraint above is the ONLY binding
    # conflict left.
}


# The CHECK constraints a caller's binding write can reach once the write
# schema has passed it, mapped to a 422 naming why. Keyed by constraint name so
# an entry goes quiet by itself once a migration drops or renames its
# constraint. Any other check violation stays a server fault.
_CHECK_CONSTRAINT_MESSAGES = {
    # 0024's both-or-neither route check. The write schema admits a Slack
    # binding naming a declared identity with no endpoint (ADR-0168 decision
    # 1), and this check still refuses that row until
    # [#3146](https://github.com/curie-eng/curie/issues/3146) widens it. The
    # schema already refuses every other shape this check covers.
    "agent_channels_route_pair_ck": (
        "a Slack binding naming an identity other than 'default' cannot be "
        "stored until the database admits it "
        "(https://github.com/curie-eng/curie/issues/3146). The identity is "
        "declared; bind the channel under 'default' instead"
    ),
}


def _driver_diag(exc: IntegrityError, attr: str) -> str | None:
    """Read an asyncpg diagnostic field, walking the `__cause__` chain.

    asyncpg surfaces `sqlstate` on SQLAlchemy's DBAPI wrapper (`exc.orig`) but
    exposes `constraint_name` only on the underlying `asyncpg` error one link
    down the `__cause__` chain. Walk both so either shape resolves; guard
    against a cyclic chain.
    """
    obj = getattr(exc, "orig", None)
    seen: set[int] = set()
    while obj is not None and id(obj) not in seen:
        seen.add(id(obj))
        value = getattr(obj, attr, None)
        if value is not None:
            return str(value)
        obj = getattr(obj, "__cause__", None)
    return None


def classify_integrity_error(exc: IntegrityError) -> tuple[int, str] | None:
    """Map a caller-caused constraint violation to a `(status, message)` pair.

    A genuine unique_violation (SQLSTATE 23505) is a caller conflict (409). A
    check_violation (23514) on a constraint in `_CHECK_CONSTRAINT_MESSAGES` is
    a request the database cannot store (422). A NOT NULL or FK violation, or
    any other check, is a server fault and must surface as a 500, so this
    returns `None` for those (the caller re-raises). The human message is
    chosen by the violated constraint's name from asyncpg's structured fields,
    not by substring-matching the stringified driver error.
    """
    sqlstate = _driver_diag(exc, "sqlstate")
    constraint_name = _driver_diag(exc, "constraint_name")
    if sqlstate == _CHECK_VIOLATION:
        check_message = _CHECK_CONSTRAINT_MESSAGES.get(constraint_name or "")
        if check_message is None:
            return None
        return status.HTTP_422_UNPROCESSABLE_ENTITY, check_message
    if sqlstate != _UNIQUE_VIOLATION:
        return None
    message = "agent violates a uniqueness constraint"
    if constraint_name is not None:
        message = _UNIQUE_CONSTRAINT_MESSAGES.get(constraint_name, message)
    return status.HTTP_409_CONFLICT, message


@router.post("", response_model=AgentOut, status_code=status.HTTP_201_CREATED)
async def create_agent(data: AgentCreate, session: SessionDep) -> AgentOut:
    # Reject oversized behavior packs (#936) before we touch the DB.
    if data.behavior_packs is not None:
        enforce_behavior_packs_size(data.behavior_packs)
    # name and repo_full_name are unique. A collision is a caller conflict (409),
    # not a server fault: catch the DB IntegrityError and map it, rather than
    # letting it bubble as an opaque 500. A non-unique violation (NOT NULL, FK)
    # is a genuine server fault -- re-raise it so it surfaces as a 500.
    try:
        agent = await crud.create_agent(session, data)
    except IntegrityError as exc:
        await session.rollback()
        classified = classify_integrity_error(exc)
        if classified is None:
            raise
        status_code, message = classified
        raise HTTPException(status_code, message) from exc
    return AgentOut.model_validate(agent)


@router.get("", response_model=list[AgentOut])
async def list_agents(session: SessionDep) -> list[AgentOut]:
    agents = await crud.list_agents(session)
    return [AgentOut.model_validate(a) for a in agents]


@router.get("/{agent_id}", response_model=AgentOut)
async def get_agent(agent_id: uuid.UUID, session: SessionDep) -> AgentOut:
    agent = await crud.get_agent(session, agent_id)
    if agent is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "agent not found")
    return AgentOut.model_validate(agent)


@router.patch("/{agent_id}", response_model=AgentOut)
async def update_agent(
    agent_id: uuid.UUID, data: AgentUpdate, session: SessionDep, store: StoreDep
) -> AgentOut:
    # No binding key here since ADR-0118: an agent may hold several bindings, so
    # "move the agent's channel" has no referent and the write surface is the
    # `/agents/{agent_id}/channels` subresource below. A caller still sending the
    # retired key is refused by the schema (422), never ignored.
    agent = await crud.get_agent(session, agent_id)
    if agent is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "agent not found")
    # The declared/bound approval-route join, judged BEFORE the first mutation
    # (#2436). A write to `approval_routes` is a full replacement (`--route`,
    # `--routes-from` and `--clear-routes` all replace the whole map), so
    # dropping a route a live bundle declares strands the human its gate exists
    # to reach exactly as effectively as a bad deploy does; ADR-0050's bounded
    # residual has to hold on this side of the join too.
    #
    # It is a PREFLIGHT rather than a check beside the write further down
    # because the field blocks below commit through independently committing
    # helpers, so a refusal placed at the `approval_routes` block would leave
    # `model`, `thinking`, `memory` and `approval_required_tools` persisted from
    # a request this handler answered 422.
    #
    # EVERY active row is judged, in both environments: git-flow appends a new
    # active row per push without superseding older ones and `end_deployment`
    # stops exactly one row, so several coexist and the worker boots from an
    # ordering over all of them. Rows sharing a version share one bundle object,
    # so `list_active_deployment_versions` collapses them to one version each --
    # which turns the common case of repeated pushes of one version into a
    # single fetch.
    if data.approval_routes is not None:
        proposed = {name: b.model_dump() for name, b in data.approval_routes.items()}
        for version in await crud.list_active_deployment_versions(session, agent_id):
            try:
                await deploy.check_approval_route_bindings(store, version, proposed)
            except deploy.BundleTooLarge as exc:
                # Learning what a live bundle declares means extracting it, so
                # this preflight inherits the caps question even when the write
                # keeps every declared route bound. Unlike `POST /deployments`,
                # nothing here calls `revalidate_stored_bundle` first, so this is
                # where an over-cap stored bundle surfaces -- as the same
                # actionable 422 that endpoint returns (ADR-0059 decision 3),
                # never a 500.
                raise HTTPException(status.HTTP_422_UNPROCESSABLE_ENTITY, str(exc)) from exc
            except deploy.ApprovalRoutesUnbound as exc:
                raise HTTPException(status.HTTP_422_UNPROCESSABLE_ENTITY, str(exc)) from exc
    # Presence, not truthiness (#1310). `is not None` conflates "the client did
    # not mention this field" with "the client explicitly sent null", so setting
    # either override used to be a one-way door: nothing could put it back to the
    # platform default. `model_fields_set` carries exactly the keys the request
    # actually contained, which is the distinction the API's own semantics rest
    # on. Both nullable overrides get it -- they are the same seam, and fixing
    # one beside the other would leave the sibling broken on an adjacent line.
    sent = data.model_fields_set
    if "model" in sent:
        agent = await crud.update_agent_model(session, agent, data.model)
    if "thinking" in sent:
        agent = await crud.update_agent_thinking(session, agent, data.thinking)
    if "execution_deadline_seconds" in sent:
        agent = await crud.update_agent_execution_deadline(
            session, agent, data.execution_deadline_seconds
        )
    if "runner_resources" in sent:
        # Shape already ran in AgentUpdate. Quota is the persistence boundary:
        # a refusal leaves the stored block untouched, including an explicit clear.
        if data.runner_resources is not None:
            settings = get_settings()
            refusal = quota_refusal(
                data.runner_resources,
                requests_cpu=settings.sandbox_quota_requests_cpu,
                requests_memory=settings.sandbox_quota_requests_memory,
                limits_cpu=settings.sandbox_quota_limits_cpu,
                limits_memory=settings.sandbox_quota_limits_memory,
            )
            if refusal is not None:
                raise HTTPException(status.HTTP_422_UNPROCESSABLE_ENTITY, refusal)
        agent = await crud.update_agent_runner_resources(session, agent, data.runner_resources)
    if data.memory is not None:
        # Omitted leaves it unchanged; unlike `model`/`thinking` there is no
        # separate "platform default" a null would clear back to, so this
        # follows the plain-None-check siblings below rather than the
        # model_fields_set pair above.
        agent = await crud.update_agent_memory(session, agent, data.memory)
    if data.approval_required_tools is not None:
        # Omitted leaves the gates unchanged; an explicit [] clears them (#245).
        agent = await crud.update_agent_approval_tools(session, agent, data.approval_required_tools)
    if data.approval_routes is not None:
        # Omitted leaves the bindings unchanged; an explicit {} clears them (#247).
        # Only the write: the preflight at the top of this handler has already
        # judged this same map against every active deployment's declared routes
        # (#2436), because by here four other fields are already committed.
        agent = await crud.update_agent_approval_routes(
            session,
            agent,
            {name: b.model_dump() for name, b in data.approval_routes.items()},
        )
    if data.repo_full_name is not None:
        # Binds this agent to a repository so git-flow can route pushes to it
        # (ADR-0091). Several agents may share one, so this cannot collide.
        agent = await crud.update_agent_repo(session, agent, data.repo_full_name)
    if data.secrets is not None:
        # Omitted leaves the secrets unchanged; an explicit {} clears them (#429).
        agent = await crud.update_agent_secrets(session, agent, data.secrets)
    if data.hook_partitions is not None:
        # Omitted leaves the partitions unchanged; an explicit {} clears them
        # (ADR-0134). Plain `is not None` like the siblings above rather than
        # `model_fields_set`: there is no platform default a null would clear
        # back to, which is the distinction `memory` already draws.
        agent = await crud.update_agent_hook_partitions(session, agent, data.hook_partitions)
    if data.source_bindings is not None:
        agent = await crud.update_agent_source_bindings(session, agent, data.source_bindings)
    if (
        "publication_policy" in sent
        or "publication_draft" in sent
        or "publication_branch_prefix" in sent
    ):
        try:
            agent = await crud.update_agent_publication_policy(
                session,
                agent,
                policy=data.publication_policy if "publication_policy" in sent else None,
                draft=data.publication_draft if "publication_draft" in sent else None,
                branch_prefix=(
                    data.publication_branch_prefix if "publication_branch_prefix" in sent else None
                ),
                prefix_sent="publication_branch_prefix" in sent,
            )
        except PublicationPolicyConflict as exc:
            raise HTTPException(
                status.HTTP_409_CONFLICT,
                {
                    "code": "publication.policy_version_conflict",
                    "message": "publication policy version changed; retry the read",
                },
            ) from exc
    return AgentOut.model_validate(agent)


@router.delete("/{agent_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_agent(agent_id: uuid.UUID, session: SessionDep) -> None:
    # Deleting an agent cascades its versions and deployments rows (bundle
    # objects in RustFS are left as-is, out of scope). Refuse while a deployment
    # is still active so a live agent cannot be pulled out from under Slack
    # traffic; the caller must stop it (kill/undeploy) first.
    agent = await crud.get_agent(session, agent_id)
    if agent is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "agent not found")
    if await crud.agent_has_active_deployment(session, agent_id):
        raise HTTPException(
            status.HTTP_409_CONFLICT,
            "agent has an active deployment; stop it before deleting",
        )
    await crud.delete_agent(session, agent_id)


# --- the channel-binding subresource (ADR-0118, #1525) ------------------------
#
# One agent holds one or more `(kind, address)` bindings, so add, move and
# remove are three verbs here instead of one overloaded `AgentUpdate.channel`
# field, each with exactly one meaning. The pair selects the binding on PATCH
# and DELETE, passed as QUERY parameters: it is the routing key every other
# layer already uses (`binding._RESOLVE_SQL`, `agent_channels_kind_address_key`)
# and an `address` is opaque per kind, so a `/` in one would have to survive as
# `%2F` in a path segment -- a proxy hazard the query string does not have.


async def _agent_or_404(session: AsyncSession, agent_id: uuid.UUID) -> Agent:
    agent = await crud.get_agent(session, agent_id)
    if agent is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "agent not found")
    return agent


def _binding_for(
    bindings: list[AgentChannel], kind: str, address: str, adapter: str | None
) -> AgentChannel:
    """Pick the route's row out of THIS agent's locked set, or 404 or 409.

    The matching RULE lives in `crud.matching_bindings`, shared with every
    other reader of a route including `add_agent_channel`'s idempotence check
    below, so the two surfaces here agree on what counts as "the same
    binding". Migration 0023's `agent_channels_kind_address_key` (UNIQUE
    kind, address) holds one row per pair, so the 409 below (several
    identities on one pair) cannot fire against real data until the contract
    migration for ADR-0168 decision 3 (#3100) widens the constraint to the
    triple and several identities can share one pair.

    Selecting from the locked list rather than issuing a second, unlocked query
    is what makes the lock load-bearing. It is also the authorization boundary:
    a route belonging to a DIFFERENT agent names no row here, so it reads as
    404 rather than becoming a cross-agent write the caller sees a 200 for.
    """

    matches = crud.matching_bindings(bindings, kind, address, adapter)
    if len(matches) > 1:
        raise HTTPException(
            status.HTTP_409_CONFLICT,
            f"several identities are bound to {kind}:{address}; pass adapter to name one",
        )
    if matches:
        return matches[0]
    if adapter is not None:
        # Names the identity that found nothing, not only the pair, so a
        # caller who passed the right pair but the wrong identity does not
        # read the same 404 as one who mistyped the address itself.
        identity = route_identity(kind, adapter)
        raise HTTPException(
            status.HTTP_404_NOT_FOUND,
            f"this agent has no {kind}:{address} binding as {identity!r}",
        )
    raise HTTPException(
        status.HTTP_404_NOT_FOUND,
        f"this agent has no {kind}:{address} binding",
    )


def _conflict_message(
    route_owner: uuid.UUID | None,
    pair_owner: uuid.UUID | None,
    agent_id: uuid.UUID,
    kind: str,
    adapter: str | None,
    address: str,
) -> str:
    """The 409 sentence for a taken route, accurate about WHO holds it.

    The generic map message says "another agent is already bound", which is
    false -- and actively misleading -- when the duplicate is this agent's
    own. Two different "this agent's own" cases need two different sentences:

    - `route_owner == agent_id`: the EXACT route this write asked for
      (kind, resolved identity, address) already exists -- the ordinary
      idempotent-recheck case.
    - `route_owner is None` but `pair_owner == agent_id`: the database
      constraint is still the pair alone (`agent_channels_kind_address_key`,
      migration 0023, until the contract migration for ADR-0168 decision 3
      (#3100) widens it), so the identity-precise lookup can answer `None` while
      this agent still holds `(kind, address)` under a DIFFERENT route -- a
      custom-transport binding a bare identity-form write collided with, for
      instance. Reporting the generic "another agent" sentence here is false:
      no other agent is involved, this agent's own other route is what is in
      the way.
    - Otherwise (both `None`, or naming a different agent): the generic map
      message. Both `None` means the winning row was deleted between the
      failed insert and this lookup -- the pair is free again -- and the
      generic sentence is the safe answer either way, since the caller
      retries.
    """

    if route_owner is not None and route_owner == agent_id:
        identity = route_identity(kind, adapter)
        route = f"{kind}:{identity}:{address}" if identity is not None else f"{kind}:{address}"
        return (
            f"this agent is already bound to {route}; the binding you "
            "asked for already exists, so nothing was changed"
        )
    if pair_owner is not None and pair_owner == agent_id:
        return (
            f"this agent already holds {kind}:{address} under another route; "
            "this installation allows only one route per (kind, address) pair "
            "-- move or delete the other binding first"
        )
    return _UNIQUE_CONSTRAINT_MESSAGES["agent_channels_kind_address_key"]


# Postgres SQLSTATE for `deadlock_detected`. asyncpg surfaces it as `sqlstate`
# on the driver exception SQLAlchemy wraps in `DBAPIError` -- and NOT as an
# `IntegrityError`, so the unique-violation recovery below never sees it.
_DEADLOCK_DETECTED = "40P01"

# The 409 a broken deadlock earns. Deliberately the same STATUS as a taken
# pair: from the caller's side both mean "the binding set moved under you, the
# write did not land, retry" -- and a deadlock victim is the one caller for
# whom a retry is near-certain to succeed, since its opponent has by then
# committed. Left as a 500 it reads as a server fault and an operator stops
# retrying the one request that would work.
_DEADLOCK_MESSAGE = (
    "the binding set changed concurrently: another binding write was moving "
    "the same channel pairs and the database broke the tie by aborting this "
    "request. Nothing was changed -- retry it."
)


def _is_deadlock(exc: DBAPIError) -> bool:
    """Whether this wrapped driver error is Postgres breaking a lock cycle."""

    return getattr(exc.orig, "sqlstate", None) == _DEADLOCK_DETECTED


@asynccontextmanager
async def _deadlock_as_conflict() -> AsyncIterator[None]:
    """Turn a broken lock cycle into a retryable 409 instead of a 500.

    `lock_agent_bindings` locks ONE agent's rows, but a `(kind, address)` pair
    is globally unique: two callers swapping their agents' pairs in opposite
    directions each hold their own agent's rows and then wait on the other's
    uncommitted index entry. That is a genuine cycle, Postgres aborts one side
    with `40P01`, and without this the victim gets an unexplained 500 for a
    race it can simply retry.

    Wraps the WHOLE handler body rather than the savepoint alone: the cycle can
    close on the locking read, on the flush, or on the commit, and all three are
    the same answer to the caller. A non-deadlock `DBAPIError` (`IntegrityError`
    included, since it is a subclass) is re-raised untouched -- the unique
    violation still belongs to the owner-accurate recovery below.
    """

    try:
        yield
    except DBAPIError as exc:
        if not _is_deadlock(exc):
            raise
        raise HTTPException(status.HTTP_409_CONFLICT, _DEADLOCK_MESSAGE) from exc


async def _raise_binding_conflict(
    exc: IntegrityError, session: AsyncSession, agent_id: uuid.UUID, channel: ChannelBindingWrite
) -> NoReturn:
    """Turn a binding write's `IntegrityError` into an owner-accurate 409.

    Runs after the SAVEPOINT rolled back -- and only the savepoint, so the outer
    transaction and the row locks `lock_agent_bindings` took are still live and
    the owner lookup reads the same serialized snapshot the guard above did. A
    `session.rollback()` here would discard those locks and answer about a world
    that may have moved again; no rollback would leave the session failed and
    answer 500 `PendingRollbackError` on the lookup itself.

    A non-unique violation (NOT NULL, FK) is a server fault, so it is re-raised
    rather than dressed up as a conflict; a mapped check violation is not a
    conflict either, and goes back as its own status and message.
    """

    classified = classify_integrity_error(exc)
    if classified is None:
        raise exc
    if classified[0] != status.HTTP_409_CONFLICT:
        raise HTTPException(*classified) from exc
    route_owner = await crud.agent_id_for_route(
        session, channel.kind, channel.adapter, channel.address
    )
    pair_owner = route_owner
    if route_owner is None:
        # The database constraint is the PAIR, not the triple
        # (`agent_channels_kind_address_key`, migration 0023, until the
        # contract migration for ADR-0168 decision 3 (#3100)). The
        # identity-precise lookup above can legitimately answer None while
        # this agent still holds the pair under a DIFFERENT route, so recheck
        # at the pair level before `_conflict_message` concludes the pair is
        # free or belongs to someone else.
        pair_owner = await crud.agent_id_for_channel_pair(session, channel.kind, channel.address)
    raise HTTPException(
        status.HTTP_409_CONFLICT,
        _conflict_message(
            route_owner, pair_owner, agent_id, channel.kind, channel.adapter, channel.address
        ),
    ) from exc


@router.post("/{agent_id}/channels", response_model=AgentOut, status_code=status.HTTP_201_CREATED)
async def add_agent_channel(
    agent_id: uuid.UUID, data: ChannelBindingWrite, session: SessionDep
) -> AgentOut:
    """Bind this agent to one more channel. Appends; never moves."""

    async with _deadlock_as_conflict():
        agent = await _agent_or_404(session, agent_id)
        # Taken before the insert even though nothing is read from the set: it
        # serializes this add against a concurrent move or delete of the same
        # agent's bindings, which is what keeps the last-binding guard sound.
        bindings = await crud.lock_agent_bindings(session, agent_id)
        # A re-POST of a pair this agent already holds is an idempotent
        # success that changes nothing. `crud.matching_bindings` is the same
        # rule `_binding_for` selects by, so a re-POST naming no adapter finds
        # this agent's Slack custom-transport row the way a PATCH or DELETE
        # naming none does. The pair check behind it covers a repeat naming a
        # different adapter: migration 0023's `agent_channels_kind_address_key`
        # lets the pair carry one row, so that repeat cannot be a second route
        # and would otherwise fail the insert as a conflict with itself.
        if crud.matching_bindings(bindings, data.kind, data.address, data.adapter) or any(
            binding.kind == data.kind and binding.address == data.address for binding in bindings
        ):
            return AgentOut.model_validate(await crud.refresh_with_channels(session, agent))
        try:
            async with session.begin_nested():  # SAVEPOINT
                await crud.add_channel_binding(session, agent_id, data)
        except IntegrityError as exc:
            classified = classify_integrity_error(exc)
            if classified is None:
                raise
            if classified[0] != status.HTTP_409_CONFLICT:
                raise HTTPException(*classified) from exc
            # Two concurrent idempotent adds can both observe the pair absent;
            # the winner inserts and the loser reaches the unique constraint.
            # Once the savepoint has rolled back, treat that winner as the same
            # successful desired state when it belongs to this agent. Asked of
            # the PAIR, the key the violated constraint enforces, for the same
            # reason as the check above.
            owner = await crud.agent_id_for_channel_pair(session, data.kind, data.address)
            if owner == agent_id:
                return AgentOut.model_validate(await crud.refresh_with_channels(session, agent))
            await _raise_binding_conflict(exc, session, agent_id, data)
        return AgentOut.model_validate(await crud.refresh_with_channels(session, agent))


@router.patch("/{agent_id}/channels", response_model=AgentOut)
async def move_agent_channel(
    agent_id: uuid.UUID,
    kind: str,
    address: str,
    data: ChannelBindingPatch,
    session: SessionDep,
    adapter: str | None = None,
    expected_generation: int | None = None,
) -> AgentOut:
    """Move (or re-assert) the binding the `(kind, adapter, address)` route names.

    `adapter` selects the IDENTITY (ADR-0168 decision 3): omitted, it means
    Slack's default identity, and for any other kind it selects the only row
    on `(kind, address)` -- see `_binding_for`.

    `expected_generation` is an OPTIONAL compare-and-set, in the shape
    `routers/state.py` uses for its versioned rows, so the two CAS surfaces read
    alike. Optional because an operator moving a binding from the CLI has no
    generation to quote; a channel adapter holding a token minted against
    generation N does, and it is exactly the caller that must not overwrite a
    rebind it never saw.
    """

    async with _deadlock_as_conflict():
        agent = await _agent_or_404(session, agent_id)
        bindings = await crud.lock_agent_bindings(session, agent_id)
        binding = _binding_for(bindings, kind, address, adapter)
        if expected_generation is not None and expected_generation != binding.generation:
            raise HTTPException(
                status.HTTP_409_CONFLICT,
                f"generation mismatch: expected {expected_generation}, stored {binding.generation}",
            )
        try:
            async with session.begin_nested():  # SAVEPOINT
                await crud.update_channel_binding(session, binding, data)
        except IntegrityError as exc:
            # The same recovery as the add: a move onto a pair another agent (or
            # this one) already holds raises the identical violation and needs the
            # identical owner recheck, inside the same still-live transaction.
            await _raise_binding_conflict(exc, session, agent_id, data)
        return AgentOut.model_validate(await crud.refresh_with_channels(session, agent))


@router.delete("/{agent_id}/channels", status_code=status.HTTP_204_NO_CONTENT)
async def remove_agent_channel(
    agent_id: uuid.UUID, kind: str, address: str, session: SessionDep, adapter: str | None = None
) -> None:
    """Unbind the route, unless it is this agent's last binding.

    `adapter` selects the IDENTITY (ADR-0168 decision 3), the same as on the
    move endpoint above -- see `_binding_for`.

    The last one is refused: an agent with zero bindings is deployed,
    healthy-looking and unable to receive a turn -- #38's silent-shadow state,
    and the same reason `AgentCreate.channel` is required rather than optional.
    The count comes from the locked set, so two concurrent deletes of different
    pairs cannot both read "two left" and leave the agent at zero.
    """

    async with _deadlock_as_conflict():
        await _agent_or_404(session, agent_id)
        bindings = await crud.lock_agent_bindings(session, agent_id)
        binding = _binding_for(bindings, kind, address, adapter)
        if len(bindings) <= 1:
            raise HTTPException(
                status.HTTP_409_CONFLICT,
                f"{kind}:{address} is this agent's last binding; an agent with no "
                "binding cannot receive a turn. Add another binding first, or "
                "delete the agent.",
            )
        await crud.delete_channel_binding(session, binding)
        await session.commit()


@router.post(
    "/{agent_id}/versions",
    response_model=VersionOut,
    status_code=status.HTTP_201_CREATED,
)
async def create_version(
    agent_id: uuid.UUID, data: VersionCreate, session: SessionDep
) -> VersionOut:
    if await crud.get_agent(session, agent_id) is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "agent not found")
    version = await crud.create_version(session, agent_id, data)
    return VersionOut.model_validate(version)


@router.get("/{agent_id}/versions", response_model=list[VersionOut])
async def list_versions(agent_id: uuid.UUID, session: SessionDep) -> list[VersionOut]:
    if await crud.get_agent(session, agent_id) is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "agent not found")
    versions = await crud.list_versions(session, agent_id)
    return [VersionOut.model_validate(v) for v in versions]


@router.get("/{agent_id}/versions/{version_id}/connectors", response_model=ConnectorManifests)
async def read_version_connectors(
    agent_id: uuid.UUID,
    version_id: uuid.UUID,
    session: SessionDep,
    store: StoreDep,
    release: str,
    namespace: str,
    app_name: str,
) -> ConnectorManifests:
    """Render this version's declared connectors into Kubernetes objects.

    Read-only and side-effect free: the API computes the manifests and returns
    them; the CALLER applies them with its own cluster credentials. That split
    is deliberate -- rendering is a pure function, so the API needs no cluster
    access for it, and this service (which receives internet webhooks) keeps the
    read-only `pods: list` + `pods/log: get` RBAC it has today (ADR-0086).

    `release`, `namespace`, and `app_name` are supplied by the caller because
    they are install-time facts the API does not know: the Helm release name and
    nameOverride live with whoever ran `cluster up`, not in the bundle.
    """

    version = await crud.get_version(session, version_id)
    if version is None or version.agent_id != agent_id:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "version not found")
    if version.bundle_ref is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "no bundle stored for this version")
    # Object names are scoped to the agent, not just the release (#1116). Curie
    # runs many agents per release, so a release-scoped name lets two agents
    # that each declare `grafana` overwrite one another's Deployment, Service,
    # and credential with no error. The agent NAME (not the id) is used so the
    # objects stay recognisable in `kubectl get`.
    agent = await crud.get_agent(session, agent_id)
    if agent is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "agent not found")
    data = await store.get(version.bundle_ref)
    settings = get_settings()
    agent_name = agent.name

    def _render() -> ConnectorManifests:
        with tempfile.TemporaryDirectory() as tmp:
            bundles.extract_and_validate(
                data,
                Path(tmp),
                max_uncompressed_bytes=settings.bundle_max_uncompressed_bytes,
                max_compression_ratio=settings.bundle_max_compression_ratio,
                max_members=settings.bundle_max_members,
            )
            # Resolve every `build:` connector to the digest the bundle already
            # records before anything renders (ADR 0113). `apply_lock` reads a
            # recorded fact and never resolves or builds one, so the API stays a
            # pure renderer under ADR-0087.
            #
            # `portable=True` because EVERY consumer of this route applies what
            # it returns to a Kubernetes cluster: the worker's connector
            # reconcile loop (`curie_worker.connector_loop.HttpManifestSource`,
            # ADR-0090) and `curie cluster deploy`'s `sync_connectors`
            # (`cli/src/main.rs`, ADR-0086). Nothing reads these manifests for
            # display. A `local-daemon` lock records a bare docker image id that
            # names nothing a node can pull, so rendering one here yields a
            # Deployment that ImagePullBackOffs long after the deploy reported
            # success -- with every gate green. Bundle intake keeps
            # `portable=False` (`plugin_format.validate`): a local-tier version
            # is a legitimate stored artifact. The refusal belongs at the render
            # that feeds an applier, which is this one.
            try:
                declared = connector_lock.apply_lock(
                    bundles.read_connectors(Path(tmp)),
                    bundles.read_connector_lock(Path(tmp)),
                    portable=True,
                )
            except ValueError as exc:
                # 422, matching how this service reports a stored bundle that
                # cannot yield a deployable artifact (`create_deployment`,
                # `upload_bundle`): the request is well formed, the bundle is
                # not applicable. A 500 would read as an API fault and send the
                # operator to the API logs instead of to `curie build
                # --plugin-dir <dir> --registry <ref>`, which the message names.
                raise HTTPException(status.HTTP_422_UNPROCESSABLE_ENTITY, str(exc)) from exc
            # Per-agent too: a release-scoped Secret means deploying the prod
            # agent overwrites the dev agent's token in place (#1116).
            secret_name = f"{release}-{agent_name}-connector-secrets"
            return ConnectorManifests(
                manifests=bundles.render_connector_manifests(
                    declared,
                    release=release,
                    agent=agent_name,
                    namespace=namespace,
                    app_name=app_name,
                    secret_name=secret_name,
                ),
                mcp_entries=bundles.connector_mcp_entries(
                    declared, release=release, agent=agent_name, namespace=namespace
                ),
                # Which keys the CALLER must resolve a value for. Stated rather
                # than inferable: a referenced Secret's key renders an identical
                # secretKeyRef, and resolving it would defeat the point (#1163).
                owned_secret_name=secret_name,
                owned_secret_keys=bundles.owned_secret_keys(declared),
                version_id=version.id,
                triggers=bundles.read_manifest_triggers(Path(tmp)),
            )

    # `object_name` fails closed on an agent name that forges its `-mcp-` join
    # (#1446), and that raise surfaces HERE: every derivation the renderer
    # touches goes through it. The create path refuses such a name now, so the
    # only rows still holding one predate that validator -- which is exactly the
    # case this read-only endpoint has to survive.
    #
    # 422 rather than letting it fall through as a 500. The request is
    # well-formed -- real ids, a stored bundle, valid install-time params -- and
    # nothing about it can be corrected; what cannot be satisfied is the STORED
    # agent name. A 500 tells an operator running `cluster deploy` only that the
    # server broke, with no name in the body and nothing to act on, and the name
    # is not necessarily one they are holding: the bundle's deploy.yaml chose
    # which agent this renders for.
    #
    # "Recreate", not "rename": `AgentUpdate` carries no `name` field, so there
    # is no in-place rename to point them at. Saying "rename" would send them
    # looking for an API that does not exist.
    try:
        return await run_in_threadpool(_render)
    except AmbiguousObjectName as exc:
        raise HTTPException(
            status.HTTP_422_UNPROCESSABLE_ENTITY,
            f"the stored name of agent {agent_name!r} cannot produce an "
            f"unambiguous connector object name ({exc}). Connector objects are named "
            "'{release}-{agent}-mcp-{connector}', so this name makes two "
            "different agents render the same Kubernetes objects and share one "
            "connector's credential (#1446). Recreate the agent under a name "
            "that neither contains '-mcp-' nor ends in '-mcp'; an agent's name "
            "cannot be changed in place.",
        ) from exc


@router.get("/{agent_id}/versions/{version_id}/files", response_model=BundleFiles)
async def read_version_files(
    agent_id: uuid.UUID,
    version_id: uuid.UUID,
    session: SessionDep,
    store: StoreDep,
) -> BundleFiles:
    # The UI reads a version's authored text (skills, manifest, eval cases) to
    # render the bundle without pulling the raw archive. 404 covers a missing
    # agent, a version that is not this agent's, and a version with no bundle
    # stored yet -- there is nothing to read in any of those cases.
    version = await crud.get_version(session, version_id)
    if version is None or version.agent_id != agent_id:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "version not found")
    if version.bundle_ref is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "no bundle stored for this version")
    data = await store.get(version.bundle_ref)
    settings = get_settings()
    read = functools.partial(
        bundles.read_bundle_text_files,
        max_uncompressed_bytes=settings.bundle_max_uncompressed_bytes,
        max_compression_ratio=settings.bundle_max_compression_ratio,
        max_members=settings.bundle_max_members,
    )
    files = await run_in_threadpool(read, data)
    return BundleFiles(files=[BundleFile(path=p, content=c) for p, c in files])
