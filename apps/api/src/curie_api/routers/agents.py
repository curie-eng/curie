"""Agents and their versions."""

import functools
import tempfile
import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path
from typing import NoReturn

from fastapi import APIRouter, Depends, HTTPException, status
from plugin_format import connector_lock
from sqlalchemy.exc import DBAPIError, IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from starlette.concurrency import run_in_threadpool

from .. import bundles, crud
from ..auth import require_api_key
from ..config import get_settings
from ..deps import SessionDep, StoreDep
from ..models import Agent, AgentChannel
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
    # belong to every channel kind.
    "agent_channels_kind_address_key": (
        "another agent is already bound to that channel kind and address; one "
        "agent per route (move or delete the other agent, or pick another "
        "address)"
    ),
    # No entry for `agent_channels_agent_id_key`: migration 0028 drops that
    # constraint (ADR-0116), so its message can never fire again, and it said
    # the opposite of what this API now does. A dead entry is worse than none --
    # it reads as a protection. The pair constraint above is the ONLY binding
    # conflict left.
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
    """Map a real unique-constraint violation to a `(409, message)` conflict.

    Only a genuine unique_violation (SQLSTATE 23505) is a caller conflict; a
    NOT NULL or FK violation is a server fault and must surface as a 500, so
    this returns `None` for those (the caller re-raises). The human message is
    chosen by the violated constraint's name from asyncpg's structured fields,
    not by substring-matching the stringified driver error.
    """
    if _driver_diag(exc, "sqlstate") != _UNIQUE_VIOLATION:
        return None
    constraint_name = _driver_diag(exc, "constraint_name")
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
async def update_agent(agent_id: uuid.UUID, data: AgentUpdate, session: SessionDep) -> AgentOut:
    # No binding key here since ADR-0116: an agent may hold several bindings, so
    # "move the agent's channel" has no referent and the write surface is the
    # `/agents/{agent_id}/channels` subresource below. A caller still sending the
    # retired key is refused by the schema (422), never ignored.
    agent = await crud.get_agent(session, agent_id)
    if agent is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "agent not found")
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
    if data.approval_required_tools is not None:
        # Omitted leaves the gates unchanged; an explicit [] clears them (#245).
        agent = await crud.update_agent_approval_tools(session, agent, data.approval_required_tools)
    if data.approval_routes is not None:
        # Omitted leaves the bindings unchanged; an explicit {} clears them (#247).
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


# --- the channel-binding subresource (ADR-0116, #1525) ------------------------
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


def _binding_for(bindings: list[AgentChannel], kind: str, address: str) -> AgentChannel:
    """Pick the pair's row out of THIS agent's locked set, or 404.

    Selecting from the locked list rather than issuing a second, unlocked query
    is what makes the lock load-bearing. It is also the authorization boundary:
    a pair belonging to a DIFFERENT agent names no row here, so it reads as 404
    rather than becoming a cross-agent write the caller sees a 200 for.
    """

    for binding in bindings:
        if binding.kind == kind and binding.address == address:
            return binding
    raise HTTPException(
        status.HTTP_404_NOT_FOUND,
        f"this agent has no {kind}:{address} binding",
    )


def _conflict_message(owner: uuid.UUID | None, agent_id: uuid.UUID, kind: str, address: str) -> str:
    """The 409 sentence for a taken pair, accurate about WHO holds it.

    The generic map message says "another agent is already bound", which is
    false -- and actively misleading -- when the duplicate is this agent's own:
    it sends an operator looking for an agent that does not exist, and the CLI's
    ensure-bound recheck cannot tell a satisfied desired state from a real
    collision. A `None` owner means the winning row was deleted between the
    failed insert and this lookup; the pair is free again, and the generic
    sentence is the safe answer since either way the caller retries.
    """

    if owner is not None and owner == agent_id:
        return (
            f"this agent is already bound to {kind}:{address}; the binding you "
            "asked for already exists, so nothing was changed"
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
    rather than dressed up as a conflict.
    """

    if classify_integrity_error(exc) is None:
        raise exc
    owner = await crud.agent_id_for_pair(session, channel.kind, channel.address)
    raise HTTPException(
        status.HTTP_409_CONFLICT,
        _conflict_message(owner, agent_id, channel.kind, channel.address),
    ) from exc


@router.post(
    "/{agent_id}/channels", response_model=AgentOut, status_code=status.HTTP_201_CREATED
)
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
        if any(
            binding.kind == data.kind and binding.address == data.address
            for binding in bindings
        ):
            return AgentOut.model_validate(await crud.refresh_with_channels(session, agent))
        try:
            async with session.begin_nested():  # SAVEPOINT
                await crud.add_channel_binding(session, agent_id, data)
        except IntegrityError as exc:
            if classify_integrity_error(exc) is None:
                raise
            # Two concurrent idempotent adds can both observe the pair absent;
            # the winner inserts and the loser reaches the unique constraint.
            # Once the savepoint has rolled back, treat that winner as the same
            # successful desired state when it belongs to this agent.
            owner = await crud.agent_id_for_pair(session, data.kind, data.address)
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
    expected_generation: int | None = None,
) -> AgentOut:
    """Move (or re-assert) the binding the `(kind, address)` pair names.

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
        binding = _binding_for(bindings, kind, address)
        if expected_generation is not None and expected_generation != binding.generation:
            raise HTTPException(
                status.HTTP_409_CONFLICT,
                f"generation mismatch: expected {expected_generation}, "
                f"stored {binding.generation}",
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
    agent_id: uuid.UUID, kind: str, address: str, session: SessionDep
) -> None:
    """Unbind the pair, unless it is this agent's last binding.

    The last one is refused: an agent with zero bindings is deployed,
    healthy-looking and unable to receive a turn -- #38's silent-shadow state,
    and the same reason `AgentCreate.channel` is required rather than optional.
    The count comes from the locked set, so two concurrent deletes of different
    pairs cannot both read "two left" and leave the agent at zero.
    """

    async with _deadlock_as_conflict():
        await _agent_or_404(session, agent_id)
        bindings = await crud.lock_agent_bindings(session, agent_id)
        binding = _binding_for(bindings, kind, address)
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
            )

    return await run_in_threadpool(_render)


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
