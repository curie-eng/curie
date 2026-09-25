"""Durable agent-scoped key/value state (#23, #248).

A small compare-and-set KV/document store: namespace + key per agent, an
arbitrary-JSON value, Postgres JSONB backing. Operations: get / put-with-CAS /
list / delete / append. Two hard non-goals keep this from becoming a database
product: there is no query language (get-by-key + list-by-namespace only), and
both a single value and a whole namespace are size-capped (#248). This is the
API surface the approvals epic (#22) and other cross-turn workflow state
consume. It is also exposed to bundle code (#249) via the auto-mounted
``curie-state`` MCP server and the ``CURIE_STATE_URL`` / ``CURIE_STATE_TOKEN``
boot-env pair, so a skill reads and writes state without shipping its own server;
the sandbox authenticates with a scoped ``state`` token (ADR-0033), never the
platform key.
"""

import enum
import hashlib
import uuid
from typing import Annotated, Any

from fastapi import APIRouter, Depends, Header, HTTPException, Response, status
from sqlalchemy import Text, cast, delete, func, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from .. import crud, sandbox_token, transcripts
from ..auth import verify_platform_key
from ..config import get_settings
from ..deps import SessionDep
from ..models import ThreadTranscript, WorkflowStateEntry
from ..schemas import StateAppendIn, StateEntryOut, StateEntryPut, StateNamespaceOut
from ..transcripts import TRANSCRIPT_NAMESPACE
from ..transcripts import json_size as _json_size

# Two scoped-token scopes the state router accepts (ADR-0033). The BROAD scope is
# minted for the runner's own memory/history loaders, which MUST read and write
# the reserved namespaces to rehydrate the agent across a suspend/resume; it
# reaches every namespace. The NARROW scope is minted for the bundle-facing
# ``CURIE_STATE_TOKEN`` and is refused on the reserved namespaces by
# ``forbid_reserved_namespace`` below -- so a skill using the mounted state
# interface (the ``curie-state`` MCP tools or a direct ``CURIE_STATE_URL``
# call) cannot reach the memory/history ports even by composing the URL itself.
# Both strings are mirrored at the worker mint site (``binding.py``); a
# byte-identical string on both sides is the contract, like ``sandbox_token``.
STATE_SCOPE = "state"
STATE_APP_SCOPE = "state.app"

# Namespaces owned by the memory (#264) and history (#20) ports; the narrow
# app-scoped (bundle) token may not touch them. Literals rather than an import
# because ``routers.memory`` imports ``_enforce_caps`` from THIS module (a real
# import cycle otherwise). Mirrors the runner client's ``RESERVED_NAMESPACES``
# (``runner/src/curie_runner/state.py``) and ``memory.MEMORY_NAMESPACE`` /
# the history transcript key -- a bundle wanting durable memory uses the remember
# tool, not raw state. A future fixed namespace must be added here too.
RESERVED_NAMESPACES = frozenset({"memory", TRANSCRIPT_NAMESPACE})


class StateCaller(enum.Enum):
    """Which credential authorized a state-router request, and thus how far it
    reaches. PLATFORM (the shared key) and STATE (the broad scoped token, i.e.
    the memory/history loaders) are unrestricted; APP (the narrow bundle token)
    is refused on ``RESERVED_NAMESPACES``."""

    PLATFORM = "platform"
    STATE = "state"
    APP = "app"


async def require_state_access(
    agent_id: uuid.UUID,
    x_api_key: Annotated[str | None, Header()] = None,
) -> StateCaller:
    """State-router auth (ADR-0033): the platform key (trusted callers) OR a
    scoped token bound to this path's ``agent_id`` (the sandbox). The broad
    ``state`` scope (the runner's memory/history loaders) reaches every namespace;
    the narrow app scope (the bundle-facing ``CURIE_STATE_TOKEN``) is refused on
    the reserved namespaces by ``forbid_reserved_namespace``. Every other router
    keeps the platform-key-only ``require_api_key``. Returns which caller
    authenticated so the namespace guard can apply the right reach."""

    if verify_platform_key(x_api_key):
        return StateCaller.PLATFORM
    if x_api_key is not None:
        api_key = get_settings().api_key
        agent = str(agent_id)
        if sandbox_token.verify(x_api_key, api_key, agent=agent, scope=STATE_SCOPE):
            return StateCaller.STATE
        if sandbox_token.verify(x_api_key, api_key, agent=agent, scope=STATE_APP_SCOPE):
            return StateCaller.APP
    raise HTTPException(
        status.HTTP_401_UNAUTHORIZED, detail="missing or invalid credential"
    )


async def _binding_scope(
    session: AsyncSession, agent_id: uuid.UUID, kind: str, address: str
) -> str:
    """The `workflow_state_entries.binding_scope` value for one named binding
    (#1525 follow-up): `"{kind}:{address}"`, once confirmed to actually belong
    to this agent.

    Not a security boundary -- the caller already reached this far only by
    presenting a credential authenticating it as THIS agent's own sandbox (or
    the platform key), and a scope string is just a partition key within that
    one agent's already-fully-accessible general-state store, the same as any
    `namespace`/`key` a caller could always freely choose. Checking it against
    `agent_channels` is a correctness guard -- a typo or a stale binding name
    fails loudly as 404 instead of silently opening a new, orphaned partition
    that corresponds to nothing -- not an authorization check. That is also
    why this reads the database directly rather than trusting a claim on the
    presented credential: the credential (`sandbox_token`) authenticates WHICH
    agent, never which of that agent's own bindings, by design (ADR-0033;
    rejected alternative for #1525 was widening it to carry one, but a
    same-agent partition key has no privilege for that credential to carry in
    the first place).

    No caller here NAMES an adapter -- the state API has no such parameter --
    and ADR-0168 decision 4 rules that this agent's own bindings on one pair
    share ONE binding-state scope whichever identity holds them: unlike a
    route-resolution reader (`crud.binding_for_route`), which narrows to one
    identity via `crud.matching_bindings` because it must pick the single row
    a turn resolves through, this reader asks a strictly coarser question --
    does this agent hold ANY row here -- so it must not narrow by identity
    first. `crud.agent_id_for_channel_pair` is the "ignoring identity" pair
    lookup for exactly that reason; `crud.binding_for_route`'s identity-
    narrowed one is what 404'd every named-identity binding's own state (A3).
    """

    owner = await crud.agent_id_for_channel_pair(session, kind, address)
    if owner is None or owner != agent_id:
        raise HTTPException(
            status.HTTP_404_NOT_FOUND, f"this agent has no {kind}:{address} binding"
        )
    return f"{kind}:{address}"


async def forbid_reserved_namespace(
    namespace: str,
    caller: Annotated[StateCaller, Depends(require_state_access)],
) -> None:
    """Server-side backstop for the reserved-namespace rule (#249): a narrow
    app-scoped (bundle) token may not read or write the memory/transcript
    namespaces -- those belong to the memory (#264) and history (#20) ports. The
    platform key and the broad ``state`` token (the loaders) are unrestricted.
    Without this a skill could bypass the ``curie-state`` tool's own client-side
    refusal by composing ``CURIE_STATE_URL`` directly with the token it holds;
    here the token it holds is simply refused."""
    if caller is StateCaller.APP and namespace in RESERVED_NAMESPACES:
        raise HTTPException(
            status.HTTP_403_FORBIDDEN,
            f"namespace {namespace!r} is reserved by the platform "
            f"(reserved: {', '.join(sorted(RESERVED_NAMESPACES))}); "
            "use the memory or history tools instead",
        )


router = APIRouter(
    prefix="/agents", tags=["state"], dependencies=[Depends(require_state_access)]
)


# Advisory-lock class for the per-agent namespace-count cap (#933). The
# TWO-argument ``pg_advisory_xact_lock(int4, int4)`` form is used deliberately:
# Postgres keeps the two-int4 lock space entirely separate from the
# one-argument bigint space, so this can never collide with a
# ``pg_advisory_lock(<bigint>)`` taken anywhere else -- including the test-only
# write gates in apps/api/tests/, which use the one-arg form. The number is the
# issue.
_NAMESPACE_LOCK_CLASS = 933


def _namespace_lock_key(agent_id: uuid.UUID) -> int:
    """A stable int4 advisory-lock key for one agent (#933).

    Deterministic in every process and across restarts, which is the whole
    point: Python's builtin ``hash()`` is PER-PROCESS randomized (PYTHONHASHSEED),
    so two API workers would derive different keys for the same agent and the
    lock would silently stop serializing anything. Hence hashlib. blake2b over
    the UUID's 16 raw bytes, truncated to a signed int4 because
    ``pg_advisory_xact_lock(int4, int4)`` takes int4s.

    A collision between two DIFFERENT agents is SAFE: it only makes those two
    agents serialize their new-namespace creations against each other for the
    few statements the lock is held. It can never produce a wrong verdict,
    because every query in the critical section -- the existence probe, the
    re-check, and the ``count(distinct namespace)`` -- is still filtered by
    ``agent_id``.
    """
    digest = hashlib.blake2b(agent_id.bytes, digest_size=4).digest()
    return int.from_bytes(digest, "big", signed=True)


async def _namespace_exists(
    session: AsyncSession, agent_id: uuid.UUID, scope: str | None, namespace: str
) -> bool:
    """Does this agent already have any row in ``namespace``? (#933)

    Extracted so the unlocked pre-check and the re-check under the advisory lock
    are provably the same query; a future edit cannot let them drift apart.
    """
    found = await session.scalar(
        select(WorkflowStateEntry.namespace)
        .where(
            WorkflowStateEntry.agent_id == agent_id,
            WorkflowStateEntry.binding_scope == scope,
            WorkflowStateEntry.namespace == namespace,
        )
        .limit(1)
    )
    return found is not None


async def _enforce_caps(
    session: AsyncSession,
    agent_id: uuid.UUID,
    scope: str | None,
    namespace: str,
    key: str,
    value: Any,
    *,
    reserve_bytes: int | None = None,
) -> None:
    """Reject a write that breaks the per-value or per-namespace size cap (#248).

    ``reserve_bytes`` (#2927) additionally refuses a value that fits the
    per-value cap but leaves fewer than that many bytes free under it. The
    runner's transcript appends set it so the worker's publication outcome
    append always has room; the refusal is the runner's compaction trigger, not
    a persistence failure, so it records no failure metric.

    The namespace total counts the incoming value plus every *other* key already
    in the namespace (the key being written replaces its own prior size). Both
    the byte totals and the namespace-count cap below are scoped by `scope`
    (#1525 follow-up): a memory=False agent's bindings are meant to be
    isolated, so one binding filling its own namespace or hitting the
    namespace-count cap must not block or inflate another's unrelated usage.
    """
    settings = get_settings()
    value_bytes = _json_size(value)
    if value_bytes > settings.state_max_value_bytes:
        raise HTTPException(
            413,
            f"value for key {key!r} is {value_bytes} bytes, over the "
            f"{settings.state_max_value_bytes}-byte per-value cap",
        )
    if reserve_bytes is not None and settings.state_max_value_bytes - value_bytes < reserve_bytes:
        raise HTTPException(
            413,
            f"value for key {key!r} is {value_bytes} bytes, leaving under the "
            f"{reserve_bytes}-byte reserve of the "
            f"{settings.state_max_value_bytes}-byte per-value cap",
        )

    # Cap unit is compact json.dumps (ensure_ascii=True). jsonb::text is not
    # byte-identical: object whitespace makes it an *upper* bound for ASCII
    # JSON, but UTF-8 output is shorter than ensure_ascii escapes, so a
    # multibyte sibling must take the exact fallback. The aggregates return
    # two scalars; fetching sibling values is only the over-bound path.
    sibling_filter = (
        WorkflowStateEntry.agent_id == agent_id,
        WorkflowStateEntry.binding_scope == scope,
        WorkflowStateEntry.namespace == namespace,
        WorkflowStateEntry.key != key,
    )
    sibling_text = (
        select(cast(WorkflowStateEntry.value, Text).label("json_text"))
        .where(*sibling_filter)
        .subquery()
    )
    sibling_bound, has_multibyte = (
        await session.execute(
            select(
                func.coalesce(func.sum(func.octet_length(sibling_text.c.json_text)), 0),
                func.coalesce(
                    func.bool_or(
                        func.octet_length(sibling_text.c.json_text)
                        != func.length(sibling_text.c.json_text)
                    ),
                    False,
                ),
            )
        )
    ).one()
    sibling_bound_bytes = int(sibling_bound or 0)
    if bool(has_multibyte) or (
        value_bytes + sibling_bound_bytes > settings.state_max_namespace_bytes
    ):
        others = await session.execute(
            select(WorkflowStateEntry.key, WorkflowStateEntry.value).where(*sibling_filter)
        )
        sizes = {key: value_bytes}
        sizes.update((other_key, _json_size(v)) for other_key, v in others)
        namespace_bytes = sum(sizes.values())
        if namespace_bytes > settings.state_max_namespace_bytes:
            # Name the key holding the most bytes (#2820), which is often not
            # the key whose write was refused.
            largest = max(sizes, key=lambda k: (sizes[k], k == key))
            raise HTTPException(
                413,
                f"namespace {namespace!r} would be {namespace_bytes} bytes, over the "
                f"{settings.state_max_namespace_bytes}-byte per-namespace cap; "
                f"largest key {largest!r} is {sizes[largest]} bytes",
            )

    # Per-agent namespace-count cap (#852): refuse only a NEW namespace, and only
    # once the agent is at its limit -- writes to an existing namespace never hit
    # this. Without it a sandbox could loop creating unbounded namespaces, each
    # under the byte caps, after #840 made the namespace agent-chosen.
    #
    # #933: the check and the caller's INSERT are separate statements, so the
    # bare check was a TOCTOU a concurrent burst could walk straight past (N
    # requests to N brand-new namespaces all read cap-1 and all pass). The guard
    # below is DOUBLE-CHECKED LOCKING: an unlocked pre-check keeps the hot path
    # free, and only a would-be namespace CREATION serializes on a per-agent
    # advisory lock held to COMMIT/ROLLBACK.

    # 1. Hot path. Every write into an already-existing namespace returns here,
    #    at exactly the cost of the single probe this code ran before #933 --
    #    no lock, no extra round trip. dadf93e2's "writes to an existing
    #    namespace are unaffected" is load-bearing and is preserved literally.
    if await _namespace_exists(session, agent_id, scope, namespace):
        return

    # 2. Serialize the creation. Transaction-level, so it is released by the
    #    COMMIT or the ROLLBACK with no explicit unlock and no leak path when
    #    the 403 below propagates. This REQUIRES a transaction to already be
    #    open -- outside one the lock would be released immediately and this
    #    guard would be vacuous. The byte-cap SQL bound SELECT above runs
    #    unconditionally and autobegins it; moving this block above those
    #    queries would silently make the lock meaningless.
    #
    #    LOCK ORDERING (no deadlock cycle exists, and this is what a future
    #    change would break): the advisory lock is only ever requested when the
    #    namespace has no rows for this agent, so no ``SELECT ... FOR UPDATE``
    #    row lock is held at that moment -- in ``append_state`` and in
    #    ``routers/memory.py`` a FOR UPDATE that matches zero rows takes no
    #    lock, and any caller that DOES hold a row lock is by definition writing
    #    an already-existing namespace and returned at step 1. The order is
    #    strictly one-way, advisory lock -> row locks. Moving a FOR UPDATE above
    #    the existence check, or making this lock unconditional, reopens that
    #    analysis from scratch.
    await session.execute(
        text("SELECT pg_advisory_xact_lock(:cls, :key)"),
        {"cls": _NAMESPACE_LOCK_CLASS, "key": _namespace_lock_key(agent_id)},
    )

    # 3. Re-check under the lock. A sibling request may have created this exact
    #    namespace while we waited; refusing it then would be a FALSE POSITIVE
    #    -- two concurrent writes to the same brand-new namespace must both
    #    succeed when the agent has room. Not optional.
    #
    #    READ COMMITTED DEPENDENCY: this re-check and the count below only see
    #    the sibling's commit because READ COMMITTED gives each statement a
    #    fresh snapshot. Under REPEATABLE READ or SERIALIZABLE the snapshot
    #    predates that commit and this guard degrades SILENTLY -- no error, just
    #    the old overshoot plus a spurious 403 here. Nothing sets an isolation
    #    level today; changing that breaks this.
    if await _namespace_exists(session, agent_id, scope, namespace):
        return

    namespace_count = await session.scalar(
        select(func.count(func.distinct(WorkflowStateEntry.namespace))).where(
            WorkflowStateEntry.agent_id == agent_id,
            WorkflowStateEntry.binding_scope == scope,
        )
    )
    if (namespace_count or 0) >= settings.state_max_namespaces:
        raise HTTPException(
            status.HTTP_403_FORBIDDEN,
            f"agent is at its {settings.state_max_namespaces}-namespace cap; "
            f"delete a namespace or reuse an existing one before creating "
            f"{namespace!r}",
        )


def _transcript_out(row: ThreadTranscript) -> StateEntryOut:
    """A transcript row in the state API's entry shape (ADR-0170)."""
    return StateEntryOut(
        namespace=TRANSCRIPT_NAMESPACE,
        key=row.thread_key,
        value=row.value,
        version=row.version,
        updated_at=row.updated_at,
    )


async def _get_entry(
    session: AsyncSession, agent_id: uuid.UUID, scope: str | None, namespace: str, key: str
) -> WorkflowStateEntry | None:
    entry: WorkflowStateEntry | None = await session.scalar(
        select(WorkflowStateEntry).where(
            WorkflowStateEntry.agent_id == agent_id,
            WorkflowStateEntry.binding_scope == scope,
            WorkflowStateEntry.namespace == namespace,
            WorkflowStateEntry.key == key,
        )
    )
    return entry


async def _get_entry_locked(
    session: AsyncSession, agent_id: uuid.UUID, scope: str | None, namespace: str, key: str
) -> WorkflowStateEntry | None:
    """Same lookup as ``_get_entry``, row-locked for a read-modify-write (#2927).

    Used by both a CAS put and an append: a plain read let a concurrent write
    commit between the version check and the write, and the later write then
    overwrote it.
    """

    entry: WorkflowStateEntry | None = await session.scalar(
        select(WorkflowStateEntry)
        .where(
            WorkflowStateEntry.agent_id == agent_id,
            WorkflowStateEntry.binding_scope == scope,
            WorkflowStateEntry.namespace == namespace,
            WorkflowStateEntry.key == key,
        )
        .with_for_update()
    )
    return entry


async def _put_state(
    agent_id: uuid.UUID,
    scope: str | None,
    namespace: str,
    key: str,
    data: StateEntryPut,
    session: AsyncSession,
) -> StateEntryOut:
    # Unknown agent is a 404 (the FK would also reject, but this is the clear
    # signal). expected_version opts into compare-and-set.
    if await crud.get_agent(session, agent_id) is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "agent not found")
    if namespace == TRANSCRIPT_NAMESPACE:
        row = await transcripts.put(
            session, agent_id, scope, key, data.value, data.expected_version
        )
        return _transcript_out(row)
    await _enforce_caps(session, agent_id, scope, namespace, key, data.value)
    entry = await _get_entry_locked(session, agent_id, scope, namespace, key)
    if entry is None:
        if data.expected_version is not None:
            # A CAS put that expects a prior version cannot create the entry.
            raise HTTPException(
                status.HTTP_409_CONFLICT,
                "version mismatch: entry does not exist yet",
            )
        entry = WorkflowStateEntry(
            agent_id=agent_id, binding_scope=scope, namespace=namespace, key=key, value=data.value
        )
        session.add(entry)
    else:
        if data.expected_version is not None and data.expected_version != entry.version:
            raise HTTPException(
                status.HTTP_409_CONFLICT,
                f"version mismatch: expected {data.expected_version}, "
                f"stored {entry.version}",
            )
        entry.value = data.value
        entry.version += 1
    await session.commit()
    await session.refresh(entry)
    return StateEntryOut.model_validate(entry)


@router.put(
    "/{agent_id}/state/{namespace}/{key}",
    response_model=StateEntryOut,
    dependencies=[Depends(forbid_reserved_namespace)],
)
async def put_state(
    agent_id: uuid.UUID, namespace: str, key: str, data: StateEntryPut, session: SessionDep
) -> StateEntryOut:
    return await _put_state(agent_id, None, namespace, key, data, session)


@router.put(
    "/{agent_id}/state/bindings/{kind}/{address}/{namespace}/{key}",
    response_model=StateEntryOut,
    dependencies=[Depends(forbid_reserved_namespace)],
)
async def put_state_for_binding(
    agent_id: uuid.UUID,
    kind: str,
    address: str,
    namespace: str,
    key: str,
    data: StateEntryPut,
    session: SessionDep,
) -> StateEntryOut:
    scope = await _binding_scope(session, agent_id, kind, address)
    return await _put_state(agent_id, scope, namespace, key, data, session)


async def _append_state(
    agent_id: uuid.UUID,
    scope: str | None,
    namespace: str,
    key: str,
    data: StateAppendIn,
    session: AsyncSession,
) -> StateEntryOut:
    """Append an item to a log-shaped (JSON array) entry (#248).

    Creates the entry as a single-element array if absent; otherwise the stored
    value must already be an array, else the append is a 409. Subject to the
    same per-value and per-namespace size caps as a put.
    """
    if await crud.get_agent(session, agent_id) is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "agent not found")
    if namespace == TRANSCRIPT_NAMESPACE:
        row = await transcripts.append(
            session, agent_id, scope, key, data.item, data.reserve_bytes
        )
        return _transcript_out(row)
    entry = await _get_entry_locked(session, agent_id, scope, namespace, key)
    if entry is None:
        new_value = [data.item]
        await _enforce_caps(
            session, agent_id, scope, namespace, key, new_value, reserve_bytes=data.reserve_bytes
        )
        entry = WorkflowStateEntry(
            agent_id=agent_id, binding_scope=scope, namespace=namespace, key=key, value=new_value
        )
        session.add(entry)
    else:
        if not isinstance(entry.value, list):
            raise HTTPException(
                status.HTTP_409_CONFLICT,
                "cannot append: stored value is not a JSON array",
            )
        new_value = [*entry.value, data.item]
        await _enforce_caps(
            session, agent_id, scope, namespace, key, new_value, reserve_bytes=data.reserve_bytes
        )
        entry.value = new_value
        entry.version += 1
    await session.commit()
    await session.refresh(entry)
    return StateEntryOut.model_validate(entry)


@router.post(
    "/{agent_id}/state/{namespace}/{key}/append",
    response_model=StateEntryOut,
    dependencies=[Depends(forbid_reserved_namespace)],
)
async def append_state(
    agent_id: uuid.UUID, namespace: str, key: str, data: StateAppendIn, session: SessionDep
) -> StateEntryOut:
    return await _append_state(agent_id, None, namespace, key, data, session)


@router.post(
    "/{agent_id}/state/bindings/{kind}/{address}/{namespace}/{key}/append",
    response_model=StateEntryOut,
    dependencies=[Depends(forbid_reserved_namespace)],
)
async def append_state_for_binding(
    agent_id: uuid.UUID,
    kind: str,
    address: str,
    namespace: str,
    key: str,
    data: StateAppendIn,
    session: SessionDep,
) -> StateEntryOut:
    scope = await _binding_scope(session, agent_id, kind, address)
    return await _append_state(agent_id, scope, namespace, key, data, session)


async def _list_namespaces(
    agent_id: uuid.UUID,
    scope: str | None,
    session: AsyncSession,
    caller: StateCaller,
) -> list[StateNamespaceOut]:
    """List the namespaces stored under one scope, each with its key count and
    the most recent write time (#250). This is the enumeration the operator's
    read/inspect surface needs on top of get-by-key + list-by-namespace; it stays
    within the store's non-goals (no query language, just a grouped summary).
    Namespaces are returned most-recently-written first.

    This route has no ``namespace`` path param, so ``forbid_reserved_namespace``
    cannot gate it; instead the reserved namespaces are filtered out for the
    narrow app (bundle) token (#856), the enumeration equivalent of that guard.
    Which SCOPE this lists is entirely a function of which URL was called
    (#1525 follow-up) -- the plain path always lists the shared scope, the
    ``/bindings/{kind}/{address}`` path always lists exactly that binding's,
    for every caller alike; an operator wanting the full picture of a
    memory=False agent calls once per binding, the same way its own bundle
    code only ever sees the one scope the worker handed it.
    """
    query = (
        select(
            WorkflowStateEntry.namespace,
            func.count().label("key_count"),
            func.max(WorkflowStateEntry.updated_at).label("last_updated"),
        )
        .where(WorkflowStateEntry.agent_id == agent_id, WorkflowStateEntry.binding_scope == scope)
        .group_by(WorkflowStateEntry.namespace)
        .order_by(func.max(WorkflowStateEntry.updated_at).desc())
    )
    rows = await session.execute(query)
    listed = [
        StateNamespaceOut(
            namespace=row.namespace,
            key_count=row.key_count,
            last_updated=row.last_updated,
        )
        for row in rows
        # The narrow app (bundle) token must not even learn the reserved
        # namespaces exist -- their key counts and write times are exactly what
        # the state.app scope fences off (#856).
        if not (caller is StateCaller.APP and row.namespace in RESERVED_NAMESPACES)
    ]
    # Transcripts live in their own table (ADR-0170) but are still listed here
    # as the reserved namespace the operator's inspector already knows.
    if caller is not StateCaller.APP:
        threads = await transcripts.summary(session, agent_id, scope)
        if threads is not None:
            listed.append(
                StateNamespaceOut(
                    namespace=TRANSCRIPT_NAMESPACE,
                    key_count=threads[0],
                    last_updated=threads[1],
                )
            )
            listed.sort(key=lambda row: row.last_updated, reverse=True)
    return listed


@router.get("/{agent_id}/state", response_model=list[StateNamespaceOut])
async def list_namespaces(
    agent_id: uuid.UUID,
    session: SessionDep,
    caller: Annotated[StateCaller, Depends(require_state_access)],
) -> list[StateNamespaceOut]:
    return await _list_namespaces(agent_id, None, session, caller)


@router.get("/{agent_id}/state/bindings/{kind}/{address}", response_model=list[StateNamespaceOut])
async def list_namespaces_for_binding(
    agent_id: uuid.UUID,
    kind: str,
    address: str,
    session: SessionDep,
    caller: Annotated[StateCaller, Depends(require_state_access)],
) -> list[StateNamespaceOut]:
    scope = await _binding_scope(session, agent_id, kind, address)
    return await _list_namespaces(agent_id, scope, session, caller)


async def _get_state(
    agent_id: uuid.UUID, scope: str | None, namespace: str, key: str, session: AsyncSession
) -> StateEntryOut:
    if namespace == TRANSCRIPT_NAMESPACE:
        row = await transcripts.get(session, agent_id, scope, key)
        if row is None:
            raise HTTPException(status.HTTP_404_NOT_FOUND, "state entry not found")
        return _transcript_out(row)
    entry = await _get_entry(session, agent_id, scope, namespace, key)
    if entry is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "state entry not found")
    return StateEntryOut.model_validate(entry)


@router.get(
    "/{agent_id}/state/{namespace}/{key}",
    response_model=StateEntryOut,
    dependencies=[Depends(forbid_reserved_namespace)],
)
async def get_state(
    agent_id: uuid.UUID, namespace: str, key: str, session: SessionDep
) -> StateEntryOut:
    return await _get_state(agent_id, None, namespace, key, session)


@router.get(
    "/{agent_id}/state/bindings/{kind}/{address}/{namespace}/{key}",
    response_model=StateEntryOut,
    dependencies=[Depends(forbid_reserved_namespace)],
)
async def get_state_for_binding(
    agent_id: uuid.UUID, kind: str, address: str, namespace: str, key: str, session: SessionDep
) -> StateEntryOut:
    scope = await _binding_scope(session, agent_id, kind, address)
    return await _get_state(agent_id, scope, namespace, key, session)


async def _list_state(
    agent_id: uuid.UUID, scope: str | None, namespace: str, session: AsyncSession
) -> list[StateEntryOut]:
    # Which scope this lists is a function of which URL was called, same as
    # _list_namespaces above -- uniform for every caller, no caller-type
    # branching here either.
    if namespace == TRANSCRIPT_NAMESPACE:
        rows = await transcripts.list_threads(session, agent_id, scope)
        return [_transcript_out(row) for row in rows]
    query = select(WorkflowStateEntry).where(
        WorkflowStateEntry.agent_id == agent_id,
        WorkflowStateEntry.binding_scope == scope,
        WorkflowStateEntry.namespace == namespace,
    )
    entries = await session.scalars(query.order_by(WorkflowStateEntry.key))
    return [StateEntryOut.model_validate(e) for e in entries]


@router.get(
    "/{agent_id}/state/{namespace}",
    response_model=list[StateEntryOut],
    dependencies=[Depends(forbid_reserved_namespace)],
)
async def list_state(
    agent_id: uuid.UUID, namespace: str, session: SessionDep
) -> list[StateEntryOut]:
    return await _list_state(agent_id, None, namespace, session)


@router.get(
    "/{agent_id}/state/bindings/{kind}/{address}/{namespace}",
    response_model=list[StateEntryOut],
    dependencies=[Depends(forbid_reserved_namespace)],
)
async def list_state_for_binding(
    agent_id: uuid.UUID, kind: str, address: str, namespace: str, session: SessionDep
) -> list[StateEntryOut]:
    scope = await _binding_scope(session, agent_id, kind, address)
    return await _list_state(agent_id, scope, namespace, session)


async def _delete_state(
    agent_id: uuid.UUID,
    scope: str | None,
    namespace: str,
    key: str,
    expected_version: int | None,
    session: AsyncSession,
) -> Response:
    # expected_version opts into compare-and-delete (#2820), so an operator
    # that exported a transcript never deletes turns appended after the export.
    # The version is a predicate of the DELETE itself, so an append that
    # commits between the read and the delete cannot be removed with it.
    if namespace == TRANSCRIPT_NAMESPACE:
        await transcripts.remove(session, agent_id, scope, key, expected_version)
        return Response(status_code=status.HTTP_204_NO_CONTENT)
    entry = await _get_entry(session, agent_id, scope, namespace, key)
    if expected_version is None:
        if entry is not None:
            await session.delete(entry)
            await session.commit()
    else:
        stored = entry.version if entry is not None else None
        deleted = None
        if stored == expected_version and entry is not None:
            deleted = await session.scalar(
                delete(WorkflowStateEntry)
                .where(
                    WorkflowStateEntry.id == entry.id,
                    WorkflowStateEntry.version == expected_version,
                )
                .returning(WorkflowStateEntry.id)
            )
            await session.commit()
        if deleted is None:
            if stored is None:
                found = "entry does not exist"
            elif stored != expected_version:
                found = f"stored {stored}"
            else:
                found = "entry changed during the delete"
            raise HTTPException(
                status.HTTP_409_CONFLICT,
                f"version mismatch: expected {expected_version}, {found}",
            )
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.delete(
    "/{agent_id}/state/{namespace}/{key}",
    status_code=status.HTTP_204_NO_CONTENT,
    dependencies=[Depends(forbid_reserved_namespace)],
)
async def delete_state(
    agent_id: uuid.UUID,
    namespace: str,
    key: str,
    session: SessionDep,
    expected_version: int | None = None,
) -> Response:
    return await _delete_state(agent_id, None, namespace, key, expected_version, session)


@router.delete(
    "/{agent_id}/state/bindings/{kind}/{address}/{namespace}/{key}",
    status_code=status.HTTP_204_NO_CONTENT,
    dependencies=[Depends(forbid_reserved_namespace)],
)
async def delete_state_for_binding(
    agent_id: uuid.UUID,
    kind: str,
    address: str,
    namespace: str,
    key: str,
    session: SessionDep,
    expected_version: int | None = None,
) -> Response:
    scope = await _binding_scope(session, agent_id, kind, address)
    return await _delete_state(agent_id, scope, namespace, key, expected_version, session)
