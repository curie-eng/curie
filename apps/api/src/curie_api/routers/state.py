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
import json
import uuid
from typing import Annotated, Any

from fastapi import APIRouter, Depends, Header, HTTPException, Response, status
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from .. import crud, sandbox_token
from ..auth import verify_platform_key
from ..config import get_settings
from ..deps import SessionDep
from ..models import WorkflowStateEntry
from ..schemas import StateAppendIn, StateEntryOut, StateEntryPut, StateNamespaceOut

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
RESERVED_NAMESPACES = frozenset({"memory", "transcript"})


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
    raise HTTPException(status.HTTP_401_UNAUTHORIZED, detail="missing or invalid credential")


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


router = APIRouter(prefix="/agents", tags=["state"], dependencies=[Depends(require_state_access)])


def _json_size(value: Any) -> int:
    """Serialized-JSON byte length, the unit both size caps are measured in."""
    return len(json.dumps(value, separators=(",", ":")).encode("utf-8"))


async def _enforce_caps(
    session: AsyncSession,
    agent_id: uuid.UUID,
    namespace: str,
    key: str,
    value: Any,
) -> None:
    """Reject a write that breaks the per-value or per-namespace size cap (#248).

    The namespace total counts the incoming value plus every *other* key already
    in the namespace (the key being written replaces its own prior size).
    """
    settings = get_settings()
    value_bytes = _json_size(value)
    if value_bytes > settings.state_max_value_bytes:
        raise HTTPException(
            413,
            f"value is {value_bytes} bytes, over the "
            f"{settings.state_max_value_bytes}-byte per-value cap",
        )

    others = await session.scalars(
        select(WorkflowStateEntry.value).where(
            WorkflowStateEntry.agent_id == agent_id,
            WorkflowStateEntry.namespace == namespace,
            WorkflowStateEntry.key != key,
        )
    )
    namespace_bytes = value_bytes + sum(_json_size(v) for v in others)
    if namespace_bytes > settings.state_max_namespace_bytes:
        raise HTTPException(
            413,
            f"namespace {namespace!r} would be {namespace_bytes} bytes, over the "
            f"{settings.state_max_namespace_bytes}-byte per-namespace cap",
        )

    # Per-agent namespace-count cap (#852): refuse only a NEW namespace, and only
    # once the agent is at its limit -- writes to an existing namespace never hit
    # this. Without it a sandbox could loop creating unbounded namespaces, each
    # under the byte caps, after #840 made the namespace agent-chosen.
    namespace_exists = await session.scalar(
        select(WorkflowStateEntry.namespace)
        .where(
            WorkflowStateEntry.agent_id == agent_id,
            WorkflowStateEntry.namespace == namespace,
        )
        .limit(1)
    )
    if namespace_exists is None:
        namespace_count = await session.scalar(
            select(func.count(func.distinct(WorkflowStateEntry.namespace))).where(
                WorkflowStateEntry.agent_id == agent_id
            )
        )
        if (namespace_count or 0) >= settings.state_max_namespaces:
            raise HTTPException(
                status.HTTP_403_FORBIDDEN,
                f"agent is at its {settings.state_max_namespaces}-namespace cap; "
                f"delete a namespace or reuse an existing one before creating "
                f"{namespace!r}",
            )


async def _get_entry(
    session: AsyncSession, agent_id: uuid.UUID, namespace: str, key: str
) -> WorkflowStateEntry | None:
    entry: WorkflowStateEntry | None = await session.scalar(
        select(WorkflowStateEntry).where(
            WorkflowStateEntry.agent_id == agent_id,
            WorkflowStateEntry.namespace == namespace,
            WorkflowStateEntry.key == key,
        )
    )
    return entry


@router.put(
    "/{agent_id}/state/{namespace}/{key}",
    response_model=StateEntryOut,
    dependencies=[Depends(forbid_reserved_namespace)],
)
async def put_state(
    agent_id: uuid.UUID,
    namespace: str,
    key: str,
    data: StateEntryPut,
    session: SessionDep,
) -> StateEntryOut:
    # Unknown agent is a 404 (the FK would also reject, but this is the clear
    # signal). expected_version opts into compare-and-set.
    if await crud.get_agent(session, agent_id) is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "agent not found")
    await _enforce_caps(session, agent_id, namespace, key, data.value)
    entry = await _get_entry(session, agent_id, namespace, key)
    if entry is None:
        if data.expected_version is not None:
            # A CAS put that expects a prior version cannot create the entry.
            raise HTTPException(
                status.HTTP_409_CONFLICT,
                "version mismatch: entry does not exist yet",
            )
        entry = WorkflowStateEntry(
            agent_id=agent_id, namespace=namespace, key=key, value=data.value
        )
        session.add(entry)
    else:
        if data.expected_version is not None and data.expected_version != entry.version:
            raise HTTPException(
                status.HTTP_409_CONFLICT,
                f"version mismatch: expected {data.expected_version}, stored {entry.version}",
            )
        entry.value = data.value
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
    agent_id: uuid.UUID,
    namespace: str,
    key: str,
    data: StateAppendIn,
    session: SessionDep,
) -> StateEntryOut:
    """Append an item to a log-shaped (JSON array) entry (#248).

    Creates the entry as a single-element array if absent; otherwise the stored
    value must already be an array, else the append is a 409. Subject to the
    same per-value and per-namespace size caps as a put.
    """
    if await crud.get_agent(session, agent_id) is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "agent not found")
    entry: WorkflowStateEntry | None = await session.scalar(
        select(WorkflowStateEntry)
        .where(
            WorkflowStateEntry.agent_id == agent_id,
            WorkflowStateEntry.namespace == namespace,
            WorkflowStateEntry.key == key,
        )
        .with_for_update()
    )
    if entry is None:
        new_value = [data.item]
        await _enforce_caps(session, agent_id, namespace, key, new_value)
        entry = WorkflowStateEntry(agent_id=agent_id, namespace=namespace, key=key, value=new_value)
        session.add(entry)
    else:
        if not isinstance(entry.value, list):
            raise HTTPException(
                status.HTTP_409_CONFLICT,
                "cannot append: stored value is not a JSON array",
            )
        new_value = [*entry.value, data.item]
        await _enforce_caps(session, agent_id, namespace, key, new_value)
        entry.value = new_value
        entry.version += 1
    await session.commit()
    await session.refresh(entry)
    return StateEntryOut.model_validate(entry)


@router.get("/{agent_id}/state", response_model=list[StateNamespaceOut])
async def list_namespaces(
    agent_id: uuid.UUID,
    session: SessionDep,
    caller: Annotated[StateCaller, Depends(require_state_access)],
) -> list[StateNamespaceOut]:
    """List the namespaces an agent has stored, each with its key count and the
    most recent write time (#250). This is the enumeration the operator's
    read/inspect surface needs on top of get-by-key + list-by-namespace; it stays
    within the store's non-goals (no query language, just a grouped summary).
    Namespaces are returned most-recently-written first.

    This route has no ``namespace`` path param, so ``forbid_reserved_namespace``
    cannot gate it; instead the reserved namespaces are filtered out for the
    narrow app (bundle) token (#856), the enumeration equivalent of that guard.
    The platform key (the UI inspector) and the broad ``state`` loaders keep full
    reach. ``require_state_access`` is a router-level dependency, so re-declaring
    it here only surfaces the already-resolved caller (FastAPI caches it).
    """
    rows = await session.execute(
        select(
            WorkflowStateEntry.namespace,
            func.count().label("key_count"),
            func.max(WorkflowStateEntry.updated_at).label("last_updated"),
        )
        .where(WorkflowStateEntry.agent_id == agent_id)
        .group_by(WorkflowStateEntry.namespace)
        .order_by(func.max(WorkflowStateEntry.updated_at).desc())
    )
    return [
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


@router.get(
    "/{agent_id}/state/{namespace}/{key}",
    response_model=StateEntryOut,
    dependencies=[Depends(forbid_reserved_namespace)],
)
async def get_state(
    agent_id: uuid.UUID, namespace: str, key: str, session: SessionDep
) -> StateEntryOut:
    entry = await _get_entry(session, agent_id, namespace, key)
    if entry is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "state entry not found")
    return StateEntryOut.model_validate(entry)


@router.get(
    "/{agent_id}/state/{namespace}",
    response_model=list[StateEntryOut],
    dependencies=[Depends(forbid_reserved_namespace)],
)
async def list_state(
    agent_id: uuid.UUID, namespace: str, session: SessionDep
) -> list[StateEntryOut]:
    entries = await session.scalars(
        select(WorkflowStateEntry)
        .where(
            WorkflowStateEntry.agent_id == agent_id,
            WorkflowStateEntry.namespace == namespace,
        )
        .order_by(WorkflowStateEntry.key)
    )
    return [StateEntryOut.model_validate(e) for e in entries]


@router.delete(
    "/{agent_id}/state/{namespace}/{key}",
    status_code=status.HTTP_204_NO_CONTENT,
    dependencies=[Depends(forbid_reserved_namespace)],
)
async def delete_state(
    agent_id: uuid.UUID, namespace: str, key: str, session: SessionDep
) -> Response:
    entry = await _get_entry(session, agent_id, namespace, key)
    if entry is not None:
        await session.delete(entry)
        await session.commit()
    return Response(status_code=status.HTTP_204_NO_CONTENT)
