"""Agent memory API: inspect, trace-back, seed, edit, and delete what an agent learned.

Memory is a scoped namespace over the durable state store (#264, ADR-0025): the
runner writes an append-only log at namespace ``memory`` key ``log``, each item a
``{content, provenance}`` record, reloaded at the next session boot. This router
is the operator's read/write surface over that one log key -- it does NOT add a
second store. #266 adds the learned-from trace-back (resolve an entry's session +
source traces); #267 adds edit/delete (an edit preserves the entry's provenance;
a delete removes exactly one entry); #1904 adds operator seed (append a validated
record with operator provenance). Because the runner rehydrates from the same
key, creates, edits, and deletes are reflected at the next boot.
"""

import uuid
from datetime import UTC, datetime
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query, Response, status
from sqlalchemy import select

from .. import crud
from ..auth import require_api_key
from ..config import get_settings
from ..deps import SessionDep
from ..memory_guidance import DEFAULT_MEMORY_GUIDANCE
from ..models import WorkflowStateEntry
from ..schemas import (
    MemoryEntryCreate,
    MemoryEntryEdit,
    MemoryEntryOut,
    MemoryGuidanceIn,
    MemoryGuidanceOut,
    MemoryProvenanceOut,
    MemoryTraceBackOut,
    SourceTraceOut,
)
from .state import _enforce_caps

router = APIRouter(
    prefix="/agents", tags=["memory"], dependencies=[Depends(require_api_key)]
)

# The runner writes memory here (mirrors runner/curie_runner/memory.py: a
# single log-shaped key inside the reserved ``memory`` namespace).
MEMORY_NAMESPACE = "memory"
MEMORY_LOG_KEY = "log"
# Operator guidance for the runner's memory tools (#1461): ``{"text": ...}`` at
# the agent-wide scope, next to the log. Never a memory entry itself.
MEMORY_GUIDANCE_KEY = "guidance"
# Provenance ``source`` stamped on operator-seeded records (#1904). Distinct from
# session-learned entries, which omit the field or leave it null.
OPERATOR_MEMORY_SOURCE = "operator"


async def _require_agent(session: SessionDep, agent_id: uuid.UUID) -> None:
    if await crud.get_agent(session, agent_id) is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "agent not found")


async def _get_log_entry(
    session: SessionDep, agent_id: uuid.UUID
) -> WorkflowStateEntry | None:
    entry: WorkflowStateEntry | None = await session.scalar(
        select(WorkflowStateEntry).where(
            WorkflowStateEntry.agent_id == agent_id,
            WorkflowStateEntry.namespace == MEMORY_NAMESPACE,
            WorkflowStateEntry.key == MEMORY_LOG_KEY,
        )
    )
    return entry


def _records_from_value(value: Any) -> list[dict[str, Any]]:
    """The log's ``{content, provenance}`` records, or [] when absent/malformed."""
    if not isinstance(value, list):
        return []
    return [r for r in value if isinstance(r, dict) and "content" in r]


def _records_of(entry: WorkflowStateEntry | None) -> list[dict[str, Any]]:
    """The log's ``{content, provenance}`` records, or [] when absent/malformed."""
    if entry is None:
        return []
    return _records_from_value(entry.value)


def _operator_record(content: str) -> dict[str, Any]:
    """One operator-authored log item. Provenance is server-stamped, never trusted."""
    return {
        "content": content,
        "provenance": {
            "learned_from_session_id": None,
            "source_trace_ids": [],
            "recorded_at": datetime.now(UTC).isoformat(),
            "source": OPERATOR_MEMORY_SOURCE,
        },
    }


async def _locked_log(
    session: SessionDep, agent_id: uuid.UUID, expected_version: int
) -> tuple[WorkflowStateEntry, list[dict[str, Any]]]:
    """Load and lock the memory log, then validate its expected parent version."""
    await _require_agent(session, agent_id)
    entry: WorkflowStateEntry | None = await session.scalar(
        select(WorkflowStateEntry)
        .where(
            WorkflowStateEntry.agent_id == agent_id,
            WorkflowStateEntry.namespace == MEMORY_NAMESPACE,
            WorkflowStateEntry.key == MEMORY_LOG_KEY,
        )
        .with_for_update()
    )
    if entry is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "memory entry not found")
    if expected_version != entry.version:
        raise HTTPException(
            status.HTTP_409_CONFLICT,
            f"version mismatch: expected {expected_version}, stored {entry.version}",
        )
    return entry, _records_of(entry)


def _provenance_of(record: dict[str, Any]) -> dict[str, Any]:
    prov = record.get("provenance")
    return prov if isinstance(prov, dict) else {}


def _to_out(index: int, record: dict[str, Any], version: int) -> MemoryEntryOut:
    return MemoryEntryOut(
        index=index,
        content=str(record.get("content", "")),
        provenance=MemoryProvenanceOut(**_provenance_of(record)),
        version=version,
    )


def _trace_url(trace_id: str) -> str:
    """A Langfuse deep link for a source trace id (the trace-back target)."""
    base = get_settings().langfuse_host.rstrip("/")
    return f"{base}/trace/{trace_id}"


@router.get("/{agent_id}/memory", response_model=list[MemoryEntryOut])
async def list_memory(agent_id: uuid.UUID, session: SessionDep) -> list[MemoryEntryOut]:
    """List an agent's learned memory entries, oldest first, with provenance."""
    await _require_agent(session, agent_id)
    entry = await _get_log_entry(session, agent_id)
    if entry is None:
        return []
    return [_to_out(i, r, entry.version) for i, r in enumerate(_records_of(entry))]


@router.post(
    "/{agent_id}/memory",
    response_model=MemoryEntryOut,
    status_code=status.HTTP_201_CREATED,
)
async def create_memory(
    agent_id: uuid.UUID, data: MemoryEntryCreate, session: SessionDep
) -> MemoryEntryOut:
    """Append an operator-authored memory record (#1904).

    Creates the ``memory/log`` key when absent, otherwise appends under the same
    row lock the state-append path uses. Provenance is stamped here so a caller
    cannot claim a session or traces they did not produce. The runner rehydrates
    the log at the next session boot; a live thread keeps the memory it booted
    with.
    """
    await _require_agent(session, agent_id)
    record = _operator_record(data.content)
    entry: WorkflowStateEntry | None = await session.scalar(
        select(WorkflowStateEntry)
        .where(
            WorkflowStateEntry.agent_id == agent_id,
            WorkflowStateEntry.namespace == MEMORY_NAMESPACE,
            WorkflowStateEntry.key == MEMORY_LOG_KEY,
        )
        .with_for_update()
    )
    if entry is None:
        new_value = [record]
        await _enforce_caps(
            session, agent_id, None, MEMORY_NAMESPACE, MEMORY_LOG_KEY, new_value
        )
        entry = WorkflowStateEntry(
            agent_id=agent_id,
            namespace=MEMORY_NAMESPACE,
            key=MEMORY_LOG_KEY,
            value=new_value,
        )
        session.add(entry)
    else:
        if not isinstance(entry.value, list):
            raise HTTPException(
                status.HTTP_409_CONFLICT,
                "cannot append: stored value is not a JSON array",
            )
        new_value = [*entry.value, record]
        await _enforce_caps(
            session, agent_id, None, MEMORY_NAMESPACE, MEMORY_LOG_KEY, new_value
        )
        entry.value = new_value
        entry.version += 1
    await session.commit()
    await session.refresh(entry)
    index = len(_records_from_value(entry.value)) - 1
    return _to_out(index, record, entry.version)


async def _get_guidance_entry(
    session: SessionDep, agent_id: uuid.UUID
) -> WorkflowStateEntry | None:
    entry: WorkflowStateEntry | None = await session.scalar(
        select(WorkflowStateEntry).where(
            WorkflowStateEntry.agent_id == agent_id,
            WorkflowStateEntry.binding_scope.is_(None),
            WorkflowStateEntry.namespace == MEMORY_NAMESPACE,
            WorkflowStateEntry.key == MEMORY_GUIDANCE_KEY,
        )
    )
    return entry


def _guidance_out(entry: WorkflowStateEntry | None) -> MemoryGuidanceOut:
    """The effective guidance: the stored text, else the platform default.

    A stored value that is not ``{"text": <non-blank str>}`` reads as the
    default, which is also what the runner does with it.
    """
    value = entry.value if entry is not None else None
    if isinstance(value, dict):
        text = value.get("text")
        if isinstance(text, str) and text.strip():
            return MemoryGuidanceOut(text=text, source="operator")
    return MemoryGuidanceOut(text=DEFAULT_MEMORY_GUIDANCE, source="default")


# The guidance routes are registered before ``/memory/{index}`` so the literal
# ``guidance`` segment is never parsed as an int index (a 422).
@router.get("/{agent_id}/memory/guidance", response_model=MemoryGuidanceOut)
async def get_memory_guidance(
    agent_id: uuid.UUID, session: SessionDep
) -> MemoryGuidanceOut:
    """The agent's effective memory guidance and its source (#1461)."""
    await _require_agent(session, agent_id)
    return _guidance_out(await _get_guidance_entry(session, agent_id))


@router.put("/{agent_id}/memory/guidance", response_model=MemoryGuidanceOut)
async def put_memory_guidance(
    agent_id: uuid.UUID, data: MemoryGuidanceIn, session: SessionDep
) -> MemoryGuidanceOut:
    """Store operator guidance, replacing any earlier one (#1461).

    The runner reads it at the next session boot; a live thread keeps the
    guidance it booted with.
    """
    await _require_agent(session, agent_id)
    value = {"text": data.text}
    await _enforce_caps(
        session, agent_id, None, MEMORY_NAMESPACE, MEMORY_GUIDANCE_KEY, value
    )
    entry: WorkflowStateEntry | None = await session.scalar(
        select(WorkflowStateEntry)
        .where(
            WorkflowStateEntry.agent_id == agent_id,
            WorkflowStateEntry.binding_scope.is_(None),
            WorkflowStateEntry.namespace == MEMORY_NAMESPACE,
            WorkflowStateEntry.key == MEMORY_GUIDANCE_KEY,
        )
        .with_for_update()
    )
    if entry is None:
        entry = WorkflowStateEntry(
            agent_id=agent_id,
            namespace=MEMORY_NAMESPACE,
            key=MEMORY_GUIDANCE_KEY,
            value=value,
        )
        session.add(entry)
    else:
        entry.value = value
        entry.version += 1
    await session.commit()
    await session.refresh(entry)
    return _guidance_out(entry)


@router.delete(
    "/{agent_id}/memory/guidance", status_code=status.HTTP_204_NO_CONTENT
)
async def delete_memory_guidance(agent_id: uuid.UUID, session: SessionDep) -> Response:
    """Remove operator guidance, so the platform default applies again (#1461).

    Removing guidance that is not stored is a no-op, still 204.
    """
    await _require_agent(session, agent_id)
    entry = await _get_guidance_entry(session, agent_id)
    if entry is not None:
        await session.delete(entry)
        await session.commit()
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.get(
    "/{agent_id}/memory/{index}/provenance", response_model=MemoryTraceBackOut
)
async def memory_trace_back(
    agent_id: uuid.UUID, index: int, session: SessionDep
) -> MemoryTraceBackOut:
    """Resolve one entry's learned-from trace-back (#266).

    Returns the session and the source traces the lesson was distilled from, each
    with a Langfuse deep link. Reuses the provenance the runner recorded at
    ``remember`` time -- it does not re-derive or invent provenance.
    """
    await _require_agent(session, agent_id)
    records = _records_of(await _get_log_entry(session, agent_id))
    if index < 0 or index >= len(records):
        raise HTTPException(status.HTTP_404_NOT_FOUND, "memory entry not found")
    record = records[index]
    prov = _provenance_of(record)
    trace_ids = prov.get("source_trace_ids") or []
    return MemoryTraceBackOut(
        index=index,
        content=str(record.get("content", "")),
        learned_from_session_id=prov.get("learned_from_session_id"),
        recorded_at=prov.get("recorded_at", ""),
        source_traces=[
            SourceTraceOut(trace_id=str(tid), trace_url=_trace_url(str(tid)))
            for tid in trace_ids
        ],
    )


@router.put("/{agent_id}/memory/{index}", response_model=MemoryEntryOut)
async def edit_memory(
    agent_id: uuid.UUID, index: int, data: MemoryEntryEdit, session: SessionDep
) -> MemoryEntryOut:
    """Edit one entry's content using the parent log version.

    Rewrites the log array with the entry's ``content`` replaced. The recorded
    provenance is carried through unchanged. A stale version conflicts before
    the positional index can address a changed or reordered log.
    """
    entry, records = await _locked_log(session, agent_id, data.expected_version)
    if index < 0 or index >= len(records):
        raise HTTPException(status.HTTP_404_NOT_FOUND, "memory entry not found")
    updated = {**records[index], "content": data.content}
    replacement = [*records[:index], updated, *records[index + 1 :]]
    # None: the memory port stays agent-wide regardless of `agents.memory`
    # (#1525 follow-up) -- it is a different reserved namespace with no
    # per-binding story, same as every row this port has ever written.
    await _enforce_caps(
        session, agent_id, None, MEMORY_NAMESPACE, MEMORY_LOG_KEY, replacement
    )
    entry.value = replacement
    entry.version += 1
    await session.commit()
    return _to_out(index, updated, entry.version)


@router.delete(
    "/{agent_id}/memory/{index}", status_code=status.HTTP_204_NO_CONTENT
)
async def delete_memory(
    agent_id: uuid.UUID,
    index: int,
    session: SessionDep,
    expected_version: int = Query(
        description=(
            "Parent log version returned with the positional memory entry. "
            "A stale version conflicts if the log changed or reordered."
        )
    ),
) -> Response:
    """Delete one entry using its parent log version.

    Remaining entries keep their order. A stale version conflicts before the
    positional index can address a changed or reordered log.
    """
    entry, records = await _locked_log(session, agent_id, expected_version)
    if index < 0 or index >= len(records):
        raise HTTPException(status.HTTP_404_NOT_FOUND, "memory entry not found")
    entry.value = [*records[:index], *records[index + 1 :]]
    entry.version += 1
    await session.commit()
    return Response(status_code=status.HTTP_204_NO_CONTENT)
