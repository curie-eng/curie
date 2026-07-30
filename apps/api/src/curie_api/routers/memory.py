"""Agent memory API: inspect, trace-back, edit, and delete what an agent learned.

Memory is a scoped namespace over the durable state store (#264, ADR-0025): the
runner writes an append-only log at namespace ``memory`` key ``log``, each item a
``{content, provenance}`` record, reloaded at the next session boot. This router
is the operator's read/write surface over that one log key -- it does NOT add a
second store. #266 adds the learned-from trace-back (resolve an entry's session +
source traces); #267 adds edit/delete (an edit preserves the entry's provenance;
a delete removes exactly one entry). Because the runner rehydrates from the same
key, edits and deletes are reflected at the next boot.
"""

import uuid
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query, Response, status
from sqlalchemy import select

from .. import crud
from ..auth import require_api_key
from ..config import get_settings
from ..deps import SessionDep
from ..models import WorkflowStateEntry
from ..schemas import (
    MemoryEntryEdit,
    MemoryEntryOut,
    MemoryProvenanceOut,
    MemoryTraceBackOut,
    SourceTraceOut,
)
from .state import _enforce_caps

router = APIRouter(prefix="/agents", tags=["memory"], dependencies=[Depends(require_api_key)])

# The runner writes memory here (mirrors runner/curie_runner/memory.py: a
# single log-shaped key inside the reserved ``memory`` namespace).
MEMORY_NAMESPACE = "memory"
MEMORY_LOG_KEY = "log"


async def _require_agent(session: SessionDep, agent_id: uuid.UUID) -> None:
    if await crud.get_agent(session, agent_id) is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "agent not found")


async def _get_log_entry(session: SessionDep, agent_id: uuid.UUID) -> WorkflowStateEntry | None:
    entry: WorkflowStateEntry | None = await session.scalar(
        select(WorkflowStateEntry).where(
            WorkflowStateEntry.agent_id == agent_id,
            WorkflowStateEntry.namespace == MEMORY_NAMESPACE,
            WorkflowStateEntry.key == MEMORY_LOG_KEY,
        )
    )
    return entry


def _records_of(entry: WorkflowStateEntry | None) -> list[dict[str, Any]]:
    """The log's ``{content, provenance}`` records, or [] when absent/malformed."""
    if entry is None or not isinstance(entry.value, list):
        return []
    return [r for r in entry.value if isinstance(r, dict) and "content" in r]


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


@router.get("/{agent_id}/memory/{index}/provenance", response_model=MemoryTraceBackOut)
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
            SourceTraceOut(trace_id=str(tid), trace_url=_trace_url(str(tid))) for tid in trace_ids
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
    if data.expected_version != entry.version:
        raise HTTPException(
            status.HTTP_409_CONFLICT,
            f"version mismatch: expected {data.expected_version}, stored {entry.version}",
        )
    records = _records_of(entry)
    if index < 0 or index >= len(records):
        raise HTTPException(status.HTTP_404_NOT_FOUND, "memory entry not found")
    updated = {**records[index], "content": data.content}
    replacement = [*records[:index], updated, *records[index + 1 :]]
    await _enforce_caps(session, agent_id, MEMORY_NAMESPACE, MEMORY_LOG_KEY, replacement)
    entry.value = replacement
    entry.version += 1
    await session.commit()
    return _to_out(index, updated, entry.version)


@router.delete("/{agent_id}/memory/{index}", status_code=status.HTTP_204_NO_CONTENT)
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
    records = _records_of(entry)
    if index < 0 or index >= len(records):
        raise HTTPException(status.HTTP_404_NOT_FOUND, "memory entry not found")
    entry.value = [*records[:index], *records[index + 1 :]]
    entry.version += 1
    await session.commit()
    return Response(status_code=status.HTTP_204_NO_CONTENT)
