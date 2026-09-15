"""The memory port: resolve ``CURIE_MEMORY_REF`` and load/append agent memory.

This is the first loader for the memory seam (issue #264, epic #28). Until now
``memory_ref`` was a ``SessionConfig`` field carried end-to-end but never
dereferenced (see ``docs/interfaces/memory/INTERFACE.md``). This module defines
the port and its first concrete backing.

Design (ADR-0025):

- **Memory lives outside the sandbox** (ADR-0003, stateless-first). A resumed
  thread must rehydrate from an external, durable resource, never from surviving
  in-process state. So the store is reached over the network at boot, not held in
  pod-local scratch.
- **The backing reuses the durable KV/document store** landed for #23/#248
  (``apps/api`` ``/agents/{agent_id}/state/{namespace}/{key}``, Postgres JSONB),
  rather than inventing a new datastore. Memory is a **scoped namespace** over
  that store: the log-shaped ``append`` endpoint gives us the append-only,
  provenance-carrying write the memory port needs, and ``get`` gives us load.
- **The port is small and swappable** -- ``load`` / ``append`` -- matching the
  "seventh swappable job, one default backing" framing of the interface doc. A
  future S3- or API-backed loader is a drop-in ``MemoryStore``.

``CURIE_MEMORY_REF`` resolution: the ref is the URL of the agent's memory
namespace on the state API (e.g. ``http://api:8000/agents/<id>/state/memory``).
The runner authenticates to that API with ``CURIE_MEMORY_TOKEN`` (a
runner-local knob, like ``CURIE_RUNNER_TOKEN``/``CURIE_MODEL`` -- NOT part of
the frozen ACI ``SessionConfig`` env, so no frozen-contract change). An ``s3://``
or other scheme is reserved for a future loader and rejected loudly today.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, Protocol, runtime_checkable

import aiohttp
from aci_protocol import BootEnv

logger = logging.getLogger(__name__)

# The single log-shaped key inside the agent's memory namespace. The whole
# namespace is reserved for memory; one append-only log key keeps load a single
# GET and append a single POST against the #248 log endpoint.
MEMORY_LOG_KEY = "log"

# Runner-local env carrying the bearer the state API expects (X-API-Key). Not a
# model credential and not part of the frozen ACI SessionConfig -- resolved the
# same way as the other runner-local knobs in config.py. The worker declares and
# renders it, so the name is read from that one declaration (#488).
MEMORY_TOKEN_ENV = BootEnv.env_key("memory_token")


class MemoryError(RuntimeError):
    """A memory reference could not be resolved or dereferenced."""


@dataclass(frozen=True)
class Provenance:
    """Where a memory record was learned from -- links the entry to its sources.

    ``learned_from_session_id`` is the ACI session that produced the record;
    ``source_trace_ids`` are the OTel/Langfuse trace ids of the turns the lesson
    was distilled from. ``recorded_at`` is set at append time. This is the shape
    the epic (#28) calls for: entry -> source trace ids.
    """

    learned_from_session_id: str | None = None
    source_trace_ids: tuple[str, ...] = ()
    recorded_at: str = ""
    # ``operator`` for CLI/API-seeded records (#1904); None/absent for learned.
    source: str | None = None

    def to_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "learned_from_session_id": self.learned_from_session_id,
            "source_trace_ids": list(self.source_trace_ids),
            "recorded_at": self.recorded_at,
        }
        if self.source is not None:
            payload["source"] = self.source
        return payload

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> Provenance:
        source = data.get("source")
        return cls(
            learned_from_session_id=data.get("learned_from_session_id"),
            source_trace_ids=tuple(data.get("source_trace_ids") or ()),
            recorded_at=data.get("recorded_at", ""),
            source=str(source) if source else None,
        )


@dataclass(frozen=True)
class MemoryRecord:
    """One durable memory entry: the learned content plus its provenance."""

    content: str
    provenance: Provenance = field(default_factory=Provenance)

    def to_dict(self) -> dict[str, Any]:
        return {"content": self.content, "provenance": self.provenance.to_dict()}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> MemoryRecord:
        prov = data.get("provenance")
        return cls(
            content=data["content"],
            provenance=Provenance.from_dict(prov) if isinstance(prov, Mapping) else Provenance(),
        )


@runtime_checkable
class MemoryStore(Protocol):
    """The memory port: load prior records, append a new one with provenance.

    Deliberately narrow -- no query language, no consolidate (that is a later
    slice, #265/#266/#267). A concrete store dereferences a ``memory_ref`` to a
    durable, rehydratable backing that lives outside the sandbox.
    """

    async def load(self) -> list[MemoryRecord]:
        """Return prior memory records, oldest first (empty when none)."""
        ...

    async def append(self, record: MemoryRecord) -> None:
        """Durably append one record; it must survive suspend/resume."""
        ...


@runtime_checkable
class SupportsReplace(Protocol):
    """Optional capability: atomically replace the whole record set.

    The narrow ``MemoryStore`` port is append-only, which is all the boot path
    (#264) needs. Consolidation (#265) additionally needs to *rewrite* the log
    with a compacted record set, so it is a separate, opt-in capability rather
    than a widening of the frozen ``MemoryStore`` contract. A store that cannot
    rewrite (``NullMemoryStore``) simply does not implement it, and the
    consolidation pass no-ops against such a store.
    """

    async def replace(self, records: Sequence[MemoryRecord]) -> None:
        """Overwrite the durable record set with ``records`` (oldest first)."""
        ...


class NullMemoryStore:
    """The no-memory store used when ``CURIE_MEMORY_REF`` is unset.

    ``load`` yields nothing and ``append`` is a silent no-op, so the boot path is
    uniform whether or not an agent has memory configured.
    """

    async def load(self) -> list[MemoryRecord]:
        return []

    async def append(self, record: MemoryRecord) -> None:  # noqa: ARG002 - null sink
        return None

    async def replace(self, records: Sequence[MemoryRecord]) -> None:  # noqa: ARG002
        return None


class StateApiMemoryStore:
    """Memory backed by the durable state store (#23/#248), the default loader.

    ``memory_ref`` is the URL of the agent's memory namespace on the state API
    (``.../agents/<id>/state/memory``). Load is a GET of the single log key;
    append is a POST to the log's ``/append`` endpoint. The state API enforces
    the size caps (#248) and the Postgres JSONB backing gives durability across
    suspend/resume for free.
    """

    def __init__(self, namespace_url: str, token: str | None) -> None:
        # Normalize to no trailing slash so key URLs compose cleanly.
        self._base = namespace_url.rstrip("/")
        self._token = token

    @property
    def _log_url(self) -> str:
        return f"{self._base}/{MEMORY_LOG_KEY}"

    def _headers(self) -> dict[str, str]:
        return {"X-API-Key": self._token} if self._token else {}

    async def load(self) -> list[MemoryRecord]:
        timeout = aiohttp.ClientTimeout(total=15)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(self._log_url, headers=self._headers()) as resp:
                if resp.status == 404:
                    # No memory written yet -- a fresh agent, not an error.
                    return []
                if resp.status != 200:
                    body = await resp.text()
                    raise MemoryError(
                        f"memory load failed: {resp.status} {body[:200]}"
                    )
                payload = await resp.json()
        value = payload.get("value")
        if not isinstance(value, list):
            raise MemoryError("memory log is not a JSON array")
        records: list[MemoryRecord] = []
        for item in value:
            if isinstance(item, Mapping) and "content" in item:
                records.append(MemoryRecord.from_dict(item))
        return records

    async def append(self, record: MemoryRecord) -> None:
        timeout = aiohttp.ClientTimeout(total=15)
        body = json.dumps({"item": record.to_dict()})
        headers = {**self._headers(), "Content-Type": "application/json"}
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.post(
                f"{self._log_url}/append", data=body, headers=headers
            ) as resp:
                if resp.status not in (200, 201):
                    text = await resp.text()
                    raise MemoryError(
                        f"memory append failed: {resp.status} {text[:200]}"
                    )

    async def replace(self, records: Sequence[MemoryRecord]) -> None:
        """Overwrite the log key with ``records`` via a blind PUT (#248).

        This is the write-back side of consolidation (#265): the compacted
        record set replaces the append-only log in a single PUT. A blind put
        (no ``expected_version``) is deliberate -- consolidation runs at boot
        before any turn appends, so there is no concurrent writer to race, and
        we want the compaction to land rather than 409 on a stale version.
        """
        timeout = aiohttp.ClientTimeout(total=15)
        value = [record.to_dict() for record in records]
        body = json.dumps({"value": value})
        headers = {**self._headers(), "Content-Type": "application/json"}
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.put(
                self._log_url, data=body, headers=headers
            ) as resp:
                if resp.status not in (200, 201):
                    text = await resp.text()
                    raise MemoryError(
                        f"memory replace failed: {resp.status} {text[:200]}"
                    )


def resolve_memory(memory_ref: str | None, env: Mapping[str, str]) -> MemoryStore:
    """Resolve ``CURIE_MEMORY_REF`` to a concrete ``MemoryStore`` at boot.

    An absent ref yields the ``NullMemoryStore`` (memory is optional). An
    ``http(s)://`` ref is the state-API namespace URL and yields the default
    ``StateApiMemoryStore``. Any other scheme (``s3://`` etc.) is reserved for a
    future loader and rejected loudly rather than silently ignored, so a
    misconfigured ref fails visibly at boot rather than dropping memory.
    """

    if not memory_ref:
        return NullMemoryStore()
    if memory_ref.startswith(("http://", "https://")):
        return StateApiMemoryStore(memory_ref, env.get(MEMORY_TOKEN_ENV))
    raise MemoryError(
        f"unsupported CURIE_MEMORY_REF scheme: {memory_ref!r} "
        "(only http(s):// state-API refs are implemented today)"
    )


_OPERATOR_MEMORY_HEADING = "# Agent memory (provided by the operator)"
_LEARNED_MEMORY_HEADING = "# Agent memory (learned from prior sessions)"


def _is_operator_record(record: MemoryRecord) -> bool:
    return record.provenance.source == "operator"


def _memory_record_line(record: MemoryRecord) -> str:
    prov = record.provenance
    traces = ", ".join(prov.source_trace_ids) if prov.source_trace_ids else ""
    suffix = f"  (learned from traces: {traces})" if traces else ""
    return f"- {record.content}{suffix}"


def _preamble_section(heading: str, records: Sequence[MemoryRecord]) -> list[str]:
    lines = [heading, ""]
    for record in records:
        lines.append(_memory_record_line(record))
    return lines


def format_memory_preamble(records: Sequence[MemoryRecord]) -> str | None:
    """Render prior memory as a system-prompt preamble, or None when empty.

    This is how loaded memory is *delivered into the sandbox*: it is composed
    into the runner's effective system prompt at boot, so the model sees prior
    lessons as durable context. Provenance is summarized inline so a lesson is
    traceable back to the turns it came from. Operator-authored records are a
    separate heading so they are not framed as lessons from prior sessions.
    """

    if not records:
        return None
    operator = [record for record in records if _is_operator_record(record)]
    learned = [record for record in records if not _is_operator_record(record)]
    sections: list[list[str]] = []
    if operator:
        sections.append(_preamble_section(_OPERATOR_MEMORY_HEADING, operator))
    if learned:
        sections.append(_preamble_section(_LEARNED_MEMORY_HEADING, learned))
    lines: list[str] = []
    for section in sections:
        if lines:
            lines.append("")
        lines.extend(section)
    return "\n".join(lines)


def utcnow_iso() -> str:
    """An RFC3339 UTC timestamp for a provenance record's ``recorded_at``."""
    return datetime.now(UTC).isoformat()


# --- Consolidation pipeline (#265) ---------------------------------------
#
# Raw ``remember`` calls accumulate an append-only log that grows monotonically
# and can hold near-duplicate lessons (the same thing learned across several
# sessions). Consolidation compacts that log: it merges records with equivalent
# content into one, **unioning their provenance** so no source trace is lost,
# then writes the compacted set back over a store that supports ``replace``.
# The load-bearing acceptance constraint (#265) is *compaction reduces
# redundancy without losing provenance* -- so the merge is provenance-preserving
# by construction, never a lossy summarize-and-discard.


def _normalize_content(content: str) -> str:
    """The dedup key for a lesson: case- and whitespace-insensitive content."""
    return " ".join(content.split()).casefold()


def merge_provenance(a: Provenance, b: Provenance) -> Provenance:
    """Union two provenances without losing any source link.

    ``source_trace_ids`` are unioned preserving first-seen order; the
    ``learned_from_session_id`` keeps the first non-empty value; ``recorded_at``
    keeps the earliest timestamp so a consolidated record still points at when
    the lesson was *first* learned.
    """
    trace_ids: list[str] = list(a.source_trace_ids)
    for tid in b.source_trace_ids:
        if tid not in trace_ids:
            trace_ids.append(tid)
    session_id = a.learned_from_session_id or b.learned_from_session_id
    stamps = [s for s in (a.recorded_at, b.recorded_at) if s]
    recorded_at = min(stamps) if stamps else ""
    source: str | None
    if a.source == "operator" or b.source == "operator":
        source = "operator"
    else:
        source = a.source or b.source
    return Provenance(
        learned_from_session_id=session_id,
        source_trace_ids=tuple(trace_ids),
        recorded_at=recorded_at,
        source=source,
    )


def consolidate_records(records: Sequence[MemoryRecord]) -> list[MemoryRecord]:
    """Merge equivalent-content records into one, unioning provenance.

    Deterministic and model-free: records whose normalized content matches are
    collapsed into a single record (keeping the first-seen surface form of the
    content), and their provenances are merged via :func:`merge_provenance`.
    Order of first appearance is preserved so the compacted log reads oldest
    first, matching ``load``'s contract. This is the default consolidator; a
    model-backed summarizer is a drop-in that produces a shorter record set.
    """
    merged: dict[str, MemoryRecord] = {}
    for record in records:
        key = _normalize_content(record.content)
        existing = merged.get(key)
        if existing is None:
            merged[key] = record
        else:
            merged[key] = MemoryRecord(
                content=existing.content,
                provenance=merge_provenance(existing.provenance, record.provenance),
            )
    return list(merged.values())


@dataclass(frozen=True)
class ConsolidationResult:
    """Outcome of a consolidation pass: what changed and whether it was written."""

    before: int
    after: int
    records: list[MemoryRecord]
    written: bool

    @property
    def removed(self) -> int:
        """How many redundant records the pass compacted away."""
        return self.before - self.after


async def consolidate_memory(
    store: MemoryStore,
    *,
    consolidate: Any = consolidate_records,
) -> ConsolidationResult:
    """Load, consolidate, and write back the agent's memory (the #265 entry point).

    Loads the current record set, runs ``consolidate`` (default:
    :func:`consolidate_records`) over it, and -- only when the pass actually
    reduced the record count and the store advertises the ``replace`` capability
    -- writes the compacted set back. A store that cannot rewrite
    (``NullMemoryStore``, or any future read-only backing) is a no-op that still
    reports what a pass *would* have done. Nothing is written when there is no
    redundancy to remove, so a steady-state boot does not churn the store.
    """
    records = await store.load()
    consolidated = list(consolidate(records))
    written = False
    if len(consolidated) < len(records) and isinstance(store, SupportsReplace):
        await store.replace(consolidated)
        written = True
    return ConsolidationResult(
        before=len(records),
        after=len(consolidated),
        records=consolidated,
        written=written,
    )
