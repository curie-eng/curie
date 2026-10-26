"""Wire shapes of the typed twin memory routes (ADR 0249, Contract r4).

The routes live under ``/agents/{agent_id}/state/_memory`` on the state router.
``tests/vectors/twin-memory.json`` holds an example of every response object
here; ``apps/api/tests/test_twin_memory.py`` pins their key sets to what the
API really returns, and the runner and CLI readers parse the same vectors.
"""

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, Field

from .state import MemoryGuidanceOut

FACT_ID_PATTERN = r"^fact-[0-9a-f]{32}$"

MemoryKind = Literal["fact", "instruction"]
MemoryScope = Literal["agent", "channel"]
MemoryOperation = Literal["baseline", "create", "update", "delete", "archive", "restore", "revert"]


class MemoryRecordOut(BaseModel):
    """One canonical memory entry (a fact or a standing instruction).

    ``author`` and ``stated_at`` are null only for an entry stored before ADR
    0249 that never named them; the migration does not invent provenance.
    ``conversation`` is the signed source conversation of the last sandbox
    write, null for operator writes and migrated entries."""

    id: str
    scope: MemoryScope
    kind: MemoryKind
    title: str
    statement: str
    author: str | None
    stated_at: str | None
    conversation: str | None
    archived: bool
    version: int
    updated_at: datetime


class MemoryIndexItemOut(BaseModel):
    """One active fact in the boot index: its title, never its statement."""

    id: str
    scope: MemoryScope
    title: str
    version: int


class MemoryTotalsOut(BaseModel):
    """Counts over both visible scopes: active instructions, active facts, and
    archived entries of either kind."""

    instructions: int
    facts: int
    archived: int


class MemoryOmittedOut(BaseModel):
    """Active entries the bounded context left out."""

    instructions: int
    facts: int


class MemoryContextOut(BaseModel):
    """The bounded boot context (``GET .../_memory/context``).

    At most 20 active instructions (full records, empty unless agent-wide
    learning is on) and at most 50 active fact titles, newest updated first,
    from agent memory plus the caller's own channel. No fact statement appears
    anywhere in it."""

    agent_wide_learning: bool
    memory_writes: bool
    guidance: MemoryGuidanceOut
    instructions: list[MemoryRecordOut]
    index: list[MemoryIndexItemOut]
    totals: MemoryTotalsOut
    omitted: MemoryOmittedOut


class MemoryRevisionOut(BaseModel):
    """One immutable revision. ``snapshot`` is the resulting canonical value,
    null for a deletion (a forget, or a revert to absence)."""

    version: int
    operation: MemoryOperation
    actor: str
    source: str
    conversation: str | None
    created_at: datetime
    reverted_to_version: int | None
    snapshot: Any | None


class MemoryDetailOut(BaseModel):
    """An entry with a page of its history (``GET .../_memory/entries/{id}``,
    ``POST .../_memory/revert``).

    ``entry`` is null and ``deleted`` true when the head was forgotten or
    reverted to absence; ``version`` is then the deletion's version, the one a
    revert must name as ``expected_version``. ``history`` is at most the 10
    newest revisions older than the requested ``before_version``;
    ``history_before`` is the value to pass back for the next older page, null
    when there is none."""

    entry: MemoryRecordOut | None
    deleted: bool
    version: int
    history: list[MemoryRevisionOut]
    history_before: int | None


class MemorySearchOut(BaseModel):
    """A page of literal search results over active and archived entries."""

    entries: list[MemoryRecordOut]
    next_cursor: str | None


class MemoryLifecycleIn(BaseModel):
    """Archive or restore one entry under compare-and-set.

    ``channel_kind``/``channel_address`` name the binding for a platform-key
    caller with ``scope="channel"``. A sandbox caller's channel is the binding
    its credential is signed for, and naming one is refused."""

    scope: MemoryScope
    id: str = Field(pattern=FACT_ID_PATTERN)
    expected_version: int = Field(ge=0)
    channel_kind: str | None = None
    channel_address: str | None = None


class MemoryRevertIn(MemoryLifecycleIn):
    """Revert one entry to an earlier version; ``to_version`` 0 is absence."""

    to_version: int = Field(ge=0)
