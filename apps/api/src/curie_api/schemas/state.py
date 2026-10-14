import uuid
from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator


class StateEntryPut(BaseModel):
    """Write a durable state entry (#23). ``expected_version`` opts into
    compare-and-set: the write is rejected with 409 unless it matches the stored
    version (omit it for a blind upsert). ``value`` is any JSON value (object,
    array, or scalar); an array value is what ``append`` grows."""

    value: Any
    expected_version: int | None = None


class StateAppendIn(BaseModel):
    """Append ``item`` to a log-shaped (JSON array) state entry (#248). If the
    entry does not exist it is created as a single-element array; if it exists
    its value must already be an array, else the append is rejected.

    ``reserve_bytes`` (#2927) refuses the append with 413 when the new value
    would leave fewer than that many bytes free under the per-value cap. The
    runner sets it on transcript appends to keep headroom for the worker's
    publication outcome append; omitting it keeps the plain cap."""

    item: Any
    reserve_bytes: int | None = Field(default=None, ge=0)


class SandboxCredentialReleasedIn(BaseModel):
    """The worker reporting that a sandbox claim released its boot credential
    (#3823). ``credential`` is the ``cred`` claim shared by that claim's
    ``state`` and ``state.app`` tokens."""

    agent_id: uuid.UUID
    credential: str = Field(pattern=r"^[0-9a-f]{32}$")


class MemoryTurnClosedIn(BaseModel):
    """The worker reporting that a turn has ended (#3776): from now on the API
    refuses memory writes made with that turn's per-turn credential (ADR-0188),
    even before it expires. ``turn`` is the credential's ``turn`` claim, opaque
    to the API."""

    agent_id: uuid.UUID
    turn: str = Field(min_length=1, max_length=512)


class StateEntryOut(BaseModel):
    """A durable state entry as returned to the caller."""

    model_config = ConfigDict(from_attributes=True)

    namespace: str
    key: str
    value: Any
    version: int
    updated_at: datetime


class StateNamespaceOut(BaseModel):
    """One namespace in an agent's durable state store, for the operator's
    read/inspect surface (#250): the namespace, how many keys it holds, and when
    it was most recently written."""

    namespace: str
    key_count: int
    last_updated: datetime


# --- Agent memory (#266 trace-back; #267 inspect/edit/delete) ---------------


class MemoryProvenanceOut(BaseModel):
    """Where a memory entry was learned from (#264 ``Provenance`` shape).

    ``source`` distinguishes an operator-seeded record (``operator``) from a
    session-learned one. Absent or null means learned/unspecified, matching
    records written before the operator seed path existed.
    """

    learned_from_session_id: str | None = None
    source_trace_ids: list[str] = Field(default_factory=list)
    recorded_at: str = ""
    source: str | None = None


class SourceTraceOut(BaseModel):
    """One resolved source trace: its id plus a link to view it in Langfuse."""

    trace_id: str
    trace_url: str


class MemoryEntryOut(BaseModel):
    """One learned memory entry as returned to an operator.

    ``index`` is the entry's current position in the memory log. It is valid for
    mutation only with this response's parent log ``version``. A mutation with
    a stale version conflicts if another change has reordered the log.
    """

    index: int
    content: str
    provenance: MemoryProvenanceOut
    version: int


class MemoryTraceBackOut(BaseModel):
    """The learned-from trace-back for one memory entry (#266).

    Resolves an entry's recorded provenance into the concrete session and source
    traces the lesson was learned from -- the answer to "how did it learn that?".
    """

    index: int
    content: str
    learned_from_session_id: str | None = None
    recorded_at: str = ""
    source_traces: list[SourceTraceOut] = Field(default_factory=list)


class MemoryEntryEdit(BaseModel):
    """Edit one memory entry using its parent log version.

    The required version prevents a stale positional index from changing an
    entry after the log has changed. Provenance is preserved.
    """

    content: str
    expected_version: int


class MemoryEntryCreate(BaseModel):
    """Append one operator-authored memory record (#1904).

    Provenance is stamped by the server. Extra body fields, including a
    caller-supplied provenance object, are ignored rather than trusted.
    """

    content: str

    @field_validator("content")
    @classmethod
    def _content_not_blank(cls, value: str) -> str:
        stripped = value.strip()
        if not stripped:
            raise ValueError("content must not be empty")
        return stripped


class MemoryGuidanceIn(BaseModel):
    """Operator memory guidance for one agent (#1461).

    Stored verbatim at ``memory/guidance`` as ``{"text": ...}``; the runner shows
    it to the model beside its memory tools in place of the platform default.
    """

    text: str

    @field_validator("text")
    @classmethod
    def _text_not_blank(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("text must not be empty")
        return value


class MemoryGuidanceOut(BaseModel):
    """The agent's effective memory guidance and where it comes from (#1461).

    ``source`` is ``operator`` when guidance is stored for the agent, else
    ``default`` and ``text`` is the platform default.
    """

    text: str
    source: Literal["default", "operator"]
