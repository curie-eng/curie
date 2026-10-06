import uuid
from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class ActionRecord(BaseModel):
    """The opening frame of a side-effecting call, as the worker forwards it.

    ``dedupe_key`` is the triggering event id and the call id. The worker
    redelivers at least once (ADR-0013), so a replayed turn must adopt the record
    it already wrote rather than mint a second account of one call.
    """

    agent_id: uuid.UUID | None = None
    conversation_id: str
    call_id: str
    tool: str
    arguments: dict[str, Any] | None = None
    detail: str | None = None
    # The approval that gated this call, when one did. The worker knows it
    # because a gated call only executes on an approval-resume turn.
    gate_approval_id: uuid.UUID | None = None
    dedupe_key: str


class ActionComplete(BaseModel):
    """The closing frame: what came back, and what it takes to put it back.

    ``prior_state`` and ``target`` are what a restore replays. Both are optional
    because a connector that answers in prose reports neither, and a record that
    holds neither is not undoable -- which is the honest answer rather than a
    missing one.
    """

    failed: bool = False
    result: dict[str, Any] | None = None
    prior_state: dict[str, Any] | None = None
    post_state: dict[str, Any] | None = None
    target: dict[str, Any] | None = None
    detail: str | None = None


class ActionOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    agent_id: uuid.UUID | None
    conversation_id: str
    call_id: str
    tool: str
    arguments: dict[str, Any] | None
    result: dict[str, Any] | None
    prior_state: dict[str, Any] | None
    post_state: dict[str, Any] | None
    target: dict[str, Any] | None
    detail: str | None
    gate_approval_id: uuid.UUID | None
    status: str
    dedupe_key: str
    created_at: datetime
    completed_at: datetime | None
    undone_at: datetime | None
    undone_by: str | None
    # Derived at read time, never stored, so a record cannot claim a
    # reversibility nothing captured the state for (ADR-0117). Computed by
    # ``curie_api.action_undoable`` (ACTION-EXECUTOR-11).
    undoable: bool


class ActionUndo(BaseModel):
    """A request to put back what an action changed.

    @spec ACTION-EXECUTOR-3: the actor is the authenticated principal, never
    this body. ``actor`` is accepted only as a cross-check: one that differs
    from the principal is refused. Channel evidence likewise comes from the
    principal. The platform observes the live version itself through the
    pinned connector (ACTION-EXECUTOR-15), so a caller-supplied observation is
    no longer evidence; an ``observed_state`` sent by an older caller is
    ignored as an unknown field, never compared.
    """

    actor: str | None = Field(default=None, max_length=256)


class ActionUndoOut(BaseModel):
    """A requested restore, not a receipt and not the call to make.

    @spec ACTION-EXECUTOR-3: the ruling answers with the execution it created
    and that execution's state. It never carries the ``target``, the sealed
    ``prior_state`` or a version; the receipt is the execution's own read.
    """

    execution_id: uuid.UUID
    state: str


class ActionAuditOut(BaseModel):
    """One entry in an action's audit trail: an authorized undo, or a refused one."""

    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    action_id: uuid.UUID
    action: str
    actor: str
    actor_channel: str | None
    authorizer: str
    authorized: bool
    reason: str | None
    evidence: dict[str, Any] | None
    created_at: datetime
