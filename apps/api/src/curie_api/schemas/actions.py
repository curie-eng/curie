import uuid
from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from ..sealed_snapshot import MAX_VERSION_LENGTH, is_post_version
from .action_executions import CONNECTOR_MAX_LENGTH, CONNECTOR_PATTERN, DIGEST_PATTERN


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
    # The opaque version the call left, from the connector's sealed reply
    # (ACTION-EXECUTOR-9); one ingredient of ``undoable`` (ACTION-EXECUTOR-11).
    # The worker sends it only beside a valid envelope. A malformed one is a
    # 422, never truncated or stored.
    post_version: str | None = Field(
        default=None, min_length=1, max_length=MAX_VERSION_LENGTH, pattern=r"^[\x20-\x7e]+$"
    )
    target: dict[str, Any] | None = None
    detail: str | None = None
    # The hosted connector the call reached and the image digest it was served
    # at, as the worker's recorder wrapper attributed them from two Deployment
    # reads bracketing the call (ACTION-EXECUTOR-12). Null is the ordinary
    # answer: local tier, plugin server, straddled rollout, unreadable
    # Deployment. Same grammars as the probe route; a pair or neither.
    connector: str | None = Field(
        default=None, min_length=1, max_length=CONNECTOR_MAX_LENGTH, pattern=CONNECTOR_PATTERN
    )
    connector_digest: str | None = Field(default=None, pattern=DIGEST_PATTERN)

    @field_validator("post_version")
    @classmethod
    def _post_version_is_well_formed(cls, value: str | None) -> str | None:
        """@spec ACTION-EXECUTOR-9: printable ASCII, at most 256, no placeholder."""

        if value is not None and not is_post_version(value):
            raise ValueError("post_version must be 1 to 256 printable ASCII characters")
        return value

    @model_validator(mode="after")
    def _attribution_is_a_pair(self) -> "ActionComplete":
        """@spec ACTION-EXECUTOR-11 @spec ACTION-EXECUTOR-12: both or neither.

        A digest names an image only together with the connector it was read
        from, so half a pair is refused rather than stored as half an answer.
        """

        if (self.connector is None) != (self.connector_digest is None):
            raise ValueError("connector and connector_digest are recorded together or not at all")
        return self


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
