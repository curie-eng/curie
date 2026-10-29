"""Strict, metadata-only worker recovery messages."""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Annotated, Literal, Self

from pydantic import BaseModel, ConfigDict, Field, StrictInt, model_validator

BoundedIdentity = Annotated[str, Field(min_length=1, max_length=256, pattern=r"\S")]
PositiveInt = Annotated[StrictInt, Field(ge=1, le=9223372036854775807)]


class RecoveryMessage(BaseModel):
    model_config = ConfigDict(extra="forbid")


class RecoveryRepository(RecoveryMessage):
    provider: BoundedIdentity
    host: BoundedIdentity
    project_id: BoundedIdentity
    path: Annotated[str, Field(min_length=1, max_length=2048, pattern=r"\S")]


class RecoveryObjectDeclaration(RecoveryMessage):
    kind: Literal["base", "patch", "manifest"]
    size_bytes: Annotated[StrictInt, Field(ge=0, le=9223372036854775807)]
    sha256: Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]


class RecoveryObjectReceipt(RecoveryObjectDeclaration):
    key: Annotated[str, Field(min_length=1, max_length=512)]


class RecoveryCaptureAuthority(RecoveryMessage):
    work_item_id: uuid.UUID
    source_request_id: uuid.UUID
    runtime_epoch: PositiveInt
    dispatch_generation: PositiveInt


class RecoveryReserve(RecoveryCaptureAuthority):
    idempotency_key: BoundedIdentity
    repository: RecoveryRepository
    base_commit: Annotated[str, Field(pattern=r"^(?:[0-9a-f]{40}|[0-9a-f]{64})$")]
    target_branch: Annotated[str, Field(min_length=1, max_length=1024, pattern=r"\S")]
    base_archive_limit_bytes: PositiveInt
    objects: Annotated[list[RecoveryObjectDeclaration], Field(min_length=2, max_length=3)]
    # The aggregate capture budget still left at this call; the ledger's capture
    # deadline is ``now`` plus it. Excluded from the idempotency digest.
    capture_budget_seconds: Annotated[float, Field(gt=0, le=60)] = 60.0

    @model_validator(mode="after")
    def _object_set(self) -> Self:
        kinds = [item.kind for item in self.objects]
        if len(set(kinds)) != len(kinds) or not {"patch", "manifest"} <= set(kinds):
            raise ValueError("declare one patch and one manifest, with an optional base")
        return self


class RecoveryReservation(RecoveryMessage):
    checkpoint_id: uuid.UUID
    objects: list[RecoveryObjectReceipt]
    capture_deadline: datetime
    staging_expires_at: datetime
    # True when the idempotency key matched an existing checkpoint, so this call
    # did not create it; a replayed reservation never asks for a retraction.
    replayed: bool


class RecoveryComplete(RecoveryCaptureAuthority):
    checkpoint_id: uuid.UUID
    receipts: Annotated[list[RecoveryObjectReceipt], Field(min_length=1, max_length=3)]


class RecoveryCompletion(RecoveryMessage):
    checkpoint_id: uuid.UUID
    status: Literal["complete"] = "complete"
    completed_at: datetime
    expires_at: datetime


class RecoveryFail(RecoveryCaptureAuthority):
    checkpoint_id: uuid.UUID
    reason: Literal[
        "upload_failed", "capture_failed", "capture_timeout", "torn_capture", "lease_lost"
    ]
    # Only the writer whose own complete call returned may withdraw a complete
    # checkpoint; without it a fail on a complete checkpoint is refused.
    retract: bool = False


class RecoveryCaptureFailure(RecoveryMessage):
    checkpoint_id: uuid.UUID
    status: Literal["tombstoned", "retracted"] = "tombstoned"


class RecoveryCaptureContext(RecoveryMessage):
    repository: RecoveryRepository
    base_commit: str
    target_branch: str


class RecoveryOutcome(RecoveryCaptureAuthority):
    reason: Literal[
        "torn_capture",
        "capture_timeout",
        "capture_failed",
        "runner_unavailable",
        "patch_too_large",
        "manifest_too_large",
        "base_mismatch",
    ]


class RecoveryOutcomeRecorded(RecoveryMessage):
    recorded: bool


class RecoveryAcquireReader(RecoveryMessage):
    work_item_id: uuid.UUID
    checkpoint_id: uuid.UUID
    holder_id: BoundedIdentity


class RecoveryReader(RecoveryMessage):
    lease_id: uuid.UUID
    checkpoint_id: uuid.UUID
    holder_id: str
    expires_at: datetime
    objects: list[RecoveryObjectReceipt]


class RecoveryReleaseReader(RecoveryMessage):
    lease_id: uuid.UUID
    holder_id: BoundedIdentity


class RecoveryAcknowledged(RecoveryMessage):
    released: bool = True


class RecoveryClaimCollection(RecoveryMessage):
    collector_id: BoundedIdentity
    limit: Annotated[StrictInt, Field(ge=1, le=100)] = 100


class RecoveryCollectionClaim(RecoveryMessage):
    object_id: uuid.UUID
    key: str
    claim_id: uuid.UUID
    version: int
    claimed_until: datetime


class RecoveryCollectionClaims(RecoveryMessage):
    claims: list[RecoveryCollectionClaim]


class RecoveryFinalizeCollection(RecoveryMessage):
    collector_id: BoundedIdentity
    object_id: uuid.UUID
    claim_id: uuid.UUID
    version: PositiveInt


class RecoveryFinalized(RecoveryMessage):
    finalized: bool = True
