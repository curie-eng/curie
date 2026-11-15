"""Strict, bounded worker messages for private publication candidates (#5428).

Candidates are private and non-authorizing: no message here carries source
bytes, and the operator metadata shapes carry only ids, statuses, timestamps,
counts, sizes and digests.
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime
from typing import Annotated, Literal, Self

from pydantic import BaseModel, ConfigDict, Field, StrictBool, StrictInt, model_validator

BoundedIdentity = Annotated[str, Field(min_length=1, max_length=256, pattern=r"\S")]
PositiveInt = Annotated[StrictInt, Field(ge=1, le=9223372036854775807)]
Sha256 = Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]
CommitSha = Annotated[str, Field(pattern=r"^(?:[0-9a-f]{40}|[0-9a-f]{64})$")]
# Postgres text and jsonb refuse NUL, so the wire refuses it before the database.
ChangedPath = Annotated[str, Field(min_length=1, max_length=4096, pattern=r"^[^\x00]+$")]

CandidateStatus = Literal["staging", "complete", "tombstoned"]

PATCH_CEILING_BYTES = 900000
CHANGED_PATHS_MAX_OCTETS = 1048576


class CandidateMessage(BaseModel):
    model_config = ConfigDict(extra="forbid")


class CandidateRepository(CandidateMessage):
    provider: BoundedIdentity
    host: BoundedIdentity
    project_id: BoundedIdentity
    path: Annotated[str, Field(min_length=1, max_length=2048, pattern=r"\S")]


class CandidateObjectDeclaration(CandidateMessage):
    kind: Literal["base", "patch"]
    size_bytes: Annotated[StrictInt, Field(ge=0, le=9223372036854775807)]
    sha256: Sha256


class CandidateObjectReceipt(CandidateObjectDeclaration):
    key: Annotated[str, Field(min_length=1, max_length=512)]


class CandidateCaptureAuthority(CandidateMessage):
    work_item_id: uuid.UUID
    request_id: uuid.UUID
    runtime_epoch: PositiveInt
    dispatch_generation: PositiveInt


class CandidateCaptureContext(CandidateMessage):
    repository: CandidateRepository
    base_commit: str
    target_branch: str


class CandidateOutcome(CandidateCaptureAuthority):
    reason: Literal[
        "capture_timeout",
        "capture_failed",
        "base_unavailable",
        "base_digest_mismatch",
        "base_mismatch",
        "ledger_unavailable",
        "upload_failed",
        "patch_too_large",
    ]


class CandidateOutcomeRecorded(CandidateMessage):
    recorded: bool


class CandidateReserve(CandidateCaptureAuthority):
    dedupe_key: BoundedIdentity
    repository: CandidateRepository
    base_commit: CommitSha
    target_branch: Annotated[str, Field(min_length=1, max_length=1024, pattern=r"\S")]
    base_archive_limit_bytes: PositiveInt
    patch_limit_bytes: Annotated[StrictInt, Field(ge=1, le=PATCH_CEILING_BYTES)]
    changed_paths: Annotated[list[ChangedPath], Field(max_length=4096)]
    contains_workflow_files: StrictBool
    title: Annotated[str, Field(max_length=256, pattern=r"^[^\x00]*$")]
    body: Annotated[str, Field(max_length=65536, pattern=r"^[^\x00]*$")]
    objects: Annotated[list[CandidateObjectDeclaration], Field(min_length=1, max_length=2)]
    # The capture budget still left at this call; the ledger's capture deadline
    # is ``now`` plus it. Excluded from the idempotency digest.
    capture_budget_seconds: Annotated[float, Field(gt=0, le=60)] = 60.0

    @model_validator(mode="after")
    def _bounded(self) -> Self:
        kinds = [item.kind for item in self.objects]
        if len(set(kinds)) != len(kinds) or "patch" not in kinds:
            raise ValueError("declare exactly one patch, with an optional base")
        # The same text Postgres stores for the jsonb array, so the SQL octet
        # bound is never the first to refuse.
        encoded = json.dumps(self.changed_paths, ensure_ascii=False).encode()
        if len(encoded) > CHANGED_PATHS_MAX_OCTETS:
            raise ValueError("changed paths exceed their encoded bound")
        return self


class CandidateReservation(CandidateMessage):
    candidate_id: uuid.UUID
    objects: list[CandidateObjectReceipt]
    capture_deadline: datetime
    staging_expires_at: datetime
    # True when the dedupe identity matched an existing candidate, so this call
    # did not create it.
    replayed: bool


class CandidateComplete(CandidateCaptureAuthority):
    candidate_id: uuid.UUID
    receipts: Annotated[list[CandidateObjectReceipt], Field(min_length=1, max_length=2)]


class CandidateCompletion(CandidateMessage):
    candidate_id: uuid.UUID
    status: Literal["complete"] = "complete"
    completed_at: datetime


class CandidateFail(CandidateCaptureAuthority):
    candidate_id: uuid.UUID
    reason: Literal["upload_failed", "capture_failed", "capture_timeout"]


class CandidateCaptureFailure(CandidateMessage):
    candidate_id: uuid.UUID
    status: Literal["tombstoned"] = "tombstoned"


class CandidateAcquireReader(CandidateMessage):
    work_item_id: uuid.UUID
    candidate_id: uuid.UUID
    holder_id: BoundedIdentity


class CandidateReader(CandidateMessage):
    lease_id: uuid.UUID
    candidate_id: uuid.UUID
    holder_id: str
    expires_at: datetime
    objects: list[CandidateObjectReceipt]


class CandidateReleaseReader(CandidateMessage):
    lease_id: uuid.UUID
    holder_id: BoundedIdentity


class CandidateAcknowledged(CandidateMessage):
    released: bool = True


class CandidateClaimCollection(CandidateMessage):
    collector_id: BoundedIdentity
    limit: Annotated[StrictInt, Field(ge=1, le=100)] = 100


class CandidateCollectionClaim(CandidateMessage):
    object_id: uuid.UUID
    key: str
    claim_id: uuid.UUID
    version: int
    claimed_until: datetime


class CandidateCollectionClaims(CandidateMessage):
    claims: list[CandidateCollectionClaim]


class CandidateFinalizeCollection(CandidateMessage):
    collector_id: BoundedIdentity
    object_id: uuid.UUID
    claim_id: uuid.UUID
    version: PositiveInt


class CandidateFinalized(CandidateMessage):
    finalized: bool = True


class CandidatePublicPatch(CandidateMessage):
    size_bytes: Annotated[StrictInt, Field(ge=0)]
    sha256: Sha256


class CandidatePublicBase(CandidatePublicPatch):
    base_commit: CommitSha


class CandidatePublic(CandidateMessage):
    candidate_id: uuid.UUID
    origin_request_id: uuid.UUID
    owner_request_id: uuid.UUID
    previous_owner_request_id: uuid.UUID | None
    owner_transferred_at: datetime | None
    runtime_epoch: PositiveInt
    status: CandidateStatus
    created_at: datetime
    completed_at: datetime | None
    # The owning request's final disposition (its terminal_at), when it has one.
    final_disposition_at: datetime | None
    expires_at: datetime | None
    available: bool
    changed_path_count: Annotated[StrictInt, Field(ge=0, le=4096)]
    patch: CandidatePublicPatch | None
    base: CandidatePublicBase | None


class CandidatePublicList(CandidateMessage):
    work_item_id: uuid.UUID
    capture_outcome: Annotated[str, Field(max_length=128)] | None
    capture_outcome_at: datetime | None
    candidates: Annotated[list[CandidatePublic], Field(max_length=3)]
