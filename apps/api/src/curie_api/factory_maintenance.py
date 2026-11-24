"""Admission port for repairs to an existing factory pull request."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal, Protocol
from uuid import UUID

MaintenanceReason = Literal["late_ci_red", "base_conflict"]


@dataclass(frozen=True, kw_only=True)
class MaintenanceObservation:
    """Expected state from the reconciler. Admission verifies it independently."""

    work_item_id: UUID
    request_id: UUID
    lineage_id: UUID
    work_item_version: int
    request_version: int
    lineage_version: int
    head_sha: str
    base_ref: str
    base_sha: str
    reason: MaintenanceReason


@dataclass(frozen=True, kw_only=True)
class MaintenanceRepair:
    """Receipt for a transaction that owns dispatch of the named request."""

    status: Literal["admitted", "replayed"]
    request_id: UUID
    mode: Literal["successor"] = "successor"


@dataclass(frozen=True, kw_only=True)
class MaintenanceAuthority:
    """Publication pins read from the admission by its repair request ID."""

    repair_request_id: UUID
    reason: MaintenanceReason
    head_sha: str
    base_ref: str
    base_sha: str


@dataclass(frozen=True, kw_only=True)
class MaintenanceInstructions:
    objective: str
    publication_mode: Literal["base_conflict_merge"] | None


class MaintenanceReasonHook(Protocol):
    def __call__(
        self, authority: MaintenanceAuthority, /, *, objective: str
    ) -> MaintenanceInstructions: ...


RefusalReason = Literal[
    "repair_disabled",
    "reason_unavailable",
    "work_item_cancelled",
    "pr_closed",
    "identity_unverified",
    "owner_changed",
    "enrollment_fenced",
    "superseded",
    "snapshot_changed",
    "foreign_head",
    "base_changed",
    "window_expired",
    "budget_exhausted",
    "source_not_completed",
    "prior_green_missing",
    "evidence_unavailable",
    "failure_not_attributable",
    "no_current_conflict",
    "retarget_pending",
]


@dataclass(frozen=True, kw_only=True)
class MaintenanceRefusal:
    reason: RefusalReason


class MaintenanceAdmission(Protocol):
    async def __call__(
        self, observation: MaintenanceObservation, /
    ) -> MaintenanceRepair | MaintenanceRefusal: ...
