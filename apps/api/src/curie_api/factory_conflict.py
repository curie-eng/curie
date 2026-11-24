"""Verify the Git conflict and describe the admitted successor's resolution task."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from .factory_maintenance import (
    MaintenanceAuthority,
    MaintenanceInstructions,
    MaintenanceObservation,
    RefusalReason,
)
from .factory_rebase import RebaseRefused, RebaseSnapshot, inspect_same_base_conflicts
from .models import ThreadPublicationLineage
from .repository_access import IssuedCredential


@dataclass(frozen=True)
class VerifiedConflict:
    objective: str
    paths: tuple[str, ...]


async def inspect_conflict_objective(
    observation: MaintenanceObservation,
    lineage: ThreadPublicationLineage,
    credential: IssuedCredential,
    observed_at: datetime,
    objective: str,
) -> tuple[VerifiedConflict | None, RefusalReason | None]:
    repository = lineage.repository
    if repository is None:
        return None, "identity_unverified"
    if lineage.base_ref != observation.base_ref:
        return None, "base_changed"
    if lineage.base_sha == observation.base_sha:
        return None, "no_current_conflict"
    snapshot = RebaseSnapshot(
        transition_id=observation.request_id,
        repository=repository,
        branch=lineage.branch,
        old_base_sha=lineage.base_sha,
        old_head_sha=observation.head_sha,
        target_ref=observation.base_ref,
        target_sha=observation.base_sha,
        committer_name="Curie",
        committer_email="factory@example.com",
        committer_time=observed_at,
    )
    try:
        paths = await inspect_same_base_conflicts(snapshot, credential)
    except RebaseRefused as exc:
        if exc.code == "head_changed":
            return None, "foreign_head"
        if exc.code == "target_changed":
            return None, "base_changed"
        if exc.code == "no_conflict":
            return None, "no_current_conflict"
        return None, "evidence_unavailable"
    return VerifiedConflict(objective, paths), None


def conflict_instructions(
    authority: MaintenanceAuthority, /, *, objective: str
) -> MaintenanceInstructions:
    return MaintenanceInstructions(
        publication_mode="base_conflict_merge",
        objective=(
            f"{objective}\n\nResolve the conflict on the existing pull request. "
            f"The workspace starts at the admitted base {authority.base_sha}. "
            "The local ref refs/curie/conflict-head names the approved PR head "
            f"{authority.head_sha}. "
            "Run git merge --squash refs/curie/conflict-head. Resolve and stage the conflicts. "
            "Run the repository's tests for both the PR change and the base change, "
            "including a case that exercises both together where applicable. "
            "If both intentions are clear and the checks pass, submit the resolution for "
            "publication. If the intentions disagree or either check fails, report the "
            "unresolved behavior for human review. Leave the base branch and PR target in place."
        ),
    )
