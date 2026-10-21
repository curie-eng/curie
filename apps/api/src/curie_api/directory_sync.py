"""Generic directory projection writes for ADR 0220, independent of any IdP.

This is a transaction primitive, not a remotely exposed ingestion API. A
DirectoryScope is supplied only by a trusted in-process caller; constructing
one is not authentication. The directory router verifies a scoped credential
before calling this primitive. Audit rows share each projection transaction.

The caller owns the transaction and commits on success. Each operation uses a
savepoint so a failed write cannot leave a partially replaced team in a caller
that catches the error. Scope and epoch checks are repeated while holding the
team row lock. Account status has its own versioned path: replacing membership
never reactivates a suspended person.
"""

from __future__ import annotations

import hashlib
import uuid
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, datetime
from urllib.parse import urlsplit

from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from . import identity_audit
from .models import Principal, PrincipalTeam, Team

MAX_MEMBERS = 10_000
FAILURE_CODES = frozenset({"provider_unavailable", "provider_rejected", "invalid_snapshot"})


class DirectorySyncError(ValueError):
    """A bounded reason token, never raw provider data or credentials."""


class DirectoryConflict(DirectorySyncError):
    """Another writer changed the projection or principal first."""


@dataclass(frozen=True)
class DirectoryScope:
    tenant_id: uuid.UUID
    issuer: str
    managed_by: str
    team_ids: frozenset[uuid.UUID]

    def __post_init__(self) -> None:
        try:
            issuer = urlsplit(self.issuer)
            valid = (
                issuer.scheme == "https"
                and bool(issuer.hostname)
                and issuer.username is None
                and issuer.password is None
                and not issuer.query
                and not issuer.fragment
                and isinstance(self.tenant_id, uuid.UUID)
                and bool(self.managed_by.strip())
                and len(self.managed_by) <= 128
                and isinstance(self.team_ids, frozenset)
                and bool(self.team_ids)
                and all(isinstance(team, uuid.UUID) for team in self.team_ids)
            )
        except (TypeError, ValueError, AttributeError):
            valid = False
        if not valid:
            raise DirectorySyncError("invalid_scope")


@dataclass(frozen=True)
class SnapshotResult:
    epoch: int
    member_count: int
    digest: str


def require_version(value: int) -> None:
    if type(value) is not int or value < 0:
        raise DirectorySyncError("invalid_version")


async def locked_team(
    session: AsyncSession,
    scope: DirectoryScope,
    team_id: uuid.UUID,
    expected_epoch: int,
) -> Team:
    require_version(expected_epoch)
    if team_id not in scope.team_ids:
        raise DirectorySyncError("team_out_of_scope")
    team = await session.scalar(
        select(Team)
        .where(
            Team.id == team_id,
            Team.tenant_id == scope.tenant_id,
        )
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    if team is None or team.source != "idp_group" or team.managed_by != scope.managed_by:
        raise DirectorySyncError("team_out_of_scope")
    if team.epoch != expected_epoch:
        raise DirectoryConflict("stale_epoch")
    return team


async def apply_snapshot(
    session: AsyncSession,
    *,
    scope: DirectoryScope,
    team_id: uuid.UUID,
    expected_epoch: int,
    principal_ids: Iterable[uuid.UUID],
    complete: bool,
) -> SnapshotResult:
    """Replace one managed team's complete membership, with optimistic fencing.

    Principals already exist as IdP projections. Unknown, cross-tenant and
    other-issuer people are refused rather than created or guessed here.
    """
    if complete is not True:
        raise DirectorySyncError("incomplete_snapshot")
    ids: set[uuid.UUID] = set()
    for value in principal_ids:
        if not isinstance(value, uuid.UUID) or value in ids or len(ids) >= MAX_MEMBERS:
            raise DirectorySyncError("invalid_members")
        ids.add(value)
    async with session.begin_nested():
        team = await locked_team(session, scope, team_id, expected_epoch)
        if ids:
            found = set(
                await session.scalars(
                    select(Principal.id).where(
                        Principal.id.in_(ids),
                        Principal.tenant_id == scope.tenant_id,
                        Principal.idp_issuer == scope.issuer,
                        Principal.type == "human",
                    )
                )
            )
            if found != ids:
                raise DirectorySyncError("principal_out_of_scope")
        epoch = team.epoch + 1
        now = datetime.now(UTC)
        digest = hashlib.sha256("\n".join(sorted(map(str, ids))).encode("ascii")).hexdigest()
        await session.execute(
            delete(PrincipalTeam).where(
                PrincipalTeam.team_id == team.id,
                PrincipalTeam.tenant_id == scope.tenant_id,
            )
        )
        session.add_all(
            PrincipalTeam(
                principal_id=person,
                team_id=team.id,
                tenant_id=scope.tenant_id,
                source="idp_group",
                version=epoch,
                synced_at=now,
            )
            for person in sorted(ids)
        )
        team.epoch, team.member_count, team.digest = epoch, len(ids), digest
        team.last_synced_at, team.last_attempt_at, team.last_error = now, now, None
        identity_audit.record(
            session,
            tenant_id=scope.tenant_id,
            event_type="identity.directory_snapshot_applied",
            actor=scope.managed_by,
            team_id=team.id,
            details={"epoch": epoch, "member_count": len(ids), "digest": digest},
        )
        await session.flush()
    return SnapshotResult(epoch, len(ids), digest)


async def record_failure(
    session: AsyncSession,
    *,
    scope: DirectoryScope,
    team_id: uuid.UUID,
    expected_epoch: int,
    error: str,
) -> None:
    """Record a failed attempt without altering the last successful snapshot."""
    if error not in FAILURE_CODES:
        raise DirectorySyncError("invalid_failure_code")
    async with session.begin_nested():
        team = await locked_team(session, scope, team_id, expected_epoch)
        team.last_attempt_at, team.last_error = datetime.now(UTC), error
        identity_audit.record(
            session,
            tenant_id=scope.tenant_id,
            event_type="identity.directory_sync_failed",
            actor=scope.managed_by,
            team_id=team.id,
            details={"epoch": team.epoch, "error": error},
        )
        await session.flush()


async def set_account_status(
    session: AsyncSession,
    *,
    scope: DirectoryScope,
    principal_id: uuid.UUID,
    expected_version: int,
    status: str,
) -> int:
    """Apply an independently versioned IdP account lifecycle event."""
    require_version(expected_version)
    if status not in {"active", "disabled", "revoked"}:
        raise DirectorySyncError("invalid_status")
    async with session.begin_nested():
        principal = await session.scalar(
            select(Principal)
            .where(
                Principal.id == principal_id,
                Principal.tenant_id == scope.tenant_id,
                Principal.idp_issuer == scope.issuer,
                Principal.type == "human",
            )
            .with_for_update()
            .execution_options(populate_existing=True)
        )
        if principal is None:
            raise DirectorySyncError("principal_out_of_scope")
        if principal.authorization_version != expected_version:
            raise DirectoryConflict("stale_authorization_version")
        principal.status = status
        principal.authorization_version += 1
        version = principal.authorization_version
        identity_audit.record(
            session,
            tenant_id=scope.tenant_id,
            event_type="identity.account_status_changed",
            actor=scope.managed_by,
            principal_id=principal.id,
            details={"authorization_version": version, "status": status},
        )
        await session.flush()
    return version
