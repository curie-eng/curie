"""Internal persistence primitives; no authentication or provider I/O.

@spec HTTP-PRINCIPAL-ADOPTION-V1-1 to HTTP-PRINCIPAL-ADOPTION-V1-13.
"""

from __future__ import annotations

import uuid
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Literal

from pydantic import ValidationError
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from . import identity_audit
from .models import ConsoleSession, Principal, Tenant, TenantIdentityProvider
from .schemas.http_authority import AuthorityConfigV1, HttpSelectionV1, LocalProjectionSelectionV1
from .schemas.http_identity import (
    MAX_RESPONSE_BYTES,
    FactsRefusal,
    FreshnessLimits,
    LookupRequestV1,
    LookupResponseV1,
    validate_lookup,
)


class PrincipalAuthorityRefusal(ValueError):
    """@spec HTTP-PRINCIPAL-ADOPTION-V1-13. Carries only a bounded reason."""

    def __init__(self, code: str):
        """@spec HTTP-PRINCIPAL-ADOPTION-V1-13."""
        self.code = code
        super().__init__(code)


@dataclass(frozen=True)
class AuthorityWriteResult:
    """@spec HTTP-PRINCIPAL-ADOPTION-V1-3."""

    configuration: AuthorityConfigV1
    lifecycle_generation: int
    revoked_session_count: int


@dataclass(frozen=True)
class AuthorityCapture:
    """@spec HTTP-PRINCIPAL-ADOPTION-V1-3."""

    configuration: AuthorityConfigV1
    lifecycle_generation: int


@dataclass(frozen=True)
class PrincipalApplication:
    """@spec HTTP-PRINCIPAL-ADOPTION-V1-3."""

    outcome: Literal["resolved", "inactive", "unresolved"]
    principal_id: uuid.UUID | None
    authorization_version: int | None
    lifecycle_generation: int


def utc_now() -> datetime:
    """@spec HTTP-PRINCIPAL-ADOPTION-V1-6."""
    return datetime.now(UTC)


def _integer(value: int) -> None:
    """@spec HTTP-PRINCIPAL-ADOPTION-V1-3."""
    if type(value) is not int or value < 0:
        raise PrincipalAuthorityRefusal("invalid_authority_input")


def _transaction(session: AsyncSession) -> None:
    """@spec HTTP-PRINCIPAL-ADOPTION-V1-3."""
    if not session.in_transaction():
        raise PrincipalAuthorityRefusal("invalid_authority_input")


async def lock_scope(
    session: AsyncSession, tenant_id: uuid.UUID, *, require_active: bool = True
) -> tuple[Tenant, TenantIdentityProvider | None]:
    """@spec HTTP-PRINCIPAL-ADOPTION-V1-4. No global fence is acquired here."""
    if not isinstance(tenant_id, uuid.UUID):
        raise PrincipalAuthorityRefusal("invalid_authority_input")
    with session.no_autoflush:
        tenant = await session.scalar(
            select(Tenant)
            .where(Tenant.id == tenant_id)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
        if tenant is None or (require_active and tenant.status != "active"):
            raise PrincipalAuthorityRefusal("tenant_unavailable")
        row = await session.scalar(
            select(TenantIdentityProvider)
            .where(TenantIdentityProvider.tenant_id == tenant_id)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
    return tenant, row


def stored_configuration(row: TenantIdentityProvider) -> AuthorityConfigV1:
    """@spec HTTP-PRINCIPAL-ADOPTION-V1-2. Corruption never selects local mode."""
    selection: HttpSelectionV1 | LocalProjectionSelectionV1
    try:
        _integer(row.lifecycle_generation)
        if row.http_selection is not None:
            selection = HttpSelectionV1.model_validate(row.http_selection)
            if selection.issuer != row.issuer or row.lifecycle_generation < 1:
                raise ValueError
        else:
            selection = LocalProjectionSelectionV1.model_validate(
                dict(
                    mode="local_projection",
                    issuer=row.issuer,
                    governed_domains=row.governed_domains,
                    managed_by=row.managed_by,
                )
            )
        return AuthorityConfigV1(
            schema_version=1,
            tenant_id=str(row.tenant_id),
            version=row.version,
            selection=selection,
        )
    except (ValueError, TypeError, AttributeError):
        raise PrincipalAuthorityRefusal("invalid_authority_state") from None


async def read_authority(
    session: AsyncSession, *, tenant_id: uuid.UUID
) -> AuthorityConfigV1 | None:
    """@spec HTTP-PRINCIPAL-ADOPTION-V1-2."""
    _, row = await lock_scope(session, tenant_id)
    return stored_configuration(row) if row else None


async def require_local_scope(
    session: AsyncSession, tenant_id: uuid.UUID
) -> TenantIdentityProvider | None:
    """@spec HTTP-PRINCIPAL-ADOPTION-V1-9. Directory's existing error boundary."""
    from .directory_sync import DirectoryConflict, DirectorySyncError

    try:
        _, row = await lock_scope(session, tenant_id)
    except PrincipalAuthorityRefusal as exc:
        raise DirectorySyncError(exc.code) from None
    if row is not None and row.http_selection is not None:
        raise DirectoryConflict("authority_owned_by_http")
    return row


def _selection(selection: HttpSelectionV1) -> HttpSelectionV1:
    """@spec HTTP-PRINCIPAL-ADOPTION-V1-3."""
    try:
        if not isinstance(selection, HttpSelectionV1):
            raise ValueError
        return HttpSelectionV1.model_validate(selection)
    except (ValueError, TypeError, AttributeError):
        raise PrincipalAuthorityRefusal("invalid_authority_input") from None


def _expected(row: TenantIdentityProvider | None, version: int, generation: int) -> None:
    """@spec HTTP-PRINCIPAL-ADOPTION-V1-5."""
    if version != (row.version if row else 0):
        raise PrincipalAuthorityRefusal("stale_authority_version")
    if generation != (row.lifecycle_generation if row else 0):
        raise PrincipalAuthorityRefusal("stale_lifecycle_generation")


async def _revoke(session: AsyncSession, ids: list[uuid.UUID], now: datetime) -> int:
    """@spec HTTP-PRINCIPAL-ADOPTION-V1-8."""
    if not ids:
        return 0
    rows = list(
        await session.scalars(
            select(ConsoleSession)
            .where(ConsoleSession.principal_id.in_(ids), ConsoleSession.revoked_at.is_(None))
            .order_by(ConsoleSession.id)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
    )
    for row in rows:
        row.revoked_at = now.replace(tzinfo=None)
    return len(rows)


async def _write(
    session: AsyncSession,
    *,
    tenant_id: uuid.UUID,
    expected_version: int,
    expected_lifecycle_generation: int,
    selection: HttpSelectionV1,
    replace: bool,
) -> AuthorityWriteResult:
    """@spec HTTP-PRINCIPAL-ADOPTION-V1-3, HTTP-PRINCIPAL-ADOPTION-V1-5."""
    _transaction(session)
    _integer(expected_version)
    _integer(expected_lifecycle_generation)
    selection = _selection(selection)
    async with session.begin_nested():
        _, row = await lock_scope(session, tenant_id)
        if row:
            stored_configuration(row)
        _expected(row, expected_version, expected_lifecycle_generation)
        if replace:
            if row is None or row.http_selection is None:
                raise PrincipalAuthorityRefusal("http_authority_required")
        elif row is not None and row.http_selection is not None:
            raise PrincipalAuthorityRefusal("authority_already_adopted")
        if row is not None and row.issuer != selection.issuer:
            raise PrincipalAuthorityRefusal("authority_scope_mismatch")
        if row is None:
            row = TenantIdentityProvider(
                tenant_id=tenant_id,
                issuer=selection.issuer,
                governed_domains=None,
                managed_by=None,
                version=1,
                identity_epoch=0,
                lifecycle_generation=1,
            )
            session.add(row)
        else:
            row.version += 1
            row.lifecycle_generation += 1
        row.http_selection = selection.model_dump(mode="json")
        revoked = 0
        if not replace:
            principals = list(
                await session.scalars(
                    select(Principal)
                    .where(
                        Principal.tenant_id == tenant_id,
                        Principal.idp_issuer == selection.issuer,
                    )
                    .order_by(Principal.id)
                    .with_for_update()
                    .execution_options(populate_existing=True)
                )
            )
            revoked = await _revoke(
                session,
                [p.id for p in principals if p.status in {"disabled", "revoked"}],
                utc_now(),
            )
        identity_audit.record(
            session,
            tenant_id=tenant_id,
            event_type="identity.http_authority_changed"
            if replace
            else "identity.http_authority_adopted",
            actor="platform",
            details={
                "authority_version": row.version,
                "lifecycle_generation": row.lifecycle_generation,
                **({} if replace else {"revoked_session_count": revoked}),
            },
        )
        await session.flush()
        return AuthorityWriteResult(stored_configuration(row), row.lifecycle_generation, revoked)


async def adopt_http_authority(
    session: AsyncSession,
    *,
    tenant_id: uuid.UUID,
    expected_version: int,
    expected_lifecycle_generation: int,
    selection: HttpSelectionV1,
) -> AuthorityWriteResult:
    """@spec HTTP-PRINCIPAL-ADOPTION-V1-5."""
    return await _write(
        session,
        tenant_id=tenant_id,
        expected_version=expected_version,
        expected_lifecycle_generation=expected_lifecycle_generation,
        selection=selection,
        replace=False,
    )


async def replace_http_configuration(
    session: AsyncSession,
    *,
    tenant_id: uuid.UUID,
    expected_version: int,
    expected_lifecycle_generation: int,
    selection: HttpSelectionV1,
) -> AuthorityWriteResult:
    """@spec HTTP-PRINCIPAL-ADOPTION-V1-5."""
    return await _write(
        session,
        tenant_id=tenant_id,
        expected_version=expected_version,
        expected_lifecycle_generation=expected_lifecycle_generation,
        selection=selection,
        replace=True,
    )


async def capture_http_authority(
    sessionmaker: async_sessionmaker[AsyncSession], *, tenant_id: uuid.UUID
) -> AuthorityCapture:
    """@spec HTTP-PRINCIPAL-ADOPTION-V1-6."""
    async with sessionmaker() as session, session.begin():
        _, row = await lock_scope(session, tenant_id)
        if row is None:
            raise PrincipalAuthorityRefusal("http_authority_required")
        config = stored_configuration(row)
        if not isinstance(config.selection, HttpSelectionV1):
            raise PrincipalAuthorityRefusal("http_authority_required")
        result = AuthorityCapture(config, row.lifecycle_generation)
    return result


async def guarded_principal(
    session: AsyncSession,
    *,
    tenant_id: uuid.UUID,
    principal_id: uuid.UUID,
) -> Principal | None:
    """@spec HTTP-PRINCIPAL-ADOPTION-V1-10, HTTP-PRINCIPAL-ADOPTION-V1-11."""
    tenant, row = await lock_scope(session, tenant_id, require_active=False)
    if row is not None and row.http_selection is not None and tenant.status != "active":
        raise PrincipalAuthorityRefusal("tenant_unavailable")
    with session.no_autoflush:
        principal = await session.scalar(
            select(Principal)
            .where(Principal.id == principal_id)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
    if row is not None and row.http_selection is not None:
        config = stored_configuration(row)
        assert isinstance(config.selection, HttpSelectionV1)
        if principal is None or principal.tenant_id != tenant_id:
            raise PrincipalAuthorityRefusal("http_principal_not_provisioned")
        if principal.idp_issuer != config.selection.issuer:
            raise PrincipalAuthorityRefusal("authority_scope_mismatch")
        if principal.type != "human":
            raise PrincipalAuthorityRefusal("principal_kind_conflict")
        if principal.status != "active":
            raise PrincipalAuthorityRefusal("principal_inactive")
    return principal


async def apply_http_principal(
    session: AsyncSession,
    *,
    capture: AuthorityCapture,
    request: LookupRequestV1,
    response_bytes: bytes,
    clock: Callable[[], datetime] = utc_now,
) -> PrincipalApplication:
    """@spec HTTP-PRINCIPAL-ADOPTION-V1-6 to HTTP-PRINCIPAL-ADOPTION-V1-8."""
    _transaction(session)
    try:
        if not isinstance(capture, AuthorityCapture) or not isinstance(request, LookupRequestV1):
            raise ValueError
        config = AuthorityConfigV1.model_validate(capture.configuration)
        _integer(capture.lifecycle_generation)
        if capture.lifecycle_generation < 1 or not isinstance(config.selection, HttpSelectionV1):
            raise ValueError
        request = LookupRequestV1.model_validate(request)
        if not callable(clock):
            raise ValueError
    except (ValueError, TypeError, AttributeError):
        raise PrincipalAuthorityRefusal("invalid_authority_input") from None
    async with session.begin_nested():
        tenant_id = uuid.UUID(config.tenant_id)
        _, row = await lock_scope(session, tenant_id)
        if row is None:
            raise PrincipalAuthorityRefusal("stale_authority_version")
        current = stored_configuration(row)
        if not isinstance(current.selection, HttpSelectionV1):
            raise PrincipalAuthorityRefusal("authority_scope_mismatch")
        if current.selection.issuer != config.selection.issuer:
            raise PrincipalAuthorityRefusal("authority_scope_mismatch")
        if current != config:
            raise PrincipalAuthorityRefusal("stale_authority_version")
        if row.lifecycle_generation != capture.lifecycle_generation:
            raise PrincipalAuthorityRefusal("stale_lifecycle_generation")
        if request.authority_version != config.version:
            raise PrincipalAuthorityRefusal("stale_authority_version")
        if (
            request.scope.tenant_id != config.tenant_id
            or request.scope.expected_issuer != config.selection.issuer
        ):
            raise PrincipalAuthorityRefusal("authority_scope_mismatch")
        # This parse supplies only a lock key. T1 below owns all acceptance.
        preliminary = None
        if isinstance(response_bytes, bytes) and len(response_bytes) <= MAX_RESPONSE_BYTES:
            try:
                preliminary = LookupResponseV1.model_validate_json(response_bytes)
            except (ValidationError, ValueError):
                pass
        principal = None
        if preliminary is not None and preliminary.identity is not None:
            principal = await session.scalar(
                select(Principal)
                .where(
                    Principal.tenant_id == tenant_id,
                    Principal.idp_issuer == config.selection.issuer,
                    Principal.idp_subject == preliminary.identity.subject,
                )
                .order_by(Principal.id)
                .with_for_update()
                .execution_options(populate_existing=True)
            )
        try:
            now = clock()
            if not isinstance(now, datetime) or now.utcoffset() != timedelta(0):
                raise ValueError
        except Exception:  # noqa: BLE001 - do not expose a caller clock exception
            raise PrincipalAuthorityRefusal("invalid_authority_input") from None
        facts = validate_lookup(
            response_bytes,
            request,
            FreshnessLimits(
                max_identity_age_ms=config.selection.max_identity_age_ms,
                max_membership_age_ms=config.selection.max_membership_age_ms,
                max_clock_skew_ms=config.selection.max_clock_skew_ms,
            ),
            now=now,
        )
        if isinstance(facts, FactsRefusal):
            raise PrincipalAuthorityRefusal("identity_facts_" + facts.code)
        if facts.outcome == "unresolved":
            return PrincipalApplication("unresolved", None, None, row.lifecycle_generation)
        identity = facts.identity
        assert identity is not None
        if identity.kind == "agent":
            raise PrincipalAuthorityRefusal("unsupported_principal_kind")
        if principal is None:
            principal = Principal(
                id=uuid.uuid4(),
                tenant_id=tenant_id,
                idp_issuer=identity.issuer,
                idp_subject=identity.subject,
                type=identity.kind,
                status=identity.account_status,
                authorization_version=1,
            )
            session.add(principal)
            # The audit has a composite FK; flush the new canonical row first.
            await session.flush()
        elif principal.type != identity.kind:
            raise PrincipalAuthorityRefusal("principal_kind_conflict")
        elif principal.status != identity.account_status:
            principal.status = identity.account_status
            principal.authorization_version += 1
        revoked = 0
        if identity.account_status in {"disabled", "revoked"}:
            revoked = await _revoke(session, [principal.id], now)
        row.lifecycle_generation += 1
        identity_audit.record(
            session,
            tenant_id=tenant_id,
            event_type="identity.http_principal_observed",
            actor="http_authority",
            principal_id=principal.id,
            details=dict(
                status=principal.status,
                authorization_version=principal.authorization_version,
                lifecycle_generation=row.lifecycle_generation,
                revoked_session_count=revoked,
            ),
        )
        await session.flush()
        return PrincipalApplication(
            facts.outcome, principal.id, principal.authorization_version, row.lifecycle_generation
        )
