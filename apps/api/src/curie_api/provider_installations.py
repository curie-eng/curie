"""Provider installations: a connected external account (#2909, ADR 0166 step 4).

A provider installation is the connection a tenant authorised -- ADR 0166's
original meaning, restored by ADR 0193 decision 3 after ADR 0168 decision 1
had made it one row per channel identity. Who Curie speaks as through that
connection is :mod:`.channel_identities`, a separate table attached to this
one by ``provider_installation_id``.

``authority`` scopes ``external_account_id``: empty for a provider with one
global service (Slack, Google, Atlassian Cloud), the canonical hostname for a
self-hosted or per-host one (a GHES instance, an Atlassian Data Center base
URL), so a self-hosted installation's id cannot collide with the same id on
another host.

No row is created at boot: an installation exists only once its target is
known (ADR 0193 decision 3), so until #3039 lands, an operator creates it
through the admin routes and attaches a channel identity to it there too.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from .models import ProviderInstallation
from .schemas.provider_installations import ProviderInstallationCreate, ProviderInstallationUpdate

# Migration 0051's auto-provisioned tenant.
DEFAULT_TENANT_ID = uuid.UUID("00000000-0000-0000-0000-000000000001")


class InstallationConflict(Exception):
    """The (tenant, provider, authority, external_account_id) key is already taken."""


class InstallationInvalid(Exception):
    """A request the table rejects; the message never includes a submitted value."""


# Only named constraints are translated; anything else is a bug and re-raises.
_CONSTRAINT_ERRORS: dict[str, tuple[type[Exception], str]] = {
    "provider_installations_tenant_provider_authority_account_key": (
        InstallationConflict,
        "a provider installation for this tenant, provider, authority and "
        "external account already exists",
    ),
    "provider_installations_tenant_id_fkey": (
        InstallationInvalid,
        "tenant_id does not name a tenant",
    ),
    "provider_installations_installer_fkey": (
        InstallationInvalid,
        "installed_by_principal_id is not a principal of this tenant",
    ),
    # Raised on the REFERENCING table (channel_identities) when deleting an
    # installation that one or more identities are still attached to: the FK
    # is declared NO ACTION, so detaching is an explicit operation and
    # deletion cannot bypass it (ADR 0193 decision 3).
    "channel_identities_installation_fkey": (
        InstallationConflict,
        "cannot delete: one or more channel identities are still attached to "
        "this installation",
    ),
}


def _translate(exc: IntegrityError) -> Exception | None:
    constraint = getattr(exc.orig.__cause__, "constraint_name", None) if exc.orig else None
    if constraint not in _CONSTRAINT_ERRORS:
        return None
    kind, message = _CONSTRAINT_ERRORS[constraint]
    return kind(message)


async def _commit(session: AsyncSession) -> None:
    try:
        await session.commit()
    except IntegrityError as exc:
        await session.rollback()
        translated = _translate(exc)
        if translated is None:
            raise
        # `from None`: the IntegrityError carries the statement's parameters.
        raise translated from None


async def create_installation(
    session: AsyncSession, data: ProviderInstallationCreate
) -> ProviderInstallation:
    installation = ProviderInstallation(
        tenant_id=data.tenant_id or DEFAULT_TENANT_ID,
        provider=data.provider,
        authority=data.authority,
        external_account_id=data.external_account_id,
        display_name=data.display_name,
        status=data.status,
        # Caller-asserted: no principal-session auth on this admin router yet
        # (#2908/#3009), so there is nothing yet to cross-check it against.
        installed_by_principal_id=data.installed_by_principal_id,
        disconnected_at=datetime.now(UTC) if data.status == "disconnected" else None,
    )
    session.add(installation)
    await _commit(session)
    await session.refresh(installation)
    return installation


async def list_installations(
    session: AsyncSession,
    *,
    provider: str | None = None,
    tenant_id: uuid.UUID | None = None,
) -> list[ProviderInstallation]:
    query = select(ProviderInstallation).order_by(
        ProviderInstallation.installed_at, ProviderInstallation.id
    )
    if provider is not None:
        query = query.where(ProviderInstallation.provider == provider)
    if tenant_id is not None:
        query = query.where(ProviderInstallation.tenant_id == tenant_id)
    return list((await session.scalars(query)).all())


async def get_installation(
    session: AsyncSession, installation_id: uuid.UUID
) -> ProviderInstallation | None:
    return await session.get(ProviderInstallation, installation_id)


async def update_installation(
    session: AsyncSession,
    installation: ProviderInstallation,
    data: ProviderInstallationUpdate,
) -> ProviderInstallation:
    fields = data.model_fields_set
    for field in ("authority", "display_name", "external_account_id"):
        if field in fields:
            setattr(installation, field, getattr(data, field))
    if "status" in fields and data.status != installation.status:
        # disconnected_at marks exactly the disconnected span.
        installation.disconnected_at = datetime.now(UTC) if data.status == "disconnected" else None
        installation.status = data.status  # type: ignore[assignment]
    await _commit(session)
    await session.refresh(installation)
    return installation


async def delete_installation(session: AsyncSession, installation: ProviderInstallation) -> None:
    await session.delete(installation)
    await _commit(session)
