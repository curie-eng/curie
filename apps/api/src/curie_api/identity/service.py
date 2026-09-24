"""Which principal is this provider-native id? (#2910, ADR 0155 step 5).

``resolve_principal`` answers from ``identity_links`` alone, by exact equality
on ``(tenant, installation, native id)``. It never reads a principal's email
or display name and never folds case, trims or prefix-matches: a near miss is
``no_link``, not a guess. Unresolved is an answer, not an error, so every
outcome is a ``PrincipalResolution`` and nothing here raises for a miss.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from typing import Literal

from sqlalchemy import exists, select
from sqlalchemy.ext.asyncio import AsyncSession

from ..models import IdentityLink, Principal, ProviderInstallation

ResolutionStatus = Literal["resolved", "unresolved"]
ResolutionReason = Literal[
    "linked",
    "no_link",
    "linked_to_bot",
    "principal_inactive",
    "installation_not_found",
    "installation_disconnected",
]


@dataclass(frozen=True)
class PrincipalResolution:
    status: ResolutionStatus
    # Set only when resolved: an inactive principal's id is not handed out.
    principal_id: uuid.UUID | None
    reason: ResolutionReason
    # The installation the answer is about; None when none was found.
    provider_installation_id: uuid.UUID | None


def _unresolved(
    reason: ResolutionReason, provider_installation_id: uuid.UUID | None
) -> PrincipalResolution:
    return PrincipalResolution(
        status="unresolved",
        principal_id=None,
        reason=reason,
        provider_installation_id=provider_installation_id,
    )


async def find_provider_installation(
    session: AsyncSession, *, tenant_id: uuid.UUID, provider: str, external_account_id: str
) -> uuid.UUID | None:
    """The installation for an external account (a Slack team id), by its unique key."""
    installation_id: uuid.UUID | None = await session.scalar(
        select(ProviderInstallation.id).where(
            ProviderInstallation.tenant_id == tenant_id,
            ProviderInstallation.provider == provider,
            ProviderInstallation.external_account_id == external_account_id,
        )
    )
    return installation_id


async def resolve_principal(
    session: AsyncSession,
    *,
    tenant_id: uuid.UUID,
    provider_installation_id: uuid.UUID,
    provider_subject: str,
) -> PrincipalResolution:
    """Resolve a provider-native id on one installation to an active principal.

    The installation is checked first, in the caller's tenant, so an id from
    another tenant is ``installation_not_found`` whichever locator found it. A
    principal link wins over bot links; it is unique, and the bot check is an
    EXISTS because one bot user may front many Agents.
    """
    installation_status = await session.scalar(
        select(ProviderInstallation.status).where(
            ProviderInstallation.tenant_id == tenant_id,
            ProviderInstallation.id == provider_installation_id,
        )
    )
    if installation_status is None:
        return _unresolved("installation_not_found", None)
    if installation_status == "disconnected":
        return _unresolved("installation_disconnected", provider_installation_id)

    link_matches = (
        IdentityLink.tenant_id == tenant_id,
        IdentityLink.provider_installation_id == provider_installation_id,
        IdentityLink.provider_native_id == provider_subject,
    )
    principal = (
        await session.execute(
            select(Principal.id, Principal.status)
            .join(
                IdentityLink,
                (IdentityLink.tenant_id == Principal.tenant_id)
                & (IdentityLink.principal_id == Principal.id),
            )
            .where(*link_matches)
        )
    ).one_or_none()
    if principal is not None:
        if principal.status != "active":
            return _unresolved("principal_inactive", provider_installation_id)
        return PrincipalResolution(
            status="resolved",
            principal_id=principal.id,
            reason="linked",
            provider_installation_id=provider_installation_id,
        )

    linked_to_bot = await session.scalar(
        select(exists().where(*link_matches, IdentityLink.bot_id.is_not(None)))
    )
    if linked_to_bot:
        return _unresolved("linked_to_bot", provider_installation_id)
    return _unresolved("no_link", provider_installation_id)
