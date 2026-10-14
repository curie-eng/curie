"""Principal resolution: ADR 0198 decision 7's ordered checks (#2910).

``resolve_principal`` never guesses. The first check that fails answers with a
named reason and nothing after it runs. The tenant and the receiving channel
identity come from the caller's connection (ADR 0168 decision 2), the
namespace's ``authority`` from the receiving installation, and the sender's
team and id from the provider mapping (ADR 0201). A link is found only by
exact match on ``(namespace, native id)``, among active links.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from typing import Literal

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from ..models import (
    ChannelIdentity,
    IdentityLink,
    IdentityNamespace,
    Principal,
    ProviderInstallation,
)
from .slack import SUBJECT_UNIDENTIFIED, SlackEvidence, derive

REASONS = (
    "linked",
    "identity_not_found",
    "identity_inactive",
    "installation_mismatch",
    "installation_unattached",
    "installation_disconnected",
    "subject_unidentified",
    "namespace_conflict",
    "provider_unsupported",
    "namespace_unknown",
    "namespace_disabled",
    "no_link",
    "linked_to_bot",
    "principal_inactive",
)

SLACK_WORKSPACE = "slack_workspace"


@dataclass(frozen=True)
class Resolution:
    status: Literal["resolved", "unresolved"]
    reason: str  # one of REASONS
    principal_id: uuid.UUID | None
    channel_identity_id: uuid.UUID | None
    namespace_id: uuid.UUID | None


def _unresolved(
    reason: str,
    *,
    channel_identity_id: uuid.UUID | None = None,
    namespace_id: uuid.UUID | None = None,
) -> Resolution:
    return Resolution("unresolved", reason, None, channel_identity_id, namespace_id)


async def resolve_principal(
    session: AsyncSession,
    *,
    tenant_id: uuid.UUID,
    provider: str,
    channel_identity: str,
    slack: SlackEvidence | None,
) -> Resolution:
    # 1-2. The receiving identity, keyed (tenant, provider, name), and active.
    identity = await session.scalar(
        select(ChannelIdentity).where(
            ChannelIdentity.tenant_id == tenant_id,
            ChannelIdentity.provider == provider,
            ChannelIdentity.name == channel_identity,
        )
    )
    if identity is None:
        return _unresolved("identity_not_found")
    identity_id = identity.id
    if identity.status != "active":
        return _unresolved("identity_inactive", channel_identity_id=identity_id)

    # 3. Its installation: not mismatched, attached, connected.
    if identity.installation_mismatch:
        return _unresolved("installation_mismatch", channel_identity_id=identity_id)
    if identity.provider_installation_id is None:
        return _unresolved("installation_unattached", channel_identity_id=identity_id)
    installation = await session.scalar(
        select(ProviderInstallation).where(
            ProviderInstallation.id == identity.provider_installation_id
        )
    )
    if installation is None or installation.status != "connected":
        return _unresolved("installation_disconnected", channel_identity_id=identity_id)

    # 4. The provider's mapping: Slack is the only realization so far.
    if provider != "slack":
        return _unresolved("provider_unsupported", channel_identity_id=identity_id)
    if slack is None:
        return _unresolved(SUBJECT_UNIDENTIFIED, channel_identity_id=identity_id)
    derived = derive(
        slack,
        identity_attributes=dict(identity.attributes or {}),
        installation_authority=installation.authority,
        installation_account=installation.external_account_id,
    )
    if isinstance(derived, str):
        return _unresolved(derived, channel_identity_id=identity_id)
    sender_team, native_id = derived

    # 5-6. The tenant holds that namespace, under the installation's authority.
    namespace = await session.scalar(
        select(IdentityNamespace).where(
            IdentityNamespace.tenant_id == tenant_id,
            IdentityNamespace.provider == provider,
            IdentityNamespace.authority == installation.authority,
            IdentityNamespace.kind == SLACK_WORKSPACE,
            IdentityNamespace.key == sender_team,
        )
    )
    if namespace is None:
        return _unresolved("namespace_unknown", channel_identity_id=identity_id)
    if namespace.status != "active":
        return _unresolved(
            "namespace_disabled", channel_identity_id=identity_id, namespace_id=namespace.id
        )

    # 7. An active link, by exact match; revoked links are never consulted.
    link = await session.scalar(
        select(IdentityLink).where(
            IdentityLink.identity_namespace_id == namespace.id,
            IdentityLink.provider_native_id == native_id,
            IdentityLink.revoked_at.is_(None),
        )
    )
    if link is None:
        return _unresolved("no_link", channel_identity_id=identity_id, namespace_id=namespace.id)
    if link.principal_id is None:
        return _unresolved(
            "linked_to_bot", channel_identity_id=identity_id, namespace_id=namespace.id
        )
    principal_status = await session.scalar(
        select(Principal.status).where(
            Principal.tenant_id == tenant_id, Principal.id == link.principal_id
        )
    )
    if principal_status != "active":
        return _unresolved(
            "principal_inactive", channel_identity_id=identity_id, namespace_id=namespace.id
        )
    return Resolution("resolved", "linked", link.principal_id, identity_id, namespace.id)


async def find_or_create_namespace(
    session: AsyncSession,
    *,
    tenant_id: uuid.UUID,
    provider: str,
    authority: str,
    kind: str,
    key: str,
) -> IdentityNamespace:
    """Atomic under concurrency: the insert never raises on a duplicate.

    ON CONFLICT names the one natural key; the other unique keys all contain
    the fresh ``id``, so they cannot collide. The caller commits.
    """

    await session.execute(
        insert(IdentityNamespace)
        .values(
            id=uuid.uuid4(),
            tenant_id=tenant_id,
            provider=provider,
            authority=authority,
            kind=kind,
            key=key,
        )
        .on_conflict_do_nothing(
            constraint="identity_namespaces_tenant_provider_authority_kind_key_key"
        )
    )
    namespace = await session.scalar(
        select(IdentityNamespace)
        .where(
            IdentityNamespace.tenant_id == tenant_id,
            IdentityNamespace.provider == provider,
            IdentityNamespace.authority == authority,
            IdentityNamespace.kind == kind,
            IdentityNamespace.key == key,
        )
        .execution_options(populate_existing=True)
    )
    if namespace is None:  # pragma: no cover - only a concurrent delete gets here
        raise RuntimeError("identity namespace vanished between insert and select")
    return namespace
