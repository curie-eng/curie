"""Slack identity reports: #3039's core (ADR 0198 decision 4).

The dispatcher calls ``auth.test`` with each Slack identity's own token and
reports what Slack said. The API cannot check that answer without holding the
token, so the trust boundary is the platform key, the same one that already
allows an operator to attach an identity by PATCH: this route adds no
privilege.

One transaction, and the lock order every writer shares (installation row,
then identity row) so concurrent reports, PATCHes and installation retargets
serialize instead of interleaving:

1. find or create the installation for the REPORTED team, then hold it FOR
   SHARE, so it cannot be retargeted under the attachment;
2. lock the identity FOR UPDATE, re-reading it;
3. record the answer under the reserved ``slack_auth_test`` key, and attach an
   unattached identity, clear the mismatch on a matching one, or set it
   (keeping the attachment) on one attached elsewhere;
4. find or create the reported team's namespace; commit.
"""

from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from ..channel_identities import check_attributes
from ..models import ChannelIdentity, ProviderInstallation
from .service import SLACK_WORKSPACE, find_or_create_namespace
from .slack import AUTH_TEST_KEY

_LOG = logging.getLogger("curie_api.identity.attach")

# A retarget can move the row out from under the FOR SHARE select between the
# insert and the lock; each retry inserts afresh, so one retry normally does.
_INSTALLATION_ATTEMPTS = 5


class IdentityNotFound(LookupError):
    """No Slack channel identity of that name in the tenant."""


@dataclass(frozen=True)
class SlackReportResult:
    identity_id: uuid.UUID
    # The installation the identity is attached to AFTER the report: on a
    # mismatch, the existing attachment, not the reported team's.
    provider_installation_id: uuid.UUID
    namespace_id: uuid.UUID
    installation_mismatch: bool


async def _installation_for_share(
    session: AsyncSession, *, tenant_id: uuid.UUID, team_id: str
) -> ProviderInstallation:
    for _ in range(_INSTALLATION_ATTEMPTS):
        await session.execute(
            insert(ProviderInstallation)
            .values(
                id=uuid.uuid4(),
                tenant_id=tenant_id,
                provider="slack",
                authority="",
                external_account_id=team_id,
            )
            .on_conflict_do_nothing(
                constraint="provider_installations_tenant_provider_authority_account_key"
            )
        )
        installation = await session.scalar(
            select(ProviderInstallation)
            .where(
                ProviderInstallation.tenant_id == tenant_id,
                ProviderInstallation.provider == "slack",
                ProviderInstallation.authority == "",
                ProviderInstallation.external_account_id == team_id,
            )
            .with_for_update(read=True)
            .execution_options(populate_existing=True)
        )
        if installation is not None:
            return installation
    raise RuntimeError("the reported installation kept moving; retry the report")


async def report_slack_identity(
    session: AsyncSession,
    *,
    tenant_id: uuid.UUID,
    name: str,
    team_id: str,
    enterprise_id: str | None,
    enterprise_id_present: bool,
    is_enterprise_install: bool | None,
) -> SlackReportResult:
    identity_key = (
        ChannelIdentity.tenant_id == tenant_id,
        ChannelIdentity.provider == "slack",
        ChannelIdentity.name == name,
    )
    # Checked before anything is created: an unknown name leaves no rows.
    if await session.scalar(select(ChannelIdentity.id).where(*identity_key)) is None:
        await session.rollback()
        raise IdentityNotFound(name)

    installation = await _installation_for_share(session, tenant_id=tenant_id, team_id=team_id)
    identity = await session.scalar(
        select(ChannelIdentity)
        .where(*identity_key)
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    if identity is None:  # deleted after the first check
        await session.rollback()
        raise IdentityNotFound(name)

    attributes = dict(identity.attributes or {})
    attributes[AUTH_TEST_KEY] = {
        "team_id": team_id,
        "enterprise_id": enterprise_id,
        "enterprise_id_present": enterprise_id_present,
        "is_enterprise_install": is_enterprise_install,
        "reported_at": datetime.now(UTC).isoformat(),
    }
    try:
        check_attributes(attributes)
    except Exception:
        await session.rollback()
        raise
    identity.attributes = attributes

    if identity.provider_installation_id is None:
        identity.provider_installation_id = installation.id
        identity.installation_mismatch = False
    elif identity.provider_installation_id == installation.id:
        identity.installation_mismatch = False
    else:
        # Kept attached: only fresh matching evidence or an operator's
        # reattach clears this (ADR 0198 decision 4).
        identity.installation_mismatch = True
        _LOG.warning(
            "slack identity report installation_mismatch identity=%s reported_team=%s "
            "attached_installation=%s",
            identity.name,
            team_id,
            identity.provider_installation_id,
        )

    namespace = await find_or_create_namespace(
        session,
        tenant_id=tenant_id,
        provider="slack",
        authority="",
        kind=SLACK_WORKSPACE,
        key=team_id,
    )
    result = SlackReportResult(
        identity_id=identity.id,
        provider_installation_id=identity.provider_installation_id,
        namespace_id=namespace.id,
        installation_mismatch=identity.installation_mismatch,
    )
    await session.commit()
    return result
