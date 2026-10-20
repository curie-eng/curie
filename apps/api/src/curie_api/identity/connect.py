"""Sender-bound, single-use account connection (ADR 0220 sections 3 and 4).

The caller owns the transaction. Only the provider resolver may establish the
chat account. Browser sign-in supplies the person; neither input is a mapping
chosen by an administrator. Token plaintext never enters a database row.
"""

from __future__ import annotations

import hashlib
import re
import secrets
import uuid
from datetime import UTC, datetime, timedelta

from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from .. import identity_audit
from ..crud.console import principal_is_active
from ..models import (
    IdentityConnectToken,
    IdentityLink,
    IdentityNamespace,
    Principal,
    Team,
    Tenant,
    TenantIdentityProvider,
)
from ..oidc import OidcClaims
from .service import find_or_create_namespace, resolve_principal
from .slack import SlackEvidence

TOKEN_TTL = timedelta(minutes=10)
PROMPT_SPACING = timedelta(minutes=1)
MAX_ACCOUNT_TOKENS = 5
TOKEN_PATTERN = re.compile(r"[A-Za-z0-9_-]{43}\Z")


class ConnectRefused(ValueError):
    """A bounded, non-sensitive rejection; never include token values."""


def hash_token(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


async def configured_provider(
    session: AsyncSession, tenant_id: uuid.UUID
) -> TenantIdentityProvider | None:
    config = await session.scalar(
        select(TenantIdentityProvider)
        .where(TenantIdentityProvider.tenant_id == tenant_id)
        .with_for_update(read=True)
        .execution_options(populate_existing=True)
    )
    tenant_status = await session.scalar(select(Tenant.status).where(Tenant.id == tenant_id))
    if config is None or tenant_status != "active":
        return None
    # A configured issuer alone must not turn an empty projection into an
    # identity authority. A successful full snapshot is required once.
    synced = await session.scalar(
        select(Team.id)
        .where(
            Team.tenant_id == tenant_id,
            Team.source == "idp_group",
            Team.managed_by == config.managed_by,
            Team.epoch > 0,
            Team.last_synced_at.is_not(None),
        )
        .limit(1)
    )
    return config if synced is not None else None


async def issue_for_sender(
    session: AsyncSession,
    *,
    tenant_id: uuid.UUID,
    provider: str,
    channel_identity: str,
    slack: SlackEvidence | None,
) -> str | None:
    resolution = await resolve_principal(
        session,
        tenant_id=tenant_id,
        provider=provider,
        channel_identity=channel_identity,
        slack=slack,
    )
    if resolution.reason != "no_link" or resolution.namespace_id is None or slack is None:
        return None
    # resolve_principal has already derived and verified this exact native id.
    native_id = slack.user_id
    if native_id is None:
        return None
    config = await configured_provider(session, tenant_id)
    namespace = await session.scalar(
        select(IdentityNamespace)
        .where(
            IdentityNamespace.id == resolution.namespace_id,
            IdentityNamespace.tenant_id == tenant_id,
        )
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    if namespace is None or namespace.status != "active" or config is None:
        return None
    existing = await session.scalar(
        select(IdentityLink.id).where(
            IdentityLink.identity_namespace_id == namespace.id,
            IdentityLink.provider_native_id == native_id,
            IdentityLink.revoked_at.is_(None),
        )
    )
    if existing is not None:
        return None
    now = datetime.now(UTC)
    account = (
        IdentityConnectToken.identity_namespace_id == namespace.id,
        IdentityConnectToken.provider_native_id == native_id,
    )
    # Bounded per-account storage: retain the current ten minute issuance
    # budget, deleting only old tokens for this exact trusted account.
    await session.execute(
        delete(IdentityConnectToken).where(
            *account,
            IdentityConnectToken.expires_at <= now,
        )
    )
    rows = list(await session.scalars(select(IdentityConnectToken).where(*account)))
    if len(rows) >= MAX_ACCOUNT_TOKENS or any(
        row.created_at > now - PROMPT_SPACING for row in rows
    ):
        return None
    raw = secrets.token_urlsafe(32)
    session.add(
        IdentityConnectToken(
            token_hash=hash_token(raw),
            tenant_id=tenant_id,
            identity_namespace_id=namespace.id,
            provider_native_id=native_id,
            idp_issuer=config.issuer,
            expires_at=now + TOKEN_TTL,
            created_at=now,
        )
    )
    await session.flush()
    return raw


async def live_token(
    session: AsyncSession, token: str, *, lock: bool = False
) -> IdentityConnectToken:
    if not TOKEN_PATTERN.fullmatch(token):
        raise ConnectRefused("invalid_connect")
    query = select(IdentityConnectToken).where(IdentityConnectToken.token_hash == hash_token(token))
    if lock:
        query = query.with_for_update()
    row = await session.scalar(query.execution_options(populate_existing=True))
    if row is None or row.consumed_at is not None or row.expires_at <= datetime.now(UTC):
        raise ConnectRefused("invalid_connect")
    config = await configured_provider(session, row.tenant_id)
    namespace = await session.get(
        IdentityNamespace, row.identity_namespace_id, populate_existing=True
    )
    if (
        config is None
        or config.issuer != row.idp_issuer
        or namespace is None
        or namespace.tenant_id != row.tenant_id
        or namespace.status != "active"
    ):
        raise ConnectRefused("invalid_connect")
    return row


async def preserve_verified_claims(
    session: AsyncSession,
    *,
    token: str,
    principal: Principal,
    claims: OidcClaims,
) -> None:
    row = await live_token(session, token, lock=True)
    if (
        principal.tenant_id != row.tenant_id
        or principal.idp_issuer != row.idp_issuer
        or claims.issuer != row.idp_issuer
        or claims.subject != principal.idp_subject
        or not await principal_is_active(session, principal)
    ):
        raise ConnectRefused("invalid_connect")
    # The strict boolean comes from the server-validated ID token. A stored
    # Principal.email is a display attribute and cannot substitute for proof.
    config = await configured_provider(session, row.tenant_id)
    parsed = (
        governed_email(claims.email, list(config.governed_domains))
        if claims.raw.get("email_verified") is True and claims.email and config
        else None
    )
    row.verified_email = parsed[0] if parsed else None
    row.verified_principal_id = principal.id
    row.verified_at = datetime.now(UTC)
    await session.flush()


def governed_email(email: str, domains: list[str]) -> tuple[str, str] | None:
    if len(email) > 320 or email != email.strip() or email.count("@") != 1:
        return None
    local, domain = email.rsplit("@", 1)
    domain = domain.lower()
    if not local or not domain or domain not in domains or any(ord(c) < 33 for c in email):
        return None
    # The mail transport and directory both use the normalized mailbox form
    # from schemas.channels.normalize_caller_id, including the local part.
    return f"{local}@{domain}".lower(), domain


async def link_verified_email(
    session: AsyncSession,
    *,
    tenant_id: uuid.UUID,
    principal: Principal,
    email: str,
    issuer: str,
) -> None:
    config = await configured_provider(session, tenant_id)
    if config is None or issuer != config.issuer:
        return
    parsed = governed_email(email, list(config.governed_domains))
    if parsed is None:
        return
    address, domain = parsed
    found_namespace = await find_or_create_namespace(
        session,
        tenant_id=tenant_id,
        provider="email",
        authority=issuer,
        kind="email_domain",
        key=domain,
    )
    namespace = await session.scalar(
        select(IdentityNamespace)
        .where(IdentityNamespace.id == found_namespace.id)
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    if namespace is None or namespace.status != "active":
        return
    existing = await session.scalar(
        select(IdentityLink).where(
            IdentityLink.identity_namespace_id == namespace.id,
            IdentityLink.provider_native_id == address,
            IdentityLink.revoked_at.is_(None),
        )
    )
    # Any existing live mapping is authoritative; in particular directory
    # reassignment always wins, and connect never replaces another person's link.
    if existing is not None:
        return
    session.add(
        IdentityLink(
            tenant_id=tenant_id,
            identity_namespace_id=namespace.id,
            provider_native_id=address,
            principal_id=principal.id,
            verification_source="provider_event_verified",
            created_by_principal_id=principal.id,
        )
    )
    await session.flush()


async def confirm(session: AsyncSession, *, token: str, principal: Principal) -> None:
    row = await live_token(session, token, lock=True)
    namespace_ids = {row.identity_namespace_id}
    if row.verified_principal_id == principal.id and row.verified_at and row.verified_email:
        config = await configured_provider(session, row.tenant_id)
        parsed = (
            governed_email(row.verified_email, list(config.governed_domains)) if config else None
        )
        if parsed is not None:
            email_namespace = await find_or_create_namespace(
                session,
                tenant_id=row.tenant_id,
                provider="email",
                authority=row.idp_issuer,
                kind="email_domain",
                key=parsed[1],
            )
            namespace_ids.add(email_namespace.id)
    # Directory ingestion uses this same deterministic namespace order before
    # locking principals. Locking chat then email can otherwise invert it.
    namespaces = list(
        await session.scalars(
            select(IdentityNamespace)
            .where(IdentityNamespace.id.in_(namespace_ids))
            .order_by(IdentityNamespace.id)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
    )
    namespace = next((ns for ns in namespaces if ns.id == row.identity_namespace_id), None)
    # Re-read the principal while holding a lock: concurrent deactivation and
    # connection serialize rather than admitting a stale in-memory principal.
    person = await session.scalar(
        select(Principal)
        .where(Principal.id == principal.id)
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    if (
        person is None
        or person.tenant_id != row.tenant_id
        or person.idp_issuer != row.idp_issuer
        or person.type != "human"
        or not await principal_is_active(session, person)
        or namespace is None
        or namespace.status != "active"
    ):
        raise ConnectRefused("invalid_connect")
    existing = await session.scalar(
        select(IdentityLink.id).where(
            IdentityLink.identity_namespace_id == namespace.id,
            IdentityLink.provider_native_id == row.provider_native_id,
            IdentityLink.revoked_at.is_(None),
        )
    )
    if existing is not None:
        raise ConnectRefused("account_already_linked")
    row.consumed_at = datetime.now(UTC)
    link = IdentityLink(
        tenant_id=row.tenant_id,
        identity_namespace_id=namespace.id,
        provider_native_id=row.provider_native_id,
        principal_id=person.id,
        verification_source="provider_event_verified",
        created_by_principal_id=person.id,
    )
    session.add(link)
    await session.flush()
    if (
        row.verified_principal_id == person.id
        and row.verified_at is not None
        and row.verified_email
    ):
        await link_verified_email(
            session,
            tenant_id=row.tenant_id,
            principal=person,
            email=row.verified_email,
            issuer=row.idp_issuer,
        )
    identity_audit.record(
        session,
        tenant_id=row.tenant_id,
        event_type="identity.account_connected",
        actor=f"principal:{person.id}",
        principal_id=person.id,
        details={"identity_namespace_id": str(namespace.id), "link_id": str(link.id)},
    )
