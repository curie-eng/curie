"""Authenticated authority checks and atomic IdP projection replacement."""

from __future__ import annotations

import re
import uuid
from datetime import UTC, datetime

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from . import directory_sync, identity_audit, principal_authority
from .directory_credential import DirectoryClaims
from .models import IdentityLink, IdentityNamespace, Principal, TenantIdentityProvider
from .schemas.directory import DirectoryConfigIn, DirectoryPersonIn, DirectorySnapshotIn


async def configuration(
    session: AsyncSession,
    tenant_id: uuid.UUID,
    *,
    write: bool = False,
) -> TenantIdentityProvider:
    # @spec HTTP-PRINCIPAL-ADOPTION-V1-9.
    row = await principal_authority.require_local_scope(session, tenant_id)
    if row is None:
        raise directory_sync.DirectorySyncError("directory_not_configured")
    assert row.governed_domains is not None and row.managed_by is not None
    return row


async def update_configuration(
    session: AsyncSession,
    tenant_id: uuid.UUID,
    body: DirectoryConfigIn,
) -> TenantIdentityProvider:
    # @spec HTTP-PRINCIPAL-ADOPTION-V1-9.
    row = await principal_authority.require_local_scope(session, tenant_id)
    if body.expected_version != (row.version if row else 0):
        raise directory_sync.DirectoryConflict("stale_config_version")
    if row is None:
        row = TenantIdentityProvider(
            tenant_id=tenant_id,
            issuer=body.issuer,
            governed_domains=body.governed_domains,
            managed_by=body.managed_by,
            version=1,
            lifecycle_generation=1,
        )
        session.add(row)
    else:
        row.issuer, row.governed_domains, row.managed_by = (
            body.issuer,
            body.governed_domains,
            body.managed_by,
        )
        row.version += 1
        row.lifecycle_generation += 1
    identity_audit.record(
        session,
        tenant_id=tenant_id,
        event_type="identity.directory_configured",
        actor="platform",
        details={"version": row.version},
    )
    await session.flush()
    return row


async def authenticate(
    session: AsyncSession,
    claims: DirectoryClaims,
    *,
    write: bool = False,
) -> TenantIdentityProvider:
    config = await configuration(session, claims.scope.tenant_id, write=write)
    if (
        config.version != claims.config_version
        or config.issuer != claims.scope.issuer
        or config.managed_by != claims.scope.managed_by
    ):
        raise directory_sync.DirectorySyncError("directory_credential_revoked")
    return config


async def scoped_namespaces(
    session: AsyncSession,
    claims: DirectoryClaims,
    config: TenantIdentityProvider,
) -> dict[uuid.UUID, IdentityNamespace]:
    assert config.governed_domains is not None
    # The same namespace lock serializes connect confirmations and replacements.
    rows = list(
        await session.scalars(
            select(IdentityNamespace)
            .where(
                IdentityNamespace.id.in_(claims.namespace_ids),
                IdentityNamespace.tenant_id == claims.scope.tenant_id,
                IdentityNamespace.status == "active",
            )
            .order_by(IdentityNamespace.id)
            .with_for_update()
        )
    )
    if len(rows) != len(claims.namespace_ids):
        raise directory_sync.DirectorySyncError("namespace_out_of_scope")
    for row in rows:
        if row.kind == "email_domain" and (
            row.key not in config.governed_domains or row.authority != config.issuer
        ):
            raise directory_sync.DirectorySyncError("namespace_out_of_scope")
    return {row.id: row for row in rows}


def validated_accounts(
    people: list[DirectoryPersonIn],
    namespaces: dict[uuid.UUID, IdentityNamespace],
    config: TenantIdentityProvider,
) -> dict[str, set[tuple[uuid.UUID, str]]]:
    assert config.governed_domains is not None
    accounts: dict[str, set[tuple[uuid.UUID, str]]] = {}
    all_keys: set[tuple[uuid.UUID, str]] = set()
    for person in people:
        if person.subject in accounts:
            raise directory_sync.DirectorySyncError("duplicate_subject")
        keys: set[tuple[uuid.UUID, str]] = set()
        for account in person.accounts:
            ns = namespaces.get(account.identity_namespace_id)
            if ns is None:
                raise directory_sync.DirectorySyncError("namespace_out_of_scope")
            native = account.provider_native_id
            if ns.kind == "slack_workspace":
                if ns.provider != "slack" or not re.fullmatch(r"[UW][A-Z0-9]{2,63}", native):
                    raise directory_sync.DirectorySyncError("invalid_account")
            elif ns.kind == "email_domain":
                if (
                    ns.provider != "email"
                    or native != native.lower()
                    or native.count("@") != 1
                    or native.rsplit("@", 1)[1] != ns.key
                    or ns.key not in config.governed_domains
                    or not re.fullmatch(r"[^\s@]+@[^\s@]+", native)
                ):
                    raise directory_sync.DirectorySyncError("ungoverned_email")
            else:
                raise directory_sync.DirectorySyncError("unsupported_namespace")
            key = ns.id, native
            if key in keys:
                raise directory_sync.DirectorySyncError("duplicate_account")
            keys.add(key)
        # IdP email attributes are authoritative only inside declared domains.
        if person.email is not None:
            email = person.email.lower()
            if (
                email != person.email
                or not re.fullmatch(r"[^\s@]+@[^\s@]+", email)
                or email.rsplit("@", 1)[1] not in config.governed_domains
            ):
                raise directory_sync.DirectorySyncError("ungoverned_email")
            for ns in namespaces.values():
                if ns.kind == "email_domain" and ns.key == email.rsplit("@", 1)[1]:
                    keys.add((ns.id, email))
        if keys & all_keys:
            raise directory_sync.DirectorySyncError("duplicate_account")
        all_keys.update(keys)
        accounts[person.subject] = keys
    return accounts


async def ingest_snapshot(
    session: AsyncSession,
    claims: DirectoryClaims,
    config: TenantIdentityProvider,
    team_id: uuid.UUID,
    body: DirectorySnapshotIn,
) -> directory_sync.SnapshotResult:
    async with session.begin_nested():
        # @spec HTTP-PRINCIPAL-ADOPTION-V1-9. Never trust a pre-lock ORM config.
        config = await authenticate(session, claims, write=True)
        created = False
        await directory_sync.locked_team(session, claims.scope, team_id, body.expected_epoch)
        if not body.complete:
            raise directory_sync.DirectorySyncError("incomplete_snapshot")
        namespaces = await scoped_namespaces(session, claims, config)
        accounts = validated_accounts(body.people, namespaces, config)
        if body.expected_identity_epoch is None:
            if claims.namespace_ids:
                raise directory_sync.DirectorySyncError("identity_epoch_required")
        elif body.expected_identity_epoch != config.identity_epoch:
            raise directory_sync.DirectoryConflict("stale_identity_epoch")
        # @spec HTTP-PRINCIPAL-ADOPTION-V1-4. Lock existing people by UUID,
        # before subject-ordered creation/profile work under the same scope.
        await session.scalars(
            select(Principal).where(
                Principal.tenant_id == claims.scope.tenant_id,
                Principal.idp_issuer == claims.scope.issuer,
                Principal.idp_subject.in_([person.subject for person in body.people]),
            ).order_by(Principal.id).with_for_update()
            .execution_options(populate_existing=True)
        )
        principal_ids: list[uuid.UUID] = []
        now = datetime.now(UTC)
        for person in sorted(body.people, key=lambda person: person.subject):
            # Unique issuer+subject is the only identity key. Concurrent group
            # ingestion cannot race a second human into existence.
            inserted = await session.scalar(
                insert(Principal)
                .values(
                    id=uuid.uuid4(),
                    tenant_id=claims.scope.tenant_id,
                    idp_issuer=claims.scope.issuer,
                    idp_subject=person.subject,
                    type="human",
                    status=person.status,
                    display_name=person.display_name,
                    email=person.email,
                )
                .on_conflict_do_nothing(constraint="principals_tenant_issuer_subject_key")
                .returning(Principal.id)
            )
            created = created or inserted is not None
            principal = await session.scalar(
                select(Principal)
                .where(
                    Principal.tenant_id == claims.scope.tenant_id,
                    Principal.idp_issuer == claims.scope.issuer,
                    Principal.idp_subject == person.subject,
                )
                .with_for_update()
                .execution_options(populate_existing=True)
            )
            if principal is None or principal.type != "human":
                raise directory_sync.DirectorySyncError("principal_out_of_scope")
            principal.display_name, principal.email = person.display_name, person.email
            principal_ids.append(principal.id)
            desired = accounts[person.subject]
            existing = list(
                await session.scalars(
                    select(IdentityLink).where(
                        IdentityLink.tenant_id == claims.scope.tenant_id,
                        IdentityLink.identity_namespace_id.in_(claims.namespace_ids),
                        IdentityLink.principal_id == principal.id,
                        IdentityLink.revoked_at.is_(None),
                        IdentityLink.verification_source == "idp_directory",
                    )
                )
            )
            for link in existing:
                if (link.identity_namespace_id, link.provider_native_id) not in desired:
                    link.revoked_at = now
                    identity_audit.record(
                        session,
                        tenant_id=claims.scope.tenant_id,
                        event_type="identity.directory_link_revoked",
                        actor=claims.scope.managed_by,
                        principal_id=principal.id,
                        details={"link_id": str(link.id)},
                    )
            for namespace_id, native in sorted(desired):
                current = await session.scalar(
                    select(IdentityLink).where(
                        IdentityLink.identity_namespace_id == namespace_id,
                        IdentityLink.provider_native_id == native,
                        IdentityLink.revoked_at.is_(None),
                    )
                )
                if (
                    current is not None
                    and current.principal_id == principal.id
                    and current.verification_source == "idp_directory"
                    and current.created_by_directory == claims.scope.managed_by
                ):
                    continue
                if current is not None:
                    current.revoked_at = now
                    identity_audit.record(
                        session,
                        tenant_id=claims.scope.tenant_id,
                        event_type="identity.directory_link_revoked",
                        actor=claims.scope.managed_by,
                        principal_id=current.principal_id,
                        details={"link_id": str(current.id)},
                    )
                    await session.flush()
                link = IdentityLink(
                    tenant_id=claims.scope.tenant_id,
                    identity_namespace_id=namespace_id,
                    provider_native_id=native,
                    principal_id=principal.id,
                    verification_source="idp_directory",
                    created_by_directory=claims.scope.managed_by,
                    verified_at=now,
                )
                session.add(link)
                await session.flush()
                identity_audit.record(
                    session,
                    tenant_id=claims.scope.tenant_id,
                    event_type="identity.directory_link_created",
                    actor=claims.scope.managed_by,
                    principal_id=principal.id,
                    details={"link_id": str(link.id)},
                )
        result = await directory_sync.apply_snapshot(
            session,
            scope=claims.scope,
            team_id=team_id,
            expected_epoch=body.expected_epoch,
            principal_ids=principal_ids,
            complete=True,
        )
        if created:
            config.lifecycle_generation += 1
        if claims.namespace_ids:
            # This must roll back together with links, membership and audits.
            config.identity_epoch += 1
            await session.flush()
        await session.flush()
        return result
