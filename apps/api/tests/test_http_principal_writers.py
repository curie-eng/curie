"""Direct writer, connection, callback and mint participation in adoption.

@spec HTTP-PRINCIPAL-ADOPTION-V1-9 HTTP-PRINCIPAL-ADOPTION-V1-10
@spec HTTP-PRINCIPAL-ADOPTION-V1-11 HTTP-PRINCIPAL-ADOPTION-V1-13.
"""

from __future__ import annotations

import uuid
from typing import Any

import pytest
from curie_api import directory_service, directory_sync
from curie_api.crud import console
from curie_api.directory_credential import DirectoryClaims
from curie_api.identity import connect
from curie_api.models import (
    ConsoleSession,
    IdentityConnectToken,
    IdentityLink,
    Principal,
    Tenant,
    TenantIdentityProvider,
)
from curie_api.routers import console as console_router
from curie_api.schemas.directory import DirectoryConfigIn, DirectorySnapshotIn
from principal_adoption_support import (
    ISSUER,
    adopt,
    api,
    claims,
    connection_fixture,
    cookie,
    principal,
    run,
    seed,
    team,
)
from sqlalchemy import func, select, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from starlette.requests import Request


@pytest.mark.parametrize(
    "writer",
    [
        "configuration",
        "authenticate",
        "update_configuration",
        "ingest_snapshot",
        "set_account_status",
        "locked_team",
        "apply_snapshot",
        "record_failure",
    ],
)
def test_each_direct_directory_writer_rechecks_adopted_owner(clean_db: None, writer: str) -> None:
    """@spec HTTP-PRINCIPAL-ADOPTION-V1-9 HTTP-PRINCIPAL-ADOPTION-V1-13."""
    api()

    async def body(f: async_sessionmaker[AsyncSession]) -> None:
        """@spec HTTP-PRINCIPAL-ADOPTION-V1-9 HTTP-PRINCIPAL-ADOPTION-V1-13."""
        tenant, person_id = await seed(f, local=True, status="active")
        async with f() as old:
            stale = await old.get(TenantIdentityProvider, tenant)
            async with f() as s, s.begin():
                group = await team(s, tenant, synced=False)
            await adopt(f, tenant, local=True)
            scope = directory_sync.DirectoryScope(
                tenant, ISSUER, "synthetic-directory", frozenset({group.id})
            )
            # Even a synthesized current-version credential cannot restore local ownership.
            credentials = DirectoryClaims(scope, frozenset(), 4, 4_000_000_000)
            async with f() as s, s.begin():
                with pytest.raises(
                    directory_sync.DirectoryConflict, match="^authority_owned_by_http$"
                ):
                    if writer == "configuration":
                        await directory_service.configuration(s, tenant)
                    elif writer == "authenticate":
                        await directory_service.authenticate(s, credentials)
                    elif writer == "update_configuration":
                        await directory_service.update_configuration(
                            s,
                            tenant,
                            DirectoryConfigIn(
                                issuer=ISSUER,
                                governed_domains=["example.test"],
                                managed_by="synthetic-directory",
                                expected_version=4,
                            ),
                        )
                    elif writer == "ingest_snapshot":
                        await directory_service.ingest_snapshot(
                            s,
                            credentials,
                            stale,
                            group.id,
                            DirectorySnapshotIn(
                                expected_epoch=0,
                                complete=True,
                                people=[
                                    {"subject": "NewSubject", "status": "active", "accounts": []}
                                ],
                            ),
                        )
                    elif writer == "set_account_status":
                        await directory_sync.set_account_status(
                            s,
                            scope=scope,
                            principal_id=person_id,
                            expected_version=4,
                            status="disabled",
                        )
                    elif writer == "locked_team":
                        await directory_sync.locked_team(s, scope, group.id, 0)
                    elif writer == "apply_snapshot":
                        await directory_sync.apply_snapshot(
                            s,
                            scope=scope,
                            team_id=group.id,
                            expected_epoch=0,
                            principal_ids=[person_id],
                            complete=True,
                        )
                    else:
                        await directory_sync.record_failure(
                            s,
                            scope=scope,
                            team_id=group.id,
                            expected_epoch=0,
                            error="provider_unavailable",
                        )
        async with f() as s:
            assert (await s.get(TenantIdentityProvider, tenant)).version == 4
            person_row = await s.get(Principal, person_id)
            assert (person_row.status, person_row.authorization_version) == ("active", 4)
            assert (
                await s.scalar(
                    select(func.count()).select_from(Principal).where(Principal.tenant_id == tenant)
                )
                == 1
            )

    run(body)


@pytest.mark.parametrize("mode", ["none", "local", "http"])
@pytest.mark.parametrize(
    ("case", "code"),
    [
        ("unknown", "http_principal_not_provisioned"),
        ("issuer", "authority_scope_mismatch"),
        ("service", "principal_kind_conflict"),
        ("human", None),
    ],
)
def test_oidc_first_create_and_profile_behavior_are_mode_specific(
    clean_db: None, mode: str, case: str, code: str | None
) -> None:
    """@spec HTTP-PRINCIPAL-ADOPTION-V1-9 HTTP-PRINCIPAL-ADOPTION-V1-10."""
    service = api()

    async def body(f: async_sessionmaker[AsyncSession]) -> None:
        """@spec HTTP-PRINCIPAL-ADOPTION-V1-9 HTTP-PRINCIPAL-ADOPTION-V1-10."""
        tenant, person_id = await seed(
            f,
            local=mode == "local",
            status=None if case == "unknown" else "active",
            kind="service" if case == "service" else "human",
        )
        if mode == "http":
            await adopt(f, tenant)
        value = claims(issuer="https://other.example.test" if case == "issuer" else ISSUER)
        async with f() as s, s.begin():
            if mode == "http" and code:
                with pytest.raises(service.PrincipalAuthorityRefusal, match=f"^{code}$"):
                    await console.resolve_principal(s, value, tenant_id=tenant)
            else:
                person_row = await console.resolve_principal(s, value, tenant_id=tenant)
                assert (person_row.email, person_row.display_name) == (
                    "new@example.test",
                    "Fresh name",
                )
                if case == "human":
                    assert person_row.id == person_id
        async with f() as s:
            row = await s.get(TenantIdentityProvider, tenant)
            if mode == "none":
                assert row is None
            elif mode == "local":
                assert row.lifecycle_generation == (1 if case in {"unknown", "issuer"} else 0)
                assert (row.version, row.identity_epoch) == (3, 7)
            elif code:
                assert row.lifecycle_generation == 1

    run(body)


@pytest.mark.parametrize("inactive", ["disabled", "revoked", "tenant"])
@pytest.mark.parametrize("path", ["direct", "exchange"])
def test_stale_active_object_cannot_mint_or_consume_principal_code(
    clean_db: None, inactive: str, path: str
) -> None:
    """@spec HTTP-PRINCIPAL-ADOPTION-V1-11 HTTP-PRINCIPAL-ADOPTION-V1-13."""
    service = api()

    async def body(f: async_sessionmaker[AsyncSession]) -> None:
        """@spec HTTP-PRINCIPAL-ADOPTION-V1-11 HTTP-PRINCIPAL-ADOPTION-V1-13."""
        tenant, person_id = await seed(f, status="active")
        await adopt(f, tenant)
        async with f() as s, s.begin():
            code, row = await cookie(s, person_id, consumed=False)
        async with f() as stale_session:
            stale = await stale_session.get(Principal, person_id)
            async with f() as s, s.begin():
                # Deliberately inconsistent row: exchange must enforce stored status itself.
                if inactive == "tenant":
                    (await s.get(Tenant, tenant)).status = "suspended"
                else:
                    await s.execute(
                        update(Principal).where(Principal.id == person_id).values(status=inactive)
                    )
            assert stale.status == "active"
            if path == "direct":
                expected = "tenant_unavailable" if inactive == "tenant" else "principal_inactive"
                with pytest.raises(service.PrincipalAuthorityRefusal, match=f"^{expected}$"):
                    await console.create_principal_console_session(stale_session, stale)
            else:
                assert await console.exchange_console_login_code(stale_session, code) is None
            assert stale_session.in_transaction(), "refusal must not commit the caller transaction"
            await stale_session.rollback()
        async with f() as s:
            saved = await s.get(ConsoleSession, row.id)
            assert saved.consumed_at is None and saved.session_token_hash is None
            assert (
                await s.scalar(
                    select(func.count())
                    .select_from(ConsoleSession)
                    .where(ConsoleSession.principal_id == person_id)
                )
                == 1
            )

    run(body)


@pytest.mark.parametrize(
    ("case", "expected"),
    [
        ("tenant", "tenant_unavailable"),
        ("missing", "http_principal_not_provisioned"),
        ("issuer", "authority_scope_mismatch"),
        ("kind", "principal_kind_conflict"),
        ("disabled", "principal_inactive"),
        ("revoked", "principal_inactive"),
    ],
)
def test_direct_mint_refusal_order_is_precise_and_never_commits_profile(
    clean_db: None, case: str, expected: str
) -> None:
    """@spec HTTP-PRINCIPAL-ADOPTION-V1-11 HTTP-PRINCIPAL-ADOPTION-V1-13."""
    service = api()

    async def body(f: async_sessionmaker[AsyncSession]) -> None:
        """@spec HTTP-PRINCIPAL-ADOPTION-V1-11 HTTP-PRINCIPAL-ADOPTION-V1-13."""
        tenant, person_id = await seed(f, status="active")
        await adopt(f, tenant)
        async with f() as s:
            stale = await s.get(Principal, person_id)
            stale.display_name = "Uncommitted profile"
            async with f() as writer, writer.begin():
                row = await writer.get(Principal, person_id)
                if case == "tenant":
                    (await writer.get(Tenant, tenant)).status = "suspended"
                    row.type, row.status = "service", "disabled"
                elif case == "missing":
                    await writer.delete(row)
                elif case == "issuer":
                    row.idp_issuer, row.type, row.status = (
                        "https://other.example.test",
                        "service",
                        "disabled",
                    )
                elif case == "kind":
                    row.type, row.status = "service", "disabled"
                else:
                    row.status = case
            # Keep the caller object stale without allowing autoflush to overwrite DB state.
            with s.no_autoflush:
                with pytest.raises(service.PrincipalAuthorityRefusal, match=f"^{expected}$"):
                    await console.create_principal_console_session(s, stale)
            await s.rollback()
        async with f() as s:
            assert (
                await s.scalar(
                    select(func.count())
                    .select_from(ConsoleSession)
                    .where(ConsoleSession.principal_id == person_id)
                )
                == 0
            )
            row = await s.get(Principal, person_id)
            assert row is None or row.display_name == "Stored name"

    run(body)


@pytest.mark.parametrize(
    ("retained", "synced", "mismatch", "available"),
    [
        (True, True, False, True),
        (False, True, False, False),
        (True, False, False, False),
        (True, True, True, False),
    ],
)
def test_retained_connection_metadata_has_separate_exact_scope_predicate(
    clean_db: None, retained: bool, synced: bool, mismatch: bool, available: bool
) -> None:
    """@spec HTTP-PRINCIPAL-ADOPTION-V1-2 HTTP-PRINCIPAL-ADOPTION-V1-10."""
    api()

    async def body(f: async_sessionmaker[AsyncSession]) -> None:
        """@spec HTTP-PRINCIPAL-ADOPTION-V1-2 HTTP-PRINCIPAL-ADOPTION-V1-10."""
        tenant, _ = await seed(f, local=retained)
        await adopt(f, tenant, local=retained)
        async with f() as s, s.begin():
            await team(s, tenant, synced=synced)
            if mismatch:
                (await s.get(TenantIdentityProvider, tenant)).issuer = "https://other.example.test"
        async with f() as s:
            result = await connect.configured_provider(s, tenant)
            assert (result is not None) == available
            if result:
                assert result.issuer == ISSUER and result.governed_domains == ["example.test"]
            if not mismatch:
                with pytest.raises(
                    directory_sync.DirectoryConflict, match="^authority_owned_by_http$"
                ):
                    await directory_service.configuration(s, tenant)

    run(body)


@pytest.mark.parametrize("consumer", ["preserve_verified_claims", "confirm", "link_verified_email"])
@pytest.mark.parametrize("stored", ["active", "disabled", "service"])
def test_retained_connection_proofs_reload_active_human_before_writes(
    clean_db: None, consumer: str, stored: str
) -> None:
    """@spec HTTP-PRINCIPAL-ADOPTION-V1-10."""
    api()

    async def body(f: async_sessionmaker[AsyncSession]) -> None:
        """@spec HTTP-PRINCIPAL-ADOPTION-V1-10."""
        tenant, person_id = await seed(f, local=True, status="active")
        await adopt(f, tenant, local=True)
        async with f() as s, s.begin():
            await team(s, tenant)
            token, namespace = await connection_fixture(s, tenant)
        async with f() as s:
            stale = await s.get(Principal, person_id)
            async with f() as writer, writer.begin():
                row = await writer.get(Principal, person_id)
                if stored == "disabled":
                    row.status = "disabled"
                elif stored == "service":
                    row.type = "service"

            async def invoke() -> None:
                """@spec HTTP-PRINCIPAL-ADOPTION-V1-10."""
                if consumer == "preserve_verified_claims":
                    await connect.preserve_verified_claims(
                        s, token=token, principal=stale, claims=claims()
                    )
                elif consumer == "confirm":
                    await connect.confirm(s, token=token, principal=stale)
                else:
                    await connect.link_verified_email(
                        s,
                        tenant_id=tenant,
                        principal=stale,
                        email="new@example.test",
                        issuer=ISSUER,
                    )

            if stored == "active":
                await invoke()
                await s.commit()
            else:
                try:
                    await invoke()
                except (connect.ConnectRefused, api().PrincipalAuthorityRefusal):
                    pass
                await s.commit()
        async with f() as s:
            row = await s.get(IdentityConnectToken, connect.hash_token(token))
            links = list(
                await s.scalars(select(IdentityLink).where(IdentityLink.tenant_id == tenant))
            )
            if stored != "active":
                assert row.verified_principal_id is None and row.consumed_at is None and not links
            elif consumer == "preserve_verified_claims":
                assert (
                    row.verified_principal_id == person_id
                    and row.verified_email == "new@example.test"
                )
            else:
                assert len(links) == 1 and links[0].principal_id == person_id
                if consumer == "confirm":
                    assert (
                        links[0].identity_namespace_id == namespace.id
                        and row.consumed_at is not None
                    )

    run(body)


@pytest.mark.parametrize("branch", ["ordinary", "connect"])
@pytest.mark.parametrize("case", ["unknown", "issuer", "service", "disabled", "revoked", "tenant"])
def test_callback_ownership_and_mint_refusals_rollback_and_remain_indistinguishable(
    clean_db: None, monkeypatch: pytest.MonkeyPatch, branch: str, case: str
) -> None:
    """
    @spec HTTP-PRINCIPAL-ADOPTION-V1-10 HTTP-PRINCIPAL-ADOPTION-V1-11
    HTTP-PRINCIPAL-ADOPTION-V1-13.
    """
    api()

    async def body(f: async_sessionmaker[AsyncSession]) -> None:
        """@spec HTTP-PRINCIPAL-ADOPTION-V1-10 HTTP-PRINCIPAL-ADOPTION-V1-11"""
        tenant, person_id = await seed(
            f,
            local=branch == "connect",
            status=None if case == "unknown" else "active",
            kind="service" if case == "service" else "human",
        )
        await adopt(f, tenant, local=branch == "connect")
        token = None
        if branch == "connect":
            async with f() as s, s.begin():
                await team(s, tenant)
                token, _ = await connection_fixture(s, tenant)
        async with f() as s:
            start = await console.create_oidc_login_attempt(s)
            stale = await s.get(Principal, person_id) if person_id else None
            await s.rollback()
            # Keep a deliberately stale active object for the mint/proof paths.
            if case in {"disabled", "revoked", "tenant"}:
                async with f() as writer, writer.begin():
                    if case == "tenant":
                        (await writer.get(Tenant, tenant)).status = "suspended"
                    else:
                        (await writer.get(Principal, person_id)).status = case
                stale = Principal(
                    id=person_id,
                    tenant_id=tenant,
                    idp_issuer=ISSUER,
                    idp_subject="SampleSubject",
                    type="human",
                    status="active",
                    display_name="Uncommitted profile",
                )
            original = console.resolve_principal

            async def resolver(session: AsyncSession, value: Any, **kwargs: Any) -> Principal:
                """@spec HTTP-PRINCIPAL-ADOPTION-V1-11. Exercise wrapper with stale caller."""
                if case in {"disabled", "revoked", "tenant"}:
                    await session.execute(
                        update(Principal)
                        .where(Principal.id == person_id)
                        .values(display_name="Uncommitted profile")
                    )
                    return stale
                return await original(session, value, tenant_id=tenant)

            async def exchange(code: str, verifier: str) -> str:
                """@spec HTTP-PRINCIPAL-ADOPTION-V1-4. OIDC I/O boundary has no DB transaction."""
                assert not s.in_transaction()
                return "synthetic-id-token"

            async def validate(token_value: str, *, nonce: str) -> Any:
                """@spec HTTP-PRINCIPAL-ADOPTION-V1-10."""
                return claims(issuer="https://other.example.test" if case == "issuer" else ISSUER)

            async def no_limit(*args: Any, **kwargs: Any) -> None:
                """@spec HTTP-PRINCIPAL-ADOPTION-V1-10. No unrelated shared limiter."""

            monkeypatch.setattr(console_router, "_require_oidc_enabled", lambda: None)
            monkeypatch.setattr(console_router, "limit_oidc", no_limit)
            monkeypatch.setattr(console_router.oidc, "exchange_code", exchange)
            monkeypatch.setattr(console_router.oidc, "validate_id_token", validate)
            monkeypatch.setattr(console_router.oidc, "meets_required_claims", lambda *args: True)
            monkeypatch.setattr(console, "resolve_principal", resolver)
            headers = (
                [(b"cookie", f"{console_router.CONNECT_COOKIE}={token}".encode())] if token else []
            )
            request = Request(
                {
                    "type": "http",
                    "method": "GET",
                    "scheme": "https",
                    "path": "/console/oidc/callback",
                    "headers": headers,
                    "server": ("console.example.test", 443),
                    "query_string": b"",
                }
            )
            response = await console_router.oidc_callback(
                request, s, code="synthetic-code", state=start.state, state_cookie=start.state
            )
            reference = console_router._refuse_callback("synthetic reference refusal")
            assert response.status_code == 401 and response.body == reference.body
            cookies = response.headers.getlist("set-cookie")
            assert not any(console_router.SESSION_COOKIE in value for value in cookies)
            if token:
                assert any(
                    console_router.CONNECT_COOKIE in value and "Max-Age=0" in value
                    for value in cookies
                )
            # A caller committing after a refused callback must not persist its refresh.
            await s.commit()
        async with f() as observer:
            if person_id:
                assert (await observer.get(Principal, person_id)).display_name == "Stored name"
            assert (
                await observer.scalar(
                    select(func.count())
                    .select_from(ConsoleSession)
                    .where(
                        ConsoleSession.principal_id.in_(
                            select(Principal.id).where(Principal.tenant_id == tenant)
                        )
                    )
                )
                == 0
            )
            if case == "unknown":
                assert await principal(observer, tenant) is None

    run(body)


def test_local_generation_bookkeeping_preserves_status_epochs_and_live_cookie(
    clean_db: None,
) -> None:
    """@spec HTTP-PRINCIPAL-ADOPTION-V1-9. Local suspension has no permanent bulk revocation."""
    api()

    async def body(f: async_sessionmaker[AsyncSession]) -> None:
        """@spec HTTP-PRINCIPAL-ADOPTION-V1-9. Local suspension has no permanent bulk revocation."""
        tenant, person_id = await seed(f, local=True, status="active")
        async with f() as s, s.begin():
            group = await team(s, tenant, synced=False)
            _, row = await cookie(s, person_id)
            config = await directory_service.update_configuration(
                s,
                tenant,
                DirectoryConfigIn(
                    issuer=ISSUER,
                    governed_domains=["example.test"],
                    managed_by="synthetic-directory",
                    expected_version=3,
                ),
            )
            assert (config.version, config.lifecycle_generation, config.identity_epoch) == (4, 1, 7)
            scope = directory_sync.DirectoryScope(
                tenant, ISSUER, "synthetic-directory", frozenset({group.id})
            )
            credentials = DirectoryClaims(scope, frozenset(), 4, 4_000_000_000)
            snapshot = DirectorySnapshotIn(
                expected_epoch=0,
                complete=True,
                people=[
                    {
                        "subject": "SampleSubject",
                        "status": "revoked",
                        "display_name": "Refresh",
                        "accounts": [],
                    },
                    {"subject": "AnotherSubject", "status": "active", "accounts": []},
                ],
            )
            result = await directory_service.ingest_snapshot(
                s, credentials, config, group.id, snapshot
            )
            assert result.epoch == 1 and config.lifecycle_generation == 2
            assert (await principal(s, tenant)).status == "active"
            snapshot.expected_epoch = 1
            await directory_service.ingest_snapshot(s, credentials, config, group.id, snapshot)
            assert config.lifecycle_generation == 2
            version = await directory_sync.set_account_status(
                s, scope=scope, principal_id=person_id, expected_version=4, status="disabled"
            )
            assert version == 5 and config.lifecycle_generation == 3
            assert config.identity_epoch == 7 and config.version == 4
            assert (await s.get(ConsoleSession, row.id)).revoked_at is None

    run(body)


def test_subject_only_login_code_and_random_consumed_oidc_half_keep_legacy_contract(
    clean_db: None,
) -> None:
    """@spec HTTP-PRINCIPAL-ADOPTION-V1-11."""
    api()

    async def body(f: async_sessionmaker[AsyncSession]) -> None:
        """@spec HTTP-PRINCIPAL-ADOPTION-V1-11."""
        tenant, person_id = await seed(f, status="active")
        await adopt(f, tenant)
        async with f() as s:
            code, legacy = await console.create_console_login_code(s, subject="U0EXAMPLE1")
            exchanged = await console.exchange_console_login_code(s, code)
            assert exchanged is not None and exchanged[1].subject == "U0EXAMPLE1"
            assert exchanged[1].principal_id is None
            token, oidc = await console.create_principal_console_session(
                s, await s.get(Principal, person_id)
            )
            assert token and oidc.consumed_at is not None
            # Even a known synthetic code cannot redeem a consumed OIDC half.
            known = "known-synthetic-code"
            oidc.login_code_hash = console.hash_console_credential(known)
            await s.commit()
            assert await console.exchange_console_login_code(s, known) is None

    run(body)


@pytest.mark.parametrize("route", ["read", "update", "provision", "credential"])
def test_existing_platform_key_directory_paths_map_owner_conflict_to_409(
    clean_db: None, route: str
) -> None:
    """@spec HTTP-PRINCIPAL-ADOPTION-V1-9 HTTP-PRINCIPAL-ADOPTION-V1-13."""
    from curie_api.routers import directory as routes
    from curie_api.schemas.directory import DirectoryCredentialIn, DirectoryTeamIn
    from fastapi import HTTPException, Response

    api()

    async def body(f: async_sessionmaker[AsyncSession]) -> None:
        """@spec HTTP-PRINCIPAL-ADOPTION-V1-9 HTTP-PRINCIPAL-ADOPTION-V1-13."""
        tenant, _ = await seed(f, local=True)
        await adopt(f, tenant, local=True)
        async with f() as s:
            with pytest.raises(HTTPException) as exc:
                if route == "read":
                    await routes.get_directory_config(tenant, s)
                elif route == "update":
                    await routes.put_directory_config(
                        tenant,
                        DirectoryConfigIn(
                            issuer=ISSUER,
                            governed_domains=["example.test"],
                            managed_by="synthetic-directory",
                            expected_version=4,
                        ),
                        s,
                    )
                elif route == "provision":
                    await routes.provision_directory_team(
                        DirectoryTeamIn(
                            tenant_id=tenant, external_id="fresh-synthetic-group", name="Fresh"
                        ),
                        s,
                    )
                else:
                    await routes.issue_directory_credential(
                        DirectoryCredentialIn(
                            tenant_id=tenant,
                            expected_config_version=4,
                            team_ids=[uuid.uuid4()],
                            namespace_ids=[],
                        ),
                        s,
                        Response(),
                    )
            assert exc.value.status_code == 409
            assert exc.value.detail == "authority_owned_by_http"
            await s.commit()
        async with f() as s:
            assert (await s.get(TenantIdentityProvider, tenant)).version == 4

    run(body)
