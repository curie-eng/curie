"""Deterministic scope races, observed on test-owned Postgres backends.

@spec HTTP-PRINCIPAL-ADOPTION-V1-3 HTTP-PRINCIPAL-ADOPTION-V1-4
@spec HTTP-PRINCIPAL-ADOPTION-V1-6 HTTP-PRINCIPAL-ADOPTION-V1-8
@spec HTTP-PRINCIPAL-ADOPTION-V1-9 HTTP-PRINCIPAL-ADOPTION-V1-11.
"""

from __future__ import annotations

import asyncio
from datetime import timedelta
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
    IdentityNamespace,
    Principal,
    Team,
    Tenant,
    TenantIdentityProvider,
)
from curie_api.schemas.directory import DirectorySnapshotIn
from principal_adoption_support import (
    ISSUER,
    NOW,
    adopt,
    api,
    apply,
    blocked,
    capture,
    claims,
    connection_fixture,
    cookie,
    pid,
    principal,
    run,
    seed,
    selection,
    team,
)
from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker


@pytest.mark.parametrize("first", ["legacy", "adopt"])
@pytest.mark.parametrize("writer", ["oidc", "snapshot", "status"])
def test_legacy_global_then_scope_and_new_scope_only_have_no_cycle(
    clean_db: None, first: str, writer: str
) -> None:
    """@spec HTTP-PRINCIPAL-ADOPTION-V1-4 HTTP-PRINCIPAL-ADOPTION-V1-9."""
    service = api()

    async def body(f: async_sessionmaker[AsyncSession]) -> None:
        """@spec HTTP-PRINCIPAL-ADOPTION-V1-4 HTTP-PRINCIPAL-ADOPTION-V1-9."""
        tenant, person_id = await seed(
            f, local=True, status="active" if writer == "status" else None
        )
        async with f() as setup, setup.begin():
            group = await team(setup, tenant, synced=False)
            if person_id:
                _, old_cookie = await cookie(setup, person_id)
        scope = directory_sync.DirectoryScope(
            tenant, ISSUER, "synthetic-directory", frozenset({group.id})
        )
        async with f() as legacy, f() as new, f() as observer:
            legacy_pid, new_pid = await pid(legacy), await pid(new)
            old_config = await legacy.get(TenantIdentityProvider, tenant)
            # This is the actual advisory fence key used by identity_fence.mutation.
            await legacy.execute(text("SELECT pg_advisory_xact_lock(2250220)"))

            async def legacy_write() -> Any:
                """@spec HTTP-PRINCIPAL-ADOPTION-V1-9."""
                if writer == "oidc":
                    return await console.resolve_principal(legacy, claims(), tenant_id=tenant)
                if writer == "snapshot":
                    return await directory_service.ingest_snapshot(
                        legacy,
                        DirectoryClaims(scope, frozenset(), 3, 4_000_000_000),
                        old_config,
                        group.id,
                        DirectorySnapshotIn(
                            expected_epoch=0,
                            complete=True,
                            people=[
                                {"subject": "SampleSubject", "status": "active", "accounts": []}
                            ],
                        ),
                    )
                return await directory_sync.set_account_status(
                    legacy,
                    scope=scope,
                    principal_id=person_id,
                    expected_version=4,
                    status="disabled",
                )

            async def adoption(generation: int) -> Any:
                """@spec HTTP-PRINCIPAL-ADOPTION-V1-4."""
                return await service.adopt_http_authority(
                    new,
                    tenant_id=tenant,
                    expected_version=3,
                    expected_lifecycle_generation=generation,
                    selection=selection(),
                )

            if first == "legacy":
                await legacy_write()
                task = asyncio.create_task(adoption(1))
                try:
                    await blocked(observer, new_pid, legacy_pid)
                    await legacy.commit()
                    result = await task
                    await new.commit()
                    assert result.lifecycle_generation == 2
                finally:
                    if not task.done():
                        task.cancel()
                        await asyncio.gather(task, return_exceptions=True)
            else:
                await adoption(0)
                task = asyncio.create_task(legacy_write())
                try:
                    await blocked(observer, legacy_pid, new_pid)
                    # Must not ask for the global fence already held by legacy.
                    await new.commit()
                    error = (
                        service.PrincipalAuthorityRefusal
                        if writer == "oidc"
                        else directory_sync.DirectoryConflict
                    )
                    expected = (
                        "http_principal_not_provisioned"
                        if writer == "oidc"
                        else "authority_owned_by_http"
                    )
                    if writer == "status":
                        with pytest.raises(error, match=f"^{expected}$"):
                            await task
                    else:
                        with pytest.raises(error, match=f"^{expected}$"):
                            await task
                    await legacy.rollback()
                finally:
                    if not task.done():
                        task.cancel()
                        await asyncio.gather(task, return_exceptions=True)
        async with f() as s:
            row = await principal(s, tenant)
            if writer in {"oidc", "snapshot"}:
                assert (row is not None) == (first == "legacy")
            else:
                assert row.status == ("disabled" if first == "legacy" else "active")
                revoked = (await s.get(ConsoleSession, old_cookie.id)).revoked_at
                assert (revoked is not None) == (first == "legacy")

    run(body)


def test_capture_closes_scope_and_global_fence_before_return(clean_db: None) -> None:
    """@spec HTTP-PRINCIPAL-ADOPTION-V1-3 HTTP-PRINCIPAL-ADOPTION-V1-4."""
    api()

    async def body(f: async_sessionmaker[AsyncSession]) -> None:
        """@spec HTTP-PRINCIPAL-ADOPTION-V1-3 HTTP-PRINCIPAL-ADOPTION-V1-4."""
        tenant, _ = await seed(f)
        await adopt(f, tenant)
        captured = await capture(f, tenant)
        async with f() as s, s.begin():
            await s.execute(text("SET LOCAL lock_timeout = '500ms'"))
            assert (
                await s.scalar(
                    select(Tenant.id).where(Tenant.id == tenant).with_for_update(nowait=True)
                )
                == tenant
            )
            assert await s.scalar(text("SELECT pg_try_advisory_xact_lock(2250220)")) is True
        assert captured.configuration.tenant_id == str(tenant)

    run(body)


@pytest.mark.parametrize("expiry", ["identity", "membership"])
@pytest.mark.parametrize("wait_on", ["tenant", "principal"])
@pytest.mark.parametrize("boundary", ["equal", "later"])
def test_freshness_clock_after_real_lock_wait_preserves_t1_equality(
    clean_db: None, expiry: str, wait_on: str, boundary: str
) -> None:
    """@spec HTTP-PRINCIPAL-ADOPTION-V1-6. T1 equality is fresh; strictly later is stale."""
    service = api()

    async def body(f: async_sessionmaker[AsyncSession]) -> None:
        """@spec HTTP-PRINCIPAL-ADOPTION-V1-6. Revalidate at the post-lock clock."""
        tenant, person_id = await seed(f, status="active")
        async with f() as s, s.begin():
            await service.adopt_http_authority(
                s,
                tenant_id=tenant,
                expected_version=0,
                expected_lifecycle_generation=0,
                selection=selection(
                    max_identity_age_ms=2000 if expiry == "identity" else 10000,
                    max_membership_age_ms=2000,
                ),
            )
        cap = await capture(f, tenant)
        clock_value = NOW
        calls: list[Any] = []

        def clock() -> Any:
            """@spec HTTP-PRINCIPAL-ADOPTION-V1-6."""
            calls.append(clock_value)
            return clock_value

        async with f() as holder, f() as writer, f() as observer:
            holder_pid, writer_pid = await pid(holder), await pid(writer)
            model, key = (Tenant, tenant) if wait_on == "tenant" else (Principal, person_id)
            await holder.execute(select(model.id).where(model.id == key).with_for_update())
            task = asyncio.create_task(apply(writer, cap, clock=clock))
            try:
                await blocked(observer, writer_pid, holder_pid)
                clock_value = NOW + timedelta(
                    seconds=1, microseconds=1 if boundary == "later" else 0
                )
                await holder.commit()
                if boundary == "later":
                    with pytest.raises(
                        service.PrincipalAuthorityRefusal, match=f"^identity_facts_{expiry}_stale$"
                    ):
                        await task
                else:
                    result = await task
                    assert result.authorization_version == 4
                    assert result.lifecycle_generation == 2
                # A caught refusal followed by commit still cannot persist effects.
                await writer.commit()
            finally:
                if not task.done():
                    task.cancel()
                    await asyncio.gather(task, return_exceptions=True)
        assert calls and calls[-1] == clock_value
        async with f() as s:
            row = await principal(s, tenant)
            assert (row.status, row.authorization_version) == ("active", 4)
            assert (await s.get(TenantIdentityProvider, tenant)).lifecycle_generation == (
                1 if boundary == "later" else 2
            )

    run(body)


@pytest.mark.parametrize("first", ["mint", "inactive"])
def test_mint_and_inactive_commit_serialize_and_old_cookie_never_returns(
    clean_db: None, first: str
) -> None:
    """@spec HTTP-PRINCIPAL-ADOPTION-V1-8 HTTP-PRINCIPAL-ADOPTION-V1-11."""
    service = api()

    async def body(f: async_sessionmaker[AsyncSession]) -> None:
        """@spec HTTP-PRINCIPAL-ADOPTION-V1-8 HTTP-PRINCIPAL-ADOPTION-V1-11."""
        tenant, person_id = await seed(f, status="active")
        await adopt(f, tenant)
        cap = await capture(f, tenant)
        async with f() as mint, f() as inactive, f() as observer:
            mint_pid, inactive_pid = await pid(mint), await pid(inactive)
            stale = await mint.get(Principal, person_id)
            if first == "mint":
                await mint.execute(select(Tenant.id).where(Tenant.id == tenant).with_for_update())
                task = asyncio.create_task(apply(inactive, cap, status="disabled"))
                try:
                    await blocked(observer, inactive_pid, mint_pid)
                    token, minted = await console.create_principal_console_session(mint, stale)
                    await task
                    await inactive.commit()
                finally:
                    if not task.done():
                        task.cancel()
                        await asyncio.gather(task, return_exceptions=True)
                async with f() as s:
                    assert (await s.get(ConsoleSession, minted.id)).revoked_at is not None
                    assert await console.live_principal_session(s, token) is None
            else:
                await apply(inactive, cap, status="disabled")
                task = asyncio.create_task(console.create_principal_console_session(mint, stale))
                try:
                    await blocked(observer, mint_pid, inactive_pid)
                    await inactive.commit()
                    with pytest.raises(
                        service.PrincipalAuthorityRefusal, match="^principal_inactive$"
                    ):
                        await task
                    await mint.rollback()
                finally:
                    if not task.done():
                        task.cancel()
                        await asyncio.gather(task, return_exceptions=True)
                async with f() as s:
                    assert (
                        await s.scalar(
                            select(func.count())
                            .select_from(ConsoleSession)
                            .where(ConsoleSession.principal_id == person_id)
                        )
                        == 0
                    )

    run(body)


def test_two_same_generation_results_have_exactly_one_winner(clean_db: None) -> None:
    """@spec HTTP-PRINCIPAL-ADOPTION-V1-6 HTTP-PRINCIPAL-ADOPTION-V1-7."""
    service = api()

    async def body(f: async_sessionmaker[AsyncSession]) -> None:
        """@spec HTTP-PRINCIPAL-ADOPTION-V1-6 HTTP-PRINCIPAL-ADOPTION-V1-7."""
        tenant, _ = await seed(f, status="active")
        await adopt(f, tenant)
        first_cap, second_cap = await capture(f, tenant), await capture(f, tenant)
        async with f() as first, f() as second, f() as observer:
            first_pid, second_pid = await pid(first), await pid(second)
            result = await apply(first, first_cap)
            task = asyncio.create_task(apply(second, second_cap))
            try:
                await blocked(observer, second_pid, first_pid)
                await first.commit()
                with pytest.raises(
                    service.PrincipalAuthorityRefusal, match="^stale_lifecycle_generation$"
                ):
                    await task
                await second.commit()
            finally:
                if not task.done():
                    task.cancel()
                    await asyncio.gather(task, return_exceptions=True)
            assert result.authorization_version == 4
        assert (await capture(f, tenant)).lifecycle_generation == 2

    run(body)


@pytest.mark.parametrize(
    "consumer", ["token", "confirm", "proof", "email", "exchange", "locked_team"]
)
def test_discovery_never_locks_token_namespace_or_session_before_scope(
    clean_db: None, consumer: str
) -> None:
    """
    @spec HTTP-PRINCIPAL-ADOPTION-V1-4 HTTP-PRINCIPAL-ADOPTION-V1-10
    HTTP-PRINCIPAL-ADOPTION-V1-11.
    """
    api()

    async def body(f: async_sessionmaker[AsyncSession]) -> None:
        """@spec HTTP-PRINCIPAL-ADOPTION-V1-4 HTTP-PRINCIPAL-ADOPTION-V1-10"""
        tenant, person_id = await seed(f, local=True, status="active")
        await adopt(f, tenant, local=True)
        async with f() as s, s.begin():
            group = await team(s, tenant)
            token, namespace = await connection_fixture(s, tenant)
            code, session_row = await cookie(s, person_id, consumed=False)
        async with f() as scope, f() as consumer_session, f() as observer, f() as probe:
            scope_pid, consumer_pid = await pid(scope), await pid(consumer_session)
            await scope.execute(select(Tenant.id).where(Tenant.id == tenant).with_for_update())
            person_row = await consumer_session.get(Principal, person_id)

            async def invoke() -> None:
                """@spec HTTP-PRINCIPAL-ADOPTION-V1-4."""
                if consumer == "locked_team":
                    scope_value = directory_sync.DirectoryScope(
                        tenant, ISSUER, "synthetic-directory", frozenset({group.id})
                    )
                    await directory_sync.locked_team(consumer_session, scope_value, group.id, 1)
                elif consumer == "token":
                    await connect.live_token(consumer_session, token, lock=True)
                elif consumer == "confirm":
                    await connect.confirm(consumer_session, token=token, principal=person_row)
                elif consumer == "proof":
                    await connect.preserve_verified_claims(
                        consumer_session, token=token, principal=person_row, claims=claims()
                    )
                elif consumer == "email":
                    await connect.link_verified_email(
                        consumer_session,
                        tenant_id=tenant,
                        principal=person_row,
                        email="new@example.test",
                        issuer=ISSUER,
                    )
                else:
                    await console.exchange_console_login_code(consumer_session, code)

            task = asyncio.create_task(invoke())
            try:
                await blocked(observer, consumer_pid, scope_pid)
                # These NOWAIT probes prove no later-order row was locked while waiting.
                await probe.execute(
                    select(Team).where(Team.id == group.id).with_for_update(nowait=True)
                )
                await probe.execute(
                    select(IdentityConnectToken)
                    .where(IdentityConnectToken.token_hash == connect.hash_token(token))
                    .with_for_update(nowait=True)
                )
                await probe.execute(
                    select(IdentityNamespace)
                    .where(IdentityNamespace.id == namespace.id)
                    .with_for_update(nowait=True)
                )
                await probe.execute(
                    select(Principal).where(Principal.id == person_id).with_for_update(nowait=True)
                )
                await probe.execute(
                    select(ConsoleSession)
                    .where(ConsoleSession.id == session_row.id)
                    .with_for_update(nowait=True)
                )
                await probe.rollback()
                await scope.commit()
                if consumer == "locked_team":
                    # Scope-first locking does not grant a displaced directory writer authority.
                    with pytest.raises(
                        directory_sync.DirectoryConflict, match="^authority_owned_by_http$"
                    ):
                        await task
                else:
                    await task
                await consumer_session.commit()
            finally:
                if not task.done():
                    task.cancel()
                    await asyncio.gather(task, return_exceptions=True)

    run(body)


def test_stale_connection_proof_waits_for_inactive_commit_and_cannot_link(clean_db: None) -> None:
    """@spec HTTP-PRINCIPAL-ADOPTION-V1-4 HTTP-PRINCIPAL-ADOPTION-V1-10."""
    api()

    async def body(f: async_sessionmaker[AsyncSession]) -> None:
        """@spec HTTP-PRINCIPAL-ADOPTION-V1-4 HTTP-PRINCIPAL-ADOPTION-V1-10."""
        tenant, person_id = await seed(f, local=True, status="active")
        await adopt(f, tenant, local=True)
        async with f() as s, s.begin():
            await team(s, tenant)
            token, _ = await connection_fixture(s, tenant)
        cap = await capture(f, tenant)
        async with f() as inactive, f() as confirmer, f() as observer:
            inactive_pid, confirmer_pid = await pid(inactive), await pid(confirmer)
            stale = await confirmer.get(Principal, person_id)
            await apply(inactive, cap, status="disabled")
            task = asyncio.create_task(connect.confirm(confirmer, token=token, principal=stale))
            try:
                await blocked(observer, confirmer_pid, inactive_pid)
                await inactive.commit()
                with pytest.raises((connect.ConnectRefused, api().PrincipalAuthorityRefusal)):
                    await task
                await confirmer.commit()
            finally:
                if not task.done():
                    task.cancel()
                    await asyncio.gather(task, return_exceptions=True)
        async with f() as s:
            assert (
                await s.scalar(
                    select(func.count())
                    .select_from(IdentityLink)
                    .where(IdentityLink.tenant_id == tenant)
                )
                == 0
            )
            assert (
                await s.get(IdentityConnectToken, connect.hash_token(token))
            ).consumed_at is None

    run(body)
