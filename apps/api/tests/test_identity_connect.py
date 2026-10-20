"""One-time connection uses trusted provider evidence and explicit confirmation."""

import asyncio
import hashlib
import uuid
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime, timedelta

import pytest
from curie_api.config import get_settings
from curie_api.identity import connect
from curie_api.identity.slack import SlackEvidence
from curie_api.models import (
    ChannelIdentity,
    IdentityConnectToken,
    IdentityLink,
    IdentityNamespace,
    Principal,
    ProviderInstallation,
    Team,
    Tenant,
    TenantIdentityProvider,
)
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine

ISSUER = "https://idp.example.com"
EVIDENCE = SlackEvidence(
    delivery="block_actions", user_id="UALICE001", interaction_user_team_id="TEXAMPLE1"
)


async def seed(s: AsyncSession) -> tuple[Tenant, Principal, IdentityNamespace, Team]:
    tenant = Tenant(deployment_id=f"connect-test-{uuid.uuid4()}")
    s.add(tenant)
    await s.flush()
    person = Principal(tenant_id=tenant.id, idp_issuer=ISSUER, idp_subject="alice", type="human")
    namespace = IdentityNamespace(
        tenant_id=tenant.id, provider="slack", authority="", kind="slack_workspace", key="TEXAMPLE1"
    )
    installation = ProviderInstallation(
        tenant_id=tenant.id, provider="slack", external_account_id="TEXAMPLE1"
    )
    config = TenantIdentityProvider(
        tenant_id=tenant.id,
        issuer=ISSUER,
        governed_domains=["example.com"],
        managed_by="example-adapter",
    )
    team = Team(
        tenant_id=tenant.id,
        source="idp_group",
        external_id="example-group",
        name="Example",
        managed_by="example-adapter",
        epoch=1,
        member_count=0,
        digest="0" * 64,
        last_synced_at=datetime.now(UTC),
    )
    s.add_all([person, namespace, installation, config, team])
    await s.flush()
    s.add(
        ChannelIdentity(
            tenant_id=tenant.id,
            provider="slack",
            name="example-bot",
            provider_installation_id=installation.id,
        )
    )
    await s.flush()
    return tenant, person, namespace, team


def run(body: Callable[[AsyncSession], Awaitable[None]]) -> None:
    async def check() -> None:
        engine = create_async_engine(get_settings().database_url)
        try:
            async with engine.connect() as conn:
                transaction = await conn.begin()
                try:
                    async with AsyncSession(bind=conn, expire_on_commit=False) as session:
                        await body(session)
                finally:
                    await transaction.rollback()
        finally:
            await engine.dispose()

    asyncio.run(check())


async def issue(s: AsyncSession, tenant: Tenant, evidence: SlackEvidence = EVIDENCE) -> str | None:
    return await connect.issue_for_sender(
        s, tenant_id=tenant.id, provider="slack", channel_identity="example-bot", slack=evidence
    )


def test_issue_hashes_short_lived_token_and_limits_account(migrated: None) -> None:
    async def body(s: AsyncSession) -> None:
        tenant, _, ns, _ = await seed(s)
        raw = await issue(s, tenant)
        assert raw is not None and len(raw) >= 40
        row = await s.get(IdentityConnectToken, hashlib.sha256(raw.encode()).hexdigest())
        assert row is not None and row.provider_native_id == EVIDENCE.user_id
        assert row.identity_namespace_id == ns.id
        assert timedelta(minutes=9) < row.expires_at - datetime.now(UTC) <= timedelta(minutes=10)
        assert await issue(s, tenant) is None
        assert (
            await s.scalar(
                select(func.count())
                .select_from(IdentityConnectToken)
                .where(IdentityConnectToken.tenant_id == tenant.id)
            )
            == 1
        )

    run(body)


@pytest.mark.parametrize("bad", ["namespace", "evidence", "sync", "issuer", "linked"])
def test_issue_fails_closed_without_trusted_unlinked_account(migrated: None, bad: str) -> None:
    async def body(s: AsyncSession) -> None:
        tenant, person, ns, team = await seed(s)
        evidence = EVIDENCE
        if bad == "namespace":
            ns.status = "disabled"
        elif bad == "evidence":
            evidence = SlackEvidence(delivery="app_mention", user_id="UALICE001")
        elif bad == "sync":
            team.epoch, team.member_count, team.digest, team.last_synced_at = 0, 0, None, None
        elif bad == "issuer":
            config = await s.get(TenantIdentityProvider, tenant.id)
            assert config is not None
            await s.delete(config)
        else:
            s.add(
                IdentityLink(
                    tenant_id=tenant.id,
                    identity_namespace_id=ns.id,
                    provider_native_id="UALICE001",
                    principal_id=person.id,
                    verification_source="provider_event_verified",
                    created_by_principal_id=person.id,
                )
            )
        await s.flush()
        assert await issue(s, tenant, evidence) is None

    run(body)


def test_confirm_links_once_and_records_audit(migrated: None) -> None:
    async def body(s: AsyncSession) -> None:
        tenant, person, ns, _ = await seed(s)
        raw = await issue(s, tenant)
        assert raw
        await connect.confirm(s, token=raw, principal=person)
        links = list(
            await s.scalars(select(IdentityLink).where(IdentityLink.identity_namespace_id == ns.id))
        )
        assert len(links) == 1 and links[0].principal_id == person.id
        assert links[0].verification_source == "provider_event_verified"
        with pytest.raises(connect.ConnectRefused):
            await connect.confirm(s, token=raw, principal=person)
        from curie_api.models import IdentityAuditEvent

        assert (
            await s.scalar(
                select(func.count())
                .select_from(IdentityAuditEvent)
                .where(IdentityAuditEvent.tenant_id == tenant.id)
            )
            == 1
        )

    run(body)


@pytest.mark.parametrize(
    "bad", ["expired", "disabled", "issuer", "tenant", "principal", "idp_wins"]
)
def test_confirm_rejects_invalid_state_without_linking(migrated: None, bad: str) -> None:
    async def body(s: AsyncSession) -> None:
        tenant, person, ns, _ = await seed(s)
        raw = await issue(s, tenant)
        assert raw
        row = await s.get(IdentityConnectToken, connect.hash_token(raw))
        assert row
        if bad == "expired":
            row.expires_at = datetime.now(UTC) - timedelta(seconds=1)
        elif bad == "disabled":
            ns.status = "disabled"
        elif bad == "issuer":
            person.idp_issuer = "https://other.example.com"
        elif bad == "tenant":
            other = Tenant(deployment_id=f"connect-other-{uuid.uuid4()}")
            s.add(other)
            await s.flush()
            person.tenant_id = other.id
        elif bad == "principal":
            person.status = "disabled"
        else:
            s.add(
                IdentityLink(
                    tenant_id=tenant.id,
                    identity_namespace_id=ns.id,
                    provider_native_id="UALICE001",
                    principal_id=person.id,
                    verification_source="idp_directory",
                    created_by_directory="example-adapter",
                )
            )
        await s.flush()
        with pytest.raises(connect.ConnectRefused):
            await connect.confirm(s, token=raw, principal=person)
        assert row.consumed_at is None

    run(body)


@pytest.mark.parametrize("verified", [True, False, "true", None])
def test_email_link_uses_only_validated_verified_governed_claim(
    migrated: None, verified: object
) -> None:
    from curie_api.oidc import OidcClaims

    async def body(s: AsyncSession) -> None:
        tenant, person, _, _ = await seed(s)
        # A stored display attribute must never become link evidence.
        person.email = "untrusted@example.com"
        raw = await issue(s, tenant)
        assert raw
        await connect.preserve_verified_claims(
            s,
            token=raw,
            principal=person,
            claims=OidcClaims(
                ISSUER, "alice", "alice@example.com", "Alice", raw={"email_verified": verified}
            ),
        )
        assert not list(
            await s.scalars(select(IdentityLink).where(IdentityLink.tenant_id == tenant.id))
        )
        await connect.confirm(s, token=raw, principal=person)
        email_links = list(
            await s.scalars(
                select(IdentityLink)
                .where(IdentityLink.tenant_id == tenant.id)
                .join(IdentityNamespace, IdentityNamespace.id == IdentityLink.identity_namespace_id)
                .where(IdentityNamespace.provider == "email")
            )
        )
        assert [link.provider_native_id for link in email_links] == (
            ["alice@example.com"] if verified is True else []
        )

    run(body)


def test_email_outside_governed_domain_and_directory_mapping_are_preserved(migrated: None) -> None:
    from curie_api.oidc import OidcClaims

    async def body(s: AsyncSession) -> None:
        tenant, person, _, _ = await seed(s)
        raw = await issue(s, tenant)
        assert raw
        await connect.preserve_verified_claims(
            s,
            token=raw,
            principal=person,
            claims=OidcClaims(
                ISSUER, "alice", "alice@outside.example", "Alice", raw={"email_verified": True}
            ),
        )
        await connect.confirm(s, token=raw, principal=person)
        assert (
            len(
                list(
                    await s.scalars(select(IdentityLink).where(IdentityLink.tenant_id == tenant.id))
                )
            )
            == 1
        )
        ns = IdentityNamespace(
            tenant_id=tenant.id,
            provider="email",
            authority=ISSUER,
            kind="email_domain",
            key="example.com",
        )
        other = Principal(tenant_id=tenant.id, idp_issuer=ISSUER, idp_subject="other", type="human")
        s.add_all([ns, other])
        await s.flush()
        s.add(
            IdentityLink(
                tenant_id=tenant.id,
                identity_namespace_id=ns.id,
                provider_native_id="alice@example.com",
                principal_id=other.id,
                verification_source="idp_directory",
                created_by_directory="example-adapter",
            )
        )
        await s.flush()
        await connect.link_verified_email(
            s, tenant_id=tenant.id, principal=person, email="alice@example.com", issuer=ISSUER
        )
        assert (
            await s.scalar(
                select(IdentityLink.principal_id).where(IdentityLink.identity_namespace_id == ns.id)
            )
            == other.id
        )

    run(body)


def test_prompt_budget_is_bounded_and_old_rows_are_reclaimed(migrated: None) -> None:
    async def body(s: AsyncSession) -> None:
        tenant, _, _, _ = await seed(s)
        for _ in range(connect.MAX_ACCOUNT_TOKENS):
            assert await issue(s, tenant)
            rows = list(
                await s.scalars(
                    select(IdentityConnectToken).where(IdentityConnectToken.tenant_id == tenant.id)
                )
            )
            for row in rows:
                row.created_at -= timedelta(minutes=2)
            await s.flush()
        assert await issue(s, tenant) is None
        for row in rows:
            row.expires_at = datetime.now(UTC) - timedelta(seconds=1)
        await s.flush()
        assert await issue(s, tenant)
        assert (
            await s.scalar(
                select(func.count())
                .select_from(IdentityConnectToken)
                .where(IdentityConnectToken.tenant_id == tenant.id)
            )
            == 1
        )

    run(body)


@pytest.fixture
def connect_browser(migrated: None):
    """Production browser routes, real SQL/Valkey, external IdP HTTP fixture."""
    from contextlib import asynccontextmanager

    from curie_api import oidc
    from curie_api.crud import console as crud_console
    from curie_api.routers import console, identity_connect
    from fastapi import FastAPI
    from fastapi.testclient import TestClient
    from oidc_test_idp import AUDIENCE, CLIENT_SECRET, REDIRECT_URI, TestIdP, enabled_env, oidc_env
    from redis.asyncio import Redis
    from sqlalchemy.ext.asyncio import async_sessionmaker

    with TestIdP(
        client_id=AUDIENCE,
        client_secret=CLIENT_SECRET,
        redirect_uri=REDIRECT_URI,
        subject="alice",
        extra_claims={"email_verified": True},
    ) as idp:
        with oidc_env(enabled_env(idp)):
            oidc.reset_caches()

            async def prepare():
                engine = create_async_engine(get_settings().database_url)
                try:
                    async with AsyncSession(engine, expire_on_commit=False) as s:
                        tenant, person, _, _ = await seed(s)
                        person.idp_issuer = idp.issuer
                        config = await s.get(TenantIdentityProvider, tenant.id)
                        assert config
                        config.issuer = idp.issuer
                        await s.flush()
                        raw = await issue(s, tenant)
                        assert raw
                        session_token, _ = await crud_console.create_principal_console_session(
                            s, person
                        )
                        return raw, tenant.id, person.id, session_token
                finally:
                    await engine.dispose()

            raw, tenant_id, principal_id, session_token = asyncio.run(prepare())

            @asynccontextmanager
            async def lifespan(app):
                engine = create_async_engine(get_settings().database_url)
                app.state.sessionmaker = async_sessionmaker(engine, expire_on_commit=False)
                app.state.valkey = Redis.from_url(get_settings().valkey_dsn())
                try:
                    yield
                finally:
                    await app.state.valkey.aclose()
                    await engine.dispose()

            app = FastAPI(lifespan=lifespan)
            app.include_router(console.router)
            app.include_router(identity_connect.router)
            with TestClient(
                app,
                base_url="https://testserver",
                client=(f"2001:db8::{uuid.uuid4().int & 65535:x}", 5000),
            ) as client:
                yield client, raw, tenant_id, principal_id, session_token, idp
            oidc.reset_caches()


def browser_links(tenant_id: uuid.UUID) -> list[tuple[str, uuid.UUID | None]]:
    async def read():
        engine = create_async_engine(get_settings().database_url)
        try:
            async with AsyncSession(engine) as s:
                return list(
                    (
                        await s.execute(
                            select(
                                IdentityLink.provider_native_id, IdentityLink.principal_id
                            ).where(IdentityLink.tenant_id == tenant_id)
                        )
                    ).tuples()
                )
        finally:
            await engine.dispose()

    return asyncio.run(read())


def test_browser_requires_explicit_confirm_and_never_replays(connect_browser) -> None:
    import re

    from curie_api.auth import CONSOLE_SESSION_COOKIE

    client, raw, tenant_id, principal_id, session_token, _ = connect_browser
    path = f"/identity/connect/{raw}"
    page = client.get(path)
    assert page.status_code == 200 and "Sign in with your organization" in page.text
    assert "UALICE001" in page.text and "TEXAMPLE1" in page.text
    assert page.headers["cache-control"] == "private, no-store"
    assert page.headers["referrer-policy"] == "no-referrer"
    for value in page.headers.get_list("set-cookie"):
        assert "Secure" in value and "HttpOnly" in value and "SameSite=lax" in value
    assert browser_links(tenant_id) == []
    client.cookies.set(CONSOLE_SESSION_COOKIE, session_token)
    page = client.get(path)
    assert "Confirm account connection" in page.text
    csrf = re.search(r'name="csrf" value="([^"]+)"', page.text).group(1)
    assert browser_links(tenant_id) == []
    confirmed = client.post(
        f"{path}/confirm", data={"csrf": csrf}, headers={"Origin": "https://testserver"}
    )
    assert confirmed.status_code == 200 and "send your request again" in confirmed.text
    assert browser_links(tenant_id) == [("UALICE001", principal_id)]
    assert client.get(path).status_code == 400


@pytest.mark.parametrize("bad", ["origin", "missing_origin", "csrf", "token", "session"])
def test_browser_blocks_csrf_and_unbound_confirmation(connect_browser, bad: str) -> None:
    import re

    from curie_api.auth import CONSOLE_SESSION_COOKIE
    from curie_api.routers.identity_connect import CONNECT_COOKIE

    client, raw, tenant_id, _, session_token, _ = connect_browser
    client.cookies.set(CONSOLE_SESSION_COOKIE, session_token)
    path = f"/identity/connect/{raw}"
    page = client.get(path)
    csrf = re.search(r'name="csrf" value="([^"]+)"', page.text).group(1)
    headers = {"Origin": "https://testserver"}
    if bad == "origin":
        headers["Origin"] = "https://sibling.example.com"
    elif bad == "missing_origin":
        headers = {}
    elif bad == "csrf":
        csrf = "X" * 43
    elif bad == "token":
        client.cookies.delete(CONNECT_COOKIE)
    else:
        client.cookies.delete(CONSOLE_SESSION_COOKIE)
    response = client.post(f"{path}/confirm", data={"csrf": csrf}, headers=headers)
    assert response.status_code in {400, 401, 403}
    assert browser_links(tenant_id) == []


def test_oidc_callback_returns_to_confirmation_and_preserves_verified_claim(
    connect_browser,
) -> None:
    import re
    from urllib.parse import urlsplit

    import httpx

    client, raw, tenant_id, principal_id, _, idp = connect_browser
    path = f"/identity/connect/{raw}"
    page = client.get(path)
    assert page.status_code == 200
    login = client.get("/console/oidc/login", follow_redirects=False)
    assert login.status_code == 302
    with httpx.Client(trust_env=False, follow_redirects=False, timeout=10) as browser:
        authorized = browser.get(login.headers["location"])
    back = urlsplit(authorized.headers["location"])
    callback = client.get(f"{back.path}?{back.query}", follow_redirects=False)
    assert callback.status_code == 303, callback.text
    assert callback.headers["location"] == path
    assert idp.token_requests and browser_links(tenant_id) == []
    page = client.get(path)
    csrf = re.search(r'name="csrf" value="([^"]+)"', page.text).group(1)
    response = client.post(
        f"{path}/confirm", data={"csrf": csrf}, headers={"Origin": "https://testserver"}
    )
    assert response.status_code == 200, response.text
    assert set(browser_links(tenant_id)) == {
        ("UALICE001", principal_id),
        ("alice@example.com", principal_id),
    }


def test_concurrent_issuance_and_confirmation_have_one_winner(migrated: None) -> None:
    async def check() -> None:
        engine = create_async_engine(get_settings().database_url)
        try:
            async with AsyncSession(engine, expire_on_commit=False) as s:
                tenant, person, _, _ = await seed(s)
                tenant_id, person_id = tenant.id, person.id
                await s.commit()

            async def mint() -> str | None:
                async with AsyncSession(engine) as s:
                    raw = await connect.issue_for_sender(
                        s,
                        tenant_id=tenant_id,
                        provider="slack",
                        channel_identity="example-bot",
                        slack=EVIDENCE,
                    )
                    await s.commit()
                    return raw

            tokens = await asyncio.wait_for(asyncio.gather(mint(), mint()), timeout=10)
            assert sum(token is not None for token in tokens) == 1
            raw = next(token for token in tokens if token is not None)

            async def confirm_once() -> bool:
                async with AsyncSession(engine) as s:
                    principal = await s.get(Principal, person_id)
                    assert principal
                    try:
                        await connect.confirm(s, token=raw, principal=principal)
                        await s.commit()
                        return True
                    except connect.ConnectRefused:
                        await s.rollback()
                        return False

            results = await asyncio.wait_for(
                asyncio.gather(confirm_once(), confirm_once()), timeout=10
            )
            assert results.count(True) == 1
            async with AsyncSession(engine) as s:
                assert (
                    await s.scalar(
                        select(func.count())
                        .select_from(IdentityLink)
                        .where(IdentityLink.tenant_id == tenant_id)
                    )
                    == 1
                )
        finally:
            await engine.dispose()

    asyncio.run(check())


@pytest.mark.parametrize("bad", ["tenant_inactive", "provider_changed", "no_sync"])
def test_configuration_change_revokes_outstanding_connection(migrated: None, bad: str) -> None:
    async def body(s: AsyncSession) -> None:
        tenant, person, _, team = await seed(s)
        raw = await issue(s, tenant)
        assert raw
        if bad == "tenant_inactive":
            tenant.status = "disabled"
        elif bad == "provider_changed":
            config = await s.get(TenantIdentityProvider, tenant.id)
            assert config
            config.issuer = "https://other.example.com"
        else:
            team.epoch, team.digest, team.member_count, team.last_synced_at = 0, None, 0, None
        await s.flush()
        with pytest.raises(connect.ConnectRefused):
            await connect.confirm(s, token=raw, principal=person)
        if bad != "provider_changed":
            assert await issue(s, tenant) is None

    run(body)


def test_form_parser_errors_are_private_and_do_not_link(connect_browser) -> None:
    from curie_api.auth import CONSOLE_SESSION_COOKIE

    client, raw, tenant_id, _, session_token, _ = connect_browser
    client.cookies.set(CONSOLE_SESSION_COOKIE, session_token)
    path = f"/identity/connect/{raw}"
    client.get(path)
    response = client.post(f"{path}/confirm", data={}, headers={"Origin": "https://testserver"})
    assert response.status_code == 400
    assert response.headers["cache-control"] == "private, no-store"
    assert response.headers["referrer-policy"] == "no-referrer"
    assert browser_links(tenant_id) == []
