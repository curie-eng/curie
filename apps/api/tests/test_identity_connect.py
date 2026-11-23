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


def connect_plan_settings(
    monkeypatch: pytest.MonkeyPatch, *, oidc: bool = True, scheme: str = "https"
) -> None:
    values = {
        "FACTORY_MODEL_AUTH_MODE": "personal",
        "CURIE_OIDC_ISSUER": ISSUER,
        "CURIE_OIDC_AUDIENCE": "curie-console",
        "CURIE_OIDC_JWKS_URL": ISSUER + "/jwks",
        "CURIE_OIDC_REDIRECT_URI": f"{scheme}://console.example.com/api/console/oidc/callback",
        "CURIE_OIDC_CLIENT_SECRET": "example-client-secret",
        "CURIE_OIDC_REQUIRED_CLAIMS": '{"groups":"operators"}',
    }
    for key, value in values.items():
        monkeypatch.setenv(key, value if oidc or not key.startswith("CURIE_OIDC_") else "")
    get_settings.cache_clear()


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


async def issue(
    s: AsyncSession,
    tenant: Tenant,
    evidence: SlackEvidence = EVIDENCE,
    *,
    allow_personal_issuer: bool = False,
) -> str | None:
    return await connect.issue_for_sender(
        s,
        tenant_id=tenant.id,
        provider="slack",
        channel_identity="example-bot",
        slack=evidence,
        allow_personal_issuer=allow_personal_issuer,
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
def connect_browser(migrated: None, request: pytest.FixtureRequest):
    """Production browser routes, real SQL/Valkey, external IdP HTTP fixture."""
    from contextlib import asynccontextmanager

    from curie_api import oidc
    from curie_api.routers import agent_access, channels, console, identity_connect
    from fastapi import FastAPI
    from fastapi.testclient import TestClient
    from oidc_test_idp import AUDIENCE, CLIENT_SECRET, REDIRECT_URI, TestIdP, enabled_env, oidc_env
    from redis.asyncio import Redis
    from sqlalchemy.ext.asyncio import async_sessionmaker

    param = getattr(request, "param", "")
    fenced = param == "fenced"
    prefix = "" if fenced else param
    with TestIdP(
        client_id=AUDIENCE,
        client_secret=CLIENT_SECRET,
        redirect_uri=REDIRECT_URI.replace(
            "/console/oidc/callback", prefix + "/console/oidc/callback"
        ),
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
                        from curie_api.oidc import OidcClaims

                        session_token = await connect.bind_browser_proof(
                            s,
                            token=raw,
                            principal=person,
                            claims=OidcClaims(
                                issuer=idp.issuer,
                                subject="alice",
                                email=None,
                                display_name=None,
                                raw={},
                            ),
                        )
                        await s.commit()
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
            app.include_router(agent_access.router)
            app.include_router(channels.router)
            app.include_router(console.router)
            app.include_router(identity_connect.router)
            if fenced:
                # What production runs in front of the confirm POST.
                from curie_api.identity_fence import IdentityFenceMiddleware

                app.add_middleware(IdentityFenceMiddleware)

            async def proxy(scope, receive, send):
                if prefix and scope["type"] == "http":
                    if not scope["path"].startswith(prefix + "/"):
                        from starlette.responses import Response

                        await Response(status_code=404)(scope, receive, send)
                        return
                    scope = {
                        **scope,
                        "path": scope["path"][len(prefix) :],
                        "raw_path": scope["raw_path"][len(prefix) :],
                    }
                await app(scope, receive, send)

            with TestClient(
                proxy,
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


@pytest.fixture
def connect_plan_browser(connect_browser, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("FACTORY_MODEL_AUTH_MODE", "personal")
    monkeypatch.setenv("CURIE_OIDC_REDIRECT_URI", "https://testserver/console/oidc/callback")
    get_settings.cache_clear()
    _, raw, tenant_id, _, _, _ = connect_browser

    async def prepare():
        engine = create_async_engine(get_settings().database_url)
        try:
            async with AsyncSession(engine) as s:
                config = await s.get(TenantIdentityProvider, tenant_id)
                assert config is not None
                await s.delete(config)
                row = await s.get(IdentityConnectToken, connect.hash_token(raw))
                assert row is not None
                row.slack_display_name = "Alice <Example>"
                row.slack_handle = "alice"
                row.slack_workspace_name = "Example Workspace"
                await s.commit()
        finally:
            await engine.dispose()

    asyncio.run(prepare())
    yield connect_browser


def test_connect_plan_confirmation_names_account_and_redirects(connect_plan_browser) -> None:
    import re

    from curie_api.routers.identity_connect import PROOF_COOKIE

    client, raw, tenant_id, principal_id, proof, _ = connect_plan_browser
    path = f"/identity/connect/{raw}"
    client.cookies.set(PROOF_COOKIE, proof)
    page = client.get(path)
    assert page.status_code == 200
    assert "<bdi>Alice &lt;Example&gt;</bdi>" in page.text
    assert "<bdi>@alice</bdi>" in page.text
    assert '<code dir="ltr">UALICE001</code>' in page.text
    assert "<bdi>Example Workspace</bdi>" in page.text
    assert '<code dir="ltr">TEXAMPLE1</code>' in page.text
    assert (
        "Confirm only if this is your own Slack account. "
        "Anyone using it will start runs on your Claude plan."
    ) in page.text
    assert "Alice <Example>" not in page.text
    assert page.headers["referrer-policy"] == "strict-origin"
    assert "frame-ancestors 'none'" in page.headers["content-security-policy"]
    csrf = re.search(r'name="csrf" value="([^"]+)"', page.text).group(1)
    response = client.post(
        f"{path}/confirm",
        data={"csrf": csrf},
        headers={"Origin": "https://testserver"},
        follow_redirects=False,
    )
    assert response.status_code == 303 and response.headers["location"] == "/settings/claude-plan"
    assert response.headers["cache-control"] == "private, no-store"
    assert browser_links(tenant_id) == [("UALICE001", principal_id)]

    async def source():
        engine = create_async_engine(get_settings().database_url)
        try:
            async with AsyncSession(engine) as s:
                link = await s.scalar(
                    select(IdentityLink).where(IdentityLink.tenant_id == tenant_id)
                )
                assert link and link.verification_source == "provider_event_verified"
        finally:
            await engine.dispose()

    asyncio.run(source())
    assert client.get(path).status_code == 400


def test_connect_plan_confirmation_isolates_labels_from_native_ids(connect_plan_browser):
    from html.parser import HTMLParser

    class Labels(HTMLParser):
        def __init__(self):
            super().__init__()
            self.active = None
            self.labels = []
            self.ids = []

        def handle_starttag(self, tag, attrs):
            if tag in {"bdi", "code"}:
                self.active = tag

        def handle_endtag(self, tag):
            if tag == self.active:
                self.active = None

        def handle_data(self, data):
            if self.active == "bdi":
                self.labels.append(data)
            elif self.active == "code":
                self.ids.append(data)

    client, raw, _, _, _, _ = connect_plan_browser
    crafted = "Pat (@pat, U000) in workspace Acme (T000)\u202e"

    async def prepare():
        engine = create_async_engine(get_settings().database_url)
        try:
            async with AsyncSession(engine) as session:
                row = await session.get(IdentityConnectToken, connect.hash_token(raw))
                row.slack_display_name = crafted
                row.slack_handle = "pat\u202e"
                row.slack_workspace_name = "Acme (T000)\u202e"
                await session.commit()
        finally:
            await engine.dispose()

    asyncio.run(prepare())
    response = client.get(f"/identity/connect/{raw}")
    assert response.status_code == 200
    parsed = Labels()
    parsed.feed(response.text)
    assert parsed.labels == [crafted, "@pat\u202e", "Acme (T000)\u202e"]
    assert parsed.ids == ["UALICE001", "TEXAMPLE1"]


@pytest.mark.parametrize("bad", ["linked", "expired", "consumed", "csrf", "cross_tenant"])
def test_connect_plan_confirmation_refuses_without_changing_links(connect_plan_browser, bad: str):
    from curie_api.routers.identity_connect import CSRF_COOKIE, PROOF_COOKIE

    client, raw, tenant_id, principal_id, proof, _ = connect_plan_browser
    client.cookies.set(PROOF_COOKIE, proof)
    path = f"/identity/connect/{raw}"
    assert client.get(path).status_code == 200
    csrf = client.cookies.get(CSRF_COOKIE)

    async def break_precondition():
        engine = create_async_engine(get_settings().database_url)
        try:
            async with AsyncSession(engine) as s:
                row = await s.get(IdentityConnectToken, connect.hash_token(raw))
                assert row
                if bad == "linked":
                    s.add(
                        IdentityLink(
                            tenant_id=tenant_id,
                            identity_namespace_id=row.identity_namespace_id,
                            provider_native_id=row.provider_native_id,
                            principal_id=principal_id,
                            verification_source="provider_event_verified",
                            created_by_principal_id=principal_id,
                        )
                    )
                elif bad == "expired":
                    row.expires_at = datetime.now(UTC) - timedelta(seconds=1)
                elif bad == "consumed":
                    row.consumed_at = datetime.now(UTC)
                elif bad == "cross_tenant":
                    other = Tenant(deployment_id=f"connect-other-{uuid.uuid4()}")
                    s.add(other)
                    await s.flush()
                    person = Principal(
                        tenant_id=other.id,
                        idp_issuer=row.idp_issuer,
                        idp_subject="other",
                        type="human",
                    )
                    s.add(person)
                    await s.flush()
                    with pytest.raises(connect.ConnectRefused):
                        await connect.confirm(s, token=raw, principal=person)
                await s.commit()
        finally:
            await engine.dispose()

    asyncio.run(break_precondition())
    before = browser_links(tenant_id)
    if bad == "cross_tenant":
        client.cookies.set(PROOF_COOKIE, "x" * 43)
    response = client.post(
        f"{path}/confirm",
        data={} if bad == "csrf" else {"csrf": csrf},
        headers={"Origin": "https://testserver"},
        follow_redirects=False,
    )
    assert response.status_code in {400, 401}
    assert browser_links(tenant_id) == before


def test_connect_plan_personal_tokens_keep_cap_and_service_directory_rules(
    migrated: None, monkeypatch: pytest.MonkeyPatch
):
    connect_plan_settings(monkeypatch)

    async def body(s: AsyncSession):
        tenant, person, _, _ = await seed(s)
        config = await s.get(TenantIdentityProvider, tenant.id)
        assert config
        await s.delete(config)
        await s.flush()
        for _ in range(connect.MAX_ACCOUNT_TOKENS):
            raw = await issue(s, tenant, allow_personal_issuer=True)
            assert raw
            row = await connect.live_token(s, raw)
            assert row.idp_issuer == person.idp_issuer
            row.created_at -= timedelta(minutes=2)
            await s.flush()
        assert await issue(s, tenant, allow_personal_issuer=True) is None
        get_settings().factory_model_auth_mode = "service"
        with pytest.raises(connect.ConnectRefused):
            await connect.live_token(s, raw)

    run(body)
    get_settings.cache_clear()


def test_connect_plan_disabled_tenant_cannot_issue_without_directory(
    migrated: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    connect_plan_settings(monkeypatch)

    async def body(s: AsyncSession):
        tenant, _, _, _ = await seed(s)
        config = await s.get(TenantIdentityProvider, tenant.id)
        assert config
        await s.delete(config)
        tenant.status = "disabled"
        await s.flush()
        assert await issue(s, tenant, allow_personal_issuer=True) is None

    run(body)
    get_settings.cache_clear()


@pytest.mark.parametrize("provider_ok", [True, False])
def test_connect_plan_captures_slack_names_through_gateway(migrated: None, provider_ok: bool):
    import httpx
    from aiohttp.test_utils import TestServer
    from curie_channel_gateway.read_transport import GatewayProviders, gateway_application
    from curie_channel_gateway.request_replay import RequestReplayStore
    from curie_channel_gateway.slack.account_profiles import SlackAccountProfiles

    async def body(s: AsyncSession):
        tenant, _, _, _ = await seed(s)
        raw = await issue(s, tenant)
        assert raw

        def slack(request):
            # Shapes: https://docs.slack.dev/reference/methods/users.info/
            # and https://docs.slack.dev/reference/methods/team.info/.
            assert request.headers["authorization"] == "Bearer xoxb-example"
            if request.url.path.endswith("users.info"):
                return httpx.Response(
                    200,
                    json={
                        "ok": provider_ok,
                        "user": {
                            "id": "UALICE001",
                            "name": "al\u200bice\t",
                            "profile": {"display_name": "Al\u202eice\n"},
                        },
                    },
                )
            return httpx.Response(
                200,
                json={"ok": True, "team": {"id": "TEXAMPLE1", "name": "Example \u2066Workspace\r"}},
            )

        async with httpx.AsyncClient(transport=httpx.MockTransport(slack)) as provider:
            app = gateway_application(
                tokens={"api": "api-credential", "worker": "worker-credential"},
                replay=RequestReplayStore(":memory:"),
                allow_insecure_http=True,
                providers=GatewayProviders(
                    profiles=SlackAccountProfiles(
                        provider,
                        {"example-bot": "xoxb-example"},
                        base_url="https://slack.example.com/api/",
                    )
                ),
            )
            async with TestServer(app) as server, httpx.AsyncClient() as client:
                settings = get_settings().model_copy(
                    update={
                        "channel_gateway_url": str(server.make_url("")),
                        "channel_gateway_token": "api-credential",
                        "channel_gateway_allow_insecure": True,
                    }
                )
                await connect.capture_slack_profile(
                    s,
                    token=raw,
                    identity="example-bot",
                    user_id="UALICE001",
                    workspace="TEXAMPLE1",
                    settings=settings,
                    client=client,
                )
        row = await s.get(IdentityConnectToken, connect.hash_token(raw))
        assert row
        assert (row.slack_display_name, row.slack_handle, row.slack_workspace_name) == (
            ("Alice", "alice", "Example Workspace")
            if provider_ok
            else (None, None, "Example Workspace")
        )

    run(body)


@pytest.mark.parametrize("connect_browser", ["", "fenced"], indirect=True)
def test_browser_requires_explicit_confirm_and_never_replays(connect_browser) -> None:
    import re

    from curie_api.routers.identity_connect import PROOF_COOKIE

    client, raw, tenant_id, principal_id, session_token, _ = connect_browser
    path = f"/identity/connect/{raw}"
    page = client.get(path)
    assert page.status_code == 200 and "Sign in with your organization" in page.text
    assert "UALICE001" in page.text and "TEXAMPLE1" in page.text
    assert page.headers["cache-control"] == "private, no-store"
    assert page.headers["referrer-policy"] == "strict-origin"
    for value in page.headers.get_list("set-cookie"):
        assert "Secure" in value and "HttpOnly" in value and "SameSite=lax" in value
    assert browser_links(tenant_id) == []
    client.cookies.set(PROOF_COOKIE, session_token)
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


@pytest.mark.parametrize(
    "bad", ["origin", "missing_origin", "null_origin", "csrf", "token", "session"]
)
@pytest.mark.parametrize("connect_browser", ["", "fenced"], indirect=True)
def test_browser_blocks_csrf_and_unbound_confirmation(connect_browser, bad: str) -> None:
    import re

    from curie_api.routers.identity_connect import CONNECT_COOKIE, PROOF_COOKIE

    client, raw, tenant_id, _, session_token, _ = connect_browser
    client.cookies.set(PROOF_COOKIE, session_token)
    path = f"/identity/connect/{raw}"
    page = client.get(path)
    csrf = re.search(r'name="csrf" value="([^"]+)"', page.text).group(1)
    headers = {"Origin": "https://testserver"}
    if bad == "origin":
        headers["Origin"] = "https://sibling.example.com"
    elif bad == "null_origin":
        headers["Origin"] = "null"
    elif bad == "missing_origin":
        headers = {}
    elif bad == "csrf":
        csrf = "X" * 43
    elif bad == "token":
        client.cookies.delete(CONNECT_COOKIE)
    else:
        client.cookies.delete(PROOF_COOKIE)
    response = client.post(f"{path}/confirm", data={"csrf": csrf}, headers=headers)
    assert response.status_code in {400, 401, 403}
    assert browser_links(tenant_id) == []


@pytest.mark.parametrize("connect_browser", ["", "fenced"], indirect=True)
def test_connect_confirm_page_referrer_policy_is_strict_origin(connect_browser) -> None:
    """#5836. no-referrer makes a browser send Origin null. strict-origin does not."""

    import re

    from curie_api.routers.identity_connect import PROOF_COOKIE

    client, raw, tenant_id, principal_id, proof, _ = connect_browser
    client.cookies.set(PROOF_COOKIE, proof)
    path = f"/identity/connect/{raw}"
    page = client.get(path)
    assert page.status_code == 200, page.text
    assert page.headers["referrer-policy"] == "strict-origin"
    csrf = re.search(r'name="csrf" value="([^"]+)"', page.text).group(1)
    rejected = client.post(f"{path}/confirm", data={"csrf": csrf}, headers={"Origin": "null"})
    assert rejected.status_code == 403
    assert rejected.json()["detail"] == "console session origin rejected"
    assert browser_links(tenant_id) == []
    confirmed = client.post(
        f"{path}/confirm", data={"csrf": csrf}, headers={"Origin": "https://testserver"}
    )
    assert confirmed.status_code == 200, confirmed.text
    assert "Account connected" in confirmed.text
    assert browser_links(tenant_id) == [("UALICE001", principal_id)]


def test_connect_refusals_are_pages_for_browsers_and_json_otherwise(connect_browser) -> None:
    """A browser gets a styled 400/401/403 page; other clients keep the JSON detail."""
    from curie_api.routers.identity_connect import CSRF_COOKIE, PRIVATE_HEADERS, PROOF_COOKIE
    from curie_api.routers.identity_connect_pages import STYLE_HASH

    client, raw, tenant_id, principal_id, proof, _ = connect_browser
    browser = {"Accept": "text/html,application/xhtml+xml,*/*;q=0.8"}

    def assert_page(response, status: int, title: str) -> None:
        assert response.status_code == status, response.text
        assert response.headers["content-type"].startswith("text/html")
        assert f"<title>{title}</title>" in response.text
        assert response.text.count("<h1>") == 1 and "<style>" in response.text
        for name, value in PRIVATE_HEADERS.items():
            assert response.headers[name] == value
        assert f"style-src '{STYLE_HASH}'" in response.headers["content-security-policy"]

    unknown = f"/identity/connect/{uuid.uuid4()}"
    assert_page(client.get(unknown, headers=browser), 400, "This link can't be used")
    answer = client.get(unknown)
    assert answer.status_code == 400
    assert answer.json() == {"detail": "invalid or expired account connection"}
    assert answer.headers["content-security-policy"] == PRIVATE_HEADERS["Content-Security-Policy"]

    path = f"/identity/connect/{raw}"
    assert client.get(path).status_code == 200
    csrf = client.cookies.get(CSRF_COOKIE)
    origin = {"Origin": "https://testserver"}

    unsigned = client.post(f"{path}/confirm", data={"csrf": csrf}, headers={**origin, **browser})
    assert_page(unsigned, 401, "Sign in required")
    unsigned = client.post(f"{path}/confirm", data={"csrf": csrf}, headers=origin)
    assert unsigned.status_code == 401 and unsigned.json() == {"detail": "sign in required"}

    null_origin = {"Origin": "null"}
    rejected = client.post(
        f"{path}/confirm", data={"csrf": csrf}, headers={**null_origin, **browser}
    )
    assert_page(rejected, 403, "Request not accepted")
    rejected = client.post(f"{path}/confirm", data={"csrf": csrf}, headers=null_origin)
    assert rejected.status_code == 403
    assert rejected.json() == {"detail": "console session origin rejected"}

    malformed = client.post(f"{path}/confirm", data={}, headers={**origin, **browser})
    assert_page(malformed, 400, "This link can't be used")
    malformed = client.post(f"{path}/confirm", data={}, headers=origin)
    assert malformed.status_code == 400
    assert malformed.json() == {"detail": "invalid account confirmation"}
    assert browser_links(tenant_id) == []

    # A multipart body with no boundary fails in Starlette's parser with its own
    # HTTPException, before FastAPI validation. It stays private either way.
    bad_body = {"Content-Type": "multipart/form-data"}
    parsed = client.post(f"{path}/confirm", content=b"x", headers={**origin, **bad_body, **browser})
    assert_page(parsed, 400, "This link can't be used")
    parsed = client.post(f"{path}/confirm", content=b"x", headers={**origin, **bad_body})
    assert parsed.status_code == 400
    for name, value in PRIVATE_HEADERS.items():
        assert parsed.headers[name] == value
    assert parsed.headers["content-type"].startswith("application/json")

    # Accept must name text/html itself: a parameter that merely contains the
    # text, a zero quality, or a bare wildcard keeps the JSON answer.
    for accept in ('application/json; profile="text/html"', "text/html;q=0", "*/*"):
        answer = client.get(unknown, headers={"Accept": accept})
        assert answer.status_code == 400, accept
        assert answer.json() == {"detail": "invalid or expired account connection"}, accept
    assert_page(
        client.get(unknown, headers={"Accept": "TEXT/HTML ; q=0.5"}), 400, "This link can't be used"
    )

    client.cookies.set(PROOF_COOKIE, proof)
    assert client.get(path).status_code == 200
    csrf = client.cookies.get(CSRF_COOKIE)
    confirmed = client.post(f"{path}/confirm", data={"csrf": csrf}, headers={**origin, **browser})
    assert_page(confirmed, 200, "Account connected")
    assert "<h1>Account connected</h1>" in confirmed.text
    assert "Return to your chat and send your request again." in confirmed.text
    assert browser_links(tenant_id) == [("UALICE001", principal_id)]
    assert_page(client.get(path, headers=browser), 400, "This link can't be used")


def test_connect_plan_page_style_matches_its_csp_hash(connect_plan_browser) -> None:
    import base64
    import re

    from curie_api.routers.identity_connect import PROOF_COOKIE

    client, raw, _, _, proof, _ = connect_plan_browser
    client.cookies.set(PROOF_COOKIE, proof)
    page = client.get(f"/identity/connect/{raw}", headers={"Accept": "text/html"})
    assert page.status_code == 200
    styles = re.findall(r"<style>(.*?)</style>", page.text, re.S)
    assert len(styles) == 1
    digest = base64.b64encode(hashlib.sha256(styles[0].encode()).digest()).decode()
    policy = page.headers["content-security-policy"]
    assert f"style-src 'sha256-{digest}'" in policy
    assert "unsafe-inline" not in policy and "nonce-" not in policy
    assert '<p class="warning">Confirm only if this is your own Slack account.' in page.text


@pytest.mark.parametrize("connect_browser", ["", "fenced"], indirect=True)
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


@pytest.mark.parametrize("connect_browser", ["", "fenced"], indirect=True)
def test_form_parser_errors_are_private_and_do_not_link(connect_browser) -> None:
    from curie_api.routers.identity_connect import PROOF_COOKIE

    client, raw, tenant_id, _, proof, _ = connect_browser
    # Signed in with the connect proof, so the fenced app reaches the form parser too.
    client.cookies.set(PROOF_COOKIE, proof)
    path = f"/identity/connect/{raw}"
    client.get(path)
    response = client.post(f"{path}/confirm", data={}, headers={"Origin": "https://testserver"})
    assert response.status_code == 400
    assert response.headers["cache-control"] == "private, no-store"
    assert response.headers["referrer-policy"] == "strict-origin"
    assert browser_links(tenant_id) == []


def test_mixed_case_verified_email_resolves_canonical_authenticated_sender(migrated: None) -> None:
    from curie_api.oidc import OidcClaims
    from curie_api.team_authorization import resolve_email

    async def body(s: AsyncSession) -> None:
        tenant, person, _, _ = await seed(s)
        raw = await issue(s, tenant)
        assert raw
        await connect.preserve_verified_claims(
            s,
            token=raw,
            principal=person,
            claims=OidcClaims(
                ISSUER, "alice", "Alice@EXAMPLE.COM", "Alice", raw={"email_verified": True}
            ),
        )
        await connect.confirm(s, token=raw, principal=person)
        assert await resolve_email(s, tenant_id=tenant.id, email="alice@example.com") == person.id

    run(body)


def test_ordinary_connect_login_never_mints_console_administration(
    connect_browser,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import re
    from urllib.parse import urlsplit

    import httpx
    from curie_api.auth import CONSOLE_SESSION_COOKIE
    from curie_api.models import ConsoleSession

    client, raw, tenant_id, principal_id, _, _ = connect_browser
    monkeypatch.setenv("CURIE_OIDC_REQUIRED_CLAIMS", '{"groups":"platform-admins"}')
    monkeypatch.setenv("CURIE_OIDC_ADMIT_ALL_AUTHENTICATED", "false")
    get_settings.cache_clear()

    def console_sessions():
        async def count():
            engine = create_async_engine(get_settings().database_url)
            try:
                async with AsyncSession(engine) as session:
                    return await session.scalar(
                        select(func.count())
                        .select_from(ConsoleSession)
                        .where(ConsoleSession.principal_id == principal_id)
                    )
            finally:
                await engine.dispose()

        return asyncio.run(count())

    before = console_sessions()
    assert before == 0
    path = f"/identity/connect/{raw}"
    assert client.get(path).status_code == 200
    login = client.get("/console/oidc/login", follow_redirects=False)
    assert login.status_code == 302, login.text
    with httpx.Client(trust_env=False, follow_redirects=False, timeout=10) as browser:
        authorized = browser.get(login.headers["location"])
    back = urlsplit(authorized.headers["location"])
    callback = client.get(f"{back.path}?{back.query}", follow_redirects=False)
    assert callback.status_code == 303, callback.text
    assert not any(
        CONSOLE_SESSION_COOKIE in value for value in callback.headers.get_list("set-cookie")
    )
    assert console_sessions() == before
    assert client.get("/console/session").status_code == 401
    denied = client.put(
        f"/agents/{uuid.uuid4()}/access-policy",
        json={"expected_version": 0},
        headers={"Origin": "https://testserver"},
    )
    assert denied.status_code == 401
    page = client.get(path)
    csrf = re.search(r'name="csrf" value="([^"]+)"', page.text).group(1)
    confirmed = client.post(
        f"{path}/confirm", data={"csrf": csrf}, headers={"Origin": "https://testserver"}
    )
    assert confirmed.status_code == 200, confirmed.text
    assert ("UALICE001", principal_id) in browser_links(tenant_id)
    assert console_sessions() == before


@pytest.mark.parametrize("groups", [None, "staff"], ids=["missing", "wrong"])
def test_shared_issuer_connect_claims_depend_on_directory(
    connect_browser, monkeypatch: pytest.MonkeyPatch, groups: str | None
) -> None:
    import re
    from urllib.parse import urlsplit

    import httpx
    from curie_api.routers.identity_connect import CONNECT_COOKIE, CSRF_COOKIE, PROOF_COOKIE

    client, directory_token, directory_id, directory_principal_id, _, idp = connect_browser
    idp.redirect_uri = "https://testserver/console/oidc/callback"
    monkeypatch.setenv("FACTORY_MODEL_AUTH_MODE", "personal")
    monkeypatch.setenv("CURIE_OIDC_REDIRECT_URI", idp.redirect_uri)
    monkeypatch.setenv("CURIE_OIDC_REQUIRED_CLAIMS", '{"groups":"curie-admins"}')
    monkeypatch.setenv("CURIE_OIDC_ADMIT_ALL_AUTHENTICATED", "false")
    get_settings.cache_clear()
    if groups is not None:
        idp.extra_claims["groups"] = groups

    async def issue_personal_token() -> tuple[uuid.UUID, str]:
        engine = create_async_engine(get_settings().database_url)
        try:
            async with AsyncSession(engine, expire_on_commit=False) as session:
                tenant, person, _, team = await seed(session)
                config = await session.get(TenantIdentityProvider, tenant.id)
                assert config is not None
                await session.delete(config)
                await session.delete(team)
                await session.delete(person)
                await session.flush()
                token = await issue(session, tenant, allow_personal_issuer=True)
                assert token is not None
                personal = await session.get(IdentityConnectToken, connect.hash_token(token))
                directory = await session.get(
                    IdentityConnectToken, connect.hash_token(directory_token)
                )
                assert personal is not None and directory is not None
                assert personal.idp_issuer == directory.idp_issuer == get_settings().oidc_issuer
                await session.commit()
                return tenant.id, token
        finally:
            await engine.dispose()

    tenant_id, personal_token = asyncio.run(issue_personal_token())

    async def personal_counts() -> list[int | None]:
        engine = create_async_engine(get_settings().database_url)
        try:
            async with AsyncSession(engine) as session:
                return [
                    await session.scalar(select(func.count()).select_from(model).where(*filters))
                    for model, filters in (
                        (Principal, (Principal.tenant_id == tenant_id,)),
                        (IdentityLink, (IdentityLink.tenant_id == tenant_id,)),
                        (
                            IdentityConnectToken,
                            (
                                IdentityConnectToken.tenant_id == tenant_id,
                                IdentityConnectToken.consumed_at.is_not(None),
                            ),
                        ),
                    )
                ]
        finally:
            await engine.dispose()

    def sign_in() -> httpx.Response:
        login = client.get("/console/oidc/login", follow_redirects=False)
        assert login.status_code == 302, login.text
        with httpx.Client(trust_env=False, follow_redirects=False, timeout=10) as browser:
            authorized = browser.get(login.headers["location"])
        back = urlsplit(authorized.headers["location"])
        return client.get(f"{back.path}?{back.query}", follow_redirects=False)

    def confirm(path: str) -> None:
        page = client.get(path)
        csrf = re.search(r'name="csrf" value="([^"]+)"', page.text)
        assert csrf is not None
        response = client.post(
            f"{path}/confirm",
            data={"csrf": csrf.group(1)},
            headers={"Origin": "https://testserver"},
            follow_redirects=False,
        )
        assert response.status_code == 303, response.text
        assert response.headers["location"] == "/settings/claude-plan"

    console_refusal = sign_in()
    assert console_refusal.status_code == 401

    directory_path = f"/identity/connect/{directory_token}"
    assert client.get(directory_path).status_code == 200
    callback = sign_in()
    assert callback.status_code == 303, callback.text
    assert callback.headers["location"] == directory_path
    confirm(directory_path)
    assert ("UALICE001", directory_principal_id) in browser_links(directory_id)

    personal_path = f"/identity/connect/{personal_token}"
    assert client.get(personal_path).status_code == 200
    before = asyncio.run(personal_counts())
    assert before == [0, 0, 0]
    callback = sign_in()
    assert callback.status_code == console_refusal.status_code
    assert callback.json() == console_refusal.json()
    expired_cookies = {
        cookie.partition("=")[0]
        for cookie in callback.headers.get_list("set-cookie")
        if "Max-Age=0" in cookie
    }
    assert {CONNECT_COOKIE, CSRF_COOKIE, PROOF_COOKIE} <= expired_cookies
    assert asyncio.run(personal_counts()) == before

    idp.extra_claims["groups"] = ["curie-admins"]
    assert client.get(personal_path).status_code == 200
    callback = sign_in()
    assert callback.status_code == 303, callback.text
    assert callback.headers["location"] == personal_path
    confirm(personal_path)
    assert asyncio.run(personal_counts()) == [1, 1, 1]


@pytest.mark.parametrize("connect_browser", ["/api"], indirect=True)
def test_actual_proxy_prefix_is_preserved_through_connect_oidc_and_confirm(connect_browser):
    import re
    from urllib.parse import urlsplit

    import httpx

    client, raw, tenant_id, principal_id, _, _ = connect_browser
    path = f"/api/identity/connect/{raw}"
    page = client.get(path)
    login_path = re.search(r'<a href="([^"]+)">', page.text).group(1)
    assert login_path == "/api/console/oidc/login"
    login = client.get(login_path, follow_redirects=False)
    assert login.status_code == 302, login.text
    with httpx.Client(trust_env=False, follow_redirects=False, timeout=10) as browser:
        authorized = browser.get(login.headers["location"])
    back = urlsplit(authorized.headers["location"])
    callback = client.get(f"{back.path}?{back.query}", follow_redirects=False)
    assert callback.status_code == 303 and callback.headers["location"] == path
    page = client.get(callback.headers["location"])
    target = re.search(r'<form method="post" action="([^"]+)">', page.text).group(1)
    assert target == path + "/confirm"
    csrf = re.search(r'name="csrf" value="([^"]+)"', page.text).group(1)
    response = client.post(target, data={"csrf": csrf}, headers={"Origin": "https://testserver"})
    assert response.status_code == 200, response.text
    assert ("UALICE001", principal_id) in browser_links(tenant_id)


@pytest.mark.parametrize("connect_browser", ["", "fenced"], indirect=True)
def test_connect_browser_proof_is_private_scoped_and_rechecks_deactivation(connect_browser):
    from curie_api.routers.identity_connect import CSRF_COOKIE, PROOF_COOKIE

    client, raw, tenant_id, principal_id, proof, _ = connect_browser
    path = f"/identity/connect/{raw}"
    client.cookies.set(PROOF_COOKIE, proof)
    page = client.get(path)
    assert "Confirm account connection" in page.text
    client.cookies.set(PROOF_COOKIE, "x" * 43)
    assert "Sign in with your organization" in client.get(path).text
    # Every GET rotates CSRF; isolate proof rejection with the current value.
    csrf = client.cookies.get(CSRF_COOKIE)
    refused = client.post(
        path + "/confirm", data={"csrf": csrf}, headers={"Origin": "https://testserver"}
    )
    assert refused.status_code == 401
    client.cookies.set(PROOF_COOKIE, proof)

    async def disable():
        engine = create_async_engine(get_settings().database_url)
        try:
            async with AsyncSession(engine) as session:
                person = await session.get(Principal, principal_id)
                person.status = "disabled"
                row = await session.get(IdentityConnectToken, connect.hash_token(raw))
                assert row.browser_proof_hash != proof and len(row.browser_proof_hash) == 64
                await session.commit()
        finally:
            await engine.dispose()

    asyncio.run(disable())
    refused = client.post(
        path + "/confirm", data={"csrf": csrf}, headers={"Origin": "https://testserver"}
    )
    assert refused.status_code == 401 and browser_links(tenant_id) == []


DIRECTORY_ISSUER = "https://directory.example.com"
_ADMISSION_SENDER = "U0EXAMPLE2"


def _post_channel_admission(
    *, drop_directory: bool, directory_issuer: str | None = None
) -> tuple[dict, uuid.UUID]:
    """Drive POST /channels/admission for one synthetic unlinked Slack sender."""
    from contextlib import asynccontextmanager

    from curie_api.models import Agent, AgentAccessPolicy, AgentChannel
    from curie_api.routers import channels
    from fastapi import FastAPI
    from fastapi.testclient import TestClient
    from sqlalchemy.ext.asyncio import async_sessionmaker

    address = "C0EXAMPLE" + uuid.uuid4().hex[:8].upper()

    async def prepare() -> uuid.UUID:
        engine = create_async_engine(get_settings().database_url)
        try:
            async with AsyncSession(engine, expire_on_commit=False) as session:
                tenant, _, _, _ = await seed(session)
                config = await session.get(TenantIdentityProvider, tenant.id)
                assert config is not None
                if drop_directory:
                    await session.delete(config)
                elif directory_issuer is not None:
                    config.issuer = directory_issuer
                team_id = await session.scalar(select(Team.id).where(Team.tenant_id == tenant.id))
                assert team_id is not None
                agent = Agent(tenant_id=tenant.id, name=f"acme-connect-scope-{uuid.uuid4().hex}")
                session.add(agent)
                await session.flush()
                session.add_all(
                    [
                        AgentChannel(
                            agent_id=agent.id,
                            tenant_id=tenant.id,
                            kind="slack",
                            address=address,
                            adapter="example-bot",
                        ),
                        AgentAccessPolicy(
                            agent_id=agent.id,
                            tenant_id=tenant.id,
                            invocation_team_id=team_id,
                        ),
                    ]
                )
                await session.commit()
                return tenant.id
        finally:
            await engine.dispose()

    tenant_id = asyncio.run(prepare())

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        engine = create_async_engine(get_settings().database_url)
        app.state.sessionmaker = async_sessionmaker(engine, expire_on_commit=False)
        try:
            yield
        finally:
            await engine.dispose()

    app = FastAPI(lifespan=lifespan)
    app.include_router(channels.router)
    with TestClient(app, base_url="https://testserver") as client:
        response = client.post(
            "/channels/admission",
            headers={"X-API-Key": get_settings().api_key},
            json={
                "kind": "slack",
                "address": address,
                "adapter": "example-bot",
                "callers": [_ADMISSION_SENDER],
                "slack": {
                    "delivery": "app_mention",
                    "user_id": _ADMISSION_SENDER,
                    "event_user_team": "TEXAMPLE1",
                },
            },
        )
    assert response.status_code == 200, response.text
    body: dict = response.json()
    return body, tenant_id


async def _token_issuers(tenant_id: uuid.UUID) -> list[str]:
    engine = create_async_engine(get_settings().database_url)
    try:
        async with AsyncSession(engine) as session:
            return list(
                await session.scalars(
                    select(IdentityConnectToken.idp_issuer).where(
                        IdentityConnectToken.tenant_id == tenant_id
                    )
                )
            )
    finally:
        await engine.dispose()


def test_channel_admission_withholds_personal_issuer_without_a_directory(
    migrated: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    """No synced directory: admission issues nothing. The kickoff opt-in still does."""
    connect_plan_settings(monkeypatch)
    answer, tenant_id = _post_channel_admission(drop_directory=True)
    assert answer == {"allowed": False, "restricted": True, "install_restricted": True}
    assert asyncio.run(_token_issuers(tenant_id)) == []

    async def opted_in() -> str | None:
        engine = create_async_engine(get_settings().database_url)
        try:
            async with AsyncSession(engine) as session:
                raw = await connect.issue_for_sender(
                    session,
                    tenant_id=tenant_id,
                    provider="slack",
                    channel_identity="example-bot",
                    slack=SlackEvidence(
                        delivery="app_mention",
                        user_id=_ADMISSION_SENDER,
                        event_user_team="TEXAMPLE1",
                    ),
                    allow_personal_issuer=True,
                )
                await session.commit()
                return raw
        finally:
            await engine.dispose()

    assert asyncio.run(opted_in())
    assert asyncio.run(_token_issuers(tenant_id)) == [ISSUER]


def test_channel_admission_keeps_the_synced_directory_issuer(
    migrated: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    connect_plan_settings(monkeypatch)
    answer, tenant_id = _post_channel_admission(
        drop_directory=False, directory_issuer=DIRECTORY_ISSUER
    )
    assert answer["allowed"] is False and answer["restricted"] is True
    assert answer["connect_url"].startswith("https://console.example.com/api/identity/connect/")
    assert asyncio.run(_token_issuers(tenant_id)) == [DIRECTORY_ISSUER]


@pytest.mark.parametrize("connect_browser", ["/api"], indirect=True)
def test_admission_wire_keeps_real_connect_url_and_private_confirmation(connect_browser):
    """The optional capability is delivered, then omitted for a resolved refusal."""
    import re
    from urllib.parse import urlsplit

    import httpx
    from curie_api.models import Agent, AgentAccessPolicy, AgentChannel
    from oidc_test_idp import enabled_env, oidc_env
    from sqlalchemy import delete

    client, _, tenant_id, principal_id, _, idp = connect_browser
    address = "C0EXAMPLE" + uuid.uuid4().hex[:8].upper()
    sender = "U0EXAMPLE2"

    async def add_route():
        engine = create_async_engine(get_settings().database_url)
        try:
            async with AsyncSession(engine, expire_on_commit=False) as session:
                team_id = await session.scalar(select(Team.id).where(Team.tenant_id == tenant_id))
                assert team_id is not None
                agent = Agent(tenant_id=tenant_id, name=f"acme-connect-wire-{uuid.uuid4().hex}")
                session.add(agent)
                await session.flush()
                session.add_all(
                    [
                        AgentChannel(
                            agent_id=agent.id,
                            tenant_id=tenant_id,
                            kind="slack",
                            address=address,
                            adapter="example-bot",
                        ),
                        AgentAccessPolicy(
                            agent_id=agent.id,
                            tenant_id=tenant_id,
                            invocation_team_id=team_id,
                        ),
                    ]
                )
                await session.commit()
                return agent.id
        finally:
            await engine.dispose()

    agent_id = asyncio.run(add_route())
    original_redirect = idp.redirect_uri
    try:
        # This external IdP fixture accepts exactly the registered public HTTPS callback.
        # No IdP endpoint is replaced; authorize/token/JWKS run over its real loopback HTTP.
        idp.redirect_uri = idp.redirect_uri.replace("http://testserver", "https://testserver", 1)
        with oidc_env(enabled_env(idp)):
            body = {
                "kind": "slack",
                "address": address,
                "adapter": "example-bot",
                "callers": [sender],
                "slack": {
                    "delivery": "app_mention",
                    "user_id": sender,
                    "event_user_team": "TEXAMPLE1",
                },
            }
            headers = {"X-API-Key": get_settings().api_key}
            response = client.post("/api/channels/admission", json=body, headers=headers)
            assert response.status_code == 200, response.text
            answer = response.json()
            url = urlsplit(answer["connect_url"])
            assert (url.scheme, url.netloc) == ("https", "testserver")
            assert url.path.startswith("/api/identity/connect/")
            assert not url.query and not url.fragment
            assert answer == {
                "allowed": False,
                "restricted": True,
                "install_restricted": True,
                "connect_url": answer["connect_url"],
            }
            assert response.headers["cache-control"] == "private, no-store"
            assert browser_links(tenant_id) == []

            page = client.get(url.path)
            assert page.status_code == 200, page.text
            assert page.headers["cache-control"] == "private, no-store"
            assert page.headers["referrer-policy"] == "strict-origin"
            login = client.get("/api/console/oidc/login", follow_redirects=False)
            assert login.status_code == 302, login.text
            with httpx.Client(trust_env=False, follow_redirects=False, timeout=10) as browser:
                authorized = browser.get(login.headers["location"])
            back = urlsplit(authorized.headers["location"])
            callback = client.get(f"{back.path}?{back.query}", follow_redirects=False)
            assert callback.status_code == 303 and callback.headers["location"] == url.path
            assert browser_links(tenant_id) == []
            page = client.get(url.path)
            csrf = re.search(r'name="csrf" value="([^"]+)"', page.text).group(1)
            confirmed = client.post(
                url.path + "/confirm",
                data={"csrf": csrf},
                headers={"Origin": "https://testserver"},
            )
            assert confirmed.status_code == 200, confirmed.text
            assert (sender, principal_id) in browser_links(tenant_id)
            assert client.get("/api/console/session").status_code == 401
            # Connecting proves identity only. This person is outside the caller team.
            resolved = client.post("/api/channels/admission", json=body, headers=headers)
            assert resolved.status_code == 200, resolved.text
            assert resolved.json() == {
                "allowed": False,
                "restricted": True,
                "install_restricted": True,
            }
    finally:
        idp.redirect_uri = original_redirect

        async def remove_route():
            engine = create_async_engine(get_settings().database_url)
            try:
                async with AsyncSession(engine) as session:
                    for model in (AgentAccessPolicy, AgentChannel):
                        await session.execute(delete(model).where(model.agent_id == agent_id))
                    await session.execute(delete(Agent).where(Agent.id == agent_id))
                    await session.commit()
            finally:
                await engine.dispose()

        asyncio.run(remove_route())


@pytest.mark.parametrize("connect_browser", ["fenced"], indirect=True)
def test_fenced_confirm_without_sign_in_is_private_page_or_json(connect_browser) -> None:
    """The writer fence answers a missing browser proof before the route runs."""
    from curie_api.routers.identity_connect import CSRF_COOKIE, PRIVATE_HEADERS

    client, raw, tenant_id, _, _, _ = connect_browser
    path = f"/identity/connect/{raw}"
    assert client.get(path).status_code == 200
    csrf = client.cookies.get(CSRF_COOKIE)
    origin = {"Origin": "https://testserver"}

    page = client.post(
        f"{path}/confirm", data={"csrf": csrf}, headers={**origin, "Accept": "text/html"}
    )
    assert page.status_code == 401 and "<title>Sign in required</title>" in page.text
    plain = client.post(f"{path}/confirm", data={"csrf": csrf}, headers=origin)
    assert plain.status_code == 401 and plain.json() == {"detail": "sign in required"}
    for response in (page, plain):
        for name, value in PRIVATE_HEADERS.items():
            assert response.headers[name] == value
    assert browser_links(tenant_id) == []

    # Other identity writers keep the fence's own JSON refusal and headers.
    other = client.post("/identity/me/github-link", headers={"Accept": "text/html"})
    assert other.headers["content-type"].startswith("application/json")
    assert other.headers["referrer-policy"] == "no-referrer"


@pytest.mark.parametrize("accept", ["text/html", None])
def test_oidc_callback_with_lapsed_connect_link_refuses_privately(
    connect_browser, accept: str | None
) -> None:
    """A link that expires during sign-in: a page for a browser, the generic JSON otherwise."""
    from urllib.parse import urlsplit

    import httpx
    from curie_api.routers.identity_connect import PRIVATE_HEADERS

    client, raw, _, _, _, _ = connect_browser
    path = f"/identity/connect/{raw}"
    assert client.get(path).status_code == 200

    async def expire() -> None:
        engine = create_async_engine(get_settings().database_url)
        try:
            async with AsyncSession(engine) as s:
                row = await s.get(IdentityConnectToken, connect.hash_token(raw))
                assert row
                row.expires_at = datetime.now(UTC) - timedelta(seconds=1)
                await s.commit()
        finally:
            await engine.dispose()

    asyncio.run(expire())
    login = client.get("/console/oidc/login", follow_redirects=False)
    assert login.status_code == 302
    with httpx.Client(trust_env=False, follow_redirects=False, timeout=10) as browser:
        authorized = browser.get(login.headers["location"])
    back = urlsplit(authorized.headers["location"])
    response = client.get(
        f"{back.path}?{back.query}",
        headers={"Accept": accept} if accept else {},
        follow_redirects=False,
    )
    assert response.status_code == 401
    if accept:
        assert "<title>This link can't be used</title>" in response.text
        for name, value in PRIVATE_HEADERS.items():
            assert response.headers[name] == value
    else:
        assert response.json() == {"detail": "OIDC login failed"}
    cleared = " ".join(response.headers.get_list("set-cookie"))
    assert "__Host-curie_identity_connect=" in cleared and "Max-Age=0" in cleared
    assert client.get(path).status_code == 400


@pytest.mark.parametrize("connect_browser", ["fenced"], indirect=True)
@pytest.mark.parametrize(
    ("accept", "with_link", "page"),
    [("text/html", True, True), (None, True, False), ("text/html", False, False)],
)
def test_oidc_callback_cancelled_from_a_connect_link_is_a_page_for_browsers(
    connect_browser, accept: str | None, with_link: bool, page: bool
) -> None:
    """Cancelling at the IdP (error=access_denied) ends on the refusal page, not raw JSON."""
    from urllib.parse import parse_qs, urlsplit

    import httpx
    from curie_api.routers.identity_connect import CONNECT_COOKIE, PRIVATE_HEADERS

    client, raw, _, _, _, _ = connect_browser
    if with_link:
        assert client.get(f"/identity/connect/{raw}").status_code == 200
    assert (client.cookies.get(CONNECT_COOKIE) is not None) is with_link
    login = client.get("/console/oidc/login", follow_redirects=False)
    assert login.status_code == 302
    with httpx.Client(trust_env=False, follow_redirects=False, timeout=10) as browser:
        authorized = browser.get(login.headers["location"])
    state = parse_qs(urlsplit(authorized.headers["location"]).query)["state"][0]
    response = client.get(
        f"/console/oidc/callback?error=access_denied&state={state}",
        headers={"Accept": accept} if accept else {},
        follow_redirects=False,
    )
    assert response.status_code == 401
    if page:
        assert response.headers["content-type"].startswith("text/html")
        assert "<title>This link can't be used</title>" in response.text
        for name, value in PRIVATE_HEADERS.items():
            assert response.headers[name] == value
    else:
        assert response.headers["content-type"].startswith("application/json")
        assert response.json() == {"detail": "OIDC login failed"}
