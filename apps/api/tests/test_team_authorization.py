"""Caller and approver roles stay separate and use current trusted identity."""

import asyncio
import uuid
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime

import pytest
from curie_api import team_authorization as authz
from curie_api.config import get_settings
from curie_api.identity.slack import SlackEvidence
from curie_api.models import (
    Agent,
    AgentAccessPolicy,
    AgentChannel,
    ChannelIdentity,
    IdentityAuditEvent,
    IdentityConnectToken,
    IdentityLink,
    IdentityNamespace,
    Principal,
    PrincipalTeam,
    ProviderInstallation,
    Team,
    Tenant,
    TenantIdentityProvider,
)
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine

ISSUER = "https://idp.example.com"


async def seed(s: AsyncSession):
    tenant = Tenant(id=uuid.uuid4(), deployment_id=f"team-auth-{uuid.uuid4()}", status="active")
    s.add(tenant)
    await s.flush()
    s.add(
        TenantIdentityProvider(
            tenant_id=tenant.id,
            issuer=ISSUER,
            governed_domains=["example.com"],
            managed_by="example-directory",
        )
    )
    person = Principal(tenant_id=tenant.id, idp_issuer=ISSUER, idp_subject="member", type="human")
    caller = Team(
        tenant_id=tenant.id,
        source="idp_group",
        managed_by="example-directory",
        external_id="callers",
        name="Callers",
        epoch=1,
        member_count=1,
        digest="example-digest",
        last_synced_at=datetime.now(UTC),
    )
    approver = Team(
        tenant_id=tenant.id,
        source="idp_group",
        managed_by="example-directory",
        external_id="approvers",
        name="Approvers",
        epoch=1,
        member_count=0,
        digest="empty-digest",
        last_synced_at=datetime.now(UTC),
    )
    agent = Agent(tenant_id=tenant.id, name=f"acme-{uuid.uuid4()}")
    install = ProviderInstallation(
        tenant_id=tenant.id, provider="slack", external_account_id="T0EXAMPLE1"
    )
    namespace = IdentityNamespace(
        tenant_id=tenant.id,
        provider="slack",
        authority="",
        kind="slack_workspace",
        key="T0EXAMPLE1",
    )
    s.add_all([person, caller, approver, agent, install, namespace])
    await s.flush()
    identity = ChannelIdentity(
        tenant_id=tenant.id, provider="slack", name="default", provider_installation_id=install.id
    )
    binding = AgentChannel(
        agent_id=agent.id,
        tenant_id=tenant.id,
        kind="slack",
        address="C0EXAMPLE1",
        adapter="default",
    )
    s.add_all(
        [
            identity,
            binding,
            AgentAccessPolicy(
                agent_id=agent.id,
                tenant_id=tenant.id,
                invocation_team_id=caller.id,
                approver_team_id=approver.id,
            ),
            PrincipalTeam(
                principal_id=person.id, team_id=caller.id, tenant_id=tenant.id, source="idp_group"
            ),
            IdentityLink(
                tenant_id=tenant.id,
                identity_namespace_id=namespace.id,
                provider_native_id="U0EXAMPLE1",
                principal_id=person.id,
                created_by_principal_id=person.id,
                verification_source="provider_event_verified",
            ),
        ]
    )
    await s.flush()
    evidence = SlackEvidence(
        delivery="app_mention", user_id="U0EXAMPLE1", event_user_team="T0EXAMPLE1"
    )
    return person, caller, approver, agent, binding, evidence


def run(body: Callable[[AsyncSession], Awaitable[None]]) -> None:
    async def check():
        engine = create_async_engine(get_settings().database_url)
        try:
            async with engine.connect() as conn:
                tx = await conn.begin()
                try:
                    async with AsyncSession(conn, expire_on_commit=False) as s:
                        await body(s)
                finally:
                    await tx.rollback()
        finally:
            await engine.dispose()

    asyncio.run(check())


@pytest.mark.parametrize("case", ["member", "nonmember", "inactive", "unlinked", "not_synced"])
def test_invocation_team_uses_current_identity_and_projection(migrated, case):
    async def body(s):
        person, team, _, _, binding, evidence = await seed(s)
        if case == "nonmember":
            from sqlalchemy import delete

            await s.execute(delete(PrincipalTeam).where(PrincipalTeam.team_id == team.id))
        elif case == "inactive":
            person.status = "disabled"
        elif case == "unlinked":
            evidence.user_id = "U0EXAMPLE2"
        elif case == "not_synced":
            team.epoch, team.member_count, team.digest, team.last_synced_at = 0, 0, None, None
        await s.flush()
        result = await authz.check_binding(s, binding, role="invocation", slack=evidence)
        assert result.restricted
        assert result.allowed is (case == "member")
        assert result.unlinked is (case == "unlinked")

    run(body)


def test_caller_membership_does_not_grant_approval(migrated):
    async def body(s):
        person, _, approver, _, binding, evidence = await seed(s)
        assert (await authz.check_binding(s, binding, role="invocation", slack=evidence)).allowed
        assert not (await authz.check_binding(s, binding, role="approver", slack=evidence)).allowed
        s.add(
            PrincipalTeam(
                principal_id=person.id,
                team_id=approver.id,
                tenant_id=person.tenant_id,
                source="idp_group",
            )
        )
        await s.flush()
        result = await authz.check_binding(s, binding, role="approver", slack=evidence)
        assert result.allowed and result.team_id == approver.id and result.epoch == 1

    run(body)


def test_http_admission_cannot_bypass_team_with_an_open_binding(migrated):
    from curie_api.routers import channels
    from fastapi import FastAPI
    from fastapi.testclient import TestClient
    from sqlalchemy import delete
    from sqlalchemy.ext.asyncio import async_sessionmaker
    from sqlalchemy.pool import NullPool

    engine = create_async_engine(get_settings().database_url, poolclass=NullPool)
    factory = async_sessionmaker(engine, expire_on_commit=False)

    async def setup():
        async with factory() as s:
            person, _, _, agent, binding, evidence = await seed(s)
            await s.commit()
            return person.tenant_id, binding.address, evidence

    tenant_id, address, evidence = asyncio.run(setup())
    try:
        app = FastAPI()
        app.state.sessionmaker = factory
        app.include_router(channels.router)
        with TestClient(app) as client:
            payload = {
                "kind": "slack",
                "address": address,
                "adapter": "default",
                "callers": ["U0EXAMPLE1"],
                "slack": evidence.model_dump(),
            }
            headers = {"X-API-Key": get_settings().api_key}
            member = client.post("/channels/admission", json=payload, headers=headers)
            assert member.status_code == 200, member.text
            assert member.json()["allowed"] and member.json()["restricted"]
            payload["slack"]["user_id"] = "U0EXAMPLE2"
            payload["callers"] = ["U0EXAMPLE2"]
            other = client.post("/channels/admission", json=payload, headers=headers)
            assert other.status_code == 200 and not other.json()["allowed"]
            assert other.json()["install_restricted"]
    finally:

        async def cleanup():
            async with factory() as s:
                for model in (
                    IdentityConnectToken,
                    IdentityAuditEvent,
                    IdentityLink,
                    PrincipalTeam,
                    AgentAccessPolicy,
                    AgentChannel,
                    ChannelIdentity,
                    ProviderInstallation,
                    IdentityNamespace,
                    Agent,
                    Team,
                    Principal,
                    TenantIdentityProvider,
                    Tenant,
                ):
                    key = model.id if model is Tenant else model.tenant_id
                    await s.execute(delete(model).where(key == tenant_id))
                await s.commit()
            await engine.dispose()

        asyncio.run(cleanup())
