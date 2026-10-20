"""IdP approver teams use current directory state across authenticated surfaces."""

import asyncio
import time
import uuid
from collections.abc import AsyncIterator, Iterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from typing import Any

import pytest
from curie_api import adapter_principal, approval_principal
from curie_api.auth import CONSOLE_SESSION_COOKIE
from curie_api.config import get_settings
from curie_api.crud import console as crud_console
from curie_api.identity.slack import SlackEvidence
from curie_api.models import (
    Agent,
    AgentAccessPolicy,
    AgentChannel,
    Approval,
    ApprovalAuditEntry,
    ChannelIdentity,
    IdentityLink,
    IdentityNamespace,
    Principal,
    PrincipalTeam,
    ProviderInstallation,
    Team,
    Tenant,
    TenantIdentityProvider,
)
from curie_api.resumequeue import ResumeQueue
from curie_api.routers import approvals
from curie_api.slack_approvers import SlackApproverSetSelector
from fastapi import FastAPI
from fastapi.testclient import TestClient
from redis.asyncio import Redis
from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

ISSUER = "https://idp.example.com"
USER = "U0EXAMPLE1"
CHANNEL = "C0EXAMPLE1"
EVIDENCE = SlackEvidence(
    delivery="block_actions", user_id=USER, interaction_user_team_id="TEXAMPLE1"
)


def db(body):
    async def run():
        engine = create_async_engine(get_settings().database_url)
        try:
            async with AsyncSession(engine, expire_on_commit=False) as s:
                result = await body(s)
                await s.commit()
                return result
        finally:
            await engine.dispose()

    return asyncio.run(run())


@pytest.fixture
def team_client(migrated: None) -> Iterator[Any]:
    async def seed(s):
        adapter_name = f"example-bot-{uuid.uuid4().hex}"
        tenant = Tenant(deployment_id=f"approval-team-{uuid.uuid4()}")
        s.add(tenant)
        await s.flush()
        person = Principal(
            tenant_id=tenant.id, idp_issuer=ISSUER, idp_subject="alice", type="human"
        )
        team = Team(
            tenant_id=tenant.id,
            source="idp_group",
            external_id="approvers",
            name="Approvers",
            managed_by="example-adapter",
            epoch=1,
            member_count=1,
            digest="a" * 64,
            last_synced_at=datetime.now(UTC),
        )
        callers = Team(
            tenant_id=tenant.id,
            source="idp_group",
            external_id="callers",
            name="Callers",
            managed_by="example-adapter",
            epoch=1,
            member_count=0,
            digest="b" * 64,
            last_synced_at=datetime.now(UTC),
        )
        agent = Agent(tenant_id=tenant.id, name=f"approval-team-{uuid.uuid4().hex}")
        config = TenantIdentityProvider(
            tenant_id=tenant.id,
            issuer=ISSUER,
            governed_domains=["example.com"],
            managed_by="example-adapter",
        )
        installation = ProviderInstallation(
            tenant_id=tenant.id, provider="slack", external_account_id="TEXAMPLE1"
        )
        ns = IdentityNamespace(
            tenant_id=tenant.id,
            provider="slack",
            kind="slack_workspace",
            authority="",
            key="TEXAMPLE1",
        )
        s.add_all([person, team, callers, agent, config, installation, ns])
        await s.flush()
        channel = AgentChannel(
            tenant_id=tenant.id,
            agent_id=agent.id,
            kind="slack",
            adapter=adapter_name,
            address=CHANNEL,
        )
        s.add_all(
            [
                ChannelIdentity(
                    tenant_id=tenant.id,
                    provider="slack",
                    name=adapter_name,
                    provider_installation_id=installation.id,
                ),
                IdentityLink(
                    tenant_id=tenant.id,
                    identity_namespace_id=ns.id,
                    provider_native_id=USER,
                    principal_id=person.id,
                    verification_source="provider_event_verified",
                    created_by_principal_id=person.id,
                ),
                PrincipalTeam(
                    tenant_id=tenant.id,
                    principal_id=person.id,
                    team_id=team.id,
                    source="idp_group",
                    version=1,
                    synced_at=datetime.now(UTC),
                ),
                AgentAccessPolicy(
                    agent_id=agent.id,
                    tenant_id=tenant.id,
                    invocation_team_id=callers.id,
                    approver_team_id=team.id,
                ),
                channel,
            ]
        )
        approval = Approval(
            agent_id=agent.id,
            conversation_id=f"thread-{uuid.uuid4()}",
            author=USER,
            summary="Confirm example action",
            reply_kind="slack",
            reply_channel=CHANNEL,
            reply_adapter=adapter_name,
            reply_identity=adapter_name,
            reply_placeholder="example-ts",
            dedupe_key=uuid.uuid4().hex,
            gate_kind="policy",
        )
        s.add(approval)
        await s.flush()
        token, _ = await crud_console.create_principal_console_session(s, person)
        return {
            "adapter": adapter_name,
            "tenant": tenant.id,
            "principal": person.id,
            "team": team.id,
            "callers": callers.id,
            "agent": agent.id,
            "binding": channel.id,
            "approval": approval.id,
            "session": token,
            "namespace": ns.id,
        }

    world = db(seed)
    stream = f"test:approval-team:{uuid.uuid4().hex}"

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        engine = create_async_engine(get_settings().database_url)
        client = Redis.from_url(get_settings().valkey_dsn())
        app.state.sessionmaker = async_sessionmaker(engine, expire_on_commit=False)
        app.state.resume_queue = ResumeQueue(client, stream=stream)
        app.state.approver_sets = SlackApproverSetSelector(None)
        app.state.bundle_store = None
        try:
            yield
        finally:
            await client.delete(stream)
            await client.aclose()
            await engine.dispose()

    app = FastAPI(lifespan=lifespan)
    app.include_router(approvals.router)
    with TestClient(app, base_url="https://testserver") as client:
        yield client, world


def headers(world, *, kind="chat", evidence=EVIDENCE):
    if kind == "console":
        return {
            "Cookie": f"{CONSOLE_SESSION_COOKIE}={world['session']}",
            "Origin": "https://testserver",
        }
    key = (
        get_settings().api_key
        if kind == "operator"
        else get_settings().approval_chat_attester_secret
    )
    token = approval_principal.mint(
        key,
        subject=USER,
        kind=kind,
        scope=approval_principal.APPROVE_SCOPE,
        exp=int(time.time()) + 60,
        actor_channel=CHANNEL if kind != "operator" else None,
        approval_id=str(world["approval"]) if kind != "operator" else None,
        slack=evidence if kind == "chat" else None,
    )
    return {"X-Curie-Approval-Principal": token}


def audits(world):
    async def read(s):
        return list(
            await s.scalars(
                select(ApprovalAuditEntry).where(
                    ApprovalAuditEntry.approval_id == world["approval"]
                )
            )
        )

    return db(read)


@pytest.mark.parametrize("kind", ["chat", "console"])
def test_current_approver_member_may_resolve_without_invocation_membership(team_client, kind):
    client, world = team_client
    response = client.post(
        f"/approvals/{world['approval']}/resolve",
        json={"decision": "approved"},
        headers=headers(world, kind=kind),
    )
    assert response.status_code == 200, response.text
    entry = audits(world)[0]
    assert entry.authorized and entry.team_id == world["team"]
    assert entry.team_epoch == 1 and entry.team_digest == "a" * 64
    assert entry.authorizer is None


@pytest.mark.parametrize(
    "bad", ["removed", "disabled", "namespace", "no_evidence", "wrong_team", "issuer"]
)
def test_pending_approval_observes_current_identity_state(team_client, bad):
    client, world = team_client

    async def mutate(s):
        if bad == "removed":
            await s.execute(delete(PrincipalTeam).where(PrincipalTeam.team_id == world["team"]))
            team = await s.get(Team, world["team"])
            team.epoch, team.member_count, team.digest = 2, 0, "c" * 64
        elif bad == "disabled":
            (await s.get(Principal, world["principal"])).status = "disabled"
        elif bad == "namespace":
            (await s.get(IdentityNamespace, world["namespace"])).status = "disabled"
        elif bad == "issuer":
            (
                await s.get(TenantIdentityProvider, world["tenant"])
            ).issuer = "https://other.example.com"

    db(mutate)
    evidence = None if bad == "no_evidence" else EVIDENCE
    if bad == "wrong_team":
        evidence = EVIDENCE.model_copy(update={"interaction_user_team_id": "TOTHER001"})
    response = client.post(
        f"/approvals/{world['approval']}/resolve",
        json={"decision": "approved"},
        headers=headers(world, evidence=evidence),
    )
    assert response.status_code == 403, response.text
    assert audits(world)[0].action == "denied"


@pytest.mark.parametrize("kind", ["operator", "test_driver"])
def test_platform_minted_subject_and_test_driver_cannot_replace_idp_person(
    team_client, kind, monkeypatch
):
    from curie_internal.driver_declaration import DeclaredDriver

    client, world = team_client
    settings = get_settings()
    monkeypatch.setattr(settings, "test_installation_enabled", True)
    monkeypatch.setattr(
        settings, "test_installation_drivers", (DeclaredDriver(CHANNEL, "B0EXAMPLE1", USER),)
    )

    async def prepare(s):
        (await s.get(Agent, world["agent"])).approval_routes = {
            "example": {
                "resolution": {"kind": "slack", "address": CHANNEL},
                "approvers": {"users": [USER]},
            }
        }
        (await s.get(Approval, world["approval"])).route = "example"

    db(prepare)
    response = client.post(
        f"/approvals/{world['approval']}/resolve",
        json={"decision": "approved"},
        headers=headers(world, kind=kind),
    )
    assert response.status_code == 403, response.text


def test_explicit_route_user_list_can_only_narrow_the_team(team_client):
    client, world = team_client

    async def mutate(s):
        agent = await s.get(Agent, world["agent"])
        agent.approval_routes = {
            "example": {
                "resolution": {"kind": "slack", "address": CHANNEL},
                "approvers": {"users": ["U0EXAMPLE2"]},
            }
        }
        (await s.get(Approval, world["approval"])).route = "example"

    db(mutate)
    response = client.post(
        f"/approvals/{world['approval']}/resolve",
        json={"decision": "approved"},
        headers=headers(world),
    )
    assert response.status_code == 403, response.text
    assert audits(world)[0].authorizer == "ExplicitUserListAuthorizer"


@pytest.mark.parametrize("listed", [None, ["alice@example.com"], ["other@example.com"]])
def test_email_approver_team_uses_exact_authenticated_mailbox(team_client, listed):
    client, world = team_client

    async def prepare(s):
        binding = await s.get(AgentChannel, world["binding"])
        binding.kind, binding.address = "email", "help@example.com"
        binding.endpoint = "https://mail.example.com/reply"
        binding.allowed_callers = ["alice@example.com"]
        approval = await s.get(Approval, world["approval"])
        approval.reply_kind, approval.reply_channel = "email", "help@example.com"
        approval.reply_endpoint = binding.endpoint
        if listed is not None:
            approval.route = "example"
            (await s.get(Agent, world["agent"])).approval_routes = {
                "example": {
                    "resolution": {"mode": "requesting_surface"},
                    "approvers": {"emails": listed},
                }
            }
        ns = IdentityNamespace(
            tenant_id=world["tenant"],
            provider="email",
            authority=ISSUER,
            kind="email_domain",
            key="example.com",
        )
        s.add(ns)
        await s.flush()
        s.add(
            IdentityLink(
                tenant_id=world["tenant"],
                identity_namespace_id=ns.id,
                provider_native_id="alice@example.com",
                principal_id=world["principal"],
                verification_source="idp_directory",
                created_by_directory="example-adapter",
            )
        )

    db(prepare)
    token = adapter_principal.mint(
        get_settings().api_key,
        subject="example-mail-adapter",
        bindings=frozenset({world["binding"]}),
        exp=int(time.time()) + 60,
    )
    response = client.post(
        f"/approvals/{world['approval']}/resolve",
        json={"decision": "approved"},
        headers={"X-Curie-Adapter-Principal": token, "X-Curie-Approval-Actor": "alice@example.com"},
    )
    expected = 403 if listed == ["other@example.com"] else 200
    assert response.status_code == expected, response.text
    assert audits(world)[0].team_id == world["team"]


@pytest.mark.parametrize("change", ["membership", "route", "policy_added"])
def test_team_and_explicit_route_are_rechecked_after_external_group_lookup(team_client, change):
    from curie_api.usergroups import UserGroupMembership

    client, world = team_client

    async def prepare(s):
        if change == "policy_added":
            (await s.get(AgentAccessPolicy, world["agent"])).approver_team_id = None
        (await s.get(Agent, world["agent"])).approval_routes = {
            "example": {
                "resolution": {"kind": "slack", "address": CHANNEL},
                "approvers": {"group": "S0EXAMPLE1"},
            }
        }
        (await s.get(Approval, world["approval"])).route = "example"

    db(prepare)

    class ChangingExternalGroup:
        async def members(self, group_id):
            engine = create_async_engine(get_settings().database_url)
            try:
                async with AsyncSession(engine) as s:
                    if change in {"membership", "policy_added"}:
                        await s.execute(
                            delete(PrincipalTeam).where(PrincipalTeam.team_id == world["team"])
                        )
                        team = await s.get(Team, world["team"])
                        team.epoch, team.member_count, team.digest = 2, 0, "c" * 64
                        if change == "policy_added":
                            (
                                await s.get(AgentAccessPolicy, world["agent"])
                            ).approver_team_id = world["team"]
                    else:
                        (await s.get(Agent, world["agent"])).approval_routes = {
                            "example": {
                                "resolution": {"kind": "slack", "address": CHANNEL},
                                "approvers": {"users": ["U0EXAMPLE2"]},
                            }
                        }
                    await s.commit()
            finally:
                await engine.dispose()
            return UserGroupMembership(group_id, frozenset({USER}), datetime.now(UTC), 0)

    client.app.state.approver_sets = SlackApproverSetSelector(
        None, identity_clients={world["adapter"]: ChangingExternalGroup()}
    )
    response = client.post(
        f"/approvals/{world['approval']}/resolve",
        json={"decision": "approved"},
        headers=headers(world),
    )
    assert response.status_code == 403, response.text
    assert audits(world)[0].action == "denied"


def test_foreign_tenant_oidc_session_cannot_approve(team_client):
    client, world = team_client

    async def prepare(s):
        tenant = Tenant(deployment_id=f"approval-other-{uuid.uuid4()}")
        s.add(tenant)
        await s.flush()
        person = Principal(
            tenant_id=tenant.id, idp_issuer=ISSUER, idp_subject="other", type="human"
        )
        s.add(person)
        await s.flush()
        token, _ = await crud_console.create_principal_console_session(s, person)
        return token

    token = db(prepare)
    response = client.post(
        f"/approvals/{world['approval']}/resolve",
        json={"decision": "approved"},
        headers={"Cookie": f"{CONSOLE_SESSION_COOKIE}={token}", "Origin": "https://testserver"},
    )
    assert response.status_code == 403, response.text
    assert audits(world)[0].action == "denied"


def test_serving_mail_adapter_cannot_answer_fixed_slack_card_with_team_membership(team_client):
    client, world = team_client

    async def prepare(s):
        agent = await s.get(Agent, world["agent"])
        agent.approval_routes = {"example": {"resolution": {"kind": "slack", "address": CHANNEL}}}
        approval = await s.get(Approval, world["approval"])
        approval.route, approval.card_channel = "example", CHANNEL
        approval.reply_kind, approval.reply_channel = "email", "help@example.com"
        approval.reply_adapter, approval.reply_endpoint = (
            "example-mail",
            "https://mail.example.com/reply",
        )
        ns = IdentityNamespace(
            tenant_id=world["tenant"],
            provider="email",
            authority=ISSUER,
            kind="email_domain",
            key="example.com",
        )
        s.add(ns)
        await s.flush()
        s.add(
            IdentityLink(
                tenant_id=world["tenant"],
                identity_namespace_id=ns.id,
                provider_native_id="alice@example.com",
                principal_id=world["principal"],
                verification_source="idp_directory",
                created_by_directory="example-adapter",
            )
        )

    db(prepare)
    token = adapter_principal.mint(
        get_settings().api_key,
        subject="example-mail-adapter",
        bindings=frozenset({world["binding"]}),
        exp=int(time.time()) + 60,
    )
    response = client.post(
        f"/approvals/{world['approval']}/resolve",
        json={"decision": "approved"},
        headers={"X-Curie-Adapter-Principal": token, "X-Curie-Approval-Actor": "alice@example.com"},
    )
    assert response.status_code == 403, response.text
