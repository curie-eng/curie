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
from curie_api.routers import actions, approvals
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
    app.include_router(actions.router)
    app.include_router(actions.undo_router)
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


@pytest.mark.parametrize("revocation", ["membership", "status"])
def test_remediation_rechecks_authority_after_preclaim_helper_commit(team_client, revocation):
    """The real remediation judge commits; revocation must win before claim."""
    import hashlib
    from concurrent.futures import ThreadPoolExecutor

    from curie_api.identity_fence import mutation
    from curie_api.models import (
        ActionExecution,
        RemediationApprovalRequest,
        RemediationNomination,
        RemediationNominationSubmission,
        RemediationPolicy,
        RemediationPolicyGeneration,
    )
    from sqlalchemy import event
    from sqlalchemy.orm import Session

    client, world = team_client
    nomination_id = uuid.uuid4()
    digest = hashlib.sha256(b"{}").hexdigest()

    async def prepare(s):
        approval = await s.get(Approval, world["approval"])
        approval.purpose = "remediation"
        approval.granted_tool, approval.granted_arguments = "mcp__example__apply", {}
        operation = uuid.uuid4()
        s.add_all(
            [
                RemediationPolicy(
                    agent_id=world["agent"],
                    hook="example-alert",
                    generation=1,
                    operation_id=operation,
                    armed=True,
                    active=True,
                ),
                RemediationPolicyGeneration(
                    agent_id=world["agent"],
                    hook="example-alert",
                    generation=1,
                    operation_id=operation,
                    intent_sha256="0" * 64,
                    armed=True,
                    active=True,
                    bound_by="example-operator",
                    document={
                        "actions": [
                            {
                                "name": "example-action",
                                "kind": "remediate",
                                "connector": "example",
                                "tool": "apply",
                            }
                        ]
                    },
                ),
                RemediationNominationSubmission(
                    event_id=str(nomination_id),
                    agent_id=world["agent"],
                    hook="example-alert",
                    block_sha256="0" * 64,
                ),
            ]
        )
        await s.flush()
        s.add(
            RemediationNomination(
                id=nomination_id,
                agent_id=world["agent"],
                hook="example-alert",
                event_id=str(nomination_id),
                admitted_generation=1,
                current_generation=1,
                action="example-action",
                kind="remediate",
                arguments="{}",
                arguments_sha256=digest,
                target="example",
                reason="Example action",
                state="approval_requested",
                approval_id=world["approval"],
            )
        )
        await s.flush()
        s.add(
            RemediationApprovalRequest(
                approval_id=world["approval"],
                nomination_id=nomination_id,
                agent_id=world["agent"],
                hook="example-alert",
                action="example-action",
                arguments_sha256=digest,
                failed_check="precondition_not_met",
            )
        )

    db(prepare)

    async def revoke(s):
        # Exercise the actual identity writer fence, not an uncoordinated SQL
        # edit that could ignore a reader still holding its transaction lock.
        engine = create_async_engine(get_settings().database_url)
        redis = Redis.from_url(get_settings().valkey_dsn())
        try:
            async with mutation(async_sessionmaker(engine), redis):
                if revocation == "membership":
                    await s.execute(
                        delete(PrincipalTeam).where(PrincipalTeam.team_id == world["team"])
                    )
                    team = await s.get(Team, world["team"])
                    team.epoch, team.member_count, team.digest = 2, 0, "c" * 64
                else:
                    (await s.get(Principal, world["principal"])).status = "disabled"
                await s.commit()
        finally:
            await redis.aclose()
            await engine.dispose()

    engine = client.app.state.sessionmaker.kw["bind"]
    commits = 0
    revoked = False
    with ThreadPoolExecutor(max_workers=1) as writer:

        def after_commit(session):
            nonlocal commits, revoked
            if session.get_bind() is not engine.sync_engine:
                return
            commits += 1
            if commits == 2:
                # Commit one releases the initial read before group lookups;
                # commit two is resolution_refusal's actual read transaction.
                writer.submit(db, revoke).result(timeout=10)
                revoked = True

        event.listen(Session, "after_commit", after_commit)
        try:
            response = client.post(
                f"/approvals/{world['approval']}/resolve",
                json={"decision": "approved"},
                headers=headers(world),
            )
        finally:
            event.remove(Session, "after_commit", after_commit)
    assert revoked
    assert response.status_code == 403, response.text

    async def outcomes(s):
        approval = await s.get(Approval, world["approval"])
        nomination = await s.get(RemediationNomination, nomination_id)
        executions = list(
            await s.scalars(
                select(ActionExecution).where(ActionExecution.agent_id == world["agent"])
            )
        )
        assert approval.status == "pending" and nomination.state == "approval_requested"
        assert not executions

    db(outcomes)


@pytest.mark.parametrize("revocation", ["membership", "status"])
def test_gated_undo_observes_current_idp_authority_before_record_details(team_client, revocation):
    from curie_api.models import ActionAuditEntry, AgentAction

    client, world = team_client

    async def prepare(s):
        action = AgentAction(
            agent_id=world["agent"],
            conversation_id="example-thread",
            call_id=uuid.uuid4().hex,
            tool="example-change",
            status="succeeded",
            dedupe_key=uuid.uuid4().hex,
            gate_approval_id=world["approval"],
        )
        s.add(action)
        if revocation == "membership":
            await s.execute(delete(PrincipalTeam).where(PrincipalTeam.team_id == world["team"]))
        else:
            (await s.get(Principal, world["principal"])).status = "disabled"
        await s.flush()
        return action.id

    action_id = db(prepare)
    response = client.post(f"/actions/{action_id}/undo", json={}, headers=headers(world))
    assert response.status_code == 403, response.text

    async def read(s):
        entry = await s.scalar(
            select(ActionAuditEntry).where(ActionAuditEntry.action_id == action_id)
        )
        assert entry.action == "refused_unauthorized"

    db(read)


@pytest.mark.parametrize("kind", ["chat", "console"])
def test_recovery_preserves_authenticated_actor_proof(team_client, kind):
    from curie_api.remediation_undo_recovery import _approving_principal

    client, world = team_client
    response = client.post(
        f"/approvals/{world['approval']}/resolve",
        json={"decision": "approved"},
        headers=headers(world, kind=kind),
    )
    assert response.status_code == 200, response.text

    async def recover(s):
        approval = await s.get(Approval, world["approval"])
        recovered = await _approving_principal(s, approval)
        assert recovered is not None
        if kind == "chat":
            assert recovered.slack == EVIDENCE
        else:
            assert recovered.principal_id == world["principal"]

    db(recover)


@pytest.fixture
def sealed_team_action(team_client, tmp_path, monkeypatch):
    """Actual stored bundle and database records, with no substituted storage."""
    import io
    import json
    import tarfile

    from _sealed_actions import CONNECTOR, DIGEST, ENVELOPE, SEALED_CONNECTORS, TARGET
    from curie_api.models import AgentAction, AgentVersion, ConnectorCapability, Deployment
    from curie_api.storage import BundleStore

    monkeypatch.setenv("CURIE_ACTION_EXECUTOR_ENABLED", "true")
    get_settings.cache_clear()
    client, world = team_client
    root = tmp_path / "example-plugin"
    (root / ".claude-plugin").mkdir(parents=True)
    (root / ".claude-plugin" / "plugin.json").write_text(
        json.dumps({"name": "example-plugin", "version": "0.1.0", "description": "Example"})
    )
    (root / "skills" / "example").mkdir(parents=True)
    (root / "skills" / "example" / "SKILL.md").write_text(
        "---\nname: example\ndescription: Example\n---\nExample skill\n"
    )
    (root / "connectors.yaml").write_text(SEALED_CONNECTORS)
    content = io.BytesIO()
    with tarfile.open(fileobj=content, mode="w:gz") as archive:
        archive.add(root, arcname="example-plugin")
    bundle_ref = f"test/undo-team/{uuid.uuid4()}.tar.gz"

    async def seed(s):
        store = BundleStore(get_settings())
        await store.ensure_bucket()
        await store.put(bundle_ref, content.getvalue(), "application/gzip")
        version = AgentVersion(
            agent_id=world["agent"],
            tenant_id=world["tenant"],
            version_label="example",
            created_by="example",
            bundle_ref=bundle_ref,
        )
        s.add(version)
        await s.flush()
        s.add(
            Deployment(
                agent_id=world["agent"],
                tenant_id=world["tenant"],
                version_id=version.id,
                environment="dev",
            )
        )
        s.add(
            ConnectorCapability(
                agent_id=world["agent"],
                connector=CONNECTOR,
                digest=DIGEST,
                restore_capable=True,
            )
        )
        action = AgentAction(
            agent_id=world["agent"],
            conversation_id="example-thread",
            call_id=uuid.uuid4().hex,
            tool="example-change",
            status="succeeded",
            dedupe_key=uuid.uuid4().hex,
            gate_approval_id=world["approval"],
            prior_state=ENVELOPE,
            target=TARGET,
            post_version="rv-example",
            connector=CONNECTOR,
            connector_digest=DIGEST,
        )
        s.add(action)
        await s.flush()
        return action.id

    action_id = db(seed)
    try:
        yield client, world, action_id
    finally:
        get_settings.cache_clear()


@pytest.mark.parametrize("kind", ["chat", "console"])
def test_gated_undo_records_current_team_authority_and_restores_once(sealed_team_action, kind):
    from curie_api.models import ActionExecution

    client, world, action_id = sealed_team_action
    response = client.post(f"/actions/{action_id}/undo", json={}, headers=headers(world, kind=kind))
    assert response.status_code == 202, response.text
    audit = client.get(f"/actions/{action_id}/audit", headers={"X-API-Key": get_settings().api_key})
    assert audit.status_code == 200, audit.text
    granted = next(entry for entry in audit.json() if entry["action"] == "authorized")
    assert granted["authorizer"] is None
    assert (granted["team_id"], granted["team_epoch"], granted["team_digest"]) == (
        str(world["team"]),
        1,
        "a" * 64,
    )
    again = client.post(f"/actions/{action_id}/undo", json={}, headers=headers(world, kind=kind))
    assert again.status_code == 409, again.text

    async def read(s):
        executions = list(
            await s.scalars(
                select(ActionExecution).where(
                    ActionExecution.subject_action_id == action_id,
                )
            )
        )
        assert len(executions) == 1 and executions[0].state == "requested"

    db(read)


@pytest.mark.parametrize("revocation", ["membership", "status", "route", "generic_gate"])
def test_gated_undo_rechecks_team_after_actual_bundle_read(sealed_team_action, revocation):
    from concurrent.futures import ThreadPoolExecutor

    from curie_api.identity_fence import mutation
    from curie_api.models import ActionAuditEntry, ActionExecution
    from curie_api.storage import BundleStore

    client, world, action_id = sealed_team_action

    async def alter(s):
        engine = create_async_engine(get_settings().database_url)
        redis = Redis.from_url(get_settings().valkey_dsn())
        try:
            async with mutation(async_sessionmaker(engine), redis):
                if revocation in {"membership", "generic_gate"}:
                    await s.execute(
                        delete(PrincipalTeam).where(PrincipalTeam.team_id == world["team"])
                    )
                elif revocation == "status":
                    (await s.get(Principal, world["principal"])).status = "disabled"
                else:
                    (await s.get(Agent, world["agent"])).approval_routes = {
                        "example": {
                            "resolution": {"kind": "slack", "address": CHANNEL},
                            "approvers": {"users": ["U0EXAMPLE2"]},
                        },
                    }
                await s.commit()
        finally:
            await redis.aclose()
            await engine.dispose()

    async def prepare(s):
        approval = await s.get(Approval, world["approval"])
        if revocation == "generic_gate":
            approval.agent_id = None
        elif revocation == "route":
            approval.route = "example"
            (await s.get(Agent, world["agent"])).approval_routes = {
                "example": {"resolution": {"kind": "slack", "address": CHANNEL}},
            }

    db(prepare)

    # Execute the real object-store read, then suspend at its completed future.
    # The writer uses the actual exclusive fence; a stale shared lock deadlocks
    # the bounded future instead of silently making this test pass.
    class ObservedStore(BundleStore):
        async def get(self, key):
            data = await super().get(key)
            writer.submit(db, alter).result(timeout=10)
            return data

    with ThreadPoolExecutor(max_workers=1) as writer:
        client.app.state.bundle_store = ObservedStore(get_settings())
        response = client.post(f"/actions/{action_id}/undo", json={}, headers=headers(world))
    assert response.status_code == 403, response.text

    async def read(s):
        assert not list(
            await s.scalars(
                select(ActionExecution).where(
                    ActionExecution.subject_action_id == action_id,
                )
            )
        )
        entry = await s.scalar(
            select(ActionAuditEntry).where(ActionAuditEntry.action_id == action_id)
        )
        assert entry.action == "refused_unauthorized" and not entry.authorized

    db(read)


@pytest.mark.parametrize("kind", ["chat", "console"])
@pytest.mark.parametrize("revocation", [None, "membership", "status", "proof_missing"])
def test_approved_undo_recovery_rehydrates_proof_and_uses_current_team(
    sealed_team_action, kind, revocation
):
    from curie_api.models import ActionAuditEntry, ActionExecution, RemediationEscalation
    from curie_api.remediation_undo_recovery import reconcile_undo_approvals
    from sqlalchemy import text

    client, world, action_id = sealed_team_action
    trigger = f"undo_team_{uuid.uuid4().hex}"

    async def prepare(s):
        approval = await s.get(Approval, world["approval"])
        approval.purpose = "remediation"
        approval.dedupe_key = f"remediation-undo:{uuid.uuid4()}"
        s.add(
            RemediationEscalation(
                agent_id=world["agent"],
                nomination_id=uuid.uuid4(),
                action_id=action_id,
                outcome="not-recovered",
                undo_approval_id=approval.id,
            )
        )
        await s.execute(
            text(
                f"CREATE FUNCTION curie.{trigger}() RETURNS trigger LANGUAGE plpgsql AS $$ "
                "BEGIN RAISE EXCEPTION 'synthetic restore insert fault'; END $$"
            )
        )
        await s.execute(
            text(
                f"CREATE TRIGGER {trigger} BEFORE INSERT ON curie.action_executions "
                f"FOR EACH ROW WHEN (NEW.subject_action_id = '{action_id}') "
                f"EXECUTE FUNCTION curie.{trigger}()"
            )
        )

    db(prepare)
    try:
        with pytest.raises(Exception, match="synthetic restore insert fault"):
            client.post(
                f"/approvals/{world['approval']}/resolve",
                json={"decision": "approved"},
                headers=headers(world, kind=kind),
            )
    finally:

        async def cleanup(s):
            await s.execute(text(f"DROP TRIGGER IF EXISTS {trigger} ON curie.action_executions"))
            await s.execute(text(f"DROP FUNCTION IF EXISTS curie.{trigger}()"))

        db(cleanup)

    async def recover(s):
        assert (await s.get(Approval, world["approval"])).status == "approved"
        assert not list(
            await s.scalars(
                select(ActionExecution).where(ActionExecution.subject_action_id == action_id)
            )
        )
        if revocation == "membership":
            await s.execute(delete(PrincipalTeam).where(PrincipalTeam.team_id == world["team"]))
        elif revocation == "status":
            (await s.get(Principal, world["principal"])).status = "disabled"
        elif revocation == "proof_missing":
            row = await s.scalar(
                select(ApprovalAuditEntry).where(
                    ApprovalAuditEntry.approval_id == world["approval"],
                    ApprovalAuditEntry.action == "resolved",
                )
            )
            row.evidence = {
                key: value
                for key, value in (row.evidence or {}).items()
                if key != "authenticated_actor"
            }
        await s.commit()
        assert await reconcile_undo_approvals(s, approver_sets=SlackApproverSetSelector(None)) == 1
        executions = list(
            await s.scalars(
                select(ActionExecution).where(ActionExecution.subject_action_id == action_id)
            )
        )
        entry = await s.scalar(
            select(ActionAuditEntry).where(ActionAuditEntry.action_id == action_id)
        )
        assert entry.team_id == world["team"]
        if revocation is None:
            assert len(executions) == 1 and entry.action == "authorized"
            assert executions[0].requested_by == (
                USER if kind == "chat" else str(world["principal"])
            )
        else:
            assert not executions and entry.action == "refused_unauthorized"
        assert await reconcile_undo_approvals(s, approver_sets=SlackApproverSetSelector(None)) == 0

    db(recover)


def test_http_approval_audit_retains_historical_team_after_directory_changes(team_client):
    client, world = team_client
    response = client.post(
        f"/approvals/{world['approval']}/resolve",
        json={"decision": "approved"},
        headers=headers(world),
    )
    assert response.status_code == 200, response.text

    async def change(s):
        team = await s.get(Team, world["team"])
        team.epoch, team.member_count, team.digest = 2, 0, "c" * 64
        await s.execute(delete(PrincipalTeam).where(PrincipalTeam.team_id == world["team"]))

    db(change)
    audit = client.get(
        f"/approvals/{world['approval']}/audit", headers={"X-API-Key": get_settings().api_key}
    )
    assert audit.status_code == 200, audit.text
    resolved = next(entry for entry in audit.json() if entry["action"] == "resolved")
    assert resolved["authorizer"] is None
    assert (resolved["team_id"], resolved["team_epoch"], resolved["team_digest"]) == (
        str(world["team"]),
        1,
        "a" * 64,
    )


@pytest.mark.parametrize(
    "kind,listed,expected",
    [
        ("operator", USER, 403),
        ("test_driver", USER, 401),
        ("chat", "U0EXAMPLE2", 403),
        ("chat", USER, 202),
    ],
)
def test_gated_undo_legacy_explicit_list_only_narrows_current_team(
    sealed_team_action,
    kind,
    listed,
    expected,
):
    client, world, action_id = sealed_team_action

    async def setup(s):
        (await s.get(Agent, world["agent"])).approval_routes = {
            "example": {
                "resolution": {"kind": "slack", "address": CHANNEL},
                "approvers": {"users": [listed]},
            },
        }
        (await s.get(Approval, world["approval"])).route = "example"

    db(setup)
    response = client.post(f"/actions/{action_id}/undo", json={}, headers=headers(world, kind=kind))
    assert response.status_code == expected, response.text
    if kind == "test_driver":
        assert (
            response.json()["detail"] == "test driver principal is limited to approval resolution"
        )
    if expected != 202:
        from curie_api.models import ActionExecution

        async def unchanged(s):
            assert not list(
                await s.scalars(
                    select(ActionExecution).where(
                        ActionExecution.subject_action_id == action_id,
                    )
                )
            )

        db(unchanged)
    if kind == "chat":
        audit = client.get(
            f"/actions/{action_id}/audit", headers={"X-API-Key": get_settings().api_key}
        )
        assert audit.status_code == 200, audit.text
        assert audit.json()[0]["authorizer"] == "ExplicitUserListAuthorizer"
        assert audit.json()[0]["team_id"] == str(world["team"])


@pytest.mark.parametrize("sender", ["alice@example.com", "other@example.com"])
def test_gated_undo_email_requires_exact_verified_mailbox_link(sealed_team_action, sender):
    client, world, action_id = sealed_team_action

    async def setup(s):
        binding = await s.get(AgentChannel, world["binding"])
        binding.kind, binding.adapter, binding.address = (
            "email",
            f"example-mail-{uuid.uuid4().hex}",
            "ops@example.com",
        )
        binding.endpoint = "https://mail.example.com/webhook"
        approval = await s.get(Approval, world["approval"])
        approval.reply_kind, approval.reply_adapter = "email", binding.adapter
        approval.reply_channel, approval.reply_endpoint = binding.address, binding.endpoint
        namespace = IdentityNamespace(
            tenant_id=world["tenant"],
            provider="email",
            authority=ISSUER,
            kind="email_domain",
            key="example.com",
        )
        s.add(namespace)
        await s.flush()
        s.add(
            IdentityLink(
                tenant_id=world["tenant"],
                identity_namespace_id=namespace.id,
                provider_native_id="alice@example.com",
                principal_id=world["principal"],
                verification_source="idp_directory",
                created_by_directory="example-adapter",
            )
        )

    db(setup)
    token = adapter_principal.mint(
        get_settings().api_key,
        subject="example-mail-adapter",
        bindings=frozenset({world["binding"]}),
        exp=int(time.time()) + 60,
    )
    response = client.post(
        f"/actions/{action_id}/undo",
        json={},
        headers={
            "X-Curie-Adapter-Principal": token,
            "X-Curie-Approval-Actor": sender,
        },
    )
    assert response.status_code == (202 if sender == "alice@example.com" else 403), response.text
