"""Operator HTTP authority and issuer group lifecycle contracts."""

import time
import uuid
from typing import Any

import pytest
from curie_api import channel_token
from curie_api.config import get_settings
from fastapi.testclient import TestClient
from principal_adoption_support import selection
from role_policy_support import run, seed


def scope() -> tuple[str, str]:
    result = []

    async def persist(factory: Any) -> None:
        result.extend(str(value) for value in await seed(factory))

    run(persist)
    return result[0], result[1]


def headers() -> dict[str, str]:
    return {"X-API-Key": get_settings().api_key}


def register(client: TestClient, tenant: str, **changes: Any) -> None:
    from curie_api.identity.http_credential_registry import build_provider_credential_registry
    from curie_api.identity.http_invocation import (
        ReceiverRegistrationV1,
        build_http_invocation_runtime,
    )
    from curie_api.identity.http_transport import ProviderBearer
    from curie_api.schemas.http_authority import AuthorityConfigV1
    from curie_api.schemas.http_identity import NamespaceV1

    config = AuthorityConfigV1(
        schema_version=1,
        tenant_id=tenant,
        version=1,
        selection=selection(**changes),
    )
    registry = build_provider_credential_registry(((config, ProviderBearer("SyntheticBearer")),))
    receiver = ReceiverRegistrationV1(
        schema_version=1,
        receiver_id=str(uuid.uuid4()),
        generation=1,
        tenant_id=tenant,
        channel_identity_id=str(uuid.uuid4()),
        channel_identity_name="synthetic-operator",
        provider_installation_id=str(uuid.uuid4()),
        namespace=NamespaceV1(
            id=str(uuid.uuid4()), authority="", kind="slack_workspace", key="T0EXAMPLE1"
        ),
        installation_account="T0EXAMPLE1",
        bot_user_id="U0EXAMPLEBOT",
        binding_ids=(str(uuid.uuid4()),),
        human_native_ids=(),
    )
    client.app.state.http_identity_runtime = build_http_invocation_runtime(
        registry,
        ((receiver, b"r" * 32),),
        (("synthetic-context", 1, b"c" * 32),),
        signing_key=("synthetic-context", 1),
    )


def adoption() -> dict[str, Any]:
    return {
        "request_id": str(uuid.uuid4()),
        "expected_version": 0,
        "expected_lifecycle_generation": 0,
        "selection": selection().model_dump(mode="json"),
    }


def test_authority_lifecycle_is_versioned_idempotent_and_audited(
    clean_db: None,
    hooks_client: TestClient,
) -> None:
    tenant, _ = scope()
    register(hooks_client, tenant)
    path = f"/identity/authorities/{tenant}"
    body = adoption()
    adopted = hooks_client.put(path, headers=headers(), json=body)
    assert adopted.status_code == 200, adopted.text
    assert adopted.json()["configuration"]["version"] == 1
    assert hooks_client.put(path, headers=headers(), json=body).json() == adopted.json()
    assert hooks_client.get(path, headers=headers()).json() == adopted.json()
    listed = hooks_client.get("/identity/authorities", headers=headers()).json()
    assert any(item["configuration"]["tenant_id"] == tenant for item in listed["items"])
    conflict = hooks_client.put(path, headers=headers(), json=adoption())
    assert conflict.status_code == 409 and conflict.json()["detail"]
    changed = {
        **body,
        "request_id": str(uuid.uuid4()),
        "expected_version": 1,
        "expected_lifecycle_generation": 1,
    }
    changed["selection"] = {**body["selection"], "max_identity_age_ms": 4000}
    rotated = hooks_client.post(path + "/replace", headers=headers(), json=changed)
    assert rotated.status_code == 200, rotated.text
    assert rotated.json()["configuration"]["version"] == 2
    assert (
        hooks_client.post(path + "/replace", headers=headers(), json=changed).json()
        == rotated.json()
    )
    retirement = {
        "request_id": str(uuid.uuid4()),
        "expected_version": 2,
        "expected_lifecycle_generation": 2,
    }
    retired = hooks_client.post(path + "/retire", headers=headers(), json=retirement)
    assert retired.status_code == 200, retired.text
    assert retired.json()["configuration"]["version"] == 3
    assert retired.json()["configuration"]["selection"] == {"mode": "disabled"}
    assert (
        hooks_client.post(path + "/retire", headers=headers(), json=retirement).json()
        == retired.json()
    )
    stale = hooks_client.post(
        path + "/replace", headers=headers(), json=changed | {"request_id": str(uuid.uuid4())}
    )
    assert stale.status_code == 409 and stale.json()["detail"]

    async def audits(factory: Any) -> None:
        from curie_api.models import IdentityAuditEvent
        from sqlalchemy import select

        async with factory() as session:
            events = list(
                await session.scalars(
                    select(IdentityAuditEvent).where(
                        IdentityAuditEvent.tenant_id == uuid.UUID(tenant),
                        IdentityAuditEvent.event_type == "identity.operator_request",
                    )
                )
            )
            assert len(events) == 3
            assert {event.details["operation"] for event in events} == {
                "adopt",
                "replace",
                "retire",
            }
            assert all(event.actor == "platform" for event in events)

    run(audits)


@pytest.mark.parametrize(
    "defect", ["wrong_role", "http", "literal_secret", "literal_ref", "id_reuse"]
)
def test_authority_refusals_never_echo_secrets(
    clean_db: None,
    hooks_client: TestClient,
    defect: str,
) -> None:
    tenant, _ = scope()
    register(hooks_client, tenant)
    path = f"/identity/authorities/{tenant}"
    body = adoption()
    auth = headers()
    expected = 422
    if defect == "wrong_role":
        token = channel_token.mint(
            get_settings().api_key,
            channel_id=str(uuid.uuid4()),
            generation=1,
            scope=channel_token.CHANNEL_ENQUEUE_SCOPE,
            exp=int(time.time()) + 60,
        )
        assert (
            channel_token.verify_claims(
                token,
                get_settings().api_key,
                scope=channel_token.CHANNEL_ENQUEUE_SCOPE,
            )
            is not None
        )
        auth = {"X-API-Key": token}
        expected = 401
    elif defect == "http":
        body["selection"]["lookup_url"] = "http://idp.example.com/lookup"
    elif defect == "literal_secret":
        body["selection"]["bearer"] = "PRIVATE_LITERAL_MARKER"
    elif defect == "literal_ref":
        body["selection"]["credential_ref"] = "PRIVATE_LITERAL_MARKER"
    else:
        assert hooks_client.put(path, headers=auth, json=body).status_code == 200
        body["selection"]["max_identity_age_ms"] = 4000
        expected = 409
    response = hooks_client.put(path, headers=auth, json=body)
    assert response.status_code == expected, response.text
    assert response.json()["detail"]
    assert "PRIVATE_LITERAL_MARKER" not in response.text
    assert response.headers["cache-control"].endswith("no-store")


def test_group_filters_preserve_roles_and_reject_stale_writes(
    clean_db: None,
    hooks_client: TestClient,
) -> None:
    tenant, agent = scope()
    register(hooks_client, tenant)
    assert (
        hooks_client.put(
            f"/identity/authorities/{tenant}",
            headers=headers(),
            json=adoption(),
        ).status_code
        == 200
    )
    path = f"/identity/roles/{tenant}/{agent}"
    group = {"issuer": selection().issuer, "id": "operators"}
    body = {"request_id": str(uuid.uuid4()), "expected_version": 0, "groups": [group]}
    response = hooks_client.put(path + "/invocation", headers=headers(), json=body)
    assert response.status_code == 200, response.text
    assert (
        hooks_client.put(path + "/invocation", headers=headers(), json=body).json()
        == response.json()
    )
    assert hooks_client.get(path, headers=headers()).json()["roles"]["invocation"]["filter"][
        "entries"
    ] == [{"kind": "issuer_group", "group": group}]
    stale = hooks_client.put(
        path + "/approver", headers=headers(), json=body | {"request_id": str(uuid.uuid4())}
    )
    assert stale.status_code == 409 and stale.json()["detail"]
    forbidden = hooks_client.put(
        path + "/invocation", headers={"X-API-Key": "wrong-role"}, json=body
    )
    assert forbidden.status_code == 401 and forbidden.json()["detail"]
    invalid_role = hooks_client.put(path + "/administrator", headers=headers(), json=body)
    assert invalid_role.status_code == 422 and invalid_role.json()["detail"]
    removal = {"request_id": str(uuid.uuid4()), "expected_version": 1}
    removed = hooks_client.request(
        "DELETE",
        path + "/invocation",
        headers=headers(),
        json=removal,
    )
    assert removed.status_code == 200, removed.text
    assert removed.json()["roles"]["invocation"]["filter"] is None
    assert (
        hooks_client.request(
            "DELETE",
            path + "/invocation",
            headers=headers(),
            json=removal,
        ).json()
        == removed.json()
    )
    approver = hooks_client.put(
        path + "/approver",
        headers=headers(),
        json=body | {"request_id": str(uuid.uuid4()), "expected_version": 2},
    )
    assert approver.status_code == 200, approver.text
    assert approver.json()["roles"]["invocation"]["filter"] is None
    assert approver.json()["roles"]["approver"]["filter"]["entries"] == [
        {"kind": "issuer_group", "group": group}
    ]

    async def evidence(factory: Any) -> None:
        from curie_api.models import IdentityAuditEvent
        from sqlalchemy import select

        async with factory() as session:
            events = list(
                await session.scalars(
                    select(IdentityAuditEvent).where(
                        IdentityAuditEvent.tenant_id == uuid.UUID(tenant),
                        IdentityAuditEvent.event_type == "identity.operator_request",
                    )
                )
            )
            events = [event for event in events if event.details["operation"] != "adopt"]
            assert len(events) == 3
            assert [event.details["operation"] for event in events].count("remove_filter") == 1

    run(evidence)


def test_retirement_refuses_principal_and_console_authorization(
    clean_db: None,
    hooks_client: TestClient,
) -> None:
    from curie_api import principal_authority
    from curie_api.crud import console
    from curie_api.identity.connect import configured_provider
    from principal_adoption_support import claims

    tenant, _ = scope()
    register(hooks_client, tenant)
    path = f"/identity/authorities/{tenant}"
    assert hooks_client.put(path, headers=headers(), json=adoption()).status_code == 200
    retire = {
        "request_id": str(uuid.uuid4()),
        "expected_version": 1,
        "expected_lifecycle_generation": 1,
    }
    assert hooks_client.post(path + "/retire", headers=headers(), json=retire).status_code == 200

    async def verify(factory: Any) -> None:
        async with factory() as session, session.begin():
            with pytest.raises(
                principal_authority.PrincipalAuthorityRefusal, match="http_authority_required"
            ):
                await principal_authority.guarded_principal(
                    session, tenant_id=uuid.UUID(tenant), principal_id=uuid.uuid4()
                )
            with pytest.raises(
                principal_authority.PrincipalAuthorityRefusal, match="http_authority_required"
            ):
                await console.resolve_principal(session, claims(), tenant_id=uuid.UUID(tenant))
            assert await configured_provider(session, uuid.UUID(tenant)) is None

    run(verify)
    replacement = adoption() | {"expected_version": 2, "expected_lifecycle_generation": 2}
    response = hooks_client.post(path + "/replace", headers=headers(), json=replacement)
    assert response.status_code == 200, response.text
    assert response.json()["configuration"]["version"] == 3


def test_stale_generation_and_duplicate_group_refusals_are_fixed(
    clean_db: None,
    hooks_client: TestClient,
) -> None:
    tenant, agent = scope()
    register(hooks_client, tenant)
    path = f"/identity/authorities/{tenant}"
    body = adoption()
    assert hooks_client.put(path, headers=headers(), json=body).status_code == 200
    stale = body | {"request_id": str(uuid.uuid4()), "expected_version": 1}
    response = hooks_client.post(path + "/replace", headers=headers(), json=stale)
    assert response.status_code == 409
    assert response.json() == {"detail": "stale_lifecycle_generation"}
    group = {"issuer": selection().issuer, "id": "operators"}
    response = hooks_client.put(
        f"/identity/roles/{tenant}/{agent}/invocation",
        headers=headers(),
        json={"request_id": str(uuid.uuid4()), "expected_version": 0, "groups": [group, group]},
    )
    assert response.status_code == 422
    assert response.json() == {"detail": "invalid_identity_operator_request"}
    assert (
        hooks_client.get(f"/identity/roles/{tenant}/{agent}", headers=headers()).json()["version"]
        == 0
    )


@pytest.mark.parametrize("authority", ["absent", "local", "retired", "foreign_issuer"])
def test_group_put_requires_current_http_authority_and_its_issuer(
    clean_db: None,
    hooks_client: TestClient,
    authority: str,
) -> None:
    from curie_api.models import TenantIdentityProvider

    tenant, agent = scope()
    if authority == "local":

        async def local(factory: Any) -> None:
            async with factory() as session, session.begin():
                session.add(
                    TenantIdentityProvider(
                        tenant_id=uuid.UUID(tenant),
                        issuer=selection().issuer,
                        governed_domains=["example.test"],
                        managed_by="synthetic-directory",
                        version=1,
                        identity_epoch=0,
                    )
                )

        run(local)
    elif authority != "absent":
        register(hooks_client, tenant)
        assert (
            hooks_client.put(
                f"/identity/authorities/{tenant}", headers=headers(), json=adoption()
            ).status_code
            == 200
        )
        if authority == "retired":
            assert (
                hooks_client.post(
                    f"/identity/authorities/{tenant}/retire",
                    headers=headers(),
                    json={
                        "request_id": str(uuid.uuid4()),
                        "expected_version": 1,
                        "expected_lifecycle_generation": 1,
                    },
                ).status_code
                == 200
            )
    issuer = "https://other.example.test" if authority == "foreign_issuer" else selection().issuer
    response = hooks_client.put(
        f"/identity/roles/{tenant}/{agent}/invocation",
        headers=headers(),
        json={
            "request_id": str(uuid.uuid4()),
            "expected_version": 0,
            "groups": [{"issuer": issuer, "id": "operators"}],
        },
    )
    assert response.status_code == 409, response.text
    assert response.json() == {
        "detail": "authority_scope_mismatch"
        if authority == "foreign_issuer"
        else "http_authority_required"
    }
    assert (
        hooks_client.get(f"/identity/roles/{tenant}/{agent}", headers=headers()).json()["version"]
        == 0
    )


@pytest.mark.parametrize("operation", ["adopt", "replace"])
@pytest.mark.parametrize("dry_run", [False, True])
@pytest.mark.parametrize("defect", ["absent_runtime", "reference", "tenant", "issuer", "endpoint"])
def test_unregistered_provider_is_refused_without_changing_authority(
    clean_db: None,
    hooks_client: TestClient,
    operation: str,
    dry_run: bool,
    defect: str,
) -> None:
    tenant, _ = scope()
    path = f"/identity/authorities/{tenant}"
    register(hooks_client, tenant)
    body = adoption()
    previous = None
    if operation == "replace":
        previous = hooks_client.put(path, headers=headers(), json=body).json()
        body |= {
            "request_id": str(uuid.uuid4()),
            "expected_version": 1,
            "expected_lifecycle_generation": 1,
        }
    if defect == "absent_runtime":
        hooks_client.app.state.http_identity_runtime = None
    elif defect == "tenant":
        register(hooks_client, str(uuid.uuid4()))
    elif defect == "reference":
        body["selection"]["credential_ref"]["generation"] += 1
    elif defect == "issuer":
        register(hooks_client, tenant, issuer="https://other.example.test")
    else:
        body["selection"]["lookup_url"] = "https://other.example.test/lookup"
    target = path + ("/replace" if operation == "replace" else "")
    response = hooks_client.request(
        "POST" if operation == "replace" else "PUT",
        target,
        params={"dry_run": str(dry_run).lower()},
        headers=headers(),
        json=body,
    )
    assert response.status_code == 409, response.text
    assert response.json() == {"detail": "provider_credential_unavailable"}
    current = hooks_client.get(path, headers=headers())
    assert current.json() == previous if previous is not None else current.status_code == 404


@pytest.mark.parametrize("operation", ["adopt", "replace"])
def test_registered_authority_dry_run_validates_without_committing(
    clean_db: None,
    hooks_client: TestClient,
    operation: str,
) -> None:
    tenant, _ = scope()
    register(hooks_client, tenant)
    path = f"/identity/authorities/{tenant}"
    body = adoption()
    if operation == "replace":
        assert hooks_client.put(path, headers=headers(), json=body).status_code == 200
        body |= {
            "request_id": str(uuid.uuid4()),
            "expected_version": 1,
            "expected_lifecycle_generation": 1,
        }
    response = hooks_client.request(
        "PUT" if operation == "adopt" else "POST",
        path + ("/replace" if operation == "replace" else ""),
        params={"dry_run": "true"},
        headers=headers(),
        json=body,
    )
    assert response.status_code == 200, response.text
    assert response.json()["dry_run"] is True
    assert response.json()["configuration"]["version"] == (1 if operation == "adopt" else 2)
    current = hooks_client.get(path, headers=headers())
    assert (
        current.status_code == 404
        if operation == "adopt"
        else current.json()["configuration"]["version"] == 1
    )

    async def evidence(factory: Any) -> None:
        from curie_api.models import IdentityAuditEvent
        from sqlalchemy import func, select

        async with factory() as session:
            count = await session.scalar(
                select(func.count())
                .select_from(IdentityAuditEvent)
                .where(IdentityAuditEvent.tenant_id == uuid.UUID(tenant))
            )
            assert count == (0 if operation == "adopt" else 2)

    run(evidence)


def test_retire_revokes_issuer_sessions_once_and_reactivation_revokes_inactive_sessions(
    clean_db: None,
    hooks_client: TestClient,
) -> None:
    from curie_api.models import ConsoleSession, IdentityAuditEvent, Principal
    from principal_adoption_support import cookie
    from sqlalchemy import select

    tenant, _ = scope()
    register(hooks_client, tenant)
    path = f"/identity/authorities/{tenant}"
    assert hooks_client.put(path, headers=headers(), json=adoption()).status_code == 200
    sessions = {}
    principals = {}

    async def persist(factory: Any) -> None:
        async with factory() as session, session.begin():
            for status in ("active", "disabled", "foreign"):
                principal = Principal(
                    tenant_id=uuid.UUID(tenant),
                    idp_issuer="https://other.example.test"
                    if status == "foreign"
                    else selection().issuer,
                    idp_subject="synthetic-" + status,
                    type="human",
                    status="active" if status == "foreign" else status,
                    authorization_version=1,
                )
                session.add(principal)
                await session.flush()
                principals[status] = principal.id
                _, row = await cookie(session, principal.id)
                sessions[status] = row.id
            _, row = await cookie(session, None)
            sessions["operator"] = row.id

    run(persist)
    body = {
        "request_id": str(uuid.uuid4()),
        "expected_version": 1,
        "expected_lifecycle_generation": 1,
    }
    retired = hooks_client.post(path + "/retire", headers=headers(), json=body)
    assert retired.status_code == 200, retired.text
    assert (
        hooks_client.post(path + "/retire", headers=headers(), json=body).json() == retired.json()
    )

    async def inspect_retirement(factory: Any) -> None:
        async with factory() as session, session.begin():
            for status, identifier in sessions.items():
                row = await session.get(ConsoleSession, identifier)
                assert (row.revoked_at is not None) == (status in {"active", "disabled"})
            events = list(
                await session.scalars(
                    select(IdentityAuditEvent).where(
                        IdentityAuditEvent.tenant_id == uuid.UUID(tenant),
                        IdentityAuditEvent.event_type == "identity.http_authority_retired",
                    )
                )
            )
            assert len(events) == 1
            assert events[0].actor == "platform"
            assert events[0].details == {
                "authority_version": 2,
                "lifecycle_generation": 2,
                "revoked_session_count": 2,
            }
            for status in ("active", "disabled"):
                _, row = await cookie(session, principals[status])
                sessions["new_" + status] = row.id

    run(inspect_retirement)
    replacement = adoption() | {"expected_version": 2, "expected_lifecycle_generation": 2}
    preview = hooks_client.post(path + "/replace?dry_run=true", headers=headers(), json=replacement)
    assert preview.status_code == 200, preview.text

    async def inspect_preview(factory: Any) -> None:
        async with factory() as session:
            assert (await session.get(ConsoleSession, sessions["new_disabled"])).revoked_at is None

    run(inspect_preview)
    changed = hooks_client.post(path + "/replace", headers=headers(), json=replacement)
    assert changed.status_code == 200, changed.text
    assert (
        hooks_client.post(path + "/replace", headers=headers(), json=replacement).json()
        == changed.json()
    )

    async def inspect_reactivation(factory: Any) -> None:
        async with factory() as session:
            assert (
                await session.get(ConsoleSession, sessions["new_disabled"])
            ).revoked_at is not None
            assert (await session.get(ConsoleSession, sessions["new_active"])).revoked_at is None
            events = list(
                await session.scalars(
                    select(IdentityAuditEvent).where(
                        IdentityAuditEvent.tenant_id == uuid.UUID(tenant),
                        IdentityAuditEvent.event_type == "identity.http_authority_changed",
                    )
                )
            )
            assert len(events) == 1
            assert events[0].details["revoked_session_count"] == 1

    run(inspect_reactivation)


def test_group_put_replaces_surface_account_and_issuer_subject_selectors(
    clean_db: None,
    hooks_client: TestClient,
) -> None:
    from curie_api import agent_role_policy
    from role_policy_support import change

    tenant, agent = scope()
    register(hooks_client, tenant)
    assert (
        hooks_client.put(
            f"/identity/authorities/{tenant}", headers=headers(), json=adoption()
        ).status_code
        == 200
    )

    async def old_filter(factory: Any) -> None:
        async with factory() as session, session.begin():
            await agent_role_policy.apply_role_change(
                session,
                change=change(
                    uuid.UUID(tenant),
                    uuid.UUID(agent),
                    filter={
                        "entries": [
                            {
                                "kind": "issuer_subject",
                                "issuer": selection().issuer,
                                "subject": "synthetic-human",
                            },
                            {
                                "kind": "surface_account",
                                "provider": "slack",
                                "native_id": "U0EXAMPLE1",
                                "namespace": {
                                    "id": str(uuid.uuid4()),
                                    "authority": "",
                                    "kind": "slack_workspace",
                                    "key": "T0EXAMPLE1",
                                },
                            },
                        ]
                    },
                ),
            )

    run(old_filter)
    group = {"issuer": selection().issuer, "id": "operators"}
    response = hooks_client.put(
        f"/identity/roles/{tenant}/{agent}/invocation",
        headers=headers(),
        json={
            "request_id": str(uuid.uuid4()),
            "expected_version": 1,
            "groups": [group],
        },
    )
    assert response.status_code == 200, response.text
    assert response.json()["roles"]["invocation"]["filter"]["entries"] == [
        {"kind": "issuer_group", "group": group}
    ]
