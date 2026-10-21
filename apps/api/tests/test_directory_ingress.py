"""Authenticated directory writes through HTTP and a real disposable Postgres."""

import uuid
from collections.abc import Iterator

import pytest
from curie_api.config import get_settings
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import NullPool


@pytest.fixture
def directory_client(migrated: None) -> Iterator[TestClient]:
    from curie_api.db import create_sessionmaker
    from curie_api.routers.directory import router

    app = FastAPI()
    app.state.sessionmaker = create_sessionmaker(
        create_async_engine(get_settings().database_url, poolclass=NullPool)
    )
    app.include_router(router)
    with TestClient(app) as client:
        yield client


def test_directory_credential_is_strictly_sibling_scoped() -> None:
    from curie_api.directory_credential import mint, verify
    from curie_api.directory_sync import DirectoryScope

    scope = DirectoryScope(
        uuid.uuid4(), "https://idp.example.com", "example-adapter", frozenset({uuid.uuid4()})
    )
    token = mint("test-secret", scope=scope, namespace_ids=frozenset(), config_version=1, exp=200)
    assert verify(token, "test-secret", now=199) is not None
    assert verify(token, "test-secret", now=201) is None
    for value in [
        token.replace("dir.", "chn.", 1),
        token + ".extra",
        "test-secret",
        "dir.bad.bad",
    ]:
        assert verify(value, "test-secret", now=100) is None
    assert verify(token, "wrong-secret", now=100) is None


def setup_directory(client: TestClient) -> tuple[str, str, str, dict[str, str]]:
    import asyncio

    from curie_api.models import Tenant
    from sqlalchemy.ext.asyncio import AsyncSession

    tenant_id = uuid.uuid4()

    async def seed() -> None:
        engine = create_async_engine(get_settings().database_url)
        try:
            async with AsyncSession(engine) as session:
                session.add(
                    Tenant(id=tenant_id, deployment_id=f"directory-{tenant_id}", status="active")
                )
                await session.commit()
        finally:
            await engine.dispose()

    asyncio.run(seed())
    admin = {"X-API-Key": get_settings().api_key}
    config = {
        "issuer": "https://idp.example.com",
        "governed_domains": ["example.com"],
        "managed_by": "example-adapter",
        "expected_version": 0,
    }
    response = client.put(f"/directory/config/{tenant_id}", json=config, headers=admin)
    assert response.status_code == 200, response.text
    response = client.post(
        "/directory/teams",
        json={"tenant_id": str(tenant_id), "external_id": "group-example", "name": "Example team"},
        headers=admin,
    )
    assert response.status_code == 201, response.text
    team_id = response.json()["id"]
    response = client.post(
        "/directory/credentials",
        json={
            "tenant_id": str(tenant_id),
            "expected_config_version": 1,
            "team_ids": [team_id],
            "namespace_ids": [],
            "expires_in_seconds": 3600,
        },
        headers=admin,
    )
    assert response.status_code == 201, response.text
    return str(tenant_id), team_id, response.json()["credential"], admin


def test_http_directory_snapshots_status_cas_and_revocation(directory_client: TestClient) -> None:
    client = directory_client
    tenant_id, team_id, token, admin = setup_directory(client)
    headers = {"X-Curie-Directory-Credential": token}
    path = f"/directory/teams/{team_id}/snapshot"
    payload = {
        "expected_epoch": 0,
        "complete": True,
        "people": [
            {
                "subject": "person-example",
                "display_name": "Example person",
                "status": "disabled",
                "accounts": [],
            }
        ],
    }
    assert client.put(path, json=payload, headers=admin).status_code == 401
    response = client.put(path, json=payload, headers=headers)
    assert response.status_code == 200, response.text
    assert response.json()["member_count"] == 1
    assert client.put(path, json=payload, headers=headers).status_code == 409
    people = client.get("/directory/people", headers=headers).json()["people"]
    assert people == [
        {"subject": "person-example", "status": "disabled", "authorization_version": 1}
    ]
    payload["expected_epoch"] = 1
    payload["people"][0]["status"] = "active"
    assert client.put(path, json=payload, headers=headers).status_code == 200
    assert (
        client.get("/directory/people", headers=headers).json()["people"][0]["status"] == "disabled"
    )
    status = {"subject": "person-example", "expected_version": 1, "status": "active"}
    assert client.post("/directory/people/status", json=status, headers=headers).json() == {
        "authorization_version": 2
    }
    assert client.post("/directory/people/status", json=status, headers=headers).status_code == 409
    partial = {"expected_epoch": 2, "complete": False, "people": []}
    assert client.put(path, json=partial, headers=headers).status_code == 422
    state = client.get(f"/directory/teams/{team_id}", headers=headers).json()
    assert state["epoch"] == 2 and state["last_error"] == "invalid_snapshot"
    partial["complete"] = True
    assert client.put(path, json=partial, headers=headers).json()["member_count"] == 0
    assert (
        client.put(
            f"/directory/teams/{uuid.uuid4()}/snapshot", json=partial, headers=headers
        ).status_code
        == 403
    )
    updated = {
        "issuer": "https://idp.example.com",
        "governed_domains": ["example.com"],
        "managed_by": "example-adapter",
        "expected_version": 1,
    }
    assert (
        client.put(f"/directory/config/{tenant_id}", json=updated, headers=headers).status_code
        == 401
    )
    assert (
        client.put(f"/directory/config/{tenant_id}", json=updated, headers=admin).status_code == 200
    )
    assert client.get("/directory/people", headers=headers).status_code == 401


def namespace_credential(
    client: TestClient,
    tenant_id: str,
    team_id: str,
    admin: dict[str, str],
    key: str,
    provider: str = "slack",
) -> tuple[str, str]:
    kind = "slack_workspace" if provider == "slack" else "email_domain"
    response = client.post(
        "/directory/namespaces",
        json={"tenant_id": tenant_id, "provider": provider, "kind": kind, "key": key},
        headers=admin,
    )
    assert response.status_code == 201, response.text
    namespace_id = response.json()["id"]
    response = client.post(
        "/directory/credentials",
        json={
            "tenant_id": tenant_id,
            "expected_config_version": 1,
            "team_ids": [team_id],
            "namespace_ids": [namespace_id],
        },
        headers=admin,
    )
    assert response.status_code == 201, response.text
    return namespace_id, response.json()["credential"]


def test_directory_links_replace_reassign_and_preserve_namespace_scope(
    directory_client: TestClient,
) -> None:
    from _migration_support import sql_dicts

    client = directory_client
    tenant_id, team_id, _, admin = setup_directory(client)
    namespace_id, token = namespace_credential(client, tenant_id, team_id, admin, "TEXAMPLE1")
    outside_id, outside_token = namespace_credential(client, tenant_id, team_id, admin, "TEXAMPLE2")
    path = f"/directory/teams/{team_id}/snapshot"
    headers = {"X-Curie-Directory-Credential": token}

    def person(subject: str, namespace: str, native: str) -> dict[str, object]:
        return {
            "subject": subject,
            "accounts": [{"identity_namespace_id": namespace, "provider_native_id": native}],
        }

    def snapshot(epoch: int, people: list[dict[str, object]], credential: str = token) -> None:
        binding = client.get(
            "/directory/binding", headers={"X-Curie-Directory-Credential": credential}
        )
        assert binding.status_code == 200
        result = client.put(
            path,
            json={
                "expected_epoch": epoch,
                "expected_identity_epoch": binding.json()["identity_epoch"],
                "complete": True,
                "people": people,
            },
            headers={"X-Curie-Directory-Credential": credential},
        )
        assert result.status_code == 200, result.text

    snapshot(0, [person("person-a", outside_id, "UEXAMPLE2")], outside_token)
    snapshot(1, [person("person-a", namespace_id, "UEXAMPLE1")])
    snapshot(
        2, [person("person-b", namespace_id, "UEXAMPLE1"), {"subject": "person-a", "accounts": []}]
    )
    rows = sql_dicts(
        "SELECT p.idp_subject, l.identity_namespace_id::text AS namespace, l.revoked_at, "
        "l.created_by_principal_id, l.created_by_directory FROM curie.identity_links l JOIN "
        "curie.principals p ON p.id=l.principal_id WHERE l.tenant_id=:tenant",
        {"tenant": uuid.UUID(tenant_id)},
    )
    active = {(row["idp_subject"], row["namespace"]) for row in rows if row["revoked_at"] is None}
    assert active == {("person-a", outside_id), ("person-b", namespace_id)}
    assert all(
        row["created_by_principal_id"] is None and row["created_by_directory"] == "example-adapter"
        for row in rows
    )
    snapshot(3, [{"subject": "person-b", "accounts": []}])
    active = sql_dicts(
        "SELECT provider_native_id FROM curie.identity_links WHERE tenant_id=:tenant "
        "AND identity_namespace_id=:namespace AND revoked_at IS NULL",
        {"tenant": uuid.UUID(tenant_id), "namespace": uuid.UUID(namespace_id)},
    )
    assert active == []
    result = client.put(
        path,
        json={
            "expected_epoch": 4,
            "complete": True,
            "people": [person("person-c", outside_id, "UEXAMPLE3")],
        },
        headers=headers,
    )
    assert result.status_code == 403
    state = client.get(f"/directory/teams/{team_id}", headers=headers).json()
    assert state["epoch"] == 4 and state["member_count"] == 1
    # A rejected new-person snapshot leaves no person, link or applied audit.
    assert (
        sql_dicts(
            "SELECT id FROM curie.principals WHERE tenant_id=:tenant AND idp_subject='person-c'",
            {"tenant": uuid.UUID(tenant_id)},
        )
        == []
    )
    audit = sql_dicts(
        "SELECT details FROM curie.identity_audit_events WHERE tenant_id=:tenant "
        "AND event_type='identity.directory_snapshot_applied' ORDER BY created_at",
        {"tenant": uuid.UUID(tenant_id)},
    )
    assert [row["details"]["epoch"] for row in audit] == [1, 2, 3, 4]


def test_email_governance_invalid_input_redaction_and_atomic_failure(
    directory_client: TestClient,
) -> None:
    import asyncio

    import httpx
    from _migration_support import sql_dicts

    client = directory_client
    tenant_id, team_id, _, admin = setup_directory(client)
    namespace_id, token = namespace_credential(
        client, tenant_id, team_id, admin, "example.com", "email"
    )
    headers = {"X-Curie-Directory-Credential": token}
    path = f"/directory/teams/{team_id}/snapshot"
    good = {
        "expected_epoch": 0,
        "expected_identity_epoch": 0,
        "complete": True,
        "people": [{"subject": "person-a", "email": "one@example.com", "accounts": []}],
    }
    assert client.put(path, json=good, headers=headers).status_code == 200
    # The IdP reassigns a mailbox. Exactly one active link changes hands.
    good["expected_epoch"] = 1
    good["expected_identity_epoch"] = 1
    good["people"] = [{"subject": "person-b", "email": "one@example.com", "accounts": []}]
    assert client.put(path, json=good, headers=headers).status_code == 200
    rows = sql_dicts(
        "SELECT p.idp_subject FROM curie.identity_links l JOIN curie.principals p "
        "ON p.id=l.principal_id WHERE l.tenant_id=:tenant AND l.identity_namespace_id=:namespace "
        "AND l.revoked_at IS NULL",
        {"tenant": uuid.UUID(tenant_id), "namespace": uuid.UUID(namespace_id)},
    )
    assert rows == [{"idp_subject": "person-b"}]
    invalid = {
        "expected_epoch": 2,
        "complete": True,
        "people": [{"subject": "person-c", "email": "private@outside.example", "accounts": []}],
    }
    response = client.put(path, json=invalid, headers=headers)
    assert response.status_code == 422 and response.json() == {"detail": "ungoverned_email"}
    invalid["people"][0]["accounts"] = "private-provider-payload"

    async def malformed_request():
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=client.app), base_url="http://testserver"
        ) as request_client:
            return await asyncio.wait_for(
                request_client.put(path, json=invalid, headers=headers), timeout=5
            )

    # Failure recording uses another DB session; it must not wait on this
    # malformed request's dependency lock while that request is unwinding.
    response = asyncio.run(malformed_request())
    assert response.status_code == 422 and "private-provider-payload" not in response.text
    assert "private@" not in response.text
    state = client.get(f"/directory/teams/{team_id}", headers=headers).json()
    assert (
        state["epoch"] == 2
        and state["member_count"] == 1
        and state["last_error"] == "invalid_snapshot"
    )
    assert (
        sql_dicts(
            "SELECT id FROM curie.principals WHERE tenant_id=:tenant AND idp_subject='person-c'",
            {"tenant": uuid.UUID(tenant_id)},
        )
        == []
    )
    duplicates = {
        "expected_epoch": 2,
        "complete": True,
        "people": [
            {"subject": "person-new", "accounts": []},
            {"subject": "person-new", "accounts": []},
        ],
    }
    assert client.put(path, json=duplicates, headers=headers).status_code == 422
    assert (
        sql_dicts(
            "SELECT id FROM curie.principals WHERE tenant_id=:tenant AND idp_subject='person-new'",
            {"tenant": uuid.UUID(tenant_id)},
        )
        == []
    )


def test_tenant_scope_cas_and_credential_cannot_administer(directory_client: TestClient) -> None:
    client = directory_client
    tenant_id, team_id, token, admin = setup_directory(client)
    other_tenant, other_team, _, _ = setup_directory(client)
    headers = {"X-Curie-Directory-Credential": token}
    assert client.get(f"/directory/teams/{other_team}", headers=headers).status_code == 403
    assert (
        client.post(
            "/directory/credentials",
            json={
                "tenant_id": tenant_id,
                "expected_config_version": 1,
                "team_ids": [other_team],
                "namespace_ids": [],
            },
            headers=admin,
        ).status_code
        == 403
    )
    assert (
        client.post(
            "/directory/teams",
            json={"tenant_id": other_tenant, "external_id": "malicious", "name": "Example"},
            headers=headers,
        ).status_code
        == 401
    )
    assert (
        client.post(
            "/directory/namespaces",
            json={
                "tenant_id": tenant_id,
                "provider": "email",
                "kind": "email_domain",
                "key": "outside.example",
            },
            headers=admin,
        ).status_code
        == 422
    )
    assert (
        client.post(
            "/directory/people/status",
            json={"subject": "missing", "expected_version": 1, "status": "active"},
            headers=headers,
        ).status_code
        == 422
    )
    assert client.get("/directory/people", headers={**headers, **admin}).status_code == 401
    config = {
        "issuer": "https://idp.example.com",
        "governed_domains": ["example.com"],
        "managed_by": "example-adapter",
        "expected_version": 0,
    }
    assert (
        client.put(f"/directory/config/{tenant_id}", json=config, headers=admin).status_code == 409
    )
    assert client.get(f"/directory/teams/{team_id}", headers=headers).json()["epoch"] == 0


def test_invalid_later_person_rolls_back_earlier_projection_and_links(
    directory_client: TestClient,
) -> None:
    from _migration_support import sql_dicts

    client = directory_client
    tenant_id, team_id, _, admin = setup_directory(client)
    namespace_id, token = namespace_credential(client, tenant_id, team_id, admin, "TEXAMPLE1")
    sql_dicts(
        "INSERT INTO curie.principals (id,tenant_id,idp_issuer,idp_subject,type) VALUES "
        "(:id,:tenant,'https://idp.example.com','z-service','service')",
        {"id": uuid.uuid4(), "tenant": uuid.UUID(tenant_id)},
    )
    response = client.put(
        f"/directory/teams/{team_id}/snapshot",
        json={
            "expected_epoch": 0,
            "expected_identity_epoch": 0,
            "complete": True,
            "people": [
                {
                    "subject": "a-new",
                    "accounts": [
                        {"identity_namespace_id": namespace_id, "provider_native_id": "UEXAMPLE1"}
                    ],
                },
                {"subject": "z-service", "accounts": []},
            ],
        },
        headers={"X-Curie-Directory-Credential": token},
    )
    assert response.status_code == 403
    assert (
        sql_dicts(
            "SELECT id FROM curie.principals WHERE tenant_id=:tenant AND idp_subject='a-new'",
            {"tenant": uuid.UUID(tenant_id)},
        )
        == []
    )
    assert (
        sql_dicts(
            "SELECT id FROM curie.identity_links WHERE tenant_id=:tenant",
            {"tenant": uuid.UUID(tenant_id)},
        )
        == []
    )
    applied = sql_dicts(
        "SELECT id FROM curie.identity_audit_events WHERE tenant_id=:tenant AND "
        "event_type IN ('identity.directory_link_created','identity.directory_snapshot_applied')",
        {"tenant": uuid.UUID(tenant_id)},
    )
    assert applied == []
    failures = sql_dicts(
        "SELECT details FROM curie.identity_audit_events WHERE tenant_id=:tenant "
        "AND event_type='identity.directory_sync_failed'",
        {"tenant": uuid.UUID(tenant_id)},
    )
    assert failures == [{"details": {"epoch": 0, "error": "invalid_snapshot"}}]
    assert client.get(
        "/directory/binding", headers={"X-Curie-Directory-Credential": token}
    ).json()["identity_epoch"] == 0


def test_http_directory_email_links_resolve_only_under_current_configured_issuer(
    directory_client: TestClient,
) -> None:
    import asyncio

    from curie_api.team_authorization import resolve_email
    from sqlalchemy.ext.asyncio import AsyncSession

    client = directory_client
    tenant_id, team_id, _, admin = setup_directory(client)
    namespace_id, token = namespace_credential(
        client, tenant_id, team_id, admin, "example.com", "email"
    )
    path = f"/directory/teams/{team_id}/snapshot"
    snapshot = {
        "expected_epoch": 0,
        "expected_identity_epoch": 0,
        "complete": True,
        "people": [{"subject": "mailbox-person", "email": "person@example.com", "accounts": []}],
    }
    assert (
        client.put(path, json=snapshot, headers={"X-Curie-Directory-Credential": token}).status_code
        == 200
    )

    async def resolve():
        engine = create_async_engine(get_settings().database_url)
        try:
            async with AsyncSession(engine) as session:
                return await resolve_email(
                    session, tenant_id=uuid.UUID(tenant_id), email="person@example.com"
                )
        finally:
            await engine.dispose()

    first_person = asyncio.run(resolve())
    assert first_person is not None
    updated = {
        "issuer": "https://new-idp.example.com",
        "governed_domains": ["example.com"],
        "managed_by": "example-adapter",
        "expected_version": 1,
    }
    assert (
        client.put(f"/directory/config/{tenant_id}", json=updated, headers=admin).status_code == 200
    )
    assert asyncio.run(resolve()) is None
    refused = client.post(
        "/directory/credentials",
        json={
            "tenant_id": tenant_id,
            "expected_config_version": 2,
            "team_ids": [team_id],
            "namespace_ids": [namespace_id],
        },
        headers=admin,
    )
    assert refused.status_code == 403
    hostile = client.post(
        "/directory/namespaces",
        json={
            "tenant_id": tenant_id,
            "provider": "email",
            "kind": "email_domain",
            "key": "example.com",
            "authority": "https://other-idp.example.com",
        },
        headers=admin,
    )
    assert hostile.status_code == 422
    fresh = client.post(
        "/directory/namespaces",
        json={
            "tenant_id": tenant_id,
            "provider": "email",
            "kind": "email_domain",
            "key": "example.com",
        },
        headers=admin,
    )
    assert fresh.status_code == 201 and fresh.json()["id"] != namespace_id
    credential = client.post(
        "/directory/credentials",
        json={
            "tenant_id": tenant_id,
            "expected_config_version": 2,
            "team_ids": [team_id],
            "namespace_ids": [fresh.json()["id"]],
        },
        headers=admin,
    )
    assert credential.status_code == 201, credential.text
    snapshot["expected_epoch"] = 1
    snapshot["expected_identity_epoch"] = 1
    assert (
        client.put(
            path,
            json=snapshot,
            headers={"X-Curie-Directory-Credential": credential.json()["credential"]},
        ).status_code
        == 200
    )
    assert asyncio.run(resolve()) not in {None, first_person}


def binding_state(tenant_id: str) -> list[dict[str, object]]:
    from _migration_support import sql_dicts

    return sql_dicts(
        "SELECT jsonb_build_object("
        "'configuration', (SELECT jsonb_agg(to_jsonb(r)) FROM curie.tenant_identity_providers r "
        "WHERE tenant_id=:tenant), "
        "'teams', (SELECT jsonb_agg(to_jsonb(r) ORDER BY id) FROM curie.teams r "
        "WHERE tenant_id=:tenant), "
        "'namespaces', (SELECT jsonb_agg(to_jsonb(r) ORDER BY id) FROM curie.identity_namespaces r "
        "WHERE tenant_id=:tenant), "
        "'people', (SELECT jsonb_agg(to_jsonb(r) ORDER BY id) FROM curie.principals r "
        "WHERE tenant_id=:tenant), "
        "'memberships', (SELECT jsonb_agg(to_jsonb(r) ORDER BY principal_id, team_id) "
        "FROM curie.principal_teams r WHERE tenant_id=:tenant), "
        "'links', (SELECT jsonb_agg(to_jsonb(r) ORDER BY id) FROM curie.identity_links r "
        "WHERE tenant_id=:tenant), "
        "'audits', (SELECT jsonb_agg(to_jsonb(r) ORDER BY id) FROM curie.identity_audit_events r "
        "WHERE tenant_id=:tenant)) AS state",
        {"tenant": uuid.UUID(tenant_id)},
    )


def test_binding_returns_only_authenticated_scope_without_mutation(
    directory_client: TestClient, caplog: pytest.LogCaptureFixture,
) -> None:
    # Mutation controls: dropping scope filtering, omitting current authority
    # checks, or echoing credential data must fail this real HTTP boundary.
    caplog.set_level("DEBUG")
    client = directory_client
    tenant_id, team_id, _, admin = setup_directory(client)
    other_tenant, other_team, other_token, _ = setup_directory(client)
    namespace_id, _ = namespace_credential(
        client, tenant_id, team_id, admin, "example.com", "email",
    )
    outside_id, _ = namespace_credential(client, tenant_id, team_id, admin, "TEXAMPLE2")
    issued = client.post(
        "/directory/credentials",
        headers=admin,
        json={"tenant_id": tenant_id, "expected_config_version": 1,
              "team_ids": [team_id], "namespace_ids": [namespace_id]},
    )
    assert issued.status_code == 201
    token, expires_at = issued.json()["credential"], issued.json()["expires_at"]
    headers = {"X-Curie-Directory-Credential": token}
    assert client.put(
        f"/directory/teams/{team_id}/snapshot", headers=headers,
        json={"expected_epoch": 0, "expected_identity_epoch": 0, "complete": True,
              "people": [{"subject": "example-person", "email": "person@example.com"}]},
    ).status_code == 200
    before = binding_state(tenant_id), binding_state(other_tenant)
    response = client.get("/directory/binding", headers=headers)
    assert response.status_code == 200
    assert response.headers["cache-control"] == "private, no-store"
    assert response.json() == {
        "tenant_id": tenant_id, "issuer": "https://idp.example.com",
        "managed_by": "example-adapter", "team_ids": [team_id],
        "namespace_ids": [namespace_id], "config_version": 1, "identity_epoch": 1,
        "expires_at": expires_at,
    }
    assert (binding_state(tenant_id), binding_state(other_tenant)) == before
    for private in (token, other_token, other_tenant, other_team, outside_id):
        assert private not in response.text
    assert token not in caplog.text and other_token not in caplog.text
    # Another independently valid credential describes its own binding only.
    other = client.get("/directory/binding", headers={"X-Curie-Directory-Credential": other_token})
    assert other.status_code == 200
    assert other.json()["tenant_id"] == other_tenant
    assert other.json()["team_ids"] == [other_team]
    assert other.json()["namespace_ids"] == []


@pytest.mark.parametrize("change", [
    "expired", "stale_version", "issuer", "manager", "foreign_team", "foreign_namespace",
    "disabled_namespace", "namespace_authority", "team_manager",
])
def test_binding_refuses_invalid_current_authority_without_mutation(
    directory_client: TestClient, change: str, caplog: pytest.LogCaptureFixture,
) -> None:
    import time
    from dataclasses import replace

    from _migration_support import sql_rows
    from curie_api import directory_credential

    caplog.set_level("DEBUG")
    client = directory_client
    tenant_id, team_id, _, admin = setup_directory(client)
    namespace_id, token = namespace_credential(
        client, tenant_id, team_id, admin, "example.com", "email",
    )
    other_tenant, other_team, _, other_admin = setup_directory(client)
    other_namespace, _ = namespace_credential(
        client, other_tenant, other_team, other_admin, "TEXAMPLE2",
    )
    claims = directory_credential.verify(token, get_settings().api_key)
    assert claims is not None
    scope = claims.scope
    version, expiry, namespaces = claims.config_version, claims.exp, claims.namespace_ids
    if change == "expired":
        expiry = int(time.time()) - 1
    elif change == "stale_version":
        version += 1
    elif change == "issuer":
        scope = replace(scope, issuer="https://other-idp.example.com")
    elif change == "manager":
        scope = replace(scope, managed_by="another-adapter")
    elif change == "foreign_team":
        scope = replace(scope, team_ids=frozenset({uuid.UUID(other_team)}))
    elif change == "foreign_namespace":
        namespaces = frozenset({uuid.UUID(other_namespace)})
    elif change == "disabled_namespace":
        sql_rows("UPDATE curie.identity_namespaces SET status='disabled' WHERE id=:id",
                 {"id": uuid.UUID(namespace_id)})
    elif change == "namespace_authority":
        sql_rows("UPDATE curie.identity_namespaces SET authority='https://other-idp.example.com' "
                 "WHERE id=:id", {"id": uuid.UUID(namespace_id)})
    elif change == "team_manager":
        sql_rows("UPDATE curie.teams SET managed_by='another-adapter' WHERE id=:id",
                 {"id": uuid.UUID(team_id)})
    # Signing invalid scopes is an adversarial server-side fixture, not a mock
    # of verification or persistence. The real route must reject signed but
    # currently unauthorized claims, including every namespace/team in scope.
    token = directory_credential.mint(
        get_settings().api_key, scope=scope, namespace_ids=namespaces,
        config_version=version, exp=expiry,
    )
    before = binding_state(tenant_id), binding_state(other_tenant)
    response = client.get("/directory/binding", headers={"X-Curie-Directory-Credential": token})
    assert response.status_code == 401
    assert response.json() == {"detail": "invalid_directory_credential"}
    assert "no-store" in response.headers["cache-control"]
    assert (binding_state(tenant_id), binding_state(other_tenant)) == before
    assert token not in response.text and token not in caplog.text


@pytest.mark.parametrize("field,value", [
    ("issuer", "https://other-idp.example.com"),
    ("managed_by", "another-adapter"),
    ("governed_domains", ["other.example.com"]),
])
def test_binding_refuses_configuration_rotation(
    directory_client: TestClient, field: str, value: object,
) -> None:
    client = directory_client
    tenant_id, _, token, admin = setup_directory(client)
    config: dict[str, object] = {
        "issuer": "https://idp.example.com", "managed_by": "example-adapter",
        "governed_domains": ["example.com"], "expected_version": 1,
    }
    config[field] = value
    changed = client.put(f"/directory/config/{tenant_id}", headers=admin, json=config)
    assert changed.status_code == 200
    before = binding_state(tenant_id)
    response = client.get("/directory/binding", headers={"X-Curie-Directory-Credential": token})
    assert response.status_code == 401
    assert binding_state(tenant_id) == before


@pytest.mark.parametrize("header,value", [
    ("X-API-Key", "platform"),
    ("X-Curie-Adapter-Principal", "adp.example"),
    ("X-Curie-Approval-Principal", "operator.example"),
    ("X-Curie-Worker-Token", "worker.example"),
    ("X-Curie-Channel-Read", "chr.example"),
    ("Authorization", "Bearer example"),
    ("Cookie", "__Host-curie_console_session=example"),
])
@pytest.mark.parametrize("mixed", [False, True])
def test_binding_accepts_no_alternate_or_mixed_credentials(
    directory_client: TestClient, header: str, value: str, mixed: bool,
) -> None:
    client = directory_client
    tenant_id, _, token, _ = setup_directory(client)
    headers = {header: get_settings().api_key if value == "platform" else value}
    if mixed:
        headers["X-Curie-Directory-Credential"] = token
    before = binding_state(tenant_id)
    response = client.get("/directory/binding", headers=headers)
    assert response.status_code == 401
    assert response.json() == {"detail": "invalid_directory_credential"}
    assert "no-store" in response.headers["cache-control"]
    assert binding_state(tenant_id) == before


@pytest.mark.parametrize("change", ["rotation", "expiry"])
def test_binding_waits_for_identity_writer_before_reading_authority(
    directory_client: TestClient, worker_id: str, change: str,
) -> None:
    import asyncio
    import time
    from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

    import redis.asyncio as redis
    from curie_api import directory_credential, identity_fence
    from sqlalchemy import text
    from sqlalchemy.ext.asyncio import async_sessionmaker

    client = directory_client
    tenant_id, _, token, admin = setup_directory(client)

    async def check() -> None:
        engine = create_async_engine(get_settings().database_url, poolclass=NullPool)
        database = 15 if worker_id == "master" else int(worker_id.removeprefix("gw")) + 1
        assert 1 <= database <= 15
        endpoint = urlsplit(get_settings().valkey_dsn())
        query = urlencode([(k, v) for k, v in parse_qsl(endpoint.query) if k != "db"])
        valkey = redis.from_url(urlunsplit(endpoint._replace(path=f"/{database}", query=query)))
        factory = async_sessionmaker(engine)
        pending = None
        try:
            assert await valkey.ping()
            async with identity_fence.mutation(factory, valkey):
                selected = token
                if change == "expiry":
                    claims = directory_credential.verify(token, get_settings().api_key)
                    assert claims is not None
                    expiry = int(time.time()) + 3
                    selected = directory_credential.mint(
                        get_settings().api_key, scope=claims.scope,
                        namespace_ids=claims.namespace_ids, config_version=claims.config_version,
                        exp=expiry,
                    )
                pending = asyncio.create_task(asyncio.to_thread(
                    client.get, "/directory/binding",
                    headers={"X-Curie-Directory-Credential": selected},
                ))
                async with asyncio.timeout(5):
                    async with engine.connect() as connection:
                        while not await connection.scalar(text(
                            "SELECT count(*) FROM pg_locks WHERE locktype='advisory' "
                            "AND objid=2250220 AND mode='ShareLock' AND NOT granted "
                            "AND database=(SELECT oid FROM pg_database "
                            "WHERE datname=current_database())"
                        )):
                            await asyncio.sleep(0.01)
                assert not pending.done()
                # If the GET loaded config before its advisory lock, its row
                # lock would prevent this independent real HTTP config write.
                if change == "rotation":
                    changed = await asyncio.wait_for(asyncio.to_thread(
                        client.put, f"/directory/config/{tenant_id}", headers=admin,
                        json={"issuer": "https://other-idp.example.com",
                              "managed_by": "example-adapter", "governed_domains": ["example.com"],
                              "expected_version": 1},
                    ), timeout=5)
                    assert changed.status_code == 200
                else:
                    async with asyncio.timeout(5):
                        while int(time.time()) < expiry:
                            await asyncio.sleep(0.01)
            response = await asyncio.wait_for(pending, timeout=5)
            assert response.status_code == 401
        finally:
            if pending is not None:
                await asyncio.wait_for(pending, timeout=5)
            await valkey.delete(identity_fence.FENCE_KEY)
            await valkey.aclose()
            await engine.dispose()

    asyncio.run(check())


@pytest.mark.parametrize("credential", [None, "dir.bad.bad", "chn.example", "stt.example"])
def test_binding_requires_a_valid_directory_credential(
    directory_client: TestClient, credential: str | None,
) -> None:
    headers = {} if credential is None else {"X-Curie-Directory-Credential": credential}
    response = directory_client.get("/directory/binding", headers=headers)
    assert response.status_code == 401
    assert response.json() == {"detail": "invalid_directory_credential"}
    assert "no-store" in response.headers["cache-control"]


def test_binding_rejects_colliding_directory_headers(directory_client: TestClient) -> None:
    client = directory_client
    tenant_id, _, token, _ = setup_directory(client)
    _, _, other_token, _ = setup_directory(client)
    before = binding_state(tenant_id)
    response = client.get("/directory/binding", headers=[
        ("X-Curie-Directory-Credential", token),
        ("X-Curie-Directory-Credential", other_token),
    ])
    assert response.status_code == 401
    assert response.json() == {"detail": "invalid_directory_credential"}
    assert binding_state(tenant_id) == before


def test_delayed_other_team_snapshot_cannot_restore_a_reassigned_mailbox(
    directory_client: TestClient,
) -> None:
    import asyncio

    from _migration_support import sql_dicts
    from curie_api.team_authorization import resolve_email
    from sqlalchemy.ext.asyncio import AsyncSession

    client = directory_client
    tenant_id, team_a, _, admin = setup_directory(client)
    created = client.post(
        "/directory/teams",
        headers=admin,
        json={"tenant_id": tenant_id, "external_id": "other-group", "name": "Other team"},
    )
    assert created.status_code == 201, created.text
    team_b = created.json()["id"]
    namespace_id, _ = namespace_credential(
        client, tenant_id, team_a, admin, "example.com", "email"
    )
    issued = client.post(
        "/directory/credentials",
        headers=admin,
        json={
            "tenant_id": tenant_id,
            "expected_config_version": 1,
            "team_ids": [team_a, team_b],
            "namespace_ids": [namespace_id],
        },
    )
    assert issued.status_code == 201, issued.text
    headers = {"X-Curie-Directory-Credential": issued.json()["credential"]}

    def observe_binding() -> dict[str, object]:
        response = client.get("/directory/binding", headers=headers)
        assert response.status_code == 200, response.text
        return response.json()

    def observed_snapshot(
        binding: dict[str, object], team_epoch: int, subject: str
    ) -> dict[str, object]:
        payload: dict[str, object] = {
            "expected_epoch": team_epoch,
            "complete": True,
            "people": [{"subject": subject, "email": "ops@example.com", "accounts": []}],
        }
        # Use only this observation's public token. Omitting the not-yet-shipped
        # field lets the original implementation demonstrate its actual200
        # overwrite, rather than failing early for an unknown request field.
        if "identity_epoch" in binding:
            payload["expected_identity_epoch"] = binding["identity_epoch"]
        return payload

    def resolved_mailbox() -> uuid.UUID | None:
        async def resolve() -> uuid.UUID | None:
            engine = create_async_engine(get_settings().database_url)
            try:
                async with AsyncSession(engine) as session:
                    return await resolve_email(
                        session, tenant_id=uuid.UUID(tenant_id), email="ops@example.com"
                    )
            finally:
                await engine.dispose()

        return asyncio.run(resolve())

    def team_b_state() -> list[dict[str, object]]:
        return sql_dicts(
            "SELECT jsonb_build_object("
            "'team', (SELECT jsonb_build_object('epoch',epoch,'member_count',member_count,"
            "'digest',digest) FROM curie.teams WHERE id=:team), "
            "'memberships', (SELECT jsonb_agg(to_jsonb(r) ORDER BY principal_id) "
            "FROM curie.principal_teams r WHERE team_id=:team), "
            "'audit', (SELECT jsonb_agg(to_jsonb(r) ORDER BY id) "
            "FROM curie.identity_audit_events r WHERE team_id=:team)) AS state",
            {"team": uuid.UUID(team_b)},
        )

    path_a = f"/directory/teams/{team_a}/snapshot"
    path_b = f"/directory/teams/{team_b}/snapshot"
    initial = observed_snapshot(observe_binding(), 0, "alice")
    response = client.put(path_a, headers=headers, json=initial)
    assert response.status_code == 200, response.text
    alice = resolved_mailbox()
    assert alice is not None

    # B starts observing Alice before A learns the mailbox has changed hands.
    # B's own team epoch is still valid: no B snapshot has ever been applied.
    old_b_binding = observe_binding()
    response = client.get(f"/directory/teams/{team_b}", headers=headers)
    assert response.status_code == 200 and response.json()["epoch"] == 0
    old_b_payload = observed_snapshot(old_b_binding, response.json()["epoch"], "alice")

    newer_a_payload = observed_snapshot(observe_binding(), 1, "bob")
    response = client.put(path_a, headers=headers, json=newer_a_payload)
    assert response.status_code == 200, response.text
    bob = resolved_mailbox()
    assert bob is not None and bob != alice
    before_b = team_b_state()
    binding_after_reassignment = observe_binding()

    # Deliver exactly the captured old B payload. Never refresh or relabel it
    # with a binding fetched after A's newer directory observation committed.
    delayed = client.put(path_b, headers=headers, json=old_b_payload)
    resolved_after_delayed = resolved_mailbox()
    assert delayed.status_code == 409, {
        "status": delayed.status_code,
        "body": delayed.text,
        "resolved_after_delayed": str(resolved_after_delayed),
        "alice": str(alice),
        "bob": str(bob),
    }
    assert delayed.json() == {"detail": "stale_identity_epoch"}
    assert resolved_after_delayed == bob
    assert sql_dicts(
        "SELECT p.idp_subject FROM curie.identity_links l JOIN curie.principals p "
        "ON p.id=l.principal_id WHERE l.identity_namespace_id=:namespace "
        "AND l.provider_native_id='ops@example.com' AND l.revoked_at IS NULL",
        {"namespace": uuid.UUID(namespace_id)},
    ) == [{"idp_subject": "bob"}]
    assert team_b_state() == before_b
    binding_after_refusal = observe_binding()
    assert (
        binding_after_refusal["identity_epoch"]
        == binding_after_reassignment["identity_epoch"]
    )

    # A fresh binding precedes a new B observation of Bob; it does not bless
    # the old Alice payload. This new observation can advance B from epoch0.
    fresh_b_binding = observe_binding()
    old_identity_epoch = old_b_binding["identity_epoch"]
    fresh_identity_epoch = fresh_b_binding["identity_epoch"]
    assert isinstance(old_identity_epoch, int) and isinstance(fresh_identity_epoch, int)
    assert fresh_identity_epoch > old_identity_epoch
    fresh_b_payload = observed_snapshot(fresh_b_binding, 0, "bob")
    retried = client.put(path_b, headers=headers, json=fresh_b_payload)
    assert retried.status_code == 200, retried.text
    assert retried.json()["epoch"] == 1 and retried.json()["member_count"] == 1
    assert resolved_mailbox() == bob


def test_no_namespace_directory_snapshot_keeps_team_epoch_compatibility(
    directory_client: TestClient,
) -> None:
    from _migration_support import sql_dicts

    client = directory_client
    tenant_id, team_id, token, _ = setup_directory(client)
    headers = {"X-Curie-Directory-Credential": token}
    binding = client.get("/directory/binding", headers=headers)
    assert binding.status_code == 200 and binding.json()["namespace_ids"] == []
    payload = {
        "expected_epoch": 0,
        "complete": True,
        "people": [{"subject": "membership-only", "accounts": []}],
    }
    path = f"/directory/teams/{team_id}/snapshot"
    applied = client.put(path, headers=headers, json=payload)
    assert applied.status_code == 200, applied.text
    assert applied.json()["epoch"] == 1 and applied.json()["member_count"] == 1
    assert client.put(path, headers=headers, json=payload).status_code == 409
    assert sql_dicts(
        "SELECT id FROM curie.identity_links WHERE tenant_id=:tenant",
        {"tenant": uuid.UUID(tenant_id)},
    ) == []


@pytest.mark.parametrize("precondition", [None, -1, True, "0", 0.5])
def test_namespace_snapshot_requires_a_strict_shared_identity_epoch(
    directory_client: TestClient, precondition: object,
) -> None:
    client = directory_client
    tenant_id, team_id, _, admin = setup_directory(client)
    _, token = namespace_credential(client, tenant_id, team_id, admin, "example.com", "email")
    headers = {"X-Curie-Directory-Credential": token}
    # Namespace authority is present even when no people/accounts are reported.
    # It must not bypass the shared identity precondition on an empty snapshot.
    payload: dict[str, object] = {"expected_epoch": 0, "complete": True, "people": []}
    if precondition is not None:
        payload["expected_identity_epoch"] = precondition
    before = binding_state(tenant_id)
    response = client.put(f"/directory/teams/{team_id}/snapshot", headers=headers, json=payload)
    assert response.status_code == 422, response.text
    if precondition is None:
        assert response.json() == {"detail": "identity_epoch_required"}
    after = binding_state(tenant_id)
    # Invalid-attempt health/audit writes are intentional; projection authority
    # and its epochs must remain unchanged even for an empty namespace payload.
    for state in (before[0]["state"], after[0]["state"]):
        state.pop("audits")
        for team in state["teams"]:
            team.pop("last_attempt_at")
            team.pop("last_error")
    assert after == before


def test_concurrent_team_snapshots_share_one_identity_epoch(
    directory_client: TestClient,
) -> None:
    import asyncio

    import httpx
    from _migration_support import sql_dicts
    from curie_api.team_authorization import resolve_email
    from sqlalchemy.ext.asyncio import AsyncSession

    client = directory_client
    tenant_id, team_a, _, admin = setup_directory(client)
    created = client.post(
        "/directory/teams",
        headers=admin,
        json={"tenant_id": tenant_id, "external_id": "concurrent-group", "name": "Other team"},
    )
    assert created.status_code == 201, created.text
    team_b = created.json()["id"]
    namespace_id, _ = namespace_credential(
        client, tenant_id, team_a, admin, "example.com", "email"
    )
    issued = client.post(
        "/directory/credentials",
        headers=admin,
        json={"tenant_id": tenant_id, "expected_config_version": 1,
              "team_ids": [team_a, team_b], "namespace_ids": [namespace_id]},
    )
    assert issued.status_code == 201, issued.text
    headers = {"X-Curie-Directory-Credential": issued.json()["credential"]}
    bindings = []
    for _ in range(2):
        response = client.get("/directory/binding", headers=headers)
        assert response.status_code == 200, response.text
        bindings.append(response.json())
    initial_epoch = bindings[0]["identity_epoch"]
    assert type(initial_epoch) is int and bindings[1]["identity_epoch"] == initial_epoch
    tenant = {"tenant": uuid.UUID(tenant_id)}
    before_teams = {
        row["id"]: row["state"] for row in sql_dicts(
            "SELECT id::text AS id,to_jsonb(t) AS state FROM curie.teams t "
            "WHERE tenant_id=:tenant", tenant,
        )
    }

    def team_audit(team_id: str) -> list[dict[str, object]]:
        return sql_dicts(
            "SELECT to_jsonb(e) AS state FROM curie.identity_audit_events e "
            "WHERE team_id=:team ORDER BY id", {"team": uuid.UUID(team_id)},
        )

    before_audit = {team: team_audit(team) for team in (team_a, team_b)}
    # Each binding is captured before its independent synthetic directory
    # observation. Both team epochs are valid0; account ownership is shared.
    observations = [(team_a, "alice"), (team_b, "bob")]
    payloads = [
        {"expected_epoch": 0, "expected_identity_epoch": binding["identity_epoch"],
         "complete": True,
         "people": [{"subject": subject, "email": "ops@example.com", "accounts": []}]}
        for binding, (_, subject) in zip(bindings, observations, strict=True)
    ]

    async def submit_both() -> list[httpx.Response]:
        # Cancellable ASGI HTTP requests exercise actual FastAPI dependencies
        # and distinct real Postgres transactions. No blocking thread pool can
        # conceal a lock-upgrade/deadlock by hanging during executor shutdown.
        barrier = asyncio.Barrier(2)
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=client.app), base_url="http://testserver"
        ) as concurrent:
            async def submit(index: int) -> httpx.Response:
                await barrier.wait()
                team, _ = observations[index]
                return await concurrent.put(
                    f"/directory/teams/{team}/snapshot", headers=headers, json=payloads[index]
                )

            return await asyncio.wait_for(
                asyncio.gather(submit(0), submit(1)), timeout=10
            )

    responses = asyncio.run(submit_both())
    assert sorted(response.status_code for response in responses) == [200, 409], [
        {"status": response.status_code, "body": response.text} for response in responses
    ]
    winner_index = next(i for i, response in enumerate(responses) if response.status_code == 200)
    loser_index = 1 - winner_index
    winner_team, winner_subject = observations[winner_index]
    loser_team, loser_subject = observations[loser_index]
    assert responses[loser_index].json() == {"detail": "stale_identity_epoch"}
    binding = client.get("/directory/binding", headers=headers)
    assert binding.status_code == 200
    assert binding.json()["identity_epoch"] == initial_epoch + 1
    after_teams = {
        row["id"]: row["state"] for row in sql_dicts(
            "SELECT id::text AS id,to_jsonb(t) AS state FROM curie.teams t "
            "WHERE tenant_id=:tenant", tenant,
        )
    }
    assert after_teams[loser_team] == before_teams[loser_team]
    assert team_audit(loser_team) == before_audit[loser_team]
    assert after_teams[winner_team]["epoch"] == 1
    assert after_teams[winner_team]["member_count"] == 1
    assert sql_dicts(
        "SELECT pt.team_id::text AS team,p.idp_subject AS subject "
        "FROM curie.principal_teams pt JOIN curie.principals p ON p.id=pt.principal_id "
        "WHERE pt.tenant_id=:tenant", tenant,
    ) == [{"team": winner_team, "subject": winner_subject}]
    active_links = sql_dicts(
        "SELECT p.id AS principal,p.idp_subject AS subject FROM curie.identity_links l "
        "JOIN curie.principals p ON p.id=l.principal_id WHERE l.identity_namespace_id=:namespace "
        "AND l.provider_native_id='ops@example.com' AND l.revoked_at IS NULL",
        {"namespace": uuid.UUID(namespace_id)},
    )
    assert len(active_links) == 1 and active_links[0]["subject"] == winner_subject
    assert sql_dicts(
        "SELECT idp_subject FROM curie.principals WHERE tenant_id=:tenant "
        "AND idp_subject=:subject", {**tenant, "subject": loser_subject},
    ) == []

    async def resolve_winner() -> uuid.UUID | None:
        engine = create_async_engine(get_settings().database_url)
        try:
            async with AsyncSession(engine) as session:
                return await resolve_email(
                    session, tenant_id=uuid.UUID(tenant_id), email="ops@example.com"
                )
        finally:
            await engine.dispose()

    assert asyncio.run(resolve_winner()) == active_links[0]["principal"]
    applied = sql_dicts(
        "SELECT team_id::text AS team FROM curie.identity_audit_events "
        "WHERE tenant_id=:tenant AND event_type='identity.directory_snapshot_applied'", tenant,
    )
    assert applied == [{"team": winner_team}]
