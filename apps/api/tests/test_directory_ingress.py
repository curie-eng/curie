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
        result = client.put(
            path,
            json={"expected_epoch": epoch, "complete": True, "people": people},
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
        "complete": True,
        "people": [{"subject": "person-a", "email": "one@example.com", "accounts": []}],
    }
    assert client.put(path, json=good, headers=headers).status_code == 200
    # The IdP reassigns a mailbox. Exactly one active link changes hands.
    good["expected_epoch"] = 1
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
    response = client.put(path, json=invalid, headers=headers)
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
