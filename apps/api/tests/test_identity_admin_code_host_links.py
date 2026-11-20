"""Operator code host links on the platform-key identity admin routes (#5414).

POST /identity/admin/{tenant_id}/code-host-links records an admin_mapped link in
the host's github_host namespace. POST /identity/admin/{tenant_id}/links/{link_id}/revoke
is idempotent and keeps the first revoked_at.
"""

from __future__ import annotations

import uuid
from typing import Any

import pytest
from _migration_support import sql_dicts, sql_rows
from fastapi.testclient import TestClient

HOST = "github.example.com"
ACCOUNT = "42"


def _exec(statement: str, params: dict[str, Any]) -> None:
    sql_rows(statement, params)


def _seed(*, principal_status: str = "active") -> dict[str, uuid.UUID]:
    tenant_id = uuid.uuid4()
    principal_id = uuid.uuid4()
    creator_id = uuid.uuid4()
    _exec(
        "INSERT INTO curie.tenants (id, deployment_id, status) VALUES (:id, :deployment, 'active')",
        {"id": tenant_id, "deployment": f"code-host-{tenant_id.hex[:8]}"},
    )
    for principal, status, subject in (
        (principal_id, principal_status, "principal"),
        (creator_id, "active", "creator"),
    ):
        _exec(
            "INSERT INTO curie.principals (id, tenant_id, idp_subject, type, status) "
            "VALUES (:id, :tenant_id, :subject, 'human', :status)",
            {
                "id": principal,
                "tenant_id": tenant_id,
                "subject": f"code-host-{subject}-{principal.hex[:8]}",
                "status": status,
            },
        )
    return {"tenant": tenant_id, "principal": principal_id, "creator": creator_id}


def _body(ids: dict[str, uuid.UUID], **overrides: Any) -> dict[str, Any]:
    body: dict[str, Any] = {
        "principal_id": str(ids["principal"]),
        "code_host_kind": "github",
        "host": HOST,
        "account_id": ACCOUNT,
        "created_by_principal_id": str(ids["creator"]),
    }
    body.update(overrides)
    return body


def _create(
    client: TestClient,
    ids: dict[str, uuid.UUID],
    headers: dict[str, str],
    **overrides: Any,
) -> Any:
    return client.post(
        f"/identity/admin/{ids['tenant']}/code-host-links",
        json=_body(ids, **overrides),
        headers=headers,
    )


def _revoke(
    client: TestClient, tenant_id: uuid.UUID, link_id: uuid.UUID, headers: dict[str, str]
) -> Any:
    return client.post(
        f"/identity/admin/{tenant_id}/links/{link_id}/revoke",
        headers=headers,
    )


def _audits(tenant_id: uuid.UUID, event_type: str) -> list[dict[str, Any]]:
    return sql_dicts(
        "SELECT event_type, actor, principal_id::text AS principal_id, details "
        "FROM curie.identity_audit_events "
        "WHERE tenant_id = :tenant_id AND event_type = :event_type "
        "ORDER BY created_at",
        {"tenant_id": tenant_id, "event_type": event_type},
    )


def test_create_returns_201_and_writes_an_audit_row(
    client: TestClient, auth_headers: dict[str, str]
) -> None:
    ids = _seed()
    response = _create(client, ids, auth_headers)
    assert response.status_code == 201, response.text
    payload = response.json()
    link_id = uuid.UUID(payload["link_id"])
    namespace_id = uuid.UUID(payload["identity_namespace_id"])
    assert sql_dicts(
        "SELECT provider, authority, kind, key FROM curie.identity_namespaces WHERE id = :id",
        {"id": namespace_id},
    ) == [{"provider": "github", "authority": HOST, "kind": "github_host", "key": HOST}]
    assert sql_dicts(
        "SELECT provider_native_id, verification_source, principal_id, revoked_at "
        "FROM curie.identity_links WHERE id = :id",
        {"id": link_id},
    ) == [
        {
            "provider_native_id": ACCOUNT,
            "verification_source": "admin_mapped",
            "principal_id": ids["principal"],
            "revoked_at": None,
        }
    ]
    audits = _audits(ids["tenant"], "identity.code_host_link_created")
    assert len(audits) == 1
    assert audits[0]["actor"] == "platform"
    assert audits[0]["principal_id"] == str(ids["principal"])
    assert audits[0]["details"]["link_id"] == str(link_id)
    assert audits[0]["details"]["namespace_id"] == str(namespace_id)


def test_active_duplicate_is_409_and_a_revoked_account_can_be_linked_again(
    client: TestClient, auth_headers: dict[str, str]
) -> None:
    ids = _seed()
    created = _create(client, ids, auth_headers)
    assert created.status_code == 201, created.text
    other = uuid.uuid4()
    _exec(
        "INSERT INTO curie.principals (id, tenant_id, idp_subject, type, status) "
        "VALUES (:id, :tenant_id, :subject, 'human', 'active')",
        {
            "id": other,
            "tenant_id": ids["tenant"],
            "subject": f"code-host-other-{other.hex[:8]}",
        },
    )
    duplicate = _create(client, ids, auth_headers, principal_id=str(other))
    assert duplicate.status_code == 409, duplicate.text
    assert duplicate.json().get("detail") == {"code": "link_exists"}
    revoked = _revoke(client, ids["tenant"], uuid.UUID(created.json()["link_id"]), auth_headers)
    assert revoked.status_code == 200, revoked.text
    again = _create(client, ids, auth_headers, principal_id=str(other))
    assert again.status_code == 201, again.text
    assert again.json()["link_id"] != created.json()["link_id"]


def test_unknown_and_inactive_principals_are_404_beside_a_valid_link(
    client: TestClient, auth_headers: dict[str, str]
) -> None:
    ids = _seed()
    valid = _create(client, ids, auth_headers)
    assert valid.status_code == 201, valid.text
    unknown = _create(client, ids, auth_headers, principal_id=str(uuid.uuid4()))
    assert unknown.status_code == 404, unknown.text
    assert unknown.json().get("detail") == {"code": "principal_not_found"}
    disabled = _seed(principal_status="disabled")
    inactive = _create(client, disabled, auth_headers)
    assert inactive.status_code == 404, inactive.text
    assert inactive.json().get("detail") == {"code": "principal_not_found"}
    assert _audits(disabled["tenant"], "identity.code_host_link_created") == []


@pytest.mark.parametrize(
    "override",
    [
        {"account_id": "U0EXAMPLE1"},
        {"account_id": "0"},
        {"host": "GitHub.example.com"},
    ],
    ids=["login", "zero", "uppercase"],
)
def test_malformed_code_host_link_is_422_beside_a_valid_one(
    client: TestClient,
    auth_headers: dict[str, str],
    override: dict[str, str],
) -> None:
    ids = _seed()
    valid = _create(client, ids, auth_headers)
    assert valid.status_code == 201, valid.text
    refused = _create(client, ids, auth_headers, **override)
    assert refused.status_code == 422, refused.text
    assert sql_dicts(
        "SELECT id FROM curie.identity_links WHERE tenant_id = :tenant_id",
        {"tenant_id": ids["tenant"]},
    ) == [{"id": uuid.UUID(valid.json()["link_id"])}]


def test_revoke_missing_link_is_404_and_a_real_revoke_keeps_the_first_time(
    client: TestClient, auth_headers: dict[str, str]
) -> None:
    ids = _seed()
    missing = _revoke(client, ids["tenant"], uuid.uuid4(), auth_headers)
    assert missing.status_code == 404, missing.text
    assert missing.json().get("detail") == {"code": "link_not_found"}
    created = _create(client, ids, auth_headers)
    assert created.status_code == 201, created.text
    link_id = uuid.UUID(created.json()["link_id"])
    first = _revoke(client, ids["tenant"], link_id, auth_headers)
    assert first.status_code == 200, first.text
    assert uuid.UUID(first.json()["link_id"]) == link_id
    assert first.json()["revoked_at"]
    second = _revoke(client, ids["tenant"], link_id, auth_headers)
    assert second.status_code == 200, second.text
    assert second.json()["revoked_at"] == first.json()["revoked_at"]
    assert second.json()["link_id"] == first.json()["link_id"]
    stored = sql_dicts(
        "SELECT revoked_at FROM curie.identity_links WHERE id = :id",
        {"id": link_id},
    )
    assert stored[0]["revoked_at"] is not None
    audits = _audits(ids["tenant"], "identity.link_revoked")
    assert len(audits) == 1
    assert audits[0]["actor"] == "platform"
    assert audits[0]["details"]["link_id"] == str(link_id)


def test_create_without_the_platform_key_is_401_beside_a_valid_create(
    client: TestClient, auth_headers: dict[str, str]
) -> None:
    ids = _seed()
    denied = _create(client, ids, {})
    assert denied.status_code == 401, denied.text
    assert _audits(ids["tenant"], "identity.code_host_link_created") == []
    allowed = _create(client, ids, auth_headers)
    assert allowed.status_code == 201, allowed.text
    assert len(_audits(ids["tenant"], "identity.code_host_link_created")) == 1
