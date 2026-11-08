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
