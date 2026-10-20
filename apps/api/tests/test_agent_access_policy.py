"""An agent's invocation and approver teams: table and routes (#4585, ADR 0220 s.8).

The policy is stored and administered here; nothing enforces it yet. Each
role names an IdP group team of the agent's own tenant, or nothing, which keeps
today's behavior for that role. A write names the version it expects, so two
operators cannot silently overwrite each other: version 0 means "no policy
yet", and every accepted write moves it up by one.
"""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import Iterator
from typing import Any

import pytest
from curie_api.config import get_settings
from curie_api.models import AgentAccessPolicy
from fastapi.testclient import TestClient
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import create_async_engine

DEFAULT_TENANT = uuid.UUID("00000000-0000-0000-0000-000000000001")
MARK = "access-policy-test-"


def _sql(statement: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    async def run() -> list[dict[str, Any]]:
        engine = create_async_engine(get_settings().database_url)
        try:
            async with engine.begin() as conn:
                result = await conn.execute(text(statement), params or {})
                return [dict(row) for row in result.mappings().all()] if result.returns_rows else []
        finally:
            await engine.dispose()

    return asyncio.run(run())


def _cleanup() -> None:
    _sql(
        "DELETE FROM curie.agent_access_policies WHERE agent_id IN "
        "(SELECT id FROM curie.agents WHERE name LIKE :mark)",
        {"mark": f"{MARK}%"},
    )
    _sql("DELETE FROM curie.agents WHERE name LIKE :mark", {"mark": f"{MARK}%"})
    _sql("DELETE FROM curie.teams WHERE name LIKE :mark", {"mark": f"{MARK}%"})
    _sql("DELETE FROM curie.tenants WHERE deployment_id LIKE :mark", {"mark": f"{MARK}%"})


@pytest.fixture
def clean(migrated: None) -> Iterator[None]:
    _cleanup()
    yield
    _cleanup()


def _tenant() -> uuid.UUID:
    tenant_id = uuid.uuid4()
    _sql(
        "INSERT INTO curie.tenants (id, deployment_id, status) VALUES (:id, :d, 'active')",
        {"id": tenant_id, "d": f"{MARK}{tenant_id}"},
    )
    return tenant_id


def _agent(tenant_id: uuid.UUID = DEFAULT_TENANT) -> uuid.UUID:
    agent_id = uuid.uuid4()
    _sql(
        "INSERT INTO curie.agents (id, name, tenant_id) VALUES (:id, :name, :tenant)",
        {"id": agent_id, "name": f"{MARK}{agent_id.hex[:8]}", "tenant": tenant_id},
    )
    return agent_id


def _team(tenant_id: uuid.UUID = DEFAULT_TENANT, *, source: str = "idp_group") -> uuid.UUID:
    team_id = uuid.uuid4()
    _sql(
        "INSERT INTO curie.teams (id, tenant_id, source, external_id, name) "
        "VALUES (:id, :tenant, :source, :external, :name)",
        {
            "id": team_id,
            "tenant": tenant_id,
            "source": source,
            "external": f"00g{team_id.hex[:10]}" if source == "idp_group" else None,
            "name": f"{MARK}{team_id.hex[:8]}",
        },
    )
    return team_id


def _policy_rows(agent_id: uuid.UUID) -> list[dict[str, Any]]:
    return _sql(
        "SELECT invocation_team_id, approver_team_id, version "
        "FROM curie.agent_access_policies WHERE agent_id = :id",
        {"id": agent_id},
    )


# --- model and database ----------------------------------------------------


def test_model_shape() -> None:
    columns = AgentAccessPolicy.__table__.columns
    assert columns["agent_id"].primary_key
    for name in ("tenant_id", "version", "updated_at"):
        assert columns[name].nullable is False, name
    for name in ("invocation_team_id", "approver_team_id"):
        assert columns[name].nullable is True, name


def test_cross_tenant_team_is_refused_by_the_database(clean: None) -> None:
    other = _tenant()
    agent_id = _agent()
    foreign_team = _team(other)
    with pytest.raises(IntegrityError) as exc_info:
        _sql(
            "INSERT INTO curie.agent_access_policies "
            "(agent_id, tenant_id, invocation_team_id, version) "
            "VALUES (:agent, :tenant, :team, 1)",
            {"agent": agent_id, "tenant": DEFAULT_TENANT, "team": foreign_team},
        )
    cause = exc_info.value.orig.__cause__
    assert getattr(cause, "constraint_name", None) == "agent_access_policies_invocation_team_fkey"


def test_deleting_the_agent_removes_its_policy_and_a_team_in_use_stays(clean: None) -> None:
    agent_id = _agent()
    team_id = _team()
    _sql(
        "INSERT INTO curie.agent_access_policies "
        "(agent_id, tenant_id, approver_team_id, version) VALUES (:a, :t, :team, 1)",
        {"a": agent_id, "t": DEFAULT_TENANT, "team": team_id},
    )
    with pytest.raises(IntegrityError) as exc_info:
        _sql("DELETE FROM curie.teams WHERE id = :id", {"id": team_id})
    cause = exc_info.value.orig.__cause__
    assert getattr(cause, "constraint_name", None) == "agent_access_policies_approver_team_fkey"
    _sql("DELETE FROM curie.agents WHERE id = :id", {"id": agent_id})
    assert _policy_rows(agent_id) == []


# --- routes ----------------------------------------------------------------


def _path(agent_id: uuid.UUID | str) -> str:
    return f"/agents/{agent_id}/access-policy"


def _put(
    client: TestClient,
    headers: dict[str, str],
    agent_id: uuid.UUID,
    *,
    invocation: uuid.UUID | None,
    approver: uuid.UUID | None,
    expected_version: int,
) -> Any:
    return client.put(
        _path(agent_id),
        json={
            "invocation_team_id": str(invocation) if invocation else None,
            "approver_team_id": str(approver) if approver else None,
            "expected_version": expected_version,
        },
        headers=headers,
    )


def test_routes_require_a_key(client: TestClient, clean: None) -> None:
    agent_id = _agent()
    assert client.get(_path(agent_id)).status_code == 401
    body = {"invocation_team_id": None, "approver_team_id": None, "expected_version": 0}
    assert client.put(_path(agent_id), json=body).status_code == 401


def test_an_agent_without_a_policy_reads_as_version_zero(
    client: TestClient, auth_headers: dict[str, str], clean: None
) -> None:
    agent_id = _agent()
    response = client.get(_path(agent_id), headers=auth_headers)
    assert response.status_code == 200, response.text
    assert response.json() == {
        "agent_id": str(agent_id),
        "tenant_id": str(DEFAULT_TENANT),
        "invocation_team_id": None,
        "approver_team_id": None,
        "version": 0,
        "updated_at": None,
    }


def test_set_read_clear_and_versions(
    client: TestClient, auth_headers: dict[str, str], clean: None
) -> None:
    agent_id = _agent()
    requesters = _team()
    approvers = _team()

    first = _put(
        client, auth_headers, agent_id,
        invocation=requesters, approver=approvers, expected_version=0
    )
    assert first.status_code == 200, first.text
    assert first.json()["version"] == 1
    assert first.json()["invocation_team_id"] == str(requesters)
    assert first.json()["approver_team_id"] == str(approvers)
    assert first.json()["updated_at"] is not None
    assert client.get(_path(agent_id), headers=auth_headers).json() == first.json()

    # Clearing one role leaves the other; each role is independent.
    second = _put(
        client, auth_headers, agent_id, invocation=None, approver=approvers, expected_version=1
    )
    assert second.status_code == 200, second.text
    assert second.json()["version"] == 2
    assert second.json()["invocation_team_id"] is None
    assert second.json()["approver_team_id"] == str(approvers)


def test_a_stale_version_is_409_and_changes_nothing(
    client: TestClient, auth_headers: dict[str, str], clean: None
) -> None:
    agent_id = _agent()
    team_id = _team()
    assert (
        _put(client, auth_headers, agent_id, invocation=team_id, approver=None, expected_version=0)
    ).status_code == 200
    stale = _put(
        client, auth_headers, agent_id, invocation=None, approver=team_id, expected_version=0
    )
    assert stale.status_code == 409
    assert "version mismatch" in stale.json()["detail"]
    assert _policy_rows(agent_id) == [
        {"invocation_team_id": team_id, "approver_team_id": None, "version": 1}
    ]
    # Creating needs version 0; a positive guess on a missing policy is stale too.
    other_agent = _agent()
    ahead = _put(
        client, auth_headers, other_agent, invocation=team_id, approver=None, expected_version=3
    )
    assert ahead.status_code == 409
    assert _policy_rows(other_agent) == []


@pytest.mark.parametrize("case", ["other_tenant", "curie_managed", "unknown"])
def test_an_unusable_team_is_422_and_stores_nothing(
    client: TestClient, auth_headers: dict[str, str], clean: None, case: str
) -> None:
    agent_id = _agent()
    if case == "other_tenant":
        team_id = _team(_tenant())
    elif case == "curie_managed":
        team_id = _team(source="curie_managed")
    else:
        team_id = uuid.uuid4()
    response = _put(
        client, auth_headers, agent_id, invocation=None, approver=team_id, expected_version=0
    )
    assert response.status_code == 422, response.text
    assert _policy_rows(agent_id) == []


def test_unknown_agent_is_404(
    client: TestClient, auth_headers: dict[str, str], clean: None,
) -> None:
    missing = uuid.uuid4()
    assert client.get(_path(missing), headers=auth_headers).status_code == 404
    response = _put(
        client, auth_headers, missing, invocation=None, approver=None, expected_version=0
    )
    assert response.status_code == 404


@pytest.mark.parametrize(
    "body",
    [
        {"approver_team_id": None, "expected_version": 0},
        {"invocation_team_id": None, "expected_version": 0},
        {"invocation_team_id": None, "approver_team_id": None},
        {"invocation_team_id": None, "approver_team_id": None, "expected_version": -1},
        {"invocation_team_id": None, "approver_team_id": None, "expected_version": 0, "x": 1},
    ],
)
def test_a_malformed_body_is_422(
    client: TestClient, auth_headers: dict[str, str], clean: None, body: dict[str, Any]
) -> None:
    agent_id = _agent()
    response = client.put(_path(agent_id), json=body, headers=auth_headers)
    assert response.status_code == 422, response.text
    assert _policy_rows(agent_id) == []
