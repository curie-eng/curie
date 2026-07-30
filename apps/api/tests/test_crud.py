"""CRUD round-trip against the real compose Postgres.

create agent -> create version -> deploy to dev -> list/get, the B1 done-when.
"""

import asyncio
from typing import Any

from curie_api.config import get_settings
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine


def _count(query: str, agent_id: str) -> int:
    async def run() -> int:
        engine = create_async_engine(get_settings().database_url)
        try:
            async with engine.connect() as conn:
                result = await conn.execute(text(query), {"aid": agent_id})
                return int(result.scalar_one())
        finally:
            await engine.dispose()

    return asyncio.run(run())


def test_full_round_trip(client: Any, auth_headers: dict[str, str], clean_db: None) -> None:
    # create agent
    resp = client.post(
        "/agents",
        json={"name": "triage-bot", "slack_channel": "C0TRIAGE01"},
        headers=auth_headers,
    )
    assert resp.status_code == 201, resp.text
    agent = resp.json()
    agent_id = agent["id"]
    assert agent["name"] == "triage-bot"

    # create version
    resp = client.post(
        f"/agents/{agent_id}/versions",
        json={"version_label": "v1", "created_by": "bconn"},
        headers=auth_headers,
    )
    assert resp.status_code == 201, resp.text
    version = resp.json()
    version_id = version["id"]
    assert version["bundle_ref"] is None
    assert version["agent_id"] == agent_id

    # deploy to dev
    resp = client.post(
        "/deployments",
        json={
            "agent_id": agent_id,
            "version_id": version_id,
            "environment": "dev",
        },
        headers=auth_headers,
    )
    assert resp.status_code == 201, resp.text
    deployment = resp.json()
    deployment_id = deployment["id"]
    assert deployment["environment"] == "dev"
    assert deployment["status"] == "active"

    # list + get every resource
    listed_agents = client.get("/agents", headers=auth_headers).json()
    assert [a["id"] for a in listed_agents] == [agent_id]

    got_agent = client.get(f"/agents/{agent_id}", headers=auth_headers)
    assert got_agent.status_code == 200
    assert got_agent.json()["slack_channel"] == "C0TRIAGE01"

    listed_versions = client.get(f"/agents/{agent_id}/versions", headers=auth_headers).json()
    assert [v["id"] for v in listed_versions] == [version_id]

    listed_deployments = client.get(
        "/deployments", params={"agent_id": agent_id}, headers=auth_headers
    ).json()
    assert [d["id"] for d in listed_deployments] == [deployment_id]

    got_deployment = client.get(f"/deployments/{deployment_id}", headers=auth_headers)
    assert got_deployment.status_code == 200
    assert got_deployment.json()["version_id"] == version_id


def test_missing_agent_returns_404(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    missing = "00000000-0000-0000-0000-000000000000"
    assert client.get(f"/agents/{missing}", headers=auth_headers).status_code == 404


def test_version_for_missing_agent_returns_404(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    missing = "00000000-0000-0000-0000-000000000000"
    resp = client.post(
        f"/agents/{missing}/versions",
        json={"version_label": "v1", "created_by": "bconn"},
        headers=auth_headers,
    )
    assert resp.status_code == 404


def test_patch_agent_moves_slack_channel(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    # A redeploy that passes a new --slack-channel must actually move the channel
    # of the existing agent (the audit MAJOR: the channel was silently ignored).
    agent = client.post(
        "/agents",
        json={"name": "mover", "slack_channel": "C000000OLD"},
        headers=auth_headers,
    ).json()
    agent_id = agent["id"]

    resp = client.patch(
        f"/agents/{agent_id}",
        json={"slack_channel": "C000000NEW"},
        headers=auth_headers,
    )
    assert resp.status_code == 200, resp.text
    assert resp.json()["slack_channel"] == "C000000NEW"

    # The change is persisted, not just echoed back.
    got = client.get(f"/agents/{agent_id}", headers=auth_headers).json()
    assert got["slack_channel"] == "C000000NEW"


def test_patch_agent_omitted_field_is_noop(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    agent = client.post(
        "/agents",
        json={"name": "stable", "slack_channel": "C0000KEEP1"},
        headers=auth_headers,
    ).json()
    resp = client.patch(f"/agents/{agent['id']}", json={}, headers=auth_headers)
    assert resp.status_code == 200, resp.text
    assert resp.json()["slack_channel"] == "C0000KEEP1"


def test_create_agent_rejects_non_id_channel(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    # The API is the authoritative gate (the CLI check is UX-only): a #name
    # binding never routes, so create must reject it with a 422, not persist a
    # dead binding a non-CLI caller (the UI) could create.
    resp = client.post(
        "/agents",
        json={"name": "bad-create", "slack_channel": "#general"},
        headers=auth_headers,
    )
    assert resp.status_code == 422, resp.text
    assert "slack channel" in resp.text.lower()
    # Nothing was persisted despite the rejected create.
    assert [a["id"] for a in client.get("/agents", headers=auth_headers).json()] == []


def test_patch_agent_rejects_non_id_channel(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    # A redeploy that PATCHes a #name channel onto an existing agent must be
    # rejected too, and must not clobber the agent's current (valid) channel.
    agent = client.post(
        "/agents",
        json={"name": "patch-bad", "slack_channel": "C000GOOD01"},
        headers=auth_headers,
    ).json()
    agent_id = agent["id"]

    resp = client.patch(
        f"/agents/{agent_id}",
        json={"slack_channel": "general"},
        headers=auth_headers,
    )
    assert resp.status_code == 422, resp.text
    assert "slack channel" in resp.text.lower()

    # The rejected PATCH left the original channel intact.
    got = client.get(f"/agents/{agent_id}", headers=auth_headers).json()
    assert got["slack_channel"] == "C000GOOD01"


def test_patch_missing_agent_returns_404(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    missing = "00000000-0000-0000-0000-000000000000"
    resp = client.patch(
        f"/agents/{missing}",
        json={"slack_channel": "C000000X01"},
        headers=auth_headers,
    )
    assert resp.status_code == 404


def test_delete_agent_removes_it_and_cascades_versions(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    # An agent with a version but no active deployment deletes cleanly, and the
    # version rows go with it (FK cascade) rather than lingering as orphans.
    agent = client.post(
        "/agents",
        json={"name": "disposable", "slack_channel": "C0000GONE1"},
        headers=auth_headers,
    ).json()
    agent_id = agent["id"]
    client.post(
        f"/agents/{agent_id}/versions",
        json={"version_label": "v1", "created_by": "bconn"},
        headers=auth_headers,
    )
    assert (
        _count(
            "SELECT count(*) FROM curie.agent_versions WHERE agent_id = :aid",
            agent_id,
        )
        == 1
    )

    resp = client.delete(f"/agents/{agent_id}", headers=auth_headers)
    assert resp.status_code == 204, resp.text

    # Agent is gone from the list and by id, and its version rows are deleted.
    assert client.get(f"/agents/{agent_id}", headers=auth_headers).status_code == 404
    assert [a["id"] for a in client.get("/agents", headers=auth_headers).json()] == []
    assert (
        _count(
            "SELECT count(*) FROM curie.agent_versions WHERE agent_id = :aid",
            agent_id,
        )
        == 0
    )


def test_delete_agent_with_active_deployment_returns_409(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    # A live agent (active deployment) must not be deletable out from under Slack
    # traffic; the endpoint refuses with 409 and leaves everything intact.
    agent = client.post(
        "/agents",
        json={"name": "live-one", "slack_channel": "C0000LIVE1"},
        headers=auth_headers,
    ).json()
    agent_id = agent["id"]
    version = client.post(
        f"/agents/{agent_id}/versions",
        json={"version_label": "v1", "created_by": "bconn"},
        headers=auth_headers,
    ).json()
    client.post(
        "/deployments",
        json={
            "agent_id": agent_id,
            "version_id": version["id"],
            "environment": "dev",
        },
        headers=auth_headers,
    )

    resp = client.delete(f"/agents/{agent_id}", headers=auth_headers)
    assert resp.status_code == 409, resp.text
    assert "active deployment" in resp.json()["detail"]

    # The agent (and its rows) survive the refused delete.
    assert client.get(f"/agents/{agent_id}", headers=auth_headers).status_code == 200


def test_delete_missing_agent_returns_404(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    missing = "00000000-0000-0000-0000-000000000000"
    resp = client.delete(f"/agents/{missing}", headers=auth_headers)
    assert resp.status_code == 404
