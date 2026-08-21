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


def test_full_round_trip(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    # create agent
    resp = client.post(
        "/agents",
        json={"name": "triage-bot", "channel": {"kind": "slack", "address": "C0TRIAGE01"}},
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
    assert version["commit_sha"] is None
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
    assert deployment["commit_sha"] is None

    # list + get every resource
    listed_agents = client.get("/agents", headers=auth_headers).json()
    assert [a["id"] for a in listed_agents] == [agent_id]

    got_agent = client.get(f"/agents/{agent_id}", headers=auth_headers)
    assert got_agent.status_code == 200
    assert got_agent.json()["channels"] == [{"kind": "slack", "address": "C0TRIAGE01"}]

    listed_versions = client.get(
        f"/agents/{agent_id}/versions", headers=auth_headers
    ).json()
    assert [v["id"] for v in listed_versions] == [version_id]
    assert listed_versions[0]["commit_sha"] is None

    listed_deployments = client.get(
        "/deployments", params={"agent_id": agent_id}, headers=auth_headers
    ).json()
    assert [d["id"] for d in listed_deployments] == [deployment_id]
    assert listed_deployments[0]["commit_sha"] is None

    got_deployment = client.get(
        f"/deployments/{deployment_id}", headers=auth_headers
    )
    assert got_deployment.status_code == 200
    assert got_deployment.json()["version_id"] == version_id
    assert got_deployment.json()["commit_sha"] is None


def test_version_and_deployment_persist_same_commit_sha(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    commit_sha = "0123456789abcdef0123456789abcdef01234567"
    agent = client.post(
        "/agents",
        json={
            "name": "commit_same",
            "channel": {"kind": "slack", "address": "C0EXAMPLE1"},
        },
        headers=auth_headers,
    ).json()
    agent_id = agent["id"]

    version_resp = client.post(
        f"/agents/{agent_id}/versions",
        json={
            "version_label": "v1",
            "created_by": "bconn",
            "commit_sha": commit_sha,
        },
        headers=auth_headers,
    )
    assert version_resp.status_code == 201, version_resp.text
    version = version_resp.json()
    assert version["commit_sha"] == commit_sha

    deployment_resp = client.post(
        "/deployments",
        json={
            "agent_id": agent_id,
            "version_id": version["id"],
            "environment": "dev",
            "commit_sha": commit_sha,
        },
        headers=auth_headers,
    )
    assert deployment_resp.status_code == 201, deployment_resp.text
    deployment = deployment_resp.json()
    assert deployment["commit_sha"] == commit_sha

    listed_versions = client.get(
        f"/agents/{agent_id}/versions", headers=auth_headers
    ).json()
    assert listed_versions[0]["commit_sha"] == commit_sha
    listed_deployments = client.get(
        "/deployments", params={"agent_id": agent_id}, headers=auth_headers
    ).json()
    assert listed_deployments[0]["commit_sha"] == commit_sha


def test_public_version_create_rejects_git_flow_provenance(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    agent = client.post(
        "/agents",
        json={
            "name": "provenance_guard",
            "channel": {"kind": "slack", "address": "C0EXAMPLE1"},
        },
        headers=auth_headers,
    ).json()
    agent_id = agent["id"]

    version_resp = client.post(
        f"/agents/{agent_id}/versions",
        json={"version_label": "v1", "created_by": "git-flow"},
        headers=auth_headers,
    )

    assert version_resp.status_code == 422, version_resp.text
    assert client.get(f"/agents/{agent_id}/versions", headers=auth_headers).json() == []


def test_version_and_deployment_create_reject_blank_commit_sha(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    agent = client.post(
        "/agents",
        json={
            "name": "commit_sha_guard",
            "channel": {"kind": "slack", "address": "C0EXAMPLE1"},
        },
        headers=auth_headers,
    ).json()
    agent_id = agent["id"]

    for value in ("", "   "):
        response = client.post(
            f"/agents/{agent_id}/versions",
            json={"version_label": "invalid", "created_by": "bconn", "commit_sha": value},
            headers=auth_headers,
        )
        assert response.status_code == 422, response.text

    version = client.post(
        f"/agents/{agent_id}/versions",
        json={"version_label": "valid", "created_by": "bconn"},
        headers=auth_headers,
    ).json()
    for value in ("", "   "):
        response = client.post(
            "/deployments",
            json={
                "agent_id": agent_id,
                "version_id": version["id"],
                "environment": "dev",
                "commit_sha": value,
            },
            headers=auth_headers,
        )
        assert response.status_code == 422, response.text


# The eval trigger fallback requires deployment provenance to remain independent.
def test_version_and_deployment_persist_distinct_commit_shas(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    version_sha = "0123456789abcdef0123456789abcdef01234567"
    deployment_sha = "89abcdef0123456789abcdef0123456789abcdef"
    agent = client.post(
        "/agents",
        json={
            "name": "commit_distinct",
            "channel": {"kind": "slack", "address": "C0EXAMPLE1"},
        },
        headers=auth_headers,
    ).json()
    agent_id = agent["id"]

    version_resp = client.post(
        f"/agents/{agent_id}/versions",
        json={
            "version_label": "v1",
            "created_by": "bconn",
            "commit_sha": version_sha,
        },
        headers=auth_headers,
    )
    assert version_resp.status_code == 201, version_resp.text
    version = version_resp.json()

    deployment_resp = client.post(
        "/deployments",
        json={
            "agent_id": agent_id,
            "version_id": version["id"],
            "environment": "dev",
            "commit_sha": deployment_sha,
        },
        headers=auth_headers,
    )
    assert deployment_resp.status_code == 201, deployment_resp.text
    deployment = deployment_resp.json()

    listed_versions = client.get(
        f"/agents/{agent_id}/versions", headers=auth_headers
    ).json()
    assert listed_versions[0]["commit_sha"] == version_sha
    got_deployment = client.get(
        f"/deployments/{deployment['id']}", headers=auth_headers
    )
    assert got_deployment.status_code == 200, got_deployment.text
    assert got_deployment.json()["commit_sha"] == deployment_sha


def test_missing_agent_returns_404(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    missing = "00000000-0000-0000-0000-000000000000"
    assert (
        client.get(f"/agents/{missing}", headers=auth_headers).status_code == 404
    )


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


def test_update_channel_binding_moves_the_channel(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    # A redeploy that passes a new --slack-channel must actually move the channel
    # of the existing agent (the audit MAJOR: the channel was silently ignored).
    # The seam moved from `crud.update_agent_binding` to
    # `crud.update_channel_binding` behind the subresource (ADR-0116); it is
    # driven here through HTTP because that is where the round trip is real.
    agent = client.post(
        "/agents",
        json={"name": "mover", "channel": {"kind": "slack", "address": "C000000OLD"}},
        headers=auth_headers,
    ).json()
    agent_id = agent["id"]

    resp = client.patch(
        f"/agents/{agent_id}/channels",
        params={"kind": "slack", "address": "C000000OLD"},
        json={"kind": "slack", "address": "C000000NEW"},
        headers=auth_headers,
    )
    assert resp.status_code == 200, resp.text
    assert resp.json()["channels"] == [{"kind": "slack", "address": "C000000NEW"}]

    # The change is persisted, not just echoed back.
    got = client.get(f"/agents/{agent_id}", headers=auth_headers).json()
    assert got["channels"] == [{"kind": "slack", "address": "C000000NEW"}]


def test_add_channel_binding_appends_and_leaves_the_first_alone(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    # `crud.add_channel_binding`, through the subresource: appending must not be
    # a disguised move. An add that silently replaced would leave the operator
    # believing the agent listens on two channels while one of them is dead --
    # #38's shadow state reached through the very verb meant to prevent it.
    agent = client.post(
        "/agents",
        json={"name": "adder", "channel": {"kind": "slack", "address": "C0000ADD01"}},
        headers=auth_headers,
    ).json()

    added = client.post(
        f"/agents/{agent['id']}/channels",
        json={"kind": "slack", "address": "C0EXAMPLE1"},
        headers=auth_headers,
    )
    assert added.status_code == 201, added.text

    got = client.get(f"/agents/{agent['id']}", headers=auth_headers).json()
    assert got["channels"] == [
        {"kind": "slack", "address": "C0000ADD01"},
        {"kind": "slack", "address": "C0EXAMPLE1"},
    ]


def test_delete_channel_binding_removes_only_the_named_row(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    # `crud.delete_channel_binding`, through the subresource. The agent keeps
    # every other binding, and the freed pair is genuinely free -- a row deleted
    # from the response but not from the table would hold its address hostage
    # against every future agent.
    agent = client.post(
        "/agents",
        json={"name": "remover", "channel": {"kind": "slack", "address": "C0000DEL01"}},
        headers=auth_headers,
    ).json()
    assert (
        client.post(
            f"/agents/{agent['id']}/channels",
            json={"kind": "slack", "address": "C0000DEL02"},
            headers=auth_headers,
        ).status_code
        == 201
    )

    removed = client.request(
        "DELETE",
        f"/agents/{agent['id']}/channels",
        params={"kind": "slack", "address": "C0000DEL02"},
        headers=auth_headers,
    )
    assert removed.status_code == 204, removed.text

    got = client.get(f"/agents/{agent['id']}", headers=auth_headers).json()
    assert got["channels"] == [{"kind": "slack", "address": "C0000DEL01"}]
    assert (
        _count(
            "SELECT count(*) FROM curie.agent_channels WHERE agent_id = :aid",
            agent["id"],
        )
        == 1
    )

    reused = client.post(
        "/agents",
        json={"name": "reuses", "channel": {"kind": "slack", "address": "C0000DEL02"}},
        headers=auth_headers,
    )
    assert reused.status_code == 201, reused.text


def test_patch_agent_omitted_field_is_noop(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    agent = client.post(
        "/agents",
        json={"name": "stable", "channel": {"kind": "slack", "address": "C0000KEEP1"}},
        headers=auth_headers,
    ).json()
    resp = client.patch(
        f"/agents/{agent['id']}", json={}, headers=auth_headers
    )
    assert resp.status_code == 200, resp.text
    assert resp.json()["channels"] == [{"kind": "slack", "address": "C0000KEEP1"}]


def test_create_agent_rejects_non_id_channel(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    # The API is the authoritative gate (the CLI check is UX-only): a #name
    # binding never routes, so create must reject it with a 422, not persist a
    # dead binding a non-CLI caller (the UI) could create.
    resp = client.post(
        "/agents",
        json={"name": "bad-create", "channel": {"kind": "slack", "address": "#general"}},
        headers=auth_headers,
    )
    assert resp.status_code == 422, resp.text
    assert "slack channel" in resp.text.lower()
    # Nothing was persisted despite the rejected create.
    assert [a["id"] for a in client.get("/agents", headers=auth_headers).json()] == []


def test_binding_writes_reject_a_non_id_channel(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    # A redeploy that moves an existing agent onto a #name channel must be
    # rejected too, and must not clobber the agent's current (valid) channel.
    # Both binding verbs are checked: the address validator lives on the write
    # schema, and a subresource that reached the database on ADD while only
    # PATCH validated would persist a dead binding through the other door.
    agent = client.post(
        "/agents",
        json={"name": "patch-bad", "channel": {"kind": "slack", "address": "C000GOOD01"}},
        headers=auth_headers,
    ).json()
    agent_id = agent["id"]

    moved = client.patch(
        f"/agents/{agent_id}/channels",
        params={"kind": "slack", "address": "C000GOOD01"},
        json={"kind": "slack", "address": "general"},
        headers=auth_headers,
    )
    assert moved.status_code == 422, moved.text
    assert "slack channel" in moved.text.lower()

    added = client.post(
        f"/agents/{agent_id}/channels",
        json={"kind": "slack", "address": "general"},
        headers=auth_headers,
    )
    assert added.status_code == 422, added.text
    assert "slack channel" in added.text.lower()

    # The rejected writes left the original channel intact, and added nothing.
    got = client.get(f"/agents/{agent_id}", headers=auth_headers).json()
    assert got["channels"] == [{"kind": "slack", "address": "C000GOOD01"}]


def test_patch_missing_agent_returns_404(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    missing = "00000000-0000-0000-0000-000000000000"
    resp = client.patch(
        f"/agents/{missing}",
        json={"model": "claude-sonnet-5"},
        headers=auth_headers,
    )
    assert resp.status_code == 404


def test_binding_writes_for_a_missing_agent_return_404(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    # The subresource is agent-scoped, so an unknown agent is a 404 on all three
    # verbs -- never a 422 about the pair, which would send the caller looking
    # for a problem with the address they sent.
    #
    # NOT a fail-first test, and deliberately recorded as such: an ABSENT route
    # answers 404 too, so this passes vacuously until the subresource exists. It
    # becomes load-bearing the moment it does, which is why it is written now
    # rather than after -- an agent-scoped route that resolved a pair before
    # checking the agent would answer 422 or 200 here.
    missing = "00000000-0000-0000-0000-000000000000"
    pair = {"kind": "slack", "address": "C000000X01"}

    assert (
        client.post(f"/agents/{missing}/channels", json=pair, headers=auth_headers).status_code
        == 404
    )
    assert (
        client.patch(
            f"/agents/{missing}/channels", params=pair, json=pair, headers=auth_headers
        ).status_code
        == 404
    )
    assert (
        client.request(
            "DELETE", f"/agents/{missing}/channels", params=pair, headers=auth_headers
        ).status_code
        == 404
    )


def test_delete_agent_removes_it_and_cascades_versions(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    # An agent with a version but no active deployment deletes cleanly, and the
    # version rows go with it (FK cascade) rather than lingering as orphans.
    agent = client.post(
        "/agents",
        json={"name": "disposable", "channel": {"kind": "slack", "address": "C0000GONE1"}},
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
        json={"name": "live-one", "channel": {"kind": "slack", "address": "C0000LIVE1"}},
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


def _create_active_deployment(
    client: Any, auth_headers: dict[str, str], *, name: str
) -> tuple[str, str, str]:
    agent_resp = client.post(
        "/agents",
        json={
            "name": name,
            "channel": {"kind": "slack", "address": "C0EXAMPLE1"},
        },
        headers=auth_headers,
    )
    assert agent_resp.status_code == 201, agent_resp.text
    agent = agent_resp.json()

    version_resp = client.post(
        f"/agents/{agent['id']}/versions",
        json={"version_label": "v1", "created_by": "bconn"},
        headers=auth_headers,
    )
    assert version_resp.status_code == 201, version_resp.text
    version = version_resp.json()

    deployment_resp = client.post(
        "/deployments",
        json={
            "agent_id": agent["id"],
            "version_id": version["id"],
            "environment": "dev",
        },
        headers=auth_headers,
    )
    assert deployment_resp.status_code == 201, deployment_resp.text
    deployment = deployment_resp.json()
    assert deployment["status"] == "active"
    return agent["id"], version["id"], deployment["id"]


def test_end_deployment_marks_row_stopped_and_preserves_history(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    agent_id, _, deployment_id = _create_active_deployment(
        client, auth_headers, name="stop-history"
    )

    resp = client.delete(f"/deployments/{deployment_id}", headers=auth_headers)
    assert resp.status_code == 204, resp.text

    got = client.get(f"/deployments/{deployment_id}", headers=auth_headers)
    assert got.status_code == 200, got.text
    assert got.json()["id"] == deployment_id
    assert got.json()["status"] == "stopped"

    listed = client.get(
        "/deployments", params={"agent_id": agent_id}, headers=auth_headers
    )
    assert listed.status_code == 200, listed.text
    assert [deployment["id"] for deployment in listed.json()] == [deployment_id]
    assert listed.json()[0]["status"] == "stopped"


def test_end_deployment_is_idempotent(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    _, _, deployment_id = _create_active_deployment(
        client, auth_headers, name="stop-idempotent"
    )

    first = client.delete(f"/deployments/{deployment_id}", headers=auth_headers)
    assert first.status_code == 204, first.text
    second = client.delete(f"/deployments/{deployment_id}", headers=auth_headers)
    assert second.status_code == 204, second.text

    got = client.get(f"/deployments/{deployment_id}", headers=auth_headers)
    assert got.status_code == 200, got.text
    assert got.json()["status"] == "stopped"


def test_end_missing_deployment_returns_404(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    missing = "00000000-0000-0000-0000-000000000000"
    resp = client.delete(f"/deployments/{missing}", headers=auth_headers)
    assert resp.status_code == 404, resp.text


def test_end_deployment_requires_authentication(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    _, _, deployment_id = _create_active_deployment(
        client, auth_headers, name="stop-auth"
    )

    resp = client.delete(f"/deployments/{deployment_id}")
    assert resp.status_code == 401, resp.text

    got = client.get(f"/deployments/{deployment_id}", headers=auth_headers)
    assert got.status_code == 200, got.text
    assert got.json()["status"] == "active"


def test_agent_delete_requires_every_deployment_to_be_stopped(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    agent_id, version_id, first_deployment_id = _create_active_deployment(
        client, auth_headers, name="stop-before-delete"
    )

    first_stop = client.delete(
        f"/deployments/{first_deployment_id}", headers=auth_headers
    )
    assert first_stop.status_code == 204, first_stop.text

    redeploy_resp = client.post(
        "/deployments",
        json={
            "agent_id": agent_id,
            "version_id": version_id,
            "environment": "dev",
        },
        headers=auth_headers,
    )
    assert redeploy_resp.status_code == 201, redeploy_resp.text
    redeployment_id = redeploy_resp.json()["id"]
    assert redeploy_resp.json()["status"] == "active"

    blocked_delete = client.delete(f"/agents/{agent_id}", headers=auth_headers)
    assert blocked_delete.status_code == 409, blocked_delete.text

    second_stop = client.delete(
        f"/deployments/{redeployment_id}", headers=auth_headers
    )
    assert second_stop.status_code == 204, second_stop.text

    delete = client.delete(f"/agents/{agent_id}", headers=auth_headers)
    assert delete.status_code == 204, delete.text
    assert client.get(f"/agents/{agent_id}", headers=auth_headers).status_code == 404
