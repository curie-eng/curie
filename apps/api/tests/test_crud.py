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
    assert got_agent.json()["channels"] == [{"kind": "slack", "address": "C0TRIAGE01"}]

    listed_versions = client.get(
        f"/agents/{agent_id}/versions", headers=auth_headers
    ).json()
    assert [v["id"] for v in listed_versions] == [version_id]

    listed_deployments = client.get(
        "/deployments", params={"agent_id": agent_id}, headers=auth_headers
    ).json()
    assert [d["id"] for d in listed_deployments] == [deployment_id]

    got_deployment = client.get(
        f"/deployments/{deployment_id}", headers=auth_headers
    )
    assert got_deployment.status_code == 200
    assert got_deployment.json()["version_id"] == version_id


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
    # `crud.update_channel_binding` behind the subresource (ADR-0107); it is
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
