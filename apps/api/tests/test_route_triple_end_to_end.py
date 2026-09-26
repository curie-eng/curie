"""Two Slack identities on one channel, through the API and the worker resolver.

ADR-0168 decision 3: the route is `(kind, address, adapter)`, so one channel
can be bound under two identities -- by two agents, or by one agent that
appears as two bots -- and the worker answers each identity's turn with the
agent bound under it, and a publication raised under each keeps its own
binding.
"""

from __future__ import annotations

import asyncio
import json
import uuid
from collections.abc import Iterator
from typing import Any

import pytest
from curie_api.config import get_settings
from curie_worker.binding import BindingResolver
from curie_worker.config import WorkerConfig
from fastapi.testclient import TestClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from apps.api.tests.test_publications import (
    _create_deployment,
    _create_publication,
    _publication_payload,
)
from apps.api.tests.test_publications import publication_stack as publication_stack

CHANNEL = "C0EXAMPLE1"
TWO_IDENTITIES = json.dumps(
    [
        {
            "name": "default",
            "app_token_env": "SLACK_APP_TOKEN",
            "bot_token_env": "SLACK_BOT_TOKEN",
            "signing_secret_env": "SLACK_SIGNING_SECRET",
        },
        {
            "name": "second",
            "app_token_env": "CURIE_SLACK_APP_TOKEN__0",
            "bot_token_env": "CURIE_SLACK_BOT_TOKEN__0",
            "signing_secret_env": None,
        },
    ]
)


@pytest.fixture
def two_identities(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    monkeypatch.setenv("CURIE_SLACK_IDENTITIES", TWO_IDENTITIES)
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


def _create(client: Any, headers: dict[str, str], name: str, adapter: str | None = None) -> str:
    channel: dict[str, Any] = {"kind": "slack", "address": CHANNEL}
    if adapter is not None:
        channel["adapter"] = adapter
    resp = client.post("/agents", json={"name": name, "channel": channel}, headers=headers)
    assert resp.status_code == 201, resp.text
    return str(resp.json()["id"])


def _deploy(agent_id: str) -> None:
    """A version and an active deployment, so `resolve` has something to boot."""

    async def go() -> None:
        engine = create_async_engine(get_settings().database_url)
        version_id = uuid.uuid4()
        try:
            async with engine.begin() as conn:
                await conn.execute(
                    text(
                        "INSERT INTO curie.agent_versions "
                        "(id, agent_id, version_label, bundle_ref, created_by) "
                        "VALUES (:id, :agent, 'v1', :ref, 'route-triple-test')"
                    ),
                    {"id": version_id, "agent": agent_id, "ref": f"bundles/{agent_id}.tar.gz"},
                )
                await conn.execute(
                    text(
                        "INSERT INTO curie.deployments "
                        "(id, agent_id, version_id, environment, status) "
                        "VALUES (:id, :agent, :version, CAST('prod' AS curie.environment), "
                        "'active')"
                    ),
                    {"id": uuid.uuid4(), "agent": agent_id, "version": version_id},
                )
        finally:
            await engine.dispose()

    asyncio.run(go())


def _resolved_agent(adapter: str | None) -> str | None:
    async def go() -> str | None:
        url = get_settings().database_url
        engine = create_async_engine(url)
        try:
            resolver = BindingResolver(engine, WorkerConfig(database_url=url, db_schema="curie"))
            resolved = await resolver.resolve("slack", adapter, CHANNEL)
            return None if resolved is None else str(resolved.agent_id)
        finally:
            await engine.dispose()

    return asyncio.run(go())


def test_two_agents_bind_one_channel_under_two_identities(
    client: Any, auth_headers: dict[str, str], clean_db: None, two_identities: None
) -> None:
    first = _create(client, auth_headers, "default-bot")
    second = _create(client, auth_headers, "second-bot", adapter="second")
    _deploy(first)
    _deploy(second)

    assert _resolved_agent(None) == first
    assert _resolved_agent("default") == first
    assert _resolved_agent("second") == second
    listed = {a["name"]: a["channels"] for a in client.get("/agents", headers=auth_headers).json()}
    assert listed["default-bot"] == [{"kind": "slack", "address": CHANNEL, "adapter": "default"}]
    assert listed["second-bot"] == [{"kind": "slack", "address": CHANNEL, "adapter": "second"}]


def test_one_agent_binds_one_channel_under_two_identities(
    client: Any, auth_headers: dict[str, str], clean_db: None, two_identities: None
) -> None:
    agent = _create(client, auth_headers, "two-bots")
    added = client.post(
        f"/agents/{agent}/channels",
        json={"kind": "slack", "address": CHANNEL, "adapter": "second"},
        headers=auth_headers,
    )
    assert added.status_code == 201, added.text
    assert [c["adapter"] for c in added.json()["channels"]] == ["default", "second"]
    _deploy(agent)

    assert _resolved_agent("default") == agent
    assert _resolved_agent("second") == agent


def test_an_identity_binds_a_channel_once(
    client: Any, auth_headers: dict[str, str], clean_db: None, two_identities: None
) -> None:
    _create(client, auth_headers, "holder", adapter="second")
    taken = client.post(
        "/agents",
        json={
            "name": "intruder",
            "channel": {"kind": "slack", "address": CHANNEL, "adapter": "second"},
        },
        headers=auth_headers,
    )
    assert taken.status_code == 409, taken.text
    assert "identity" in taken.json()["detail"], taken.text


def test_a_repost_of_the_same_route_is_idempotent(
    client: Any, auth_headers: dict[str, str], clean_db: None, two_identities: None
) -> None:
    agent = _create(client, auth_headers, "repost", adapter="second")
    again = client.post(
        f"/agents/{agent}/channels",
        json={"kind": "slack", "address": CHANNEL, "adapter": "second"},
        headers=auth_headers,
    )
    assert again.status_code == 201, again.text
    assert again.json()["channels"] == [{"kind": "slack", "address": CHANNEL, "adapter": "second"}]


def _binding_ids(agent_id: str) -> dict[str, str]:
    async def go() -> dict[str, str]:
        engine = create_async_engine(get_settings().database_url)
        try:
            async with engine.connect() as conn:
                rows = await conn.execute(
                    text("SELECT adapter, id FROM curie.agent_channels WHERE agent_id = :a"),
                    {"a": agent_id},
                )
                return {adapter: str(row_id) for adapter, row_id in rows.all()}
        finally:
            await engine.dispose()

    return asyncio.run(go())


def _lineage_binding_id(publication_id: str) -> str | None:
    async def go() -> str | None:
        engine = create_async_engine(get_settings().database_url)
        try:
            async with engine.connect() as conn:
                result = await conn.execute(
                    text(
                        "SELECT l.binding_id FROM curie.publications p "
                        "JOIN curie.thread_publication_lineages l ON l.id = p.lineage_id "
                        "WHERE p.id = :id"
                    ),
                    {"id": publication_id},
                )
                value = result.scalar_one()
                return None if value is None else str(value)
        finally:
            await engine.dispose()

    return asyncio.run(go())


def test_a_publication_under_each_identity_keeps_its_own_binding(
    publication_stack: tuple[TestClient, str],
    auth_headers: dict[str, str],
    clean_db: None,
    two_identities: None,
) -> None:
    """One agent answering as two bots on one channel: the lineage a
    publication opens captures the binding of the identity it was raised
    under, which is what its publication identity and review lane read."""

    client, _ = publication_stack
    deployment = _create_deployment(client, auth_headers, channel=CHANNEL)
    added = client.post(
        f"/agents/{deployment['agent_id']}/channels",
        json={"kind": "slack", "address": CHANNEL, "adapter": "second"},
        headers=auth_headers,
    )
    assert added.status_code == 201, added.text
    bindings = _binding_ids(deployment["agent_id"])

    for identity in ("default", "second"):
        payload = _publication_payload(deployment["id"])
        payload["reply_adapter"] = identity
        _, publication = _create_publication(client, payload)
        assert _lineage_binding_id(publication["id"]) == bindings[identity], identity
