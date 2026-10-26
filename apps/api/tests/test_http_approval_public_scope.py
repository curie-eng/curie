"""Public approval creation classifies HTTP scope without changing other tenants.

Actual ASGI route and PostgreSQL rows; synthetic inputs only.
@spec HTTP-APPROVAL-CONTINUITY-1.
"""

from __future__ import annotations

import uuid
from types import SimpleNamespace
from typing import Any

from aci_protocol.turn import DEFAULT_IDENTITY
from curie_api.config import get_settings
from curie_api.db import SCHEMA
from curie_api.main import create_app
from fastapi.testclient import TestClient
from role_policy_support import run
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from apps.api.tests.test_http_approval_creation import creation_case as creation_case
from apps.api.tests.test_http_approval_creation import rows
from apps.api.tests.test_http_invocation_membership import synthetic_provider as synthetic_provider

KIND = "synthetic-chat"


def seed(bindings: list[dict[str, Any]]) -> list[uuid.UUID]:
    """@spec HTTP-APPROVAL-CONTINUITY-1: committed agents and routes, tenant optional."""
    agents: list[uuid.UUID] = []

    async def insert(factory: async_sessionmaker[AsyncSession]) -> None:
        """@spec HTTP-APPROVAL-CONTINUITY-1."""
        async with factory() as session, session.begin():
            for item in bindings:
                agent = item.get("agent") or uuid.uuid4()
                adapter = DEFAULT_IDENTITY if item["kind"] == "slack" else item.get("adapter")
                if item.get("agent") is None:
                    await session.execute(
                        text(f'INSERT INTO "{SCHEMA}".agents (id, name) VALUES (:id, :name)'),
                        {"id": agent, "name": f"synthetic-public-{agent.hex[:12]}"},
                    )
                await session.execute(
                    text(
                        f'INSERT INTO "{SCHEMA}".agent_channels '
                        "(id, agent_id, tenant_id, kind, address, adapter, endpoint) "
                        "SELECT :id, a.id, a.tenant_id, :kind, :address, :adapter, :endpoint "
                        f'FROM "{SCHEMA}".agents a WHERE a.id = :agent'
                    ),
                    {
                        "id": uuid.uuid4(), "agent": agent, "kind": item["kind"],
                        "address": item["address"], "adapter": adapter,
                        # agent_channels_route_ck: a non-Slack adapter has an endpoint.
                        "endpoint": None if item["kind"] == "slack" or adapter is None
                        else "https://synthetic-route.example.test/hook",
                    },
                )
                agents.append(agent)

    run(insert)
    return agents


def request(agent: uuid.UUID, kind: str, address: str, adapter: str | None) -> dict[str, Any]:
    """@spec HTTP-APPROVAL-CONTINUITY-1: an ordinary public creation body."""
    return {
        "agent_id": str(agent), "conversation_id": f"th-{uuid.uuid4().hex[:8]}",
        "author": "U0EXAMPLE1", "summary": "Approve the synthetic public operation",
        "reply_kind": kind, "reply_channel": address, "reply_adapter": adapter,
        "reply_placeholder": "p-1", "dedupe_key": uuid.uuid4().hex, "card_channel": address,
    }


def create(client: TestClient, body: dict[str, Any]) -> Any:
    """@spec HTTP-APPROVAL-CONTINUITY-1."""
    return client.post("/approvals", json=body, headers={"X-API-Key": get_settings().api_key})


def test_non_http_tenant_keeps_creation_for_an_agent_other_than_the_route_binding(
    clean_db: None,
) -> None:
    """@spec HTTP-APPROVAL-CONTINUITY-1: disagreement refuses only for HTTP-selected tenants."""
    address = "C0EXAMPLEPUB" + uuid.uuid4().hex[:6].upper()
    requester, _bound = seed([
        {"kind": KIND, "address": "unrelated-" + uuid.uuid4().hex, "adapter": "a1"},
        {"kind": "slack", "address": address},
    ])
    with TestClient(create_app()) as client:
        body = request(requester, "slack", address, None)
        response = create(client, body)
        assert response.status_code == 201, response.text
    assert len(rows(body["dedupe_key"])) == 1


def test_non_http_tenant_keeps_creation_on_an_ambiguous_unqualified_route(
    clean_db: None,
) -> None:
    """@spec HTTP-APPROVAL-CONTINUITY-1: ambiguity refuses only with an HTTP candidate."""
    address = "synthetic-room-" + uuid.uuid4().hex
    first, _second = seed([
        {"kind": KIND, "address": address, "adapter": "a1"},
        {"kind": KIND, "address": address, "adapter": "a2"},
    ])
    with TestClient(create_app()) as client:
        body = request(first, KIND, address, None)
        response = create(client, body)
        assert response.status_code == 201, response.text
    assert len(rows(body["dedupe_key"])) == 1


def test_ambiguous_route_with_an_http_selected_candidate_refuses_definitively(
    creation_case: SimpleNamespace,
) -> None:
    """@spec HTTP-APPROVAL-CONTINUITY-1: every candidate is classified, not only one."""
    case = creation_case
    address = "synthetic-room-" + uuid.uuid4().hex
    adopted = uuid.UUID(case.scope["agent"])
    _adopted, other = seed([
        {"kind": KIND, "address": address, "adapter": "a1", "agent": adopted},
        {"kind": KIND, "address": address, "adapter": "a2"},
    ])
    body = request(other, KIND, address, None)
    response = create(case.client, body)
    assert response.status_code == 409, response.text
    assert response.json() == {"detail": "unsupported_approval"}
    assert rows(body["dedupe_key"]) == []
    assert len(case.provider.requests) == 1
