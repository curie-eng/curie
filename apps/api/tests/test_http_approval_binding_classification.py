"""Approval continuity classifies by binding, reading the invocation and approver roles.

Actual ASGI routes, the real approvals router and PostgreSQL rows in one adopted
tenant with one registered identity; synthetic inputs only.
@spec HTTP-APPROVAL-CONTINUITY-14.
"""

from __future__ import annotations

import uuid
from types import SimpleNamespace
from typing import Any

import pytest
from curie_api.config import get_settings
from curie_api.db import SCHEMA
from curie_api.identity.http_approval import approval_is_selected
from curie_api.identity.http_transport import HttpIdentityClient
from curie_api.main import create_app
from curie_api.models import Agent, Approval
from fastapi import FastAPI
from fastapi.testclient import TestClient
from role_policy_support import run
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from test_http_approval_legacy_writers import chat_headers, operator_headers
from test_http_identity_per_receiver import _restrict, _sibling
from test_http_invocation_membership import (
    _runtime,
    _runtime_api,
    _scope,
    synthetic_provider,  # noqa: F401 - pytest fixture reused from the vertical suite.
)

pytestmark = pytest.mark.usefixtures("clean_db")
ROUTE = "synthetic-approvers"


def _app(scope: dict[str, Any], provider: SimpleNamespace, *, initialized: bool) -> FastAPI:
    """@spec HTTP-APPROVAL-CONTINUITY-14: with the registered identity, or no runtime at all."""
    if not initialized:
        return create_app()
    return create_app(
        http_identity_runtime=_runtime(_runtime_api(), scope),
        http_identity_client=HttpIdentityClient(tls_context=provider.tls),
    )


def _body(binding: dict[str, Any], **changes: Any) -> dict[str, Any]:
    """@spec HTTP-APPROVAL-CONTINUITY-14: an ordinary provenance-free public creation."""
    body = {
        "agent_id": binding["agent"], "conversation_id": f"th-{uuid.uuid4().hex[:8]}",
        "author": "U0EXAMPLE1", "summary": "Approve the synthetic per-binding operation",
        "reply_kind": binding["kind"], "reply_channel": binding["address"],
        "reply_adapter": binding["adapter"], "reply_placeholder": "p-1",
        "reply_endpoint": None if binding["kind"] == "slack"
        else "https://mail.example.test/reply",
        "dedupe_key": uuid.uuid4().hex, "card_channel": binding["address"],
    }
    body.update(changes)
    return body


def _create(client: TestClient, body: dict[str, Any]) -> Any:
    """@spec HTTP-APPROVAL-CONTINUITY-14: the real public approvals router."""
    return client.post("/approvals", json=body, headers={"X-API-Key": get_settings().api_key})


def _stored(dedupe: str) -> list[dict[str, Any]]:
    """@spec HTTP-APPROVAL-CONTINUITY-14: committed rows by dedupe key."""
    found: list[dict[str, Any]] = []

    async def read(factory: async_sessionmaker[AsyncSession]) -> None:
        """@spec HTTP-APPROVAL-CONTINUITY-14."""
        async with factory() as session:
            result = await session.execute(
                text(f'SELECT id, status, identity_context FROM "{SCHEMA}".approvals '
                     "WHERE dedupe_key = :k"),
                {"k": dedupe},
            )
            found.extend(dict(row) for row in result.mappings())

    run(read)
    return found


def _insert(binding: dict[str, Any]) -> uuid.UUID:
    """@spec HTTP-APPROVAL-CONTINUITY-14: a provenance-free pending row on the binding's route.

    It stands for a row written before the agent's approver role changed.
    """
    approval_id = uuid.uuid4()

    async def write(factory: async_sessionmaker[AsyncSession]) -> None:
        """@spec HTTP-APPROVAL-CONTINUITY-14."""
        async with factory() as session, session.begin():
            await session.execute(
                text(f'INSERT INTO "{SCHEMA}".approvals (id, agent_id, conversation_id, author, '
                     "summary, reply_kind, reply_channel, reply_adapter, reply_endpoint, "
                     "reply_placeholder, dedupe_key, status, card_channel) VALUES (:id, :agent, "
                     ":conversation, 'U0EXAMPLE1', 'Approve the synthetic stored operation', "
                     ":kind, :channel, :adapter, :endpoint, 'p-1', :dedupe, 'pending', :channel)"),
                {
                    "id": approval_id, "agent": uuid.UUID(binding["agent"]),
                    "conversation": f"th-{uuid.uuid4().hex[:8]}", "kind": binding["kind"],
                    "channel": binding["address"], "adapter": binding["adapter"],
                    "endpoint": None if binding["kind"] == "slack"
                    else "https://mail.example.test/reply",
                    "dedupe": uuid.uuid4().hex,
                },
            )

    run(write)
    return approval_id


def _route(binding: dict[str, Any], approvers: list[str]) -> None:
    """@spec HTTP-APPROVAL-CONTINUITY-14: a legacy approval route with an explicit user list."""

    async def write(factory: async_sessionmaker[AsyncSession]) -> None:
        """@spec HTTP-APPROVAL-CONTINUITY-14."""
        async with factory() as session, session.begin():
            agent = await session.get(Agent, uuid.UUID(binding["agent"]))
            assert agent is not None
            agent.approval_routes = {ROUTE: {
                "resolution": {"kind": binding["kind"], "address": binding["address"]},
                "approvers": {"users": approvers},
            }}

    run(write)


def _selected(client: TestClient, approval_id: uuid.UUID) -> bool:
    """@spec HTTP-APPROVAL-CONTINUITY-14: the owner every approval reader calls."""
    runtime = getattr(client.app.state, "http_identity_runtime", None)
    answer: list[bool] = []

    async def classify(factory: async_sessionmaker[AsyncSession]) -> None:
        """@spec HTTP-APPROVAL-CONTINUITY-14."""
        async with factory() as session:
            row = await session.get(Approval, approval_id)
            assert row is not None
            answer.append(await approval_is_selected(session, row, runtime=runtime))

    run(classify)
    return answer[0]


@pytest.mark.parametrize("kind", ["slack", "email"])
@pytest.mark.parametrize("initialized", [True, False])
def test_legacy_binding_in_an_adopted_tenant_keeps_the_legacy_approval_path(
    synthetic_provider: SimpleNamespace,  # noqa: F811 - the reused fixture.
    kind: str,
    initialized: bool,
) -> None:
    """@spec HTTP-APPROVAL-CONTINUITY-14: no HTTP-only role, unregistered: legacy, not 409.

    The legacy route's own approver list decides the answer, so the row
    reaches legacy authorization instead of the selected-row refusal.
    """
    scope = _scope(synthetic_provider.url)
    sibling = _sibling(scope, kind=kind)
    _route(sibling, ["U0EXAMPLEOTHER"])
    with TestClient(_app(scope, synthetic_provider, initialized=initialized)) as client:
        body = _body(sibling, route=ROUTE)
        response = _create(client, body)
        assert response.status_code == 201, response.text
        stored = _stored(body["dedupe_key"])
        assert [(row["status"], row["identity_context"]) for row in stored] == [("pending", None)]
        approval_id = stored[0]["id"]
        assert _selected(client, approval_id) is False

        def resolve() -> Any:
            """@spec HTTP-APPROVAL-CONTINUITY-14: an operator principal, U0OPERATOR1."""
            return client.post(
                f"/approvals/{approval_id}/resolve", json={"decision": "approved"},
                headers=operator_headers(),
            )

        outside = resolve()
        assert outside.status_code == 403, outside.text
        assert outside.json() != {"detail": "unsupported_approval"}
        assert _stored(body["dedupe_key"])[0]["status"] == "pending"
        _route(sibling, ["U0OPERATOR1"])
        inside = resolve()
        assert inside.status_code == 200, inside.text
        assert inside.json()["status"] == "approved"
    assert synthetic_provider.requests == []


@pytest.mark.parametrize(
    ("kind", "operation", "initialized"),
    [
        ("slack", "migrate_role", True),
        ("email", "set_filter", True),
        ("slack", "set_filter", False),
        ("email", "migrate_role", False),
    ],
)
def test_approver_only_agent_keeps_its_approvals_selected(
    synthetic_provider: SimpleNamespace,  # noqa: F811 - the reused fixture.
    kind: str,
    operation: str,
    initialized: bool,
) -> None:
    """@spec HTTP-APPROVAL-CONTINUITY-14: ADR 0259 decision 1, the approver clause.

    The invocation role is absent. Legacy checks never read the approver
    filter, so a legacy answer would admit actors the filter excludes: creation
    refuses and a stored provenance-free row is selected, refusing every legacy
    resolver before authorization.
    """
    scope = _scope(synthetic_provider.url)
    sibling = _sibling(scope, kind=kind)
    _restrict(sibling, operation, target="approver")
    with TestClient(_app(scope, synthetic_provider, initialized=initialized)) as client:
        body = _body(sibling)
        refused = _create(client, body)
        assert refused.status_code == 409, refused.text
        assert refused.json() == {"detail": "unsupported_approval"}
        assert _stored(body["dedupe_key"]) == []

        approval_id = _insert(sibling)
        assert _selected(client, approval_id) is True
        for headers in (operator_headers(), chat_headers(approval_id, sibling["address"])):
            response = client.post(
                f"/approvals/{approval_id}/resolve", json={"decision": "approved"},
                headers=headers,
            )
            assert response.status_code == 403, response.text
            assert response.json() == {"detail": "unsupported_approval"}
    assert synthetic_provider.requests == []


def test_adopted_agent_route_outside_its_own_binding_stays_selected(
    synthetic_provider: SimpleNamespace,  # noqa: F811 - the reused fixture.
) -> None:
    """@spec HTTP-APPROVAL-CONTINUITY-14: only the agent's own binding can make it legacy.

    The approval's agent has no HTTP-only role, but its route resolves to no
    binding, or to another agent's binding, so the agent's approver role is not
    shown to be legacy-evaluable.
    """
    scope = _scope(synthetic_provider.url)
    requester = _sibling(scope)
    other = _sibling(scope, kind="email")
    with TestClient(_app(scope, synthetic_provider, initialized=True)) as client:
        for changes in (
            {"reply_channel": "C0EXAMPLEUNBOUND", "card_channel": "C0EXAMPLEUNBOUND"},
            {
                "reply_kind": "email", "reply_channel": other["address"],
                "reply_adapter": other["adapter"],
                "reply_endpoint": "https://mail.example.test/reply",
                "card_channel": other["address"],
            },
        ):
            body = _body(requester, **changes)
            response = _create(client, body)
            assert response.status_code == 409, response.text
            assert response.json() == {"detail": "unsupported_approval"}
            assert _stored(body["dedupe_key"]) == []
