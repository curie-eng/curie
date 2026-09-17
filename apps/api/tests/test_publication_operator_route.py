"""Operator resolution of publication approvals that name an audience route.

Pins #2705: a publication that carries a bound `approvers.users` route is
resolvable by an operator principal. Omitting the route stays fail-closed.
"""

from __future__ import annotations

import asyncio
import time
import uuid
from typing import Any

from curie_api import approval_principal
from curie_api.config import get_settings
from curie_worker.publication_store import PostgresPublicationStore
from fastapi.testclient import TestClient
from sqlalchemy.ext.asyncio import create_async_engine

from apps.api.tests.test_publications import (
    CLUSTER_MESSAGE_ADAPTER,
    REPO,
    WORKER_HEADERS,
    _create_deployment,
    _create_publication,
    _publication_payload,
    _rows,
    _stream_entries,
)
from apps.api.tests.test_publications import publication_stack as publication_stack

PRINCIPAL_HEADER = "X-Curie-Approval-Principal"
SUBJECT = "U0EXAMPLE1"
OTHER = "U0EXAMPLE2"
ELIGIBILITY_403 = (
    "operator approval principals can resolve only routes bound to an explicit user list"
)
MEMBERSHIP_403 = "explicit list of approvers"
UNBOUND_403 = "no longer bound"


def _operator_token(
    subject: str = SUBJECT,
    *,
    scope: str = approval_principal.APPROVE_SCOPE,
    exp: int | None = None,
) -> str:
    return approval_principal.mint(
        get_settings().api_key,
        subject=subject,
        kind="operator",
        scope=scope,
        exp=exp if exp is not None else int(time.time()) + 60,
    )


def _principal_headers(token: str) -> dict[str, str]:
    return {PRINCIPAL_HEADER: token}


def _users_binding(users: list[str], *, address: str = "C0EXAMPLE1") -> dict[str, Any]:
    return {
        "resolution": {"kind": "slack", "address": address},
        "approvers": {"users": users},
    }


def _create_deployment_with_routes(
    client: TestClient,
    auth_headers: dict[str, str],
    *,
    route: str,
    users: list[str],
    channel: str = "C0EXAMPLE1",
) -> dict[str, Any]:
    suffix = uuid.uuid4().hex[:8]
    agent_response = client.post(
        "/agents",
        json={
            "name": f"publisher-{suffix}",
            "channel": {"kind": "slack", "address": channel},
            "repo_full_name": REPO,
            "approval_routes": {route: _users_binding(users, address=channel)},
        },
        headers=auth_headers,
    )
    assert agent_response.status_code == 201, agent_response.text
    agent_id = agent_response.json()["id"]
    version_response = client.post(
        f"/agents/{agent_id}/versions",
        json={"version_label": "v1", "created_by": "operator"},
        headers=auth_headers,
    )
    assert version_response.status_code == 201, version_response.text
    deployment_response = client.post(
        "/deployments",
        json={
            "agent_id": agent_id,
            "version_id": version_response.json()["id"],
            "environment": "dev",
            "workspace_enabled": True,
        },
        headers=auth_headers,
    )
    assert deployment_response.status_code == 201, deployment_response.text
    return deployment_response.json()


def _operator_resolve(
    client: TestClient,
    approval_id: str,
    *,
    decision: str = "approved",
    subject: str = SUBJECT,
) -> Any:
    return client.post(
        f"/approvals/{approval_id}/resolve",
        json={"decision": decision},
        headers=_principal_headers(_operator_token(subject)),
    )


def test_operator_principal_resolves_a_publication_with_explicit_users_route(
    publication_stack: tuple[TestClient, str],
    auth_headers: dict[str, str],
    clean_db: None,
) -> None:
    client, _ = publication_stack
    route = f"operators-{uuid.uuid4().hex[:8]}"
    deployment = _create_deployment_with_routes(
        client, auth_headers, route=route, users=[SUBJECT]
    )
    payload = _publication_payload(deployment["id"])
    payload["route"] = route
    _, publication = _create_publication(client, payload)

    stored_approval = client.get(
        f"/approvals/{publication['approval_id']}", headers=auth_headers
    )
    assert stored_approval.status_code == 200, stored_approval.text
    assert stored_approval.json()["route"] == route

    resolved = _operator_resolve(client, publication["approval_id"])
    assert resolved.status_code == 200, resolved.text

    audit = client.get(
        f"/approvals/{publication['approval_id']}/audit", headers=auth_headers
    )
    assert audit.status_code == 200, audit.text
    assert audit.json()[0]["principal_kind"] == "operator"
    assert audit.json()[0]["actor"] == SUBJECT
    assert audit.json()[0]["authorized"] is True

    stored = client.get(f"/publications/{publication['id']}", headers=auth_headers)
    assert stored.status_code == 200, stored.text
    assert stored.json()["status"] == "approved"


def test_operator_reject_denies_publication_and_issues_no_credential(
    publication_stack: tuple[TestClient, str],
    auth_headers: dict[str, str],
    clean_db: None,
) -> None:
    client, runs_stream = publication_stack
    route = f"operators-{uuid.uuid4().hex[:8]}"
    deployment = _create_deployment_with_routes(
        client, auth_headers, route=route, users=[SUBJECT]
    )
    payload = _publication_payload(deployment["id"])
    payload["route"] = route
    _, publication = _create_publication(client, payload)

    denied = _operator_resolve(
        client, publication["approval_id"], decision="rejected"
    )
    assert denied.status_code == 200, denied.text
    stored = client.get(f"/publications/{publication['id']}", headers=auth_headers)
    assert stored.json()["status"] == "denied"
    assert stored.json()["version"] == 2
    credential = client.post(
        f"/v1/internal/publications/{publication['id']}/credential",
        headers=WORKER_HEADERS,
    )
    assert credential.status_code == 409
    assert credential.headers["cache-control"] == "no-store"
    assert "approved" in credential.json()["detail"].lower()
    assert _stream_entries(runs_stream) == []
    assert _rows(
        "SELECT count(*) AS count FROM curie.publications "
        "WHERE status IN ('approved', 'launching', 'running')"
    ) == [{"count": 0}]


def test_omitted_publication_route_still_refuses_operator_with_eligibility_403(
    publication_stack: tuple[TestClient, str],
    auth_headers: dict[str, str],
    clean_db: None,
) -> None:
    client, _ = publication_stack
    route = f"operators-{uuid.uuid4().hex[:8]}"
    deployment = _create_deployment_with_routes(
        client, auth_headers, route=route, users=[SUBJECT]
    )
    _, publication = _create_publication(
        client, _publication_payload(deployment["id"])
    )

    stored_approval = client.get(
        f"/approvals/{publication['approval_id']}", headers=auth_headers
    )
    assert stored_approval.status_code == 200, stored_approval.text
    assert stored_approval.json()["route"] is None

    refused = _operator_resolve(client, publication["approval_id"])
    assert refused.status_code == 403, refused.text
    assert ELIGIBILITY_403 in refused.json()["detail"]

    stored = client.get(f"/publications/{publication['id']}", headers=auth_headers)
    assert stored.json()["status"] == "pending"


def test_operator_not_on_the_users_list_gets_membership_403(
    publication_stack: tuple[TestClient, str],
    auth_headers: dict[str, str],
    clean_db: None,
) -> None:
    client, _ = publication_stack
    route = f"operators-{uuid.uuid4().hex[:8]}"
    deployment = _create_deployment_with_routes(
        client, auth_headers, route=route, users=[OTHER]
    )
    payload = _publication_payload(deployment["id"])
    payload["route"] = route
    _, publication = _create_publication(client, payload)

    stored_approval = client.get(
        f"/approvals/{publication['approval_id']}", headers=auth_headers
    )
    assert stored_approval.status_code == 200, stored_approval.text
    assert stored_approval.json()["route"] == route

    refused = _operator_resolve(client, publication["approval_id"])
    assert refused.status_code == 403, refused.text
    detail = refused.json()["detail"]
    assert MEMBERSHIP_403 in detail
    assert ELIGIBILITY_403 not in detail

    stored = client.get(f"/publications/{publication['id']}", headers=auth_headers)
    assert stored.json()["status"] == "pending"


def test_named_unbound_publication_route_refuses_operator_without_channel_membership(
    publication_stack: tuple[TestClient, str],
    auth_headers: dict[str, str],
    clean_db: None,
) -> None:
    client, _ = publication_stack
    deployment = _create_deployment(client, auth_headers)
    payload = _publication_payload(deployment["id"])
    payload["route"] = "missing-route"
    _, publication = _create_publication(client, payload)

    stored_approval = client.get(
        f"/approvals/{publication['approval_id']}", headers=auth_headers
    )
    assert stored_approval.status_code == 200, stored_approval.text
    assert stored_approval.json()["route"] == "missing-route"

    refused = _operator_resolve(client, publication["approval_id"])
    assert refused.status_code == 403, refused.text
    detail = refused.json()["detail"]
    assert UNBOUND_403 in detail
    assert ELIGIBILITY_403 not in detail

    stored = client.get(f"/publications/{publication['id']}", headers=auth_headers)
    assert stored.json()["status"] == "pending"
    audit = client.get(
        f"/approvals/{publication['approval_id']}/audit", headers=auth_headers
    )
    assert audit.status_code == 200, audit.text
    assert audit.json()[-1]["authorizer"] == "UnboundRouteBinding"


def test_operator_principal_resolves_a_cluster_message_publication_after_card_claim(
    publication_stack: tuple[TestClient, str],
    auth_headers: dict[str, str],
    clean_db: None,
) -> None:
    """#2757: a cluster-message card is addressable, an operator principal on an
    explicit ``approvers.users`` route can approve it, and the reconciler can
    then claim the publication to proceed.
    """

    client, _ = publication_stack
    route = f"operators-{uuid.uuid4().hex[:8]}"
    deployment = _create_deployment_with_routes(
        client, auth_headers, route=route, users=[SUBJECT]
    )
    reply_ref = str(uuid.uuid4())
    payload = _publication_payload(deployment["id"], dedupe_key="event-cluster-message-operator")
    payload.update(
        route=route,
        reply_placeholder=reply_ref,
        reply_endpoint=None,
        reply_adapter=CLUSTER_MESSAGE_ADAPTER,
    )
    _, publication = _create_publication(client, payload)

    async def claim_and_ack() -> Any:
        engine = create_async_engine(get_settings().database_url)
        store = PostgresPublicationStore(
            engine, schema="curie", lease_owner="cluster-message-operator-claim"
        )
        try:
            work = await store.claim_pending_card()
            assert work is not None
            await store.mark_card_delivered(work.publication_id)
            return work
        finally:
            await engine.dispose()

    card = asyncio.run(claim_and_ack())
    assert str(card.publication_id) == publication["id"]
    assert card.route.adapter == CLUSTER_MESSAGE_ADAPTER
    assert card.target.reply_ref == reply_ref

    resolved = _operator_resolve(client, publication["approval_id"])
    assert resolved.status_code == 200, resolved.text
    stored = client.get(f"/publications/{publication['id']}", headers=auth_headers)
    assert stored.status_code == 200, stored.text
    assert stored.json()["status"] == "approved"
    audit = client.get(
        f"/approvals/{publication['approval_id']}/audit", headers=auth_headers
    )
    assert audit.status_code == 200, audit.text
    assert audit.json()[0]["principal_kind"] == "operator"
    assert audit.json()[0]["actor"] == SUBJECT

    async def claim_reconcile() -> Any:
        engine = create_async_engine(get_settings().database_url)
        store = PostgresPublicationStore(
            engine, schema="curie", lease_owner="cluster-message-operator-reconcile"
        )
        try:
            return await store.claim_next()
        finally:
            await engine.dispose()

    queued = asyncio.run(claim_reconcile())
    assert queued is not None
    assert str(queued.publication_id) == publication["id"]
