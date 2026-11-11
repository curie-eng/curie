"""Operator resolution of publication approvals that name an audience route.

Pins #2705: a publication that carries a bound `approvers.users` route is
resolvable by an operator principal. Omitting the route stays fail-closed.
Pins #5542: a publication that replies on a tracker shows its card on the
route's fixed Slack channel, so that route's list answers it.
"""

from __future__ import annotations

import asyncio
import time
import uuid
from typing import Any

import channel_protocol
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


def _tracker_publication_payload(
    deployment_id: str,
    *,
    route: str | None = None,
    card_channel: str | None = None,
) -> dict[str, Any]:
    """A publication admitted from a tracker issue, which replies on the repository.

    Each call names its own conversation, so two publications never share a
    lineage. ``card_channel`` is the route's fixed Slack card target (#5542).
    """

    payload = _publication_payload(
        deployment_id, conversation_id=f"issue-{uuid.uuid4().hex[:12]}"
    )
    payload.update(reply_kind="github", reply_channel=REPO, reply_placeholder=None)
    if route is not None:
        payload["route"] = route
    if card_channel is not None:
        payload["card_channel"] = card_channel
    return payload


def _claim_cards(count: int, lease_owner: str) -> list[Any]:
    """Lease ``count`` pending publication cards through the worker's real outbox."""

    async def claim() -> list[Any]:
        engine = create_async_engine(get_settings().database_url)
        store = PostgresPublicationStore(engine, schema="curie", lease_owner=lease_owner)
        try:
            works: list[Any] = []
            for _ in range(count):
                work = await store.claim_pending_card()
                assert work is not None
                works.append(work)
            assert await store.claim_pending_card() is None
            for work in works:
                await store.mark_card_delivered(work.publication_id)
            return works
        finally:
            await engine.dispose()

    return asyncio.run(claim())


def test_operator_resolves_a_tracker_publication_on_its_fixed_slack_card(
    publication_stack: tuple[TestClient, str],
    auth_headers: dict[str, str],
    clean_db: None,
) -> None:
    """#5542: a tracker publication's card is on its route's fixed Slack channel,
    so the route's explicit approver list answers it, not the tracker surface.
    """

    client, _ = publication_stack
    route = f"operators-{uuid.uuid4().hex[:8]}"
    deployment = _create_deployment_with_routes(
        client, auth_headers, route=route, users=[SUBJECT]
    )
    publications = [
        _create_publication(
            client,
            _tracker_publication_payload(
                deployment["id"], route=route, card_channel="C0EXAMPLE1"
            ),
        )[1]
        for _ in range(2)
    ]
    for publication in publications:
        stored_approval = client.get(
            f"/approvals/{publication['approval_id']}", headers=auth_headers
        )
        assert stored_approval.status_code == 200, stored_approval.text
        assert stored_approval.json()["route"] == route
        assert stored_approval.json()["reply_channel"] == REPO
        assert stored_approval.json()["card_channel"] == "C0EXAMPLE1"

    cards = _claim_cards(2, "tracker-publication-card-claim")
    assert {str(card.approval_id) for card in cards} == {
        publication["approval_id"] for publication in publications
    }
    for card in cards:
        assert card.target.kind == "slack"
        assert card.target.address == "C0EXAMPLE1"
        assert card.target.conversation_id is None
        assert card.target.reply_ref is None
        assert card.route.endpoint is None
        assert card.route.adapter is None

    approved, denied = publications
    for publication, decision, outcome in (
        (approved, "approved", "approved"),
        (denied, "rejected", "denied"),
    ):
        resolved = _operator_resolve(client, publication["approval_id"], decision=decision)
        assert resolved.status_code == 200, resolved.text
        audit = client.get(
            f"/approvals/{publication['approval_id']}/audit", headers=auth_headers
        )
        assert audit.status_code == 200, audit.text
        entry = audit.json()[-1]
        assert entry["authorizer"] == "ExplicitUserListAuthorizer"
        assert entry["principal_kind"] == "operator"
        assert entry["actor"] == SUBJECT
        assert entry["authorized"] is True
        stored = client.get(f"/publications/{publication['id']}", headers=auth_headers)
        assert stored.status_code == 200, stored.text
        assert stored.json()["status"] == outcome


def test_unlisted_operator_is_refused_by_the_list_on_a_tracker_publication(
    publication_stack: tuple[TestClient, str],
    auth_headers: dict[str, str],
    clean_db: None,
) -> None:
    client, _ = publication_stack
    route = f"operators-{uuid.uuid4().hex[:8]}"
    deployment = _create_deployment_with_routes(
        client, auth_headers, route=route, users=[OTHER]
    )
    _, publication = _create_publication(
        client,
        _tracker_publication_payload(deployment["id"], route=route, card_channel="C0EXAMPLE1"),
    )

    refused = _operator_resolve(client, publication["approval_id"])
    assert refused.status_code == 403, refused.text
    detail = refused.json()["detail"]
    assert MEMBERSHIP_403 in detail
    assert ELIGIBILITY_403 not in detail
    assert "no approver list that can be verified" not in detail

    audit = client.get(
        f"/approvals/{publication['approval_id']}/audit", headers=auth_headers
    )
    assert audit.status_code == 200, audit.text
    assert audit.json()[-1]["authorizer"] == "ExplicitUserListAuthorizer"
    assert audit.json()[-1]["authorized"] is False
    stored_approval = client.get(
        f"/approvals/{publication['approval_id']}", headers=auth_headers
    )
    assert stored_approval.status_code == 200, stored_approval.text
    assert stored_approval.json()["status"] == "pending"
    stored = client.get(f"/publications/{publication['id']}", headers=auth_headers)
    assert stored.json()["status"] == "pending"


def test_publication_card_channel_requires_a_route(
    publication_stack: tuple[TestClient, str],
    auth_headers: dict[str, str],
    clean_db: None,
) -> None:
    client, _ = publication_stack
    route = f"operators-{uuid.uuid4().hex[:8]}"
    deployment = _create_deployment_with_routes(
        client, auth_headers, route=route, users=[SUBJECT]
    )
    routeless_card = _tracker_publication_payload(deployment["id"], card_channel="C0EXAMPLE1")
    selected = client.post(
        f"/v1/internal/workspaces/{deployment['id']}/selection",
        json={
            "conversation_id": channel_protocol.scoped_conversation_id(
                "github", REPO, routeless_card["conversation_id"]
            ),
            "author": routeless_card["author"],
            "repo_full_name": REPO,
        },
        headers=WORKER_HEADERS,
    )
    assert selected.status_code == 200, selected.text

    refused = client.post(
        "/v1/internal/publications", json=routeless_card, headers=WORKER_HEADERS
    )
    assert refused.status_code == 422, refused.text
    assert "card_channel requires an approval route" in refused.text
    assert _rows("SELECT count(*) AS count FROM curie.approvals") == [{"count": 0}]
    assert _rows("SELECT count(*) AS count FROM curie.publications") == [{"count": 0}]

    # Liveness: without a card target the card stays on the reply route and the
    # tracker surface still has no approver list an operator can satisfy.
    _, publication = _create_publication(
        client, _tracker_publication_payload(deployment["id"])
    )
    stored_approval = client.get(
        f"/approvals/{publication['approval_id']}", headers=auth_headers
    )
    assert stored_approval.status_code == 200, stored_approval.text
    assert stored_approval.json()["route"] is None
    assert stored_approval.json()["card_channel"] == REPO

    unverifiable = _operator_resolve(client, publication["approval_id"])
    assert unverifiable.status_code == 403, unverifiable.text
    assert "github has no approver list that can be verified" in unverifiable.json()["detail"]
    audit = client.get(
        f"/approvals/{publication['approval_id']}/audit", headers=auth_headers
    )
    assert audit.status_code == 200, audit.text
    assert audit.json()[-1]["authorizer"] == "NoVerifiableApprovers"
    stored = client.get(f"/publications/{publication['id']}", headers=auth_headers)
    assert stored.json()["status"] == "pending"


def test_tracker_publication_replay_keeps_its_card_target(
    publication_stack: tuple[TestClient, str],
    auth_headers: dict[str, str],
    clean_db: None,
) -> None:
    """#5542: an exact replay is adopted only when it names the stored card target."""

    client, _ = publication_stack
    route = f"operators-{uuid.uuid4().hex[:8]}"
    deployment = _create_deployment_with_routes(
        client, auth_headers, route=route, users=[SUBJECT]
    )
    payload = _tracker_publication_payload(
        deployment["id"], route=route, card_channel="C0EXAMPLE1"
    )
    payload["dedupe_key"] = f"card-target-{uuid.uuid4().hex[:8]}"
    _, publication = _create_publication(client, payload)

    def stored_cards() -> list[dict[str, Any]]:
        return _rows(
            "SELECT id, card_channel FROM curie.approvals WHERE dedupe_key = :key",
            {"key": payload["dedupe_key"]},
        )

    # Liveness: the identical request is adopted, not duplicated.
    replay = client.post("/v1/internal/publications", json=payload, headers=WORKER_HEADERS)
    assert replay.status_code == 200, replay.text
    assert replay.json()["id"] == publication["id"]
    assert replay.json()["approval_id"] == publication["approval_id"]
    rows = stored_cards()
    assert len(rows) == 1
    assert rows[0]["card_channel"] == "C0EXAMPLE1"

    # Refusal: a different card target, or none (so the reply channel), conflicts.
    other_target = {**payload, "card_channel": "C0EXAMPLE2"}
    no_target = {k: v for k, v in payload.items() if k != "card_channel"}
    for changed in (other_target, no_target):
        refused = client.post(
            "/v1/internal/publications", json=changed, headers=WORKER_HEADERS
        )
        assert refused.status_code == 409, refused.text
        rows = stored_cards()
        assert len(rows) == 1
        assert rows[0]["card_channel"] == "C0EXAMPLE1"
