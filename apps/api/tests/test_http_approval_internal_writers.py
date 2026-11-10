"""Internal approval writers refuse an HTTP-selected scope before any insertion.

Publication, remediation nomination and remediation undo approvals are created
by API code outside public ``/approvals``. Each classifies the agent and every
route candidate exactly as public creation does; a selected or unclassifiable
scope writes no approval row, and a tenant without HTTP selection is unchanged.
Actual ASGI routes, PostgreSQL and Valkey; synthetic inputs only.
@spec HTTP-APPROVAL-CONTINUITY-1.
"""

from __future__ import annotations

import uuid
from pathlib import Path
from typing import Any

import pytest
from _migration_support import sql_dicts, sql_rows
from _sealed_actions import executor_enabled as executor_enabled  # noqa: F401 - fixture
from curie_api.models import TenantIdentityProvider
from principal_adoption_support import adopt, seed
from role_policy_support import run
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from test_publication_lineage_identity import (
    WORKER_HEADERS,
    _create_deployment,
    _publication_payload,
    _workspace_identity,
)
from test_publication_lineage_identity import (
    publication_stack as publication_stack,
)
from test_remediation_approvals import (
    _approvals,
    _nominate,
    _nomination,
    _request,
    _setup,
)
from test_remediation_escalation import (
    _drive,
    _one_escalation,
    _restores,
    _undoable_scenario,
)

pytestmark = pytest.mark.usefixtures("clean_db")
remediation = pytest.mark.usefixtures("executor_enabled", "runs_stream")

CORRUPT = {"mode": "http", "synthetic-private-path": "secret-marker"}


def tenant_in(mode: str) -> uuid.UUID:
    """@spec HTTP-APPROVAL-CONTINUITY-1: a committed tenant that is selected, local or corrupt."""
    created: list[uuid.UUID] = []

    async def body(factory: async_sessionmaker[AsyncSession]) -> None:
        """@spec HTTP-APPROVAL-CONTINUITY-1."""
        tenant, _ = await seed(factory, local=True)
        if mode != "local":
            await adopt(factory, tenant, local=True)
        if mode == "corrupt":
            async with factory() as session, session.begin():
                row = await session.get(TenantIdentityProvider, tenant)
                assert row is not None
                row.http_selection = CORRUPT
        created.append(tenant)

    run(body)
    return created[0]


def move_agent(agent_id: str, tenant: uuid.UUID, *, bindings: bool = False) -> None:
    """@spec HTTP-APPROVAL-CONTINUITY-1: the agent, optionally its route bindings, change tenant."""
    sql_rows(
        "UPDATE curie.agents SET tenant_id = :tenant WHERE id = :agent",
        {"tenant": tenant, "agent": uuid.UUID(agent_id)},
    )
    if bindings:
        sql_rows(
            "UPDATE curie.agent_channels SET tenant_id = :tenant WHERE agent_id = :agent",
            {"tenant": tenant, "agent": uuid.UUID(agent_id)},
        )


# --------------------------------------------------------------------------- #
# Publication approvals
# --------------------------------------------------------------------------- #


def publish(client: Any, deployment: dict[str, Any]) -> tuple[Any, dict[str, Any]]:
    """@spec HTTP-APPROVAL-CONTINUITY-1: the worker's publication request, unchanged."""
    payload = _publication_payload(deployment["id"], conversation_id=f"th-{uuid.uuid4().hex[:8]}")
    selected = client.post(
        f"/v1/internal/workspaces/{payload['deployment_id']}/selection",
        json={
            "conversation_id": _workspace_identity(payload),
            "author": payload["author"],
            "repo_full_name": payload["repo_full_name"],
        },
        headers=WORKER_HEADERS,
    )
    assert selected.status_code == 200, selected.text
    return client.post("/v1/internal/publications", json=payload, headers=WORKER_HEADERS), payload


def stored(dedupe: str) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """@spec HTTP-APPROVAL-CONTINUITY-1: approval and publication rows by the dedupe key."""
    approvals = sql_dicts("SELECT * FROM curie.approvals WHERE dedupe_key = :k", {"k": dedupe})
    publications = sql_dicts(
        "SELECT p.* FROM curie.publications p JOIN curie.approvals a ON a.id = p.approval_id "
        "WHERE a.dedupe_key = :k",
        {"k": dedupe},
    )
    return approvals, publications


def test_publication_for_a_tenant_without_http_selection_is_unchanged(
    publication_stack: tuple[Any, str], auth_headers: dict[str, str],
) -> None:
    """@spec HTTP-APPROVAL-CONTINUITY-1: the pending approval and its publication row exist."""
    client, _ = publication_stack
    deployment = _create_deployment(client, auth_headers)
    move_agent(deployment["agent_id"], tenant_in("local"), bindings=True)

    response, payload = publish(client, deployment)

    assert response.status_code == 201, response.text
    approvals, publications = stored(payload["dedupe_key"])
    assert [row["status"] for row in approvals] == ["pending"]
    assert approvals[0]["purpose"] == "publication"
    assert len(publications) == 1
    assert publications[0]["status"] == "pending"
    assert publications[0]["approval_card_reported_at"] is None


@pytest.mark.parametrize("scope", ["agent", "route"])
def test_publication_for_an_http_selected_scope_refuses_before_insertion(
    publication_stack: tuple[Any, str], auth_headers: dict[str, str], scope: str,
) -> None:
    """@spec HTTP-APPROVAL-CONTINUITY-1: the agent or any route candidate selects HTTP."""
    client, _ = publication_stack
    deployment = _create_deployment(client, auth_headers)
    selected = tenant_in("http")
    if scope == "agent":
        move_agent(deployment["agent_id"], selected)
    else:
        move_agent(deployment["agent_id"], tenant_in("local"))
        sql_rows(
            "UPDATE curie.agent_channels SET tenant_id = :tenant WHERE agent_id = :agent",
            {"tenant": selected, "agent": uuid.UUID(deployment["agent_id"])},
        )

    response, payload = publish(client, deployment)

    assert response.status_code == 409, response.text
    assert response.json()["detail"]["code"] == "publication.unsupported_approval"
    assert stored(payload["dedupe_key"]) == ([], [])
    assert sql_dicts("SELECT id FROM curie.approvals") == []


def test_publication_with_unavailable_classification_refuses_before_insertion(
    publication_stack: tuple[Any, str], auth_headers: dict[str, str],
) -> None:
    """@spec HTTP-APPROVAL-CONTINUITY-1: malformed configuration never selects legacy."""
    client, _ = publication_stack
    deployment = _create_deployment(client, auth_headers)
    move_agent(deployment["agent_id"], tenant_in("corrupt"))

    response, payload = publish(client, deployment)

    assert response.status_code == 503, response.text
    assert response.json() == {"detail": "authority_unavailable"}
    assert stored(payload["dedupe_key"]) == ([], [])


# --------------------------------------------------------------------------- #
# Remediation nomination approvals
# --------------------------------------------------------------------------- #


@remediation
def test_nomination_approval_for_a_tenant_without_http_selection_is_unchanged(
    client: Any, auth_headers: dict[str, str], tmp_path: Path,
) -> None:
    """@spec HTTP-APPROVAL-CONTINUITY-1: the pending row and the nomination link exist."""
    agent_id = _setup(client, auth_headers, tmp_path)
    move_agent(agent_id, tenant_in("local"))
    nomination_id = _nominate(agent_id)

    requested = _request(agent_id, nomination_id)

    assert requested.created is True
    assert [(row["id"], row["status"], row["purpose"]) for row in _approvals()] == [
        (requested.approval_id, "pending", "remediation")
    ]
    nomination = _nomination(nomination_id)
    assert nomination["state"] == "approval_requested"
    assert nomination["approval_id"] == requested.approval_id


@remediation
@pytest.mark.parametrize("mode", ["http", "corrupt"])
def test_nomination_approval_for_a_selected_or_unclassifiable_scope_writes_nothing(
    client: Any, auth_headers: dict[str, str], tmp_path: Path, mode: str,
) -> None:
    """@spec HTTP-APPROVAL-CONTINUITY-1: no row, no request row, nomination left unraised."""
    agent_id = _setup(client, auth_headers, tmp_path)
    move_agent(agent_id, tenant_in(mode))
    nomination_id = _nominate(agent_id)
    before = _nomination(nomination_id)

    with pytest.raises(Exception) as refused:  # noqa: PT011 - the type is asserted below
        _request(agent_id, nomination_id)

    assert type(refused.value).__name__ == "RemediationApprovalUnavailable"
    assert _approvals() == []
    assert sql_dicts("SELECT * FROM curie.remediation_approval_requests") == []
    assert _nomination(nomination_id) == before


# --------------------------------------------------------------------------- #
# Remediation undo approvals
# --------------------------------------------------------------------------- #


@remediation
def test_undo_approval_for_a_tenant_without_http_selection_is_unchanged(
    client: Any, auth_headers: dict[str, str], tmp_path: Path,
) -> None:
    """@spec HTTP-APPROVAL-CONTINUITY-1: the escalation names its pending undo approval."""
    agent_id, nomination_id, _, _ = _undoable_scenario(client, auth_headers, tmp_path)
    move_agent(agent_id, tenant_in("local"))

    _drive(client, nomination_id, "not-recovered")

    escalation = _one_escalation(nomination_id)
    assert escalation["undo_approval_id"] is not None
    assert [(row["id"], row["status"]) for row in _approvals()] == [
        (escalation["undo_approval_id"], "pending")
    ]


@remediation
@pytest.mark.parametrize("mode", ["http", "corrupt"])
def test_undo_approval_for_a_selected_or_unclassifiable_scope_is_not_offered(
    client: Any, auth_headers: dict[str, str], tmp_path: Path, mode: str,
) -> None:
    """@spec HTTP-APPROVAL-CONTINUITY-1: the outcome and report commit; no undo row exists."""
    agent_id, nomination_id, _, action_id = _undoable_scenario(client, auth_headers, tmp_path)
    move_agent(agent_id, tenant_in(mode))

    _drive(client, nomination_id, "not-recovered")

    escalation = _one_escalation(nomination_id)
    assert escalation["action_id"] == action_id
    assert escalation["undo_approval_id"] is None
    assert _approvals() == []
    assert _restores() == []
