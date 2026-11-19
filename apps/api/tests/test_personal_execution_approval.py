"""Approvals on a personal run change approval facts only (#5376 AC5).

ADR 0268 rows 4 and 12: an authorized principal other than the execution owner
may resolve an approval raised by a personal WorkItem's running request, and that
never moves the WorkItem's owner tuple or bound generation. A correctly
authenticated operator outside the route's explicit ``approvers.users`` list gets
403: the approval stays pending, nothing is enqueued on the runs stream, the
publication stays pending and no write credential is redeemable. An authorized
rejection stays rejected and never authorizes publication. The durable owner
tuple of the WorkItem and every request is read back after each control.

Every operator credential is minted by the real platform-key route
``POST /approvals/principals/operator``; resolution runs through the real
``/approvals/{id}/resolve`` route, Postgres and Valkey, with no mocked
authorization or queue. The stack and publication helpers are the publication
suite's own.
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any
from uuid import UUID

import personal_execution_support as support
import pytest
from curie_api import workitem_dispatch
from curie_api.workitem_dispatch import HeartbeatResult
from fastapi.testclient import TestClient

from apps.api.tests.test_publications import (
    WORKER_HEADERS,
    _create_publication,
    _publication_payload,
    _rows,
    _stream_entries,
)
from apps.api.tests.test_publications import publication_stack as publication_stack

ROUTE = "personal-approvers"
APPROVER = "U0APPROVER1"
UNLISTED = "U0UNLISTED1"
MEMBERSHIP_403 = "explicit list of approvers"
ELIGIBILITY_403 = (
    "operator approval principals can resolve only routes bound to an explicit user list"
)
FIRST_CONVERSATION = "1700000000.000100"
SECOND_CONVERSATION = "1700000000.000300"


@pytest.fixture
def world(clean_db: None) -> Iterator[support.World]:
    support.cleanup_owned()
    yield support.seed_world()
    assert support.cleanup_owned() == 0, "an owned tenant, principal or enrollment survived"


def _resolve(
    client: TestClient,
    auth_headers: dict[str, str],
    approval_id: str,
    *,
    subject: str,
    decision: str = "approved",
) -> Any:
    return client.post(
        f"/approvals/{approval_id}/resolve",
        json={"decision": decision},
        headers=support.operator_headers(client, auth_headers, subject),
    )


def _assert_membership_refusal(response: Any) -> None:
    assert response.status_code == 403, response.text
    detail = response.json()["detail"]
    assert MEMBERSHIP_403 in detail
    assert ELIGIBILITY_403 not in detail


def _status(client: TestClient, auth_headers: dict[str, str], path: str) -> str:
    response = client.get(path, headers=auth_headers)
    assert response.status_code == 200, response.text
    return str(response.json()["status"])


def _assert_no_credential(client: TestClient, publication_id: str) -> None:
    refused = client.post(
        f"/v1/internal/publications/{publication_id}/credential", headers=WORKER_HEADERS
    )
    assert refused.status_code == 409, refused.text
    assert refused.headers["cache-control"] == "no-store"
    assert "approved" in str(refused.json()["detail"]).lower()


def _personal_run(
    client: TestClient,
    world: support.World,
    agent_id: UUID,
    *,
    issue_number: int,
    conversation: str = FIRST_CONVERSATION,
) -> tuple[support.Admitted, int]:
    admitted = support.admit_personal(
        client,
        WORKER_HEADERS,
        world,
        agent_id,
        issue_number=issue_number,
        conversation=conversation,
    )
    support.assert_owner_tuples(
        admitted.work_item_id,
        work_item=world.personal(),
        requests={admitted.request_id: world.personal()},
    )
    return admitted, support.start_request(admitted.request_id)


def test_tool_approval_refuses_an_unlisted_operator_and_resumes_for_a_listed_one(
    world: support.World,
    publication_stack: tuple[TestClient, str],
    auth_headers: dict[str, str],
) -> None:
    client, runs_stream = publication_stack
    agent = support.api_agent(
        client,
        auth_headers,
        tenant_id=world.tenant_id,
        approval_routes={ROUTE: support.users_route([APPROVER])},
    )
    run, epoch = _personal_run(client, world, agent["agent_id"], issue_number=53791)
    held = support.with_session(
        lambda session: workitem_dispatch.hold_for_approval(
            session, run.request_id, runtime_epoch=epoch
        )
    )
    assert isinstance(held, HeartbeatResult), held
    owned = {"work_item": world.personal(), "requests": {run.request_id: world.personal()}}
    approval = support.tool_approval(client, auth_headers, agent_id=agent["agent_id"], route=ROUTE)
    approval_path = f"/approvals/{approval['id']}"

    # Correctly authenticated, not on the route's list: refused, nothing moves.
    _assert_membership_refusal(_resolve(client, auth_headers, approval["id"], subject=UNLISTED))
    assert _status(client, auth_headers, approval_path) == "pending"
    assert _stream_entries(runs_stream) == []
    support.assert_owner_tuples(run.work_item_id, **owned)

    # Listed, and a different principal from the owner: resolves and resumes once.
    approved = _resolve(client, auth_headers, approval["id"], subject=APPROVER)
    assert approved.status_code == 200, approved.text
    assert (approved.json()["status"], approved.json()["resolved_by"]) == ("approved", APPROVER)
    turns = support.stream_turns(runs_stream)
    assert [(turn.event_id, turn.author) for turn in turns] == [
        (f"approval-{approval['id']}-resolved", APPROVER)
    ]
    audit = client.get(f"{approval_path}/audit", headers=auth_headers)
    assert audit.status_code == 200, audit.text
    assert [
        (entry["action"], entry["principal_kind"], entry["actor"], entry["authorized"])
        for entry in audit.json()
    ] == [
        ("denied", "operator", UNLISTED, False),
        ("resolved", "operator", APPROVER, True),
    ]
    support.assert_owner_tuples(run.work_item_id, **owned)
    assert support.request_column(run.request_id, "status") == "running"


def test_publication_approvals_on_a_personal_run_never_move_the_owner(
    world: support.World,
    publication_stack: tuple[TestClient, str],
    auth_headers: dict[str, str],
) -> None:
    client, runs_stream = publication_stack
    agent = support.api_agent(
        client,
        auth_headers,
        tenant_id=world.tenant_id,
        approval_routes={ROUTE: support.users_route([APPROVER])},
        deployment=True,
    )
    rejected_run, _ = _personal_run(client, world, agent["agent_id"], issue_number=53792)
    approved_run, _ = _personal_run(
        client,
        world,
        agent["agent_id"],
        issue_number=53793,
        conversation=SECOND_CONVERSATION,
    )
    runs = (rejected_run, approved_run)

    def assert_owners_unchanged() -> None:
        for run in runs:
            support.assert_owner_tuples(
                run.work_item_id,
                work_item=world.personal(),
                requests={run.request_id: world.personal()},
            )

    payload = _publication_payload(agent["deployment_id"], conversation_id=FIRST_CONVERSATION)
    payload["route"] = ROUTE
    _, publication = _create_publication(client, payload)
    assert _rows(
        "SELECT execution_request_id FROM curie.publications WHERE id = :id",
        {"id": UUID(publication["id"])},
    ) == [{"execution_request_id": rejected_run.request_id}]
    approval_path = f"/approvals/{publication['approval_id']}"
    publication_path = f"/publications/{publication['id']}"

    # The unlisted operator: refused, pending, no credential, no enqueue.
    _assert_membership_refusal(
        _resolve(client, auth_headers, publication["approval_id"], subject=UNLISTED)
    )
    assert _status(client, auth_headers, approval_path) == "pending"
    assert _status(client, auth_headers, publication_path) == "pending"
    _assert_no_credential(client, publication["id"])
    assert _stream_entries(runs_stream) == []
    assert_owners_unchanged()

    # An authorized rejection stays rejected and never authorizes publication.
    rejected = _resolve(
        client, auth_headers, publication["approval_id"], subject=APPROVER, decision="rejected"
    )
    assert rejected.status_code == 200, rejected.text
    assert _status(client, auth_headers, approval_path) == "rejected"
    assert _status(client, auth_headers, publication_path) == "denied"
    _assert_no_credential(client, publication["id"])
    assert _stream_entries(runs_stream) == []
    assert _rows(
        "SELECT count(*) AS count FROM curie.publications "
        "WHERE status IN ('approved', 'launching', 'running')"
    ) == [{"count": 0}]
    assert_owners_unchanged()

    # An authorized approval of the other run's publication approves it alone.
    second = _publication_payload(agent["deployment_id"], conversation_id=SECOND_CONVERSATION)
    second["route"] = ROUTE
    _, other = _create_publication(client, second)
    assert _rows(
        "SELECT execution_request_id FROM curie.publications WHERE id = :id",
        {"id": UUID(other["id"])},
    ) == [{"execution_request_id": approved_run.request_id}]
    approved = _resolve(client, auth_headers, other["approval_id"], subject=APPROVER)
    assert approved.status_code == 200, approved.text
    assert _status(client, auth_headers, f"/approvals/{other['approval_id']}") == "approved"
    assert _status(client, auth_headers, f"/publications/{other['id']}") == "approved"
    assert _status(client, auth_headers, publication_path) == "denied"
    # Publication outcomes are reported by the platform worker, never a resumed turn.
    assert _stream_entries(runs_stream) == []
    assert_owners_unchanged()
    for run in runs:
        assert support.request_column(run.request_id, "status") == "running"
