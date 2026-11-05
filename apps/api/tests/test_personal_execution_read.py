"""The operator read shows each WorkItem's owner tuple and request snapshots (#5376 AC4).

``GET /work-items/{id}`` and ``GET /work-items`` carry ``tenant_id``,
``model_auth_mode``, ``owner_principal_id`` and ``enrollment_generation`` on the
WorkItem and on every request entry. The read happens after the transitions that
could disturb them: a tool approval resolved by a different principal, the end of
the run, a revalidation onto a replaced subscription and a revision from another
requester. The WorkItem shows the rebound generation, the terminal request keeps
the one it ran under, and a service WorkItem shows NULL subscription facts. A
wrong-agent read stays indistinguishable from an unknown id, every response is
``no-store``, and nothing in any body names a grant, token, secret or credential.

The normalized bodies are the shared contract vector
``tests/vectors/work-item-owner-read.json``, which the CLI suite serves to the
real ``curie`` binary. Normalization replaces each distinct UUID, in order of
first appearance (sections in the order ``personal_detail``, ``service_detail``,
``list``; object keys sorted), with ``00000000-0000-4000-8000-%012d`` and every
ISO timestamp, standalone or inside text, with ``2026-01-01T00:00:00Z``. Set
``CURIE_TEST_WORK_ITEM_OWNER_VECTOR_UPDATE=1`` to rewrite the vector.
"""

from __future__ import annotations

import json
import os
import re
from collections.abc import Iterator
from pathlib import Path
from typing import Any
from uuid import uuid4

import personal_execution_support as support
import pytest
from curie_api import workitem_dispatch
from curie_api.config import get_settings
from curie_api.main import create_app
from curie_api.workitems.lifecycle import WorkItemOutcome
from fastapi.testclient import TestClient

WORKER_TOKEN = "personal-read-worker-token"
WORKER_HEADERS = {"X-Curie-Worker-Token": WORKER_TOKEN}
ROUTE = "personal-approvers"
APPROVER = "U0APPROVER1"
PERSONAL_ISSUE = 53781
SERVICE_ISSUE = 53782
SERVICE_CONVERSATION = "1700000000.000200"
REVISION_OBJECTIVE = "Address the follow up comment on the issue"
VECTOR = Path(__file__).resolve().parents[3] / "tests" / "vectors" / "work-item-owner-read.json"
UPDATE_ENV = "CURIE_TEST_WORK_ITEM_OWNER_VECTOR_UPDATE"
SECTIONS = ("personal_detail", "service_detail", "list")
VECTOR_COMMENT = (
    "Normalized operator read bodies for #5376: GET /work-items/{id} for a personal "
    "and a service WorkItem and GET /work-items, after a tool approval by another "
    "principal, the run's end, a revalidation onto generation 2 and a revision. "
    "Each distinct UUID is 00000000-0000-4000-8000-%012d in order of first appearance "
    "(sections personal_detail, service_detail, list; keys sorted); every ISO timestamp "
    "is 2026-01-01T00:00:00Z. Written by apps/api/tests/test_personal_execution_read.py "
    "under CURIE_TEST_WORK_ITEM_OWNER_VECTOR_UPDATE=1; read by cli/tests/work_items.rs."
)
_UUID = re.compile(r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}")
_TIMESTAMP = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:?\d{2})?")
FIXED_TIME = "2026-01-01T00:00:00Z"


@pytest.fixture
def world(clean_db: None) -> Iterator[support.World]:
    support.cleanup_owned()
    yield support.seed_world()
    assert support.cleanup_owned() == 0, "an owned tenant, principal or enrollment survived"


@pytest.fixture
def api(
    world: support.World, runs_stream: str, monkeypatch: pytest.MonkeyPatch
) -> Iterator[TestClient]:
    support.configure_api(monkeypatch, worker_token=WORKER_TOKEN)
    with TestClient(create_app()) as client:
        yield client
    get_settings.cache_clear()


def normalize(document: dict[str, Any]) -> dict[str, Any]:
    placeholders: dict[str, str] = {}

    def placeholder(match: re.Match[str]) -> str:
        key = match.group(0).lower()
        if key not in placeholders:
            placeholders[key] = f"00000000-0000-4000-8000-{len(placeholders) + 1:012d}"
        return placeholders[key]

    def walk(node: Any) -> Any:
        if isinstance(node, dict):
            return {key: walk(node[key]) for key in sorted(node)}
        if isinstance(node, list):
            return [walk(child) for child in node]
        if isinstance(node, str):
            return _TIMESTAMP.sub(FIXED_TIME, _UUID.sub(placeholder, node))
        return node

    return {section: walk(document[section]) for section in SECTIONS}


def _owner_facts(body: dict[str, Any]) -> tuple[Any, Any, Any, Any]:
    return (
        body["tenant_id"],
        body["model_auth_mode"],
        body["owner_principal_id"],
        body["enrollment_generation"],
    )


def _request_facts(body: dict[str, Any]) -> list[tuple[Any, ...]]:
    return [
        (request["sequence"], request["status"], *_owner_facts(request))
        for request in body["requests"]
    ]


def _revalidate_onto_current_enrollment(
    api: TestClient, world: support.World, agent_id: Any, work_item_id: Any
) -> int:
    path = f"{support.INTERNAL_PREFIX}/{work_item_id}/personal-context"
    context = api.get(
        path,
        params={
            "tenant_id": str(world.tenant_id),
            "agent_id": str(agent_id),
            "owner_principal_id": str(world.owner_id),
        },
        headers=WORKER_HEADERS,
    )
    assert context.status_code == 200, context.text
    current = context.json()
    bound = api.post(
        f"{path}/revalidate",
        json={
            "tenant_id": str(world.tenant_id),
            "agent_id": str(agent_id),
            "owner_principal_id": str(world.owner_id),
            "request_id": current["request"]["id"],
            "expected_work_item_version": current["work_item_version"],
            "expected_request_version": current["request"]["version"],
            "expected_enrollment_version": current["enrollment"]["version"],
        },
        headers=WORKER_HEADERS,
    )
    assert bound.status_code == 200, bound.text
    return int(bound.json()["enrollment_generation"])


@pytest.mark.usefixtures("valkey")
def test_operator_read_exposes_the_owner_tuple_after_approval_and_revalidation(
    api: TestClient, world: support.World, auth_headers: dict[str, str]
) -> None:
    agent = support.api_agent(
        api,
        auth_headers,
        tenant_id=world.tenant_id,
        approval_routes={ROUTE: support.users_route([APPROVER])},
    )
    agent_id = agent["agent_id"]
    service = support.admit_service(
        api, WORKER_HEADERS, agent_id, issue_number=SERVICE_ISSUE, conversation=SERVICE_CONVERSATION
    )
    personal = support.admit_personal(
        api, WORKER_HEADERS, world, agent_id, issue_number=PERSONAL_ISSUE
    )
    epoch = support.start_request(personal.request_id)

    # A different principal approves a tool call on the personal run.
    approval = support.tool_approval(api, auth_headers, agent_id=agent_id, route=ROUTE)
    resolved = api.post(
        f"/approvals/{approval['id']}/resolve",
        json={"decision": "approved"},
        headers=support.operator_headers(api, auth_headers, APPROVER),
    )
    assert resolved.status_code == 200, resolved.text
    support.finish_failed(personal.request_id, epoch)

    # The owner's subscription is replaced and the WorkItem revalidated onto it.
    advanced, _ = support.advance_enrollment(world)
    assert _revalidate_onto_current_enrollment(api, world, agent_id, personal.work_item_id) == (
        advanced
    )

    # A revision from another requester runs under the rebound generation.
    revision = support.facts(
        agent_id,
        issue_number=PERSONAL_ISSUE,
        requester=support.OTHER_REQUESTER,
        objective=REVISION_OBJECTIVE,
    )
    revised = support.with_session(
        lambda session: workitem_dispatch.admit_revision(session, revision)
    )
    assert isinstance(revised, WorkItemOutcome), revised
    assert revised.request is not None and revised.request.status == "waiting"

    personal_detail = api.get(f"/work-items/{personal.work_item_id}", headers=auth_headers)
    service_detail = api.get(f"/work-items/{service.work_item_id}", headers=auth_headers)
    listed = api.get("/work-items", headers=auth_headers)
    for response in (personal_detail, service_detail, listed):
        assert response.status_code == 200, response.text
        assert response.headers["cache-control"] == "no-store"
        lowered = response.text.lower()
        for fragment in support.FORBIDDEN_FRAGMENTS:
            assert fragment not in lowered, (fragment, response.text)
    personal_body = personal_detail.json()
    service_body = service_detail.json()
    list_body = listed.json()
    for body in (personal_body, service_body, list_body):
        assert support.credential_material(body) == []

    tenant, owner = str(world.tenant_id), str(world.owner_id)
    assert _owner_facts(personal_body) == (tenant, "personal", owner, advanced)
    assert _request_facts(personal_body) == [
        (1, "failed", tenant, "personal", owner, world.generation),
        (2, "waiting", tenant, "personal", owner, advanced),
    ]
    # The newest requester is not the owner.
    assert personal_body["requester"] == support.OTHER_REQUESTER
    assert _owner_facts(service_body) == (tenant, "service", None, None)
    assert _request_facts(service_body) == [(1, "waiting", tenant, "service", None, None)]

    items = {item["id"]: item for item in list_body["items"]}
    assert set(items) == {str(personal.work_item_id), str(service.work_item_id)}
    for detail in (personal_body, service_body):
        listed_item = items[detail["id"]]
        assert listed_item["ci"] is None
        assert {**listed_item, "ci": detail["ci"]} == detail

    # A wrong-agent read is indistinguishable from an unknown id.
    unknown = api.get(f"/work-items/{uuid4()}", headers=auth_headers)
    mismatched = api.get(
        f"/work-items/{personal.work_item_id}",
        params={"agent_id": str(world.foreign_agent_id)},
        headers=auth_headers,
    )
    assert (unknown.status_code, mismatched.status_code) == (404, 404)
    assert mismatched.content == unknown.content
    assert mismatched.headers["cache-control"] == "no-store"
    matched = api.get(
        f"/work-items/{personal.work_item_id}",
        params={"agent_id": str(agent_id)},
        headers=auth_headers,
    )
    assert matched.status_code == 200, matched.text
    assert matched.json() == personal_body

    normalized = normalize(
        {"personal_detail": personal_body, "service_detail": service_body, "list": list_body}
    )
    if os.environ.get(UPDATE_ENV) == "1":
        VECTOR.write_text(json.dumps({"_comment": VECTOR_COMMENT, **normalized}, indent=2) + "\n")
    expected = json.loads(VECTOR.read_text())
    assert set(expected) == {"_comment", *SECTIONS}
    assert {section: expected[section] for section in SECTIONS} == normalized
