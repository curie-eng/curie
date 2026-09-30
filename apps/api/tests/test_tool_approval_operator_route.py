"""Operator resolution of permission-gate (tool) approvals by route (#2722).

The SRE bot's Kubernetes mutations pend on route ``sre-approvals``. These pin the
platform behavior that fix relies on: a tool approval naming a route bound to an
explicit ``approvers.users`` list is resolvable by an operator principal, and the
same approval with no route is refused with the eligibility 403.
"""

from __future__ import annotations

import time
import uuid
from typing import Any

from curie_api import approval_principal
from curie_api.config import get_settings
from fastapi.testclient import TestClient

from apps.api.tests.test_approvals import _payload
from apps.api.tests.test_approvals import approvals_client as approvals_client
from apps.api.tests.test_approvals import runs_stream as runs_stream

SUBJECT = "U0EXAMPLE1"
ROUTE = "sre-approvals"
TOOL = "mcp__kubernetes__pods_delete"
CHANNEL = "C0EXAMPLE1"
ELIGIBILITY_403 = (
    "operator approval principals can resolve only routes bound to an explicit user list"
)


def _agent(client: TestClient, headers: dict[str, str]) -> str:
    created = client.post(
        "/agents",
        json={
            "name": f"sre-bot-{uuid.uuid4().hex[:8]}",
            "channel": {"kind": "slack", "address": CHANNEL},
            "approval_routes": {
                ROUTE: {
                    "resolution": {"kind": "slack", "address": CHANNEL},
                    "approvers": {"users": [SUBJECT]},
                }
            },
        },
        headers=headers,
    )
    assert created.status_code == 201, created.text
    return str(created.json()["id"])


def _tool_approval(
    client: TestClient, headers: dict[str, str], agent_id: str, route: str | None
) -> dict[str, Any]:
    created = client.post(
        "/approvals",
        json=_payload(
            agent_id=agent_id,
            route=route,
            card_channel=CHANNEL,
            gate_kind="permission",
            granted_tool=TOOL,
            summary=f"Tool call awaiting approval: {TOOL}",
        ),
        headers=headers,
    )
    assert created.status_code in (200, 201), created.text
    return created.json()


def _operator_resolve(client: TestClient, approval_id: str) -> Any:
    token = approval_principal.mint(
        get_settings().api_key,
        subject=SUBJECT,
        kind="operator",
        scope=approval_principal.APPROVE_SCOPE,
        exp=int(time.time()) + 60,
    )
    return client.post(
        f"/approvals/{approval_id}/resolve",
        json={"decision": "approved"},
        headers={"X-Curie-Approval-Principal": token},
    )


def test_operator_resolves_a_routed_kubernetes_tool_approval(
    approvals_client: TestClient, auth_headers: dict[str, str], clean_db: None
) -> None:
    agent_id = _agent(approvals_client, auth_headers)
    approval = _tool_approval(approvals_client, auth_headers, agent_id, ROUTE)
    assert approval["route"] == ROUTE

    resolved = _operator_resolve(approvals_client, approval["id"])
    assert resolved.status_code == 200, resolved.text

    stored = approvals_client.get(f"/approvals/{approval['id']}", headers=auth_headers)
    assert stored.status_code == 200, stored.text
    assert stored.json()["status"] == "approved"
    audit = approvals_client.get(f"/approvals/{approval['id']}/audit", headers=auth_headers).json()
    assert audit[-1]["principal_kind"] == "operator"
    assert audit[-1]["actor"] == SUBJECT


def test_routeless_kubernetes_tool_approval_refuses_operator(
    approvals_client: TestClient, auth_headers: dict[str, str], clean_db: None
) -> None:
    agent_id = _agent(approvals_client, auth_headers)
    approval = _tool_approval(approvals_client, auth_headers, agent_id, None)
    assert approval["route"] is None

    refused = _operator_resolve(approvals_client, approval["id"])
    assert refused.status_code == 403, refused.text
    assert ELIGIBILITY_403 in refused.json()["detail"]

    stored = approvals_client.get(f"/approvals/{approval['id']}", headers=auth_headers)
    assert stored.json()["status"] == "pending"
