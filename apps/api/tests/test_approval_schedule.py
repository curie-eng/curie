"""Route deadline persistence through the operator and creation boundaries."""

from datetime import datetime
from typing import Any

import pytest
from curie_api.schemas.approvals import ApprovalRouteBinding, ApprovalRouteBindingOut
from pydantic import ValidationError


def binding(**fields: Any) -> dict[str, Any]:
    return {"resolution": {"kind": "slack", "address": "C0EXAMPLE1"}, **fields}


@pytest.mark.parametrize("value", [None, 1, 3600])
def test_explicit_deadline_survives_every_route_projection(value: int | None) -> None:
    raw = binding(expires_in_seconds=value, notification=None, approvers=None)
    written = ApprovalRouteBinding.model_validate(raw).model_dump()
    assert written == binding(expires_in_seconds=value)
    assert (
        ApprovalRouteBindingOut.model_validate(written).model_dump()["expires_in_seconds"] == value
    )


def test_omitted_deadline_stays_omitted_on_read_write() -> None:
    written = ApprovalRouteBinding.model_validate(binding()).model_dump()
    assert written == binding()
    assert "expires_in_seconds" not in ApprovalRouteBindingOut.model_validate(written).model_dump()


@pytest.mark.parametrize("value", [True, False, 0, -1, 1.5, 5.0, "5"])
def test_route_deadline_refuses_non_positive_or_non_integer_values(value: Any) -> None:
    with pytest.raises(ValidationError):
        ApprovalRouteBinding.model_validate(binding(expires_in_seconds=value))


@pytest.mark.parametrize(
    "override", [{}, {"expires_in_seconds": 3600}, {"expires_in_seconds": None}]
)
def test_operator_deadline_round_trip(
    client: Any, headers: dict[str, str], clean_db: None, override: dict[str, Any]
) -> None:
    created = client.post(
        "/agents",
        headers=headers,
        json={
            "name": "acme-schedule",
            "channel": {"kind": "slack", "address": "C0EXAMPLE0"},
            "approval_routes": {"review": binding(**override)},
        },
    )
    assert created.status_code == 201, created.text
    agent_id = created.json()["id"]
    assert {
        k: v
        for k, v in created.json()["approval_routes"]["review"].items()
        if k == "expires_in_seconds"
    } == override
    updated = client.patch(
        f"/agents/{agent_id}",
        headers=headers,
        json={
            "approval_routes": {"review": binding(**override, notification=None, approvers=None)},
        },
    )
    assert updated.status_code == 200, updated.text
    read = client.get(f"/agents/{agent_id}", headers=headers).json()["approval_routes"]["review"]
    assert {k: v for k, v in read.items() if k == "expires_in_seconds"} == override


@pytest.mark.parametrize(
    "override,producer",
    [({"expires_in_seconds": 3600}, 7), ({"expires_in_seconds": None}, 7), ({}, 23), ({}, None)],
)
def test_legacy_creation_freezes_route_deadline(
    client: Any,
    headers: dict[str, str],
    clean_db: None,
    override: dict[str, Any],
    producer: int | None,
) -> None:
    created = client.post(
        "/agents",
        headers=headers,
        json={
            "name": "acme-schedule",
            "channel": {"kind": "slack", "address": "C0EXAMPLE0"},
            "approval_routes": {"review": binding(**override)},
        },
    )
    assert created.status_code == 201, created.text
    agent_id = created.json()["id"]
    request = {
        "agent_id": agent_id,
        "conversation_id": "schedule-test",
        "author": "U0EXAMPLE1",
        "summary": "Approve bounded work",
        "reply_kind": "slack",
        "reply_channel": "C0EXAMPLE0",
        "reply_placeholder": None,
        "dedupe_key": "schedule-create",
        "route": "review",
        "card_channel": "C0EXAMPLE1",
        "expires_in_seconds": producer,
    }
    response = client.post("/approvals", headers=headers, json=request)
    assert response.status_code == 201, response.text
    row = client.get(f"/approvals/{response.json()['id']}", headers=headers).json()
    duration = override.get("expires_in_seconds", producer)
    if duration is None:
        assert row["expires_at"] is None
    else:
        assert (
            datetime.fromisoformat(row["expires_at"]) - datetime.fromisoformat(row["created_at"])
        ).total_seconds() == duration
    changed = client.patch(
        f"/agents/{agent_id}",
        headers=headers,
        json={"approval_routes": {"review": binding(expires_in_seconds=100)}},
    )
    assert changed.status_code == 200, changed.text
    replay = client.post("/approvals", headers=headers, json=request)
    assert replay.status_code == 200, replay.text
    assert replay.json()["id"] == row["id"]
    assert (
        client.get(f"/approvals/{row['id']}", headers=headers).json()["expires_at"]
        == row["expires_at"]
    )
