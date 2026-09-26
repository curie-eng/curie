"""A binding move that changes kind takes the new kind's route shape (ADR-0168 decision 3).

`agent_channels_route_ck` gives a Slack row an identity and no endpoint, and
any other kind both or neither, so a move across that line cannot keep the
stored route.
"""

from __future__ import annotations

from typing import Any

from curie_api.routers.agents import classify_integrity_error
from sqlalchemy.exc import IntegrityError


def _create(client: Any, headers: dict[str, str], channel: dict[str, Any]) -> str:
    resp = client.post("/agents", json={"name": "mover", "channel": channel}, headers=headers)
    assert resp.status_code == 201, resp.text
    return str(resp.json()["id"])


def test_moving_a_slack_binding_to_another_kind_leaves_it_route_less(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    aid = _create(client, auth_headers, {"kind": "slack", "address": "C0EXAMPLE1"})
    moved = client.patch(
        f"/agents/{aid}/channels",
        params={"kind": "slack", "address": "C0EXAMPLE1"},
        json={"kind": "webhook", "address": "room-1"},
        headers=auth_headers,
    )
    assert moved.status_code == 200, moved.text
    assert moved.json()["channels"] == [{"kind": "webhook", "address": "room-1", "adapter": None}]


def test_moving_a_routed_binding_to_slack_names_the_default_identity(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    aid = _create(
        client,
        auth_headers,
        {
            "kind": "email",
            "address": "ops@example.com",
            "endpoint": "https://mail.example.test/",
            "adapter": "agentmail",
        },
    )
    moved = client.patch(
        f"/agents/{aid}/channels",
        params={"kind": "email", "address": "ops@example.com"},
        json={"kind": "slack", "address": "C0EXAMPLE2"},
        headers=auth_headers,
    )
    assert moved.status_code == 200, moved.text
    assert moved.json()["channels"] == [
        {"kind": "slack", "address": "C0EXAMPLE2", "adapter": "default"}
    ]


def test_a_same_kind_move_keeps_the_stored_route(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    aid = _create(
        client,
        auth_headers,
        {
            "kind": "email",
            "address": "ops@example.com",
            "endpoint": "https://mail.example.test/",
            "adapter": "agentmail",
        },
    )
    moved = client.patch(
        f"/agents/{aid}/channels",
        params={"kind": "email", "address": "ops@example.com"},
        json={"kind": "email", "address": "desk@example.com"},
        headers=auth_headers,
    )
    assert moved.status_code == 200, moved.text
    assert moved.json()["channels"] == [
        {"kind": "email", "address": "desk@example.com", "adapter": "agentmail"}
    ]


def test_a_check_violation_is_a_server_fault_again() -> None:
    class _Driver(Exception):
        sqlstate = "23514"
        constraint_name = "agent_channels_route_ck"

    assert classify_integrity_error(IntegrityError("stmt", {}, _Driver())) is None
