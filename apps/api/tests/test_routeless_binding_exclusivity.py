"""A route-less non-Slack binding holds its pair alone across agents (ADR-0168 decision 3).

Migration 0070's key is the triple, `UNIQUE NULLS NOT DISTINCT (kind, address,
adapter)`, so `(email, x, NULL)` and `(email, x, 'inbox-a')` are different
keys. But a turn from a route-less binding carries no adapter, and an omitted
non-Slack adapter selects every route on the pair (`matching_routes`), so the
worker would find both agents' rows and run one of them under the other's
deployment. Under 0023's pair key the second agent could not bind the pair at
all; these tests hold that exclusivity for the route-less case, and only across
agents: two routed bindings under different adapters stay legal, and one agent
holding both shapes is governed by the per-agent rules it always was.
"""

from __future__ import annotations

from typing import Any

from fastapi.testclient import TestClient

EMAIL = "ops@example.com"
OTHER = "other@example.com"

ROUTELESS_BESIDE_ROUTED = (
    f"another agent holds a route on email:{EMAIL}; a binding with no adapter answers "
    "every route on that pair, so it would take that agent's turns. Bind this one with "
    "its own endpoint and adapter, move or delete the other agent, or pick another address"
)
ROUTED_BESIDE_ROUTELESS = (
    f"another agent is bound to email:{EMAIL} with no adapter, which answers every route "
    "on that pair; give that binding its own endpoint and adapter first, move or delete "
    "the other agent, or pick another address"
)


def _routed(adapter: str, address: str = EMAIL) -> dict[str, str]:
    return {
        "kind": "email",
        "address": address,
        "endpoint": f"https://{adapter}.example.test/",
        "adapter": adapter,
    }


def _routeless(address: str = EMAIL) -> dict[str, str]:
    return {"kind": "email", "address": address}


def _create(client: TestClient, headers: dict[str, str], name: str, channel: dict[str, Any]) -> Any:
    return client.post("/agents", json={"name": name, "channel": channel}, headers=headers)


def _created(
    client: TestClient, headers: dict[str, str], name: str, channel: dict[str, Any]
) -> str:
    resp = _create(client, headers, name, channel)
    assert resp.status_code == 201, resp.text
    return str(resp.json()["id"])


def test_a_routeless_binding_beside_another_agents_route_is_refused(
    hooks_client: TestClient, auth_headers: dict[str, str], clean_db: None
) -> None:
    """The review's probe: B's route-less create was a 201, and B's hook turn
    then resolved to A's deployment through A's route."""

    _created(hooks_client, auth_headers, "inbox-a", _routed("inbox-a"))

    b = _create(hooks_client, auth_headers, "inbox-b", _routeless())

    assert b.status_code == 409, b.text
    assert b.json()["detail"] == ROUTELESS_BESIDE_ROUTED


def test_a_route_beside_another_agents_routeless_binding_is_refused(
    hooks_client: TestClient, auth_headers: dict[str, str], clean_db: None
) -> None:
    _created(hooks_client, auth_headers, "inbox-b", _routeless())

    a = _create(hooks_client, auth_headers, "inbox-a", _routed("inbox-a"))

    assert a.status_code == 409, a.text
    assert a.json()["detail"] == ROUTED_BESIDE_ROUTELESS


def test_adding_a_routeless_binding_beside_another_agents_route_is_refused(
    hooks_client: TestClient, auth_headers: dict[str, str], clean_db: None
) -> None:
    _created(hooks_client, auth_headers, "inbox-a", _routed("inbox-a"))
    b = _created(hooks_client, auth_headers, "inbox-b", _routeless(OTHER))

    added = hooks_client.post(f"/agents/{b}/channels", json=_routeless(), headers=auth_headers)

    assert added.status_code == 409, added.text
    assert added.json()["detail"] == ROUTELESS_BESIDE_ROUTED


def test_moving_a_routeless_binding_beside_another_agents_route_is_refused(
    hooks_client: TestClient, auth_headers: dict[str, str], clean_db: None
) -> None:
    _created(hooks_client, auth_headers, "inbox-a", _routed("inbox-a"))
    b = _created(hooks_client, auth_headers, "inbox-b", _routeless(OTHER))

    moved = hooks_client.patch(
        f"/agents/{b}/channels",
        params={"kind": "email", "address": OTHER},
        json=_routeless(),
        headers=auth_headers,
    )

    assert moved.status_code == 409, moved.text
    assert moved.json()["detail"] == ROUTELESS_BESIDE_ROUTED


def test_moving_a_route_beside_another_agents_routeless_binding_is_refused(
    hooks_client: TestClient, auth_headers: dict[str, str], clean_db: None
) -> None:
    _created(hooks_client, auth_headers, "inbox-b", _routeless())
    a = _created(hooks_client, auth_headers, "inbox-a", _routed("inbox-a", OTHER))

    moved = hooks_client.patch(
        f"/agents/{a}/channels",
        params={"kind": "email", "address": OTHER, "adapter": "inbox-a"},
        json={"kind": "email", "address": EMAIL},
        headers=auth_headers,
    )

    assert moved.status_code == 409, moved.text
    assert moved.json()["detail"] == ROUTED_BESIDE_ROUTELESS


def test_two_agents_routed_under_different_adapters_share_a_pair(
    hooks_client: TestClient, auth_headers: dict[str, str], clean_db: None
) -> None:
    """Pin: each turn names its adapter, so neither route answers the other's."""

    _created(hooks_client, auth_headers, "inbox-a", _routed("inbox-a"))

    b = _create(hooks_client, auth_headers, "inbox-b", _routed("inbox-b"))

    assert b.status_code == 201, b.text


def test_one_agent_may_hold_a_routeless_binding_beside_its_own_route(
    hooks_client: TestClient, auth_headers: dict[str, str], clean_db: None
) -> None:
    """Pin: within one agent a route-less turn cannot reach another agent's
    deployment, and the per-agent readers already answer the ambiguity."""

    a = _created(hooks_client, auth_headers, "inbox-a", _routed("inbox-a"))

    added = hooks_client.post(f"/agents/{a}/channels", json=_routeless(), headers=auth_headers)

    assert added.status_code == 201, added.text
