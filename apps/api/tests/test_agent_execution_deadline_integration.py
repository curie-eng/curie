"""Per-agent execution deadline round-trips through the API (#3071).

Same three-way PATCH semantics as `model` and `thinking`: omitted leaves it
unchanged, explicit null clears it to the 1800 s platform default, an integer
sets it. The API bounds it to 60..10800 seconds.
"""

from typing import Any

import pytest


def _create_agent(client: Any, auth_headers: dict[str, str]) -> dict[str, Any]:
    resp = client.post(
        "/agents",
        json={"name": "deadline-bot", "channel": {"kind": "slack", "address": "CDEAD0001"}},
        headers=auth_headers,
    )
    assert resp.status_code == 201, resp.text
    return resp.json()  # type: ignore[no-any-return]


def _patch(client: Any, auth_headers: dict[str, str], agent_id: str, body: dict[str, Any]) -> Any:
    return client.patch(f"/agents/{agent_id}", json=body, headers=auth_headers)


def _read(client: Any, auth_headers: dict[str, str], agent_id: str) -> dict[str, Any]:
    resp = client.get(f"/agents/{agent_id}", headers=auth_headers)
    assert resp.status_code == 200, resp.text
    return resp.json()  # type: ignore[no-any-return]


def test_agent_defaults_to_null_execution_deadline(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    agent = _create_agent(client, auth_headers)
    assert agent["execution_deadline_seconds"] is None
    assert _read(client, auth_headers, agent["id"])["execution_deadline_seconds"] is None


def test_patch_sets_and_explicit_null_clears_execution_deadline(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    agent = _create_agent(client, auth_headers)
    resp = _patch(client, auth_headers, agent["id"], {"execution_deadline_seconds": 90})
    assert resp.status_code == 200, resp.text
    assert resp.json()["execution_deadline_seconds"] == 90
    assert _read(client, auth_headers, agent["id"])["execution_deadline_seconds"] == 90

    resp = _patch(client, auth_headers, agent["id"], {"execution_deadline_seconds": None})
    assert resp.status_code == 200, resp.text
    assert resp.json()["execution_deadline_seconds"] is None
    assert _read(client, auth_headers, agent["id"])["execution_deadline_seconds"] is None


def test_patch_omitting_execution_deadline_leaves_it_unchanged(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    agent = _create_agent(client, auth_headers)
    assert _patch(
        client, auth_headers, agent["id"], {"execution_deadline_seconds": 90}
    ).status_code == 200
    resp = _patch(client, auth_headers, agent["id"], {"thinking": "adaptive"})
    assert resp.status_code == 200, resp.text
    assert resp.json()["execution_deadline_seconds"] == 90
    assert _read(client, auth_headers, agent["id"])["execution_deadline_seconds"] == 90


@pytest.mark.parametrize("value", [60, 10800])
def test_patch_accepts_the_bounds(
    client: Any, auth_headers: dict[str, str], clean_db: None, value: int
) -> None:
    agent = _create_agent(client, auth_headers)
    resp = _patch(client, auth_headers, agent["id"], {"execution_deadline_seconds": value})
    assert resp.status_code == 200, resp.text
    assert resp.json()["execution_deadline_seconds"] == value


@pytest.mark.parametrize("value", [59, 10801, 0, -1])
def test_patch_rejects_out_of_range_and_keeps_the_stored_value(
    client: Any, auth_headers: dict[str, str], clean_db: None, value: int
) -> None:
    agent = _create_agent(client, auth_headers)
    assert _patch(
        client, auth_headers, agent["id"], {"execution_deadline_seconds": 90}
    ).status_code == 200
    resp = _patch(client, auth_headers, agent["id"], {"execution_deadline_seconds": value})
    assert resp.status_code == 422, resp.text
    assert _read(client, auth_headers, agent["id"])["execution_deadline_seconds"] == 90
