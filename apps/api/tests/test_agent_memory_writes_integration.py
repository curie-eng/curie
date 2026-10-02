"""Per-agent `memory_writes` round-trips through the API (#1461).

`memory_writes` turns on the runner's remember/update/forget tools for an
agent. It is a plain NOT NULL boolean (default false), so PATCH follows the
`memory` sibling rather than the nullable model/thinking overrides: omitted
leaves it unchanged, a boolean sets it, anything that is not a boolean is 422.
"""

from typing import Any

import pytest


def _create_agent(client: Any, auth_headers: dict[str, str]) -> dict[str, Any]:
    resp = client.post(
        "/agents",
        json={"name": "memory-writes-bot", "channel": {"kind": "slack", "address": "CMEMW0001"}},
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


def test_agent_defaults_to_memory_writes_off(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    agent = _create_agent(client, auth_headers)
    assert agent["memory_writes"] is False
    assert _read(client, auth_headers, agent["id"])["memory_writes"] is False


def test_agent_list_carries_memory_writes(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    agent = _create_agent(client, auth_headers)
    resp = client.get("/agents", headers=auth_headers)
    assert resp.status_code == 200, resp.text
    listed = next(a for a in resp.json() if a["id"] == agent["id"])
    assert listed["memory_writes"] is False


def test_patch_turns_memory_writes_on_and_back_off(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    agent = _create_agent(client, auth_headers)
    resp = _patch(client, auth_headers, agent["id"], {"memory_writes": True})
    assert resp.status_code == 200, resp.text
    assert resp.json()["memory_writes"] is True
    assert _read(client, auth_headers, agent["id"])["memory_writes"] is True

    resp = _patch(client, auth_headers, agent["id"], {"memory_writes": False})
    assert resp.status_code == 200, resp.text
    assert resp.json()["memory_writes"] is False
    assert _read(client, auth_headers, agent["id"])["memory_writes"] is False


def test_patch_omitting_memory_writes_leaves_it_unchanged(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    agent = _create_agent(client, auth_headers)
    assert _patch(client, auth_headers, agent["id"], {"memory_writes": True}).status_code == 200
    resp = _patch(client, auth_headers, agent["id"], {"thinking": "adaptive"})
    assert resp.status_code == 200, resp.text
    assert resp.json()["memory_writes"] is True
    assert _read(client, auth_headers, agent["id"])["memory_writes"] is True


def test_patch_memory_writes_does_not_touch_the_memory_flag(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    """`memory` (per-binding isolation) and `memory_writes` are separate columns."""
    agent = _create_agent(client, auth_headers)
    before = _read(client, auth_headers, agent["id"])["memory"]
    resp = _patch(client, auth_headers, agent["id"], {"memory_writes": True})
    assert resp.status_code == 200, resp.text
    assert resp.json()["memory"] == before


@pytest.mark.parametrize("value", ["sometimes", 2, [], {"on": True}])
def test_patch_rejects_a_non_boolean_and_keeps_the_stored_value(
    client: Any, auth_headers: dict[str, str], clean_db: None, value: Any
) -> None:
    agent = _create_agent(client, auth_headers)
    assert _patch(client, auth_headers, agent["id"], {"memory_writes": True}).status_code == 200
    resp = _patch(client, auth_headers, agent["id"], {"memory_writes": value})
    assert resp.status_code == 422, resp.text
    assert _read(client, auth_headers, agent["id"])["memory_writes"] is True


def test_patch_explicit_null_never_clears_the_not_null_column(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    """The column is NOT NULL: null is either refused or treated as omitted,
    never written (a 500 from the constraint) and never read back as None."""
    agent = _create_agent(client, auth_headers)
    assert _patch(client, auth_headers, agent["id"], {"memory_writes": True}).status_code == 200
    resp = _patch(client, auth_headers, agent["id"], {"memory_writes": None})
    assert resp.status_code in (200, 422), resp.text
    assert _read(client, auth_headers, agent["id"])["memory_writes"] is True
