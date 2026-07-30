"""Behavior-packs config round-trips through the API against real Postgres.

Create-with-packs, GET default (null -> all-off), PUT then GET back. The worker
reads this same JSON at bind time; the API's only job is to store and return it.
"""

from typing import Any


def _create_agent(client: Any, auth_headers: dict[str, str], **body: Any) -> dict[str, Any]:
    resp = client.post(
        "/agents",
        json={"name": "packs-bot", "slack_channel": "C000PACKS1", **body},
        headers=auth_headers,
    )
    assert resp.status_code == 201, resp.text
    return resp.json()  # type: ignore[no-any-return]


def test_agent_defaults_to_no_packs(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    agent = _create_agent(client, auth_headers)
    assert agent["behavior_packs"] is None

    # GET reads null as the all-off default rather than 404/empty.
    resp = client.get(f"/agents/{agent['id']}/behavior-packs", headers=auth_headers)
    assert resp.status_code == 200, resp.text
    packs = resp.json()
    assert packs["tips"]["enabled"] is False
    assert packs["greeting"]["enabled"] is False


def test_create_with_packs_persists(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    agent = _create_agent(
        client,
        auth_headers,
        behavior_packs={
            "greeting": {"enabled": True, "phrases": ["hi"], "reply": "yo"},
        },
    )
    assert agent["behavior_packs"]["greeting"]["reply"] == "yo"

    resp = client.get(f"/agents/{agent['id']}/behavior-packs", headers=auth_headers)
    assert resp.status_code == 200, resp.text
    assert resp.json()["greeting"]["phrases"] == ["hi"]


def test_put_then_get_round_trips(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    agent = _create_agent(client, auth_headers)
    config = {
        "load": {"enabled": True, "lines": ["Working on it!"]},
        "tips": {"enabled": True, "tips": ["Ask me for the top 5"]},
        "greeting": {"enabled": True, "phrases": ["hey"], "reply": "hello!"},
        "help": {"enabled": True, "phrases": ["help"], "reply": "here is help"},
        "settings": {
            "enabled": True,
            "settings": [
                {"key": "page_size", "kind": "int", "default": "5"},
            ],
        },
        "nav": {"enabled": True, "hub_label": "Help", "hub_command": "help"},
    }
    resp = client.put(f"/agents/{agent['id']}/behavior-packs", json=config, headers=auth_headers)
    assert resp.status_code == 200, resp.text

    resp = client.get(f"/agents/{agent['id']}/behavior-packs", headers=auth_headers)
    assert resp.status_code == 200, resp.text
    got = resp.json()
    assert got["load"]["lines"] == ["Working on it!"]
    assert got["tips"]["tips"] == ["Ask me for the top 5"]
    assert got["greeting"]["reply"] == "hello!"
    assert got["help"]["reply"] == "here is help"
    assert got["settings"]["settings"][0]["key"] == "page_size"
    assert got["nav"]["hub_command"] == "help"

    # And it surfaces on the agent read model too.
    resp = client.get(f"/agents/{agent['id']}", headers=auth_headers)
    assert resp.json()["behavior_packs"]["tips"]["enabled"] is True
