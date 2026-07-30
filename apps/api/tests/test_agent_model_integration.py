"""Per-agent model selection round-trips through the API against real Postgres.

The `model` column is forwarded as CURIE_MODEL at sandbox boot by the worker
(#254); the API's job is to store, expose, and update it. Create-with-model, the
null default, PATCH-to-set, and PATCH-leaves-unchanged.
"""

from typing import Any


def _create_agent(client: Any, auth_headers: dict[str, str], **body: Any) -> dict[str, Any]:
    resp = client.post(
        "/agents",
        json={"name": "model-bot", "slack_channel": "CMODEL001", **body},
        headers=auth_headers,
    )
    assert resp.status_code == 201, resp.text
    return resp.json()  # type: ignore[no-any-return]


def test_agent_defaults_to_null_model(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    agent = _create_agent(client, auth_headers)
    assert agent["model"] is None

    resp = client.get(f"/agents/{agent['id']}", headers=auth_headers)
    assert resp.status_code == 200, resp.text
    assert resp.json()["model"] is None


def test_create_with_model_persists(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    agent = _create_agent(client, auth_headers, model="glm-5.2")
    assert agent["model"] == "glm-5.2"

    resp = client.get(f"/agents/{agent['id']}", headers=auth_headers)
    assert resp.status_code == 200, resp.text
    assert resp.json()["model"] == "glm-5.2"


def test_patch_sets_model(client: Any, auth_headers: dict[str, str], clean_db: None) -> None:
    agent = _create_agent(client, auth_headers)
    resp = client.patch(
        f"/agents/{agent['id']}",
        json={"model": "kimi-k2.1"},
        headers=auth_headers,
    )
    assert resp.status_code == 200, resp.text
    assert resp.json()["model"] == "kimi-k2.1"


def test_patch_without_model_leaves_it_unchanged(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    agent = _create_agent(client, auth_headers, model="deepseek-v4")
    # A PATCH touching only slack_channel must not clear the model.
    resp = client.patch(
        f"/agents/{agent['id']}",
        json={"slack_channel": "CMOVED001"},
        headers=auth_headers,
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["slack_channel"] == "CMOVED001"
    assert body["model"] == "deepseek-v4"
