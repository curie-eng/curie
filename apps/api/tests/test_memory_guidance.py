"""Operator memory guidance: `GET|PUT|DELETE /agents/{id}/memory/guidance` (#1461).

The guidance is the text the runner shows the model beside its remember/
update/forget tools. With nothing stored the API reports the platform default
(`DEFAULT_MEMORY_GUIDANCE`, source "default"); an operator PUT stores
`{"text": ...}` at the agent's reserved `memory` namespace, key `guidance`,
where the runner reads it at boot (source "operator"); DELETE goes back to
the default. Platform key only, like the rest of the memory router.
"""

import uuid
from typing import Any

from curie_api.config import get_settings
from curie_api.memory_guidance import DEFAULT_MEMORY_GUIDANCE
from curie_internal.sandbox_token import mint

OPERATOR_TEXT = "Remember customer preferences; never record secrets or credentials."


def _agent(client: Any, headers: dict[str, str]) -> str:
    resp = client.post(
        "/agents",
        json={"name": "guidance-agent", "channel": {"kind": "slack", "address": "C000000G01"}},
        headers=headers,
    )
    assert resp.status_code == 201, resp.text
    agent_id: str = resp.json()["id"]
    return agent_id


def _url(agent_id: str) -> str:
    return f"/agents/{agent_id}/memory/guidance"


def _get(client: Any, headers: dict[str, str], agent_id: str) -> dict[str, Any]:
    resp = client.get(_url(agent_id), headers=headers)
    assert resp.status_code == 200, resp.text
    body: dict[str, Any] = resp.json()
    return body


def test_default_guidance_is_a_non_empty_string() -> None:
    assert isinstance(DEFAULT_MEMORY_GUIDANCE, str)
    assert DEFAULT_MEMORY_GUIDANCE.strip()


def test_get_with_nothing_stored_returns_the_default(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    aid = _agent(client, auth_headers)
    assert _get(client, auth_headers, aid) == {
        "text": DEFAULT_MEMORY_GUIDANCE,
        "source": "default",
    }


def test_put_stores_operator_guidance_and_get_returns_it(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    aid = _agent(client, auth_headers)
    resp = client.put(_url(aid), json={"text": OPERATOR_TEXT}, headers=auth_headers)
    assert resp.status_code == 200, resp.text
    assert resp.json() == {"text": OPERATOR_TEXT, "source": "operator"}
    assert _get(client, auth_headers, aid) == {"text": OPERATOR_TEXT, "source": "operator"}


def test_put_writes_the_agent_memory_namespace_guidance_key(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    """The runner reads `memory/guidance` straight from the state store, so the
    stored shape is the contract: `{"text": ...}` at the agent-wide scope."""
    aid = _agent(client, auth_headers)
    resp = client.put(_url(aid), json={"text": OPERATOR_TEXT}, headers=auth_headers)
    assert resp.status_code == 200, resp.text
    state = client.get(f"/agents/{aid}/state/memory/guidance", headers=auth_headers)
    assert state.status_code == 200, state.text
    assert state.json()["value"] == {"text": OPERATOR_TEXT}


def test_put_twice_replaces_the_stored_guidance(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    aid = _agent(client, auth_headers)
    for text in ("first guidance", OPERATOR_TEXT):
        resp = client.put(_url(aid), json={"text": text}, headers=auth_headers)
        assert resp.status_code == 200, resp.text
    assert _get(client, auth_headers, aid) == {"text": OPERATOR_TEXT, "source": "operator"}


def test_guidance_is_not_a_memory_log_entry(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    aid = _agent(client, auth_headers)
    resp = client.put(_url(aid), json={"text": OPERATOR_TEXT}, headers=auth_headers)
    assert resp.status_code == 200, resp.text
    listed = client.get(f"/agents/{aid}/memory", headers=auth_headers)
    assert listed.status_code == 200, listed.text
    assert listed.json() == []


def test_delete_returns_get_to_the_default(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    aid = _agent(client, auth_headers)
    resp = client.put(_url(aid), json={"text": OPERATOR_TEXT}, headers=auth_headers)
    assert resp.status_code == 200, resp.text
    resp = client.delete(_url(aid), headers=auth_headers)
    assert resp.status_code == 204, resp.text
    assert _get(client, auth_headers, aid) == {
        "text": DEFAULT_MEMORY_GUIDANCE,
        "source": "default",
    }
    state = client.get(f"/agents/{aid}/state/memory/guidance", headers=auth_headers)
    assert state.status_code == 404, state.text


def test_empty_text_is_refused_and_keeps_the_stored_value(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    aid = _agent(client, auth_headers)
    resp = client.put(_url(aid), json={"text": OPERATOR_TEXT}, headers=auth_headers)
    assert resp.status_code == 200, resp.text
    resp = client.put(_url(aid), json={"text": ""}, headers=auth_headers)
    assert resp.status_code == 422, resp.text
    assert _get(client, auth_headers, aid) == {"text": OPERATOR_TEXT, "source": "operator"}


def test_put_without_text_is_refused(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    aid = _agent(client, auth_headers)
    resp = client.put(_url(aid), json={}, headers=auth_headers)
    assert resp.status_code == 422, resp.text


def test_unknown_agent_is_404_on_every_method(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    missing = str(uuid.uuid4())
    assert client.get(_url(missing), headers=auth_headers).status_code == 404
    assert (
        client.put(_url(missing), json={"text": OPERATOR_TEXT}, headers=auth_headers).status_code
        == 404
    )
    assert client.delete(_url(missing), headers=auth_headers).status_code == 404


def test_guidance_requires_the_platform_key(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    aid = _agent(client, auth_headers)
    scoped = mint(get_settings().api_key, agent=aid, scope="state", exp=4102444800)
    for headers in ({}, {"X-API-Key": "not-the-key"}, {"X-API-Key": scoped}):
        assert client.get(_url(aid), headers=headers).status_code == 401, headers
        assert client.put(_url(aid), json={"text": "sneaky"}, headers=headers).status_code == 401, (
            headers
        )
        assert client.delete(_url(aid), headers=headers).status_code == 401, headers
    # Nothing a refused caller sent was stored.
    assert _get(client, auth_headers, aid)["source"] == "default"
