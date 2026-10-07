"""The completion carries the connector digest the worker attributed, against real Postgres.

@spec ACTION-EXECUTOR-11 @spec ACTION-EXECUTOR-12: the kernel's record call
passes no deployment, so the worker's recorder wrapper reads the connector's
Deployment around the call and sends ``connector`` and ``connector_digest`` on
the completion. The ruling reads both from the row (``AgentAction`` requires
both before anything is undoable), so the completion must store them, and must
refuse a value the restore would later trust: a digest outside the one form a
pinned connector renders at, a connector name outside the connector grammar,
or one of the pair without the other. A refusal stores nothing.

Credential: the platform API key alone reaches this route, and the same key can
already write ``prior_state``, ``target`` and ``post_version``. Attribution is
narrower: only the worker attributes a digest, so a completion carrying
``connector``/``connector_digest`` also needs the internal worker token
(``X-Curie-Worker-Token``, as the executor's internal routes take), and its
``connector`` must be the connector the action's stored tool names
(``mcp__<connector>__<tool>``). Otherwise a key holder could attach a call to a
digest that did not serve it, or to another connector of the same agent.

Grammars, as already held elsewhere in the API
(``schemas/action_executions.py``): connector is an RFC 1123 label
``^[a-z0-9]([a-z0-9-]*[a-z0-9])?$``; digest is ``^sha256:[0-9a-f]{64}$``.
"""

from __future__ import annotations

import asyncio
import uuid
from typing import Any

import pytest
from _sealed_actions import worker_headers
from curie_api.config import get_settings
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

pytestmark = pytest.mark.usefixtures("clean_db")

CONNECTOR = "grafana"
DIGEST = "sha256:" + "ab" * 32
OTHER_DIGEST = "sha256:" + "cd" * 32


def _worker(auth_headers: Any) -> dict[str, str]:
    """What the worker sends when it attributes: its API key and its worker token."""

    return {**auth_headers, **worker_headers()}


def _open(client: Any, auth_headers: Any, tool: str = f"mcp__{CONNECTOR}__scale_deployment") -> str:
    body = {
        "conversation_id": "C1",
        "call_id": "toolu_01",
        "tool": tool,
        "arguments": {"name": "api", "replicas": 10},
        "dedupe_key": f"event-{uuid.uuid4()}:toolu_01",
    }
    response = client.post("/actions", json=body, headers=auth_headers)
    assert response.status_code == 201
    return str(response.json()["id"])


def _complete_body(**overrides: Any) -> dict[str, Any]:
    body: dict[str, Any] = {
        "failed": False,
        "result": {"ok": True},
        "detail": "non-idempotent tool completed",
    }
    body.update(overrides)
    return body


def _row(action_id: str) -> dict[str, Any]:
    async def read() -> dict[str, Any]:
        engine = create_async_engine(get_settings().database_url)
        try:
            async with engine.connect() as conn:
                row = (
                    await conn.execute(
                        text(
                            "SELECT status, connector, connector_digest, completed_at"
                            " FROM curie.agent_actions WHERE id = :id"
                        ),
                        {"id": uuid.UUID(action_id)},
                    )
                ).one()
                return dict(row._mapping)
        finally:
            await engine.dispose()

    return asyncio.run(read())


def test_a_completion_stores_the_connector_and_its_digest(client: Any, auth_headers: Any) -> None:
    action_id = _open(client, auth_headers)

    response = client.post(
        f"/actions/{action_id}/complete",
        json=_complete_body(connector=CONNECTOR, connector_digest=DIGEST),
        headers=_worker(auth_headers),
    )

    assert response.status_code == 200
    row = _row(action_id)
    assert row["status"] == "succeeded"
    assert (row["connector"], row["connector_digest"]) == (CONNECTOR, DIGEST)


def test_a_hyphenated_connector_name_is_stored(client: Any, auth_headers: Any) -> None:
    action_id = _open(client, auth_headers, tool="mcp__grafana-ops-2__scale_deployment")

    response = client.post(
        f"/actions/{action_id}/complete",
        json=_complete_body(connector="grafana-ops-2", connector_digest=DIGEST),
        headers=_worker(auth_headers),
    )

    assert response.status_code == 200
    assert _row(action_id)["connector"] == "grafana-ops-2"


@pytest.mark.parametrize(
    "extra",
    [
        pytest.param({}, id="both_omitted"),
        pytest.param({"connector": None, "connector_digest": None}, id="both_null"),
    ],
)
def test_a_completion_without_attribution_stores_nulls(
    client: Any, auth_headers: Any, extra: dict[str, Any]
) -> None:
    """A null digest is the ordinary answer: local tier, plugin server, straddled rollout."""

    action_id = _open(client, auth_headers)

    response = client.post(
        f"/actions/{action_id}/complete", json=_complete_body(**extra), headers=auth_headers
    )

    assert response.status_code == 200
    row = _row(action_id)
    assert row["status"] == "succeeded"
    assert (row["connector"], row["connector_digest"]) == (None, None)


BAD_DIGESTS = {
    "tag_not_digest": "1.4.2",
    "bare_hex": "ab" * 32,
    "image_reference": f"registry.example/connectors/grafana@{DIGEST}",
    "uppercase_hex": "sha256:" + "AB" * 32,
    "short": "sha256:" + "ab" * 31,
    "long": "sha256:" + "ab" * 33,
    "non_hex": "sha256:" + "zz" * 32,
    "other_algorithm": "sha512:" + "ab" * 64,
    "trailing_newline": DIGEST + "\n",
    "empty": "",
}


@pytest.mark.parametrize("digest", list(BAD_DIGESTS.values()), ids=list(BAD_DIGESTS))
def test_a_malformed_digest_is_refused_and_nothing_is_stored(
    client: Any, auth_headers: Any, digest: str
) -> None:
    action_id = _open(client, auth_headers)

    response = client.post(
        f"/actions/{action_id}/complete",
        json=_complete_body(connector=CONNECTOR, connector_digest=digest),
        headers=_worker(auth_headers),
    )

    assert response.status_code == 422
    row = _row(action_id)
    assert row["status"] == "pending"
    assert row["completed_at"] is None
    assert (row["connector"], row["connector_digest"]) == (None, None)


BAD_CONNECTORS = {
    "uppercase": "Grafana",
    "underscore": "grafana_ops",
    "leading_hyphen": "-grafana",
    "trailing_hyphen": "grafana-",
    "double_underscore_join": "grafana__scale",
    "path": "ops/grafana",
    "whitespace": "grafana ",
    "over_63_chars": "a" * 64,
    "empty": "",
}


@pytest.mark.parametrize("connector", list(BAD_CONNECTORS.values()), ids=list(BAD_CONNECTORS))
def test_a_connector_outside_the_name_grammar_is_refused_and_nothing_is_stored(
    client: Any, auth_headers: Any, connector: str
) -> None:
    action_id = _open(client, auth_headers)

    response = client.post(
        f"/actions/{action_id}/complete",
        json=_complete_body(connector=connector, connector_digest=DIGEST),
        headers=_worker(auth_headers),
    )

    assert response.status_code == 422
    row = _row(action_id)
    assert row["status"] == "pending"
    assert (row["connector"], row["connector_digest"]) == (None, None)


@pytest.mark.parametrize(
    "extra",
    [
        pytest.param({"connector": CONNECTOR}, id="connector_without_digest"),
        pytest.param({"connector": CONNECTOR, "connector_digest": None}, id="digest_null"),
        pytest.param({"connector_digest": DIGEST}, id="digest_without_connector"),
        pytest.param({"connector": None, "connector_digest": DIGEST}, id="connector_null"),
    ],
)
def test_half_an_attribution_is_refused_and_nothing_is_stored(
    client: Any, auth_headers: Any, extra: dict[str, Any]
) -> None:
    """A digest names an image only together with the connector it was read from."""

    action_id = _open(client, auth_headers)

    response = client.post(
        f"/actions/{action_id}/complete",
        json=_complete_body(**extra),
        headers=_worker(auth_headers),
    )

    assert response.status_code == 422
    row = _row(action_id)
    assert row["status"] == "pending"
    assert (row["connector"], row["connector_digest"]) == (None, None)


# -- who may attribute, and to what --------------------------------------------


@pytest.mark.parametrize(
    "credential",
    [
        pytest.param("platform_key_only", id="platform_key_only"),
        pytest.param("wrong_worker_token", id="wrong_worker_token"),
    ],
)
def test_attribution_without_the_worker_token_is_refused_and_nothing_is_stored(
    client: Any, auth_headers: Any, credential: str
) -> None:
    """A platform-key holder cannot attach a call to a digest; only the worker attributes."""

    action_id = _open(client, auth_headers)
    headers = dict(auth_headers)
    if credential == "wrong_worker_token":
        headers["X-Curie-Worker-Token"] = "not-the-worker-token"

    response = client.post(
        f"/actions/{action_id}/complete",
        json=_complete_body(connector=CONNECTOR, connector_digest=DIGEST),
        headers=headers,
    )

    assert response.status_code == 403, response.text
    row = _row(action_id)
    assert row["status"] == "pending"
    assert row["completed_at"] is None
    assert (row["connector"], row["connector_digest"]) == (None, None)


def test_the_same_attribution_under_the_worker_token_is_stored(
    client: Any, auth_headers: Any
) -> None:
    action_id = _open(client, auth_headers)

    response = client.post(
        f"/actions/{action_id}/complete",
        json=_complete_body(connector=CONNECTOR, connector_digest=DIGEST),
        headers=_worker(auth_headers),
    )

    assert response.status_code == 200, response.text
    assert (_row(action_id)["connector"], _row(action_id)["connector_digest"]) == (
        CONNECTOR,
        DIGEST,
    )


def test_a_completion_without_attribution_still_takes_the_platform_key(
    client: Any, auth_headers: Any
) -> None:
    """Narrowing attribution does not move the ordinary completion off the key."""

    action_id = _open(client, auth_headers)

    response = client.post(
        f"/actions/{action_id}/complete", json=_complete_body(), headers=auth_headers
    )

    assert response.status_code == 200, response.text
    assert _row(action_id)["status"] == "succeeded"


@pytest.mark.parametrize(
    "tool",
    [
        pytest.param("mcp__slack__post_message", id="another_connector"),
        pytest.param("mcp__grafana-ops__scale_deployment", id="prefix_of_another_name"),
        pytest.param("scale_deployment", id="not_a_connector_tool"),
        pytest.param("mcp__plugin_ops_grafana__scale_deployment", id="plugin_mcp_server"),
        pytest.param("mcp__grafana", id="no_upstream_tool"),
    ],
)
def test_a_connector_the_stored_tool_does_not_name_is_refused_and_nothing_is_stored(
    client: Any, auth_headers: Any, tool: str
) -> None:
    """The attributed connector must be the ``mcp__<connector>__`` prefix of the action's tool."""

    action_id = _open(client, auth_headers, tool=tool)

    response = client.post(
        f"/actions/{action_id}/complete",
        json=_complete_body(connector=CONNECTOR, connector_digest=DIGEST),
        headers=_worker(auth_headers),
    )

    assert response.status_code == 422, response.text
    row = _row(action_id)
    assert row["status"] == "pending"
    assert (row["connector"], row["connector_digest"]) == (None, None)


@pytest.mark.parametrize(
    "replay",
    [
        pytest.param({"connector": CONNECTOR, "connector_digest": OTHER_DIGEST}, id="other_digest"),
        pytest.param({}, id="no_attribution"),
    ],
)
def test_a_replayed_completion_leaves_the_stored_digest_unchanged(
    client: Any, auth_headers: Any, replay: dict[str, Any]
) -> None:
    """A completion lands once; a redelivery cannot re-attribute the call."""

    action_id = _open(client, auth_headers)
    first = client.post(
        f"/actions/{action_id}/complete",
        json=_complete_body(connector=CONNECTOR, connector_digest=DIGEST),
        headers=_worker(auth_headers),
    )
    assert first.status_code == 200, first.text

    client.post(
        f"/actions/{action_id}/complete",
        json=_complete_body(**replay),
        headers=_worker(auth_headers),
    )

    row = _row(action_id)
    assert (row["connector"], row["connector_digest"]) == (CONNECTOR, DIGEST)
