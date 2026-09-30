"""A hook's reply surface names its route's identity (ADR-0168 decision 3).

A route is `(kind, adapter, address)` on every kind, so the hook ingress takes
an optional `adapter` query parameter beside `kind` and `address`: for Slack it
is the identity name, for any other kind the adapter slug. Omitted, the
selection is what it was before, and the turn replies as the route it selected.
"""

from __future__ import annotations

import json
import time
from collections.abc import Iterator
from typing import Any

import pytest
import redis
from aci_protocol import QueuedTurn
from curie_api.config import get_settings
from curie_api.hook_signing import derive, sign
from fastapi.testclient import TestClient

TWO_IDENTITIES = json.dumps(
    [
        {
            "name": "default",
            "app_token_env": "SLACK_APP_TOKEN",
            "bot_token_env": "SLACK_BOT_TOKEN",
            "signing_secret_env": "SLACK_SIGNING_SECRET",
        },
        {
            "name": "second",
            "app_token_env": "CURIE_SLACK_APP_TOKEN__0",
            "bot_token_env": "CURIE_SLACK_BOT_TOKEN__0",
            "signing_secret_env": None,
        },
    ]
)
EMAIL = "ops@example.com"


@pytest.fixture
def two_identities(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    monkeypatch.setenv("CURIE_SLACK_IDENTITIES", TWO_IDENTITIES)
    get_settings.cache_clear()
    yield
    monkeypatch.delenv("CURIE_SLACK_IDENTITIES", raising=False)
    get_settings.cache_clear()


def _agent(client: TestClient, headers: dict[str, str], name: str, channel: dict[str, Any]) -> str:
    created = client.post("/agents", json={"name": name, "channel": channel}, headers=headers)
    assert created.status_code == 201, created.text
    return str(created.json()["id"])


def _add(
    client: TestClient, headers: dict[str, str], agent_id: str, channel: dict[str, Any]
) -> None:
    added = client.post(f"/agents/{agent_id}/channels", json=channel, headers=headers)
    assert added.status_code == 201, added.text


def _email(adapter: str) -> dict[str, str]:
    return {
        "kind": "email",
        "address": EMAIL,
        "endpoint": f"https://{adapter}.example.test/",
        "adapter": adapter,
    }


def _hook(client: TestClient, agent_id: str, query: str, delivery_id: str) -> Any:
    body = b"{}"
    secret = derive(get_settings().api_key, agent_id=agent_id, generation=0)
    timestamp = str(int(time.time()))
    signature = sign(secret, timestamp=timestamp, delivery_id=delivery_id, body=body)
    return client.post(
        f"/hooks/{agent_id}/issues?{query}",
        content=body,
        headers={
            "X-Curie-Signature-256": signature,
            "X-Curie-Delivery-Id": delivery_id,
            "X-Curie-Timestamp": timestamp,
        },
    )


def _queued(valkey: redis.Redis, stream: str) -> list[QueuedTurn]:
    return [QueuedTurn.model_validate_json(f["payload"]) for _, f in valkey.xrange(stream)]


def test_a_named_identity_only_slack_agent_receives_a_hook_through_that_identity(
    hooks_client: TestClient,
    auth_headers: dict[str, str],
    valkey: redis.Redis,
    runs_stream: str,
    two_identities: None,
    clean_db: None,
) -> None:
    agent_id = _agent(
        hooks_client,
        auth_headers,
        "second-only",
        {"kind": "slack", "address": "C0EXAMPLE3", "adapter": "second"},
    )

    resp = _hook(hooks_client, agent_id, "kind=slack&address=C0EXAMPLE3&adapter=second", "n-1")

    assert resp.status_code == 200, resp.text
    (turn,) = _queued(valkey, runs_stream)
    assert turn.reply_handle is not None
    handle = turn.reply_handle
    assert (handle.kind, handle.channel, handle.endpoint, handle.adapter) == (
        "slack",
        "C0EXAMPLE3",
        None,
        "second",
    )


def test_two_identities_on_one_channel_are_each_selected_by_adapter(
    hooks_client: TestClient,
    auth_headers: dict[str, str],
    valkey: redis.Redis,
    runs_stream: str,
    two_identities: None,
    clean_db: None,
) -> None:
    agent_id = _agent(
        hooks_client, auth_headers, "two-bots", {"kind": "slack", "address": "C0EXAMPLE4"}
    )
    _add(
        hooks_client,
        auth_headers,
        agent_id,
        {"kind": "slack", "address": "C0EXAMPLE4", "adapter": "second"},
    )

    for delivery_id, identity in (("t-1", "default"), ("t-2", "second")):
        resp = _hook(
            hooks_client,
            agent_id,
            f"kind=slack&address=C0EXAMPLE4&adapter={identity}",
            delivery_id,
        )
        assert resp.status_code == 200, resp.text

    handles = [turn.reply_handle for turn in _queued(valkey, runs_stream)]
    assert [(h.channel, h.adapter) for h in handles if h is not None] == [
        ("C0EXAMPLE4", "default"),
        ("C0EXAMPLE4", "second"),
    ]


def test_an_adapter_picks_one_of_two_routes_on_a_non_slack_pair(
    hooks_client: TestClient,
    auth_headers: dict[str, str],
    valkey: redis.Redis,
    runs_stream: str,
    clean_db: None,
) -> None:
    agent_id = _agent(hooks_client, auth_headers, "two-inboxes", _email("inbox-a"))
    _add(hooks_client, auth_headers, agent_id, _email("inbox-b"))

    resp = _hook(hooks_client, agent_id, f"kind=email&address={EMAIL}&adapter=inbox-b", "e-1")

    assert resp.status_code == 200, resp.text
    (turn,) = _queued(valkey, runs_stream)
    assert turn.reply_handle is not None
    assert (turn.reply_handle.endpoint, turn.reply_handle.adapter) == (
        "https://inbox-b.example.test/",
        "inbox-b",
    )


def test_an_omitted_adapter_on_two_routes_says_to_pass_one(
    hooks_client: TestClient, auth_headers: dict[str, str], clean_db: None
) -> None:
    agent_id = _agent(hooks_client, auth_headers, "ambiguous-inboxes", _email("inbox-a"))
    _add(hooks_client, auth_headers, agent_id, _email("inbox-b"))

    resp = _hook(hooks_client, agent_id, f"kind=email&address={EMAIL}", "a-1")

    assert resp.status_code == 409, resp.text
    assert resp.json()["detail"] == (
        f"2 routes are bound to email:{EMAIL}; pass adapter to name one"
    )


def test_an_omitted_adapter_keeps_the_unbound_404(
    hooks_client: TestClient,
    auth_headers: dict[str, str],
    two_identities: None,
    clean_db: None,
) -> None:
    """A Slack pair bound only under a named identity has no default route,
    and the 404 names the identities that do bind it (ADR-0168's evidence:
    a refusal that does not mention the missing selector)."""
    agent_id = _agent(
        hooks_client,
        auth_headers,
        "second-only-404",
        {"kind": "slack", "address": "C0EXAMPLE5", "adapter": "second"},
    )

    resp = _hook(hooks_client, agent_id, "kind=slack&address=C0EXAMPLE5", "u-1")

    assert resp.status_code == 404, resp.text
    assert resp.json()["detail"] == (
        "this agent has no binding for the selected kind and address as 'default'; "
        "it binds slack:C0EXAMPLE5 only as 'second', so pass adapter to name one"
    )


def test_an_adapter_that_names_no_route_is_a_404_naming_it(
    hooks_client: TestClient,
    auth_headers: dict[str, str],
    two_identities: None,
    clean_db: None,
) -> None:
    agent_id = _agent(
        hooks_client, auth_headers, "default-only", {"kind": "slack", "address": "C0EXAMPLE6"}
    )

    resp = _hook(hooks_client, agent_id, "kind=slack&address=C0EXAMPLE6&adapter=second", "m-1")

    assert resp.status_code == 404, resp.text
    assert resp.json()["detail"] == (
        "this agent has no binding for the selected kind and address as 'second'"
    )


def test_an_undeclared_slack_identity_is_refused(
    hooks_client: TestClient, auth_headers: dict[str, str], clean_db: None
) -> None:
    agent_id = _agent(
        hooks_client, auth_headers, "undeclared", {"kind": "slack", "address": "C0EXAMPLE7"}
    )

    resp = _hook(hooks_client, agent_id, "kind=slack&address=C0EXAMPLE7&adapter=ghost", "g-1")

    assert resp.status_code == 422, resp.text
    assert "slack identity 'ghost' is not declared" in resp.json()["detail"]


def test_an_adapter_without_kind_and_address_is_refused(
    hooks_client: TestClient, auth_headers: dict[str, str], clean_db: None
) -> None:
    agent_id = _agent(
        hooks_client, auth_headers, "adapter-alone", {"kind": "slack", "address": "C0EXAMPLE8"}
    )

    resp = _hook(hooks_client, agent_id, "adapter=default", "x-1")

    assert resp.status_code == 422, resp.text
    assert resp.json()["detail"] == (
        "hook reply surface adapter names a route within kind and address; pass all three"
    )
