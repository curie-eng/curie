"""The binding endpoints select by the route TRIPLE (ADR-0168 decision 3).

The route is `(kind, address, adapter)`, where a Slack `adapter` is the bot
identity, stored by name (`default` for the default identity). PATCH and
DELETE take an `adapter` QUERY parameter and select by IDENTITY, not merely by
`(kind, address)`. Seeds written straight to Postgres give a Slack row its
identity and no endpoint, as `agent_channels_route_ck` requires.
"""

import asyncio
from typing import Any

from curie_api.config import get_settings
from fastapi.testclient import TestClient
from sqlalchemy import text as sql_text
from sqlalchemy.ext.asyncio import create_async_engine

# --- helpers ------------------------------------------------------------------


def _slack(address: str) -> dict[str, str]:
    return {"kind": "slack", "address": address}


def _create(client: TestClient, headers: dict[str, str], **fields: Any) -> Any:
    return client.post("/agents", json=fields, headers=headers)


def _agent(
    client: TestClient, headers: dict[str, str], name: str, channel: dict[str, Any]
) -> str:
    created = _create(client, headers, name=name, channel=channel)
    assert created.status_code == 201, created.text
    return str(created.json()["id"])


def _add(
    client: TestClient, headers: dict[str, str], agent_id: str, channel: dict[str, Any]
) -> Any:
    return client.post(f"/agents/{agent_id}/channels", json=channel, headers=headers)


def _remove(
    client: TestClient,
    headers: dict[str, str],
    agent_id: str,
    *,
    kind: str,
    address: str,
    adapter: str | None = None,
) -> Any:
    params: dict[str, Any] = {"kind": kind, "address": address}
    if adapter is not None:
        params["adapter"] = adapter
    return client.request(
        "DELETE", f"/agents/{agent_id}/channels", params=params, headers=headers
    )


def _move(
    client: TestClient,
    headers: dict[str, str],
    agent_id: str,
    *,
    kind: str,
    address: str,
    adapter: str | None = None,
    channel: dict[str, Any],
) -> Any:
    params: dict[str, Any] = {"kind": kind, "address": address}
    if adapter is not None:
        params["adapter"] = adapter
    return client.patch(
        f"/agents/{agent_id}/channels", params=params, json=channel, headers=headers
    )


def _binding_rows(agent_id: str) -> list[dict[str, Any]]:
    """This agent's `agent_channels` rows, read straight from Postgres.

    Follows `test_agent_channels_subresource.py`'s fresh-engine-per-query
    pattern, which keeps the query off the TestClient's portal loop.
    """

    async def run() -> list[dict[str, Any]]:
        engine = create_async_engine(get_settings().database_url)
        try:
            async with engine.connect() as conn:
                result = await conn.execute(
                    sql_text(
                        "SELECT id, kind, address, endpoint, adapter, generation "
                        "FROM curie.agent_channels WHERE agent_id = :aid ORDER BY kind, address"
                    ),
                    {"aid": agent_id},
                )
                return [dict(row) for row in result.mappings().all()]
        finally:
            await engine.dispose()

    return asyncio.run(run())


def _insert_binding(
    agent_id: str, *, kind: str, address: str, adapter: str | None, endpoint: str | None = None
) -> None:
    """Write a row straight to Postgres, bypassing the API's declared-identity
    gate, so a named identity needs no declaration in this file."""

    async def run() -> None:
        engine = create_async_engine(get_settings().database_url)
        try:
            async with engine.begin() as conn:
                await conn.execute(
                    sql_text(
                        "INSERT INTO curie.agent_channels "
                        "(id, agent_id, kind, address, adapter, endpoint, generation) "
                        "VALUES (gen_random_uuid(), :aid, :kind, :address, :adapter, :endpoint, 0)"
                    ),
                    {
                        "aid": agent_id,
                        "kind": kind,
                        "address": address,
                        "adapter": adapter,
                        "endpoint": endpoint,
                    },
                )
        finally:
            await engine.dispose()

    asyncio.run(run())


# --- tests ----------------------------------------------------------------


def test_one_identity_cannot_modify_another_identitys_binding(
    client: TestClient, auth_headers: dict[str, str], clean_db: None
) -> None:
    """B's `adapter=default` DELETE on an address B holds only as 'second'
    must not find the 'second' row (a 404, and the row survives), and B must
    not be able to touch A's `(slack, default, C0EXAMPLE1)` binding either --
    the ordinary cross-agent authorization boundary, now judged by identity
    too.
    """

    a = _agent(client, auth_headers, "identity-a", _slack("C0EXAMPLE1"))
    b = _agent(client, auth_headers, "identity-b", _slack("C0EXAMPLE5"))
    _insert_binding(b, kind="slack", address="C0EXAMPLE6", adapter="second")

    refused = _remove(
        client, auth_headers, b, kind="slack", address="C0EXAMPLE6", adapter="default"
    )
    assert refused.status_code == 404, refused.text
    rows = _binding_rows(b)
    assert {r["adapter"] for r in rows} == {"default", "second"}

    cross_agent = _remove(
        client, auth_headers, b, kind="slack", address="C0EXAMPLE1", adapter="default"
    )
    assert cross_agent.status_code == 404, cross_agent.text
    assert any(r["address"] == "C0EXAMPLE1" for r in _binding_rows(a))


def test_one_identity_cannot_move_another_identitys_binding(
    client: TestClient, auth_headers: dict[str, str], clean_db: None
) -> None:
    """The PATCH counterpart of the DELETE test above, on the same seed: B's
    `adapter=default` move of the 'second' identity's address finds no row (a
    404, and the row keeps its address, identity and generation),
    and B's move of A's `(slack, default, C0EXAMPLE1)` binding is refused as
    the cross-agent write it would be.
    """

    a = _agent(client, auth_headers, "move-identity-a", _slack("C0EXAMPLE1"))
    b = _agent(client, auth_headers, "move-identity-b", _slack("C0EXAMPLE5"))
    _insert_binding(b, kind="slack", address="C0EXAMPLE6", adapter="second")
    before_b = _binding_rows(b)
    before_a = _binding_rows(a)

    refused = _move(
        client,
        auth_headers,
        b,
        kind="slack",
        address="C0EXAMPLE6",
        adapter="default",
        channel=_slack("C0EXAMPLE7"),
    )
    assert refused.status_code == 404, refused.text
    assert _binding_rows(b) == before_b

    cross_agent = _move(
        client,
        auth_headers,
        b,
        kind="slack",
        address="C0EXAMPLE1",
        adapter="default",
        channel=_slack("C0EXAMPLE8"),
    )
    assert cross_agent.status_code == 404, cross_agent.text
    assert _binding_rows(a) == before_a


def test_an_omitted_slack_adapter_selects_the_default_identity(
    client: TestClient, auth_headers: dict[str, str], clean_db: None
) -> None:
    a = _agent(client, auth_headers, "omit-adapter", _slack("C0EXAMPLE2"))
    added = _add(client, auth_headers, a, _slack("C0EXAMPLE3"))
    assert added.status_code == 201, added.text

    removed = _remove(client, auth_headers, a, kind="slack", address="C0EXAMPLE2")
    assert removed.status_code == 204, removed.text


def test_a_route_taken_by_another_agent_is_a_route_conflict(
    client: TestClient, auth_headers: dict[str, str], clean_db: None
) -> None:
    """`agent_channels_route_key` fires on the triple, so the 409 says the
    route is taken under that identity."""

    _agent(client, auth_headers, "route-taken-a", _slack("C0EXAMPLE4"))
    resp = _create(client, auth_headers, name="route-taken-b", channel=_slack("C0EXAMPLE4"))
    assert resp.status_code == 409, resp.text
    detail = resp.json()["detail"]
    assert "channel kind and address" in detail, detail
    assert "identity" in detail, detail


def test_a_non_slack_post_under_another_adapter_adds_a_second_route(
    client: TestClient, auth_headers: dict[str, str], clean_db: None
) -> None:
    """A re-POST is idempotent per ROUTE, not per pair: the triple key lets
    one agent hold a `(kind, address)` pair under two adapters, so a POST
    naming another adapter is a second route and inserts it, while a repeat
    of the same route changes nothing.
    """

    a = _agent(client, auth_headers, "repost-other-adapter", _slack("C0EXAMPLE1"))
    webhook = {
        "kind": "webhook",
        "address": "https://example.test/hook",
        "endpoint": "http://webhook-adapter.test/reply",
        "adapter": "acme-webhook",
    }
    assert _add(client, auth_headers, a, webhook).status_code == 201
    before = _binding_rows(a)

    repeated = _add(client, auth_headers, a, webhook)
    assert repeated.status_code == 201, repeated.text
    assert _binding_rows(a) == before

    second = _add(
        client,
        auth_headers,
        a,
        {**webhook, "endpoint": "http://other-adapter.test/reply", "adapter": "other-webhook"},
    )

    assert second.status_code == 201, second.text
    adapters = {r["adapter"] for r in _binding_rows(a) if r["kind"] == "webhook"}
    assert adapters == {"acme-webhook", "other-webhook"}


def test_a_non_slack_adapter_selects_the_named_identity(
    client: TestClient, auth_headers: dict[str, str], clean_db: None
) -> None:
    """The "adapter given, non-Slack" arm of `crud.matching_bindings`.
    `adapter` on a non-Slack binding is a credential slug (ADR-0096), not an
    ADR-0168 identity, but the same selection code runs for every kind:
    naming the wrong slug must not find the row, and naming the right one
    must.
    """

    a = _agent(client, auth_headers, "non-slack-adapter", _slack("C0EXAMPLE2"))
    added = _add(
        client,
        auth_headers,
        a,
        {
            "kind": "webhook",
            "address": "https://example.test/hook",
            "endpoint": "http://webhook-adapter.test/reply",
            "adapter": "acme-webhook",
        },
    )
    assert added.status_code == 201, added.text

    wrong = _remove(
        client,
        auth_headers,
        a,
        kind="webhook",
        address="https://example.test/hook",
        adapter="other",
    )
    assert wrong.status_code == 404, wrong.text
    assert any(
        r["kind"] == "webhook" and r["adapter"] == "acme-webhook" for r in _binding_rows(a)
    )

    right = _remove(
        client,
        auth_headers,
        a,
        kind="webhook",
        address="https://example.test/hook",
        adapter="acme-webhook",
    )
    assert right.status_code == 204, right.text


def test_a_patch_naming_only_the_slack_adapter_persists_it(
    client: TestClient, auth_headers: dict[str, str], clean_db: None
) -> None:
    """An adapter sent ALONE -- the identity-only move
    `ChannelBindingPatch._check_route_presence` legalizes -- writes through
    `crud.update_channel_binding`, not only a route that sends `endpoint`.
    Starts from a named identity so the before/after difference is
    observable: 'second' must become 'default' once the PATCH lands.
    """

    a = _agent(client, auth_headers, "adapter-only-patch", _slack("C0EXAMPLE1"))
    _insert_binding(a, kind="slack", address="C0EXAMPLE9", adapter="second")

    moved = _move(
        client,
        auth_headers,
        a,
        kind="slack",
        address="C0EXAMPLE9",
        adapter="second",
        channel={"kind": "slack", "address": "C0EXAMPLE9", "adapter": "default"},
    )
    assert moved.status_code == 200, moved.text
    row = [r for r in _binding_rows(a) if r["address"] == "C0EXAMPLE9"][0]
    assert row["adapter"] == "default"
    assert row["endpoint"] is None


def test_a_patch_naming_neither_route_field_preserves_the_stored_route(
    client: TestClient, auth_headers: dict[str, str], clean_db: None
) -> None:
    """The gate's other half: omitting BOTH `endpoint` and `adapter` on an
    address-only move within one kind must leave the stored identity
    untouched, exactly as before the kind-aware gate.
    """

    a = _agent(client, auth_headers, "preserve-route-patch", _slack("C0EXAMPLE1"))
    _insert_binding(a, kind="slack", address="C0EXAMPLE0", adapter="second")

    moved = _move(
        client,
        auth_headers,
        a,
        kind="slack",
        address="C0EXAMPLE0",
        adapter="second",
        channel={"kind": "slack", "address": "C0EXAMPLE2"},
    )
    assert moved.status_code == 200, moved.text
    row = [r for r in _binding_rows(a) if r["address"] == "C0EXAMPLE2"][0]
    assert row["endpoint"] is None
    assert row["adapter"] == "second"
