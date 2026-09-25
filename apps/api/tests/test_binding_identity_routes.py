"""The binding endpoints select by the route TRIPLE (ADR-0168 decision 3).

The route is stored as `(kind, address)` plus a nullable `adapter` column,
where a NULL Slack `adapter` means the default identity (`route_identity`,
`aci_protocol.turn`), until the contract migration for that decision (#3100)
flips the stored form. These tests pin the read side of that contract on the
mutating endpoints: PATCH and DELETE take an `adapter` QUERY parameter and
select by IDENTITY, not merely by `(kind, address)`.

Migration 0023's `agent_channels_kind_address_key` (UNIQUE kind, address) is
unchanged until that migration, so within one agent's own bindings at most ONE
row can ever share a `(kind, address)` pair -- two identities cannot yet
collide on the SAME pair. `test_one_identity_cannot_modify_another_identitys_binding`
therefore seeds the second identity on an address the API itself never binds
for that agent, straight into Postgres, to prove the write is gated by
IDENTITY (not merely by pair) without needing two rows to share one pair.
That seed still has to satisfy 0024's `agent_channels_route_pair_ck`
(`(endpoint IS NULL) = (adapter IS NULL)`), so it carries an endpoint too,
even though this file's point is the identity check, not the transport.
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
    agent_id: str, *, kind: str, address: str, adapter: str | None, endpoint: str | None
) -> None:
    """Write a row straight to Postgres, bypassing the API's declared-identity
    gate -- used only to reach a state the write schema itself cannot produce
    yet (a second identity on one agent)."""

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
    """The (kind, address) pair key still holds, so a second identity cannot be
    seeded onto agent A's own pair. Seed it onto an address the API never
    binds for agent B instead. B's `adapter=default` DELETE on that address
    must not find the 'second' row (a 404, and the row survives), and B must
    not be able to touch A's `(slack, default, C0EXAMPLE1)` binding either --
    the ordinary cross-agent authorization boundary, now judged by identity
    too.
    """

    a = _agent(client, auth_headers, "identity-a", _slack("C0EXAMPLE1"))
    b = _agent(client, auth_headers, "identity-b", _slack("C0EXAMPLE5"))
    _insert_binding(
        b,
        kind="slack",
        address="C0EXAMPLE6",
        adapter="second",
        endpoint="http://second-identity.test/reply",
    )

    refused = _remove(
        client, auth_headers, b, kind="slack", address="C0EXAMPLE6", adapter="default"
    )
    assert refused.status_code == 404, refused.text
    rows = _binding_rows(b)
    assert {r["adapter"] for r in rows} == {None, "second"}

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
    404, and the row keeps its address, identity, endpoint and generation),
    and B's move of A's `(slack, default, C0EXAMPLE1)` binding is refused as
    the cross-agent write it would be.
    """

    a = _agent(client, auth_headers, "move-identity-a", _slack("C0EXAMPLE1"))
    b = _agent(client, auth_headers, "move-identity-b", _slack("C0EXAMPLE5"))
    _insert_binding(
        b,
        kind="slack",
        address="C0EXAMPLE6",
        adapter="second",
        endpoint="http://second-identity.test/reply",
    )
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


def test_a_pair_taken_by_another_agent_is_a_pair_conflict(
    client: TestClient, auth_headers: dict[str, str], clean_db: None
) -> None:
    """`agent_channels_kind_address_key` fires on the pair whatever identity the
    write names, so the 409 names the pair and claims no identity."""

    _agent(client, auth_headers, "route-taken-a", _slack("C0EXAMPLE4"))
    resp = _create(client, auth_headers, name="route-taken-b", channel=_slack("C0EXAMPLE4"))
    assert resp.status_code == 409, resp.text
    detail = resp.json()["detail"]
    assert "channel kind and address" in detail, detail
    assert "identity" not in detail, detail


def test_the_custom_transport_slack_binding_is_still_selected_with_no_adapter(
    client: TestClient, auth_headers: dict[str, str], clean_db: None
) -> None:
    """A pre-ADR custom-transport Slack row (endpoint plus a credential-slug
    adapter, e.g. the offline hook-approval proof rig) never matches the
    'default' identity, and its old callers (CLI, UI, the e2e proof) never
    send `adapter` either. The omitted-adapter selector must fall back to the
    one row on this pair that carries an endpoint, until the contract
    migration for ADR-0168 decision 3 (#3100) refuses a Slack endpoint.
    """

    a = _agent(client, auth_headers, "custom-transport", _slack("C0EXAMPLE7"))
    added = _add(
        client,
        auth_headers,
        a,
        {
            "kind": "slack",
            "address": "C0EXAMPLE8",
            "endpoint": "http://proof-rig.test/reply",
            "adapter": "proof-offline",
        },
    )
    assert added.status_code == 201, added.text

    removed = _remove(client, auth_headers, a, kind="slack", address="C0EXAMPLE8")
    assert removed.status_code == 204, removed.text


def test_a_repost_of_a_pair_already_held_via_custom_transport_is_idempotent(
    client: TestClient, auth_headers: dict[str, str], clean_db: None
) -> None:
    """Re-POSTing a bare Slack channel onto a pair this agent already holds
    through the custom-transport form stays idempotent (201), as it was
    before ADR-0168 -- not a 409 "another agent is already bound" from an
    idempotence check that compares identities directly and never finds the
    custom-transport row the way `_binding_for`'s omitted-adapter fallback
    does.
    """

    a = _agent(client, auth_headers, "repost-custom-transport", _slack("C0EXAMPLE1"))
    added = _add(
        client,
        auth_headers,
        a,
        {
            "kind": "slack",
            "address": "C0EXAMPLE2",
            "endpoint": "http://proof-rig.test/reply",
            "adapter": "proof-offline",
        },
    )
    assert added.status_code == 201, added.text

    reposted = _add(client, auth_headers, a, _slack("C0EXAMPLE2"))
    assert reposted.status_code == 201, reposted.text


def test_a_non_slack_repost_under_another_adapter_is_idempotent(
    client: TestClient, auth_headers: dict[str, str], clean_db: None
) -> None:
    """Re-POSTing a `(kind, address)` pair this agent already holds is the
    idempotent success it was before ADR-0168 decision 3, whatever `adapter`
    the repeat names. Migration 0023's `agent_channels_kind_address_key` lets
    the pair carry one row, so a non-Slack re-POST naming a different
    credential slug cannot be a second route: it finds the agent's own row,
    answers the binding set, and changes nothing.
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

    reposted = _add(
        client,
        auth_headers,
        a,
        {**webhook, "endpoint": "http://other-adapter.test/reply", "adapter": "other-webhook"},
    )

    assert reposted.status_code == 201, reposted.text
    assert _binding_rows(a) == before


def test_a_slack_repost_over_a_custom_transport_row_changes_nothing(
    client: TestClient, auth_headers: dict[str, str], clean_db: None
) -> None:
    """A bare Slack re-POST (no adapter) of a pair this agent holds through the
    custom-transport form is idempotent and leaves that row exactly as stored:
    same credential slug, same endpoint, same generation."""

    a = _agent(client, auth_headers, "repost-keeps-custom-transport", _slack("C0EXAMPLE1"))
    custom = {
        "kind": "slack",
        "address": "C0EXAMPLE2",
        "endpoint": "http://proof-rig.test/reply",
        "adapter": "proof-offline",
    }
    assert _add(client, auth_headers, a, custom).status_code == 201
    before = _binding_rows(a)

    for repeat in (_slack("C0EXAMPLE2"), {**custom, "adapter": "proof-other"}):
        reposted = _add(client, auth_headers, a, repeat)
        assert reposted.status_code == 201, reposted.text
        assert _binding_rows(a) == before


def test_a_move_onto_this_agents_own_pair_under_another_route_names_this_agent(
    client: TestClient, auth_headers: dict[str, str], clean_db: None
) -> None:
    """Moving a DIFFERENT binding onto a pair this agent already holds through
    the custom-transport form still 409s -- the database constraint is still
    the pair alone (`agent_channels_kind_address_key`) -- but the message must
    name THIS
    agent, not "another agent": `agent_id_for_route` answers None because the
    identities differ (the mover asks for no identity/'default', the row
    holds 'proof-offline'), and `_raise_binding_conflict` must fall back to
    the pair-level lookup rather than reporting the pair as someone else's.
    """

    a = _agent(client, auth_headers, "move-onto-custom-transport", _slack("C0EXAMPLE1"))
    added = _add(
        client,
        auth_headers,
        a,
        {
            "kind": "slack",
            "address": "C0EXAMPLE2",
            "endpoint": "http://proof-rig.test/reply",
            "adapter": "proof-offline",
        },
    )
    assert added.status_code == 201, added.text

    moved = _move(
        client,
        auth_headers,
        a,
        kind="slack",
        address="C0EXAMPLE1",
        channel={"kind": "slack", "address": "C0EXAMPLE2"},
    )
    assert moved.status_code == 409, moved.text
    detail = moved.json()["detail"]
    assert "this agent" in detail, detail
    assert "another agent" not in detail, detail


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
    Starts from the custom-transport form so the before/after difference is
    observable: 'proof-offline' must become the default identity's NULL once
    the PATCH lands, not stay 'proof-offline' because endpoint was never sent.
    """

    a = _agent(
        client,
        auth_headers,
        "adapter-only-patch",
        {
            "kind": "slack",
            "address": "C0EXAMPLE9",
            "endpoint": "http://proof-rig.test/reply",
            "adapter": "proof-offline",
        },
    )

    moved = _move(
        client,
        auth_headers,
        a,
        kind="slack",
        address="C0EXAMPLE9",
        channel={"kind": "slack", "address": "C0EXAMPLE9", "adapter": "default"},
    )
    assert moved.status_code == 200, moved.text
    row = _binding_rows(a)[0]
    assert row["adapter"] is None  # the default identity's stored form is NULL
    # Also cleared, not left at the old custom-transport URL: 0024's
    # agent_channels_route_pair_ck requires both-or-neither, so a surviving
    # endpoint here would violate the CHECK on the flush.
    assert row["endpoint"] is None


def test_a_patch_naming_neither_route_field_preserves_the_stored_route(
    client: TestClient, auth_headers: dict[str, str], clean_db: None
) -> None:
    """The gate's other half: omitting BOTH `endpoint` and `adapter` on a
    kind/address-only move must leave the stored route untouched, exactly as
    before the kind-aware gate.
    """

    a = _agent(
        client,
        auth_headers,
        "preserve-route-patch",
        {
            "kind": "slack",
            "address": "C0EXAMPLE0",
            "endpoint": "http://custom-transport.test/reply",
            "adapter": "some-credential",
        },
    )

    moved = _move(
        client,
        auth_headers,
        a,
        kind="slack",
        address="C0EXAMPLE0",
        channel={"kind": "slack", "address": "C0EXAMPLE1"},
    )
    assert moved.status_code == 200, moved.text
    row = [r for r in _binding_rows(a) if r["address"] == "C0EXAMPLE1"][0]
    assert row["endpoint"] == "http://custom-transport.test/reply"
    assert row["adapter"] == "some-credential"
