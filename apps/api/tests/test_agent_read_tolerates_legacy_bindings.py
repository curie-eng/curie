"""Serializing a stored binding must not re-litigate whether it should exist.

`ChannelBinding` carries the address-shape rule that #143 established, and it is
the base class three write paths inherit it from. It was also the element type of
`AgentOut.channels`, so the rule ran again every time an existing row was READ --
and a row the rule rejects made the whole listing fail, for every agent, not just
the one holding it.

There is a production path to such a row and it is an upgrade: migration 0021
backfills `agent_channels.address` from `agents.slack_channel` verbatim, and that
column is exactly where a literal `#name` from before the validator lived. So an
install that was merely mis-routed became an install whose agent list is
unavailable, reporting a Pydantic error rather than the bad value.

Reads now use a model with no shape rule. Writes keep it -- that is the other
half, and it is asserted here too, because a fix that made reads tolerant by
making writes tolerant would re-open #143.
"""

from __future__ import annotations

import asyncio
import uuid
from typing import Any

import pytest
from curie_api.config import get_settings
from sqlalchemy import text as sql_text
from sqlalchemy.ext.asyncio import create_async_engine

pytestmark = pytest.mark.usefixtures("clean_db")

# What #143 stored: the channel NAME, which the worker never routes on.
LEGACY_ADDRESS = "#sre-alerts"

# The allowlisted placeholder form. This repo is public and `.gitleaks.toml` has
# its own `slack-conversation-id` rule, because a real channel id committed as a
# test fixture is a permanent disclosure that cannot be rotated away. Any other
# id-shaped string fails that gate -- which it did, on the first push.


def _seed_legacy_binding(client: Any, headers: Any, name: str) -> str:
    """An agent whose binding predates the address rule, written past the API.

    Raw SQL on purpose: the point is a row the current write path would refuse,
    which is what an upgraded install holds and what no API call can create.
    """

    created = client.post(
        "/agents",
        json={"name": name, "channel": {"kind": "slack", "address": "C0EXAMPLE7"}},
        headers=headers,
    )
    assert created.status_code == 201, created.text
    agent_id = str(created.json()["id"])

    async def _write() -> None:
        # Fresh engine per query, following test_agents.py's `_binding_row`:
        # keeps the write off the TestClient's portal loop.
        engine = create_async_engine(get_settings().database_url)
        try:
            async with engine.begin() as conn:
                await conn.execute(
                    sql_text(
                        "UPDATE curie.agent_channels SET address = :addr "
                        "WHERE agent_id = :aid"
                    ),
                    {"addr": LEGACY_ADDRESS, "aid": agent_id},
                )
        finally:
            await engine.dispose()

    asyncio.run(_write())
    return agent_id


def test_a_legacy_binding_serializes_instead_of_failing(
    client: Any, auth_headers: Any
) -> None:
    """The bad value is shown, not hidden behind a 500.

    Showing it is the more useful outcome: an operator cannot fix an address the
    API refuses to tell them.
    """

    agent_id = _seed_legacy_binding(client, auth_headers, f"legacy-{uuid.uuid4().hex[:8]}")

    response = client.get(f"/agents/{agent_id}", headers=auth_headers)

    assert response.status_code == 200, response.text
    assert response.json()["channels"] == [{"kind": "slack", "address": LEGACY_ADDRESS}]


def test_one_bad_row_does_not_take_the_listing_down(client: Any, auth_headers: Any) -> None:
    """The failure this fixes: a neighbour's row hid every other agent."""

    _seed_legacy_binding(client, auth_headers, f"legacy-{uuid.uuid4().hex[:8]}")
    healthy = f"healthy-{uuid.uuid4().hex[:8]}"
    client.post(
        "/agents",
        json={"name": healthy, "channel": {"kind": "slack", "address": "C0EXAMPLE8"}},
        headers=auth_headers,
    )

    listed = client.get("/agents", headers=auth_headers)

    assert listed.status_code == 200, listed.text
    assert healthy in [a["name"] for a in listed.json()]


def test_binding_a_bad_address_is_still_refused(client: Any, auth_headers: Any) -> None:
    """The other half. Tolerant reads must not become tolerant writes (#143)."""

    response = client.post(
        "/agents",
        json={
            "name": f"reject-{uuid.uuid4().hex[:8]}",
            "channel": {"kind": "slack", "address": LEGACY_ADDRESS},
        },
        headers=auth_headers,
    )

    assert response.status_code == 422
    assert "is not a Slack channel ID" in response.text


def test_rebinding_to_a_bad_address_is_still_refused(client: Any, auth_headers: Any) -> None:
    """The binding endpoints inherit the same rule and must keep it.

    `PATCH /agents/{id}/channels` is where an address is rebound -- `AgentUpdate`
    deliberately refuses a `channels` field -- and it takes the write model, so
    tolerant reads must not reach it.
    """

    name = f"rebind-{uuid.uuid4().hex[:8]}"
    created = client.post(
        "/agents",
        json={"name": name, "channel": {"kind": "slack", "address": "C0EXAMPLE9"}},
        headers=auth_headers,
    )
    agent_id = created.json()["id"]

    response = client.patch(
        f"/agents/{agent_id}/channels",
        json={"kind": "slack", "address": LEGACY_ADDRESS},
        headers=auth_headers,
    )

    assert response.status_code == 422
    assert "is not a Slack channel ID" in response.text
