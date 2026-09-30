"""An approval is answered where it was asked, including by email (ADR-0177).

Three things are pinned here, all through the real HTTP surface against real
Postgres and Valkey:

1. A route may name ``{"mode": "requesting_surface"}`` as its card target. It
   stores and reads back as exactly that, it cannot carry a notification, and a
   mix of the mode and a fixed target is refused. A fixed target stays Slack.
2. An adapter principal is served an approval whose card went to one of its
   own bindings: the conversation that asked, routeless or in the new mode.
3. A card shown in an email thread is answered only by a sender the serving
   adapter verified whose address is on the route's approver ``emails``
   (ADR 0183), after the binding's ``allowed_callers`` admit that sender
   (ADR 0175). The requester is not admitted by default, a routeless email
   approval and an empty list admit nobody, and no operator, console session or
   Slack principal can answer it.
"""

from __future__ import annotations

import asyncio
import json
import time
import uuid
from collections.abc import Iterator
from typing import Any

import pytest
import redis
from curie_api import adapter_principal, approval_principal
from curie_api.config import get_settings
from curie_api.main import create_app
from curie_api.routers.console import SESSION_COOKIE
from fastapi.testclient import TestClient
from sqlalchemy import text as sql_text
from sqlalchemy.ext.asyncio import create_async_engine

ADAPTER_HEADER = "X-Curie-Adapter-Principal"
ACTOR_HEADER = "X-Curie-Approval-Actor"
PRINCIPAL_HEADER = "X-Curie-Approval-Principal"
ADAPTER_SUBJECT = "mail-adapter-test"
REQUESTER = "requester@example.com"
COPIED = "copied@example.com"
APPROVER = "approver@example.com"
EMAIL_ENDPOINT = "http://curie-mail-adapter:8080/"
EMAIL_ADAPTER = "agentmail-sandbox"
REQUESTING_SURFACE = {"mode": "requesting_surface"}
NOT_FOUND = "approval not found"
# The route every email agent here is created with unless a test names its own:
# the card is shown in the asking thread and only APPROVER may answer it.
LISTED_ROUTE = "approve"
LISTED = {"resolution": REQUESTING_SURFACE, "approvers": {"emails": [APPROVER]}}


@pytest.fixture
def surface_client(_disposable_db: Any, runs_stream: str) -> Iterator[TestClient]:
    with TestClient(create_app()) as test_client:
        yield test_client


# --- helpers ------------------------------------------------------------------


def _uid() -> str:
    return uuid.uuid4().hex[:8]


def _binding_ids(agent_id: str) -> dict[str, str]:
    """``address -> binding row id`` for every channel binding of the agent."""

    async def run() -> dict[str, str]:
        engine = create_async_engine(get_settings().database_url)
        try:
            async with engine.connect() as conn:
                result = await conn.execute(
                    sql_text("SELECT id, address FROM curie.agent_channels WHERE agent_id = :aid"),
                    {"aid": agent_id},
                )
                return {row.address: str(row.id) for row in result}
        finally:
            await engine.dispose()

    return asyncio.run(run())


def _stored_routes(agent_id: str) -> dict[str, Any]:
    async def run() -> dict[str, Any]:
        engine = create_async_engine(get_settings().database_url)
        try:
            async with engine.connect() as conn:
                result = await conn.execute(
                    sql_text("SELECT approval_routes FROM curie.agents WHERE id = :aid"),
                    {"aid": agent_id},
                )
                return dict(result.scalar_one())
        finally:
            await engine.dispose()

    return asyncio.run(run())


def _email_agent(
    client: TestClient,
    auth: dict[str, str],
    *,
    routes: dict[str, Any] | None = None,
) -> dict[str, str]:
    inbox = f"bot-{_uid()}@example.com"
    body: dict[str, Any] = {
        "name": f"surface-mail-{_uid()}",
        "channel": {
            "kind": "email",
            "address": inbox,
            "endpoint": EMAIL_ENDPOINT,
            "adapter": EMAIL_ADAPTER,
        },
    }
    body["approval_routes"] = routes if routes is not None else {LISTED_ROUTE: LISTED}
    created = client.post("/agents", json=body, headers=auth)
    assert created.status_code == 201, created.text
    agent_id = str(created.json()["id"])
    return {"agent_id": agent_id, "inbox": inbox, "binding_id": _binding_ids(agent_id)[inbox]}


def _email_approval(
    client: TestClient,
    auth: dict[str, str],
    agent: dict[str, str],
    *,
    route: str | None = None,
    card_channel: str | None = None,
    author: str = REQUESTER,
) -> dict[str, Any]:
    """The record the worker writes for an approval asked in an email thread."""

    payload: dict[str, Any] = {
        "conversation_id": f"thread-{_uid()}",
        "author": author,
        "summary": "Confirm the requested action",
        "reply_kind": "email",
        "reply_channel": agent["inbox"],
        "reply_placeholder": None,
        "reply_endpoint": EMAIL_ENDPOINT,
        "reply_adapter": EMAIL_ADAPTER,
        "dedupe_key": uuid.uuid4().hex,
        "agent_id": agent["agent_id"],
        "route": route,
        "card_channel": card_channel if card_channel is not None else agent["inbox"],
        "gate_kind": "policy",
    }
    response = client.post("/approvals", json=payload, headers=auth)
    assert response.status_code == 201, response.text
    return response.json()


def _adapter_token(bindings: list[str]) -> str:
    return adapter_principal.mint(
        get_settings().api_key,
        subject=ADAPTER_SUBJECT,
        bindings=bindings,
        exp=int(time.time()) + 600,
    )


def _adp(token: str, actor: str) -> dict[str, str]:
    return {ADAPTER_HEADER: token, ACTOR_HEADER: actor}


def _resolve(
    client: TestClient, approval_id: str, headers: dict[str, str], decision: str = "approved"
) -> Any:
    return client.post(
        f"/approvals/{approval_id}/resolve",
        json={"decision": decision, "note": "looks right"},
        headers=headers,
    )


def _status(client: TestClient, auth: dict[str, str], approval_id: str) -> str:
    return str(client.get(f"/approvals/{approval_id}", headers=auth).json()["status"])


def _audit(client: TestClient, auth: dict[str, str], approval_id: str) -> list[dict[str, Any]]:
    response = client.get(f"/approvals/{approval_id}/audit", headers=auth)
    assert response.status_code == 200, response.text
    return list(response.json())


def _listed(client: TestClient, token: str) -> set[str]:
    response = client.get("/approvals", headers={ADAPTER_HEADER: token})
    assert response.status_code == 200, response.text
    return {row["id"] for row in response.json()}


# --- 1. the route mode --------------------------------------------------------


def test_route_mode_is_stored_and_read_back_as_written(
    surface_client: TestClient, auth_headers: dict[str, str], clean_db: None
) -> None:
    agent = _email_agent(
        surface_client,
        auth_headers,
        routes={
            "confirm": {"resolution": REQUESTING_SURFACE},
            "fixed": {"resolution": {"kind": "slack", "address": "C0EXAMPLE1"}},
        },
    )

    shown = surface_client.get(f"/agents/{agent['agent_id']}", headers=auth_headers)
    assert shown.status_code == 200, shown.text
    routes = shown.json()["approval_routes"]
    assert routes["confirm"] == {
        "resolution": REQUESTING_SURFACE,
        "notification": None,
        "approvers": None,
    }
    assert routes["fixed"]["resolution"] == {"kind": "slack", "address": "C0EXAMPLE1"}
    # The stored JSONB, which the worker re-parses strictly, is the mode alone.
    assert _stored_routes(agent["agent_id"])["confirm"] == {"resolution": REQUESTING_SURFACE}


@pytest.mark.parametrize(
    ("binding", "why"),
    [
        (
            {
                "resolution": REQUESTING_SURFACE,
                "notification": {"kind": "slack", "address": "C0EXAMPLE1"},
            },
            "cannot carry a notification",
        ),
        ({"resolution": {**REQUESTING_SURFACE, "kind": "slack", "address": "C0EXAMPLE1"}}, None),
        ({"resolution": {"mode": "anywhere"}}, None),
        ({"resolution": {"mode": "requesting_surface", "extra": 1}}, None),
        # A fixed target stays Slack-only: the mode is the only way off Slack.
        (
            {
                "resolution": {
                    "kind": "email",
                    "address": "approvals@example.com",
                }
            },
            None,
        ),
    ],
)
def test_route_mode_refuses_every_other_shape(
    surface_client: TestClient,
    auth_headers: dict[str, str],
    clean_db: None,
    binding: dict[str, Any],
    why: str | None,
) -> None:
    created = surface_client.post(
        "/agents",
        json={
            "name": f"surface-bad-{_uid()}",
            "channel": {"kind": "slack", "address": "C0EXAMPLE2"},
            "approval_routes": {"confirm": binding},
        },
        headers=auth_headers,
    )
    assert created.status_code == 422, created.text
    if why is not None:
        assert why in created.text


# --- 2. served: the card went to one of the adapter's own bindings ------------


def test_adapter_is_served_approvals_asked_on_its_own_binding_only(
    surface_client: TestClient, auth_headers: dict[str, str], clean_db: None
) -> None:
    agent = _email_agent(
        surface_client,
        auth_headers,
        routes={
            "confirm": {"resolution": REQUESTING_SURFACE},
            "finance": {"resolution": {"kind": "slack", "address": "C0EXAMPLE1"}},
        },
    )
    other = _email_agent(surface_client, auth_headers)
    routeless = _email_approval(surface_client, auth_headers, agent)
    moded = _email_approval(surface_client, auth_headers, agent, route="confirm")
    # A fixed Slack route shows its card in Slack, not in this email thread.
    fixed = _email_approval(
        surface_client, auth_headers, agent, route="finance", card_channel="C0EXAMPLE1"
    )
    elsewhere = _email_approval(surface_client, auth_headers, other)
    token = _adapter_token([agent["binding_id"]])

    assert _listed(surface_client, token) == {routeless["id"], moded["id"]}

    for approval in (fixed, elsewhere):
        refused = _resolve(surface_client, approval["id"], _adp(token, REQUESTER))
        assert refused.status_code == 404, refused.text
        assert refused.json()["detail"] == NOT_FOUND
        assert _status(surface_client, auth_headers, approval["id"]) == "pending"
        assert _audit(surface_client, auth_headers, approval["id"]) == []


def test_one_agent_two_inboxes_the_adapter_serves_only_its_own_thread(
    surface_client: TestClient, auth_headers: dict[str, str], clean_db: None
) -> None:
    """The served match is the asking PAIR, not merely a shared agent: an
    adapter serving inbox A must not see or answer an approval asked on inbox
    B of the same agent."""

    agent = _email_agent(surface_client, auth_headers)
    inbox_b = f"second-{_uid()}@example.com"
    added = surface_client.post(
        f"/agents/{agent['agent_id']}/channels",
        json={
            "kind": "email",
            "address": inbox_b,
            "endpoint": EMAIL_ENDPOINT,
            "adapter": EMAIL_ADAPTER,
        },
        headers=auth_headers,
    )
    assert added.status_code == 201, added.text
    on_a = _email_approval(surface_client, auth_headers, agent, route=LISTED_ROUTE)
    on_b = _email_approval(
        surface_client, auth_headers, {**agent, "inbox": inbox_b}, route=LISTED_ROUTE
    )
    token = _adapter_token([agent["binding_id"]])

    assert _listed(surface_client, token) == {on_a["id"]}
    refused = _resolve(surface_client, on_b["id"], _adp(token, APPROVER))
    assert refused.status_code == 404, refused.text
    assert _status(surface_client, auth_headers, on_b["id"]) == "pending"
    accepted = _resolve(surface_client, on_a["id"], _adp(token, APPROVER))
    assert accepted.status_code == 200, accepted.text


def test_a_card_recorded_elsewhere_is_not_served_from_the_asking_binding(
    surface_client: TestClient, auth_headers: dict[str, str], clean_db: None
) -> None:
    """The row says where the card went. A routeless email approval whose card
    was recorded in another place is not the asking binding's to answer."""

    agent = _email_agent(surface_client, auth_headers)
    approval = _email_approval(
        surface_client, auth_headers, agent, card_channel="someone-else@example.com"
    )
    token = _adapter_token([agent["binding_id"]])

    assert _listed(surface_client, token) == set()
    refused = _resolve(surface_client, approval["id"], _adp(token, REQUESTER))
    assert refused.status_code == 404, refused.text


def _repoint(client: TestClient, auth: dict[str, str], agent_id: str, binding: dict) -> None:
    moved = client.patch(
        f"/agents/{agent_id}", json={"approval_routes": {"confirm": binding}}, headers=auth
    )
    assert moved.status_code == 200, moved.text


def test_a_route_repointed_after_the_ask_leaves_the_card_where_it_was_shown(
    surface_client: TestClient, auth_headers: dict[str, str], clean_db: None
) -> None:
    """The record says where the card went; re-pointing the route later does
    not move it. The email card stays answerable only in the email thread, and
    a listed Slack user (here through an operator token) cannot answer it."""

    listed = {"resolution": REQUESTING_SURFACE, "approvers": {"emails": [REQUESTER]}}
    agent = _email_agent(surface_client, auth_headers, routes={"confirm": listed})
    approval = _email_approval(surface_client, auth_headers, agent, route="confirm")
    token = _adapter_token([agent["binding_id"]])
    _repoint(
        surface_client,
        auth_headers,
        agent["agent_id"],
        {
            "resolution": {"kind": "slack", "address": "C0EXAMPLE1"},
            "approvers": {"users": ["U0EXAMPLE1"]},
        },
    )

    assert _listed(surface_client, token) == {approval["id"]}
    operator = approval_principal.mint(
        get_settings().api_key,
        subject="U0EXAMPLE1",
        kind="operator",
        scope=approval_principal.APPROVE_SCOPE,
        exp=int(time.time()) + 60,
    )
    refused = _resolve(surface_client, approval["id"], {PRINCIPAL_HEADER: operator})
    assert refused.status_code == 403, refused.text
    # The new approvers are Slack users nobody on the email thread can prove
    # to be, and the route lists no emails now: it fails closed.
    closed = _resolve(surface_client, approval["id"], _adp(token, REQUESTER))
    assert closed.status_code == 403, closed.text
    assert _status(surface_client, auth_headers, approval["id"]) == "pending"

    # Re-pointed without approvers, nobody is listed either (ADR 0183: no
    # requester-only default), so the email card still admits nobody.
    _repoint(
        surface_client,
        auth_headers,
        agent["agent_id"],
        {"resolution": {"kind": "slack", "address": "C0EXAMPLE1"}},
    )
    still_closed = _resolve(surface_client, approval["id"], _adp(token, REQUESTER))
    assert still_closed.status_code == 403, still_closed.text

    # The list is read fresh: listing the address again lets it answer.
    _repoint(surface_client, auth_headers, agent["agent_id"], listed)
    accepted = _resolve(surface_client, approval["id"], _adp(token, REQUESTER))
    assert accepted.status_code == 200, accepted.text


def test_a_repointed_route_does_not_hand_the_approval_to_the_new_target(
    surface_client: TestClient, auth_headers: dict[str, str], clean_db: None
) -> None:
    """An adapter serving the route's NEW fixed target must not see an approval
    whose card was shown somewhere else."""

    channel = f"C0NEW{_uid().upper()}"
    agent = _email_agent(
        surface_client, auth_headers, routes={"confirm": {"resolution": REQUESTING_SURFACE}}
    )
    added = surface_client.post(
        f"/agents/{agent['agent_id']}/channels",
        json={"kind": "slack", "address": channel},
        headers=auth_headers,
    )
    assert added.status_code == 201, added.text
    approval = _email_approval(surface_client, auth_headers, agent, route="confirm")
    _repoint(
        surface_client,
        auth_headers,
        agent["agent_id"],
        {"resolution": {"kind": "slack", "address": channel}},
    )
    slack_token = _adapter_token([_binding_ids(agent["agent_id"])[channel]])

    assert _listed(surface_client, slack_token) == set()
    refused = _resolve(surface_client, approval["id"], _adp(slack_token, REQUESTER))
    assert refused.status_code == 404, refused.text


def test_a_fixed_route_repointed_to_an_adapters_binding_does_not_serve_the_old_card(
    surface_client: TestClient, auth_headers: dict[str, str], clean_db: None
) -> None:
    """A Slack card posted to one fixed channel, then the route moved to
    another: an adapter serving the new channel must not list it."""

    asking, old_target, new_target = (f"C0{tag}{_uid().upper()}" for tag in ("ASK", "OLD", "NEW"))
    created = surface_client.post(
        "/agents",
        json={
            "name": f"surface-fixed-{_uid()}",
            "channel": {"kind": "slack", "address": asking},
            "approval_routes": {"ops": {"resolution": {"kind": "slack", "address": old_target}}},
        },
        headers=auth_headers,
    )
    assert created.status_code == 201, created.text
    agent_id = str(created.json()["id"])
    added = surface_client.post(
        f"/agents/{agent_id}/channels",
        json={"kind": "slack", "address": new_target},
        headers=auth_headers,
    )
    assert added.status_code == 201, added.text
    response = surface_client.post(
        "/approvals",
        json={
            "conversation_id": f"th-{_uid()}",
            "author": "U0EXAMPLE2",
            "summary": "Confirm the requested action",
            "reply_kind": "slack",
            "reply_channel": asking,
            "reply_placeholder": "p-1",
            "dedupe_key": uuid.uuid4().hex,
            "agent_id": agent_id,
            "route": "ops",
            "card_channel": old_target,
            "gate_kind": "policy",
        },
        headers=auth_headers,
    )
    assert response.status_code == 201, response.text
    moved = surface_client.patch(
        f"/agents/{agent_id}",
        json={"approval_routes": {"ops": {"resolution": {"kind": "slack", "address": new_target}}}},
        headers=auth_headers,
    )
    assert moved.status_code == 200, moved.text

    token = _adapter_token([_binding_ids(agent_id)[new_target]])
    assert _listed(surface_client, token) == set()


def test_a_slack_shaped_asking_address_is_never_read_as_the_asking_card(
    surface_client: TestClient, auth_headers: dict[str, str], clean_db: None
) -> None:
    """The record keeps the card's address but not its kind. When a non-Slack
    asking address is shaped like the Slack channel a fixed route posted to,
    the card may be that Slack card, and no adapter may answer a Slack card,
    even after the route is switched to requesting_surface."""

    shared = f"C0SHR{_uid().upper()}"
    created = surface_client.post(
        "/agents",
        json={
            "name": f"surface-shared-{_uid()}",
            "channel": {
                "kind": "webchat",
                "address": shared,
                "endpoint": EMAIL_ENDPOINT,
                "adapter": EMAIL_ADAPTER,
            },
            "approval_routes": {"confirm": {"resolution": {"kind": "slack", "address": shared}}},
        },
        headers=auth_headers,
    )
    assert created.status_code == 201, created.text
    agent_id = str(created.json()["id"])
    response = surface_client.post(
        "/approvals",
        json={
            "conversation_id": f"th-{_uid()}",
            "author": REQUESTER,
            "summary": "Confirm the requested action",
            "reply_kind": "webchat",
            "reply_channel": shared,
            "reply_placeholder": None,
            "reply_endpoint": EMAIL_ENDPOINT,
            "reply_adapter": EMAIL_ADAPTER,
            "dedupe_key": uuid.uuid4().hex,
            "agent_id": agent_id,
            "route": "confirm",
            "card_channel": shared,
            "gate_kind": "policy",
        },
        headers=auth_headers,
    )
    assert response.status_code == 201, response.text
    approval = response.json()
    _repoint(surface_client, auth_headers, agent_id, {"resolution": REQUESTING_SURFACE})
    token = _adapter_token([_binding_ids(agent_id)[shared]])

    assert _listed(surface_client, token) == set()
    refused = _resolve(surface_client, approval["id"], _adp(token, REQUESTER))
    assert refused.status_code == 404, refused.text
    assert _status(surface_client, auth_headers, approval["id"]) == "pending"


# --- 3. the approver email list (ADR 0183) -----------------------------------


def _set_callers(
    client: TestClient, auth: dict[str, str], agent: dict[str, str], callers: list[str] | None
) -> None:
    """Set the asking binding's inbound allowlist (ADR 0175)."""

    response = client.put(
        f"/agents/{agent['agent_id']}/channels/callers",
        params={"kind": "email", "address": agent["inbox"], "adapter": EMAIL_ADAPTER},
        json={"allowed_callers": callers},
        headers=auth,
    )
    assert response.status_code == 200, response.text


def _write_routes_raw(agent_id: str, routes: dict[str, Any]) -> None:
    """Write the route map around the API, as an out-of-band JSONB edit would."""

    async def run() -> None:
        engine = create_async_engine(get_settings().database_url)
        try:
            async with engine.begin() as conn:
                await conn.execute(
                    sql_text(
                        "UPDATE curie.agents SET approval_routes = CAST(:routes AS jsonb) "
                        "WHERE id = :aid"
                    ),
                    {"routes": json.dumps(routes), "aid": agent_id},
                )
        finally:
            await engine.dispose()

    asyncio.run(run())


@pytest.mark.parametrize("actor", [APPROVER, f"  {APPROVER.upper()} "])
def test_a_listed_verified_sender_answers_and_the_session_resumes(
    surface_client: TestClient,
    auth_headers: dict[str, str],
    clean_db: None,
    valkey: redis.Redis,
    runs_stream: str,
    actor: str,
) -> None:
    """The listed approver is not the person who asked: anyone on the list may
    answer. The match ignores case and the adapter's surrounding whitespace,
    and nothing else."""

    agent = _email_agent(surface_client, auth_headers)
    approval = _email_approval(surface_client, auth_headers, agent, route=LISTED_ROUTE)
    token = _adapter_token([agent["binding_id"]])

    accepted = _resolve(surface_client, approval["id"], _adp(token, actor))
    assert accepted.status_code == 200, accepted.text
    assert accepted.json()["status"] == "approved"
    assert accepted.json()["resolution_note"] == "looks right"

    audit = _audit(surface_client, auth_headers, approval["id"])
    assert len(audit) == 1
    assert audit[0]["action"] == "resolved"
    assert audit[0]["authorizer"] == "EmailApproverList"
    assert audit[0]["principal_kind"] == "adapter"
    assert audit[0]["principal_subject"] == ADAPTER_SUBJECT
    assert audit[0]["evidence"] == {
        "kind": "email_list",
        "emails": [APPROVER],
        "actor_listed": True,
    }
    # The answer wakes the paused session; it is never a new turn of its own.
    entries = valkey.xrange(runs_stream)
    assert len(entries) == 1
    assert f"approval-{approval['id']}-resolved" in str(entries[0][1])

    # A replayed answer loses: the first one won.
    again = _resolve(surface_client, approval["id"], _adp(token, APPROVER), "rejected")
    assert again.status_code == 409, again.text


@pytest.mark.parametrize("actor", [COPIED, REQUESTER, f"{APPROVER}.example.net"])
def test_an_unlisted_sender_the_inbox_admits_cannot_answer(
    surface_client: TestClient,
    auth_headers: dict[str, str],
    clean_db: None,
    valkey: redis.Redis,
    runs_stream: str,
    actor: str,
) -> None:
    """Being allowed to talk to the bot is not being allowed to approve. Every
    actor here is on the binding's ``allowed_callers``, and none is on the
    approver list: a copied person, the person who asked (no requester-only
    default), and a near miss of the listed address."""

    agent = _email_agent(surface_client, auth_headers)
    _set_callers(surface_client, auth_headers, agent, [COPIED, REQUESTER, APPROVER, actor])
    approval = _email_approval(surface_client, auth_headers, agent, route=LISTED_ROUTE)
    token = _adapter_token([agent["binding_id"]])

    denied = _resolve(surface_client, approval["id"], _adp(token, actor))
    assert denied.status_code == 403, denied.text
    assert "not an approver" in denied.json()["detail"]
    assert _status(surface_client, auth_headers, approval["id"]) == "pending"
    audit = _audit(surface_client, auth_headers, approval["id"])
    assert [(row["action"], row["authorizer"]) for row in audit] == [
        ("denied", "EmailApproverList")
    ]
    assert audit[0]["evidence"]["actor_listed"] is False
    assert valkey.xrange(runs_stream) == []


def test_a_sender_outside_allowed_callers_is_refused_before_the_approver_list(
    surface_client: TestClient,
    auth_headers: dict[str, str],
    clean_db: None,
    valkey: redis.Redis,
    runs_stream: str,
) -> None:
    """ADR 0183 decision 2: the inbound allowlist comes first. A listed approver
    the binding does not admit is refused with the channel port's own refusal,
    before any approval logic, so no audit row is written. Admitting them then
    lets the list decide."""

    agent = _email_agent(surface_client, auth_headers)
    _set_callers(surface_client, auth_headers, agent, [REQUESTER])
    approval = _email_approval(surface_client, auth_headers, agent, route=LISTED_ROUTE)
    token = _adapter_token([agent["binding_id"]])

    refused = _resolve(surface_client, approval["id"], _adp(token, APPROVER))
    assert refused.status_code == 403, refused.text
    assert refused.json()["detail"] == "caller_not_allowed"
    assert _status(surface_client, auth_headers, approval["id"]) == "pending"
    assert _audit(surface_client, auth_headers, approval["id"]) == []
    assert valkey.xrange(runs_stream) == []

    _set_callers(surface_client, auth_headers, agent, [REQUESTER, APPROVER.upper()])
    accepted = _resolve(surface_client, approval["id"], _adp(token, APPROVER))
    assert accepted.status_code == 200, accepted.text


def test_a_routeless_email_approval_admits_nobody(
    surface_client: TestClient, auth_headers: dict[str, str], clean_db: None
) -> None:
    """A routeless approval has no binding, so no approver list. The person who
    asked is no longer admitted by default (ADR 0183 decision 3)."""

    agent = _email_agent(surface_client, auth_headers)
    approval = _email_approval(surface_client, auth_headers, agent)
    token = _adapter_token([agent["binding_id"]])

    for actor in (REQUESTER, APPROVER):
        denied = _resolve(surface_client, approval["id"], _adp(token, actor))
        assert denied.status_code == 403, denied.text
        assert "only an address on the approval's approver list" in denied.json()["detail"]
    assert _status(surface_client, auth_headers, approval["id"]) == "pending"
    audit = _audit(surface_client, auth_headers, approval["id"])
    assert {row["authorizer"] for row in audit} == {"NoVerifiableApprovers"}
    assert audit[0]["evidence"] == {
        "kind": "no_verifiable_approvers",
        "surface_kind": "email",
        "slack_approvers_declared": False,
    }


def test_an_empty_email_list_is_refused_when_written_and_admits_nobody_when_read(
    surface_client: TestClient, auth_headers: dict[str, str], clean_db: None
) -> None:
    written = surface_client.post(
        "/agents",
        json={
            "name": f"surface-empty-{_uid()}",
            "channel": {"kind": "slack", "address": "C0EXAMPLE2"},
            "approval_routes": {
                "confirm": {"resolution": REQUESTING_SURFACE, "approvers": {"emails": []}}
            },
        },
        headers=auth_headers,
    )
    assert written.status_code == 422, written.text
    assert "at least one address" in written.text

    # Written around the API, the empty list reaches the resolver anyway. It
    # must not read as "no list" and fall back to anyone.
    agent = _email_agent(surface_client, auth_headers)
    approval = _email_approval(surface_client, auth_headers, agent, route=LISTED_ROUTE)
    _write_routes_raw(
        agent["agent_id"],
        {LISTED_ROUTE: {"resolution": REQUESTING_SURFACE, "approvers": {"emails": []}}},
    )
    token = _adapter_token([agent["binding_id"]])
    for actor in (APPROVER, REQUESTER):
        denied = _resolve(surface_client, approval["id"], _adp(token, actor))
        assert denied.status_code == 403, denied.text
    assert _status(surface_client, auth_headers, approval["id"]) == "pending"
    assert {row["authorizer"] for row in _audit(surface_client, auth_headers, approval["id"])} == {
        "InvalidApproversSpec"
    }


@pytest.mark.parametrize(
    ("emails", "why"),
    [
        (["Approver <approver@example.com>"], "not one bare email address"),
        (["*@example.com"], "not one bare email address"),
        (["example.com"], "not one bare email address"),
        (["a@example.com, b@example.com"], "not one bare email address"),
    ],
)
def test_approver_emails_must_be_bare_addresses(
    surface_client: TestClient,
    auth_headers: dict[str, str],
    clean_db: None,
    emails: list[str],
    why: str,
) -> None:
    created = surface_client.post(
        "/agents",
        json={
            "name": f"surface-bad-email-{_uid()}",
            "channel": {"kind": "slack", "address": "C0EXAMPLE2"},
            "approval_routes": {
                "confirm": {"resolution": REQUESTING_SURFACE, "approvers": {"emails": emails}}
            },
        },
        headers=auth_headers,
    )
    assert created.status_code == 422, created.text
    assert why in created.text


def test_approver_emails_need_a_requesting_surface_route(
    surface_client: TestClient, auth_headers: dict[str, str], clean_db: None
) -> None:
    """A fixed target is a Slack channel, where an address is never proof."""

    created = surface_client.post(
        "/agents",
        json={
            "name": f"surface-fixed-email-{_uid()}",
            "channel": {"kind": "slack", "address": "C0EXAMPLE2"},
            "approval_routes": {
                "finance": {
                    "resolution": {"kind": "slack", "address": "C0EXAMPLE1"},
                    "approvers": {"emails": [APPROVER]},
                }
            },
        },
        headers=auth_headers,
    )
    assert created.status_code == 422, created.text
    assert "need a requesting_surface resolution" in created.text


def test_approver_emails_are_stored_lowercase_once_and_read_back(
    surface_client: TestClient, auth_headers: dict[str, str], clean_db: None
) -> None:
    agent = _email_agent(
        surface_client,
        auth_headers,
        routes={
            "confirm": {
                "resolution": REQUESTING_SURFACE,
                "approvers": {
                    "users": ["U0EXAMPLE1"],
                    "emails": ["Approver@Example.com", APPROVER, COPIED],
                },
            }
        },
    )

    stored = _stored_routes(agent["agent_id"])["confirm"]["approvers"]
    assert stored == {"users": ["U0EXAMPLE1"], "emails": [APPROVER, COPIED]}
    shown = surface_client.get(f"/agents/{agent['agent_id']}", headers=auth_headers)
    assert shown.json()["approval_routes"]["confirm"]["approvers"]["emails"] == [
        APPROVER,
        COPIED,
    ]


def test_no_operator_console_or_slack_principal_can_answer_for_a_listed_address(
    surface_client: TestClient, auth_headers: dict[str, str], clean_db: None
) -> None:
    """Each one presents the listed address as its subject, so the only thing
    that can refuse it is the set's principal eligibility."""

    agent = _email_agent(surface_client, auth_headers)
    approval = _email_approval(surface_client, auth_headers, agent, route=LISTED_ROUTE)
    now = int(time.time())

    operator = approval_principal.mint(
        get_settings().api_key,
        subject=APPROVER,
        kind="operator",
        scope=approval_principal.APPROVE_SCOPE,
        exp=now + 60,
    )
    chat = approval_principal.mint(
        get_settings().approval_chat_attester_secret,
        subject=APPROVER,
        kind="chat",
        actor_channel=agent["inbox"],
        approval_id=approval["id"],
        scope=approval_principal.APPROVE_SCOPE,
        exp=now + 60,
    )
    login = surface_client.post(
        "/console/login-codes", json={"subject": APPROVER}, headers=auth_headers
    )
    assert login.status_code == 201, login.text
    session = surface_client.post("/console/session", json={"code": login.json()["code"]})
    assert session.status_code == 200, session.text
    console = surface_client.cookies.get(SESSION_COOKIE)
    assert console
    surface_client.cookies.clear()

    for kind, headers in (
        ("operator", {PRINCIPAL_HEADER: operator}),
        ("chat", {PRINCIPAL_HEADER: chat}),
        # Same-origin, so the console session itself is accepted and the
        # refusal comes from the approver set, not the origin check.
        ("console", {"Cookie": f"{SESSION_COOKIE}={console}", "Origin": "http://testserver"}),
    ):
        denied = _resolve(surface_client, approval["id"], headers)
        assert denied.status_code == 403, (kind, denied.text)
        assert "only an address on this approval's approver list" in denied.json()["detail"]
    assert _status(surface_client, auth_headers, approval["id"]) == "pending"
    audit = _audit(surface_client, auth_headers, approval["id"])
    assert [(row["principal_kind"], row["authorized"]) for row in audit] == [
        ("operator", False),
        ("chat", False),
        ("console", False),
    ]
    assert all(row["evidence"]["kind"] == "principal_set_eligibility" for row in audit)


def test_slack_approvers_added_to_a_moded_route_after_the_ask_fail_closed(
    surface_client: TestClient, auth_headers: dict[str, str], clean_db: None
) -> None:
    """The worker escalates an email approval whose route lists no emails, so
    no such approval is created. If the operator swaps the emails for Slack
    users while one pends, nobody on the email thread can prove to be one: it
    admits nobody rather than falling back to anyone."""

    agent = _email_agent(surface_client, auth_headers)
    approval = _email_approval(surface_client, auth_headers, agent, route=LISTED_ROUTE)
    _repoint_named(
        surface_client,
        auth_headers,
        agent["agent_id"],
        LISTED_ROUTE,
        {"resolution": REQUESTING_SURFACE, "approvers": {"users": ["U0EXAMPLE1"]}},
    )
    token = _adapter_token([agent["binding_id"]])

    denied = _resolve(surface_client, approval["id"], _adp(token, APPROVER))
    assert denied.status_code == 403, denied.text
    assert _status(surface_client, auth_headers, approval["id"]) == "pending"
    audit = _audit(surface_client, auth_headers, approval["id"])
    assert audit[-1]["authorizer"] == "NoVerifiableApprovers"
    assert audit[-1]["evidence"]["slack_approvers_declared"] is True


def _repoint_named(
    client: TestClient, auth: dict[str, str], agent_id: str, name: str, binding: dict
) -> None:
    moved = client.patch(
        f"/agents/{agent_id}", json={"approval_routes": {name: binding}}, headers=auth
    )
    assert moved.status_code == 200, moved.text


def _slack_moded_approval(
    client: TestClient, auth: dict[str, str], approvers: dict[str, Any] | None
) -> tuple[str, str, dict[str, Any]]:
    """An agent on a Slack channel whose route is in requesting_surface mode,
    and an approval asked in that channel: ``(agent_id, channel, approval)``."""

    channel = f"C0SURF{_uid().upper()}"
    route: dict[str, Any] = {"resolution": REQUESTING_SURFACE}
    if approvers is not None:
        route["approvers"] = approvers
    created = client.post(
        "/agents",
        json={
            "name": f"surface-slack-{_uid()}",
            "channel": {"kind": "slack", "address": channel},
            "approval_routes": {"confirm": route},
        },
        headers=auth,
    )
    assert created.status_code == 201, created.text
    agent_id = str(created.json()["id"])
    response = client.post(
        "/approvals",
        json={
            "conversation_id": f"th-{_uid()}",
            "author": "U0EXAMPLE2",
            "summary": "Confirm the requested action",
            "reply_kind": "slack",
            "reply_channel": channel,
            "reply_placeholder": "p-1",
            "dedupe_key": uuid.uuid4().hex,
            "agent_id": agent_id,
            "route": "confirm",
            "card_channel": channel,
            "gate_kind": "policy",
        },
        headers=auth,
    )
    assert response.status_code == 201, response.text
    return agent_id, channel, response.json()


def _chat_click(approval_id: str, subject: str, from_channel: str) -> dict[str, str]:
    return {
        PRINCIPAL_HEADER: approval_principal.mint(
            get_settings().approval_chat_attester_secret,
            subject=subject,
            kind="chat",
            actor_channel=from_channel,
            approval_id=approval_id,
            scope=approval_principal.APPROVE_SCOPE,
            exp=int(time.time()) + 60,
        )
    }


def test_a_slack_card_whose_route_lists_only_emails_admits_nobody(
    surface_client: TestClient, auth_headers: dict[str, str], clean_db: None
) -> None:
    """ADR 0183 decision 4: an address is never proof on Slack, and a route that
    narrowed its approvers to emails must not widen back to channel membership
    when it is asked in Slack."""

    _agent_id, channel, approval = _slack_moded_approval(
        surface_client, auth_headers, {"emails": [APPROVER]}
    )

    member = _resolve(surface_client, approval["id"], _chat_click(approval["id"], "U0EX1", channel))
    assert member.status_code == 403, member.text
    assert _status(surface_client, auth_headers, approval["id"]) == "pending"
    audit = _audit(surface_client, auth_headers, approval["id"])
    assert audit[-1]["authorizer"] == "InvalidApproversSpec"


@pytest.mark.parametrize("actor", [APPROVER, "U0EXAMPLE1"])
def test_an_adapter_principal_is_still_refused_on_a_slack_approver_set(
    surface_client: TestClient, auth_headers: dict[str, str], clean_db: None, actor: str
) -> None:
    """A route listing both Slack users and emails, asked in Slack: the Slack
    card reads only the Slack users, and an adapter serving the Slack binding
    cannot answer it by naming either a listed address or a listed Slack id
    (ADR-0177, "A separate finding")."""

    agent_id, channel, approval = _slack_moded_approval(
        surface_client, auth_headers, {"users": ["U0EXAMPLE1"], "emails": [APPROVER]}
    )
    token = _adapter_token([_binding_ids(agent_id)[channel]])

    denied = _resolve(surface_client, approval["id"], _adp(token, actor))
    assert denied.status_code == 403, denied.text
    assert "only a Slack click can prove a Slack identity" in denied.json()["detail"]
    audit = _audit(surface_client, auth_headers, approval["id"])
    assert [(row["authorizer"], row["principal_kind"]) for row in audit] == [
        ("ExplicitUserListAuthorizer", "adapter")
    ]
    listed = _resolve(
        surface_client, approval["id"], _chat_click(approval["id"], "U0EXAMPLE1", "C0EXAMPLE9")
    )
    assert listed.status_code == 200, listed.text


def test_a_moded_route_asked_in_slack_keeps_slack_channel_membership(
    surface_client: TestClient, auth_headers: dict[str, str], clean_db: None
) -> None:
    """Decision 2: a card shown in Slack is answered in Slack by the same
    people as today. The mode only changes WHERE the card goes."""

    channel = f"C0SURF{_uid().upper()}"
    created = surface_client.post(
        "/agents",
        json={
            "name": f"surface-slack-{_uid()}",
            "channel": {"kind": "slack", "address": channel},
            "approval_routes": {"confirm": {"resolution": REQUESTING_SURFACE}},
        },
        headers=auth_headers,
    )
    assert created.status_code == 201, created.text
    agent_id = str(created.json()["id"])
    response = surface_client.post(
        "/approvals",
        json={
            "conversation_id": f"th-{_uid()}",
            "author": "U0EXAMPLE2",
            "summary": "Confirm the requested action",
            "reply_kind": "slack",
            "reply_channel": channel,
            "reply_placeholder": "p-1",
            "dedupe_key": uuid.uuid4().hex,
            "agent_id": agent_id,
            "route": "confirm",
            "card_channel": channel,
            "gate_kind": "policy",
        },
        headers=auth_headers,
    )
    assert response.status_code == 201, response.text
    approval = response.json()
    now = int(time.time())

    def chat(from_channel: str) -> dict[str, str]:
        return {
            PRINCIPAL_HEADER: approval_principal.mint(
                get_settings().approval_chat_attester_secret,
                subject="U0EXAMPLE1",
                kind="chat",
                actor_channel=from_channel,
                approval_id=approval["id"],
                scope=approval_principal.APPROVE_SCOPE,
                exp=now + 60,
            )
        }

    elsewhere = _resolve(surface_client, approval["id"], chat("C0EXAMPLE9"))
    assert elsewhere.status_code == 403, elsewhere.text
    member = _resolve(surface_client, approval["id"], chat(channel))
    assert member.status_code == 200, member.text
    audit = _audit(surface_client, auth_headers, approval["id"])
    assert audit[-1]["authorizer"] == "ChannelMembershipAuthorizer"


def _binding_id_for(agent_id: str, address: str, adapter: str) -> str:
    async def run() -> str:
        engine = create_async_engine(get_settings().database_url)
        try:
            async with engine.connect() as conn:
                result = await conn.execute(
                    sql_text(
                        "SELECT id FROM curie.agent_channels "
                        "WHERE agent_id = :aid AND address = :address AND adapter = :adapter"
                    ),
                    {"aid": agent_id, "address": address, "adapter": adapter},
                )
                return str(result.scalar_one())
        finally:
            await engine.dispose()

    return asyncio.run(run())


def test_two_adapters_on_one_address_serve_only_their_own_card(
    surface_client: TestClient, auth_headers: dict[str, str], clean_db: None
) -> None:
    """One agent may bind the same (kind, address) under two adapters
    (ADR-0168 decision 3). The asking route includes the adapter, so the other
    adapter's credential can neither list nor answer the card, even naming the
    requester."""

    agent = _email_agent(surface_client, auth_headers)
    other_adapter = "agentmail-other"
    added = surface_client.post(
        f"/agents/{agent['agent_id']}/channels",
        json={
            "kind": "email",
            "address": agent["inbox"],
            "endpoint": EMAIL_ENDPOINT,
            "adapter": other_adapter,
        },
        headers=auth_headers,
    )
    assert added.status_code == 201, added.text
    asking = _binding_id_for(agent["agent_id"], agent["inbox"], EMAIL_ADAPTER)
    other = _binding_id_for(agent["agent_id"], agent["inbox"], other_adapter)
    assert asking != other
    approval = _email_approval(surface_client, auth_headers, agent, route=LISTED_ROUTE)

    other_token = _adapter_token([other])
    assert _listed(surface_client, other_token) == set()
    refused = _resolve(surface_client, approval["id"], _adp(other_token, APPROVER))
    assert refused.status_code == 404, refused.text
    assert _status(surface_client, auth_headers, approval["id"]) == "pending"

    asking_token = _adapter_token([asking])
    assert _listed(surface_client, asking_token) == {approval["id"]}
    accepted = _resolve(surface_client, approval["id"], _adp(asking_token, APPROVER))
    assert accepted.status_code == 200, accepted.text


def _clone_approval(approval_id: str, copies: int) -> None:
    """Insert ``copies`` newer pending rows shaped exactly like ``approval_id``."""

    async def run() -> None:
        engine = create_async_engine(get_settings().database_url)
        try:
            async with engine.begin() as conn:
                columns = [
                    row.column_name
                    for row in await conn.execute(
                        sql_text(
                            "SELECT column_name FROM information_schema.columns "
                            "WHERE table_schema = 'curie' AND table_name = 'approvals' "
                            "ORDER BY ordinal_position"
                        )
                    )
                ]
                overrides = {
                    "id": "gen_random_uuid()",
                    "dedupe_key": "md5(random()::text || n::text)",
                    "created_at": "created_at + make_interval(secs => n)",
                }
                select_list = ", ".join(overrides.get(c, c) for c in columns)
                await conn.execute(
                    sql_text(
                        f"INSERT INTO curie.approvals ({', '.join(columns)}) "
                        f"SELECT {select_list} FROM curie.approvals, "
                        "generate_series(1, :copies) AS n WHERE id = :aid"
                    ),
                    {"aid": approval_id, "copies": copies},
                )
        finally:
            await engine.dispose()

    asyncio.run(run())


def test_unserved_routeless_rows_do_not_crowd_a_served_card_off_the_list(
    surface_client: TestClient, auth_headers: dict[str, str], clean_db: None
) -> None:
    """Routeless rows asked on a binding the adapter does not serve are
    dropped before the listing's SQL cap, so a busy sibling inbox cannot hide
    this adapter's older card."""

    agent = _email_agent(surface_client, auth_headers)
    inbox_b = f"busy-{_uid()}@example.com"
    added = surface_client.post(
        f"/agents/{agent['agent_id']}/channels",
        json={
            "kind": "email",
            "address": inbox_b,
            "endpoint": EMAIL_ENDPOINT,
            "adapter": EMAIL_ADAPTER,
        },
        headers=auth_headers,
    )
    assert added.status_code == 201, added.text
    served = _email_approval(surface_client, auth_headers, agent)
    busy = _email_approval(surface_client, auth_headers, {**agent, "inbox": inbox_b})
    _clone_approval(busy["id"], 1100)

    assert _listed(surface_client, _adapter_token([agent["binding_id"]])) == {served["id"]}


def test_same_address_rows_under_another_adapter_do_not_crowd_the_list(
    surface_client: TestClient, auth_headers: dict[str, str], clean_db: None
) -> None:
    """Rows asked on the same (kind, address) under another adapter pass the
    SQL pair filter and are dropped only by the served predicate. More of them
    than one read batch must still leave this adapter's older card listed."""

    agent = _email_agent(surface_client, auth_headers)
    other_adapter = "agentmail-other"
    added = surface_client.post(
        f"/agents/{agent['agent_id']}/channels",
        json={
            "kind": "email",
            "address": agent["inbox"],
            "endpoint": EMAIL_ENDPOINT,
            "adapter": other_adapter,
        },
        headers=auth_headers,
    )
    assert added.status_code == 201, added.text
    served = _email_approval(surface_client, auth_headers, agent)
    crowd = surface_client.post(
        "/approvals",
        json={
            "conversation_id": f"thread-{_uid()}",
            "author": REQUESTER,
            "summary": "Confirm the requested action",
            "reply_kind": "email",
            "reply_channel": agent["inbox"],
            "reply_placeholder": None,
            "reply_endpoint": EMAIL_ENDPOINT,
            "reply_adapter": other_adapter,
            "dedupe_key": uuid.uuid4().hex,
            "agent_id": agent["agent_id"],
            "route": None,
            "card_channel": agent["inbox"],
            "gate_kind": "policy",
        },
        headers=auth_headers,
    )
    assert crowd.status_code == 201, crowd.text
    _clone_approval(crowd.json()["id"], 1100)

    token = _adapter_token([_binding_id_for(agent["agent_id"], agent["inbox"], EMAIL_ADAPTER)])
    assert _listed(surface_client, token) == {served["id"]}
