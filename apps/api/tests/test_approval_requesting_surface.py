"""An approval is answered where it was asked, including by email (ADR-0177).

Three things are pinned here, all through the real HTTP surface against real
Postgres and Valkey:

1. A route may name ``{"mode": "requesting_surface"}`` as its card target. It
   stores and reads back as exactly that, it cannot carry a notification, and a
   mix of the mode and a fixed target is refused. A fixed target stays Slack.
2. An adapter principal is served an approval whose card went to one of its
   own bindings: the conversation that asked, routeless or in the new mode.
3. A card shown on a non-Slack surface is answered by the requester alone: the
   sender the serving adapter authenticated, and only when it equals the
   approval's author. Nobody copied on the thread, no operator, no console
   session and no Slack principal can answer it.
"""

from __future__ import annotations

import asyncio
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
EMAIL_ENDPOINT = "http://curie-mail-adapter:8080/"
EMAIL_ADAPTER = "agentmail-sandbox"
REQUESTING_SURFACE = {"mode": "requesting_surface"}
NOT_FOUND = "approval not found"


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
    if routes is not None:
        body["approval_routes"] = routes
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
    on_a = _email_approval(surface_client, auth_headers, agent)
    on_b = _email_approval(surface_client, auth_headers, {**agent, "inbox": inbox_b})
    token = _adapter_token([agent["binding_id"]])

    assert _listed(surface_client, token) == {on_a["id"]}
    refused = _resolve(surface_client, on_b["id"], _adp(token, REQUESTER))
    assert refused.status_code == 404, refused.text
    assert _status(surface_client, auth_headers, on_b["id"]) == "pending"
    accepted = _resolve(surface_client, on_a["id"], _adp(token, REQUESTER))
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

    agent = _email_agent(
        surface_client, auth_headers, routes={"confirm": {"resolution": REQUESTING_SURFACE}}
    )
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
    # to be, so the requester cannot answer either: it fails closed.
    closed = _resolve(surface_client, approval["id"], _adp(token, REQUESTER))
    assert closed.status_code == 403, closed.text
    assert _status(surface_client, auth_headers, approval["id"]) == "pending"

    # Re-pointed without approvers, the card is still the requester's to answer.
    _repoint(
        surface_client,
        auth_headers,
        agent["agent_id"],
        {"resolution": {"kind": "slack", "address": "C0EXAMPLE1"}},
    )
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


# --- 3. the requester-only approver set ---------------------------------------


@pytest.mark.parametrize("route", [None, "confirm"])
def test_the_requester_answers_by_the_serving_adapter_and_the_session_resumes(
    surface_client: TestClient,
    auth_headers: dict[str, str],
    clean_db: None,
    valkey: redis.Redis,
    runs_stream: str,
    route: str | None,
) -> None:
    agent = _email_agent(
        surface_client, auth_headers, routes={"confirm": {"resolution": REQUESTING_SURFACE}}
    )
    approval = _email_approval(surface_client, auth_headers, agent, route=route)
    token = _adapter_token([agent["binding_id"]])

    # Surrounding whitespace is the adapter's formatting, not a second identity.
    accepted = _resolve(surface_client, approval["id"], _adp(token, f"  {REQUESTER} "))
    assert accepted.status_code == 200, accepted.text
    assert accepted.json()["status"] == "approved"
    assert accepted.json()["resolved_by"] == REQUESTER
    assert accepted.json()["resolution_note"] == "looks right"

    audit = _audit(surface_client, auth_headers, approval["id"])
    assert len(audit) == 1
    assert audit[0]["action"] == "resolved"
    assert audit[0]["authorizer"] == "RequesterOnly"
    assert audit[0]["principal_kind"] == "adapter"
    assert audit[0]["principal_subject"] == ADAPTER_SUBJECT
    assert audit[0]["actor"] == REQUESTER
    assert audit[0]["evidence"] == {
        "kind": "requester_only",
        "surface_kind": "email",
        "actor_is_requester": True,
    }
    # The answer wakes the paused session; it is never a new turn of its own.
    entries = valkey.xrange(runs_stream)
    assert len(entries) == 1
    assert f"approval-{approval['id']}-resolved" in str(entries[0][1])

    # A replayed answer loses: the first one won.
    again = _resolve(surface_client, approval["id"], _adp(token, REQUESTER), "rejected")
    assert again.status_code == 409, again.text
    assert f"already resolved by {REQUESTER}" in again.json()["detail"]


def test_a_person_copied_on_the_thread_cannot_answer(
    surface_client: TestClient,
    auth_headers: dict[str, str],
    clean_db: None,
    valkey: redis.Redis,
    runs_stream: str,
) -> None:
    agent = _email_agent(surface_client, auth_headers)
    approval = _email_approval(surface_client, auth_headers, agent)
    token = _adapter_token([agent["binding_id"]])

    denied = _resolve(surface_client, approval["id"], _adp(token, COPIED))
    assert denied.status_code == 403, denied.text
    assert "only the person who asked" in denied.json()["detail"]
    assert _status(surface_client, auth_headers, approval["id"]) == "pending"
    audit = _audit(surface_client, auth_headers, approval["id"])
    assert [(row["action"], row["authorizer"], row["actor"]) for row in audit] == [
        ("denied", "RequesterOnly", COPIED)
    ]
    assert audit[0]["evidence"]["actor_is_requester"] is False
    assert valkey.xrange(runs_stream) == []


def test_no_operator_console_or_slack_principal_can_answer_for_the_requester(
    surface_client: TestClient, auth_headers: dict[str, str], clean_db: None
) -> None:
    """Each one presents the requester's own address as its subject, so the
    only thing that can refuse it is the set's principal eligibility."""

    agent = _email_agent(surface_client, auth_headers)
    approval = _email_approval(surface_client, auth_headers, agent)
    now = int(time.time())

    operator = approval_principal.mint(
        get_settings().api_key,
        subject=REQUESTER,
        kind="operator",
        scope=approval_principal.APPROVE_SCOPE,
        exp=now + 60,
    )
    chat = approval_principal.mint(
        get_settings().approval_chat_attester_secret,
        subject=REQUESTER,
        kind="chat",
        actor_channel=agent["inbox"],
        approval_id=approval["id"],
        scope=approval_principal.APPROVE_SCOPE,
        exp=now + 60,
    )
    login = surface_client.post(
        "/console/login-codes", json={"subject": REQUESTER}, headers=auth_headers
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
        ("console", {"Cookie": f"{SESSION_COOKIE}={console}"}),
    ):
        denied = _resolve(surface_client, approval["id"], headers)
        assert denied.status_code == 403, (kind, denied.text)
        assert "only the person who asked" in denied.json()["detail"], kind
    assert _status(surface_client, auth_headers, approval["id"]) == "pending"
    audit = _audit(surface_client, auth_headers, approval["id"])
    assert [(row["principal_kind"], row["authorized"]) for row in audit] == [
        ("operator", False),
        ("chat", False),
        ("console", False),
    ]
    assert all(row["evidence"]["kind"] == "principal_set_eligibility" for row in audit)


def test_approvers_added_to_a_moded_route_after_the_ask_fail_closed(
    surface_client: TestClient, auth_headers: dict[str, str], clean_db: None
) -> None:
    """The worker escalates a route with approvers that lands on email, so no
    such approval is created. If the operator adds approvers while one pends,
    nobody on the email thread can prove to be a listed Slack user: it admits
    nobody rather than falling back to the requester."""

    agent = _email_agent(
        surface_client, auth_headers, routes={"confirm": {"resolution": REQUESTING_SURFACE}}
    )
    approval = _email_approval(surface_client, auth_headers, agent, route="confirm")
    narrowed = surface_client.patch(
        f"/agents/{agent['agent_id']}",
        json={
            "approval_routes": {
                "confirm": {
                    "resolution": REQUESTING_SURFACE,
                    "approvers": {"users": ["U0EXAMPLE1"]},
                }
            }
        },
        headers=auth_headers,
    )
    assert narrowed.status_code == 200, narrowed.text
    token = _adapter_token([agent["binding_id"]])

    denied = _resolve(surface_client, approval["id"], _adp(token, REQUESTER))
    assert denied.status_code == 403, denied.text
    assert _status(surface_client, auth_headers, approval["id"]) == "pending"
    audit = _audit(surface_client, auth_headers, approval["id"])
    assert audit[-1]["authorizer"] == "InvalidApproversSpec"


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
