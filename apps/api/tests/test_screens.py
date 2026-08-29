"""Screens and buttons against real Postgres and Valkey (ADR-0133).

Same posture as ``test_fleet.py``: the interesting tests are the ones that try
to get a mutation through a path that should not allow it. The rendering tests
exist mostly to keep those honest -- a button that never renders cannot be
attacked, so the suite proves the buttons are there first.
"""

from __future__ import annotations

import time
from collections.abc import Iterator
from typing import Any

import pytest
from curie_api import sandbox_token
from curie_api.config import get_settings
from curie_api.routers.fleet import CONTROL_SCOPE

OPERATOR = "U_OPERATOR"
OUTSIDER = "U_RANDOM"


@pytest.fixture
def operators() -> Iterator[None]:
    settings = get_settings()
    previous = settings.control_operators
    settings.control_operators = OPERATOR
    try:
        yield
    finally:
        settings.control_operators = previous


@pytest.fixture
def no_operators() -> Iterator[None]:
    settings = get_settings()
    previous = settings.control_operators
    settings.control_operators = ""
    try:
        yield
    finally:
        settings.control_operators = previous


def _agent(client: Any, headers: dict[str, str], name: str, channel: str) -> str:
    r = client.post(
        "/agents",
        json={"name": name, "channel": {"kind": "slack", "address": channel}},
        headers=headers,
    )
    assert r.status_code in (200, 201), r.text
    return str(r.json()["id"])


@pytest.fixture
def fleet(client: Any, auth_headers: dict[str, str], clean_db: None) -> dict[str, str]:
    control = _agent(client, auth_headers, "curie-control", "C0SCRCTL")
    target = _agent(client, auth_headers, "sre-bot", "C0SCRTGT")
    v1 = client.post(
        f"/agents/{target}/versions",
        json={"version_label": "v1", "created_by": "test"},
        headers=auth_headers,
    ).json()["id"]
    v2 = client.post(
        f"/agents/{target}/versions",
        json={"version_label": "v2", "created_by": "test"},
        headers=auth_headers,
    ).json()["id"]
    for v in (v1, v2):
        client.post(
            "/deployments",
            json={"agent_id": target, "version_id": v, "environment": "prod"},
            headers=auth_headers,
        )
    return {"control": control, "target": target, "v1": str(v1), "v2": str(v2)}


def _screen(client: Any, headers: dict[str, str], sid: str, **params: Any) -> dict[str, Any]:
    r = client.get(f"/fleet/screens/{sid}", params=params, headers=headers)
    assert r.status_code == 200, r.text
    body: dict[str, Any] = r.json()
    return body


def _press(
    client: Any,
    headers: dict[str, str],
    screen: str,
    button: str,
    actor: str = OPERATOR,
    params: dict[str, Any] | None = None,
    typed: str | None = None,
) -> Any:
    return client.post(
        "/fleet/screens/actions",
        json={
            "actor": actor,
            "screen": screen,
            "button": button,
            "params": params or {},
            "typed_confirmation": typed,
        },
        headers=headers,
    )


# -- rendering ----------------------------------------------------------------


def test_home_counts_what_needs_a_person(
    client: Any, auth_headers: dict[str, str], fleet: dict[str, str]
) -> None:
    home = _screen(client, auth_headers, "home")
    assert home["title"] == "Curie"
    assert {b["target"] for b in home["buttons"]} >= {"fleet", "approvals", "proposals"}
    assert all(b["kind"] == "navigate" for b in home["buttons"])


def test_the_control_agent_may_render_screens(
    client: Any, fleet: dict[str, str]
) -> None:
    """Rendering is a read, so the agent does it -- that is the whole point of
    putting a UI in a channel the agent is in."""

    settings = get_settings()
    previous = settings.control_agent
    settings.control_agent = "curie-control"
    try:
        token = sandbox_token.mint(
            settings.api_key,
            agent=fleet["control"],
            scope=CONTROL_SCOPE,
            exp=int(time.time()) + 300,
        )
        screen = _screen(client, {"X-API-Key": token}, "fleet")
        assert {r["agent"] for r in screen["blocks"][0]["rows"]} == {
            "curie-control",
            "sre-bot",
        }
    finally:
        settings.control_agent = previous


def test_agent_screen_offers_kill_and_not_resume_when_running(
    client: Any, auth_headers: dict[str, str], fleet: dict[str, str]
) -> None:
    """One toggle, not two, so there is no tap that can only fail."""

    screen = _screen(client, auth_headers, "agent", agent_id=fleet["target"])
    ids = {b["id"] for b in screen["buttons"]}
    assert "kill" in ids and "resume" not in ids


def test_every_mutating_button_carries_a_confirmation(
    client: Any, auth_headers: dict[str, str], fleet: dict[str, str]
) -> None:
    """A mis-tap on a phone must cost a dialog, not an agent.

    Walked across every screen rather than spot-checked: a new button added
    without a confirm is exactly the kind of thing that passes review.
    """

    pages = [
        ("home", {}),
        ("fleet", {}),
        ("agent", {"agent_id": fleet["target"]}),
        ("versions", {"agent_id": fleet["target"]}),
        ("budget", {"agent_id": fleet["target"]}),
        ("overrides", {"agent_id": fleet["target"]}),
        ("memory", {"agent_id": fleet["target"]}),
        ("threads", {"agent_id": fleet["target"], "thread_key": "C1:170.1"}),
        ("evals", {"agent_id": fleet["target"]}),
        ("surfaces", {"agent_id": fleet["target"]}),
        ("approvals", {}),
        ("proposals", {}),
        ("observability", {}),
        ("danger", {"agent_id": fleet["target"]}),
    ]
    unconfirmed = []
    for sid, params in pages:
        for button in _screen(client, auth_headers, sid, **params)["buttons"]:
            if button["kind"] == "invoke" and not button["confirm"]:
                unconfirmed.append(f"{sid}:{button['id']}")
    assert not unconfirmed, unconfirmed


def test_rollback_buttons_render_only_for_versions_that_are_not_live(
    client: Any, auth_headers: dict[str, str], fleet: dict[str, str]
) -> None:
    screen = _screen(client, auth_headers, "versions", agent_id=fleet["target"])
    targets = {b["params"]["version_id"] for b in screen["buttons"]}
    assert targets == {fleet["v1"]}


# -- who may press ------------------------------------------------------------


def test_the_control_agent_cannot_press_its_own_buttons(
    client: Any, fleet: dict[str, str]
) -> None:
    """It renders the screen; it does not get to tap it.

    Refused on caller kind before the operator list is even consulted, so this
    holds on an install that has named the agent as an operator by mistake.
    """

    settings = get_settings()
    prev_agent, prev_ops = settings.control_agent, settings.control_operators
    settings.control_agent = "curie-control"
    settings.control_operators = f"{OPERATOR},curie-control"
    try:
        token = sandbox_token.mint(
            settings.api_key,
            agent=fleet["control"],
            scope=CONTROL_SCOPE,
            exp=int(time.time()) + 300,
        )
        response = _press(
            client,
            {"X-API-Key": token},
            "agent",
            "kill",
            actor="curie-control",
            params={"agent_id": fleet["target"]},
        )
        assert response.status_code == 403
        assert "never presses them" in response.json()["detail"]
    finally:
        settings.control_agent, settings.control_operators = prev_agent, prev_ops


def test_no_configured_operators_means_no_button_works(
    client: Any, auth_headers: dict[str, str], fleet: dict[str, str], no_operators: None
) -> None:
    """The closed default. Screens still render; nothing can be pressed."""

    _screen(client, auth_headers, "agent", agent_id=fleet["target"])
    response = _press(
        client, auth_headers, "agent", "kill", params={"agent_id": fleet["target"]}
    )
    assert response.status_code == 403
    assert "no control operators are configured" in response.json()["detail"]


def test_a_non_operator_cannot_press(
    client: Any, auth_headers: dict[str, str], fleet: dict[str, str], operators: None
) -> None:
    response = _press(
        client,
        auth_headers,
        "agent",
        "kill",
        actor=OUTSIDER,
        params={"agent_id": fleet["target"]},
    )
    assert response.status_code == 403
    assert OUTSIDER in response.json()["detail"]


# -- what a press can and cannot say ------------------------------------------


def test_a_press_names_a_button_not_an_action(
    client: Any, auth_headers: dict[str, str], fleet: dict[str, str], operators: None
) -> None:
    """There is no field for "run delete_agent".

    The caller names a screen and a button id; the server re-renders and reads
    the action off its own button. So a caller cannot compose an action, and a
    button that the current screen does not render does not exist.
    """

    response = _press(
        client,
        auth_headers,
        "agent",
        "delete",  # the danger screen's button, asked for from the agent screen
        params={"agent_id": fleet["target"]},
    )
    assert response.status_code == 409
    assert "no longer on this screen" in response.json()["detail"]


def test_a_navigate_button_cannot_be_pressed_as_an_action(
    client: Any, auth_headers: dict[str, str], fleet: dict[str, str], operators: None
) -> None:
    response = _press(
        client, auth_headers, "agent", "to-versions", params={"agent_id": fleet["target"]}
    )
    assert response.status_code == 422
    assert "only navigates" in response.json()["detail"]


def test_a_stale_button_is_refused_rather_than_replayed(
    client: Any, auth_headers: dict[str, str], fleet: dict[str, str], operators: None
) -> None:
    """Two operators racing on the same posted message.

    The first kill wins; the second press re-renders, finds the screen now
    offers Resume instead, and refuses. Without the re-render the second press
    would be a second kill against whatever state had since arrived.
    """

    assert _press(
        client, auth_headers, "agent", "kill", params={"agent_id": fleet["target"]}
    ).status_code == 200
    second = _press(
        client, auth_headers, "agent", "kill", params={"agent_id": fleet["target"]}
    )
    assert second.status_code == 409


# -- pressing actually changes the fleet --------------------------------------


def test_pressing_kill_kills_and_returns_the_next_screen(
    client: Any, auth_headers: dict[str, str], fleet: dict[str, str], operators: None
) -> None:
    response = _press(
        client, auth_headers, "agent", "kill", params={"agent_id": fleet["target"]}
    )
    assert response.status_code == 200, response.text
    body = response.json()
    assert body["ok"] is True
    # The reply carries the refreshed screen, so the channel shows the new state
    # instead of "ok" and a stale page.
    assert {b["id"] for b in body["screen"]["buttons"]} >= {"resume"}
    assert client.get(
        f"/agents/{fleet['target']}/kill", headers=auth_headers
    ).json()["killed"] is True


def test_a_press_lands_in_the_same_audit_trail_as_the_cli(
    client: Any, auth_headers: dict[str, str], fleet: dict[str, str], operators: None
) -> None:
    """A change made from a phone is reconstructable from the database, not only
    from a chat log somebody can delete."""

    response = _press(
        client, auth_headers, "agent", "kill", params={"agent_id": fleet["target"]}
    )
    proposal_id = response.json()["proposal_id"]
    row = client.get(f"/fleet/proposals/{proposal_id}", headers=auth_headers).json()
    assert row["status"] == "executed"
    assert row["executed_by"] == OPERATOR
    # No model in the provenance of a button press, and the NULL says so.
    assert row["proposed_by_agent_id"] is None
    assert "new turns" in row["summary"]


def test_pressing_a_budget_preset_sets_it(
    client: Any, auth_headers: dict[str, str], fleet: dict[str, str], operators: None
) -> None:
    response = _press(
        client, auth_headers, "budget", "budget-25", params={"agent_id": fleet["target"]}
    )
    assert response.status_code == 200, response.text
    budget = client.get(f"/agents/{fleet['target']}/budget", headers=auth_headers).json()
    assert budget["max_usd_per_day"] == 25.0


def test_pressing_rollback_moves_prod(
    client: Any, auth_headers: dict[str, str], fleet: dict[str, str], operators: None
) -> None:
    response = _press(
        client,
        auth_headers,
        "versions",
        f"rollback-{fleet['v1']}",
        params={"agent_id": fleet["target"]},
    )
    assert response.status_code == 200, response.text
    assert response.json()["result"]["version_label"] == "v1"


# -- the danger zone -----------------------------------------------------------


def test_delete_needs_the_agents_name_typed(
    client: Any, auth_headers: dict[str, str], fleet: dict[str, str], operators: None
) -> None:
    response = _press(
        client, auth_headers, "danger", "delete", params={"agent_id": fleet["target"]}
    )
    assert response.status_code == 422
    assert "type the agent's name" in response.json()["detail"]
    assert client.get(f"/agents/{fleet['target']}", headers=auth_headers).status_code == 200


def test_delete_with_the_wrong_name_typed_is_refused(
    client: Any, auth_headers: dict[str, str], fleet: dict[str, str], operators: None
) -> None:
    response = _press(
        client,
        auth_headers,
        "danger",
        "delete",
        params={"agent_id": fleet["target"]},
        typed="sre-bot-dev",
    )
    assert response.status_code == 422


def test_delete_works_with_the_name_typed_and_lands_on_the_fleet(
    client: Any, auth_headers: dict[str, str], fleet: dict[str, str], operators: None
) -> None:
    response = _press(
        client,
        auth_headers,
        "danger",
        "delete",
        params={"agent_id": fleet["target"]},
        typed="sre-bot",
    )
    assert response.status_code == 200, response.text
    # The detail screen it came from no longer exists, so it returns the fleet.
    assert response.json()["screen"]["id"] == "fleet"
    assert client.get(f"/agents/{fleet['target']}", headers=auth_headers).status_code == 404


def test_the_agent_cannot_propose_a_delete_at_all(
    client: Any, auth_headers: dict[str, str], fleet: dict[str, str]
) -> None:
    """The other half of the danger story: no proposal path exists either, so
    there is no message that makes the agent ask for a delete."""

    response = client.post(
        "/fleet/proposals",
        json={"target_agent_id": fleet["target"], "action": "delete_agent", "params": {}},
        headers=auth_headers,
    )
    assert response.status_code == 422
    assert "unknown action" in response.json()["detail"]


# -- coverage, served ----------------------------------------------------------


def test_coverage_endpoint_reports_the_real_map(
    client: Any, auth_headers: dict[str, str]
) -> None:
    body = client.get("/fleet/coverage", headers=auth_headers).json()
    assert body["total"] == body["covered"] + body["exempt"]
    by_command = {r["command"]: r for r in body["rows"]}
    assert by_command["cluster kill"]["screen"] == "agent"
    # And the honest half: a command that cannot be a button says why.
    assert by_command["cluster up"]["exempt"] == "substrate-lifecycle"
    assert by_command["dev contracts"]["exempt"] == "source-checkout"


def test_deleting_an_agent_leaves_its_audit_trail_intact(
    client: Any, auth_headers: dict[str, str], fleet: dict[str, str], operators: None
) -> None:
    """The record of a deletion must survive the deletion.

    Found by the delete test failing on a cascade: the FK was CASCADE, so
    removing an agent removed every proposal ever executed against it --
    including the one that removed it. The one irreversible action was the one
    leaving no trace. Now the id goes NULL and the stored name keeps the row
    readable.
    """

    killed = _press(
        client, auth_headers, "agent", "kill", params={"agent_id": fleet["target"]}
    ).json()["proposal_id"]
    deleted = _press(
        client,
        auth_headers,
        "danger",
        "delete",
        params={"agent_id": fleet["target"]},
        typed="sre-bot",
    ).json()["proposal_id"]

    for proposal_id in (killed, deleted):
        row = client.get(f"/fleet/proposals/{proposal_id}", headers=auth_headers).json()
        assert row["status"] == "executed"
        assert row["executed_by"] == OPERATOR
        # The agent is gone, so the id is NULL -- and the name is what makes the
        # row mean anything a year from now.
        assert row["target_agent_id"] is None
        assert row["target_agent_name"] == "sre-bot"


def test_a_proposal_for_a_deleted_agent_cannot_be_run(
    client: Any, auth_headers: dict[str, str], fleet: dict[str, str], operators: None
) -> None:
    """A pending proposal outlives its agent as a record, not as an action."""

    pending = client.post(
        "/fleet/proposals",
        json={"target_agent_id": fleet["target"], "action": "kill", "params": {}},
        headers=auth_headers,
    ).json()["id"]
    _press(
        client,
        auth_headers,
        "danger",
        "delete",
        params={"agent_id": fleet["target"]},
        typed="sre-bot",
    )
    response = client.post(
        f"/fleet/proposals/{pending}/execute",
        json={"executed_by": OPERATOR},
        headers=auth_headers,
    )
    assert response.status_code == 409
    assert "has been deleted" in response.json()["detail"]


# -- surfaces ------------------------------------------------------------------


def test_a_sole_binding_renders_no_unbind_button(
    client: Any, auth_headers: dict[str, str], fleet: dict[str, str]
) -> None:
    """An agent with one binding cannot be unbound from it: zero bindings is
    deployed, healthy-looking, and unable to receive a turn. The button does not
    render rather than rendering and failing."""

    screen = _screen(client, auth_headers, "surfaces", agent_id=fleet["target"])
    assert screen["buttons"] == []
    notes = [b["text"] for b in screen["blocks"] if b["kind"] == "note"]
    assert any("only binding" in n for n in notes)


def test_unbinding_one_of_several_works(
    client: Any, auth_headers: dict[str, str], fleet: dict[str, str], operators: None
) -> None:
    added = client.post(
        f"/agents/{fleet['target']}/channels",
        json={"kind": "slack", "address": "C0SCRXTRA"},
        headers=auth_headers,
    )
    assert added.status_code in (200, 201), added.text

    screen = _screen(client, auth_headers, "surfaces", agent_id=fleet["target"])
    assert len(screen["buttons"]) == 2

    response = _press(
        client,
        auth_headers,
        "surfaces",
        "unbind-slack-C0SCRXTRA",
        params={"agent_id": fleet["target"]},
    )
    assert response.status_code == 200, response.text
    assert response.json()["result"]["unbound"] == "slack:C0SCRXTRA"
    after = _screen(client, auth_headers, "surfaces", agent_id=fleet["target"])
    assert [r["address"] for r in after["blocks"][0]["rows"]] == ["C0SCRTGT"]


def test_adding_a_surface_is_not_offered_as_a_button(
    client: Any, auth_headers: dict[str, str], fleet: dict[str, str]
) -> None:
    """A channel id cannot come from a button, and the screen says so instead of
    leaving the operation looking forgotten."""

    screen = _screen(client, auth_headers, "surfaces", agent_id=fleet["target"])
    notes = " ".join(b["text"] or "" for b in screen["blocks"] if b["kind"] == "note")
    assert "needs a channel id" in notes
