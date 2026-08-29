"""The fleet control plane's trust boundary, against real Postgres and Valkey.

ADR-0133. These tests are the security claim, so they are written as attacks
rather than as feature coverage: each one is a way an agent that is not the
control agent might reach the plane, or a way the control agent might reach past
proposing. Feature behavior (a proposal executes and the fleet changes) is
covered too, but it is the smaller half.

Nothing is mocked. The tokens are real ``sandbox_token`` mints signed with the
configured platform key, which is what makes "a state token cannot be replayed
as a control token" a fact about the signature rather than about a stub.
"""

from __future__ import annotations

import time
import uuid
from collections.abc import Iterator
from datetime import timedelta
from typing import Any

import pytest
from curie_api import sandbox_token
from curie_api.config import get_settings
from curie_api.routers.fleet import CONTROL_SCOPE
from curie_api.routers.state import STATE_APP_SCOPE, STATE_SCOPE

CONTROL_AGENT_NAME = "curie-control"


@pytest.fixture
def control_agent_configured() -> Iterator[None]:
    """Point the API's ``control_agent`` at our fixture agent for one test.

    Mutating the cached Settings object rather than the environment because
    ``get_settings`` is lru_cached and every dependency in the request path has
    already resolved it; restoring in teardown keeps the default (empty, plane
    off) for every other test in the session.
    """

    settings = get_settings()
    previous = settings.control_agent
    settings.control_agent = CONTROL_AGENT_NAME
    try:
        yield
    finally:
        settings.control_agent = previous


def _mint(agent_id: str, scope: str, *, ttl: int = 300) -> str:
    return sandbox_token.mint(
        get_settings().api_key, agent=agent_id, scope=scope, exp=int(time.time()) + ttl
    )


def _create_agent(client: Any, headers: dict[str, str], name: str, channel: str) -> str:
    response = client.post(
        "/agents",
        json={"name": name, "channel": {"kind": "slack", "address": channel}},
        headers=headers,
    )
    assert response.status_code in (200, 201), response.text
    return str(response.json()["id"])


@pytest.fixture
def fleet(client: Any, auth_headers: dict[str, str], clean_db: None) -> dict[str, str]:
    """A control agent and one ordinary agent to act on."""

    control_channel = "C0FLEETCTL"  # gitleaks:allow -- fixture channel id, not real
    target_channel = "C0FLEETTGT"  # gitleaks:allow -- fixture channel id, not real
    return {
        "control": _create_agent(client, auth_headers, CONTROL_AGENT_NAME, control_channel),
        "target": _create_agent(client, auth_headers, "sre-bot", target_channel),
    }


# -- who may reach the plane at all --------------------------------------------


def test_plane_is_off_until_an_operator_names_the_control_agent(
    client: Any, fleet: dict[str, str]
) -> None:
    """With ``control_agent`` unset -- the default -- a correctly minted,
    correctly scoped, unexpired control token authenticates nothing.

    This is the property that makes the feature safe to ship dark: deploying the
    bundle grants nothing until an operator opts in on both sides.
    """

    token = _mint(fleet["control"], CONTROL_SCOPE)
    assert get_settings().control_agent == ""
    response = client.get("/fleet/agents", headers={"X-API-Key": token})
    assert response.status_code == 401


def test_control_agent_reads_the_fleet(
    client: Any, fleet: dict[str, str], control_agent_configured: None
) -> None:
    token = _mint(fleet["control"], CONTROL_SCOPE)
    response = client.get("/fleet/agents", headers={"X-API-Key": token})
    assert response.status_code == 200, response.text
    names = {row["name"] for row in response.json()}
    assert {"curie-control", "sre-bot"} <= names


def test_another_agents_control_token_is_refused(
    client: Any, fleet: dict[str, str], control_agent_configured: None
) -> None:
    """The scope alone is not the grant: the token must NAME the control agent.

    This is the case a buggy or malicious worker produces -- a control-scoped
    token minted for the wrong agent. The API verifies against the agent id it
    resolved from its OWN configured name, so the signature check fails on the
    agent claim and the mint site's mistake does not become a privilege.
    """

    token = _mint(fleet["target"], CONTROL_SCOPE)
    response = client.get("/fleet/agents", headers={"X-API-Key": token})
    assert response.status_code == 401


@pytest.mark.parametrize("scope", [STATE_SCOPE, STATE_APP_SCOPE])
def test_a_state_token_cannot_be_replayed_as_a_control_token(
    client: Any, fleet: dict[str, str], control_agent_configured: None, scope: str
) -> None:
    """Every sandbox holds state tokens; none of them opens this plane.

    Minted for the CONTROL agent on purpose -- the agent claim is right and only
    the scope is wrong, so this isolates the scope as the thing doing the work.
    Scope is inside the signed payload, so there is no edit that fixes it.
    """

    token = _mint(fleet["control"], scope)
    response = client.get("/fleet/agents", headers={"X-API-Key": token})
    assert response.status_code == 401


def test_an_expired_control_token_is_refused(
    client: Any, fleet: dict[str, str], control_agent_configured: None
) -> None:
    token = _mint(fleet["control"], CONTROL_SCOPE, ttl=-1)
    response = client.get("/fleet/agents", headers={"X-API-Key": token})
    assert response.status_code == 401


def test_a_tampered_control_token_is_refused(
    client: Any, fleet: dict[str, str], control_agent_configured: None
) -> None:
    """Flipping the last signature character must not authenticate."""

    token = _mint(fleet["control"], CONTROL_SCOPE)
    tampered = token[:-1] + ("A" if token[-1] != "A" else "B")
    response = client.get("/fleet/agents", headers={"X-API-Key": tampered})
    assert response.status_code == 401


# -- what the control agent may do once it is in -------------------------------


def _propose(client: Any, headers: dict[str, str], target: str, action: str, **params: Any) -> Any:
    return client.post(
        "/fleet/proposals",
        json={
            "target_agent_id": target,
            "action": action,
            "params": params,
            "requested_by": "someone in a thread",
        },
        headers=headers,
    )


def test_control_agent_may_propose(
    client: Any, fleet: dict[str, str], control_agent_configured: None
) -> None:
    token = _mint(fleet["control"], CONTROL_SCOPE)
    response = _propose(client, {"X-API-Key": token}, fleet["target"], "kill")
    assert response.status_code == 201, response.text
    body = response.json()
    assert body["status"] == "pending"
    # Provenance records that an AGENT asked, distinguishably from an operator.
    assert body["proposed_by_agent_id"] == fleet["control"]


def test_control_agent_cannot_execute_its_own_proposal(
    client: Any,
    auth_headers: dict[str, str],
    fleet: dict[str, str],
    control_agent_configured: None,
) -> None:
    """The line the whole design rests on.

    The agent authenticates on this router, creates the proposal, and is refused
    on execute by caller kind -- 403, not 401, because the credential is valid
    and the authority is not. No configuration grants this.
    """

    token = _mint(fleet["control"], CONTROL_SCOPE)
    proposal = _propose(client, {"X-API-Key": token}, fleet["target"], "kill").json()

    response = client.post(
        f"/fleet/proposals/{proposal['id']}/execute",
        json={"executed_by": "the agent itself"},
        headers={"X-API-Key": token},
    )
    assert response.status_code == 403
    assert "platform key" in response.json()["detail"]

    # And it did not happen: the row is still pending and the agent is not killed.
    still = client.get(f"/fleet/proposals/{proposal['id']}", headers=auth_headers).json()
    assert still["status"] == "pending"
    kill_state = client.get(f"/agents/{fleet['target']}/kill", headers=auth_headers).json()
    assert kill_state["killed"] is False


def test_control_agent_cannot_reject_a_proposal_either(
    client: Any, fleet: dict[str, str], control_agent_configured: None
) -> None:
    """Rejecting is resolving. An agent that could reject could bury a proposal
    a human never saw."""

    token = _mint(fleet["control"], CONTROL_SCOPE)
    proposal = _propose(client, {"X-API-Key": token}, fleet["target"], "kill").json()
    response = client.post(
        f"/fleet/proposals/{proposal['id']}/reject",
        json={"executed_by": "the agent itself"},
        headers={"X-API-Key": token},
    )
    assert response.status_code == 403


# -- the summary is the platform's, not the caller's ---------------------------


def test_summary_is_rendered_by_the_api_not_supplied_by_the_caller(
    client: Any, auth_headers: dict[str, str], fleet: dict[str, str]
) -> None:
    """A caller-supplied ``summary`` is ignored, not honored.

    The human's click authorizes the action, and the summary is what they read
    when clicking. If a proposer could write it, an injected prompt would only
    need a persuasive sentence attached to different parameters.
    """

    response = client.post(
        "/fleet/proposals",
        json={
            "target_agent_id": fleet["target"],
            "action": "kill",
            "params": {},
            "summary": "Harmless no-op, safe to approve.",
        },
        headers=auth_headers,
    )
    assert response.status_code == 201, response.text
    summary = response.json()["summary"]
    assert "Harmless" not in summary
    assert "sre-bot" in summary and "new turns" in summary


def test_unknown_actions_are_refused_with_the_legal_vocabulary(
    client: Any, auth_headers: dict[str, str], fleet: dict[str, str]
) -> None:
    response = _propose(client, auth_headers, fleet["target"], "delete_agent")
    assert response.status_code == 422
    detail = response.json()["detail"]
    assert "unknown action" in detail
    # The error names the closed set, so a caller recovers without guessing.
    assert "kill" in detail and "rollback" in detail


def test_irreversible_actions_are_not_in_the_vocabulary(
    client: Any, auth_headers: dict[str, str], fleet: dict[str, str]
) -> None:
    """Deleting an agent and rotating credentials are console/CLI only.

    Asserted as a test rather than left to review because the natural drift is
    additive: someone adds the action that would have been convenient once.
    """

    for action in ("delete_agent", "rotate_credentials", "set_approval_policy"):
        assert _propose(client, auth_headers, fleet["target"], action).status_code == 422


# -- executing, once, by a human -----------------------------------------------


def test_platform_key_executes_and_the_fleet_actually_changes(
    client: Any, auth_headers: dict[str, str], fleet: dict[str, str]
) -> None:
    proposal = _propose(client, auth_headers, fleet["target"], "kill").json()
    response = client.post(
        f"/fleet/proposals/{proposal['id']}/execute",
        json={"executed_by": "operator@example.com"},
        headers=auth_headers,
    )
    assert response.status_code == 200, response.text
    body = response.json()
    assert body["status"] == "executed"
    # A human is on the durable record (ADR-0046), never "the platform key".
    assert body["executed_by"] == "operator@example.com"

    kill_state = client.get(f"/agents/{fleet['target']}/kill", headers=auth_headers).json()
    assert kill_state["killed"] is True


def test_a_proposal_executes_at_most_once(
    client: Any, auth_headers: dict[str, str], fleet: dict[str, str]
) -> None:
    proposal = _propose(client, auth_headers, fleet["target"], "kill").json()
    first = client.post(
        f"/fleet/proposals/{proposal['id']}/execute",
        json={"executed_by": "operator@example.com"},
        headers=auth_headers,
    )
    assert first.status_code == 200
    second = client.post(
        f"/fleet/proposals/{proposal['id']}/execute",
        json={"executed_by": "operator@example.com"},
        headers=auth_headers,
    )
    assert second.status_code == 409
    assert "already executed" in second.json()["detail"]


def test_an_expired_proposal_cannot_be_executed(
    client: Any, auth_headers: dict[str, str], fleet: dict[str, str], monkeypatch: Any
) -> None:
    """A stale proposal describes a fleet that has since moved, so the click a
    human makes is about a different system than the one they read about."""

    import curie_api.routers.fleet as fleet_router

    monkeypatch.setattr(fleet_router, "PROPOSAL_TTL", timedelta(seconds=-1))
    proposal = _propose(client, auth_headers, fleet["target"], "kill").json()
    response = client.post(
        f"/fleet/proposals/{proposal['id']}/execute",
        json={"executed_by": "operator@example.com"},
        headers=auth_headers,
    )
    assert response.status_code == 409
    assert "expired" in response.json()["detail"]
    after = client.get(f"/fleet/proposals/{proposal['id']}", headers=auth_headers).json()
    assert after["status"] == "expired"


def test_set_budget_renders_the_before_and_after(
    client: Any, auth_headers: dict[str, str], fleet: dict[str, str]
) -> None:
    """The summary names both values, because "change the cap to $5" alone does
    not tell a human whether that is a tightening or a 10x loosening."""

    response = _propose(client, auth_headers, fleet["target"], "set_budget", max_usd_per_day=5.0)
    assert response.status_code == 201, response.text
    summary = response.json()["summary"]
    assert "the platform default" in summary and "$5.00/day" in summary

    executed = client.post(
        f"/fleet/proposals/{response.json()['id']}/execute",
        json={"executed_by": "operator@example.com"},
        headers=auth_headers,
    )
    assert executed.status_code == 200
    budget = client.get(f"/agents/{fleet['target']}/budget", headers=auth_headers).json()
    assert budget["max_usd_per_day"] == 5.0


def test_a_proposal_against_a_missing_agent_is_404(
    client: Any, auth_headers: dict[str, str], fleet: dict[str, str]
) -> None:
    response = _propose(client, auth_headers, str(uuid.uuid4()), "kill")
    assert response.status_code == 404


# -- rollback: the action with real parameters ---------------------------------
#
# Added after mypy caught ``version.label`` on a model whose column is
# ``version_label``: every test above passed, because none of them exercised a
# rollback or listed versions. The type checker found it; these keep it found.


def _version(client: Any, headers: dict[str, str], agent_id: str, label: str) -> str:
    response = client.post(
        f"/agents/{agent_id}/versions",
        json={"version_label": label, "created_by": "test"},
        headers=headers,
    )
    assert response.status_code == 201, response.text
    return str(response.json()["id"])


def _deploy(client: Any, headers: dict[str, str], agent_id: str, version_id: str) -> None:
    response = client.post(
        "/deployments",
        json={"agent_id": agent_id, "version_id": version_id, "environment": "prod"},
        headers=headers,
    )
    assert response.status_code in (200, 201), response.text


@pytest.fixture
def deployed(client: Any, auth_headers: dict[str, str], fleet: dict[str, str]) -> dict[str, str]:
    """The target agent with two versions, running the newer one in prod."""

    old = _version(client, auth_headers, fleet["target"], "v1")
    new = _version(client, auth_headers, fleet["target"], "v2")
    _deploy(client, auth_headers, fleet["target"], old)
    _deploy(client, auth_headers, fleet["target"], new)
    return {**fleet, "old": old, "new": new}


def test_list_versions_marks_what_is_actually_active(
    client: Any, auth_headers: dict[str, str], deployed: dict[str, str]
) -> None:
    """The control agent needs this to translate "the version before this one"
    into an id, so the active marking is the load-bearing part."""

    response = client.get(f"/fleet/agents/{deployed['target']}/versions", headers=auth_headers)
    assert response.status_code == 200, response.text
    by_id = {row["id"]: row for row in response.json()}
    assert by_id[deployed["new"]]["active_in"] == ["prod"]
    assert by_id[deployed["old"]]["active_in"] == []
    assert by_id[deployed["old"]]["label"] == "v1"


def test_rollback_renders_both_version_labels(
    client: Any, auth_headers: dict[str, str], deployed: dict[str, str]
) -> None:
    """The summary must name where it is going FROM as well as TO: "roll back to
    v1" alone does not tell a human how far back that is."""

    response = _propose(
        client, auth_headers, deployed["target"], "rollback", version_id=deployed["old"]
    )
    assert response.status_code == 201, response.text
    summary = response.json()["summary"]
    assert "v2 -> version v1" in summary
    assert "sre-bot" in summary and "prod" in summary


def test_rollback_actually_moves_the_active_deployment(
    client: Any, auth_headers: dict[str, str], deployed: dict[str, str]
) -> None:
    proposal = _propose(
        client, auth_headers, deployed["target"], "rollback", version_id=deployed["old"]
    ).json()
    executed = client.post(
        f"/fleet/proposals/{proposal['id']}/execute",
        json={"executed_by": "operator@example.com"},
        headers=auth_headers,
    )
    assert executed.status_code == 200, executed.text
    assert executed.json()["result"]["version_label"] == "v1"

    versions = client.get(
        f"/fleet/agents/{deployed['target']}/versions", headers=auth_headers
    ).json()
    by_id = {row["id"]: row for row in versions}
    assert by_id[deployed["old"]]["active_in"] == ["prod"]
    assert by_id[deployed["new"]]["active_in"] == []


def test_rollback_refuses_a_version_belonging_to_another_agent(
    client: Any, auth_headers: dict[str, str], deployed: dict[str, str]
) -> None:
    """A cross-agent deploy must not even be storable as a proposal.

    Checked at create time rather than execute time so the bad combination
    never reaches a card a human could click.
    """

    foreign = _version(client, auth_headers, deployed["control"], "v9")
    response = _propose(
        client, auth_headers, deployed["target"], "rollback", version_id=foreign
    )
    assert response.status_code == 422
    assert "different agent" in response.json()["detail"]


def test_rollback_refuses_the_version_already_running(
    client: Any, auth_headers: dict[str, str], deployed: dict[str, str]
) -> None:
    response = _propose(
        client, auth_headers, deployed["target"], "rollback", version_id=deployed["new"]
    )
    assert response.status_code == 422
    assert "already runs" in response.json()["detail"]


def test_rollback_requires_a_version_id(
    client: Any, auth_headers: dict[str, str], deployed: dict[str, str]
) -> None:
    assert _propose(client, auth_headers, deployed["target"], "rollback").status_code == 422


def test_rollback_refuses_an_environment_with_no_active_deployment(
    client: Any, auth_headers: dict[str, str], deployed: dict[str, str]
) -> None:
    """Nothing is deployed to dev in this fixture, so there is nothing to roll
    back -- and the refusal names that rather than silently deploying."""

    response = _propose(
        client, auth_headers, deployed["target"], "rollback", version_id=deployed["old"], env="dev"
    )
    assert response.status_code == 422
    assert "no active dev deployment" in response.json()["detail"]
