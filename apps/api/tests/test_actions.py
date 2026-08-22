"""The action ledger and the two rules that make an undo safe."""

import uuid

from fastapi.testclient import TestClient


def _record(client: TestClient, headers: dict[str, str], **overrides: object) -> dict:
    body = {
        "conversation_id": "C1:t1",
        "turn_id": "turn-1",
        "tool": "mcp__k8s-scale__scale_deployment",
        "arguments": {"namespace": "public", "name": "api", "replicas": 10},
        "target": {"namespace": "public", "name": "api"},
        "snapshot": {"spec": {"replicas": 3}},
        "snapshot_status": "captured",
        "post_state": {"spec": {"replicas": 10}},
        "outcome": "succeeded",
        "dedupe_key": f"evt-{uuid.uuid4()}",
    }
    body.update(overrides)
    response = client.post("/actions", json=body, headers=headers)
    assert response.status_code == 201, response.text
    return response.json()


def test_a_recorded_action_with_prior_state_is_undoable(
    client: TestClient, auth_headers: dict[str, str], clean_db: None
) -> None:
    action = _record(client, auth_headers)
    assert action["undoable"] is True


def test_an_action_with_no_prior_state_is_not_undoable(
    client: TestClient, auth_headers: dict[str, str], clean_db: None
) -> None:
    """The irreversible case. Nothing special-cases the tool; the absence does."""

    action = _record(
        client,
        auth_headers,
        tool="mcp__k8s-write__restart_deployment",
        snapshot=None,
        post_state=None,
        irreversible_reason="restarting pods cannot be undone",
    )
    assert action["undoable"] is False

    refused = client.post(
        f"/actions/{action['id']}/undo",
        json={"actor": "U1", "observed_state": {"spec": {"replicas": 10}}},
        headers=auth_headers,
    )
    assert refused.status_code == 409
    assert "restarting pods cannot be undone" in refused.json()["detail"]


def test_recording_is_idempotent_on_the_dedupe_key(
    client: TestClient, auth_headers: dict[str, str], clean_db: None
) -> None:
    """At-least-once delivery must not fork two records of one real action."""

    first = _record(client, auth_headers, dedupe_key="evt-fixed")
    second = _record(client, auth_headers, dedupe_key="evt-fixed")
    assert first["id"] == second["id"]


def test_undo_restores_and_is_recorded(
    client: TestClient, auth_headers: dict[str, str], clean_db: None
) -> None:
    action = _record(client, auth_headers)
    undone = client.post(
        f"/actions/{action['id']}/undo",
        json={"actor": "U1", "observed_state": {"spec": {"replicas": 10}}},
        headers=auth_headers,
    )
    assert undone.status_code == 200
    assert undone.json()["undo_status"] == "undone"
    assert undone.json()["undone_by"] == "U1"
    assert undone.json()["undoable"] is False

    audit = client.get(f"/actions/{action['id']}/audit", headers=auth_headers).json()
    assert [entry["action"] for entry in audit] == ["undone"]
    assert audit[0]["evidence"]["restored"] == {"spec": {"replicas": 3}}


def test_undo_is_refused_when_the_world_moved(
    client: TestClient, auth_headers: dict[str, str], clean_db: None
) -> None:
    """The rule the feature lives on: somebody scaled it by hand afterwards."""

    action = _record(client, auth_headers)
    refused = client.post(
        f"/actions/{action['id']}/undo",
        json={"actor": "U1", "observed_state": {"spec": {"replicas": 7}}},
        headers=auth_headers,
    )
    assert refused.status_code == 409
    assert "changed since" in refused.json()["detail"]

    still = client.get(f"/actions/{action['id']}", headers=auth_headers).json()
    assert still["undo_status"] == "recorded"
    assert still["undoable"] is True

    audit = client.get(f"/actions/{action['id']}/audit", headers=auth_headers).json()
    assert audit[0]["action"] == "refused"
    assert audit[0]["evidence"] == {
        "expected": {"spec": {"replicas": 10}},
        "observed": {"spec": {"replicas": 7}},
    }


def test_undo_without_the_live_state_is_refused(
    client: TestClient, auth_headers: dict[str, str], clean_db: None
) -> None:
    """An unchecked restore is the failure this endpoint exists to prevent."""

    action = _record(client, auth_headers)
    refused = client.post(
        f"/actions/{action['id']}/undo", json={"actor": "U1"}, headers=auth_headers
    )
    assert refused.status_code == 428


def test_undoing_twice_is_refused(
    client: TestClient, auth_headers: dict[str, str], clean_db: None
) -> None:
    action = _record(client, auth_headers)
    payload = {"actor": "U1", "observed_state": {"spec": {"replicas": 10}}}
    first = client.post(f"/actions/{action['id']}/undo", json=payload, headers=auth_headers)
    assert first.status_code == 200
    again = client.post(f"/actions/{action['id']}/undo", json=payload, headers=auth_headers)
    assert again.status_code == 409
    assert "already undone" in again.json()["detail"]


def test_a_failed_action_is_recorded_and_not_undoable(
    client: TestClient, auth_headers: dict[str, str], clean_db: None
) -> None:
    """"It may have happened" is the state a human most needs told."""

    action = _record(client, auth_headers, outcome="failed", snapshot=None, post_state=None)
    assert action["undoable"] is False
    assert action["outcome"] == "failed"


def test_a_turn_reads_back_its_own_actions_in_order(
    client: TestClient, auth_headers: dict[str, str], clean_db: None
) -> None:
    _record(client, auth_headers, turn_id="turn-9", dedupe_key="evt-a")
    _record(
        client,
        auth_headers,
        turn_id="turn-9",
        tool="mcp__k8s-write__restart_deployment",
        snapshot=None,
        post_state=None,
        irreversible_reason="restarting pods cannot be undone",
        dedupe_key="evt-b",
    )
    rows = client.get("/actions?turn_id=turn-9", headers=auth_headers).json()
    assert [row["undoable"] for row in rows] == [True, False]
