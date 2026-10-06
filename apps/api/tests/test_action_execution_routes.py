"""The execution, observation and probe routes of the action executor, on real Postgres.

Realizes the API half of the connector action executor contract
(docs/superpowers/specs/2026-10-06-connector-action-executor.md): the undo
ruling becomes a restore execution (ACTION-EXECUTOR-3), the capability probe
route is the third and last producer of executions (ACTION-EXECUTOR-1, -13),
and the worker reports through fenced, idempotent API-key routes
(ACTION-EXECUTOR-18): ``POST /action-executions/claim``,
``POST /action-executions/{id}/observation``,
``POST /action-executions/{id}/dispatch``,
``POST /action-executions/{id}/outcome`` and ``GET /action-executions/{id}``.

The platform compares versions before a restore (ACTION-EXECUTOR-15): the
worker posts the version ``observe_version`` reported, and a difference ends
the execution ``refused`` with ``version_conflict`` and an audit
``refused_conflict`` naming both versions, so no ``dispatched`` commit (and so
no write call) can follow.

Every request goes through the real routes with the platform API key; rows are
read back with SQL only to count what exists. Nothing in any response, audit
row or refusal body may carry the sealed envelope or a state.

Shapes the spec leaves to the implementer and these tests fix (see
``.projects/plans/executor-routes-test-notes.md``): a creation answers
``{"execution_id", "state"}``; the fence is ``{"lease_owner", "attempt"}``
as the claim returned them; a claim body is ``{"lease_owner", "lease_seconds"}``
and an empty queue answers ``204``; an outcome body is the fence plus
``{"state", "code"}``; an observation body is the fence plus ``{"version"}``.
"""

from __future__ import annotations

import hashlib
import json
import os
import types
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import pytest
from _migration_support import sql_dicts, sql_rows
from _sealed_actions import (
    CONNECTOR,
    DIGEST,
    ENVELOPE,
    EXECUTOR_SETTING,
    LEFT,
    POST_VERSION,
    TARGET,
    executions_of,
    executor_enabled,  # noqa: F401 - fixture, requested by name
    operator_headers,
    sealed_action,
    undoable_agent,
    worker_headers,
)
from curie_api.config import get_settings
from curie_api.main import create_app
from fastapi.testclient import TestClient

pytestmark = pytest.mark.usefixtures("clean_db", "executor_enabled")

ACTOR = "U-operator"
OTHER_DIGEST = "sha256:" + "cd" * 32
MOVED_VERSION = "rv-2077"

# @spec ACTION-EXECUTOR-20: the closed codes, by stage.
PRE_DISPATCH_CODES = [
    "agent_stopped",
    "authority_unavailable",
    "reserved_verb_via_forward",
    "arguments_mismatch",
    "tool_not_grant_bound",
    "connector_not_hosted",
    "connector_digest_unavailable",
    "restore_not_advertised",
    "restore_schema_mismatch",
    "tool_not_advertised",
    "version_conflict",
    "sandbox_unavailable",
    "runner_unavailable",
    "connector_unreachable",
]
CONNECTOR_REFUSAL_CODES = [
    "version_conflict_at_write",
    "sealing_key_unavailable",
    "snapshot_unopenable",
]
POST_DISPATCH_CODES = [
    "connector_error",
    "unstructured_reply",
    "response_lost",
    "deadline_exceeded",
]


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #


def _assert_no_snapshot(text: str) -> None:
    """No envelope and no state in a response body, refusal body or audit row."""

    assert ENVELOPE["ciphertext"] not in text
    assert json.dumps(LEFT, separators=(",", ":")) not in text.replace(" ", "")
    assert '"prior_state"' not in text
    assert '"post_state"' not in text


def _undoable_action(client: Any, headers: dict[str, str], tmp_path: Path) -> dict[str, Any]:
    action = sealed_action(client, headers, undoable_agent(client, headers, tmp_path))
    assert action["undoable"] is True
    return action


def _undo(client: Any, headers: dict[str, str], action_id: str) -> Any:
    # The actor is the authenticated principal, not a body field (route decisions).
    return client.post(f"/actions/{action_id}/undo", json={}, headers=operator_headers(ACTOR))


def _requested(client: Any, headers: dict[str, str], tmp_path: Path) -> tuple[str, str]:
    """An authorized undo: (action id, execution id)."""

    action = _undoable_action(client, headers, tmp_path)
    ruling = _undo(client, headers, action["id"])
    assert ruling.status_code == 202, ruling.text
    return action["id"], str(ruling.json()["execution_id"])


def _claim(
    client: Any, headers: dict[str, str], owner: str = "worker-a", lease_seconds: int = 60
) -> Any:
    return client.post(
        "/action-executions/claim",
        json={"lease_owner": owner, "lease_seconds": lease_seconds},
        headers=worker_headers(),
    )


def _claimed(
    client: Any, headers: dict[str, str], execution_id: str, owner: str = "worker-a"
) -> dict[str, Any]:
    """Claim the one requested execution and return its fence."""

    response = _claim(client, headers, owner)
    assert response.status_code == 200, response.text
    body = response.json()
    assert body["id"] == execution_id
    assert body["state"] == "claimed"
    assert body["lease_owner"] == owner
    assert isinstance(body["attempt"], int)
    return {"lease_owner": owner, "attempt": body["attempt"]}


def _observe(
    client: Any, headers: dict[str, str], execution_id: str, fence: dict[str, Any], version: Any
) -> Any:
    return client.post(
        f"/action-executions/{execution_id}/observation",
        json={**fence, "version": version},
        headers=worker_headers(),
    )


def _dispatch(
    client: Any, headers: dict[str, str], execution_id: str, fence: dict[str, Any]
) -> Any:
    return client.post(
        f"/action-executions/{execution_id}/dispatch", json=fence, headers=worker_headers()
    )


def _report(
    client: Any,
    headers: dict[str, str],
    execution_id: str,
    fence: dict[str, Any],
    state: str,
    code: str | None = None,
) -> Any:
    return client.post(
        f"/action-executions/{execution_id}/outcome",
        json={**fence, "state": state, "code": code},
        headers=worker_headers(),
    )


def _dispatched(
    client: Any, headers: dict[str, str], tmp_path: Path
) -> tuple[str, str, dict[str, Any]]:
    """A restore claimed, observed unchanged and dispatched."""

    action_id, execution_id = _requested(client, headers, tmp_path)
    fence = _claimed(client, headers, execution_id)
    observed = _observe(client, headers, execution_id, fence, POST_VERSION)
    assert observed.status_code == 200, observed.text
    dispatched = _dispatch(client, headers, execution_id, fence)
    assert dispatched.status_code == 200, dispatched.text
    return action_id, execution_id, fence


def _execution(client: Any, headers: dict[str, str], execution_id: str) -> dict[str, Any]:
    response = client.get(f"/action-executions/{execution_id}", headers=headers)
    assert response.status_code == 200, response.text
    _assert_no_snapshot(response.text)
    return dict(response.json())


def _audit(client: Any, headers: dict[str, str], action_id: str) -> list[dict[str, Any]]:
    response = client.get(f"/actions/{action_id}/audit", headers=headers)
    assert response.status_code == 200, response.text
    _assert_no_snapshot(response.text)
    return list(response.json())


def _action(client: Any, headers: dict[str, str], action_id: str) -> dict[str, Any]:
    response = client.get(f"/actions/{action_id}", headers=headers)
    assert response.status_code == 200, response.text
    return dict(response.json())


def _all_executions() -> list[dict[str, Any]]:
    return sql_dicts("SELECT * FROM curie.action_executions ORDER BY created_at, id")


def _agent(client: Any, headers: dict[str, str]) -> str:
    response = client.post(
        "/agents",
        json={
            "name": f"probe-bot-{uuid.uuid4().hex[:6]}",
            "channel": {"kind": "slack", "address": f"C{uuid.uuid4().hex[:9].upper()}"},
        },
        headers=headers,
    )
    assert response.status_code == 201, response.text
    return str(response.json()["id"])


def _probe(client: Any, headers: dict[str, str], body: dict[str, Any]) -> Any:
    return client.post("/connector-capabilities/probes", json=body, headers=worker_headers())


# --------------------------------------------------------------------------- #
# The ruling (ACTION-EXECUTOR-1, -3)
# --------------------------------------------------------------------------- #


def test_the_executor_setting_off_refuses_the_undo_with_one_audit_row_and_no_execution(
    client: Any, auth_headers: dict[str, str], tmp_path: Path
) -> None:
    """@spec ACTION-EXECUTOR-1: "with the setting off, an undo of an undoable record
    returns ``executor_disabled``, writes one refusal audit row and no execution;
    with it on, the same request creates exactly one execution".
    """

    action = _undoable_action(client, auth_headers, tmp_path)

    os.environ[EXECUTOR_SETTING] = "false"
    get_settings.cache_clear()
    try:
        with TestClient(create_app()) as disabled:
            refused = _undo(disabled, auth_headers, action["id"])
    finally:
        os.environ[EXECUTOR_SETTING] = "true"
        get_settings.cache_clear()

    # Amended ACTION-EXECUTOR-20: "The ruling route answers ``executor_disabled``
    # with HTTP 503".
    assert refused.status_code == 503, refused.text
    _assert_no_snapshot(refused.text)
    entries = _audit(client, auth_headers, action["id"])
    assert [(e["action"], e["authorized"]) for e in entries] == [("executor_disabled", False)]
    assert executions_of(action["id"]) == []

    granted = _undo(client, auth_headers, action["id"])

    assert granted.status_code == 202, granted.text
    assert len(executions_of(action["id"])) == 1


def test_a_replayed_ruling_never_creates_a_second_execution(
    client: Any, auth_headers: dict[str, str], tmp_path: Path
) -> None:
    """@spec ACTION-EXECUTOR-3 @spec ACTION-EXECUTOR-2: a second ruling while the
    first restore is live is refused ``refused_restore_in_flight``, and the
    only execution is the first ruling's, keyed by its authorizing audit row.
    """

    action_id, execution_id = _requested(client, auth_headers, tmp_path)

    replay = _undo(client, auth_headers, action_id)

    assert replay.status_code == 409, replay.text
    _assert_no_snapshot(replay.text)
    executions = executions_of(action_id)
    assert [str(row["id"]) for row in executions] == [execution_id]
    entries = _audit(client, auth_headers, action_id)
    assert [e["action"] for e in entries] == ["authorized", "refused_restore_in_flight"]
    assert executions[0]["idempotency_key"] == f"restore:{action_id}:{entries[0]['id']}"


_RULING_REFUSALS: dict[str, tuple[dict[str, Any], str]] = {
    "unsuccessful": ({"failed": True}, "refused_unsuccessful"),
    "unsealed": ({"prior_state": None}, "refused_unsealed"),
    "unversioned": ({"post_version": None}, "refused_unversioned"),
    "no digest": ({"connector_digest": None}, "refused_no_digest"),
}


@pytest.mark.parametrize(
    ("overrides", "code"), list(_RULING_REFUSALS.values()), ids=list(_RULING_REFUSALS)
)
def test_every_record_refusal_writes_an_audit_row_and_no_execution(
    client: Any,
    auth_headers: dict[str, str],
    tmp_path: Path,
    overrides: dict[str, Any],
    code: str,
) -> None:
    """@spec ACTION-EXECUTOR-3 @spec ACTION-EXECUTOR-20: "every refusal writes its
    audit row and creates no execution", with its ruling-stage HTTP status.
    """

    agent_id = undoable_agent(client, auth_headers, tmp_path)
    action = sealed_action(client, auth_headers, agent_id, **overrides)

    response = _undo(client, auth_headers, action["id"])

    # Every other ruling refusal keeps the status its existing refusal used.
    assert response.status_code == 409, response.text
    _assert_no_snapshot(response.text)
    assert [e["action"] for e in _audit(client, auth_headers, action["id"])] == [code]
    assert _all_executions() == []


def test_no_agent_is_refused_with_an_audit_row_and_no_execution(
    client: Any, auth_headers: dict[str, str], tmp_path: Path
) -> None:
    """@spec ACTION-EXECUTOR-3: "refuses ``refused_no_agent`` when the record has no agent"."""

    undoable_agent(client, auth_headers, tmp_path)
    action = sealed_action(client, auth_headers, None)

    response = _undo(client, auth_headers, action["id"])

    # Every other ruling refusal keeps the status its existing refusal used.
    assert response.status_code == 409, response.text
    assert [e["action"] for e in _audit(client, auth_headers, action["id"])] == ["refused_no_agent"]
    assert _all_executions() == []


@pytest.mark.parametrize(
    ("remove", "code"),
    [
        ("DELETE FROM curie.connector_capabilities", "refused_not_restore_capable"),
        ("DELETE FROM curie.deployments", "refused_key_custody"),
    ],
    ids=["no capability row", "no custody"],
)
def test_capability_and_custody_refusals_write_an_audit_row_and_no_execution(
    client: Any, auth_headers: dict[str, str], tmp_path: Path, remove: str, code: str
) -> None:
    """@spec ACTION-EXECUTOR-3 @spec ACTION-EXECUTOR-11 @spec ACTION-EXECUTOR-20."""

    action = _undoable_action(client, auth_headers, tmp_path)
    sql_rows(remove)

    response = _undo(client, auth_headers, action["id"])

    # Every other ruling refusal keeps the status its existing refusal used.
    assert response.status_code == 409, response.text
    assert [e["action"] for e in _audit(client, auth_headers, action["id"])] == [code]
    assert _all_executions() == []


def test_a_confirmed_restore_refuses_a_later_undo_as_already_undone(
    client: Any, auth_headers: dict[str, str], tmp_path: Path
) -> None:
    """@spec ACTION-EXECUTOR-3: the existing ruling refusals keep their order."""

    action_id, execution_id, fence = _dispatched(client, auth_headers, tmp_path)
    assert _report(client, auth_headers, execution_id, fence, "confirmed").status_code == 200

    response = _undo(client, auth_headers, action_id)

    assert response.status_code == 409, response.text
    assert _audit(client, auth_headers, action_id)[-1]["action"] == "refused_already_undone"
    assert len(executions_of(action_id)) == 1


# --------------------------------------------------------------------------- #
# The probe route (ACTION-EXECUTOR-1, -13)
# --------------------------------------------------------------------------- #


def test_a_probe_creates_one_probe_execution(client: Any, auth_headers: dict[str, str]) -> None:
    """@spec ACTION-EXECUTOR-1 @spec ACTION-EXECUTOR-13: the body is exactly
    ``{agent_id, connector, digest}``; the execution is a ``probe`` with no
    tool, under ``capability_probe`` authority, keyed
    ``probe:<agent>:<connector>:<digest>``.
    """

    agent_id = _agent(client, auth_headers)

    response = _probe(
        client, auth_headers, {"agent_id": agent_id, "connector": CONNECTOR, "digest": DIGEST}
    )

    assert response.status_code == 201, response.text
    execution_id = str(response.json()["execution_id"])
    assert response.json()["state"] == "requested"
    rows = _all_executions()
    assert [str(row["id"]) for row in rows] == [execution_id]
    row = rows[0]
    assert row["kind"] == "probe"
    assert row["tool"] is None
    assert row["subject_action_id"] is None
    assert row["arguments_sha256"] is None
    assert row["forward_arguments"] is None
    assert str(row["agent_id"]) == agent_id
    assert row["connector"] == CONNECTOR
    assert row["connector_digest"] == DIGEST
    assert row["authority_kind"] == "capability_probe"
    assert row["idempotency_key"] == f"probe:{agent_id}:{CONNECTOR}:{DIGEST}"
    read = _execution(client, auth_headers, execution_id)
    assert (read["kind"], read["state"], read["tool"]) == ("probe", "requested", None)


def test_a_replayed_probe_adopts_its_execution(client: Any, auth_headers: dict[str, str]) -> None:
    """@spec ACTION-EXECUTOR-13 @spec ACTION-EXECUTOR-2: one probe execution per key."""

    agent_id = _agent(client, auth_headers)
    body = {"agent_id": agent_id, "connector": CONNECTOR, "digest": DIGEST}
    first = _probe(client, auth_headers, body)
    assert first.status_code == 201, first.text

    replay = _probe(client, auth_headers, body)

    assert replay.status_code == 200, replay.text
    assert replay.json()["execution_id"] == first.json()["execution_id"]
    assert len(_all_executions()) == 1


def test_another_digest_or_agent_is_another_probe(
    client: Any, auth_headers: dict[str, str]
) -> None:
    """@spec ACTION-EXECUTOR-13 @spec ACTION-EXECUTOR-2: the key is per agent,
    connector and digest, so each distinct triple is its own execution.
    """

    agent_id = _agent(client, auth_headers)
    other_agent = _agent(client, auth_headers)
    bodies = [
        {"agent_id": agent_id, "connector": CONNECTOR, "digest": DIGEST},
        {"agent_id": agent_id, "connector": CONNECTOR, "digest": OTHER_DIGEST},
        {"agent_id": agent_id, "connector": "vault", "digest": DIGEST},
        {"agent_id": other_agent, "connector": CONNECTOR, "digest": DIGEST},
    ]

    ids = {_probe(client, auth_headers, body).json()["execution_id"] for body in bodies}

    assert len(ids) == 4
    assert len(_all_executions()) == 4


@pytest.mark.parametrize(
    "extra",
    [
        {"tool": "restore"},
        {"arguments": {"target": {"name": "api"}}},
        {"tool": "scale", "arguments": {"replicas": 0}},
        {"kind": "forward"},
        {"idempotency_key": "probe:forged"},
        {"authority_ref": "pass-1"},
    ],
    ids=["tool", "arguments", "tool and arguments", "kind", "idempotency key", "authority"],
)
def test_a_probe_body_with_any_other_key_is_rejected_and_creates_nothing(
    client: Any, auth_headers: dict[str, str], extra: dict[str, Any]
) -> None:
    """@spec ACTION-EXECUTOR-1: "A probe body carrying a ``tool`` or ``arguments``
    key is rejected and creates no row"; the body is exactly three keys.
    """

    agent_id = _agent(client, auth_headers)

    response = _probe(
        client,
        auth_headers,
        {"agent_id": agent_id, "connector": CONNECTOR, "digest": DIGEST, **extra},
    )

    assert response.status_code == 422, response.text
    assert _all_executions() == []


@pytest.mark.parametrize("missing", ["agent_id", "connector", "digest"])
def test_a_probe_body_missing_a_key_is_rejected(
    client: Any, auth_headers: dict[str, str], missing: str
) -> None:
    """@spec ACTION-EXECUTOR-1."""

    body = {"agent_id": _agent(client, auth_headers), "connector": CONNECTOR, "digest": DIGEST}
    del body[missing]

    assert _probe(client, auth_headers, body).status_code == 422
    assert _all_executions() == []


def test_a_probe_for_an_unknown_agent_creates_nothing(
    client: Any, auth_headers: dict[str, str]
) -> None:
    """@spec ACTION-EXECUTOR-1: an execution runs under an existing agent's binding.

    The known agent's probe beside it proves the route exists, so the unknown
    agent's refusal is the route's answer, not a missing route's 404.
    """

    known = _probe(
        client,
        auth_headers,
        {"agent_id": _agent(client, auth_headers), "connector": CONNECTOR, "digest": DIGEST},
    )
    assert known.status_code == 201, known.text

    response = _probe(
        client,
        auth_headers,
        {"agent_id": str(uuid.uuid4()), "connector": CONNECTOR, "digest": DIGEST},
    )

    assert response.status_code in {404, 422}, response.text
    assert len(_all_executions()) == 1


def test_the_probe_route_requires_a_credential(client: Any) -> None:
    """@spec ACTION-EXECUTOR-1: worker credential only."""

    response = client.post(
        "/connector-capabilities/probes",
        json={"agent_id": str(uuid.uuid4()), "connector": CONNECTOR, "digest": DIGEST},
    )

    assert response.status_code == 401


def test_a_probe_can_never_be_dispatched(client: Any, auth_headers: dict[str, str]) -> None:
    """@spec ACTION-EXECUTOR-1: the probe route "can only produce a ``tools/list``".

    A probe runs the ``list`` phase in ``claimed`` and ends; a ``dispatched``
    commit, which precedes every write call, is refused.
    """

    agent_id = _agent(client, auth_headers)
    created = _probe(
        client, auth_headers, {"agent_id": agent_id, "connector": CONNECTOR, "digest": DIGEST}
    )
    execution_id = str(created.json()["execution_id"])
    fence = _claimed(client, auth_headers, execution_id)

    response = _dispatch(client, auth_headers, execution_id, fence)

    assert response.status_code == 409, response.text
    assert _execution(client, auth_headers, execution_id)["state"] == "claimed"


# --------------------------------------------------------------------------- #
# No route accepts a tool or arguments (ACTION-EXECUTOR-1)
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("route", ["observation", "dispatch", "outcome"])
@pytest.mark.parametrize(
    "extra",
    [{"tool": "scale"}, {"arguments": {"replicas": 0}}],
    ids=["tool", "arguments"],
)
def test_every_execution_route_rejects_a_tool_or_arguments(
    client: Any, auth_headers: dict[str, str], tmp_path: Path, route: str, extra: dict[str, Any]
) -> None:
    """@spec ACTION-EXECUTOR-1: "every executions route rejects a body naming a
    tool or arguments", and the rejected request moves nothing.
    """

    _, execution_id = _requested(client, auth_headers, tmp_path)
    fence = _claimed(client, auth_headers, execution_id)
    payload: dict[str, Any] = {**fence, **extra}
    if route == "observation":
        payload["version"] = POST_VERSION
    if route == "outcome":
        payload.update({"state": "refused", "code": "tool_not_advertised"})

    response = client.post(
        f"/action-executions/{execution_id}/{route}", json=payload, headers=worker_headers()
    )

    assert response.status_code == 422, response.text
    assert _execution(client, auth_headers, execution_id)["state"] == "claimed"


@pytest.mark.parametrize(
    "extra",
    [{"tool": "scale"}, {"arguments": {"replicas": 0}}],
    ids=["tool", "arguments"],
)
def test_the_claim_route_rejects_a_tool_or_arguments(
    client: Any, auth_headers: dict[str, str], tmp_path: Path, extra: dict[str, Any]
) -> None:
    """@spec ACTION-EXECUTOR-1."""

    _, execution_id = _requested(client, auth_headers, tmp_path)

    response = client.post(
        "/action-executions/claim",
        json={"lease_owner": "worker-a", "lease_seconds": 60, **extra},
        headers=worker_headers(),
    )

    assert response.status_code == 422, response.text
    assert _execution(client, auth_headers, execution_id)["state"] == "requested"


@pytest.mark.parametrize(
    ("method", "path"),
    [
        ("post", "/action-executions/claim"),
        ("get", "/action-executions/{id}"),
        ("post", "/action-executions/{id}/observation"),
        ("post", "/action-executions/{id}/dispatch"),
        ("post", "/action-executions/{id}/outcome"),
    ],
)
def test_the_execution_routes_require_a_credential(client: Any, method: str, path: str) -> None:
    """@spec ACTION-EXECUTOR-18: no route answers an unauthenticated caller."""

    response = getattr(client, method)(
        path.format(id=uuid.uuid4()), **({"json": {}} if method == "post" else {})
    )

    assert response.status_code == 401


def test_an_unknown_execution_is_a_404(client: Any, auth_headers: dict[str, str]) -> None:
    """@spec ACTION-EXECUTOR-18."""

    response = client.get(f"/action-executions/{uuid.uuid4()}", headers=auth_headers)

    assert response.status_code == 404
    # The route's own answer, not the router's for a path that does not exist.
    assert response.json()["detail"] != "Not Found"


# --------------------------------------------------------------------------- #
# Claim and the fence (ACTION-EXECUTOR-17, -18)
# --------------------------------------------------------------------------- #


def test_a_claim_takes_a_requested_execution_once_under_a_lease(
    client: Any, auth_headers: dict[str, str], tmp_path: Path
) -> None:
    """@spec ACTION-EXECUTOR-17: "A worker claims a ``requested`` row with a lease
    and a fencing attempt number"; a second worker finds nothing to claim.
    """

    _, execution_id = _requested(client, auth_headers, tmp_path)

    first = _claim(client, auth_headers, "worker-a")
    second = _claim(client, auth_headers, "worker-b")

    assert first.status_code == 200, first.text
    assert first.json()["id"] == execution_id
    assert first.json()["lease_expires_at"] is not None
    _assert_no_snapshot(first.text)
    assert second.status_code == 204, second.text
    stored = _execution(client, auth_headers, execution_id)
    assert stored["state"] == "claimed"
    assert stored["lease_owner"] == "worker-a"


def test_an_empty_queue_claims_nothing(client: Any, auth_headers: dict[str, str]) -> None:
    """@spec ACTION-EXECUTOR-18."""

    assert _claim(client, auth_headers).status_code == 204


@pytest.mark.parametrize(
    "wrong",
    [{"lease_owner": "worker-b"}, {"attempt_delta": 1}],
    ids=["another owner", "another attempt"],
)
def test_a_transition_with_a_wrong_fence_is_refused(
    client: Any, auth_headers: dict[str, str], tmp_path: Path, wrong: dict[str, Any]
) -> None:
    """@spec ACTION-EXECUTOR-17 @spec ACTION-EXECUTOR-18: "every later transition
    presents the fence"; "a stale fence is refused".
    """

    _, execution_id = _requested(client, auth_headers, tmp_path)
    fence = _claimed(client, auth_headers, execution_id)
    bad = dict(fence)
    if "lease_owner" in wrong:
        bad["lease_owner"] = wrong["lease_owner"]
    else:
        bad["attempt"] = fence["attempt"] + wrong["attempt_delta"]

    observed = _observe(client, auth_headers, execution_id, bad, POST_VERSION)
    dispatched = _dispatch(client, auth_headers, execution_id, bad)
    reported = _report(client, auth_headers, execution_id, bad, "refused", "agent_stopped")

    assert (observed.status_code, dispatched.status_code, reported.status_code) == (409, 409, 409)
    stored = _execution(client, auth_headers, execution_id)
    assert stored["state"] == "claimed"
    assert stored["refusal_code"] is None


def test_an_expired_lease_cannot_dispatch(
    client: Any, auth_headers: dict[str, str], tmp_path: Path
) -> None:
    """@spec ACTION-EXECUTOR-17: lease expiry in ``claimed`` returns the row to
    ``requested``, so the expired holder's fence no longer commits ``dispatched``.
    """

    _, execution_id = _requested(client, auth_headers, tmp_path)
    fence = _claimed(client, auth_headers, execution_id)
    assert _observe(client, auth_headers, execution_id, fence, POST_VERSION).status_code == 200
    sql_rows(
        "UPDATE curie.action_executions SET lease_expires_at = now() - interval '1 minute' "
        "WHERE id = :id",
        {"id": uuid.UUID(execution_id)},
    )

    response = _dispatch(client, auth_headers, execution_id, fence)

    assert response.status_code == 409, response.text
    assert _execution(client, auth_headers, execution_id)["state"] != "dispatched"


def test_an_expired_claim_is_reclaimed_with_the_next_attempt_and_fences_out_the_first(
    client: Any, auth_headers: dict[str, str], tmp_path: Path
) -> None:
    """@spec ACTION-EXECUTOR-17: "Lease expiry in ``claimed`` returns the row to
    ``requested`` with the attempt incremented"; the earlier holder is stale.
    """

    _, execution_id = _requested(client, auth_headers, tmp_path)
    first = _claimed(client, auth_headers, execution_id, "worker-a")
    sql_rows(
        "UPDATE curie.action_executions SET lease_expires_at = now() - interval '1 minute' "
        "WHERE id = :id",
        {"id": uuid.UUID(execution_id)},
    )

    second = _claimed(client, auth_headers, execution_id, "worker-b")

    assert second["attempt"] > first["attempt"]
    stale = _observe(client, auth_headers, execution_id, first, POST_VERSION)
    assert stale.status_code == 409, stale.text
    assert _observe(client, auth_headers, execution_id, second, POST_VERSION).status_code == 200


# --------------------------------------------------------------------------- #
# The observation record (ACTION-EXECUTOR-15)
# --------------------------------------------------------------------------- #


def test_an_unchanged_version_lets_the_restore_dispatch(
    client: Any, auth_headers: dict[str, str], tmp_path: Path
) -> None:
    """@spec ACTION-EXECUTOR-15: "Only on equality does it commit ``dispatched``"."""

    _, execution_id = _requested(client, auth_headers, tmp_path)
    fence = _claimed(client, auth_headers, execution_id)

    observed = _observe(client, auth_headers, execution_id, fence, POST_VERSION)

    assert observed.status_code == 200, observed.text
    _assert_no_snapshot(observed.text)
    assert _execution(client, auth_headers, execution_id)["state"] == "claimed"
    dispatched = _dispatch(client, auth_headers, execution_id, fence)
    assert dispatched.status_code == 200, dispatched.text
    stored = _execution(client, auth_headers, execution_id)
    assert stored["state"] == "dispatched"
    assert stored["dispatched_at"] is not None


@pytest.mark.parametrize("observed", [MOVED_VERSION, None, ""], ids=["moved", "absent", "empty"])
def test_a_moved_or_unreadable_version_refuses_the_restore_naming_both_versions(
    client: Any, auth_headers: dict[str, str], tmp_path: Path, observed: str | None
) -> None:
    """@spec ACTION-EXECUTOR-15: "On any difference, or an absent or malformed
    version, it writes audit ``refused_conflict`` naming both versions, ends the
    execution ``refused`` with ``version_conflict``, and makes no ``restore`` call".

    The ``dispatched`` commit that precedes every write is then refused, and the
    action is released, since a conflict is a provable non-write.
    """

    action_id, execution_id = _requested(client, auth_headers, tmp_path)
    fence = _claimed(client, auth_headers, execution_id)

    response = _observe(client, auth_headers, execution_id, fence, observed)

    assert response.status_code in {200, 409}, response.text
    _assert_no_snapshot(response.text)
    stored = _execution(client, auth_headers, execution_id)
    assert stored["state"] == "refused"
    assert stored["refusal_code"] == "version_conflict"
    entry = _audit(client, auth_headers, action_id)[-1]
    assert entry["action"] == "refused_conflict"
    assert entry["authorized"] is False
    named = set((entry["evidence"] or {}).values())
    assert POST_VERSION in named
    if observed:
        assert observed in named
    assert _dispatch(client, auth_headers, execution_id, fence).status_code == 409
    assert _action(client, auth_headers, action_id)["undoable"] is True


def test_a_later_ruling_after_a_conflict_requests_a_new_restore(
    client: Any, auth_headers: dict[str, str], tmp_path: Path
) -> None:
    """@spec ACTION-EXECUTOR-15: a conflict "ends ``refused`` and releases the
    action: a later ruling may try again".
    """

    action_id, execution_id = _requested(client, auth_headers, tmp_path)
    fence = _claimed(client, auth_headers, execution_id)
    _observe(client, auth_headers, execution_id, fence, MOVED_VERSION)

    again = _undo(client, auth_headers, action_id)

    assert again.status_code == 202, again.text
    assert again.json()["execution_id"] != execution_id
    assert [row["state"] for row in executions_of(action_id)] == ["refused", "requested"]


def test_a_restore_cannot_dispatch_before_its_version_is_observed(
    client: Any, auth_headers: dict[str, str], tmp_path: Path
) -> None:
    """@spec ACTION-EXECUTOR-15: the platform compares before the restore, so a
    restore with no recorded observation cannot commit ``dispatched``.
    """

    _, execution_id = _requested(client, auth_headers, tmp_path)
    fence = _claimed(client, auth_headers, execution_id)

    response = _dispatch(client, auth_headers, execution_id, fence)

    assert response.status_code == 409, response.text
    assert _execution(client, auth_headers, execution_id)["state"] == "claimed"


def test_a_replayed_observation_is_idempotent_and_a_different_one_is_refused(
    client: Any, auth_headers: dict[str, str], tmp_path: Path
) -> None:
    """@spec ACTION-EXECUTOR-18: "Each transition is idempotent for the same fence
    and payload and returns ``409`` for a conflicting one".
    """

    _, execution_id = _requested(client, auth_headers, tmp_path)
    fence = _claimed(client, auth_headers, execution_id)
    first = _observe(client, auth_headers, execution_id, fence, POST_VERSION)

    replay = _observe(client, auth_headers, execution_id, fence, POST_VERSION)
    conflicting = _observe(client, auth_headers, execution_id, fence, MOVED_VERSION)

    assert replay.status_code == 200, replay.text
    assert replay.json() == first.json()
    assert conflicting.status_code == 409, conflicting.text
    assert _execution(client, auth_headers, execution_id)["state"] == "claimed"


# --------------------------------------------------------------------------- #
# Dispatch and outcome (ACTION-EXECUTOR-17, -18)
# --------------------------------------------------------------------------- #


def test_a_replayed_dispatch_is_idempotent(
    client: Any, auth_headers: dict[str, str], tmp_path: Path
) -> None:
    """@spec ACTION-EXECUTOR-18."""

    _, execution_id, fence = _dispatched(client, auth_headers, tmp_path)
    before = _execution(client, auth_headers, execution_id)

    replay = _dispatch(client, auth_headers, execution_id, fence)

    assert replay.status_code == 200, replay.text
    assert _execution(client, auth_headers, execution_id) == before


def test_a_confirmed_restore_marks_the_action_undone_by_the_requester(
    client: Any, auth_headers: dict[str, str], tmp_path: Path
) -> None:
    """@spec ACTION-EXECUTOR-18: "A confirmed restore sets ``undone_at`` and
    ``undone_by`` (from ``requested_by``) and appends audit ``confirmed``".
    """

    action_id, execution_id, fence = _dispatched(client, auth_headers, tmp_path)
    assert _action(client, auth_headers, action_id)["undone_at"] is None

    response = _report(client, auth_headers, execution_id, fence, "confirmed")

    assert response.status_code == 200, response.text
    _assert_no_snapshot(response.text)
    stored = _execution(client, auth_headers, execution_id)
    assert stored["state"] == "confirmed"
    assert stored["finished_at"] is not None
    after = _action(client, auth_headers, action_id)
    assert after["undone_at"] is not None
    assert after["undone_by"] == ACTOR
    assert after["undoable"] is False
    assert [e["action"] for e in _audit(client, auth_headers, action_id)] == [
        "authorized",
        "confirmed",
    ]


def test_a_replayed_outcome_returns_the_stored_row_and_a_different_one_is_refused(
    client: Any, auth_headers: dict[str, str], tmp_path: Path
) -> None:
    """@spec ACTION-EXECUTOR-18: "a replayed outcome returns the stored row
    unchanged; a different outcome for the same execution is refused and the
    first stands".
    """

    action_id, execution_id, fence = _dispatched(client, auth_headers, tmp_path)
    first = _report(client, auth_headers, execution_id, fence, "confirmed")
    assert first.status_code == 200, first.text

    replay = _report(client, auth_headers, execution_id, fence, "confirmed")
    different = _report(client, auth_headers, execution_id, fence, "failed", "connector_error")

    assert replay.status_code == 200, replay.text
    assert replay.json() == first.json()
    assert different.status_code == 409, different.text
    assert _execution(client, auth_headers, execution_id)["state"] == "confirmed"
    assert [e["action"] for e in _audit(client, auth_headers, action_id)].count("confirmed") == 1


@pytest.mark.parametrize(
    ("state", "code"),
    [("failed", "connector_error"), ("indeterminate", "response_lost")],
)
def test_a_failed_or_indeterminate_restore_leaves_the_action_not_undone_and_held(
    client: Any, auth_headers: dict[str, str], tmp_path: Path, state: str, code: str
) -> None:
    """@spec ACTION-EXECUTOR-18 @spec ACTION-EXECUTOR-11: "a failed or
    indeterminate one appends its code"; it "does not [set ``undone_at``] and
    still blocks a second undo".
    """

    action_id, execution_id, fence = _dispatched(client, auth_headers, tmp_path)

    response = _report(client, auth_headers, execution_id, fence, state, code)

    assert response.status_code == 200, response.text
    stored = _execution(client, auth_headers, execution_id)
    assert stored["state"] == state
    assert stored["failure_code"] == code
    after = _action(client, auth_headers, action_id)
    assert after["undone_at"] is None
    assert after["undoable"] is False
    last = _audit(client, auth_headers, action_id)[-1]
    assert code in {last["action"], last["reason"], *(last["evidence"] or {}).values()}
    second = _undo(client, auth_headers, action_id)
    assert second.status_code == 409, second.text
    assert len(executions_of(action_id)) == 1


def test_a_pre_dispatch_refusal_releases_the_action(
    client: Any, auth_headers: dict[str, str], tmp_path: Path
) -> None:
    """@spec ACTION-EXECUTOR-17: "``refused`` means provably no write call and
    releases the action".
    """

    action_id, execution_id = _requested(client, auth_headers, tmp_path)
    fence = _claimed(client, auth_headers, execution_id)

    response = _report(
        client, auth_headers, execution_id, fence, "refused", "connector_digest_unavailable"
    )

    assert response.status_code == 200, response.text
    stored = _execution(client, auth_headers, execution_id)
    assert stored["state"] == "refused"
    assert stored["refusal_code"] == "connector_digest_unavailable"
    after = _action(client, auth_headers, action_id)
    assert after["undone_at"] is None
    assert after["undoable"] is True


def test_a_dispatched_execution_cannot_be_reported_refused(
    client: Any, auth_headers: dict[str, str], tmp_path: Path
) -> None:
    """@spec ACTION-EXECUTOR-17: past ``dispatched`` the call may have reached the
    connector, so it can never be recorded as a provable non-write.
    """

    action_id, execution_id, fence = _dispatched(client, auth_headers, tmp_path)

    response = _report(client, auth_headers, execution_id, fence, "refused", "agent_stopped")

    assert response.status_code == 409, response.text
    assert _execution(client, auth_headers, execution_id)["state"] == "dispatched"
    assert _action(client, auth_headers, action_id)["undoable"] is False


@pytest.mark.parametrize("state", ["confirmed", "failed", "indeterminate"])
def test_a_claimed_execution_cannot_be_reported_past_dispatch(
    client: Any, auth_headers: dict[str, str], tmp_path: Path, state: str
) -> None:
    """@spec ACTION-EXECUTOR-17: "Before the ``call`` request the worker commits
    ``dispatched``", so no post-dispatch outcome lands on an undispatched row.
    """

    action_id, execution_id = _requested(client, auth_headers, tmp_path)
    fence = _claimed(client, auth_headers, execution_id)
    assert _observe(client, auth_headers, execution_id, fence, POST_VERSION).status_code == 200

    response = _report(
        client,
        auth_headers,
        execution_id,
        fence,
        state,
        None if state == "confirmed" else "connector_error",
    )

    assert response.status_code == 409, response.text
    assert _execution(client, auth_headers, execution_id)["state"] == "claimed"
    assert _action(client, auth_headers, action_id)["undone_at"] is None


# --------------------------------------------------------------------------- #
# Closed codes (ACTION-EXECUTOR-20)
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("code", PRE_DISPATCH_CODES)
def test_each_pre_dispatch_code_ends_the_execution_refused(
    client: Any, auth_headers: dict[str, str], tmp_path: Path, code: str
) -> None:
    """@spec ACTION-EXECUTOR-20: the pre-dispatch stage ends ``refused``."""

    _, execution_id = _requested(client, auth_headers, tmp_path)
    fence = _claimed(client, auth_headers, execution_id)

    response = _report(client, auth_headers, execution_id, fence, "refused", code)

    assert response.status_code == 200, response.text
    stored = _execution(client, auth_headers, execution_id)
    assert (stored["state"], stored["refusal_code"], stored["failure_code"]) == (
        "refused",
        code,
        None,
    )


@pytest.mark.parametrize(
    ("state", "code"),
    [("failed", code) for code in CONNECTOR_REFUSAL_CODES]
    + [("failed", code) for code in POST_DISPATCH_CODES]
    + [("indeterminate", code) for code in POST_DISPATCH_CODES],
)
def test_each_post_dispatch_code_ends_the_execution_failed_or_indeterminate(
    client: Any, auth_headers: dict[str, str], tmp_path: Path, state: str, code: str
) -> None:
    """@spec ACTION-EXECUTOR-20: connector refusals during ``call`` end ``failed``;
    post-dispatch codes end ``failed`` or ``indeterminate``.
    """

    _, execution_id, fence = _dispatched(client, auth_headers, tmp_path)

    response = _report(client, auth_headers, execution_id, fence, state, code)

    assert response.status_code == 200, response.text
    stored = _execution(client, auth_headers, execution_id)
    assert (stored["state"], stored["failure_code"], stored["refusal_code"]) == (
        state,
        code,
        None,
    )


@pytest.mark.parametrize(
    ("state", "normalized"),
    [("failed", "connector_error"), ("indeterminate", "response_lost")],
)
def test_an_unknown_code_is_normalized_by_stage_at_the_report_route(
    client: Any, auth_headers: dict[str, str], tmp_path: Path, state: str, normalized: str
) -> None:
    """@spec ACTION-EXECUTOR-20: "An unknown code from a runner or connector is
    normalized to ``connector_error`` or ``response_lost`` by stage, never passed
    through": a definite failure is a ``connector_error``, an unknown outcome a
    ``response_lost``. The raw code reaches neither the row, the response nor
    the audit trail.
    """

    action_id, execution_id, fence = _dispatched(client, auth_headers, tmp_path)

    response = _report(client, auth_headers, execution_id, fence, state, "disk_on_fire_<script>")

    assert response.status_code == 200, response.text
    assert "disk_on_fire" not in response.text
    stored = _execution(client, auth_headers, execution_id)
    assert (stored["state"], stored["failure_code"]) == (state, normalized)
    assert "disk_on_fire" not in json.dumps(stored)
    assert "disk_on_fire" not in json.dumps(_audit(client, auth_headers, action_id))


@pytest.mark.parametrize(
    ("state", "code"),
    [("refused", "connector_error"), ("refused", None), ("refused", "refused_unsealed")],
    ids=["post-dispatch code", "no code", "ruling code"],
)
def test_a_refusal_carries_a_pre_dispatch_code(
    client: Any, auth_headers: dict[str, str], tmp_path: Path, state: str, code: str | None
) -> None:
    """@spec ACTION-EXECUTOR-20: "Each code belongs to one stage"; a ``refused``
    execution names a pre-dispatch code and nothing else.
    """

    _, execution_id = _requested(client, auth_headers, tmp_path)
    fence = _claimed(client, auth_headers, execution_id)

    response = _report(client, auth_headers, execution_id, fence, state, code)

    assert response.status_code in {409, 422}, response.text
    assert _execution(client, auth_headers, execution_id)["state"] == "claimed"


# --------------------------------------------------------------------------- #
# A finished probe records capability (ACTION-EXECUTOR-8, -13, -20 amendment)
# --------------------------------------------------------------------------- #


def _capabilities(agent_id: str) -> list[dict[str, Any]]:
    return sql_dicts(
        "SELECT connector, digest, restore_capable FROM curie.connector_capabilities "
        "WHERE agent_id = :agent_id",
        {"agent_id": uuid.UUID(agent_id)},
    )


def _probed(client: Any, headers: dict[str, str], agent_id: str) -> tuple[str, dict[str, Any]]:
    created = _probe(
        client, headers, {"agent_id": agent_id, "connector": CONNECTOR, "digest": DIGEST}
    )
    assert created.status_code == 201, created.text
    execution_id = str(created.json()["execution_id"])
    return execution_id, _claimed(client, headers, execution_id)


def _finish_probe(
    client: Any,
    headers: dict[str, str],
    execution_id: str,
    fence: dict[str, Any],
    advertised: list[str],
) -> Any:
    """Report a probe's ``list`` phase: the verbs that met ACTION-EXECUTOR-13's rule."""

    return client.post(
        f"/action-executions/{execution_id}/outcome",
        json={**fence, "state": "confirmed", "code": None, "advertised": advertised},
        headers=worker_headers(),
    )


@pytest.mark.parametrize(
    ("advertised", "capable"),
    [
        (["restore", "observe_version"], True),
        (["restore"], False),
        (["observe_version"], False),
        ([], False),
    ],
    ids=["pair", "lone restore", "observe only", "neither"],
)
def test_a_finished_probe_records_one_capability_row(
    client: Any, auth_headers: dict[str, str], advertised: list[str], capable: bool
) -> None:
    """@spec ACTION-EXECUTOR-13 @spec ACTION-EXECUTOR-8: "A finished probe records
    one ``connector_capabilities`` row for its agent, connector and digest, with
    ``restore_capable`` true only when the probe observed both ``restore`` and
    ``observe_version``". A lone ``restore`` restores nothing.
    """

    agent_id = _agent(client, auth_headers)
    execution_id, fence = _probed(client, auth_headers, agent_id)
    assert _capabilities(agent_id) == []

    response = _finish_probe(client, auth_headers, execution_id, fence, advertised)

    assert response.status_code == 200, response.text
    assert _execution(client, auth_headers, execution_id)["state"] == "confirmed"
    assert _capabilities(agent_id) == [
        {"connector": CONNECTOR, "digest": DIGEST, "restore_capable": capable}
    ]


def test_a_replayed_probe_report_records_no_second_row(
    client: Any, auth_headers: dict[str, str]
) -> None:
    """@spec ACTION-EXECUTOR-13 @spec ACTION-EXECUTOR-18: one row; the replay is idempotent."""

    agent_id = _agent(client, auth_headers)
    execution_id, fence = _probed(client, auth_headers, agent_id)
    pair = ["restore", "observe_version"]
    first = _finish_probe(client, auth_headers, execution_id, fence, pair)

    replay = _finish_probe(client, auth_headers, execution_id, fence, pair)

    assert first.status_code == 200, first.text
    assert replay.status_code == 200, replay.text
    assert replay.json() == first.json()
    assert len(_capabilities(agent_id)) == 1


def test_a_probe_refused_for_its_digest_records_no_capability(
    client: Any, auth_headers: dict[str, str]
) -> None:
    """@spec ACTION-EXECUTOR-13: a probe whose bracket fails "is refused
    ``connector_digest_unavailable`` and records nothing".
    """

    agent_id = _agent(client, auth_headers)
    execution_id, fence = _probed(client, auth_headers, agent_id)

    response = _report(
        client, auth_headers, execution_id, fence, "refused", "connector_digest_unavailable"
    )

    assert response.status_code == 200, response.text
    assert _execution(client, auth_headers, execution_id)["state"] == "refused"
    assert _capabilities(agent_id) == []


def test_a_capable_probe_makes_an_earlier_action_undoable(
    client: Any, auth_headers: dict[str, str], tmp_path: Path
) -> None:
    """@spec ACTION-EXECUTOR-13: "a conforming reference connector is recorded
    capable and its actions become undoable, including one recorded before the
    probe completed", here through the real probe and report routes.
    """

    agent_id = undoable_agent(client, auth_headers, tmp_path)
    sql_rows("DELETE FROM curie.connector_capabilities")
    action = sealed_action(client, auth_headers, agent_id)
    assert action["undoable"] is False
    execution_id, fence = _probed(client, auth_headers, agent_id)

    reported = _finish_probe(
        client, auth_headers, execution_id, fence, ["restore", "observe_version"]
    )

    assert reported.status_code == 200, reported.text
    assert _action(client, auth_headers, action["id"])["undoable"] is True


# --------------------------------------------------------------------------- #
# Route decisions: worker-only internal routes (security review F2)
# --------------------------------------------------------------------------- #


def _internal_request(
    client: Any, auth_headers: dict[str, str], tmp_path: Path, route: str
) -> tuple[str, dict[str, Any], str | None]:
    """The path and body of one internal route, against a real claimed restore."""

    if route == "probe":
        agent_id = _agent(client, auth_headers)
        return (
            "/connector-capabilities/probes",
            {"agent_id": agent_id, "connector": CONNECTOR, "digest": DIGEST},
            None,
        )
    if route == "claim":
        _, execution_id = _requested(client, auth_headers, tmp_path)
        return "/action-executions/claim", {"lease_owner": "w-x", "lease_seconds": 60}, execution_id
    _, execution_id = _requested(client, auth_headers, tmp_path)
    fence = _claimed(client, auth_headers, execution_id)
    body: dict[str, Any] = dict(fence)
    if route == "observation":
        body["version"] = POST_VERSION
    if route == "outcome":
        body.update({"state": "refused", "code": "agent_stopped"})
    return f"/action-executions/{execution_id}/{route}", body, execution_id


@pytest.mark.parametrize("route", ["probe", "claim", "observation", "dispatch", "outcome"])
@pytest.mark.parametrize("credential", ["platform key", "operator principal"])
def test_the_internal_routes_refuse_the_platform_and_operator_keys(
    client: Any, auth_headers: dict[str, str], tmp_path: Path, route: str, credential: str
) -> None:
    """Route decisions: "The internal probe, claim, observation, report and
    dispatch routes require the internal worker token, not the platform or
    operator key, so a key holder cannot forge a confirmed restore or a
    ``restore_capable`` capability row".
    """

    path, body, execution_id = _internal_request(client, auth_headers, tmp_path, route)
    before = _all_executions()
    headers = auth_headers if credential == "platform key" else operator_headers()

    response = client.post(path, json=body, headers=headers)

    assert response.status_code in {401, 403}, response.text
    assert _all_executions() == before


def test_the_internal_routes_accept_the_worker_token(
    client: Any, auth_headers: dict[str, str], tmp_path: Path
) -> None:
    """Route decisions: the internal worker token is the credential they take."""

    _, execution_id = _requested(client, auth_headers, tmp_path)

    response = client.post(
        "/action-executions/claim",
        json={"lease_owner": "worker-a", "lease_seconds": 60},
        headers=worker_headers(),
    )

    assert response.status_code == 200, response.text
    assert response.json()["id"] == execution_id


def test_the_receipt_stays_readable_with_the_platform_key(
    client: Any, auth_headers: dict[str, str], tmp_path: Path
) -> None:
    """@spec ACTION-EXECUTOR-18: ``GET /action-executions/{id}`` is the receipt the
    CLI reads (ACTION-EXECUTOR-23); only the writing routes moved to the token.
    """

    _, execution_id = _requested(client, auth_headers, tmp_path)

    response = client.get(f"/action-executions/{execution_id}", headers=auth_headers)

    assert response.status_code == 200, response.text


# --------------------------------------------------------------------------- #
# Route decisions: the executor off hands out nothing (security review F5)
# --------------------------------------------------------------------------- #


@contextmanager
def _executor_off() -> Iterator[None]:
    """Turn the executor setting off for the app already running, then back on."""

    os.environ[EXECUTOR_SETTING] = "false"
    get_settings.cache_clear()
    try:
        yield
    finally:
        os.environ[EXECUTOR_SETTING] = "true"
        get_settings.cache_clear()


def test_with_the_executor_off_claim_hands_out_nothing(
    client: Any, auth_headers: dict[str, str], tmp_path: Path
) -> None:
    """Route decisions: "With the executor disabled, claim and dispatch hand out nothing"."""

    _, execution_id = _requested(client, auth_headers, tmp_path)

    with _executor_off():
        response = _claim(client, auth_headers)

    assert response.status_code == 204, response.text
    assert _execution(client, auth_headers, execution_id)["state"] == "requested"


def test_with_the_executor_off_a_held_lease_cannot_dispatch(
    client: Any, auth_headers: dict[str, str], tmp_path: Path
) -> None:
    """Route decisions: the setting is a stop for a lease already held, not only
    for new claims: a ``claimed`` restore with an unchanged observation does not
    enter ``dispatched`` while the executor is off.
    """

    _, execution_id = _requested(client, auth_headers, tmp_path)
    fence = _claimed(client, auth_headers, execution_id)
    assert _observe(client, auth_headers, execution_id, fence, POST_VERSION).status_code == 200

    with _executor_off():
        response = _dispatch(client, auth_headers, execution_id, fence)

    assert response.status_code in {409, 503}, response.text
    stored = _execution(client, auth_headers, execution_id)
    assert stored["state"] != "dispatched"
    assert stored["dispatched_at"] is None


# --------------------------------------------------------------------------- #
# Route decisions: the ruling stores the restore's arguments digest (AE-7)
# --------------------------------------------------------------------------- #


def _canonical_sha256(arguments: dict[str, Any]) -> str:
    """ACTION-EXECUTOR-7's canonical form: sorted keys, ``,``/``:``, non-ASCII kept."""

    text = json.dumps(arguments, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def test_the_ruling_stores_the_restore_arguments_digest(
    client: Any, auth_headers: dict[str, str], tmp_path: Path
) -> None:
    """Route decisions: "The ruling stores ``arguments_sha256`` for the restore call
    it authorizes, as ACTION-EXECUTOR-7 requires of the creator".

    The restore call is ``{"target", "prior_state"}``, plus ``expected_version``
    only when the connector's schema declares it (ACTION-EXECUTOR-15), so the
    stored digest is the canonical digest of one of those two argument objects.
    """

    action_id, _ = _requested(client, auth_headers, tmp_path)

    stored = executions_of(action_id)[0]["arguments_sha256"]

    assert stored is not None
    digest = stored.removeprefix("sha256:")
    base = {"target": TARGET, "prior_state": ENVELOPE}
    assert digest in {
        _canonical_sha256(base),
        _canonical_sha256({**base, "expected_version": POST_VERSION}),
    }


# --------------------------------------------------------------------------- #
# Route decisions: a refused or failed probe does not block a later probe
# --------------------------------------------------------------------------- #


def _probe_rows(agent_id: str) -> list[dict[str, Any]]:
    return sql_dicts(
        "SELECT id, state, idempotency_key, authority_ref FROM curie.action_executions "
        "WHERE agent_id = :agent_id AND kind = 'probe' ORDER BY created_at, id",
        {"agent_id": uuid.UUID(agent_id)},
    )


def _probe_body(agent_id: str) -> dict[str, Any]:
    return {"agent_id": agent_id, "connector": CONNECTOR, "digest": DIGEST}


def _end_probe(client: Any, auth_headers: dict[str, str], agent_id: str, how: str) -> str:
    """Probe once and end it ``refused`` (through the route) or ``failed`` (SQL)."""

    execution_id, fence = _probed(client, auth_headers, agent_id)
    if how == "refused":
        ended = _report(
            client, auth_headers, execution_id, fence, "refused", "connector_unreachable"
        )
        assert ended.status_code == 200, ended.text
    else:
        # No route ends a probe ``failed`` today; the decision still covers it.
        sql_rows(
            "UPDATE curie.action_executions SET state = 'failed', "
            "failure_code = 'connector_error', finished_at = now() WHERE id = :id",
            {"id": uuid.UUID(execution_id)},
        )
    return execution_id


@pytest.mark.parametrize("how", ["refused", "failed"])
def test_a_probe_that_ended_refused_or_failed_is_followed_by_a_new_probe(
    client: Any, auth_headers: dict[str, str], how: str
) -> None:
    """Route decisions: "A probe that ended ``refused`` or ``failed`` does not block
    a later probe of the same agent, connector and digest: the next probe is a
    new execution whose key carries the next probe attempt number". A probe's
    ``authority_ref`` is its probe key.
    """

    agent_id = _agent(client, auth_headers)
    first = _end_probe(client, auth_headers, agent_id, how)

    again = _probe(client, auth_headers, _probe_body(agent_id))

    assert again.status_code == 201, again.text
    assert again.json()["execution_id"] != first
    assert again.json()["state"] == "requested"
    rows = _probe_rows(agent_id)
    assert [str(row["id"]) for row in rows] == [first, again.json()["execution_id"]]
    base = f"probe:{agent_id}:{CONNECTOR}:{DIGEST}"
    keys = [row["idempotency_key"] for row in rows]
    assert all(key.startswith(base) for key in keys)
    assert keys[0] != keys[1]
    assert all(row["authority_ref"] == row["idempotency_key"] for row in rows)


def test_each_failed_probe_attempt_gets_its_own_key(
    client: Any, auth_headers: dict[str, str]
) -> None:
    """Route decisions: the key carries the next probe attempt number each time."""

    agent_id = _agent(client, auth_headers)
    _end_probe(client, auth_headers, agent_id, "refused")
    _end_probe(client, auth_headers, agent_id, "refused")

    third = _probe(client, auth_headers, _probe_body(agent_id))

    assert third.status_code == 201, third.text
    keys = [row["idempotency_key"] for row in _probe_rows(agent_id)]
    assert len(keys) == 3
    assert len(set(keys)) == 3


@pytest.mark.parametrize("stage", ["requested", "claimed", "confirmed"])
def test_a_pending_or_confirmed_probe_is_adopted(
    client: Any, auth_headers: dict[str, str], stage: str
) -> None:
    """Route decisions: "only a non-terminal or confirmed probe is adopted"."""

    agent_id = _agent(client, auth_headers)
    created = _probe(client, auth_headers, _probe_body(agent_id))
    assert created.status_code == 201, created.text
    execution_id = str(created.json()["execution_id"])
    if stage != "requested":
        fence = _claimed(client, auth_headers, execution_id)
        if stage == "confirmed":
            finished = _finish_probe(
                client, auth_headers, execution_id, fence, ["restore", "observe_version"]
            )
            assert finished.status_code == 200, finished.text

    replay = _probe(client, auth_headers, _probe_body(agent_id))

    assert replay.status_code == 200, replay.text
    assert replay.json()["execution_id"] == execution_id
    assert len(_probe_rows(agent_id)) == 1


# --------------------------------------------------------------------------- #
# Route decisions: lease expiry (ACTION-EXECUTOR-17)
# --------------------------------------------------------------------------- #


def _expire(execution_id: str) -> None:
    sql_rows(
        "UPDATE curie.action_executions SET lease_expires_at = now() - interval '1 minute' "
        "WHERE id = :id",
        {"id": uuid.UUID(execution_id)},
    )


def _exhausted(client: Any, auth_headers: dict[str, str], tmp_path: Path) -> tuple[str, str]:
    """A restore whose claim expired three times, then one more claim pass."""

    action_id, execution_id = _requested(client, auth_headers, tmp_path)
    attempts = []
    for owner in ("worker-a", "worker-b", "worker-c"):
        attempts.append(_claimed(client, auth_headers, execution_id, owner)["attempt"])
        _expire(execution_id)
    assert attempts == sorted(set(attempts))
    assert attempts[-1] - attempts[0] == 2
    after = _claim(client, auth_headers, "worker-d")
    assert after.status_code == 204, after.text
    return action_id, execution_id


def test_a_claim_is_attempted_three_times_then_refused_runner_unavailable(
    client: Any, auth_headers: dict[str, str], tmp_path: Path
) -> None:
    """Route decisions: "a claim is attempted at most three times; an expired
    claim before dispatch is reclaimed with the next attempt, the third expiry
    ends ``refused`` with ``runner_unavailable``". A refusal releases the action.
    """

    action_id, execution_id = _exhausted(client, auth_headers, tmp_path)

    stored = _execution(client, auth_headers, execution_id)
    assert (stored["state"], stored["refusal_code"]) == ("refused", "runner_unavailable")
    assert _action(client, auth_headers, action_id)["undoable"] is True


def _expired_dispatch(client: Any, auth_headers: dict[str, str], tmp_path: Path) -> tuple[str, str]:
    action_id, execution_id, _ = _dispatched(client, auth_headers, tmp_path)
    _expire(execution_id)
    assert _claim(client, auth_headers, "worker-b").status_code == 204
    return action_id, execution_id


def test_an_expired_dispatched_execution_ends_indeterminate_response_lost(
    client: Any, auth_headers: dict[str, str], tmp_path: Path
) -> None:
    """Route decisions: "an expired dispatched execution ends ``indeterminate``
    with ``response_lost``"; it may have written, so it is never reclaimed and
    still holds the action.
    """

    action_id, execution_id = _expired_dispatch(client, auth_headers, tmp_path)

    stored = _execution(client, auth_headers, execution_id)
    assert (stored["state"], stored["failure_code"]) == ("indeterminate", "response_lost")
    after = _action(client, auth_headers, action_id)
    assert after["undone_at"] is None
    assert after["undoable"] is False


# --------------------------------------------------------------------------- #
# Route decisions: one closing audit row per terminal restore outcome
# --------------------------------------------------------------------------- #


def _ends_confirmed(client: Any, h: dict[str, str], tmp_path: Path) -> tuple[str, str, str | None]:
    action_id, execution_id, fence = _dispatched(client, h, tmp_path)
    assert _report(client, h, execution_id, fence, "confirmed").status_code == 200
    assert _report(client, h, execution_id, fence, "confirmed").status_code == 200  # replay
    return action_id, "confirmed", None


def _ends_failed(client: Any, h: dict[str, str], tmp_path: Path) -> tuple[str, str, str | None]:
    action_id, execution_id, fence = _dispatched(client, h, tmp_path)
    assert _report(client, h, execution_id, fence, "failed", "connector_error").status_code == 200
    return action_id, "failed", "connector_error"


def _ends_indeterminate(
    client: Any, h: dict[str, str], tmp_path: Path
) -> tuple[str, str, str | None]:
    action_id, execution_id, fence = _dispatched(client, h, tmp_path)
    reported = _report(client, h, execution_id, fence, "indeterminate", "deadline_exceeded")
    assert reported.status_code == 200
    return action_id, "indeterminate", "deadline_exceeded"


def _ends_refused(client: Any, h: dict[str, str], tmp_path: Path) -> tuple[str, str, str | None]:
    action_id, execution_id = _requested(client, h, tmp_path)
    fence = _claimed(client, h, execution_id)
    reported = _report(client, h, execution_id, fence, "refused", "connector_digest_unavailable")
    assert reported.status_code == 200
    return action_id, "refused", "connector_digest_unavailable"


def _ends_conflict(client: Any, h: dict[str, str], tmp_path: Path) -> tuple[str, str, str | None]:
    action_id, execution_id = _requested(client, h, tmp_path)
    fence = _claimed(client, h, execution_id)
    _observe(client, h, execution_id, fence, MOVED_VERSION)
    return action_id, "refused", "version_conflict"


def _ends_exhausted(client: Any, h: dict[str, str], tmp_path: Path) -> tuple[str, str, str | None]:
    action_id, _ = _exhausted(client, h, tmp_path)
    return action_id, "refused", "runner_unavailable"


def _ends_expired_dispatch(
    client: Any, h: dict[str, str], tmp_path: Path
) -> tuple[str, str, str | None]:
    action_id, _ = _expired_dispatch(client, h, tmp_path)
    return action_id, "indeterminate", "response_lost"


_TERMINAL_PATHS = {
    "confirmed": _ends_confirmed,
    "failed": _ends_failed,
    "indeterminate": _ends_indeterminate,
    "refused pre-dispatch": _ends_refused,
    "version conflict": _ends_conflict,
    "claim exhausted": _ends_exhausted,
    "dispatch expired": _ends_expired_dispatch,
}


@pytest.mark.parametrize("path", list(_TERMINAL_PATHS.values()), ids=list(_TERMINAL_PATHS))
def test_every_terminal_restore_writes_one_closing_audit_row(
    client: Any, auth_headers: dict[str, str], tmp_path: Path, path: Any
) -> None:
    """Route decisions: "Every terminal restore outcome writes one closing audit
    row naming its state and code, with versions only" -- including a refusal
    before dispatch, so an action's trail never ends at ``authorized``.
    """

    action_id, state, code = path(client, auth_headers, tmp_path)

    entries = _audit(client, auth_headers, action_id)
    assert entries[0]["action"] == "authorized"
    closing = entries[1:]
    assert len(closing) == 1, [e["action"] for e in closing]
    row = closing[0]
    text = json.dumps(row)
    assert state in text
    if code is not None:
        assert code in text
    _assert_no_snapshot(text)
    for key, value in (row["evidence"] or {}).items():
        assert key in {"execution_id", "state", "code"} or key.endswith("version"), key
        assert value is None or isinstance(value, str), key


# --------------------------------------------------------------------------- #
# Route decisions: uniqueness violations mapped by constraint
# --------------------------------------------------------------------------- #


def test_only_the_live_restore_index_maps_to_restore_in_flight(
    client: Any, auth_headers: dict[str, str], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Route decisions: "A database uniqueness violation is mapped to the
    constraint it names; only the live restore index maps to
    ``refused_restore_in_flight``".

    The ruling's identifiers are pinned so its idempotency key collides with a
    ``refused`` restore already holding that key: a violation of
    ``uq_action_executions_agent_idempotency_key``, not of the live restore
    index (a refused restore is outside it). The ruling must not call that an
    in-flight restore, must not fail with a server error, and must leave one
    row under the key.
    """

    action = _undoable_action(client, auth_headers, tmp_path)
    pinned = uuid.uuid4()
    key = f"restore:{action['id']}:{pinned}"
    sql_rows(
        "INSERT INTO curie.action_executions "
        "(id, kind, agent_id, connector, tool, subject_action_id, connector_digest, "
        "authority_kind, authority_ref, requested_by, idempotency_key, state, attempt, "
        "refusal_code, created_at) "
        "VALUES (:id, 'restore', :agent_id, :connector, 'restore', :subject, :digest, "
        "'undo_ruling', :ref, :actor, :key, 'refused', 1, 'connector_unreachable', now())",
        {
            "id": uuid.uuid4(),
            "agent_id": uuid.UUID(action["agent_id"]),
            "connector": CONNECTOR,
            "subject": uuid.UUID(action["id"]),
            "digest": DIGEST,
            "ref": str(pinned),
            "actor": ACTOR,
            "key": key,
        },
    )
    monkeypatch.setattr(
        "curie_api.routers.actions.uuid",
        types.SimpleNamespace(uuid4=lambda: pinned, UUID=uuid.UUID),
    )

    response = _undo(client, auth_headers, action["id"])

    assert response.status_code < 500, response.text
    assert "refused_restore_in_flight" not in [
        e["action"] for e in _audit(client, auth_headers, action["id"])
    ]
    rows = sql_dicts(
        "SELECT id FROM curie.action_executions WHERE idempotency_key = :key", {"key": key}
    )
    assert len(rows) == 1
