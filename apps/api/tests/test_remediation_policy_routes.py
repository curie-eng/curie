"""Remediation policy store and administration routes (automated remediation task 3).

Pins AUTOMATED-REMEDIATION-1, -2, -3, -10 (the limit schema) and -24 (the kind
rules) of docs/superpowers/specs/2026-10-07-automated-remediation.md through the
real routes and real Postgres:

* ``/agents/{agent_id}/hooks/{hook}/remediation-policy`` ``GET``, ``PUT`` and
  ``DELETE``, and ``POST .../arm`` and ``POST .../disarm``, under
  ``require_api_key`` (AUTOMATED-REMEDIATION-3);
* every write also needs an ADR 0106 operator principal, recorded as
  ``bound_by`` on the generation; without one it is refused
  ``operator_principal_required``; a hook-key-signed request is ``401``;
* generations follow the source policy rule: compare and swap on
  ``expected_generation`` (``stale_policy_generation``), idempotent on
  ``operation_id`` (``policy_operation_conflict`` for a different intent), never
  reused, immutable rows, and a removal that keeps a positive generation with
  ``active`` false (AUTOMATED-REMEDIATION-2);
* every validation refusal has a named code and creates no generation,
  including the channel-members fallback route and a delta bound.

Request and response shapes follow the source policy routes' precedent
(``apps/api/src/curie_api/routers/hook_source_policy.py``): generations travel as
canonical decimal strings, ``expected_generation`` ``"0"`` binds a hook that has
no policy, ``DELETE`` takes its compare and swap as query parameters, and a
refusal is ``{"detail": {"code": ...}}``. The shapes this file pins are recorded
in ``.projects/plans/task-remediation-policy.tests.md``.

The protected source policy a binding requires (AUTOMATED-REMEDIATION-1) is
seeded as its ``hook_source_policies`` row, because creating one through the
source routes needs the protected broker and runtime, which this task does not
touch.
"""

from __future__ import annotations

import asyncio
import copy
import json
import os
import threading
import time
import uuid
from collections.abc import Iterator
from typing import Any

import asyncpg
import pytest
from _migration_support import sql_dicts, sql_rows
from curie_api import approval_principal, hook_signing, hook_source_signing
from curie_api.config import get_settings
from fastapi.testclient import TestClient
from sqlalchemy import make_url
from sqlalchemy.exc import DBAPIError

pytestmark = pytest.mark.usefixtures("clean_db")

HOOK = "alerts"
ORDINARY_HOOK = "deploys"
UNBOUND_HOOK = "nightly"
OPERATOR = "U0EXAMPLE1"
OTHER_OPERATOR = "U0EXAMPLE2"
CHANNEL = "C0EXAMPLE1"
PRINCIPAL_HEADER = "X-Curie-Approval-Principal"

# Approval routes on the agent: an explicit user list, a user group, and a route
# with no approvers block (the channel-members fallback, ADR 0123).
USERS_ROUTE = "sre-oncall"
GROUP_ROUTE = "sre-group"
CHANNEL_ROUTE = "sre-channel"

REMEDIATION_SETTING = "CURIE_REMEDIATION_ENABLED"

# ---------------------------------------------------------------------------
# A valid policy document (AUTOMATED-REMEDIATION-2). Argument schemas are closed:
# each key has a ``type`` and either an ``allowed`` set or an absolute
# ``minimum``/``maximum`` range. ``target`` names the argument that identifies
# the target and lists its allowed values literally (AUTOMATED-REMEDIATION-7).
# Precondition and verifier are declared reads (AUTOMATED-REMEDIATION-17) on a
# connector other than the acting one.
# ---------------------------------------------------------------------------

PRECONDITION: dict[str, Any] = {
    "connector": "prometheus",
    "tool": "query",
    "arguments": {"query": "sum(rate(http_requests_errors_total[5m]))"},
    "pointer": "/data/result/0/value/1",
    "comparator": "gt",
    "value": 0.5,
}

VERIFIER: dict[str, Any] = {
    "connector": "prometheus",
    "tool": "query",
    "arguments": {"query": "sum(rate(http_requests_errors_total[5m]))"},
    "pointer": "/data/result/0/value/1",
    "comparator": "lt",
    "value": 0.05,
    "settle_seconds": 60,
    "deadline_seconds": 600,
    "interval_seconds": 30,
    "consecutive": 2,
}

SCALE_ACTION: dict[str, Any] = {
    "name": "scale-out-api",
    "kind": "remediate",
    "connector": "k8s",
    "tool": "scale_deployment",
    "arguments": {
        "namespace": {"type": "string", "allowed": ["app"]},
        "deployment": {"type": "string", "allowed": ["api"]},
        "replicas": {"type": "integer", "minimum": 2, "maximum": 6},
    },
    "target": {"argument": "deployment", "allowed": ["api"]},
    "reversibility": "reversible",
    "precondition": PRECONDITION,
    "verifier": VERIFIER,
    "automatic": False,
    "qualification": None,
}

LIMITS: dict[str, Any] = {
    "per_policy_per_hour": 3,
    "per_incident_per_target": 1,
    "incident_window_seconds": 3600,
    "approval_ttl_seconds": 14400,
}


def _policy(**overrides: Any) -> dict[str, Any]:
    document: dict[str, Any] = {
        "route": USERS_ROUTE,
        "limits": copy.deepcopy(LIMITS),
        "actions": [copy.deepcopy(SCALE_ACTION)],
    }
    document.update(overrides)
    return document


def _action(**overrides: Any) -> dict[str, Any]:
    action = copy.deepcopy(SCALE_ACTION)
    action.update(overrides)
    return action


def _tune_action(**overrides: Any) -> dict[str, Any]:
    """A ``tune`` action in its AUTOMATED-REMEDIATION-25 shape (the policy vector's)."""

    rule = "example-claim-slow"
    action: dict[str, Any] = {
        "name": "tune-alert-rule",
        "kind": "tune",
        "connector": "example-rules",
        "tool": "update_rule",
        "rules": {
            rule: {
                "current": {
                    "threshold": {
                        "connector": "example-rules",
                        "tool": "get_rule",
                        "arguments": {"rule": rule},
                        "pointer": f"/rules/{rule}/threshold",
                    }
                },
                "evidence": {
                    "fire_count": {
                        "connector": "example-metrics",
                        "tool": "query_value",
                        "arguments": {"query": "example_alert_fire_count"},
                        "pointer": "/data/1",
                    }
                },
            }
        },
        "change": {
            "threshold": {"type": "number", "minimum": 5, "maximum": 300},
            "retire": {"duplicate_of": [rule]},
        },
        "automatic": False,
        "qualification": None,
    }
    action.update(overrides)
    return action

# ---------------------------------------------------------------------------
# Fixtures and helpers
# ---------------------------------------------------------------------------


@pytest.fixture
def agent_id(client: TestClient, auth_headers: dict[str, str]) -> str:
    """An agent with three approval routes, a protected hook and an ordinary one."""

    created = client.post(
        "/agents",
        json={
            "name": f"sre-bot-{uuid.uuid4().hex[:8]}",
            "channel": {"kind": "slack", "address": CHANNEL},
            "approval_routes": {
                USERS_ROUTE: {
                    "resolution": {"kind": "slack", "address": CHANNEL},
                    "approvers": {"users": [OPERATOR]},
                },
                GROUP_ROUTE: {
                    "resolution": {"kind": "slack", "address": CHANNEL},
                    "approvers": {"group": "S0EXAMPLE1"},
                },
                CHANNEL_ROUTE: {"resolution": {"kind": "slack", "address": CHANNEL}},
            },
        },
        headers=auth_headers,
    )
    assert created.status_code == 201, created.text
    agent = str(created.json()["id"])
    _seed_source_policy(agent, HOOK, protected=True)
    _seed_source_policy(agent, ORDINARY_HOOK, protected=False)
    return agent


def _seed_source_policy(agent: str, hook: str, *, protected: bool) -> None:
    """The ADR 0190 source policy row a protected (or ordinary) hook carries."""

    sql_rows(
        "INSERT INTO curie.hook_source_policies "
        "(agent_id, hook, generation, operation_id, mode, tool_access, runtime_id, "
        "qualification_id, bundle_digest, legacy_generation, updated_at) "
        "VALUES (:agent_id, :hook, 1, :operation_id, :mode, :tool_access, :runtime_id, "
        ":qualification_id, :bundle_digest, 0, now())",
        {
            "agent_id": uuid.UUID(agent),
            "hook": hook,
            "operation_id": uuid.uuid4(),
            "mode": "protected" if protected else "ordinary",
            "tool_access": "read-only" if protected else None,
            "runtime_id": str(uuid.uuid4()) if protected else None,
            "qualification_id": str(uuid.uuid4()) if protected else None,
            "bundle_digest": ("sha256:" + "cd" * 32) if protected else None,
        },
    )


@pytest.fixture
def remediation_off() -> Iterator[None]:
    """The default: ``CURIE_REMEDIATION_ENABLED`` unset (AUTOMATED-REMEDIATION-1)."""

    before = os.environ.pop(REMEDIATION_SETTING, None)
    get_settings.cache_clear()
    try:
        yield
    finally:
        if before is not None:
            os.environ[REMEDIATION_SETTING] = before
        get_settings.cache_clear()


def _operator(subject: str = OPERATOR) -> dict[str, str]:
    """An ADR 0106 operator principal, minted as ``POST /approvals/principals/operator`` does."""

    token = approval_principal.mint(
        get_settings().api_key,
        subject=subject,
        kind="operator",
        scope=approval_principal.APPROVE_SCOPE,
        exp=int(time.time()) + 300,
    )
    return {PRINCIPAL_HEADER: token}


def _admin(auth_headers: dict[str, str], subject: str = OPERATOR) -> dict[str, str]:
    """The administrative credential plus an operator principal: a valid writer."""

    return {**auth_headers, **_operator(subject)}


def _url(agent: str, hook: str = HOOK, suffix: str = "") -> str:
    return f"/agents/{agent}/hooks/{hook}/remediation-policy{suffix}"


def _put(
    client: TestClient,
    agent: str,
    headers: dict[str, str],
    *,
    policy: dict[str, Any] | None = None,
    expected: str = "0",
    operation: str | None = None,
    hook: str = HOOK,
) -> Any:
    return client.put(
        _url(agent, hook),
        json={
            "expected_generation": expected,
            "operation_id": operation or str(uuid.uuid4()),
            "policy": _policy() if policy is None else policy,
        },
        headers=headers,
    )


def _post(
    client: TestClient,
    agent: str,
    verb: str,
    headers: dict[str, str],
    *,
    expected: str,
    operation: str | None = None,
) -> Any:
    return client.post(
        _url(agent, suffix=f"/{verb}"),
        json={"expected_generation": expected, "operation_id": operation or str(uuid.uuid4())},
        headers=headers,
    )


def _delete(
    client: TestClient,
    agent: str,
    headers: dict[str, str],
    *,
    expected: str,
    operation: str | None = None,
) -> Any:
    return client.request(
        "DELETE",
        _url(agent),
        params={"expected_generation": expected, "operation_id": operation or str(uuid.uuid4())},
        headers=headers,
    )


def _code(response: Any) -> str | None:
    """The refusal code, from the source policy refusal body ``{"detail": {"code"}}``."""

    body = response.json()
    detail = body.get("detail") if isinstance(body, dict) else None
    assert isinstance(detail, dict), f"refusal carries no coded detail: {response.text}"
    return detail.get("code")


def _generation(response: Any) -> int:
    """A generation from a response body; the canonical form is a decimal string."""

    raw = response.json()["generation"]
    return int(raw)


def _generation_rows(agent: str, hook: str = HOOK) -> list[dict[str, Any]]:
    return sql_dicts(
        "SELECT generation, operation_id, intent_sha256, document, armed, active, bound_by "
        "FROM curie.remediation_policy_generations "
        "WHERE agent_id = :agent_id AND hook = :hook ORDER BY generation",
        {"agent_id": uuid.UUID(agent), "hook": hook},
    )


def _policy_row(agent: str, hook: str = HOOK) -> dict[str, Any] | None:
    rows = sql_dicts(
        "SELECT generation, operation_id, armed, active FROM curie.remediation_policies "
        "WHERE agent_id = :agent_id AND hook = :hook",
        {"agent_id": uuid.UUID(agent), "hook": hook},
    )
    return rows[0] if rows else None


def _all_generation_rows() -> list[dict[str, Any]]:
    return sql_dicts("SELECT * FROM curie.remediation_policy_generations")


def _bind(client: TestClient, agent: str, auth_headers: dict[str, str]) -> int:
    response = _put(client, agent, _admin(auth_headers))
    assert response.status_code == 200, response.text
    return _generation(response)


# ---------------------------------------------------------------------------
# AUTOMATED-REMEDIATION-3: administration and the operator principal
# ---------------------------------------------------------------------------


def test_a_write_with_an_operator_principal_creates_a_generation_bound_by_it(
    client: TestClient, auth_headers: dict[str, str], agent_id: str
) -> None:
    """@spec AUTOMATED-REMEDIATION-3 @spec AUTOMATED-REMEDIATION-2

    The platform key and an operator principal create generation 1 (or above),
    recorded with the principal as ``bound_by``; the policy row and the
    immutable generation row agree, and the stored document is the one written.
    """

    response = _put(client, agent_id, _admin(auth_headers))

    assert response.status_code == 200, response.text
    assert response.headers.get("cache-control") == "no-store"
    body = response.json()
    generation = _generation(response)
    assert generation >= 1
    assert body["agent_id"] == agent_id
    assert body["hook"] == HOOK
    assert body["active"] is True
    assert body["armed"] is False
    assert body["bound_by"] == OPERATOR
    assert body["policy"] == _policy()

    row = _policy_row(agent_id)
    assert row is not None
    assert row["generation"] == generation
    assert row["active"] is True
    assert row["armed"] is False
    generations = _generation_rows(agent_id)
    assert [g["generation"] for g in generations] == [generation]
    assert generations[0]["bound_by"] == OPERATOR
    assert generations[0]["document"] == _policy()
    assert len(generations[0]["intent_sha256"]) == 64


def test_get_reads_the_current_generation_without_a_principal(
    client: TestClient, auth_headers: dict[str, str], agent_id: str
) -> None:
    """@spec AUTOMATED-REMEDIATION-3: reads need no principal and are not cached."""

    generation = _bind(client, agent_id, auth_headers)

    response = client.get(_url(agent_id), headers=auth_headers)

    assert response.status_code == 200, response.text
    assert response.headers.get("cache-control") == "no-store"
    assert _generation(response) == generation
    assert response.json()["policy"] == _policy()
    assert response.json()["bound_by"] == OPERATOR


@pytest.mark.parametrize("verb", ["put", "delete", "arm", "disarm"])
def test_a_write_without_an_operator_principal_is_refused_and_creates_nothing(
    client: TestClient, auth_headers: dict[str, str], agent_id: str, verb: str
) -> None:
    """@spec AUTOMATED-REMEDIATION-3

    The administrative credential alone is not enough for any write: ``PUT``,
    ``DELETE``, arm and disarm are each refused ``operator_principal_required``
    and leave the policy and its generations as they were.
    """

    if verb == "put":
        before_rows: list[dict[str, Any]] = []
        response = _put(client, agent_id, auth_headers)
    else:
        generation = _bind(client, agent_id, auth_headers)
        before_rows = _generation_rows(agent_id)
        if verb == "delete":
            response = _delete(client, agent_id, auth_headers, expected=str(generation))
        else:
            response = _post(client, agent_id, verb, auth_headers, expected=str(generation))

    assert response.status_code in (401, 403), response.text
    assert _code(response) == "operator_principal_required"
    assert response.headers.get("cache-control") == "no-store"
    assert _generation_rows(agent_id) == before_rows
    if verb == "put":
        assert _policy_row(agent_id) is None


def test_a_principal_without_the_administrative_credential_is_401(
    client: TestClient, agent_id: str
) -> None:
    """@spec AUTOMATED-REMEDIATION-3: the routes sit under ``require_api_key``;
    an operator principal is in addition to it, never instead of it.
    """

    response = _put(client, agent_id, _operator())

    assert response.status_code == 401, response.text
    assert _policy_row(agent_id) is None
    assert _generation_rows(agent_id) == []


def _hook_signed_headers(agent: str, body: bytes) -> dict[str, str]:
    """A valid protected hook source signature for ``agent``/``HOOK``, no platform key."""

    secret = hook_source_signing.derive(
        get_settings().api_key, agent_id=agent, hook=HOOK, generation=1
    )
    timestamp = str(int(time.time()))
    delivery_id = uuid.uuid4().hex
    signature = hook_signing.sign(
        secret,
        timestamp=timestamp,
        delivery_id=delivery_id,
        hook=HOOK,
        tool_access="read-only",
        body=body,
    )
    return {
        "Content-Type": "application/json",
        hook_signing.SIGNATURE_HEADER: signature,
        "X-Curie-Delivery-Id": delivery_id,
        "X-Curie-Timestamp": timestamp,
    }


def test_a_hook_key_signed_write_is_401_and_creates_nothing(
    client: TestClient, agent_id: str
) -> None:
    """@spec AUTOMATED-REMEDIATION-3

    The hook source's scoped key authorizes deliveries and probes and nothing
    administrative: a policy write carrying only a valid hook signature (with or
    without an operator principal) is ``401``, as is the scoped key presented
    where the platform key goes.
    """

    body = json.dumps(
        {"expected_generation": "0", "operation_id": str(uuid.uuid4()), "policy": _policy()}
    ).encode()
    signed = _hook_signed_headers(agent_id, body)

    for headers in (signed, {**signed, **_operator()}):
        response = client.put(_url(agent_id), content=body, headers=headers)
        assert response.status_code == 401, response.text

    scoped = hook_source_signing.derive(
        get_settings().api_key, agent_id=agent_id, hook=HOOK, generation=1
    )
    response = client.put(
        _url(agent_id),
        content=body,
        headers={"Content-Type": "application/json", "X-API-Key": scoped, **_operator()},
    )
    assert response.status_code == 401, response.text

    assert _policy_row(agent_id) is None
    assert _generation_rows(agent_id) == []


def test_a_hook_key_signed_read_is_401(client: TestClient, agent_id: str) -> None:
    """@spec AUTOMATED-REMEDIATION-3: reads too are administrative-credential only."""

    response = client.get(_url(agent_id), headers=_hook_signed_headers(agent_id, b""))

    assert response.status_code == 401, response.text


# ---------------------------------------------------------------------------
# AUTOMATED-REMEDIATION-2: generations, compare and swap, idempotency, removal
# ---------------------------------------------------------------------------


def test_a_stale_expected_generation_is_refused_and_changes_nothing(
    client: TestClient, auth_headers: dict[str, str], agent_id: str
) -> None:
    """@spec AUTOMATED-REMEDIATION-2: compare and swap on ``expected_generation``."""

    generation = _bind(client, agent_id, auth_headers)
    before = _generation_rows(agent_id)
    tighter = _policy(limits={**LIMITS, "per_policy_per_hour": 1})

    for expected in ("0", str(generation + 1)):
        response = _put(client, agent_id, _admin(auth_headers), policy=tighter, expected=expected)
        assert response.status_code == 409, response.text
        assert _code(response) == "stale_policy_generation"
        assert response.headers.get("cache-control") == "no-store"

    for verb in ("arm", "disarm"):
        response = _post(client, agent_id, verb, _admin(auth_headers), expected="0")
        assert response.status_code == 409, response.text
        assert _code(response) == "stale_policy_generation"

    response = _delete(client, agent_id, _admin(auth_headers), expected="0")
    assert response.status_code == 409, response.text
    assert _code(response) == "stale_policy_generation"

    assert _generation_rows(agent_id) == before
    current = client.get(_url(agent_id), headers=auth_headers)
    assert _generation(current) == generation
    assert current.json()["policy"] == _policy()


def test_a_replayed_operation_returns_the_committed_generation(
    client: TestClient, auth_headers: dict[str, str], agent_id: str
) -> None:
    """@spec AUTOMATED-REMEDIATION-2: idempotent on ``operation_id``.

    Replaying the same write under the same id answers the generation it
    committed, even after the policy has moved on, and records nothing new.
    """

    operation = str(uuid.uuid4())
    first = _put(client, agent_id, _admin(auth_headers), operation=operation)
    assert first.status_code == 200, first.text
    committed = _generation(first)

    replay = _put(client, agent_id, _admin(auth_headers), operation=operation)
    assert replay.status_code == 200, replay.text
    assert _generation(replay) == committed
    assert len(_generation_rows(agent_id)) == 1

    armed = _post(client, agent_id, "arm", _admin(auth_headers), expected=str(committed))
    assert armed.status_code == 200, armed.text
    rows_after_arm = _generation_rows(agent_id)

    late_replay = _put(client, agent_id, _admin(auth_headers), operation=operation)
    assert late_replay.status_code == 200, late_replay.text
    assert _generation(late_replay) == committed
    assert _generation_rows(agent_id) == rows_after_arm


def test_a_different_intent_under_a_used_operation_id_is_a_conflict(
    client: TestClient, auth_headers: dict[str, str], agent_id: str
) -> None:
    """@spec AUTOMATED-REMEDIATION-2: ``policy_operation_conflict``, nothing written."""

    operation = str(uuid.uuid4())
    first = _put(client, agent_id, _admin(auth_headers), operation=operation)
    assert first.status_code == 200, first.text
    before = _generation_rows(agent_id)

    different = _put(
        client,
        agent_id,
        _admin(auth_headers),
        policy=_policy(limits={**LIMITS, "per_policy_per_hour": 2}),
        operation=operation,
    )

    assert different.status_code == 409, different.text
    assert _code(different) == "policy_operation_conflict"
    assert _generation_rows(agent_id) == before


def test_arm_disarm_and_tightening_each_create_a_generation_and_rows_are_kept(
    client: TestClient, auth_headers: dict[str, str], agent_id: str
) -> None:
    """@spec AUTOMATED-REMEDIATION-2 @spec AUTOMATED-REMEDIATION-3

    Binding, arming, tightening and disarming each write a new, strictly larger
    generation, each recorded with the principal that wrote it; every earlier
    generation stays readable with its own document and flags.
    """

    g1 = _bind(client, agent_id, auth_headers)

    armed = _post(client, agent_id, "arm", _admin(auth_headers, OTHER_OPERATOR), expected=str(g1))
    assert armed.status_code == 200, armed.text
    assert armed.headers.get("cache-control") == "no-store"
    g2 = _generation(armed)
    assert armed.json()["armed"] is True
    assert armed.json()["bound_by"] == OTHER_OPERATOR

    tighter = _policy(limits={**LIMITS, "per_policy_per_hour": 1})
    tightened = _put(client, agent_id, _admin(auth_headers), policy=tighter, expected=str(g2))
    assert tightened.status_code == 200, tightened.text
    g3 = _generation(tightened)

    disarmed = _post(client, agent_id, "disarm", _admin(auth_headers), expected=str(g3))
    assert disarmed.status_code == 200, disarmed.text
    g4 = _generation(disarmed)
    assert disarmed.json()["armed"] is False

    assert g1 < g2 < g3 < g4
    rows = _generation_rows(agent_id)
    assert [r["generation"] for r in rows] == [g1, g2, g3, g4]
    assert [r["armed"] for r in rows] == [False, True, True, False]
    assert [r["bound_by"] for r in rows] == [OPERATOR, OTHER_OPERATOR, OPERATOR, OPERATOR]
    assert rows[0]["document"] == _policy()
    assert rows[2]["document"] == tighter
    assert rows[3]["document"] == tighter
    row = _policy_row(agent_id)
    assert row is not None and row["generation"] == g4 and row["armed"] is False


def test_removal_keeps_a_positive_generation_with_active_false(
    client: TestClient, auth_headers: dict[str, str], agent_id: str
) -> None:
    """@spec AUTOMATED-REMEDIATION-2

    ``DELETE`` writes a new generation with ``active`` false, ``armed`` false
    and no actions; the row keeps a positive generation, the earlier generation
    is still there, and a later binding takes a number never used before.
    """

    g1 = _bind(client, agent_id, auth_headers)
    armed = _post(client, agent_id, "arm", _admin(auth_headers), expected=str(g1))
    g2 = _generation(armed)

    removed = _delete(client, agent_id, _admin(auth_headers), expected=str(g2))

    assert removed.status_code == 200, removed.text
    assert removed.headers.get("cache-control") == "no-store"
    g3 = _generation(removed)
    assert g3 > g2
    assert removed.json()["active"] is False
    assert removed.json()["armed"] is False
    assert removed.json()["bound_by"] == OPERATOR

    row = _policy_row(agent_id)
    assert row is not None
    assert row["generation"] == g3 > 0
    assert row["active"] is False
    assert row["armed"] is False
    rows = _generation_rows(agent_id)
    assert [r["generation"] for r in rows] == [g1, g2, g3]
    assert rows[-1]["active"] is False and rows[-1]["armed"] is False
    assert rows[-1]["document"].get("actions", []) == []
    assert rows[0]["document"] == _policy()

    read = client.get(_url(agent_id), headers=auth_headers)
    assert read.status_code == 200, read.text
    assert _generation(read) == g3
    assert read.json()["active"] is False

    rebound = _put(client, agent_id, _admin(auth_headers), expected=str(g3))
    assert rebound.status_code == 200, rebound.text
    assert _generation(rebound) > g3
    assert rebound.json()["active"] is True


def test_generation_rows_cannot_be_deleted_or_rewritten_while_the_agent_exists(
    client: TestClient, auth_headers: dict[str, str], agent_id: str
) -> None:
    """@spec AUTOMATED-REMEDIATION-2: immutable generations, kept while the agent exists."""

    _bind(client, agent_id, auth_headers)
    before = _generation_rows(agent_id)

    with pytest.raises(DBAPIError):
        sql_rows(
            "DELETE FROM curie.remediation_policy_generations WHERE agent_id = :agent_id",
            {"agent_id": uuid.UUID(agent_id)},
        )
    with pytest.raises(DBAPIError):
        sql_rows(
            "UPDATE curie.remediation_policy_generations SET armed = true "
            "WHERE agent_id = :agent_id",
            {"agent_id": uuid.UUID(agent_id)},
        )
    assert _generation_rows(agent_id) == before

    sql_rows("DELETE FROM curie.agents WHERE id = :id", {"id": uuid.UUID(agent_id)})
    assert _generation_rows(agent_id) == []
    assert _policy_row(agent_id) is None


# ---------------------------------------------------------------------------
# AUTOMATED-REMEDIATION-1: closed by default, protected hooks only
# ---------------------------------------------------------------------------


@pytest.mark.usefixtures("remediation_off")
def test_policy_routes_stay_writable_with_remediation_off(
    client: TestClient, auth_headers: dict[str, str], agent_id: str
) -> None:
    """@spec AUTOMATED-REMEDIATION-1: a policy can be staged before activation."""

    assert get_settings().remediation_enabled is False

    g1 = _bind(client, agent_id, auth_headers)
    armed = _post(client, agent_id, "arm", _admin(auth_headers), expected=str(g1))

    assert armed.status_code == 200, armed.text
    assert client.get(_url(agent_id), headers=auth_headers).status_code == 200


@pytest.mark.parametrize("hook", [ORDINARY_HOOK, UNBOUND_HOOK], ids=["ordinary", "absent"])
def test_binding_a_hook_without_a_protected_source_policy_is_refused(
    client: TestClient, auth_headers: dict[str, str], agent_id: str, hook: str
) -> None:
    """@spec AUTOMATED-REMEDIATION-1: a remediation policy applies only to a
    hook whose source policy is protected; ordinary or absent is
    ``hook_not_protected`` and creates nothing.
    """

    response = _put(client, agent_id, _admin(auth_headers), hook=hook)

    assert response.status_code in (409, 422), response.text
    assert _code(response) == "hook_not_protected"
    assert _policy_row(agent_id, hook) is None
    assert _generation_rows(agent_id, hook) == []


# ---------------------------------------------------------------------------
# Validation: every refusal has a named code and creates no generation
# (AUTOMATED-REMEDIATION-2, -10 limit schema, -24 kind rules)
# ---------------------------------------------------------------------------


def _without(mapping: dict[str, Any], key: str) -> dict[str, Any]:
    out = copy.deepcopy(mapping)
    out.pop(key)
    return out


def _delta_bounded_arguments() -> dict[str, Any]:
    arguments = copy.deepcopy(SCALE_ACTION["arguments"])
    arguments["replicas"]["max_delta"] = 2
    return arguments


INVALID: list[Any] = [
    # Closed document: an unknown key at every level.
    pytest.param(_policy(extra=True), "policy_unknown_key", id="unknown-top-level-key"),
    pytest.param(
        _policy(limits={**LIMITS, "per_day": 10}), "policy_unknown_key", id="unknown-limit-key"
    ),
    pytest.param(
        _policy(actions=[_action(rollback="kubectl rollout undo")]),
        "policy_unknown_key",
        id="unknown-action-key",
    ),
    # AUTOMATED-REMEDIATION-10: a policy may only tighten the defaults.
    pytest.param(
        _policy(limits={**LIMITS, "per_policy_per_hour": 4}),
        "policy_limit_out_of_bounds",
        id="per-policy-above-ceiling",
    ),
    pytest.param(
        _policy(limits={**LIMITS, "per_incident_per_target": 2}),
        "policy_limit_out_of_bounds",
        id="per-incident-above-ceiling",
    ),
    pytest.param(
        _policy(limits={**LIMITS, "per_policy_per_hour": 2, "per_action_per_hour": 3}),
        "policy_limit_out_of_bounds",
        id="per-action-above-policy",
    ),
    pytest.param(
        _policy(limits={**LIMITS, "incident_window_seconds": 3599}),
        "policy_limit_out_of_bounds",
        id="incident-window-below-default",
    ),
    pytest.param(
        _policy(limits={**LIMITS, "approval_ttl_seconds": 86401}),
        "policy_limit_out_of_bounds",
        id="approval-ttl-above-ceiling",
    ),
    pytest.param(
        _policy(limits={**LIMITS, "per_policy_per_hour": 0}),
        "policy_limit_out_of_bounds",
        id="per-policy-not-positive",
    ),
    # A remediate or prevent action must declare both reads, automatic or not.
    pytest.param(
        _policy(actions=[_without(SCALE_ACTION, "precondition")]),
        "precondition_and_verifier_required",
        id="remediate-without-precondition",
    ),
    pytest.param(
        _policy(actions=[_without(SCALE_ACTION, "verifier")]),
        "precondition_and_verifier_required",
        id="remediate-without-verifier",
    ),
    pytest.param(
        _policy(actions=[_without(_action(kind="prevent"), "verifier")]),
        "precondition_and_verifier_required",
        id="prevent-without-verifier",
    ),
    # Magnitude is bounded by absolute ranges only (no trusted baseline).
    pytest.param(
        _policy(actions=[_action(arguments=_delta_bounded_arguments())]),
        "delta_bound_unsupported",
        id="delta-bound",
    ),
    # The approval route: explicit approvers only, and it must exist.
    pytest.param(
        _policy(route=CHANNEL_ROUTE),
        "route_approvers_not_explicit",
        id="channel-members-fallback-route",
    ),
    pytest.param(_policy(route="no-such-route"), "route_unknown", id="unknown-route"),
    # AUTOMATED-REMEDIATION-24: prevent and tune are never automatic.
    pytest.param(
        _policy(actions=[_action(kind="prevent", automatic=True)]),
        "kind_not_automatic",
        id="automatic-prevent",
    ),
    pytest.param(
        _policy(actions=[_tune_action(automatic=True)]),
        "kind_not_automatic",
        id="automatic-tune",
    ),
    # Document shape.
    pytest.param(_policy(actions=[]), "policy_document_invalid", id="no-actions"),
    pytest.param(
        _policy(actions=[_action(name="Scale Out!")]),
        "policy_document_invalid",
        id="action-name-pattern",
    ),
    pytest.param(
        _policy(actions=[_action(), _action()]),
        "policy_document_invalid",
        id="duplicate-action-name",
    ),
    pytest.param(
        _policy(actions=[_action(kind="restart")]),
        "policy_document_invalid",
        id="unknown-kind",
    ),
    pytest.param(
        _policy(actions=[_action(reversibility="best-effort")]),
        "policy_document_invalid",
        id="unknown-reversibility",
    ),
]


@pytest.mark.parametrize(("policy", "code"), INVALID)
def test_an_invalid_policy_is_refused_with_a_named_code_and_creates_no_generation(
    client: TestClient,
    auth_headers: dict[str, str],
    agent_id: str,
    policy: dict[str, Any],
    code: str,
) -> None:
    """@spec AUTOMATED-REMEDIATION-2 @spec AUTOMATED-REMEDIATION-10 @spec AUTOMATED-REMEDIATION-24

    Each refusal is a named code in ``{"detail": {"code"}}`` (not FastAPI's
    validation list), on the first bind and on a later write alike, and leaves
    no generation behind.
    """

    first = _put(client, agent_id, _admin(auth_headers), policy=policy)
    assert first.status_code == 422, first.text
    assert _code(first) == code
    assert first.headers.get("cache-control") == "no-store"
    assert _policy_row(agent_id) is None
    assert _all_generation_rows() == []

    generation = _bind(client, agent_id, auth_headers)
    before = _generation_rows(agent_id)
    later = _put(client, agent_id, _admin(auth_headers), policy=policy, expected=str(generation))
    assert later.status_code == 422, later.text
    assert _code(later) == code
    assert _generation_rows(agent_id) == before


VALID: list[Any] = [
    pytest.param(_policy(), id="defaults"),
    pytest.param(_policy(route=GROUP_ROUTE), id="user-group-route"),
    pytest.param(
        _policy(
            limits={
                "per_policy_per_hour": 1,
                "per_incident_per_target": 1,
                "per_action_per_hour": 1,
                "incident_window_seconds": 7200,
                "approval_ttl_seconds": 86400,
            }
        ),
        id="tightened-limits",
    ),
    pytest.param(_policy(actions=[_action(kind="prevent")]), id="prevent-not-automatic"),
    pytest.param(
        _policy(actions=[_action(reversibility="idempotent")]), id="idempotent-action"
    ),
]


@pytest.mark.parametrize("policy", VALID)
def test_a_valid_policy_is_accepted_and_stored_verbatim(
    client: TestClient, auth_headers: dict[str, str], agent_id: str, policy: dict[str, Any]
) -> None:
    """@spec AUTOMATED-REMEDIATION-2 @spec AUTOMATED-REMEDIATION-10 @spec AUTOMATED-REMEDIATION-24

    The positive side of every refusal above: tighter limits, a user group
    route, a non-automatic prevent action and an idempotent action are bound.
    """

    response = _put(client, agent_id, _admin(auth_headers), policy=policy)

    assert response.status_code == 200, response.text
    assert response.json()["policy"] == policy
    rows = _generation_rows(agent_id)
    assert len(rows) == 1 and rows[0]["document"] == policy


def test_a_replay_with_reordered_keys_is_the_same_intent(
    client: TestClient, auth_headers: dict[str, str], agent_id: str
) -> None:
    """@spec AUTOMATED-REMEDIATION-2: the intent is the canonical document, so a
    replay whose JSON keys arrive in another order is the same intent, and the
    stored ``intent_sha256`` is a lowercase hex digest.
    """

    operation = str(uuid.uuid4())
    first = _put(client, agent_id, _admin(auth_headers), operation=operation)
    assert first.status_code == 200, first.text

    reordered = json.loads(json.dumps(_policy(), sort_keys=True))
    reordered = dict(reversed(list(reordered.items())))
    replay = _put(client, agent_id, _admin(auth_headers), policy=reordered, operation=operation)
    assert replay.status_code == 200, replay.text
    assert _generation(replay) == _generation(first)

    digest = _generation_rows(agent_id)[0]["intent_sha256"]
    assert len(digest) == 64 and digest == digest.lower()
    int(digest, 16)


# ---------------------------------------------------------------------------
# Review round 1 (C2): a non-finite number is a named refusal, never a 500
# ---------------------------------------------------------------------------


def _number_range(minimum: Any, maximum: Any) -> dict[str, Any]:
    arguments = copy.deepcopy(SCALE_ACTION["arguments"])
    arguments["replicas"] = {"type": "number", "minimum": minimum, "maximum": maximum}
    return arguments


def _string_allowed(*values: Any) -> dict[str, Any]:
    arguments = copy.deepcopy(SCALE_ACTION["arguments"])
    arguments["namespace"] = {"type": "string", "allowed": list(values)}
    return arguments


NON_FINITE: list[Any] = [
    pytest.param(
        _policy(actions=[_action(arguments=_number_range(float("nan"), 6))]),
        id="range-minimum-nan",
    ),
    pytest.param(
        _policy(actions=[_action(arguments=_number_range(2, float("inf")))]),
        id="range-maximum-infinity",
    ),
    pytest.param(
        _policy(actions=[_action(arguments=_string_allowed("app", float("nan")))]),
        id="allowed-item-nan",
    ),
    pytest.param(
        _policy(actions=[_action(target={"argument": "deployment", "allowed": [float("inf")]})]),
        id="target-allowed-infinity",
    ),
    pytest.param(
        _policy(actions=[_action(precondition={**PRECONDITION, "value": float("nan")})]),
        id="precondition-value-nan",
    ),
    pytest.param(
        _policy(actions=[_action(verifier={**VERIFIER, "value": float("-inf")})]),
        id="verifier-value-negative-infinity",
    ),
    pytest.param(
        _policy(
            actions=[
                _action(
                    precondition={
                        **PRECONDITION,
                        "comparator": "in",
                        "value": [0.5, float("nan")],
                    }
                )
            ]
        ),
        id="precondition-in-list-nan",
    ),
    pytest.param(
        _policy(
            actions=[
                _action(
                    verifier={**VERIFIER, "arguments": {"query": "up", "step": float("inf")}}
                )
            ]
        ),
        id="read-arguments-infinity",
    ),
    pytest.param(
        _policy(limits={**LIMITS, "per_policy_per_hour": float("nan")}),
        id="limit-nan",
    ),
]


@pytest.mark.parametrize("policy", NON_FINITE)
def test_a_non_finite_number_anywhere_is_a_named_refusal_not_a_500(
    client: TestClient, auth_headers: dict[str, str], agent_id: str, policy: dict[str, Any]
) -> None:
    """@spec AUTOMATED-REMEDIATION-2: the document is canonical JSON, which has no
    NaN or Infinity. Starlette's ``json.loads`` accepts those tokens, so the
    validator must refuse them with a named code (``policy_document_invalid``)
    before anything digests the document; nothing is written.
    """

    body = {
        "expected_generation": "0",
        "operation_id": str(uuid.uuid4()),
        "policy": policy,
    }
    raw = json.dumps(body)  # allow_nan: emits the NaN / Infinity tokens
    assert "NaN" in raw or "Infinity" in raw

    response = client.put(
        _url(agent_id),
        content=raw.encode(),
        headers={**_admin(auth_headers), "Content-Type": "application/json"},
    )

    assert response.status_code == 422, response.text
    assert _code(response) == "policy_document_invalid"
    assert response.headers.get("cache-control") == "no-store"
    assert _policy_row(agent_id) is None
    assert _all_generation_rows() == []


# ---------------------------------------------------------------------------
# Review round 1 (C3): arming re-checks the approval route
# ---------------------------------------------------------------------------


def _replace_routes(
    client: TestClient, auth_headers: dict[str, str], agent: str, routes: dict[str, Any]
) -> None:
    response = client.patch(
        f"/agents/{agent}", json={"approval_routes": routes}, headers=auth_headers
    )
    assert response.status_code == 200, response.text


@pytest.mark.parametrize(
    ("users_route", "code"),
    [
        pytest.param(
            {"resolution": {"kind": "slack", "address": CHANNEL}},
            "route_approvers_not_explicit",
            id="route-now-channel-members",
        ),
        pytest.param(None, "route_unknown", id="route-now-absent"),
    ],
)
def test_arm_refuses_a_route_that_no_longer_has_explicit_approvers(
    client: TestClient,
    auth_headers: dict[str, str],
    agent_id: str,
    users_route: dict[str, Any] | None,
    code: str,
) -> None:
    """@spec AUTOMATED-REMEDIATION-2 @spec AUTOMATED-REMEDIATION-3

    Arming writes a generation whose route must still satisfy the bind rule: if
    the agent's route now falls back to channel members (or is gone), arm is
    refused with the bind's code and writes nothing.
    """

    generation = _bind(client, agent_id, auth_headers)
    routes: dict[str, Any] = {
        GROUP_ROUTE: {
            "resolution": {"kind": "slack", "address": CHANNEL},
            "approvers": {"group": "S0EXAMPLE1"},
        },
        CHANNEL_ROUTE: {"resolution": {"kind": "slack", "address": CHANNEL}},
    }
    if users_route is not None:
        routes[USERS_ROUTE] = users_route
    _replace_routes(client, auth_headers, agent_id, routes)
    before = _generation_rows(agent_id)

    armed = _post(client, agent_id, "arm", _admin(auth_headers), expected=str(generation))

    assert armed.status_code in (409, 422), armed.text
    assert _code(armed) == code
    assert armed.headers.get("cache-control") == "no-store"
    assert _generation_rows(agent_id) == before
    row = _policy_row(agent_id)
    assert row is not None and row["armed"] is False and row["generation"] == generation


# ---------------------------------------------------------------------------
# Review round 1 (C4): the protected-hook check holds against a concurrent switch
# ---------------------------------------------------------------------------


def _hold_source_switch_to_ordinary(
    agent: str,
    hook: str,
    started: threading.Event,
    hold_seconds: float,
    errors: list[BaseException],
) -> None:
    """Switch the source policy to ordinary in a transaction held open, then commit.

    Plays the source mutation coordinator's row write: the row is locked and
    rewritten before the remediation write starts, and commits while the
    remediation write is (or should be) waiting on it.
    """

    async def run() -> None:
        url = make_url(get_settings().database_url).set(drivername="postgresql")
        connection = await asyncpg.connect(url.render_as_string(hide_password=False))
        try:
            transaction = connection.transaction()
            await transaction.start()
            await connection.execute(
                "UPDATE curie.hook_source_policies SET mode = 'ordinary', tool_access = NULL, "
                "runtime_id = NULL, qualification_id = NULL, bundle_digest = NULL, "
                "generation = generation + 1, updated_at = now() "
                "WHERE agent_id = $1 AND hook = $2",
                uuid.UUID(agent),
                hook,
            )
            started.set()
            await asyncio.sleep(hold_seconds)
            await transaction.commit()
        finally:
            await connection.close()

    try:
        asyncio.run(run())
    except Exception as error:  # noqa: BLE001 - surfaced by the test
        errors.append(error)
        started.set()


@pytest.mark.parametrize("verb", ["bind", "arm"])
def test_a_concurrent_switch_to_ordinary_never_leaves_a_policy_on_an_ordinary_hook(
    client: TestClient, auth_headers: dict[str, str], agent_id: str, verb: str
) -> None:
    """@spec AUTOMATED-REMEDIATION-1

    The source policy is switched to ordinary in a transaction that is open when
    the remediation write starts and commits shortly after. The write must not
    decide on the pre-switch row: it waits for the switch (a share lock on the
    source row, or an equivalent re-read under the write transaction) and is
    refused ``hook_not_protected``, so no bound or armed generation sits on a
    hook that is ordinary.
    """

    generation = _bind(client, agent_id, auth_headers) if verb == "arm" else 0
    before = _generation_rows(agent_id)
    started = threading.Event()
    errors: list[BaseException] = []
    switch = threading.Thread(
        target=_hold_source_switch_to_ordinary,
        args=(agent_id, HOOK, started, 1.5, errors),
    )
    switch.start()
    try:
        assert started.wait(10), "the source switch did not start"
        assert not errors, errors
        if verb == "bind":
            response = _put(client, agent_id, _admin(auth_headers))
        else:
            response = _post(
                client, agent_id, "arm", _admin(auth_headers), expected=str(generation)
            )
    finally:
        switch.join(15)
    assert not errors, errors

    mode = sql_rows(
        "SELECT mode FROM curie.hook_source_policies WHERE agent_id = :a AND hook = :h",
        {"a": uuid.UUID(agent_id), "h": HOOK},
    )
    assert mode == [("ordinary",)]
    assert response.status_code == 409, response.text
    assert _code(response) == "hook_not_protected"
    assert _generation_rows(agent_id) == before
    row = _policy_row(agent_id)
    if verb == "bind":
        assert row is None
    else:
        assert row is not None and row["armed"] is False and row["generation"] == generation
