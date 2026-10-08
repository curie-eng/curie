"""The nomination read routes behind ``curie remediation list`` and ``show``.

@spec AUTOMATED-REMEDIATION-20 @spec AUTOMATED-REMEDIATION-21

docs/superpowers/specs/2026-10-07-automated-remediation.md, AUTOMATED-REMEDIATION-20:
the CLI ``remediation list`` and ``show <nomination id>`` are "the operator receipt
with the same fields under ``--json``", and the acceptance reads "the CLI ``show``
output matches the API row for every terminal state". The CLI needs a read route,
so this adds two, on the platform key as the execution receipt is
(``GET /action-executions/{id}``, ACTION-EXECUTOR-23):

* ``GET /remediation-nominations`` with optional ``agent_id``, ``state`` and
  ``limit`` (default 50, at most 200) query parameters, newest first by
  ``created_at`` then id; the body is a JSON list of rows.
* ``GET /remediation-nominations/{nomination_id}``: one row, ``404`` when absent.

Both refuse a request without ``X-Api-Key`` (``401``); the internal worker token
alone is not a credential here (the worker reads its receipts from the database,
as the card loop does). A malformed id, an unknown ``state`` or a ``limit``
outside 1 to 200 is ``422``. The routes join ``_HTTP_OPERATIONS`` as
``/remediation-nominations`` and ``/remediation-nominations/{nomination_id}``
(``test_openapi_telemetry_operations.py`` already fails an unlisted route).

A row has exactly the keys ``id``, ``agent_id``, ``hook``, ``kind``, ``action``,
``target`` (the target key), ``state``, ``stage``, ``authority``, ``code``,
``verification_outcome``, ``approval_id``, ``execution_id``, ``created_at`` and
``decided_at``. It never carries ``arguments``, ``arguments_sha256``,
``reason`` or any other argument value: the model's ``reason`` and the arguments
stay in the database row.

Derived fields (the one derivation the worker's receipts and the CLI share):

=============================  ======================  =========  ============================
state                          stage                   authority  code
=============================  ======================  =========  ============================
``refused``                    ``refused``             ``none``   ``refusal_code``
``received``,
``precondition_pending``       ``nominated``           ``none``   null
``approval_requested``,
``rejected``, ``expired``      ``approval_requested``  ``none``   ``approval_reason``
``admitted``                   ``nominated``           ``policy`` null
``approved``                   ``approval_requested``  ``approval`` ``approval_reason``
``executing``, ``verifying``   ``executed``            by approval_id ``execution_code`` or null
``finished``                   ``verification_outcome`` by approval_id ``execution_code`` or null
=============================  ======================  =========  ============================

``authority`` is ``approval`` when ``approval_id`` is set on an ``approved``,
``executing``, ``verifying`` or ``finished`` row, else ``policy`` on an admitted
or later row, else ``none``. Every identifier is a placeholder.
"""

from __future__ import annotations

import itertools
import json
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from _migration_support import sql_dicts, sql_rows
from curie_api.config import get_settings

pytestmark = pytest.mark.usefixtures("clean_db")

HOOK = "alerts"
ACTION = "scale-out-api"
TARGET_KEY = 'example-scale:"example-api"'
# Values the row must never carry (the non-target argument, the reason, a body).
NON_TARGET = "example-non-target-value-77"
ARGUMENTS = {"namespace": NON_TARGET, "deployment": "example-api", "replicas": 4}
REASON = "reason text the receipt must not carry 31c4"
ROW_KEYS = {
    "id",
    "agent_id",
    "hook",
    "kind",
    "action",
    "target",
    "state",
    "stage",
    "authority",
    "code",
    "verification_outcome",
    "approval_id",
    "execution_id",
    "created_at",
    "decided_at",
}
FORBIDDEN_KEYS = {"arguments", "arguments_sha256", "reason", "granted_arguments"}
APPROVAL = uuid.UUID("aaaaaaaa-0000-4000-8000-000000000001")
EXECUTION = uuid.UUID("bbbbbbbb-0000-4000-8000-000000000002")


_CHANNELS = itertools.count(1)


def _agent(client: Any, headers: dict[str, str]) -> str:
    created = client.post(
        "/agents",
        json={
            "name": f"receipts-bot-{uuid.uuid4().hex[:6]}",
            "channel": {"kind": "slack", "address": f"C0EXAMPLE{next(_CHANNELS) % 10}"},
        },
        headers=headers,
    )
    assert created.status_code == 201, created.text
    return str(created.json()["id"])


def _insert(agent_id: str, *, n: int = 1, age: int = 0, **columns: Any) -> uuid.UUID:
    """One nomination row of ``agent_id`` from delivery ``n``, ``age`` seconds old."""

    event_id = f"hook-{agent_id}-{HOOK}-{n:016x}"
    sql_rows(
        "INSERT INTO curie.remediation_nomination_submissions "
        "(event_id, agent_id, hook, block_sha256) VALUES (:e, :agent_id, :hook, :sha) "
        "ON CONFLICT DO NOTHING",
        {"e": event_id, "agent_id": uuid.UUID(agent_id), "hook": HOOK, "sha": "cd" * 32},
    )
    canonical = json.dumps(ARGUMENTS, sort_keys=True, separators=(",", ":"))
    values: dict[str, Any] = {
        "id": uuid.uuid4(),
        "agent_id": uuid.UUID(agent_id),
        "hook": HOOK,
        "e": event_id,
        "action": ACTION,
        "kind": "remediate",
        "arguments": canonical,
        "sha": "ab" * 32,
        "target": TARGET_KEY,
        "reason": REASON,
        "state": "admitted",
        "refusal_code": None,
        "approval_id": None,
        "execution_id": None,
        "verification_outcome": None,
        "approval_reason": None,
        "execution_code": None,
        "created_at": datetime.now(UTC) - timedelta(seconds=age),
    }
    values.update(columns)
    sql_rows(
        "INSERT INTO curie.remediation_nominations "
        "(id, agent_id, hook, event_id, action, kind, arguments, arguments_sha256, target, "
        "reason, state, refusal_code, approval_id, execution_id, verification_outcome, "
        "approval_reason, execution_code, created_at) "
        "VALUES (:id, :agent_id, :hook, :e, :action, :kind, :arguments, :sha, :target, "
        ":reason, :state, :refusal_code, :approval_id, :execution_id, :verification_outcome, "
        ":approval_reason, :execution_code, :created_at)",
        values,
    )
    return values["id"]


def _get(client: Any, path: str, headers: dict[str, str]) -> Any:
    return client.get(path, headers=headers)


def _rows(client: Any, path: str, headers: dict[str, str]) -> list[dict[str, Any]]:
    response = _get(client, path, headers)
    assert response.status_code == 200, response.text
    rows: list[dict[str, Any]] = response.json()
    return rows


# --------------------------------------------------------------------------- #
# Authentication
# --------------------------------------------------------------------------- #


def test_the_routes_need_the_platform_key(client: Any, auth_headers: dict[str, str]) -> None:
    """@spec AUTOMATED-REMEDIATION-20: the receipt route precedent is the platform key."""

    agent_id = _agent(client, auth_headers)
    nomination_id = _insert(agent_id)
    worker = {"X-Curie-Worker-Token": get_settings().internal_worker_token}

    for path in ("/remediation-nominations", f"/remediation-nominations/{nomination_id}"):
        assert client.get(path).status_code == 401, path
        assert client.get(path, headers=worker).status_code == 401, path
        assert client.get(path, headers={"X-Api-Key": "not-the-key"}).status_code == 401, path
        assert client.get(path, headers=auth_headers).status_code == 200, path


# --------------------------------------------------------------------------- #
# The row
# --------------------------------------------------------------------------- #


def test_a_row_has_exactly_the_receipt_fields_and_no_argument_or_reason(
    client: Any, auth_headers: dict[str, str]
) -> None:
    """@spec AUTOMATED-REMEDIATION-20: "never any other argument value, an envelope,
    a read result, the model's ``reason`` or the alert body".
    """

    agent_id = _agent(client, auth_headers)
    nomination_id = _insert(agent_id)

    response = _get(client, f"/remediation-nominations/{nomination_id}", auth_headers)

    assert response.status_code == 200, response.text
    row = response.json()
    assert set(row) == ROW_KEYS
    assert not FORBIDDEN_KEYS & set(row)
    assert row["id"] == str(nomination_id)
    assert row["agent_id"] == agent_id
    assert row["hook"] == HOOK
    assert row["kind"] == "remediate"
    assert row["action"] == ACTION
    assert row["target"] == TARGET_KEY
    for forbidden in (NON_TARGET, REASON, "arguments"):
        assert forbidden not in response.text
    listed = _get(client, "/remediation-nominations", auth_headers)
    assert listed.status_code == 200, listed.text
    assert listed.json() == [row]
    for forbidden in (NON_TARGET, REASON):
        assert forbidden not in listed.text


_STATES = [
    pytest.param(
        {"state": "refused", "refusal_code": "unknown_action"},
        ("refused", "none", "unknown_action", None),
        id="refused",
    ),
    pytest.param({"state": "received"}, ("nominated", "none", None, None), id="received"),
    pytest.param(
        {"state": "precondition_pending"},
        ("nominated", "none", None, None),
        id="precondition_pending",
    ),
    pytest.param(
        {
            "state": "approval_requested",
            "approval_id": APPROVAL,
            "approval_reason": "out_of_bounds",
        },
        ("approval_requested", "none", "out_of_bounds", None),
        id="approval_requested",
    ),
    pytest.param(
        {"state": "rejected", "approval_id": APPROVAL, "approval_reason": "out_of_bounds"},
        ("approval_requested", "none", "out_of_bounds", None),
        id="rejected",
    ),
    pytest.param(
        {"state": "expired", "approval_id": APPROVAL, "approval_reason": "breaker_open"},
        ("approval_requested", "none", "breaker_open", None),
        id="expired",
    ),
    pytest.param({"state": "admitted"}, ("nominated", "policy", None, None), id="admitted"),
    pytest.param(
        {"state": "approved", "approval_id": APPROVAL, "approval_reason": "out_of_bounds"},
        ("approval_requested", "approval", "out_of_bounds", None),
        id="approved",
    ),
    pytest.param(
        {"state": "executing", "execution_id": EXECUTION},
        ("executed", "policy", None, None),
        id="executing",
    ),
    pytest.param(
        {"state": "verifying", "execution_id": EXECUTION},
        ("executed", "policy", None, None),
        id="verifying-policy",
    ),
    pytest.param(
        {"state": "verifying", "execution_id": EXECUTION, "approval_id": APPROVAL},
        ("executed", "approval", None, None),
        id="verifying-approval",
    ),
    pytest.param(
        {"state": "finished", "execution_id": EXECUTION, "verification_outcome": "verified"},
        ("verified", "policy", None, "verified"),
        id="finished-verified",
    ),
    pytest.param(
        {"state": "finished", "execution_id": EXECUTION, "verification_outcome": "not-recovered"},
        ("not-recovered", "policy", None, "not-recovered"),
        id="finished-not-recovered",
    ),
    pytest.param(
        {
            "state": "finished",
            "execution_id": EXECUTION,
            "approval_id": APPROVAL,
            "verification_outcome": "verifier-unavailable",
        },
        ("verifier-unavailable", "approval", None, "verifier-unavailable"),
        id="finished-verifier-unavailable-approval",
    ),
    pytest.param(
        {"state": "finished", "execution_id": EXECUTION, "verification_outcome": "superseded"},
        ("superseded", "policy", None, "superseded"),
        id="finished-superseded",
    ),
    pytest.param(
        {
            "state": "finished",
            "execution_id": EXECUTION,
            "verification_outcome": "not-recovered",
            "execution_code": "connector_error",
        },
        ("not-recovered", "policy", "connector_error", "not-recovered"),
        id="finished-forward-failed",
    ),
]


@pytest.mark.parametrize(("columns", "expected"), _STATES)
def test_the_derived_fields_follow_the_state(
    client: Any,
    auth_headers: dict[str, str],
    columns: dict[str, Any],
    expected: tuple[str, str, str | None, str | None],
) -> None:
    """@spec AUTOMATED-REMEDIATION-20: stage, authority and code for every state."""

    agent_id = _agent(client, auth_headers)
    nomination_id = _insert(agent_id, **columns)

    response = _get(client, f"/remediation-nominations/{nomination_id}", auth_headers)
    assert response.status_code == 200, response.text
    row = response.json()

    assert (row["stage"], row["authority"], row["code"], row["verification_outcome"]) == expected
    assert row["state"] == columns["state"]
    assert row["approval_id"] == (str(columns["approval_id"]) if "approval_id" in columns else None)
    assert row["execution_id"] == (
        str(columns["execution_id"]) if "execution_id" in columns else None
    )


# --------------------------------------------------------------------------- #
# List: order, filters, bounds
# --------------------------------------------------------------------------- #


def test_the_list_is_newest_first_and_filters_by_agent_and_state(
    client: Any, auth_headers: dict[str, str]
) -> None:
    """@spec AUTOMATED-REMEDIATION-20: the operator's receipt list."""

    first, second = _agent(client, auth_headers), _agent(client, auth_headers)
    oldest = _insert(first, n=1, age=300)
    middle = _insert(first, n=2, age=200, state="refused", refusal_code="unknown_action")
    newest = _insert(first, n=3, age=100)
    other = _insert(second, n=1, age=50)

    everything = _rows(client, "/remediation-nominations", auth_headers)
    assert [row["id"] for row in everything] == [str(other), str(newest), str(middle), str(oldest)]

    mine = _rows(client, f"/remediation-nominations?agent_id={first}", auth_headers)
    assert [row["id"] for row in mine] == [str(newest), str(middle), str(oldest)]

    refused = _rows(
        client, f"/remediation-nominations?agent_id={first}&state=refused", auth_headers
    )
    assert [row["id"] for row in refused] == [str(middle)]

    limited = _rows(client, "/remediation-nominations?limit=2", auth_headers)
    assert [row["id"] for row in limited] == [str(other), str(newest)]


@pytest.mark.parametrize(
    "query",
    [
        "state=example_unknown_state",
        "limit=0",
        "limit=201",
        "limit=many",
        "agent_id=not-a-uuid",
    ],
)
def test_a_malformed_query_is_refused(
    client: Any, auth_headers: dict[str, str], query: str
) -> None:
    """@spec AUTOMATED-REMEDIATION-20: closed filters, bounded page."""

    assert _get(client, f"/remediation-nominations?{query}", auth_headers).status_code == 422


def test_show_of_an_absent_or_malformed_id(client: Any, auth_headers: dict[str, str]) -> None:
    """@spec AUTOMATED-REMEDIATION-20: ``404`` for an unknown id, ``422`` for a malformed one."""

    assert (
        _get(client, f"/remediation-nominations/{uuid.uuid4()}", auth_headers).status_code == 404
    )
    assert _get(client, "/remediation-nominations/not-a-uuid", auth_headers).status_code == 422


def test_the_read_routes_write_nothing(client: Any, auth_headers: dict[str, str]) -> None:
    """@spec AUTOMATED-REMEDIATION-20: a receipt read is a read."""

    agent_id = _agent(client, auth_headers)
    nomination_id = _insert(agent_id)
    before = sql_dicts("SELECT * FROM curie.remediation_nominations")

    _get(client, "/remediation-nominations", auth_headers)
    _get(client, f"/remediation-nominations/{nomination_id}", auth_headers)

    assert sql_dicts("SELECT * FROM curie.remediation_nominations") == before


def test_the_routes_are_registered_as_telemetry_operations() -> None:
    """@spec AUTOMATED-REMEDIATION-21: ``_HTTP_OPERATIONS`` names both routes."""

    from curie_telemetry.metrics import _HTTP_OPERATIONS

    assert "/remediation-nominations" in _HTTP_OPERATIONS
    assert "/remediation-nominations/{nomination_id}" in _HTTP_OPERATIONS
