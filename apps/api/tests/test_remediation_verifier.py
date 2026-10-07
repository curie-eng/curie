"""The verifier: scheduled samples, the four outcomes and independence (plan task 11).

@spec AUTOMATED-REMEDIATION-17 @spec AUTOMATED-REMEDIATION-18 @spec AUTOMATED-REMEDIATION-19

docs/superpowers/specs/2026-10-07-automated-remediation.md, with the
maintainer rulings of 2026-10-07 (M2, M3) and executor amendments E3, E5 and E9:

* AUTOMATED-REMEDIATION-18: when a remediation's forward execution ends
  ``confirmed``, the API schedules the verifier's samples, one ``read``
  execution per sample (AUTOMATED-REMEDIATION-12), each its own scheduled
  execution claiming and releasing its own sandbox. The outcome is
  ``verified`` (``consecutive`` satisfied samples before the deadline),
  ``superseded`` (before ``verified``, another ledger record on the same target
  key, or an observe-only execution against the acting connector reporting a
  version other than the action's ``post_version``), ``not-recovered`` (the
  deadline passes with at least one successful sample and no ``verified``) or
  ``verifier-unavailable`` (no successful sample after settle by the deadline,
  or a read execution refused, which ends the verification at once). Samples
  before ``settle_seconds`` are taken and recorded but never count. A
  ``skipped`` sample is unsuccessful, not refused. A forward execution that ends
  ``failed`` or ``indeterminate`` gets no verifier and finishes
  ``not-recovered``. The outcome is written on the nomination and the ledger
  record once.
* AUTOMATED-REMEDIATION-17: independence is checked at policy write and again
  at admission against the in-force version: the verifier's connector differs
  from the acting connector and the secret names their MCP headers expand are
  disjoint; otherwise ``verifier_not_independent``.
* AUTOMATED-REMEDIATION-19: no outcome triggers an undo; no code path calls
  the undo ruling for a policy record without an approving principal.
* AUTOMATED-REMEDIATION-20, -21: no receipt or log carries a sampled value.

Schedule (AUTOMATED-REMEDIATION-18): one sample due every ``interval_seconds``
after the forward execution's ``dispatched_at`` (``dispatched_at + k x
interval`` for k = 1, 2, ...) up to and including ``deadline_seconds`` after
dispatch, so a verification is at most 60 samples (``deadline_seconds`` <= 60 x
``interval_seconds``); a sample counts when it is due at or after
``dispatched_at + settle_seconds``. Samples are all
created when the forward execution is confirmed (the skip rule of task 7 needs
the successor rows), under the idempotency key namespace
``remediation:<nomination id>:`` and the forward execution's
``authority_kind``.

Surface these tests fix (see ``.projects/plans/task-remediation-verifier.tests.md``):

* scheduling inside ``POST /action-executions/{id}/outcome`` (a forward
  remediation ending ``confirmed``, ``failed`` or ``indeterminate``);
* evaluation after every transition that ends a sample: ``/samples``, a read's
  refused ``/outcome``, the claim route's skip and lease expiry, and
  ``/observation`` on an observe-only execution;
* ``agent_actions.verification_outcome`` and ``verified_at`` (written together,
  once), ``remediation_nominations.verification_outcome`` and state
  ``verifying`` then ``finished``;
* the policy write route refusing ``verifier_not_independent`` (422,
  ``{"detail": {"code"}}``), and
  ``curie_api.remediation_verifier.independence_refusal(session, agent_id,
  action, *, store=None)`` -> ``None`` or ``"verifier_not_independent"`` for
  admission (task 9) to call.

Migration 0091 (review round 1) adds ``remediation_nominations.execution_code``,
the code of a forward execution that ended ``failed``, ``indeterminate`` or
``refused`` after admission; 0089 carries the outcome columns and 0090 the read
executions.

Every identifier is a placeholder.
"""

from __future__ import annotations

import asyncio
import copy
import hashlib
import importlib
import io
import json
import logging
import tarfile
import time
import uuid
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
from _migration_support import sql_dicts, sql_rows
from _sealed_actions import (
    executor_enabled,  # noqa: F401 - fixture, requested by name
    worker_headers,
)
from curie_api import approval_principal
from curie_api.config import get_settings
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

pytestmark = pytest.mark.usefixtures("clean_db", "executor_enabled")

HOOK = "alerts"
GENERATION = 3
EVENT_ID = "event-0000000011"
ACTION_NAME = "scale-out-api"

ACT_CONNECTOR = "k8s"
ACT_TOOL = "scale_deployment"
ACT_DIGEST = "sha256:" + "ab" * 32
ACT_IMAGE = f"ghcr.io/example/k8s-restorer@{ACT_DIGEST}"
READ_CONNECTOR = "example-metrics"
READ_TOOL = "query_value"
READ_DIGEST = "sha256:" + "ef" * 32
READ_IMAGE = f"ghcr.io/example/metrics-mcp@{READ_DIGEST}"
OBSERVE_TOOL = "observe_version"
POST_VERSION = "rv-2001"
OBSERVED_TARGET: dict[str, Any] = {
    "kind": "Deployment",
    "namespace": "example-ns",
    "name": "example-api",
}

USERS_ROUTE = "sre-oncall"
CHANNEL = "C0EXAMPLE9"
OPERATOR = "U0EXAMPLE9"
PRINCIPAL_HEADER = "X-Curie-Approval-Principal"

NOMINATED: dict[str, Any] = {"namespace": "example-ns", "deployment": "example-api", "replicas": 4}
# A sampled value distinctive enough to find in any body or log that leaked it.
SAMPLED = 0.0137
SAMPLED_TEXT = "0.0137"
UNHEALTHY = 0.9

SETTLE = 60
INTERVAL = 30
DEADLINE = 150
# Samples every interval from dispatch up to the deadline: 30 (before settle,
# recorded but never counted), 60 (at settle), 90, 120, 150.
OFFSETS = [30, 60, 90, 120, 150]

VERIFIER: dict[str, Any] = {
    "connector": READ_CONNECTOR,
    "tool": READ_TOOL,
    "arguments": {"query": "scalar(example_error_ratio)"},
    "pointer": "/data/1",
    "comparator": "lt",
    "value": 0.05,
    "settle_seconds": SETTLE,
    "deadline_seconds": DEADLINE,
    "interval_seconds": INTERVAL,
    "consecutive": 2,
}
PRECONDITION: dict[str, Any] = {
    "connector": READ_CONNECTOR,
    "tool": READ_TOOL,
    "arguments": {"query": "scalar(example_error_ratio)"},
    "pointer": "/data/1",
    "comparator": "gt",
    "value": 0.5,
}
ACTION: dict[str, Any] = {
    "name": ACTION_NAME,
    "kind": "remediate",
    "connector": ACT_CONNECTOR,
    "tool": ACT_TOOL,
    "arguments": {
        "namespace": {"type": "string", "allowed": ["example-ns"]},
        "deployment": {"type": "string", "allowed": ["example-api", "example-worker"]},
        "replicas": {"type": "integer", "minimum": 2, "maximum": 6},
    },
    "target": {"argument": "deployment", "allowed": ["example-api", "example-worker"]},
    "reversibility": "idempotent",
    "precondition": PRECONDITION,
    "verifier": VERIFIER,
    "automatic": True,
    "qualification": None,
}
LIMITS: dict[str, Any] = {"per_policy_per_hour": 3, "per_incident_per_target": 1}


def _document(**verifier: Any) -> dict[str, Any]:
    action = copy.deepcopy(ACTION)
    action["verifier"].update(verifier)
    return {"route": USERS_ROUTE, "limits": dict(LIMITS), "actions": [action]}


def _connectors(
    *,
    act_secrets: str = "",
    read_secrets: str = "    secrets:\n      - METRICS_TOKEN\n",
    read_connector: str = READ_CONNECTOR,
) -> str:
    """``connectors.yaml``: the acting ``k8s`` (with its sealing key) and a read connector."""

    act = act_secrets or (
        "    secrets:\n      - name: SNAPSHOT_SEALING_KEY\n        from_secret: k8s-restorer-seal\n"
    )
    return (
        "connectors:\n"
        f"  {ACT_CONNECTOR}:\n    image: {ACT_IMAGE}\n{act}"
        f"  {read_connector}:\n    image: {READ_IMAGE}\n{read_secrets}"
    )


# --------------------------------------------------------------------------- #
# Helpers: an agent whose in-force version declares both connectors
# --------------------------------------------------------------------------- #


def _archive(tmp_path: Path, name: str, connectors: str) -> bytes:
    root = tmp_path / f"bundle-{uuid.uuid4().hex[:8]}"
    (root / ".claude-plugin").mkdir(parents=True)
    (root / ".claude-plugin" / "plugin.json").write_text(
        json.dumps({"name": name, "version": "0.1.0", "description": "t"}), encoding="utf-8"
    )
    (root / "skills" / name).mkdir(parents=True)
    (root / "skills" / name / "SKILL.md").write_text(
        f"---\nname: {name}\ndescription: t\n---\nhi\n", encoding="utf-8"
    )
    (root / "connectors.yaml").write_text(connectors, encoding="utf-8")
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz") as tf:
        tf.add(root, arcname=name)
    return buf.getvalue()


def _deploy(
    client: Any, headers: dict[str, str], tmp_path: Path, agent_id: str, connectors: str
) -> None:
    """A new version carrying ``connectors``, deployed: the in-force version."""

    name = client.get(f"/agents/{agent_id}", headers=headers).json()["name"]
    version = client.post(
        f"/agents/{agent_id}/versions",
        json={"version_label": f"v-{uuid.uuid4().hex[:6]}", "created_by": "test"},
        headers=headers,
    )
    assert version.status_code == 201, version.text
    version_id = str(version.json()["id"])
    upload = client.put(
        f"/agents/{agent_id}/versions/{version_id}/bundle",
        files={"file": ("bundle.tar.gz", _archive(tmp_path, name, connectors))},
        headers=headers,
    )
    assert upload.status_code == 201, upload.text
    deployment = client.post(
        "/deployments",
        json={"agent_id": agent_id, "version_id": version_id, "environment": "dev"},
        headers=headers,
    )
    assert deployment.status_code == 201, deployment.text


def _agent(
    client: Any,
    headers: dict[str, str],
    tmp_path: Path,
    *,
    connectors: str | None = None,
    observe: bool = False,
) -> str:
    """An agent with an approval route, a protected hook and both connectors in force.

    ``observe``: the acting connector's capability row records the
    ``restore`` and ``observe_version`` pair at its digest, so the verifier
    schedules an observe-only execution beside each sample.
    """

    name = f"verifier-bot-{uuid.uuid4().hex[:6]}"
    created = client.post(
        "/agents",
        json={
            "name": name,
            "channel": {"kind": "slack", "address": CHANNEL},
            "approval_routes": {
                USERS_ROUTE: {
                    "resolution": {"kind": "slack", "address": CHANNEL},
                    "approvers": {"users": [OPERATOR]},
                }
            },
        },
        headers=headers,
    )
    assert created.status_code == 201, created.text
    agent_id = str(created.json()["id"])
    _deploy(client, headers, tmp_path, agent_id, connectors or _connectors())
    sql_rows(
        "INSERT INTO curie.hook_source_policies "
        "(agent_id, hook, generation, operation_id, mode, tool_access, runtime_id, "
        "qualification_id, bundle_digest, legacy_generation, updated_at) "
        "VALUES (:agent_id, :hook, 1, :operation_id, 'protected', 'read-only', :runtime_id, "
        ":qualification_id, :bundle_digest, 0, now())",
        {
            "agent_id": uuid.UUID(agent_id),
            "hook": HOOK,
            "operation_id": uuid.uuid4(),
            "runtime_id": str(uuid.uuid4()),
            "qualification_id": str(uuid.uuid4()),
            "bundle_digest": "sha256:" + "cd" * 32,
        },
    )
    if observe:
        sql_rows(
            "INSERT INTO curie.connector_capabilities "
            "(agent_id, connector, digest, restore_capable, observed_at) "
            "VALUES (:agent_id, :connector, :digest, true, now())",
            {"agent_id": uuid.UUID(agent_id), "connector": ACT_CONNECTOR, "digest": ACT_DIGEST},
        )
    return agent_id


def _canonical(arguments: Any) -> str:
    return json.dumps(arguments, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _sha(arguments: Any) -> str:
    return hashlib.sha256(_canonical(arguments).encode("utf-8")).hexdigest()


def _bind_policy(agent_id: str, document: dict[str, Any]) -> None:
    """Generations 1 to ``GENERATION`` of the hook's policy, the last one ``document``."""

    agent = uuid.UUID(agent_id)
    for generation in range(1, GENERATION + 1):
        sql_rows(
            "INSERT INTO curie.remediation_policy_generations "
            "(agent_id, hook, generation, operation_id, intent_sha256, document, armed, "
            "active, bound_by, created_at) "
            "VALUES (:agent_id, :hook, :generation, :operation_id, :intent, "
            "CAST(:document AS jsonb), true, true, :bound_by, now())",
            {
                "agent_id": agent,
                "hook": HOOK,
                "generation": generation,
                "operation_id": uuid.uuid4(),
                "intent": f"{generation:02x}" * 32,
                "document": json.dumps(document),
                "bound_by": OPERATOR,
            },
        )
    sql_rows(
        "INSERT INTO curie.remediation_policies "
        "(agent_id, hook, generation, operation_id, armed, active, updated_at) "
        "VALUES (:agent_id, :hook, :generation, :operation_id, true, true, now())",
        {"agent_id": agent, "hook": HOOK, "generation": GENERATION, "operation_id": uuid.uuid4()},
    )


def _nominate(
    agent_id: str,
    *,
    state: str = "admitted",
    approval_id: uuid.UUID | None = None,
    event_id: str = EVENT_ID,
    arguments: dict[str, Any] | None = None,
) -> uuid.UUID:
    agent = uuid.UUID(agent_id)
    bound = NOMINATED if arguments is None else arguments
    if not sql_rows(
        "SELECT 1 FROM curie.remediation_nomination_submissions WHERE event_id = :e",
        {"e": event_id},
    ):
        sql_rows(
            "INSERT INTO curie.remediation_nomination_submissions "
            "(event_id, agent_id, hook, block_sha256) VALUES (:e, :agent_id, :hook, :sha)",
            {"e": event_id, "agent_id": agent, "hook": HOOK, "sha": "cd" * 32},
        )
    nomination_id = uuid.uuid4()
    sql_rows(
        "INSERT INTO curie.remediation_nominations "
        "(id, agent_id, hook, event_id, admitted_generation, current_generation, action, "
        "kind, arguments, arguments_sha256, target, reason, state, approval_id) "
        "VALUES (:id, :agent_id, :hook, :e, :generation, :generation, :action, 'remediate', "
        ":arguments, :sha, :target, 'error ratio above threshold', :state, :approval_id)",
        {
            "id": nomination_id,
            "agent_id": agent,
            "hook": HOOK,
            "e": event_id,
            "generation": GENERATION,
            "action": ACTION_NAME,
            "arguments": _canonical(bound),
            "sha": _sha(bound),
            "target": f'{ACT_CONNECTOR}:"{bound["deployment"]}"',
            "state": state,
            "approval_id": approval_id,
        },
    )
    return nomination_id


def _session_call(factory: Any) -> Any:
    async def run() -> Any:
        engine = create_async_engine(get_settings().database_url)
        sessions = async_sessionmaker(engine, expire_on_commit=False)
        try:
            async with sessions() as session:
                return await factory(session)
        finally:
            await engine.dispose()

    return asyncio.run(run())


def _create_forward(nomination_id: uuid.UUID, approval_id: uuid.UUID | None = None) -> Any:
    forward = importlib.import_module("curie_api.remediation_forward")
    return _session_call(
        lambda session: forward.create_remediation_forward(
            session, nomination_id, approval_id=approval_id
        )
    )


def _claim(client: Any) -> Any:
    return client.post(
        "/action-executions/claim",
        json={"lease_owner": "worker-a", "lease_seconds": 60},
        headers=worker_headers(),
    )


def _fence(claimed: dict[str, Any]) -> dict[str, Any]:
    return {"lease_owner": claimed["lease_owner"], "attempt": claimed["attempt"]}


def _dispatched(
    client: Any, nomination_id: uuid.UUID, approval_id: uuid.UUID | None = None
) -> tuple[uuid.UUID, dict[str, Any]]:
    """Create, claim and dispatch the nomination's forward execution: (id, fence)."""

    created = _create_forward(nomination_id, approval_id)
    claimed = _claim(client)
    assert claimed.status_code == 200, claimed.text
    assert claimed.json()["id"] == str(created.execution_id)
    fence = _fence(claimed.json())
    dispatched = client.post(
        f"/action-executions/{created.execution_id}/dispatch", json=fence, headers=worker_headers()
    )
    assert dispatched.status_code == 200, dispatched.text
    assert dispatched.json()["state"] == "dispatched", dispatched.text
    return created.execution_id, fence


def _end(
    client: Any, execution_id: uuid.UUID, fence: dict[str, Any], state: str = "confirmed"
) -> Any:
    body: dict[str, Any] = {**fence, "state": state}
    if state == "failed":
        body["code"] = "connector_error"
    elif state == "indeterminate":
        body["code"] = "response_lost"
    return client.post(
        f"/action-executions/{execution_id}/outcome", json=body, headers=worker_headers()
    )


def _executed(
    client: Any, nomination_id: uuid.UUID, approval_id: uuid.UUID | None = None
) -> uuid.UUID:
    """The nomination's forward execution, dispatched and confirmed. Returns its id."""

    execution_id, fence = _dispatched(client, nomination_id, approval_id)
    ended = _end(client, execution_id, fence)
    assert ended.status_code == 200, ended.text
    return execution_id


def _execution(execution_id: Any) -> dict[str, Any]:
    rows = sql_dicts("SELECT * FROM curie.action_executions WHERE id = :id", {"id": execution_id})
    assert len(rows) == 1
    return rows[0]


def _of_nomination(nomination_id: uuid.UUID, *, observe: bool) -> list[dict[str, Any]]:
    """The verification's read executions (samples, or observe-only), by schedule."""

    tool_clause = "tool = :observe" if observe else "tool <> :observe"
    return sql_dicts(
        "SELECT * FROM curie.action_executions WHERE kind = 'read' "
        f"AND idempotency_key LIKE :key AND {tool_clause} ORDER BY not_before, created_at",
        {"key": f"remediation:{nomination_id}:%", "observe": OBSERVE_TOOL},
    )


def _samples(nomination_id: uuid.UUID) -> list[dict[str, Any]]:
    return _of_nomination(nomination_id, observe=False)


def _observes(nomination_id: uuid.UUID) -> list[dict[str, Any]]:
    return _of_nomination(nomination_id, observe=True)


def _ledger(forward_id: uuid.UUID) -> dict[str, Any]:
    action_id = _execution(forward_id)["subject_action_id"]
    rows = sql_dicts("SELECT * FROM curie.agent_actions WHERE id = :id", {"id": action_id})
    assert len(rows) == 1
    return rows[0]


def _nomination(nomination_id: uuid.UUID) -> dict[str, Any]:
    rows = sql_dicts(
        "SELECT * FROM curie.remediation_nominations WHERE id = :id", {"id": nomination_id}
    )
    assert len(rows) == 1
    return rows[0]


def _db_now() -> datetime:
    now: datetime = sql_dicts("SELECT now() AS now")[0]["now"]
    return now


def _shift(delta: timedelta) -> None:
    """Move every schedule ``delta`` into the past, as if that much time had passed.

    ``not_before`` and ``dispatched_at`` move together, so the verification's
    settle and deadline keep their meaning relative to its samples.
    """

    seconds = delta.total_seconds()
    sql_rows(
        "UPDATE curie.action_executions SET "
        "not_before = not_before - make_interval(secs => :s), "
        "dispatched_at = dispatched_at - make_interval(secs => :s)",
        {"s": seconds},
    )


def _make_next_due(nomination_id: uuid.UUID) -> None:
    """Advance time just past the earliest requested sample of this verification."""

    pending = [
        row
        for row in _samples(nomination_id) + _observes(nomination_id)
        if row["state"] == "requested"
    ]
    assert pending, "no sample of this verification is still scheduled"
    earliest = min(row["not_before"] for row in pending)
    _shift(earliest - _db_now() + timedelta(seconds=1))


def _claim_read(client: Any, tool: str = READ_TOOL) -> dict[str, Any]:
    claimed = _claim(client)
    assert claimed.status_code == 200, claimed.text
    body = dict(claimed.json())
    row = _execution(body["id"])
    assert row["kind"] == "read" and row["tool"] == tool, row
    return body


def _report(client: Any, claimed: dict[str, Any], sample: str = "value", value: Any = None) -> Any:
    return client.post(
        f"/action-executions/{claimed['id']}/samples",
        json={**_fence(claimed), "sample": sample, "value": value},
        headers=worker_headers(),
    )


def _take(
    client: Any, nomination_id: uuid.UUID, value: Any = SAMPLED, sample: str = "value"
) -> Any:
    """The next sample of the verification: made due, claimed, reported."""

    _make_next_due(nomination_id)
    claimed = _claim_read(client)
    reported = _report(client, claimed, sample, value if sample == "value" else None)
    assert reported.status_code == 200, reported.text
    return reported


def _refuse_next(client: Any, nomination_id: uuid.UUID, code: str = "sandbox_unavailable") -> None:
    _make_next_due(nomination_id)
    claimed = _claim_read(client)
    refused = client.post(
        f"/action-executions/{claimed['id']}/outcome",
        json={**_fence(claimed), "state": "refused", "code": code},
        headers=worker_headers(),
    )
    assert refused.status_code == 200, refused.text


def _outcome(forward_id: uuid.UUID, nomination_id: uuid.UUID) -> str | None:
    """The outcome, asserting the ledger record and the nomination agree."""

    ledger = _ledger(forward_id)
    nomination = _nomination(nomination_id)
    assert ledger["verification_outcome"] == nomination["verification_outcome"]
    assert (ledger["verified_at"] is None) == (ledger["verification_outcome"] is None)
    outcome: str | None = ledger["verification_outcome"]
    return outcome


def _scenario(
    client: Any,
    headers: dict[str, str],
    tmp_path: Path,
    *,
    document: dict[str, Any] | None = None,
    observe: bool = False,
) -> tuple[str, uuid.UUID, uuid.UUID]:
    """An agent, a bound policy and one confirmed forward remediation."""

    agent_id = _agent(client, headers, tmp_path, observe=observe)
    _bind_policy(agent_id, document or _document())
    nomination_id = _nominate(agent_id)
    forward_id = _executed(client, nomination_id)
    return agent_id, nomination_id, forward_id


def _restores() -> list[dict[str, Any]]:
    return sql_dicts("SELECT * FROM curie.action_executions WHERE kind = 'restore'")


# --------------------------------------------------------------------------- #
# AUTOMATED-REMEDIATION-18: scheduling
# --------------------------------------------------------------------------- #


def test_a_confirmed_forward_schedules_one_read_per_interval_until_the_deadline(
    client: Any, auth_headers: dict[str, str], tmp_path: Path
) -> None:
    """@spec AUTOMATED-REMEDIATION-18 @spec AUTOMATED-REMEDIATION-12: one ``read``
    execution per sample of the declared verifier read (connector, in-force
    digest, tool, canonical arguments, pointer), due every interval from
    dispatch up to the deadline (before settle too), under the forward execution's
    authority. The nomination is ``verifying`` and no outcome is written yet.
    """

    _, nomination_id, forward_id = _scenario(client, auth_headers, tmp_path)

    dispatched_at = _execution(forward_id)["dispatched_at"]
    samples = _samples(nomination_id)
    assert [row["not_before"] - dispatched_at for row in samples] == [
        timedelta(seconds=offset) for offset in OFFSETS
    ]
    for row in samples:
        assert row["state"] == "requested"
        assert row["connector"] == READ_CONNECTOR
        assert row["connector_digest"] == READ_DIGEST
        assert row["tool"] == READ_TOOL
        assert row["forward_arguments"] == VERIFIER["arguments"]
        assert row["arguments_sha256"] == _sha(VERIFIER["arguments"])
        assert row["pointer"] == VERIFIER["pointer"]
        assert row["authority_kind"] == "policy"
        assert row["sample"] is None
    assert len({row["idempotency_key"] for row in samples}) == len(OFFSETS)
    assert _observes(nomination_id) == []
    assert _nomination(nomination_id)["state"] == "verifying"
    assert _outcome(forward_id, nomination_id) is None


def test_no_sample_is_scheduled_before_the_forward_is_confirmed(
    client: Any, auth_headers: dict[str, str], tmp_path: Path
) -> None:
    """@spec AUTOMATED-REMEDIATION-18: "When a forward execution of a remediation
    ends ``confirmed``": a dispatched execution has no verifier yet.
    """

    agent_id = _agent(client, auth_headers, tmp_path)
    _bind_policy(agent_id, _document())
    nomination_id = _nominate(agent_id)
    _dispatched(client, nomination_id)

    assert _samples(nomination_id) == []
    assert sql_dicts("SELECT id FROM curie.action_executions WHERE kind = 'read'") == []


def test_an_approved_remediation_is_verified_under_its_approval_authority(
    client: Any, auth_headers: dict[str, str], tmp_path: Path
) -> None:
    """@spec AUTOMATED-REMEDIATION-16 @spec AUTOMATED-REMEDIATION-18: "The approved
    call is verified like an automatic one"; its samples carry ``approval``.
    """

    agent_id = _agent(client, auth_headers, tmp_path)
    _bind_policy(agent_id, _document())
    approval_id = uuid.uuid4()
    nomination_id = _nominate(agent_id, state="approved", approval_id=approval_id)

    _executed(client, nomination_id, approval_id)

    samples = _samples(nomination_id)
    assert len(samples) == len(OFFSETS)
    assert {row["authority_kind"] for row in samples} == {"approval"}


def test_a_replayed_confirmation_schedules_nothing_more(
    client: Any, auth_headers: dict[str, str], tmp_path: Path
) -> None:
    """@spec AUTOMATED-REMEDIATION-18: one verification per forward execution."""

    agent_id = _agent(client, auth_headers, tmp_path)
    _bind_policy(agent_id, _document())
    nomination_id = _nominate(agent_id)
    execution_id, fence = _dispatched(client, nomination_id)
    assert _end(client, execution_id, fence).status_code == 200

    replay = _end(client, execution_id, fence)

    assert replay.status_code == 200, replay.text
    assert len(_samples(nomination_id)) == len(OFFSETS)


def test_a_verification_is_at_most_sixty_samples(
    client: Any, auth_headers: dict[str, str], tmp_path: Path
) -> None:
    """@spec AUTOMATED-REMEDIATION-12 @spec AUTOMATED-REMEDIATION-17: a deadline of
    60 intervals is exactly 60 samples, the cap.
    """

    document = _document(settle_seconds=10, interval_seconds=10, deadline_seconds=600)
    _, nomination_id, forward_id = _scenario(client, auth_headers, tmp_path, document=document)

    dispatched_at = _execution(forward_id)["dispatched_at"]
    samples = _samples(nomination_id)
    assert len(samples) == 60
    assert samples[0]["not_before"] - dispatched_at == timedelta(seconds=10)
    assert samples[-1]["not_before"] - dispatched_at == timedelta(seconds=600)


@pytest.mark.parametrize(
    ("ending", "code"), [("failed", "connector_error"), ("indeterminate", "response_lost")]
)
def test_a_forward_that_does_not_confirm_gets_no_verifier_and_finishes_not_recovered(
    client: Any, auth_headers: dict[str, str], tmp_path: Path, ending: str, code: str
) -> None:
    """@spec AUTOMATED-REMEDIATION-18: "An execution that ends ``failed``,
    ``indeterminate`` or ``refused`` after admission gets no verifier and finishes
    ``not-recovered`` for reporting, with the execution code": the code is
    recorded on the nomination (``execution_code``, migration 0091).
    """

    agent_id = _agent(client, auth_headers, tmp_path)
    _bind_policy(agent_id, _document())
    nomination_id = _nominate(agent_id)
    execution_id, fence = _dispatched(client, nomination_id)

    ended = _end(client, execution_id, fence, ending)

    assert ended.status_code == 200, ended.text
    assert _samples(nomination_id) == []
    assert _outcome(execution_id, nomination_id) == "not-recovered"
    assert _nomination(nomination_id)["state"] == "finished"
    assert _nomination(nomination_id)["execution_code"] == code
    assert _restores() == []


@pytest.mark.parametrize(
    ("code", "authority"),
    [
        ("agent_stopped", "policy"),
        ("agent_stopped", "approval"),
        ("tool_not_advertised", "policy"),
        ("sandbox_unavailable", "policy"),
        ("arguments_mismatch", "approval"),
    ],
)
def test_a_forward_refused_after_admission_finishes_not_recovered_with_its_code(
    client: Any, auth_headers: dict[str, str], tmp_path: Path, code: str, authority: str
) -> None:
    """@spec AUTOMATED-REMEDIATION-18 @spec AUTOMATED-REMEDIATION-11: a forward
    execution refused before dispatch (a kill between admission and dispatch
    refuses ``agent_stopped``) gets no verifier; its nomination finishes
    ``not-recovered`` with the execution's code. No ledger row exists, so the
    outcome lives on the nomination alone.
    """

    agent_id = _agent(client, auth_headers, tmp_path)
    _bind_policy(agent_id, _document())
    approval_id = uuid.uuid4() if authority == "approval" else None
    nomination_id = _nominate(
        agent_id,
        state="admitted" if approval_id is None else "approved",
        approval_id=approval_id,
    )
    created = _create_forward(nomination_id, approval_id)
    claimed = _claim(client)
    assert claimed.status_code == 200, claimed.text
    assert claimed.json()["id"] == str(created.execution_id)

    refused = client.post(
        f"/action-executions/{created.execution_id}/outcome",
        json={**_fence(claimed.json()), "state": "refused", "code": code},
        headers=worker_headers(),
    )

    assert refused.status_code == 200, refused.text
    assert _execution(created.execution_id)["refusal_code"] == code
    nomination = _nomination(nomination_id)
    assert nomination["state"] == "finished"
    assert nomination["verification_outcome"] == "not-recovered"
    assert nomination["execution_code"] == code
    assert _samples(nomination_id) == []
    assert sql_dicts("SELECT id FROM curie.agent_actions") == []
    assert _restores() == []


def _expire_lease(execution_id: Any) -> None:
    sql_rows(
        "UPDATE curie.action_executions SET lease_expires_at = now() - interval '1 second' "
        "WHERE id = :id",
        {"id": execution_id},
    )


def test_a_forward_whose_dispatch_lease_expires_finishes_not_recovered(
    client: Any, auth_headers: dict[str, str], tmp_path: Path
) -> None:
    """@spec AUTOMATED-REMEDIATION-18 @spec ACTION-EXECUTOR-17: the claim route ends
    a dispatched forward whose lease expired ``indeterminate`` (``response_lost``);
    its nomination finishes ``not-recovered`` with that code, on the ledger record
    too, and no verifier runs.
    """

    agent_id = _agent(client, auth_headers, tmp_path)
    _bind_policy(agent_id, _document())
    nomination_id = _nominate(agent_id)
    execution_id, _ = _dispatched(client, nomination_id)
    _expire_lease(execution_id)

    assert _claim(client).status_code == 204

    row = _execution(execution_id)
    assert row["state"] == "indeterminate"
    assert row["failure_code"] == "response_lost"
    assert _outcome(execution_id, nomination_id) == "not-recovered"
    nomination = _nomination(nomination_id)
    assert nomination["state"] == "finished"
    assert nomination["execution_code"] == "response_lost"
    assert _samples(nomination_id) == []


def test_a_forward_whose_claim_attempts_are_exhausted_finishes_not_recovered(
    client: Any, auth_headers: dict[str, str], tmp_path: Path
) -> None:
    """@spec AUTOMATED-REMEDIATION-18 @spec ACTION-EXECUTOR-17: a claimed forward
    whose lease expires on every attempt is refused ``runner_unavailable`` by the
    claim route once its attempts are spent; no ledger row exists, and its
    nomination finishes ``not-recovered`` with that code and no verifier.
    """

    agent_id = _agent(client, auth_headers, tmp_path)
    _bind_policy(agent_id, _document())
    nomination_id = _nominate(agent_id)
    created = _create_forward(nomination_id)
    for attempt in (1, 2, 3):
        claimed = _claim(client)
        assert claimed.status_code == 200, claimed.text
        assert claimed.json()["id"] == str(created.execution_id)
        assert claimed.json()["attempt"] == attempt
        _expire_lease(created.execution_id)

    assert _claim(client).status_code == 204

    row = _execution(created.execution_id)
    assert row["state"] == "refused"
    assert row["refusal_code"] == "runner_unavailable"
    nomination = _nomination(nomination_id)
    assert nomination["state"] == "finished"
    assert nomination["verification_outcome"] == "not-recovered"
    assert nomination["execution_code"] == "runner_unavailable"
    assert sql_dicts("SELECT id FROM curie.agent_actions") == []
    assert _samples(nomination_id) == []


def test_a_not_reversible_now_refusal_goes_back_to_approval_not_to_an_outcome(
    client: Any, auth_headers: dict[str, str], tmp_path: Path
) -> None:
    """@spec AUTOMATED-REMEDIATION-13 @spec AUTOMATED-REMEDIATION-18: the refusal
    AUTOMATED-REMEDIATION-13 routes back to approval (``not_reversible_now``) is
    not finished ``not-recovered``: the nomination is ``approval_requested`` with
    no outcome and no execution code, and no verifier runs. (``policy_changed``,
    the other such refusal, is the claim-time hook of executor amendment E8,
    task 9.)
    """

    agent_id = _agent(client, auth_headers, tmp_path)  # no capability row
    document = _document()
    document["actions"][0]["reversibility"] = "reversible"
    _bind_policy(agent_id, document)
    nomination_id = _nominate(agent_id)
    created = _create_forward(nomination_id)
    claimed = _claim(client)
    assert claimed.status_code == 200, claimed.text

    dispatched = client.post(
        f"/action-executions/{created.execution_id}/dispatch",
        json=_fence(claimed.json()),
        headers=worker_headers(),
    )

    assert dispatched.status_code == 200, dispatched.text
    assert dispatched.json()["state"] == "refused"
    assert dispatched.json()["refusal_code"] == "not_reversible_now"
    nomination = _nomination(nomination_id)
    assert nomination["state"] == "approval_requested"
    assert nomination["verification_outcome"] is None
    assert nomination["execution_code"] is None
    assert _samples(nomination_id) == []


def test_a_forward_execution_of_another_producer_gets_no_verifier(client: Any) -> None:
    """@spec AUTOMATED-REMEDIATION-18: only a remediation's forward execution is
    verified; a forward execution no nomination names schedules nothing.
    """

    agent_id = uuid.uuid4()
    sql_rows(
        "INSERT INTO curie.agents (id, name) VALUES (:id, :name)",
        {"id": agent_id, "name": f"example-agent-{agent_id.hex[:8]}"},
    )
    execution_id = uuid.uuid4()
    sql_rows(
        "INSERT INTO curie.action_executions (id, kind, agent_id, connector, tool, "
        "connector_digest, authority_kind, authority_ref, idempotency_key, "
        "forward_arguments, arguments_sha256) VALUES (:id, 'forward', :agent_id, :connector, "
        "'scale', :digest, 'approval', :ref, :key, CAST(:arguments AS jsonb), :sha)",
        {
            "id": execution_id,
            "agent_id": agent_id,
            "connector": ACT_CONNECTOR,
            "digest": ACT_DIGEST,
            "ref": str(uuid.uuid4()),
            "key": f"forward:{execution_id}",
            "arguments": '{"replicas":3}',
            "sha": _sha({"replicas": 3}),
        },
    )
    claimed = _claim(client)
    assert claimed.status_code == 200, claimed.text
    fence = _fence(claimed.json())
    assert (
        client.post(
            f"/action-executions/{execution_id}/dispatch", json=fence, headers=worker_headers()
        ).status_code
        == 200
    )

    assert _end(client, execution_id, fence).status_code == 200

    assert sql_dicts("SELECT id FROM curie.action_executions WHERE kind = 'read'") == []


# --------------------------------------------------------------------------- #
# AUTOMATED-REMEDIATION-18: verified, and settle
# --------------------------------------------------------------------------- #


def test_a_recovered_target_is_verified_only_after_settle(
    client: Any, auth_headers: dict[str, str], tmp_path: Path
) -> None:
    """@spec AUTOMATED-REMEDIATION-18: "Samples before ``settle_seconds`` are taken
    and recorded but never count"; ``verified`` once ``consecutive`` (2) samples
    at or after settle satisfy the predicate, written once on the ledger record
    (with ``verified_at``) and the nomination, which finishes; the rest of the
    series is never claimed.
    """

    _, nomination_id, forward_id = _scenario(client, auth_headers, tmp_path)

    _take(client, nomination_id)  # before settle: recorded, never counted
    assert _samples(nomination_id)[0]["sample"] == {"sample": "value", "value": SAMPLED}
    _take(client, nomination_id)  # the first sample at settle
    assert _outcome(forward_id, nomination_id) is None
    assert _nomination(nomination_id)["state"] == "verifying"
    _take(client, nomination_id)

    assert _outcome(forward_id, nomination_id) == "verified"
    ledger = _ledger(forward_id)
    assert ledger["verified_at"] >= _execution(forward_id)["dispatched_at"]
    assert _nomination(nomination_id)["state"] == "finished"
    assert [row["state"] for row in _samples(nomination_id)].count("requested") == 0
    _shift(timedelta(seconds=DEADLINE * 2))
    assert _claim(client).status_code == 204


def test_a_prometheus_numeric_string_satisfies_an_ordering_comparator(
    client: Any, auth_headers: dict[str, str], tmp_path: Path
) -> None:
    """@spec AUTOMATED-REMEDIATION-17: a ``scalar()`` instant query's value is a
    numeric string, compared by numeric value (``"0.01"`` is ``lt`` 0.05).
    """

    _, nomination_id, forward_id = _scenario(client, auth_headers, tmp_path)

    _take(client, nomination_id, UNHEALTHY)
    _take(client, nomination_id, "0.01")
    _take(client, nomination_id, "0.02")

    assert _outcome(forward_id, nomination_id) == "verified"


def test_satisfied_samples_that_are_not_consecutive_do_not_verify(
    client: Any, auth_headers: dict[str, str], tmp_path: Path
) -> None:
    """@spec AUTOMATED-REMEDIATION-18: after settle, satisfied, unsatisfied,
    satisfied, unsatisfied with ``consecutive`` 2 is ``not-recovered``.
    """

    _, nomination_id, forward_id = _scenario(client, auth_headers, tmp_path)

    for value in (UNHEALTHY, SAMPLED, UNHEALTHY, SAMPLED):
        _take(client, nomination_id, value)
        assert _outcome(forward_id, nomination_id) is None
    _take(client, nomination_id, UNHEALTHY)

    assert _outcome(forward_id, nomination_id) == "not-recovered"


# --------------------------------------------------------------------------- #
# AUTOMATED-REMEDIATION-18: not-recovered and verifier-unavailable
# --------------------------------------------------------------------------- #


def test_the_deadline_with_successful_unsatisfied_samples_is_not_recovered(
    client: Any, auth_headers: dict[str, str], tmp_path: Path
) -> None:
    """@spec AUTOMATED-REMEDIATION-18: the outcome waits for the deadline's sample,
    then is ``not-recovered``; the nomination finishes.
    """

    _, nomination_id, forward_id = _scenario(client, auth_headers, tmp_path)

    for _ in OFFSETS[:-1]:
        _take(client, nomination_id, UNHEALTHY)
        assert _outcome(forward_id, nomination_id) is None
    _take(client, nomination_id, UNHEALTHY)

    assert _outcome(forward_id, nomination_id) == "not-recovered"
    assert _nomination(nomination_id)["state"] == "finished"


def test_a_target_healthy_before_settle_and_unhealthy_after_is_not_recovered(
    client: Any, auth_headers: dict[str, str], tmp_path: Path
) -> None:
    """@spec AUTOMATED-REMEDIATION-18 (acceptance): "a target healthy before settle
    and unhealthy after is ``not-recovered``".
    """

    _, nomination_id, forward_id = _scenario(client, auth_headers, tmp_path)

    _take(client, nomination_id, SAMPLED)  # before settle
    for _ in OFFSETS[1:]:
        _take(client, nomination_id, UNHEALTHY)

    assert _outcome(forward_id, nomination_id) == "not-recovered"


def test_satisfied_samples_before_settle_never_verify(
    client: Any, auth_headers: dict[str, str], tmp_path: Path
) -> None:
    """@spec AUTOMATED-REMEDIATION-18: with settle three intervals, two satisfied
    samples before settle do not verify; unhealthy after is ``not-recovered``.
    """

    document = _document(settle_seconds=3 * INTERVAL)
    _, nomination_id, forward_id = _scenario(client, auth_headers, tmp_path, document=document)

    _take(client, nomination_id, SAMPLED)
    _take(client, nomination_id, SAMPLED)
    assert _outcome(forward_id, nomination_id) is None
    for _ in OFFSETS[2:]:
        _take(client, nomination_id, UNHEALTHY)

    assert _outcome(forward_id, nomination_id) == "not-recovered"


def test_a_successful_sample_only_before_settle_is_verifier_unavailable(
    client: Any, auth_headers: dict[str, str], tmp_path: Path
) -> None:
    """@spec AUTOMATED-REMEDIATION-18: "no successful sample after settle"."""

    _, nomination_id, forward_id = _scenario(client, auth_headers, tmp_path)

    _take(client, nomination_id, UNHEALTHY)
    for _ in OFFSETS[1:]:
        _take(client, nomination_id, sample="result_unstructured")

    assert _outcome(forward_id, nomination_id) == "verifier-unavailable"


def test_pointer_absent_is_a_successful_unsatisfied_sample(
    client: Any, auth_headers: dict[str, str], tmp_path: Path
) -> None:
    """@spec AUTOMATED-REMEDIATION-17: an instant query with no series is
    ``pointer_absent``, successful but satisfying only ``absent``: the deadline
    is ``not-recovered``, not ``verifier-unavailable``.
    """

    _, nomination_id, forward_id = _scenario(client, auth_headers, tmp_path)

    for _ in OFFSETS:
        _take(client, nomination_id, sample="pointer_absent")

    assert _outcome(forward_id, nomination_id) == "not-recovered"


@pytest.mark.parametrize(
    "sample", ["result_unstructured", "tool_error", "not_scalar", "value_too_long"]
)
def test_no_successful_sample_by_the_deadline_is_verifier_unavailable(
    client: Any, auth_headers: dict[str, str], tmp_path: Path, sample: str
) -> None:
    """@spec AUTOMATED-REMEDIATION-18 (maintainer ruling M3): an unreadable result
    (a range query, a non-JSON text block) is an unsuccessful sample, and a
    verifier that never gets a successful one ends ``verifier-unavailable``.
    """

    _, nomination_id, forward_id = _scenario(client, auth_headers, tmp_path)

    for _ in OFFSETS[:-1]:
        _take(client, nomination_id, sample=sample)
        assert _outcome(forward_id, nomination_id) is None
    _take(client, nomination_id, sample=sample)

    assert _outcome(forward_id, nomination_id) == "verifier-unavailable"
    assert _nomination(nomination_id)["state"] == "finished"


@pytest.mark.parametrize(
    "code",
    ["connector_unreachable", "sandbox_unavailable", "runner_unavailable", "tool_not_read_only"],
)
def test_a_refused_read_ends_the_verification_verifier_unavailable(
    client: Any, auth_headers: dict[str, str], tmp_path: Path, code: str
) -> None:
    """@spec AUTOMATED-REMEDIATION-18: ``verifier-unavailable`` when "the read
    execution is refused" (a read connector made unreachable, a sandbox
    incident): at once, even after a satisfied sample, and the rest of the series
    is never claimed.
    """

    _, nomination_id, forward_id = _scenario(client, auth_headers, tmp_path)
    _take(client, nomination_id)
    _take(client, nomination_id)

    _refuse_next(client, nomination_id, code)

    assert _outcome(forward_id, nomination_id) == "verifier-unavailable"
    assert _nomination(nomination_id)["state"] == "finished"
    assert [row["state"] for row in _samples(nomination_id)].count("requested") == 0
    _shift(timedelta(seconds=DEADLINE * 2))
    assert _claim(client).status_code == 204


def test_a_refused_read_before_settle_ends_the_verification_too(
    client: Any, auth_headers: dict[str, str], tmp_path: Path
) -> None:
    """@spec AUTOMATED-REMEDIATION-18: the refusal rule has no settle condition."""

    _, nomination_id, forward_id = _scenario(client, auth_headers, tmp_path)

    _refuse_next(client, nomination_id)

    assert _outcome(forward_id, nomination_id) == "verifier-unavailable"


@pytest.mark.parametrize(
    ("last", "expected"),
    [
        (("value", UNHEALTHY), "not-recovered"),
        (("result_unstructured", None), "verifier-unavailable"),
    ],
)
def test_samples_skipped_at_the_claim_are_unsuccessful_not_refused(
    client: Any,
    auth_headers: dict[str, str],
    tmp_path: Path,
    last: tuple[str, Any],
    expected: str,
) -> None:
    """@spec AUTOMATED-REMEDIATION-12 @spec AUTOMATED-REMEDIATION-18: when every
    sample is due at once the claim records all but the last ``skipped`` (an
    unsuccessful sample, never a refusal); the outcome follows the last one.
    """

    _, nomination_id, forward_id = _scenario(client, auth_headers, tmp_path)
    _shift(timedelta(seconds=DEADLINE + 1))

    claimed = _claim_read(client)
    skipped_sample = {"sample": "skipped", "value": None}
    skipped = [row for row in _samples(nomination_id) if row["sample"] == skipped_sample]
    assert len(skipped) == len(OFFSETS) - 1
    assert _outcome(forward_id, nomination_id) is None
    reported = _report(client, claimed, *last)
    assert reported.status_code == 200, reported.text

    assert _outcome(forward_id, nomination_id) == expected


def test_a_sample_whose_lease_expires_is_refused_and_ends_the_verification(
    client: Any, auth_headers: dict[str, str], tmp_path: Path
) -> None:
    """@spec AUTOMATED-REMEDIATION-12 @spec AUTOMATED-REMEDIATION-18: the claim
    route's lease expiry refuses the read ``runner_unavailable``, and the
    verification is decided then (``verifier-unavailable``), not left
    ``verifying``.
    """

    _, nomination_id, forward_id = _scenario(client, auth_headers, tmp_path)
    _take(client, nomination_id, UNHEALTHY)
    _take(client, nomination_id, UNHEALTHY)
    _make_next_due(nomination_id)
    claimed = _claim_read(client)
    sql_rows(
        "UPDATE curie.action_executions SET lease_expires_at = now() - interval '1 second' "
        "WHERE id = :id",
        {"id": uuid.UUID(claimed["id"])},
    )

    assert _claim(client).status_code == 204

    assert _execution(claimed["id"])["state"] == "refused"
    assert _outcome(forward_id, nomination_id) == "verifier-unavailable"


# --------------------------------------------------------------------------- #
# AUTOMATED-REMEDIATION-18: superseded
# --------------------------------------------------------------------------- #


def test_another_ledger_record_on_the_target_supersedes_before_verified(
    client: Any, auth_headers: dict[str, str], tmp_path: Path
) -> None:
    """@spec AUTOMATED-REMEDIATION-18: "before ``verified``, another ledger record
    on the same target key is created": the outcome is ``superseded`` even when
    the next sample would have verified, and the later record is not touched.
    """

    agent_id, nomination_id, forward_id = _scenario(client, auth_headers, tmp_path)
    _take(client, nomination_id)
    _take(client, nomination_id)
    other = _nominate(agent_id, event_id="event-0000000012", arguments={**NOMINATED, "replicas": 5})
    other_forward, _ = _dispatched(client, other)

    _take(client, nomination_id)

    assert _outcome(forward_id, nomination_id) == "superseded"
    assert _nomination(nomination_id)["state"] == "finished"
    assert _ledger(other_forward)["verification_outcome"] is None


def _model_turn_record(client: Any, headers: dict[str, str], agent_id: str, **fields: Any) -> None:
    """A ledger record a model turn writes through ``POST /actions``."""

    body: dict[str, Any] = {
        "agent_id": agent_id,
        "conversation_id": "C0EXAMPLE9",
        "call_id": f"toolu_{uuid.uuid4().hex[:8]}",
        "tool": f"mcp__{ACT_CONNECTOR}__{ACT_TOOL}",
        "arguments": {**NOMINATED, "replicas": 6},
        "dedupe_key": f"event-{uuid.uuid4()}:toolu_01",
    }
    body.update(fields)
    opened = client.post("/actions", json=body, headers=headers)
    assert opened.status_code == 201, opened.text


@pytest.mark.parametrize(
    "fields",
    [
        pytest.param({}, id="same-tool"),
        pytest.param(
            {
                "tool": f"mcp__{ACT_CONNECTOR}__restart_deployment",
                "arguments": {"namespace": "example-ns", "deployment": "example-api"},
            },
            id="other-tool-same-target",
        ),
    ],
)
def test_a_model_turn_record_on_the_target_supersedes_before_verified(
    client: Any, auth_headers: dict[str, str], tmp_path: Path, fields: dict[str, Any]
) -> None:
    """@spec AUTOMATED-REMEDIATION-18: "another ledger record on the same target
    key" is any ledger record, not only a remediation's: a model turn's call on
    the acting connector naming the same target (the value of the action's
    target argument) supersedes, even when the next sample would have verified.
    """

    agent_id, nomination_id, forward_id = _scenario(client, auth_headers, tmp_path)
    _take(client, nomination_id)
    _take(client, nomination_id)
    _model_turn_record(client, auth_headers, agent_id, **fields)

    _take(client, nomination_id)

    assert _outcome(forward_id, nomination_id) == "superseded"


def test_an_approval_gated_record_on_the_target_supersedes_before_verified(
    client: Any, auth_headers: dict[str, str], tmp_path: Path
) -> None:
    """@spec AUTOMATED-REMEDIATION-18: a record under an ``approval`` authority (no
    nomination) on the same target key supersedes too.
    """

    agent_id, nomination_id, forward_id = _scenario(client, auth_headers, tmp_path)
    _take(client, nomination_id)
    _take(client, nomination_id)
    approval_id = uuid.uuid4()
    sql_rows(
        "INSERT INTO curie.agent_actions (id, agent_id, conversation_id, call_id, tool, "
        "arguments, dedupe_key, connector, connector_digest, authority_kind, authority_ref, "
        "actor_kind, gate_approval_id) VALUES (:id, :agent_id, 'C0EXAMPLE9', 'toolu_02', "
        ":tool, CAST(:arguments AS jsonb), :key, :connector, :digest, 'approval', :ref, "
        "'approval', :approval)",
        {
            "id": uuid.uuid4(),
            "agent_id": uuid.UUID(agent_id),
            "tool": f"mcp__{ACT_CONNECTOR}__{ACT_TOOL}",
            "arguments": json.dumps({**NOMINATED, "replicas": 2}),
            "key": f"approval-check-{uuid.uuid4()}",
            "connector": ACT_CONNECTOR,
            "digest": ACT_DIGEST,
            "ref": str(approval_id),
            "approval": approval_id,
        },
    )

    _take(client, nomination_id)

    assert _outcome(forward_id, nomination_id) == "superseded"


def test_a_model_turn_record_on_another_target_does_not_supersede(
    client: Any, auth_headers: dict[str, str], tmp_path: Path
) -> None:
    """@spec AUTOMATED-REMEDIATION-18: only the same target key supersedes."""

    agent_id, nomination_id, forward_id = _scenario(client, auth_headers, tmp_path)
    _take(client, nomination_id)
    _take(client, nomination_id)
    _model_turn_record(
        client, auth_headers, agent_id, arguments={**NOMINATED, "deployment": "example-worker"}
    )

    _take(client, nomination_id)

    assert _outcome(forward_id, nomination_id) == "verified"


def test_a_record_on_another_target_does_not_supersede(
    client: Any, auth_headers: dict[str, str], tmp_path: Path
) -> None:
    """@spec AUTOMATED-REMEDIATION-18: only the same target key supersedes."""

    agent_id, nomination_id, forward_id = _scenario(client, auth_headers, tmp_path)
    _take(client, nomination_id)
    _take(client, nomination_id)
    other = _nominate(
        agent_id,
        event_id="event-0000000013",
        arguments={**NOMINATED, "deployment": "example-worker"},
    )
    _dispatched(client, other)

    _take(client, nomination_id)

    assert _outcome(forward_id, nomination_id) == "verified"


def _observe_document() -> dict[str, Any]:
    document = _document()
    document["actions"][0]["reversibility"] = "reversible"
    return document


def _observe_scenario(
    client: Any, headers: dict[str, str], tmp_path: Path
) -> tuple[uuid.UUID, uuid.UUID]:
    """A reversible remediation whose ledger row records its target and version
    (as the worker's completion does) before the forward execution is confirmed.
    """

    agent_id = _agent(client, headers, tmp_path, observe=True)
    _bind_policy(agent_id, _observe_document())
    nomination_id = _nominate(agent_id)
    forward_id, fence = _dispatched(client, nomination_id)
    sql_rows(
        "UPDATE curie.agent_actions SET post_version = :v, target = CAST(:t AS jsonb) "
        "WHERE id = :id",
        {
            "v": POST_VERSION,
            "t": json.dumps(OBSERVED_TARGET),
            "id": _execution(forward_id)["subject_action_id"],
        },
    )
    ended = _end(client, forward_id, fence)
    assert ended.status_code == 200, ended.text
    return nomination_id, forward_id


def _step(client: Any, nomination_id: uuid.UUID, value: Any, version: str) -> None:
    """One sample and its observe-only execution, due together, both ended."""

    _make_next_due(nomination_id)
    for _ in range(2):
        claimed = _claim(client)
        assert claimed.status_code == 200, claimed.text
        body = claimed.json()
        row = _execution(body["id"])
        assert row["kind"] == "read", row
        if row["tool"] == OBSERVE_TOOL:
            answered = client.post(
                f"/action-executions/{body['id']}/observation",
                json={**_fence(body), "version": version},
                headers=worker_headers(),
            )
        else:
            answered = _report(client, body, "value", value)
        assert answered.status_code == 200, answered.text


def test_an_observe_only_execution_is_scheduled_beside_each_sample(
    client: Any, auth_headers: dict[str, str], tmp_path: Path
) -> None:
    """@spec AUTOMATED-REMEDIATION-18 (executor amendment E3): for an action whose
    connector advertises ``observe_version``, a ``read``-kind execution of the
    acting connector's ``observe_version`` (no pointer), bound to the recorded
    target, is due with each sample under the same authority.
    """

    nomination_id, _ = _observe_scenario(client, auth_headers, tmp_path)

    samples = _samples(nomination_id)
    observes = _observes(nomination_id)
    assert len(samples) == len(OFFSETS)
    assert len(observes) == len(OFFSETS)
    assert [row["not_before"] for row in observes] == [row["not_before"] for row in samples]
    for row in observes:
        assert row["state"] == "requested"
        assert row["connector"] == ACT_CONNECTOR
        assert row["connector_digest"] == ACT_DIGEST
        assert row["pointer"] is None
        assert row["authority_kind"] == "policy"
        assert row["forward_arguments"] == {"target": OBSERVED_TARGET}
        assert row["arguments_sha256"] == _sha({"target": OBSERVED_TARGET})


def test_a_claimed_observe_only_execution_hands_its_holder_the_target(
    client: Any, auth_headers: dict[str, str], tmp_path: Path
) -> None:
    """@spec AUTOMATED-REMEDIATION-18 (E3): ``/arguments`` answers the holder
    ``observe_version`` with the recorded target and no pointer.
    """

    nomination_id, _ = _observe_scenario(client, auth_headers, tmp_path)
    _make_next_due(nomination_id)
    for _ in range(2):
        claimed = _claim(client)
        assert claimed.status_code == 200, claimed.text
        if _execution(claimed.json()["id"])["tool"] == OBSERVE_TOOL:
            break
        reported = _report(client, claimed.json(), "value", SAMPLED)
        assert reported.status_code == 200, reported.text
    body = claimed.json()
    assert _execution(body["id"])["tool"] == OBSERVE_TOOL

    answered = client.post(
        f"/action-executions/{body['id']}/arguments", json=_fence(body), headers=worker_headers()
    )

    assert answered.status_code == 200, answered.text
    assert answered.json() == {
        "tool": OBSERVE_TOOL,
        "arguments": {"target": OBSERVED_TARGET},
        "pointer": None,
    }


def test_an_observed_version_other_than_the_actions_supersedes(
    client: Any, auth_headers: dict[str, str], tmp_path: Path
) -> None:
    """@spec AUTOMATED-REMEDIATION-18: "reports a version different from the
    action's recorded ``post_version``" before ``verified``: ``superseded``.
    """

    nomination_id, forward_id = _observe_scenario(client, auth_headers, tmp_path)

    _step(client, nomination_id, SAMPLED, POST_VERSION)
    _step(client, nomination_id, SAMPLED, POST_VERSION)
    assert _outcome(forward_id, nomination_id) is None
    _step(client, nomination_id, SAMPLED, "rv-2002")

    assert _outcome(forward_id, nomination_id) == "superseded"


def test_an_unchanged_version_is_attribution_never_success_and_nothing_undoes(
    client: Any, auth_headers: dict[str, str], tmp_path: Path
) -> None:
    """@spec AUTOMATED-REMEDIATION-18 @spec AUTOMATED-REMEDIATION-19: the acting
    connector's observation is never read as success; a reversible action that is
    ``not-recovered`` is not restored automatically (no restore execution).
    """

    nomination_id, forward_id = _observe_scenario(client, auth_headers, tmp_path)

    for _ in OFFSETS:
        _step(client, nomination_id, UNHEALTHY, POST_VERSION)

    assert _outcome(forward_id, nomination_id) == "not-recovered"
    assert _restores() == []
    assert _ledger(forward_id)["undone_at"] is None


# --------------------------------------------------------------------------- #
# Once, no undo, no value in receipts
# --------------------------------------------------------------------------- #


def test_the_outcome_is_written_once(
    client: Any, auth_headers: dict[str, str], tmp_path: Path
) -> None:
    """@spec AUTOMATED-REMEDIATION-18: "The outcome is written on the nomination and
    the ledger record once": a replayed sample report changes nothing.
    """

    _, nomination_id, forward_id = _scenario(client, auth_headers, tmp_path)
    _take(client, nomination_id)
    _take(client, nomination_id)
    _make_next_due(nomination_id)
    claimed = _claim_read(client)
    assert _report(client, claimed, "value", SAMPLED).status_code == 200
    before = _ledger(forward_id)
    assert before["verification_outcome"] == "verified"

    replay = _report(client, claimed, "value", SAMPLED)

    assert replay.status_code == 200, replay.text
    after = _ledger(forward_id)
    assert after["verification_outcome"] == "verified"
    assert after["verified_at"] == before["verified_at"]


@pytest.mark.parametrize("ending", ["not-recovered", "verifier-unavailable"])
def test_no_outcome_triggers_an_undo(
    client: Any, auth_headers: dict[str, str], tmp_path: Path, ending: str
) -> None:
    """@spec AUTOMATED-REMEDIATION-19: escalate, never undo automatically: no
    restore execution and no undo audit row follow a failed verification.
    """

    _, nomination_id, forward_id = _scenario(client, auth_headers, tmp_path)
    if ending == "not-recovered":
        for _ in OFFSETS:
            _take(client, nomination_id, UNHEALTHY)
    else:
        _refuse_next(client, nomination_id)

    assert _outcome(forward_id, nomination_id) == ending
    assert _restores() == []
    ledger = _ledger(forward_id)
    assert ledger["undone_at"] is None
    audit = sql_dicts(
        "SELECT action FROM curie.action_audit_entries WHERE action_id = :id",
        {"id": ledger["id"]},
    )
    assert [row["action"] for row in audit] == ["authorized"]


def test_no_receipt_or_log_carries_a_sampled_value(
    client: Any,
    auth_headers: dict[str, str],
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """@spec AUTOMATED-REMEDIATION-12 @spec AUTOMATED-REMEDIATION-20
    @spec AUTOMATED-REMEDIATION-21: the samples receipt, the execution receipt,
    the ledger record, its audit and the logs of a whole verification never carry
    a sampled value.
    """

    caplog.set_level(logging.DEBUG)
    _, nomination_id, forward_id = _scenario(client, auth_headers, tmp_path)
    bodies = [_take(client, nomination_id).text for _ in range(3)]
    assert _outcome(forward_id, nomination_id) == "verified"
    action_id = _execution(forward_id)["subject_action_id"]
    for row in _samples(nomination_id):
        bodies.append(client.get(f"/action-executions/{row['id']}", headers=auth_headers).text)
    record = client.get(f"/actions/{action_id}", headers=auth_headers)
    assert record.status_code == 200, record.text
    assert record.json()["verification_outcome"] == "verified"
    assert record.json()["verified_at"] is not None
    bodies.append(record.text)
    bodies.append(client.get(f"/actions/{action_id}/audit", headers=auth_headers).text)

    for body in bodies:
        assert SAMPLED_TEXT not in body
    assert SAMPLED_TEXT not in caplog.text


# --------------------------------------------------------------------------- #
# AUTOMATED-REMEDIATION-17: independence at write and at admission
# --------------------------------------------------------------------------- #


def _operator() -> dict[str, str]:
    token = approval_principal.mint(
        get_settings().api_key,
        subject=OPERATOR,
        kind="operator",
        scope=approval_principal.APPROVE_SCOPE,
        exp=int(time.time()) + 300,
    )
    return {PRINCIPAL_HEADER: token}


def _put_policy(
    client: Any, headers: dict[str, str], agent_id: str, document: dict[str, Any]
) -> Any:
    # Bound with ``automatic`` false: an automatic action needs a qualification
    # record at write (AUTOMATED-REMEDIATION-23), and independence is checked
    # for every action, automatic or not.
    document = copy.deepcopy(document)
    for action in document["actions"]:
        action["automatic"] = False
    return client.put(
        f"/agents/{agent_id}/hooks/{HOOK}/remediation-policy",
        json={
            "expected_generation": "0",
            "operation_id": str(uuid.uuid4()),
            "policy": document,
        },
        headers={**headers, **_operator()},
    )


def _generations(agent_id: str) -> list[dict[str, Any]]:
    return sql_dicts(
        "SELECT generation FROM curie.remediation_policy_generations WHERE agent_id = :a",
        {"a": uuid.UUID(agent_id)},
    )


def _refusal_code(response: Any) -> Any:
    detail = response.json().get("detail")
    assert isinstance(detail, dict), response.text
    return detail.get("code")


_SHARED = "    secrets:\n      - SHARED_TOKEN\n"


@pytest.mark.parametrize(
    ("connectors", "verifier"),
    [
        pytest.param(None, {"connector": ACT_CONNECTOR}, id="acting-connector"),
        pytest.param(
            _connectors(act_secrets=_SHARED, read_secrets=_SHARED), {}, id="shared-secret-name"
        ),
    ],
)
def test_a_verifier_that_is_not_independent_is_refused_at_write(
    client: Any,
    auth_headers: dict[str, str],
    tmp_path: Path,
    connectors: str | None,
    verifier: dict[str, Any],
) -> None:
    """@spec AUTOMATED-REMEDIATION-17: a verifier naming the acting connector, or a
    connector whose MCP headers expand a secret name the acting connector's also
    expand (in the in-force version), is refused ``verifier_not_independent`` and
    creates no generation.
    """

    agent_id = _agent(client, auth_headers, tmp_path, connectors=connectors)

    response = _put_policy(client, auth_headers, agent_id, _document(**verifier))

    assert response.status_code == 422, response.text
    assert _refusal_code(response) == "verifier_not_independent"
    assert _generations(agent_id) == []


def test_a_verifier_with_its_own_connector_and_credential_is_accepted(
    client: Any, auth_headers: dict[str, str], tmp_path: Path
) -> None:
    """@spec AUTOMATED-REMEDIATION-17: different connector, disjoint secret names."""

    connectors = _connectors(
        act_secrets="    secrets:\n      - K8S_TOKEN\n",
        read_secrets="    secrets:\n      - METRICS_TOKEN\n",
    )
    agent_id = _agent(client, auth_headers, tmp_path, connectors=connectors)

    response = _put_policy(client, auth_headers, agent_id, _document())

    assert response.status_code == 200, response.text
    assert len(_generations(agent_id)) == 1


def _independence(agent_id: str) -> Any:
    verifier = importlib.import_module("curie_api.remediation_verifier")
    return _session_call(
        lambda session: verifier.independence_refusal(
            session, uuid.UUID(agent_id), copy.deepcopy(ACTION)
        )
    )


def test_a_bundle_version_that_introduces_a_shared_credential_fails_admission_independence(
    client: Any, auth_headers: dict[str, str], tmp_path: Path
) -> None:
    """@spec AUTOMATED-REMEDIATION-17 @spec AUTOMATED-REMEDIATION-8 (check 7): the
    check is repeated against the in-force version, so a later version whose
    connectors share a credential name refuses ``verifier_not_independent``.
    """

    agent_id = _agent(client, auth_headers, tmp_path)
    assert _independence(agent_id) is None

    _deploy(
        client,
        auth_headers,
        tmp_path,
        agent_id,
        _connectors(act_secrets=_SHARED, read_secrets=_SHARED),
    )

    assert _independence(agent_id) == "verifier_not_independent"
