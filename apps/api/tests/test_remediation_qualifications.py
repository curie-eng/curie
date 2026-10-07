"""Qualification records and the qualification verifier run (plan task 15).

@spec AUTOMATED-REMEDIATION-22 @spec AUTOMATED-REMEDIATION-23

docs/superpowers/specs/2026-10-07-automated-remediation.md, with the 2026-10-07
principal ruling (a qualification write and a verifier run need an ADR 0106
operator principal, because a record enables automatic execution):

* AUTOMATED-REMEDIATION-22: a ``remediation_qualifications`` row records, for
  one ``(agent_id, connector, tool, connector_digest, verifier declaration
  digest)``, the evidence ruling 5 requires, as references to rows in this
  installation that the API checks by state and digest when the record is
  written. ``PUT /agents/{agent_id}/remediation-qualifications/{id}`` writes it;
  ``POST /agents/{agent_id}/remediation-qualifications/{id}/verifier-runs``
  starts a verifier evaluation that can only create ``read`` executions of the
  declared verifier with ``authority_kind`` ``qualification``, against a target
  that is a literal member of the action's allowed list, and accepts no tool or
  argument fields. No route lets an administrator request a forward write. A
  record is valid only for its digest: a connector upgrade makes admission
  check 6 answer ``qualification_stale``.
* AUTOMATED-REMEDIATION-23: a policy write setting ``automatic`` true for an
  action without a valid record is refused ``qualification_required`` (task 3's
  write path gains the check); with a valid record it is accepted.

Surface these tests fix (recorded in
``.projects/plans/task-remediation-qualification.tests.md``):

* ``PUT /agents/{agent_id}/remediation-qualifications/{qualification_id}``,
  ``require_api_key`` plus ``X-Curie-Approval-Principal`` (operator), body
  ``{"hook", "action", "generation", "evidence", "worst_case"}``: the action is
  read from that policy generation of the hook, which fixes the connector, tool,
  reversibility, verifier declaration and the bounds the record was evaluated
  against; the connector digest is the acting connector's digest in the
  agent's in-force version at write. ``evidence`` holds
  ``restore_execution_id`` and ``conflict_execution_id`` (reversible),
  ``forward_execution_ids`` (two, idempotent), and ``verified_run_id`` and
  ``not_recovered_run_id`` (every action). 200 with
  ``{"id", "agent_id", "connector", "tool", "connector_digest",
  "verifier_sha256", "reversibility", "recorded_by", ...}``; a replay of the same
  body answers the record again, another body under the same id is ``409``
  ``qualification_conflict``. Evidence refusals are ``422``
  ``{"detail": {"code"}}`` with ``evidence_incomplete``, ``evidence_not_found``,
  ``evidence_wrong_state``, ``evidence_other_digest``, ``evidence_other_action``
  or ``evidence_not_idempotent``; a worst case statement over 2000 characters is
  ``422`` ``qualification_document_invalid``. Every refusal writes no row.
* ``POST /agents/{agent_id}/remediation-qualifications/{qualification_id}/verifier-runs``,
  same credentials, body exactly ``{"hook", "action", "target"}`` (any other
  field, ``tool`` and ``arguments`` included, is ``422`` and creates nothing),
  ``201`` with ``{"id", "qualification_id", "hook", "action", "target",
  "generation", "started_by", "outcome"}``; a target outside the action's
  ``target.allowed`` is ``422`` ``target_not_allowed``; an action the current
  generation does not declare is ``422`` ``unknown_action``. It schedules the
  declared verifier's samples exactly as AUTOMATED-REMEDIATION-18 does for a
  forward execution, anchored on the run's ``started_at`` instead of
  ``dispatched_at``; each sample is a ``read`` execution with
  ``authority_kind`` ``qualification`` and ``authority_ref`` the run id. The
  run's outcome (``verified``, ``not-recovered``, ``verifier-unavailable``) is
  read with ``GET .../verifier-runs/{run_id}``.
* ``curie_api.remediation_qualifications.qualification_refusal(session,
  agent_id, action, *, store=None)`` -> ``None``, ``"qualification_missing"`` or
  ``"qualification_stale"``: the record lookup admission check 6 (plan task 9,
  ``remediation_admission.qualification_refusal``) delegates to.
* the policy ``PUT`` refusing ``qualification_required`` (``422``, path
  ``/actions/{i}/qualification``) and creating no generation.

The evidence rows (ledger records, restore and forward executions, the conflict
audit row) are seeded as rows, because their producers (the undo ruling, the
approval resolve path and the worker) are tested in their own suites; the
verifier runs are produced through the real route and the worker's routes.

Every identifier is a placeholder.
"""

from __future__ import annotations

import asyncio
import copy
import importlib
import io
import json
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
from curie_api.action_forward import arguments_sha256
from curie_api.config import get_settings
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

pytestmark = pytest.mark.usefixtures("clean_db", "executor_enabled")

HOOK = "alerts"
ACTION_NAME = "scale-out-api"

ACT_CONNECTOR = "k8s"
ACT_TOOL = "scale_deployment"
ACT_DIGEST = "sha256:" + "ab" * 32
UPGRADED_DIGEST = "sha256:" + "ac" * 32
OTHER_DIGEST = "sha256:" + "aa" * 32
READ_CONNECTOR = "example-metrics"
READ_TOOL = "query_value"
READ_DIGEST = "sha256:" + "ef" * 32

USERS_ROUTE = "sre-oncall"
CHANNEL = "C0EXAMPLE7"
OPERATOR = "U0EXAMPLE7"
PRINCIPAL_HEADER = "X-Curie-Approval-Principal"

TARGET = "example-api"
FORWARD_ARGUMENTS: dict[str, Any] = {
    "namespace": "example-ns",
    "deployment": TARGET,
    "replicas": 4,
}
HEALTHY = 0.0137
UNHEALTHY = 0.9
WORST_CASE = (
    "Worst case: the deployment runs at 6 replicas until an operator scales it "
    "back; the metrics read tool is read only by annotation, and the two "
    "connectors use distinct credentials (checked by hand, not by name)."
)

SETTLE = 60
INTERVAL = 30
DEADLINE = 150
SAMPLES = DEADLINE // INTERVAL

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
    "reversibility": "reversible",
    "precondition": PRECONDITION,
    "verifier": VERIFIER,
    "automatic": False,
    "qualification": None,
}
LIMITS: dict[str, Any] = {"per_policy_per_hour": 3, "per_incident_per_target": 1}


def _document(**action: Any) -> dict[str, Any]:
    declared = copy.deepcopy(ACTION)
    declared.update(action)
    return {"route": USERS_ROUTE, "limits": dict(LIMITS), "actions": [declared]}


# --------------------------------------------------------------------------- #
# An agent whose in-force version declares the acting and the read connector
# --------------------------------------------------------------------------- #


def _connectors(act_digest: str = ACT_DIGEST) -> str:
    return (
        "connectors:\n"
        f"  {ACT_CONNECTOR}:\n    image: ghcr.io/example/k8s-restorer@{act_digest}\n"
        "    secrets:\n      - name: SNAPSHOT_SEALING_KEY\n        from_secret: k8s-restorer-seal\n"
        f"  {READ_CONNECTOR}:\n    image: ghcr.io/example/metrics-mcp@{READ_DIGEST}\n"
        "    secrets:\n      - METRICS_TOKEN\n"
    )


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
    client: Any, headers: dict[str, str], tmp_path: Path, agent_id: str, act_digest: str
) -> None:
    """A new version whose acting connector runs at ``act_digest``, deployed."""

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
        files={"file": ("bundle.tar.gz", _archive(tmp_path, name, _connectors(act_digest)))},
        headers=headers,
    )
    assert upload.status_code == 201, upload.text
    deployment = client.post(
        "/deployments",
        json={"agent_id": agent_id, "version_id": version_id, "environment": "dev"},
        headers=headers,
    )
    assert deployment.status_code == 201, deployment.text


def _agent(client: Any, headers: dict[str, str], tmp_path: Path) -> str:
    """An agent with an explicit approval route, a protected hook and both connectors."""

    created = client.post(
        "/agents",
        json={
            "name": f"qual-bot-{uuid.uuid4().hex[:6]}",
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
    _deploy(client, headers, tmp_path, agent_id, ACT_DIGEST)
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
    return agent_id


def _operator() -> dict[str, str]:
    token = approval_principal.mint(
        get_settings().api_key,
        subject=OPERATOR,
        kind="operator",
        scope=approval_principal.APPROVE_SCOPE,
        exp=int(time.time()) + 300,
    )
    return {PRINCIPAL_HEADER: token}


def _admin(headers: dict[str, str]) -> dict[str, str]:
    return {**headers, **_operator()}


def _current_generation(agent_id: str) -> str:
    rows = sql_dicts(
        "SELECT generation FROM curie.remediation_policies WHERE agent_id = :a AND hook = :h",
        {"a": uuid.UUID(agent_id), "h": HOOK},
    )
    return str(rows[0]["generation"]) if rows else "0"


def _put_policy(
    client: Any, headers: dict[str, str], agent_id: str, document: dict[str, Any]
) -> Any:
    return client.put(
        f"/agents/{agent_id}/hooks/{HOOK}/remediation-policy",
        json={
            "expected_generation": _current_generation(agent_id),
            "operation_id": str(uuid.uuid4()),
            "policy": document,
        },
        headers=_admin(headers),
    )


def _bind(client: Any, headers: dict[str, str], agent_id: str, document: dict[str, Any]) -> str:
    response = _put_policy(client, headers, agent_id, document)
    assert response.status_code == 200, response.text
    return str(response.json()["generation"])


def _generations(agent_id: str) -> list[int]:
    return [
        int(row["generation"])
        for row in sql_dicts(
            "SELECT generation FROM curie.remediation_policy_generations "
            "WHERE agent_id = :a ORDER BY generation",
            {"a": uuid.UUID(agent_id)},
        )
    ]


def _refusal_code(response: Any) -> Any:
    detail = response.json().get("detail")
    assert isinstance(detail, dict), response.text
    return detail.get("code")


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


def _lookup(agent_id: str, action: dict[str, Any]) -> Any:
    """Admission check 6's record lookup, as plan task 9 calls it."""

    module = importlib.import_module("curie_api.remediation_qualifications")
    return _session_call(
        lambda session: module.qualification_refusal(
            session, uuid.UUID(agent_id), copy.deepcopy(action)
        )
    )


# --------------------------------------------------------------------------- #
# Evidence rows: ledger records, restores, forwards
# --------------------------------------------------------------------------- #


def _record(
    agent_id: str,
    *,
    tool: str = ACT_TOOL,
    digest: str = ACT_DIGEST,
    post_version: str = "rv-2001",
    arguments: dict[str, Any] | None = None,
    authority_kind: str = "approval",
) -> uuid.UUID:
    """One succeeded ledger record of the acting connector, as a forward leaves it."""

    action_id = uuid.uuid4()
    sql_rows(
        "INSERT INTO curie.agent_actions "
        "(id, agent_id, conversation_id, call_id, tool, arguments, target, status, "
        "dedupe_key, post_version, connector, connector_digest, authority_kind, "
        "authority_ref, completed_at) "
        "VALUES (:id, :agent_id, 'thread-1', :call_id, :tool, CAST(:arguments AS jsonb), "
        "CAST(:target AS jsonb), 'succeeded', :dedupe, :post_version, :connector, :digest, "
        ":authority_kind, :authority_ref, now())",
        {
            "id": action_id,
            "agent_id": uuid.UUID(agent_id),
            "call_id": f"call-{action_id.hex[:8]}",
            "tool": tool,
            "arguments": json.dumps(arguments or FORWARD_ARGUMENTS),
            "target": json.dumps({"kind": "Deployment", "namespace": "example-ns", "name": TARGET}),
            "dedupe": f"drill:{action_id}",
            "post_version": post_version,
            "connector": ACT_CONNECTOR,
            "digest": digest,
            "authority_kind": authority_kind,
            "authority_ref": str(uuid.uuid4()),
        },
    )
    return action_id


def _execution(
    agent_id: str,
    *,
    kind: str,
    subject: uuid.UUID,
    state: str,
    digest: str = ACT_DIGEST,
    tool: str = "restore",
    refusal_code: str | None = None,
    outcome: dict[str, Any] | None = None,
    arguments: dict[str, Any] | None = None,
    authority_kind: str = "undo_ruling",
) -> uuid.UUID:
    execution_id = uuid.uuid4()
    sql_rows(
        "INSERT INTO curie.action_executions "
        "(id, kind, agent_id, connector, tool, subject_action_id, arguments_sha256, "
        "forward_arguments, connector_digest, authority_kind, authority_ref, requested_by, "
        "idempotency_key, state, refusal_code, outcome, finished_at) "
        "VALUES (:id, :kind, :agent_id, :connector, :tool, :subject, :sha, "
        "CAST(:arguments AS jsonb), :digest, :authority_kind, :authority_ref, :requested_by, "
        ":key, :state, :refusal_code, CAST(:outcome AS jsonb), now())",
        {
            "id": execution_id,
            "kind": kind,
            "agent_id": uuid.UUID(agent_id),
            "connector": ACT_CONNECTOR,
            "tool": tool,
            "subject": subject,
            "sha": arguments_sha256(arguments) if arguments is not None else None,
            "arguments": json.dumps(arguments) if arguments is not None else None,
            "digest": digest,
            "authority_kind": authority_kind,
            "authority_ref": str(uuid.uuid4()),
            "requested_by": OPERATOR,
            "key": f"drill:{execution_id}",
            "state": state,
            "refusal_code": refusal_code,
            "outcome": json.dumps(outcome) if outcome is not None else None,
        },
    )
    return execution_id


def _conflict_audit(action_id: uuid.UUID, versions: dict[str, Any]) -> None:
    """The ``refused_conflict`` audit row naming both versions (AE-15)."""

    sql_rows(
        "INSERT INTO curie.action_audit_entries "
        "(id, action_id, action, actor, authorizer, authorized, reason, evidence) "
        "VALUES (:id, :action_id, 'refused_conflict', :actor, 'executor', false, "
        "'the target changed after this action; refusing to restore over it', "
        "CAST(:evidence AS jsonb))",
        {
            "id": uuid.uuid4(),
            "action_id": action_id,
            "actor": OPERATOR,
            "evidence": json.dumps({"state": "refused", "code": "version_conflict", **versions}),
        },
    )


def _confirmed_restore(
    agent_id: str, *, tool: str = ACT_TOOL, digest: str = ACT_DIGEST, state: str = "confirmed"
) -> uuid.UUID:
    """A restore of a record this tool produced at ``digest``, ended in ``state``."""

    record = _record(agent_id, tool=tool, digest=digest)
    return _execution(agent_id, kind="restore", subject=record, state=state, digest=digest)


def _conflict_restore(
    agent_id: str,
    *,
    digest: str = ACT_DIGEST,
    code: str = "version_conflict",
    audit: bool = True,
) -> uuid.UUID:
    """A restore refused because the target moved, with its audit row naming both versions."""

    record = _record(agent_id, digest=digest, post_version="rv-2002")
    versions = {"recorded_version": "rv-2002", "observed_version": "rv-2077"}
    execution_id = _execution(
        agent_id,
        kind="restore",
        subject=record,
        state="refused",
        digest=digest,
        refusal_code=code,
        outcome=versions,
    )
    if audit:
        _conflict_audit(record, versions)
    return execution_id


def _forward(
    agent_id: str,
    *,
    arguments: dict[str, Any] | None = None,
    post_version: str = "rv-3001",
    state: str = "confirmed",
    digest: str = ACT_DIGEST,
) -> uuid.UUID:
    """A forward execution approved through an ordinary remediation approval."""

    bound = arguments or FORWARD_ARGUMENTS
    record = _record(agent_id, digest=digest, post_version=post_version, arguments=bound)
    return _execution(
        agent_id,
        kind="forward",
        subject=record,
        state=state,
        digest=digest,
        tool=ACT_TOOL,
        arguments=bound,
        authority_kind="approval",
    )


# --------------------------------------------------------------------------- #
# Verifier runs, through the real route and the worker's routes
# --------------------------------------------------------------------------- #


def _runs_url(agent_id: str, qualification_id: str) -> str:
    return f"/agents/{agent_id}/remediation-qualifications/{qualification_id}/verifier-runs"


def _start_run(
    client: Any,
    headers: dict[str, str],
    agent_id: str,
    qualification_id: str,
    body: dict[str, Any] | None = None,
) -> Any:
    return client.post(
        _runs_url(agent_id, qualification_id),
        json=body if body is not None else {"hook": HOOK, "action": ACTION_NAME, "target": TARGET},
        headers=_admin(headers),
    )


def _run_samples(run_id: str) -> list[dict[str, Any]]:
    return sql_dicts(
        "SELECT * FROM curie.action_executions WHERE kind = 'read' AND authority_ref = :ref "
        "ORDER BY not_before, created_at",
        {"ref": run_id},
    )


def _executions() -> list[dict[str, Any]]:
    return sql_dicts("SELECT * FROM curie.action_executions")


def _db_now() -> datetime:
    now: datetime = sql_dicts("SELECT now() AS now")[0]["now"]
    return now


def _shift(delta: timedelta) -> None:
    """Move every schedule ``delta`` into the past, run starts included."""

    seconds = delta.total_seconds()
    sql_rows(
        "UPDATE curie.action_executions SET not_before = not_before - make_interval(secs => :s)",
        {"s": seconds},
    )
    sql_rows(
        "UPDATE curie.remediation_qualification_verifier_runs "
        "SET started_at = started_at - make_interval(secs => :s)",
        {"s": seconds},
    )


def _claim(client: Any) -> Any:
    return client.post(
        "/action-executions/claim",
        json={"lease_owner": "worker-a", "lease_seconds": 60},
        headers=worker_headers(),
    )


def _take(client: Any, run_id: str, value: Any) -> None:
    """The run's next sample: made due, claimed, reported."""

    pending = [row for row in _run_samples(run_id) if row["state"] == "requested"]
    assert pending, "no sample of this run is still scheduled"
    earliest = min(row["not_before"] for row in pending)
    _shift(earliest - _db_now() + timedelta(seconds=1))
    claimed = _claim(client)
    assert claimed.status_code == 200, claimed.text
    body = claimed.json()
    assert str(body["id"]) in {str(row["id"]) for row in pending}, body
    reported = client.post(
        f"/action-executions/{body['id']}/samples",
        json={
            "lease_owner": body["lease_owner"],
            "attempt": body["attempt"],
            "sample": "value",
            "value": value,
        },
        headers=worker_headers(),
    )
    assert reported.status_code == 200, reported.text


def _run_outcome(
    client: Any, headers: dict[str, str], agent_id: str, qualification_id: str, run_id: str
) -> Any:
    response = client.get(f"{_runs_url(agent_id, qualification_id)}/{run_id}", headers=headers)
    assert response.status_code == 200, response.text
    return response.json()["outcome"]


def _verifier_run(
    client: Any,
    headers: dict[str, str],
    agent_id: str,
    qualification_id: str,
    *,
    healthy: bool,
) -> str:
    """One verifier run ended ``verified`` (healthy) or ``not-recovered``. Returns its id."""

    started = _start_run(client, headers, agent_id, qualification_id)
    assert started.status_code == 201, started.text
    run_id = str(started.json()["id"])
    if healthy:
        # Before settle (recorded, never counted), then two consecutive at or after.
        for value in (UNHEALTHY, HEALTHY, HEALTHY):
            _take(client, run_id, value)
        expected = "verified"
    else:
        for _ in range(SAMPLES):
            _take(client, run_id, UNHEALTHY)
        expected = "not-recovered"
    assert _run_outcome(client, headers, agent_id, qualification_id, run_id) == expected
    return run_id


# --------------------------------------------------------------------------- #
# The record route
# --------------------------------------------------------------------------- #


def _record_url(agent_id: str, qualification_id: str) -> str:
    return f"/agents/{agent_id}/remediation-qualifications/{qualification_id}"


def _put_record(
    client: Any,
    headers: dict[str, str],
    agent_id: str,
    qualification_id: str,
    body: dict[str, Any],
    *,
    principal: bool = True,
) -> Any:
    return client.put(
        _record_url(agent_id, qualification_id),
        json=body,
        headers=_admin(headers) if principal else headers,
    )


def _records(agent_id: str | None = None) -> list[dict[str, Any]]:
    if agent_id is None:
        return sql_dicts("SELECT * FROM curie.remediation_qualifications")
    return sql_dicts(
        "SELECT * FROM curie.remediation_qualifications WHERE agent_id = :a",
        {"a": uuid.UUID(agent_id)},
    )


class Drill:
    """An agent whose action was drilled: a bound policy, its evidence and a record body."""

    def __init__(
        self,
        client: Any,
        headers: dict[str, str],
        tmp_path: Path,
        *,
        reversibility: str = "reversible",
    ) -> None:
        self.client = client
        self.headers = headers
        self.tmp_path = tmp_path
        self.agent_id = _agent(client, headers, tmp_path)
        self.generation = _bind(
            client, headers, self.agent_id, _document(reversibility=reversibility)
        )
        self.qualification_id = str(uuid.uuid4())
        evidence: dict[str, Any] = {}
        if reversibility == "reversible":
            evidence["restore_execution_id"] = str(_confirmed_restore(self.agent_id))
            evidence["conflict_execution_id"] = str(_conflict_restore(self.agent_id))
        else:
            evidence["forward_execution_ids"] = [
                str(_forward(self.agent_id)),
                str(_forward(self.agent_id)),
            ]
        evidence["not_recovered_run_id"] = _verifier_run(
            client, headers, self.agent_id, self.qualification_id, healthy=False
        )
        evidence["verified_run_id"] = _verifier_run(
            client, headers, self.agent_id, self.qualification_id, healthy=True
        )
        self.body: dict[str, Any] = {
            "hook": HOOK,
            "action": ACTION_NAME,
            "generation": self.generation,
            "evidence": evidence,
            "worst_case": WORST_CASE,
        }
        self.reversibility = reversibility

    def put(self, body: dict[str, Any] | None = None, *, principal: bool = True) -> Any:
        return _put_record(
            self.client,
            self.headers,
            self.agent_id,
            self.qualification_id,
            self.body if body is None else body,
            principal=principal,
        )

    def record(self) -> dict[str, Any]:
        response = self.put()
        assert response.status_code == 200, response.text
        return dict(response.json())

    def automatic(self, **action: Any) -> dict[str, Any]:
        fields: dict[str, Any] = {
            "reversibility": self.reversibility,
            "automatic": True,
            "qualification": self.qualification_id,
        }
        fields.update(action)
        return _document(**fields)


@pytest.fixture
def drill(client: Any, auth_headers: dict[str, str], tmp_path: Path) -> Drill:
    return Drill(client, auth_headers, tmp_path)


@pytest.fixture
def idempotent_drill(client: Any, auth_headers: dict[str, str], tmp_path: Path) -> Drill:
    return Drill(client, auth_headers, tmp_path, reversibility="idempotent")


# --------------------------------------------------------------------------- #
# AUTOMATED-REMEDIATION-22: a complete record
# --------------------------------------------------------------------------- #


def test_a_complete_reversible_record_is_written_with_its_digest_and_actor(
    drill: Drill,
) -> None:
    """@spec AUTOMATED-REMEDIATION-22: a confirmed restore and a version-conflict
    refusal of records this tool produced at this digest, a ``verified`` and a
    ``not-recovered`` verifier run under the same declaration, and a worst case
    statement make a record for ``(agent, connector, tool, connector digest,
    verifier declaration digest)``, recorded by the operator principal.
    """

    record = drill.record()

    assert record["id"] == drill.qualification_id
    assert record["agent_id"] == drill.agent_id
    assert (record["connector"], record["tool"]) == (ACT_CONNECTOR, ACT_TOOL)
    assert record["connector_digest"] == ACT_DIGEST
    assert record["reversibility"] == "reversible"
    assert record["recorded_by"] == OPERATOR
    assert isinstance(record["verifier_sha256"], str) and len(record["verifier_sha256"]) == 64
    rows = _records(drill.agent_id)
    assert len(rows) == 1
    assert str(rows[0]["id"]) == drill.qualification_id


def test_a_complete_idempotent_record_is_written(idempotent_drill: Drill) -> None:
    """@spec AUTOMATED-REMEDIATION-22: two confirmed forwards with the same
    canonical arguments whose second left the same ``post_version``.
    """

    record = idempotent_drill.record()

    assert record["reversibility"] == "idempotent"
    assert record["connector_digest"] == ACT_DIGEST
    assert len(_records(idempotent_drill.agent_id)) == 1


def test_a_replayed_record_write_answers_the_record_and_another_body_conflicts(
    drill: Drill,
) -> None:
    """@spec AUTOMATED-REMEDIATION-22: a record is written once per id; a replay of
    the same body answers it again, another body under the same id is ``409``
    ``qualification_conflict`` and changes nothing.
    """

    first = drill.record()
    replay = drill.put()
    assert replay.status_code == 200, replay.text
    assert replay.json() == first

    changed = copy.deepcopy(drill.body)
    changed["worst_case"] = WORST_CASE + " Revised."
    conflict = drill.put(changed)

    assert conflict.status_code == 409, conflict.text
    assert _refusal_code(conflict) == "qualification_conflict"
    rows = _records(drill.agent_id)
    assert len(rows) == 1


# --------------------------------------------------------------------------- #
# AUTOMATED-REMEDIATION-22: evidence references checked by state and digest
# --------------------------------------------------------------------------- #


def _drop(key: str) -> Any:
    def mutate(drill: Drill, evidence: dict[str, Any]) -> None:
        evidence.pop(key)

    return mutate


def _set(key: str, make: Any) -> Any:
    def mutate(drill: Drill, evidence: dict[str, Any]) -> None:
        evidence[key] = str(make(drill))

    return mutate


def _another_agents_restore(drill: Drill) -> uuid.UUID:
    other = _agent(drill.client, drill.headers, drill.tmp_path)
    return _confirmed_restore(other)


def _a_forward_as_restore(drill: Drill) -> uuid.UUID:
    return _forward(drill.agent_id)


def _swap_runs(drill: Drill, evidence: dict[str, Any]) -> None:
    evidence["verified_run_id"], evidence["not_recovered_run_id"] = (
        evidence["not_recovered_run_id"],
        evidence["verified_run_id"],
    )


REVERSIBLE_REFUSALS = [
    pytest.param(_drop("restore_execution_id"), "evidence_incomplete", id="no-restore"),
    pytest.param(_drop("conflict_execution_id"), "evidence_incomplete", id="no-conflict"),
    pytest.param(_drop("verified_run_id"), "evidence_incomplete", id="no-verified-run"),
    pytest.param(_drop("not_recovered_run_id"), "evidence_incomplete", id="no-failing-run"),
    pytest.param(
        _set("restore_execution_id", lambda d: uuid.uuid4()),
        "evidence_not_found",
        id="restore-missing",
    ),
    pytest.param(
        _set("restore_execution_id", _another_agents_restore),
        "evidence_not_found",
        id="restore-of-another-agent",
    ),
    pytest.param(
        _set("verified_run_id", lambda d: uuid.uuid4()),
        "evidence_not_found",
        id="verified-run-missing",
    ),
    pytest.param(
        _set("restore_execution_id", lambda d: _confirmed_restore(d.agent_id, state="failed")),
        "evidence_wrong_state",
        id="restore-not-confirmed",
    ),
    pytest.param(
        _set("restore_execution_id", _a_forward_as_restore),
        "evidence_wrong_state",
        id="restore-is-a-forward",
    ),
    pytest.param(
        _set(
            "conflict_execution_id",
            lambda d: _conflict_restore(d.agent_id, code="sandbox_unavailable"),
        ),
        "evidence_wrong_state",
        id="conflict-refused-for-another-reason",
    ),
    pytest.param(
        _set("conflict_execution_id", lambda d: _conflict_restore(d.agent_id, audit=False)),
        "evidence_wrong_state",
        id="conflict-without-its-audit-row",
    ),
    pytest.param(
        _set("conflict_execution_id", lambda d: _confirmed_restore(d.agent_id)),
        "evidence_wrong_state",
        id="conflict-is-a-confirmed-restore",
    ),
    pytest.param(_swap_runs, "evidence_wrong_state", id="runs-swapped"),
    pytest.param(
        _set("restore_execution_id", lambda d: _confirmed_restore(d.agent_id, digest=OTHER_DIGEST)),
        "evidence_other_digest",
        id="restore-at-another-digest",
    ),
    pytest.param(
        _set("conflict_execution_id", lambda d: _conflict_restore(d.agent_id, digest=OTHER_DIGEST)),
        "evidence_other_digest",
        id="conflict-at-another-digest",
    ),
    pytest.param(
        _set("restore_execution_id", lambda d: _confirmed_restore(d.agent_id, tool="patch_hpa")),
        "evidence_other_action",
        id="restore-of-another-tools-record",
    ),
]


@pytest.mark.parametrize(("mutate", "code"), REVERSIBLE_REFUSALS)
def test_a_reversible_record_with_bad_evidence_is_refused_with_a_named_code(
    drill: Drill, mutate: Any, code: str
) -> None:
    """@spec AUTOMATED-REMEDIATION-22 (acceptance): "writing a record with a
    missing, wrong-state or other-digest evidence reference is refused with a
    named code". Nothing is written.
    """

    body = copy.deepcopy(drill.body)
    mutate(drill, body["evidence"])

    response = drill.put(body)

    assert response.status_code == 422, response.text
    assert _refusal_code(response) == code
    assert _records() == []


IDEMPOTENT_REFUSALS = [
    pytest.param(
        lambda d, e: e.update(forward_execution_ids=e["forward_execution_ids"][:1]),
        "evidence_incomplete",
        id="one-forward",
    ),
    pytest.param(
        lambda d, e: e.update(
            forward_execution_ids=[e["forward_execution_ids"][0], str(uuid.uuid4())]
        ),
        "evidence_not_found",
        id="forward-missing",
    ),
    pytest.param(
        lambda d, e: e.update(
            forward_execution_ids=[
                e["forward_execution_ids"][0],
                str(_forward(d.agent_id, state="failed")),
            ]
        ),
        "evidence_wrong_state",
        id="forward-not-confirmed",
    ),
    pytest.param(
        lambda d, e: e.update(
            forward_execution_ids=[
                e["forward_execution_ids"][0],
                str(_forward(d.agent_id, digest=OTHER_DIGEST)),
            ]
        ),
        "evidence_other_digest",
        id="forward-at-another-digest",
    ),
    pytest.param(
        lambda d, e: e.update(
            forward_execution_ids=[
                e["forward_execution_ids"][0],
                str(_forward(d.agent_id, post_version="rv-3002")),
            ]
        ),
        "evidence_not_idempotent",
        id="repeat-left-another-version",
    ),
    pytest.param(
        lambda d, e: e.update(
            forward_execution_ids=[
                e["forward_execution_ids"][0],
                str(_forward(d.agent_id, arguments={**FORWARD_ARGUMENTS, "replicas": 5})),
            ]
        ),
        "evidence_not_idempotent",
        id="repeat-with-other-arguments",
    ),
    pytest.param(
        lambda d, e: e.update(
            forward_execution_ids=[e["forward_execution_ids"][0]] * 2,
        ),
        "evidence_not_idempotent",
        id="the-same-forward-twice",
    ),
]


@pytest.mark.parametrize(("mutate", "code"), IDEMPOTENT_REFUSALS)
def test_an_idempotent_record_with_bad_evidence_is_refused_with_a_named_code(
    idempotent_drill: Drill, mutate: Any, code: str
) -> None:
    """@spec AUTOMATED-REMEDIATION-22: the repeat must be a second confirmed forward
    with the same canonical arguments that left the same ``post_version``.
    """

    body = copy.deepcopy(idempotent_drill.body)
    mutate(idempotent_drill, body["evidence"])

    response = idempotent_drill.put(body)

    assert response.status_code == 422, response.text
    assert _refusal_code(response) == code
    assert _records() == []


def test_a_verifier_run_under_another_declaration_is_not_evidence(drill: Drill) -> None:
    """@spec AUTOMATED-REMEDIATION-22: "one verifier evaluation ending ``verified``
    and one ending ``not-recovered`` under the same verifier declaration". A run
    started under a generation whose verifier differs from the record's
    generation does not count.
    """

    changed = _document(verifier={**VERIFIER, "value": 0.04})
    _bind(drill.client, drill.headers, drill.agent_id, changed)
    other_verified = _verifier_run(
        drill.client, drill.headers, drill.agent_id, drill.qualification_id, healthy=True
    )
    body = copy.deepcopy(drill.body)
    body["evidence"]["verified_run_id"] = other_verified

    response = drill.put(body)

    assert response.status_code == 422, response.text
    assert _refusal_code(response) == "evidence_other_action"
    assert _records() == []


def test_a_worst_case_statement_over_2000_characters_is_refused(drill: Drill) -> None:
    """@spec AUTOMATED-REMEDIATION-22: a worst case statement is text of at most
    2000 characters; a longer or missing one writes nothing.
    """

    for worst_case in ("x" * 2001, ""):
        body = {**drill.body, "worst_case": worst_case}
        response = drill.put(body)
        assert response.status_code == 422, response.text
        assert _refusal_code(response) == "qualification_document_invalid"
    assert _records() == []

    body = {**drill.body, "worst_case": "x" * 2000}
    assert drill.put(body).status_code == 200


# --------------------------------------------------------------------------- #
# AUTOMATED-REMEDIATION-22: the operator principal
# --------------------------------------------------------------------------- #


def test_a_record_write_without_an_operator_principal_is_refused(drill: Drill) -> None:
    """@spec AUTOMATED-REMEDIATION-22 (acceptance): "a write ... without an operator
    principal is refused ``operator_principal_required``"; the administrative
    credential alone is not enough.
    """

    response = drill.put(principal=False)

    assert response.status_code == 403, response.text
    assert _refusal_code(response) == "operator_principal_required"
    assert response.headers.get("cache-control") == "no-store"
    assert _records() == []


def test_a_record_write_without_the_administrative_credential_is_401(
    client: Any, drill: Drill
) -> None:
    """@spec AUTOMATED-REMEDIATION-22: both routes use the administrative dependency."""

    response = client.put(
        _record_url(drill.agent_id, drill.qualification_id), json=drill.body, headers=_operator()
    )

    assert response.status_code == 401, response.text
    assert _records() == []


def test_a_verifier_run_without_an_operator_principal_is_refused(
    client: Any, auth_headers: dict[str, str], tmp_path: Path
) -> None:
    """@spec AUTOMATED-REMEDIATION-22 (acceptance): a verifier run without an
    operator principal is refused ``operator_principal_required`` and creates no
    execution.
    """

    agent_id = _agent(client, auth_headers, tmp_path)
    _bind(client, auth_headers, agent_id, _document())

    response = client.post(
        _runs_url(agent_id, str(uuid.uuid4())),
        json={"hook": HOOK, "action": ACTION_NAME, "target": TARGET},
        headers=auth_headers,
    )

    assert response.status_code == 403, response.text
    assert _refusal_code(response) == "operator_principal_required"
    assert _executions() == []


# --------------------------------------------------------------------------- #
# AUTOMATED-REMEDIATION-22: the verifier-run route
# --------------------------------------------------------------------------- #


def test_a_verifier_run_creates_only_reads_of_the_declared_verifier(
    client: Any, auth_headers: dict[str, str], tmp_path: Path
) -> None:
    """@spec AUTOMATED-REMEDIATION-22 @spec AUTOMATED-REMEDIATION-12: the run "can
    only start ``read`` executions of the declared verifier with
    ``authority_kind`` ``qualification``": one per sample to the deadline, at
    the read connector's in-force digest, the declared tool and pointer, its
    reference the run, started by the operator principal.
    """

    agent_id = _agent(client, auth_headers, tmp_path)
    generation = _bind(client, auth_headers, agent_id, _document())
    qualification_id = str(uuid.uuid4())

    started = _start_run(client, auth_headers, agent_id, qualification_id)

    assert started.status_code == 201, started.text
    run = started.json()
    assert run["qualification_id"] == qualification_id
    assert (run["hook"], run["action"], run["target"]) == (HOOK, ACTION_NAME, TARGET)
    assert run["generation"] == generation
    assert run["started_by"] == OPERATOR
    assert run["outcome"] is None
    executions = _executions()
    assert len(executions) == SAMPLES
    for row in executions:
        assert row["kind"] == "read"
        assert row["authority_kind"] == "qualification"
        assert row["authority_ref"] == str(run["id"])
        assert (row["connector"], row["tool"]) == (READ_CONNECTOR, READ_TOOL)
        assert row["connector_digest"] == READ_DIGEST
        assert row["pointer"] == VERIFIER["pointer"]
        assert row["state"] == "requested"


@pytest.mark.parametrize(
    "extra",
    [
        pytest.param({"tool": ACT_TOOL}, id="tool"),
        pytest.param({"arguments": FORWARD_ARGUMENTS}, id="arguments"),
        pytest.param({"connector": ACT_CONNECTOR}, id="connector"),
        pytest.param({"verifier": VERIFIER}, id="verifier"),
    ],
)
def test_the_verifier_run_route_refuses_tool_or_argument_fields(
    client: Any, auth_headers: dict[str, str], tmp_path: Path, extra: dict[str, Any]
) -> None:
    """@spec AUTOMATED-REMEDIATION-22 (acceptance): "the verifier-run route refuses
    a body with tool or argument fields"; nothing is created.
    """

    agent_id = _agent(client, auth_headers, tmp_path)
    _bind(client, auth_headers, agent_id, _document())

    response = _start_run(
        client,
        auth_headers,
        agent_id,
        str(uuid.uuid4()),
        {"hook": HOOK, "action": ACTION_NAME, "target": TARGET, **extra},
    )

    assert response.status_code == 422, response.text
    assert _executions() == []


@pytest.mark.parametrize(
    "target",
    [
        pytest.param("example-db", id="not-listed"),
        pytest.param("example-api ", id="not-literal"),
        pytest.param("example-*", id="pattern"),
        pytest.param("example-ns", id="another-arguments-value"),
    ],
)
def test_the_verifier_run_route_refuses_a_target_outside_the_allowed_list(
    client: Any, auth_headers: dict[str, str], tmp_path: Path, target: str
) -> None:
    """@spec AUTOMATED-REMEDIATION-22 (acceptance): a target that is not a literal
    member of the action's ``target.allowed`` is refused ``target_not_allowed``.
    """

    agent_id = _agent(client, auth_headers, tmp_path)
    _bind(client, auth_headers, agent_id, _document())

    response = _start_run(
        client,
        auth_headers,
        agent_id,
        str(uuid.uuid4()),
        {"hook": HOOK, "action": ACTION_NAME, "target": target},
    )

    assert response.status_code == 422, response.text
    assert _refusal_code(response) == "target_not_allowed"
    assert _executions() == []


def test_the_verifier_run_route_refuses_an_action_the_policy_does_not_declare(
    client: Any, auth_headers: dict[str, str], tmp_path: Path
) -> None:
    """@spec AUTOMATED-REMEDIATION-22: only "the declared verifier" can be run."""

    agent_id = _agent(client, auth_headers, tmp_path)
    _bind(client, auth_headers, agent_id, _document())

    response = _start_run(
        client,
        auth_headers,
        agent_id,
        str(uuid.uuid4()),
        {"hook": HOOK, "action": "restart-api", "target": TARGET},
    )

    assert response.status_code == 422, response.text
    assert _refusal_code(response) == "unknown_action"
    assert _executions() == []


def test_a_verifier_run_ends_verified_and_another_not_recovered(
    client: Any, auth_headers: dict[str, str], tmp_path: Path
) -> None:
    """@spec AUTOMATED-REMEDIATION-22 @spec AUTOMATED-REMEDIATION-18: the run is
    evaluated as a verification is: two consecutive satisfied samples after
    settle are ``verified``, the deadline with only unsatisfied samples is
    ``not-recovered``. No forward or restore execution is ever created.
    """

    agent_id = _agent(client, auth_headers, tmp_path)
    _bind(client, auth_headers, agent_id, _document())
    qualification_id = str(uuid.uuid4())

    _verifier_run(client, auth_headers, agent_id, qualification_id, healthy=False)
    _verifier_run(client, auth_headers, agent_id, qualification_id, healthy=True)

    assert {row["kind"] for row in _executions()} == {"read"}


# --------------------------------------------------------------------------- #
# AUTOMATED-REMEDIATION-22: no administrative forward route
# --------------------------------------------------------------------------- #


def test_no_route_lets_an_administrator_request_a_write(client: Any) -> None:
    """@spec AUTOMATED-REMEDIATION-22: "No route lets an administrator request a
    write directly". The qualification surface is exactly the record write, the
    verifier-run start and its read; forward evidence comes only from ordinary
    remediation approvals.
    """

    prefix = "/agents/{agent_id}/remediation-qualifications"
    surface = {
        (method, route.path)
        for route in client.app.routes
        if getattr(route, "path", "").startswith(prefix)
        for method in (getattr(route, "methods", None) or set())
        if method not in {"HEAD", "OPTIONS"}
    }

    assert surface == {
        ("PUT", f"{prefix}/{{qualification_id}}"),
        ("POST", f"{prefix}/{{qualification_id}}/verifier-runs"),
        ("GET", f"{prefix}/{{qualification_id}}/verifier-runs/{{run_id}}"),
    }, surface


# --------------------------------------------------------------------------- #
# AUTOMATED-REMEDIATION-22: the record lookup admission check 6 calls
# --------------------------------------------------------------------------- #


def test_check_6_is_missing_without_a_record_and_passes_with_a_complete_one(
    drill: Drill,
) -> None:
    """@spec AUTOMATED-REMEDIATION-22 @spec AUTOMATED-REMEDIATION-8 (check 6): "a
    complete record makes check 6 pass"; no reference, or a reference to no
    record, is ``qualification_missing``.
    """

    action = drill.automatic()["actions"][0]
    assert _lookup(drill.agent_id, {**action, "qualification": None}) == "qualification_missing"
    assert _lookup(drill.agent_id, action) == "qualification_missing"

    drill.record()

    assert _lookup(drill.agent_id, action) is None
    unknown = {**action, "qualification": str(uuid.uuid4())}
    assert _lookup(drill.agent_id, unknown) == "qualification_missing"


def test_a_new_connector_digest_makes_the_record_stale(drill: Drill) -> None:
    """@spec AUTOMATED-REMEDIATION-22 (acceptance): "deploying a new connector
    digest makes the same nomination an approval request with
    ``qualification_stale``": the lookup admission check 6 calls answers
    ``qualification_stale`` once the in-force acting digest differs from the
    record's, until the action is requalified.
    """

    drill.record()
    action = drill.automatic()["actions"][0]
    assert _lookup(drill.agent_id, action) is None

    _deploy(drill.client, drill.headers, drill.tmp_path, drill.agent_id, UPGRADED_DIGEST)

    assert _lookup(drill.agent_id, action) == "qualification_stale"


def test_another_agents_record_does_not_qualify(
    drill: Drill, client: Any, auth_headers: dict[str, str], tmp_path: Path
) -> None:
    """@spec AUTOMATED-REMEDIATION-22 @spec AUTOMATED-REMEDIATION-23: evidence and
    records are local to one agent; another agent naming the record has none.
    """

    drill.record()
    other = _agent(client, auth_headers, tmp_path)

    assert _lookup(other, drill.automatic()["actions"][0]) == "qualification_missing"


# --------------------------------------------------------------------------- #
# AUTOMATED-REMEDIATION-23: qualification_required at policy write
# --------------------------------------------------------------------------- #


def test_an_automatic_action_with_no_qualification_reference_is_refused(
    client: Any, auth_headers: dict[str, str], tmp_path: Path
) -> None:
    """@spec AUTOMATED-REMEDIATION-23: the check runs at policy write, on a first
    binding too; ``automatic`` false with no reference stays accepted.
    """

    agent_id = _agent(client, auth_headers, tmp_path)

    refused = _put_policy(client, auth_headers, agent_id, _document(automatic=True))

    assert refused.status_code == 422, refused.text
    assert _refusal_code(refused) == "qualification_required"
    assert _generations(agent_id) == []
    assert _put_policy(client, auth_headers, agent_id, _document()).status_code == 200


def test_an_automatic_action_without_a_record_is_refused_at_policy_write(
    drill: Drill,
) -> None:
    """@spec AUTOMATED-REMEDIATION-23 (acceptance): "a policy write setting
    ``automatic`` true for an action without a valid qualification record is
    refused ``qualification_required``"; no generation is created.
    """

    before = _generations(drill.agent_id)
    for qualification in (None, str(uuid.uuid4()), drill.qualification_id):
        response = _put_policy(
            drill.client,
            drill.headers,
            drill.agent_id,
            drill.automatic(qualification=qualification),
        )
        assert response.status_code == 422, response.text
        assert _refusal_code(response) == "qualification_required"
        assert response.json()["detail"]["path"] == "/actions/0/qualification"
    assert _generations(drill.agent_id) == before


def test_an_automatic_action_with_a_valid_record_is_accepted_and_passes_check_6(
    drill: Drill,
) -> None:
    """@spec AUTOMATED-REMEDIATION-23 (acceptance): "the same write with a valid
    record is accepted"; the action then passes admission check 6 (task 9's
    end-to-end admission of the next in-bounds nomination lands with it).
    """

    drill.record()
    document = drill.automatic()

    response = _put_policy(drill.client, drill.headers, drill.agent_id, document)

    assert response.status_code == 200, response.text
    assert response.json()["policy"]["actions"][0]["automatic"] is True
    assert _lookup(drill.agent_id, document["actions"][0]) is None


def test_a_stale_record_does_not_satisfy_the_policy_write(drill: Drill) -> None:
    """@spec AUTOMATED-REMEDIATION-23 @spec AUTOMATED-REMEDIATION-22: a record is
    valid only for its digest, so after a connector upgrade the write is
    refused ``qualification_required``.
    """

    drill.record()
    _deploy(drill.client, drill.headers, drill.tmp_path, drill.agent_id, UPGRADED_DIGEST)
    before = _generations(drill.agent_id)

    response = _put_policy(drill.client, drill.headers, drill.agent_id, drill.automatic())

    assert response.status_code == 422, response.text
    assert _refusal_code(response) == "qualification_required"
    assert _generations(drill.agent_id) == before


@pytest.mark.parametrize(
    "change",
    [
        pytest.param({"verifier": {**VERIFIER, "value": 0.04}}, id="another-verifier"),
        pytest.param({"tool": "patch_hpa"}, id="another-tool"),
        pytest.param({"reversibility": "idempotent"}, id="another-reversibility"),
    ],
)
def test_a_record_for_another_declaration_does_not_satisfy_the_policy_write(
    drill: Drill, change: dict[str, Any]
) -> None:
    """@spec AUTOMATED-REMEDIATION-22 @spec AUTOMATED-REMEDIATION-23: the record is
    for one connector, tool and verifier declaration, with the evidence its
    reversibility requires; an automatic action declaring anything else under
    the same reference is refused ``qualification_required``.
    """

    drill.record()
    before = _generations(drill.agent_id)

    response = _put_policy(drill.client, drill.headers, drill.agent_id, drill.automatic(**change))

    assert response.status_code == 422, response.text
    assert _refusal_code(response) == "qualification_required"
    assert _generations(drill.agent_id) == before


def test_another_agents_record_does_not_satisfy_the_policy_write(
    drill: Drill, client: Any, auth_headers: dict[str, str], tmp_path: Path
) -> None:
    """@spec AUTOMATED-REMEDIATION-23: evidence from elsewhere does not qualify."""

    drill.record()
    other = _agent(client, auth_headers, tmp_path)

    response = _put_policy(client, auth_headers, other, drill.automatic())

    assert response.status_code == 422, response.text
    assert _refusal_code(response) == "qualification_required"
    assert _generations(other) == []
