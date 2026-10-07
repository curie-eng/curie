"""Admission, limits, breaker, disarm (automated remediation plan task 9).

@spec AUTOMATED-REMEDIATION-8 @spec AUTOMATED-REMEDIATION-9 @spec AUTOMATED-REMEDIATION-10
@spec AUTOMATED-REMEDIATION-11

docs/superpowers/specs/2026-10-07-automated-remediation.md, with the maintainer
rulings of 2026-10-07 (incident window, administrative principal) and executor
amendment E8. Every row is driven through its real producer: a signed protected
delivery admitted onto the owned disposable TLS broker (the binding and its
``remediation_generation``, task 5), the real nomination route (task 6), the real
policy routes (task 3), the kill route, the agent's in-force version deployed
through the version and deployment routes, and the executor's claim, dispatch,
outcome and sample routes (tasks 7, 8, 11) with real Postgres and Valkey.

Surface these tests fix (``.projects/plans/task-remediation-admission.tests.md``):

* Admission runs inside ``POST /v1/internal/remediation/nominations``, after the
  rows are written: each ``received`` row is evaluated in the order of
  AUTOMATED-REMEDIATION-8 and leaves the route ``refused`` (``agent_stopped``,
  check 2), ``approval_requested`` (checks 3 to 11, the failed check's reason in
  the new column ``remediation_nominations.approval_reason``) or
  ``precondition_pending`` with exactly one ``read`` execution of the declared
  precondition (connector, in-force digest, tool, canonical arguments, pointer;
  ``authority_kind`` ``policy``; idempotency key under
  ``remediation:<nomination id>:``) and the check 11 reservation taken.
* The precondition read ending (``POST /action-executions/{id}/samples``, a
  refused ``/outcome``) decides check 12: holding re-runs checks 2 to 11 and, in
  the same transaction, creates the policy forward execution
  (``remediation:<nomination id>:policy``, AUTOMATED-REMEDIATION-13) and leaves
  the nomination ``admitted`` naming it; not holding is ``precondition_not_met``;
  a refused read is ``precondition_unavailable``; a failed re-check sends the
  nomination to approval with that check's reason (or ends it ``agent_stopped``)
  and creates no forward execution. The reservation is released whenever the
  nomination does not execute.
* ``remediation_breakers`` (``id``, ``agent_id``, ``connector``, ``tool``,
  ``target``, ``opened_at``, ``closed_at``, ``closed_by``, ``close_reason``)
  opens on any verification outcome other than ``verified`` and on a forward
  execution ending ``failed`` or ``indeterminate``, whatever authorized it.
* ``POST /agents/{agent_id}/hooks/{hook}/remediation-policy/breakers/{breaker_id}/close``
  with body ``{"reason"}``, the platform key and an operator principal; 200 with
  the closed breaker (``closed_by`` the principal, ``close_reason``).
* E8: ``POST /action-executions/claim`` ends a ``policy`` forward execution
  ``refused`` with ``policy_changed`` when its generation is no longer current
  and armed or a breaker is open for its target key, before any sandbox claim;
  the nomination returns to ``approval_requested`` (reason ``policy_changed``)
  with no verification outcome.
* Check 6 is the real record lookup of plan task 15,
  ``curie_api.remediation_qualifications.qualification_refusal``. Since task 15
  a policy write with an ``automatic`` action needs a valid qualification
  record (AUTOMATED-REMEDIATION-23), so ``World.bind`` qualifies each automatic
  action declaration once per world through the real routes, as the drill
  order states: bind it with ``automatic`` false, seed the restore or repeat
  evidence on the disposable target ``DRILL_TARGET``, run the declared
  verifier through ``POST .../remediation-qualifications/{id}/verifier-runs``
  to ``not-recovered`` and to ``verified``, write the record with ``PUT``, then
  bind the automatic generation naming it.

Approval requests are asserted on the nomination row (state and reason); the
``Approval`` row itself is task 10's (``request_remediation_approval``), on a
sibling branch. Every identifier is a placeholder.
"""

from __future__ import annotations

import asyncio
import copy
import hashlib
import importlib
import io
import json
import tarfile
import time
import uuid
from collections.abc import AsyncIterator, Callable, Iterator
from contextlib import asynccontextmanager, contextmanager
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import httpx
import pytest
import redis.asyncio
import redis.exceptions
from _migration_support import sql_dicts, sql_rows
from _protected_ingress_harness import (
    BODY,
    _broker,
    deliver,
    ingress_broker_fixture,  # noqa: F401  (fixture)
    private_entries,
    record,
    signed,
)
from _protected_ingress_harness import (
    canonical as broker_canonical,
)
from _protected_ingress_harness import (
    event_id as event_id_of,
)
from channel_protocol import hook_conversation_id
from curie_api import hook_signing, hook_source_signing
from curie_api.config import get_settings
from curie_internal.keyspace import kill_key
from sqlalchemy import event as sa_event
from sqlalchemy.engine import Engine
from sqlalchemy.exc import OperationalError
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from test_hook_source_support import (  # noqa: F401  (support_db is a fixture)
    HOOK,
    scoped_secret,
    support_db,
)
from test_remediation_nomination_routes import (
    OPERATOR,
    ROUTE_NAME,
    Stage,
    admin_headers,
    block,
    remediation,
    staged,
    worker_headers,
)
from test_remediation_qualifications import (
    WORST_CASE,
    _confirmed_restore,
    _conflict_restore,
    _forward,
)

pytestmark = pytest.mark.usefixtures("support_db")
admission_service = _broker.admission_service
TIMEOUT = 120

HOOK_TWO = "pager"
ACTION_NAME = "scale-out-api"
ACT_CONNECTOR = "k8s"
ACT_TOOL = "scale_deployment"
ACT_DIGEST = "sha256:" + "ab" * 32
ACT_IMAGE = f"ghcr.io/example/k8s-restorer@{ACT_DIGEST}"
READ_CONNECTOR = "example-metrics"
READ_TOOL = "query_value"
READ_DIGEST = "sha256:" + "ef" * 32
READ_IMAGE = f"ghcr.io/example/metrics-mcp@{READ_DIGEST}"
# The placeholder ``World.bind`` replaces with the record it qualifies (task 15).
QUALIFICATION = "qual-example-1"
TARGETS = ["example-api", "example-worker", "example-web", "example-batch", "example-cron"]
# The disposable target the qualification drills run against; no test nominates it.
DRILL_TARGET = TARGETS[4]
DRILL_ARGUMENTS: dict[str, Any] = {
    "namespace": "example-ns",
    "deployment": DRILL_TARGET,
    "replicas": 4,
}
UPGRADED_ACT_DIGEST = "sha256:" + "ac" * 32
# Canonically equivalent to TARGETS-style values but not a literal member:
# "cafe" + COMBINING ACUTE ACCENT against the precomposed form the policy lists.
LISTED_COMPOSED = "café-api"
NOT_LITERAL = "café-api"

HEALTHY = 0.01
UNHEALTHY = 0.9
CONDITION_PRESENT = 0.9  # precondition is "gt 0.5"
CONDITION_ABSENT = 0.1

PRECONDITION: dict[str, Any] = {
    "connector": READ_CONNECTOR,
    "tool": READ_TOOL,
    "arguments": {"query": "scalar(example_error_ratio)"},
    "pointer": "/data/1",
    "comparator": "gt",
    "value": 0.5,
}
VERIFIER: dict[str, Any] = {
    "connector": READ_CONNECTOR,
    "tool": READ_TOOL,
    "arguments": {"query": "scalar(example_error_ratio)"},
    "pointer": "/data/1",
    "comparator": "lt",
    "value": 0.05,
    "settle_seconds": 30,
    "deadline_seconds": 60,
    "interval_seconds": 30,
    "consecutive": 1,
}
ACTION: dict[str, Any] = {
    "name": ACTION_NAME,
    "kind": "remediate",
    "connector": ACT_CONNECTOR,
    "tool": ACT_TOOL,
    "arguments": {
        "namespace": {"type": "string", "allowed": ["example-ns"]},
        "deployment": {"type": "string", "allowed": [*TARGETS, LISTED_COMPOSED]},
        "replicas": {"type": "integer", "minimum": 2, "maximum": 6},
    },
    "target": {"argument": "deployment", "allowed": [*TARGETS, LISTED_COMPOSED]},
    "reversibility": "idempotent",
    "precondition": PRECONDITION,
    "verifier": VERIFIER,
    "automatic": True,
    "qualification": QUALIFICATION,
}
LIMITS: dict[str, Any] = {
    "per_policy_per_hour": 3,
    "per_incident_per_target": 1,
    "incident_window_seconds": 3600,
    "approval_ttl_seconds": 14400,
}
CHECK_11 = {"policy_rate_limit", "action_rate_limit", "incident_limit", "turn_limit", "target_live"}


def policy(*, limits: dict[str, Any] | None = None, **action: Any) -> dict[str, Any]:
    """The bound document: one action, ``ACTION`` with ``action`` overrides."""

    declared = copy.deepcopy(ACTION)
    declared.update(action)
    return {"route": ROUTE_NAME, "limits": {**LIMITS, **(limits or {})}, "actions": [declared]}


def entry(target: str = TARGETS[0], *, replicas: int = 4, reason: str | None = None) -> Any:
    item: dict[str, Any] = {
        "action": ACTION_NAME,
        "arguments": {"namespace": "example-ns", "deployment": target, "replicas": replicas},
    }
    if reason is not None:
        item["reason"] = reason
    return item


def target_key(target: str) -> str:
    """AUTOMATED-REMEDIATION-10: the connector plus the canonical JSON of the value."""

    return f"{ACT_CONNECTOR}:{json.dumps(target, ensure_ascii=False)}"


def connectors_yaml(*, shared_secret: bool = False, act_digest: str = ACT_DIGEST) -> str:
    """The acting ``k8s`` (with its sealing key) and the read connector.

    ``shared_secret``: both connectors expand one secret name, so the verifier
    is not independent of the acting credential (AUTOMATED-REMEDIATION-17).
    """

    if shared_secret:
        shared = "    secrets:\n      - SHARED_TOKEN\n"
        act_secrets, read_secrets = shared, shared
    else:
        act_secrets = (
            "    secrets:\n      - name: SNAPSHOT_SEALING_KEY\n"
            "        from_secret: k8s-restorer-seal\n"
        )
        read_secrets = "    secrets:\n      - METRICS_TOKEN\n"
    return (
        "connectors:\n"
        f"  {ACT_CONNECTOR}:\n    image: ghcr.io/example/k8s-restorer@{act_digest}\n{act_secrets}"
        f"  {READ_CONNECTOR}:\n    image: {READ_IMAGE}\n{read_secrets}"
    )


# --------------------------------------------------------------------------- #
# Real producers
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


def _platform() -> dict[str, str]:
    return {"X-API-Key": get_settings().api_key}


async def deploy(stage: Stage, tmp_path: Path, connectors: str | None = None) -> None:
    """A new version carrying ``connectors``, deployed: the agent's in-force version."""

    client = stage.client
    agent = await client.get(f"/agents/{stage.agent}", headers=_platform())
    assert agent.status_code == 200, agent.text
    name = agent.json()["name"]
    version = await client.post(
        f"/agents/{stage.agent}/versions",
        json={"version_label": f"v-{uuid.uuid4().hex[:6]}", "created_by": "test"},
        headers=_platform(),
    )
    assert version.status_code == 201, version.text
    version_id = str(version.json()["id"])
    upload = await client.put(
        f"/agents/{stage.agent}/versions/{version_id}/bundle",
        files={
            "file": (
                "bundle.tar.gz",
                await asyncio.to_thread(_archive, tmp_path, name, connectors or connectors_yaml()),
            )
        },
        headers=_platform(),
    )
    assert upload.status_code == 201, upload.text
    deployment = await client.post(
        "/deployments",
        json={"agent_id": stage.agent, "version_id": version_id, "environment": "dev"},
        headers=_platform(),
    )
    assert deployment.status_code == 201, deployment.text


def _base(stage: Stage, hook: str = HOOK) -> str:
    return f"/agents/{stage.agent}/hooks/{hook}/remediation-policy"


class Hooks:
    """The generation each hook's policy is at, written only through the routes."""

    def __init__(self) -> None:
        self.generation: dict[str, int] = {}


async def write_policy(
    stage: Stage, hooks: Hooks, document: dict[str, Any], hook: str = HOOK
) -> int:
    """``PUT`` the hook's policy (a new binding starts disarmed; a later write keeps it)."""

    response = await stage.client.put(
        _base(stage, hook),
        json={
            "expected_generation": str(hooks.generation.get(hook, 0)),
            "operation_id": str(uuid.uuid4()),
            "policy": copy.deepcopy(document),
        },
        headers=admin_headers(),
    )
    assert response.status_code == 200, response.text
    hooks.generation[hook] = int(response.json()["generation"])
    return hooks.generation[hook]


async def set_armed(stage: Stage, hooks: Hooks, armed: bool, hook: str = HOOK) -> int:
    response = await stage.client.post(
        f"{_base(stage, hook)}/{'arm' if armed else 'disarm'}",
        json={
            "expected_generation": str(hooks.generation[hook]),
            "operation_id": str(uuid.uuid4()),
        },
        headers=admin_headers(),
    )
    assert response.status_code == 200, response.text
    assert response.json()["armed"] is armed, response.text
    hooks.generation[hook] = int(response.json()["generation"])
    return hooks.generation[hook]


async def kill(stage: Stage) -> None:
    response = await stage.client.post(f"/agents/{stage.agent}/kill", headers=_platform())
    assert response.status_code == 200, response.text


class Deliveries:
    """Signed protected deliveries through the real hook route."""

    def __init__(self, stage: Stage) -> None:
        self.stage = stage
        self.count = 0

    async def event(
        self, body: bytes | None = None, *, params: dict[str, str] | None = None
    ) -> str:
        self.count += 1
        delivery = f"admission-delivery-{self.count}-{uuid.uuid4().hex[:6]}"
        payload = BODY if body is None else body
        headers = signed(scoped_secret(self.stage.agent), delivery=delivery, body=payload)
        response = await deliver(
            self.stage.client, self.stage.agent, headers, body=payload, params=params
        )
        assert response.status_code == 200, response.text
        assert response.json()["acceptance_status"] == "accepted", response.text
        event = str(response.json()["event_id"])
        assert event == event_id_of(self.stage.agent, delivery)
        return event


async def second_hook_event(stage: Stage, template_event: str, generation: int) -> str:
    """What a delivery to ``HOOK_TWO`` admitted under ``generation`` would create.

    The harness provisions one protected hook's runtime; the plan drives tasks 6
    to 13 by inserting the records a delivery would have created. These are the
    first hook's real records with only the event id, hook and admitted
    remediation generation changed: the binding, written to the real broker,
    and the reply surface the hook route records for an admitted delivery
    (``remediation_delivery_surfaces``, AUTOMATED-REMEDIATION-15).
    """

    template = record(stage.broker, "protected:admission:binding:" + template_event)
    assert template is not None, "the template delivery left no binding"
    event = f"hook-{stage.agent}-{HOOK_TWO}-{uuid.uuid4().hex[:16]}"
    template["event_id"] = event
    template["remediation_generation"] = str(generation)
    stage.broker.command("SET", "protected:admission:binding:" + event, broker_canonical(template))
    copied = await asyncio.to_thread(
        sql_rows,
        "INSERT INTO curie.remediation_delivery_surfaces "
        "(event_id, agent_id, hook, reply_kind, reply_channel, reply_endpoint, reply_adapter) "
        "SELECT :event, agent_id, :hook, reply_kind, reply_channel, reply_endpoint, "
        "reply_adapter FROM curie.remediation_delivery_surfaces WHERE event_id = :template "
        "RETURNING event_id",
        {"event": event, "hook": HOOK_TWO, "template": template_event},
    )
    assert len(copied) == 1, "the template delivery recorded no reply surface"
    return event


def protect_second_hook(stage: Stage) -> None:
    """An active protected source policy for ``HOOK_TWO`` (a policy needs one)."""

    sql_rows(
        "INSERT INTO curie.hook_source_policies "
        "(agent_id, hook, generation, operation_id, mode, tool_access, runtime_id, "
        "qualification_id, bundle_digest, legacy_generation, updated_at) "
        "VALUES (:agent_id, :hook, 1, :operation_id, 'protected', 'read-only', :runtime_id, "
        ":qualification_id, :bundle_digest, 0, now())",
        {
            "agent_id": uuid.UUID(stage.agent),
            "hook": HOOK_TWO,
            "operation_id": uuid.uuid4(),
            "runtime_id": str(uuid.uuid4()),
            "qualification_id": str(uuid.uuid4()),
            "bundle_digest": "sha256:" + "cd" * 32,
        },
    )


async def submit(stage: Stage, event: str, *entries: Any) -> list[dict[str, Any]]:
    """Submit one block for ``event``; the event's nomination rows, in block order."""

    response = await stage.submit(event, block(*entries))
    assert response.status_code == 200, response.text
    ids = [uuid.UUID(i) for i in response.json()["nomination_ids"]]
    assert len(ids) == len(entries), response.text
    return [await nomination(i) for i in ids]


async def nominate(
    stage: Stage, deliveries: Deliveries, *entries: Any, body: bytes | None = None
) -> list[dict[str, Any]]:
    event = await deliveries.event(body)
    return await submit(stage, event, *entries)


# --------------------------------------------------------------------------- #
# Rows
# --------------------------------------------------------------------------- #


async def q(statement: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    return await asyncio.to_thread(sql_dicts, statement, params)


async def nomination(nomination_id: Any) -> dict[str, Any]:
    rows = await q(
        "SELECT * FROM curie.remediation_nominations WHERE id = :id", {"id": nomination_id}
    )
    assert len(rows) == 1, rows
    return rows[0]


async def executions_of(nomination_id: Any, kind: str | None = None) -> list[dict[str, Any]]:
    clause = "" if kind is None else "AND kind = :kind"
    return await q(
        "SELECT * FROM curie.action_executions WHERE idempotency_key LIKE :key "
        f"{clause} ORDER BY created_at, id",
        {"key": f"remediation:{nomination_id}:%", "kind": kind},
    )


async def precondition_read(nomination_id: Any) -> dict[str, Any]:
    reads = await executions_of(nomination_id, "read")
    assert len(reads) == 1, f"one precondition read expected, found {reads}"
    return reads[0]


async def forwards(nomination_id: Any) -> list[dict[str, Any]]:
    return await executions_of(nomination_id, "forward")


async def all_forwards() -> list[dict[str, Any]]:
    """Every forward execution but the qualification drill's seeded evidence."""

    return await q(
        "SELECT * FROM curie.action_executions WHERE kind = 'forward' "
        "AND idempotency_key NOT LIKE 'drill:%'"
    )


async def breakers(target: str | None = None) -> list[dict[str, Any]]:
    if target is None:
        return await q("SELECT * FROM curie.remediation_breakers ORDER BY opened_at, id")
    return await q(
        "SELECT * FROM curie.remediation_breakers WHERE target = :target ORDER BY opened_at, id",
        {"target": target_key(target)},
    )


async def ledger_rows() -> int:
    return int((await q("SELECT count(*) AS n FROM curie.agent_actions"))[0]["n"])


def assert_approval(row: dict[str, Any], reason: str) -> None:
    """Sent to an approval request naming ``reason``; nothing executes."""

    assert row["state"] == "approval_requested", row
    assert row["approval_reason"] == reason, row
    assert row["approval_id"] is not None, row
    assert row["refusal_code"] is None, row
    assert row["execution_id"] is None, row
    assert row["verification_outcome"] is None, row


def assert_stopped(row: dict[str, Any]) -> None:
    """A stopped agent asks nobody: ``refused`` ``agent_stopped``, no approval."""

    assert row["state"] == "refused", row
    assert row["refusal_code"] == "agent_stopped", row
    assert row["approval_reason"] is None, row
    assert row["approval_id"] is None and row["execution_id"] is None, row
    assert row["decided_at"] is not None, row


async def assert_pending(row: dict[str, Any]) -> dict[str, Any]:
    """Admitted to the precondition read: ``precondition_pending`` with one read."""

    assert row["state"] == "precondition_pending", row
    assert row["approval_reason"] is None and row["refusal_code"] is None, row
    assert await forwards(row["id"]) == []
    return await precondition_read(row["id"])


async def assert_nothing_executes(row: dict[str, Any]) -> None:
    assert await executions_of(row["id"]) == [], "a refused admission creates no execution"


# --------------------------------------------------------------------------- #
# The executor's routes, driven as the worker drives them
# --------------------------------------------------------------------------- #


def _fence(claimed: dict[str, Any]) -> dict[str, Any]:
    return {"lease_owner": claimed["lease_owner"], "attempt": claimed["attempt"]}


async def claim(stage: Stage) -> httpx.Response:
    return await stage.client.post(
        "/action-executions/claim",
        json={"lease_owner": "worker-a", "lease_seconds": 60},
        headers=worker_headers(),
    )


async def claim_one(stage: Stage, execution_id: Any) -> dict[str, Any]:
    claimed = await claim(stage)
    assert claimed.status_code == 200, claimed.text
    assert claimed.json()["id"] == str(execution_id), claimed.text
    return dict(claimed.json())


async def report(stage: Stage, claimed: dict[str, Any], value: Any) -> httpx.Response:
    return await stage.client.post(
        f"/action-executions/{claimed['id']}/samples",
        json={**_fence(claimed), "sample": "value", "value": value},
        headers=worker_headers(),
    )


async def end(
    stage: Stage, claimed: dict[str, Any], state: str, code: str | None = None
) -> httpx.Response:
    body: dict[str, Any] = {**_fence(claimed), "state": state}
    if code is not None:
        body["code"] = code
    return await stage.client.post(
        f"/action-executions/{claimed['id']}/outcome", json=body, headers=worker_headers()
    )


async def take_precondition(stage: Stage, row: dict[str, Any], value: Any) -> dict[str, Any]:
    """Claim the nomination's precondition read and report ``value``; the row after."""

    read = await precondition_read(row["id"])
    claimed = await claim_one(stage, read["id"])
    reported = await report(stage, claimed, value)
    assert reported.status_code == 200, reported.text
    return await nomination(row["id"])


async def admit(stage: Stage, row: dict[str, Any]) -> dict[str, Any]:
    """The precondition holds: the nomination is admitted with one policy forward."""

    after = await take_precondition(stage, row, CONDITION_PRESENT)
    assert after["state"] == "admitted", after
    created = await forwards(row["id"])
    assert len(created) == 1, created
    assert after["execution_id"] == created[0]["id"], after
    return created[0]


async def _db_now() -> datetime:
    now: datetime = (await q("SELECT now() AS now"))[0]["now"]
    return now


async def _shift_schedules(seconds: float) -> None:
    await asyncio.to_thread(
        sql_rows,
        "UPDATE curie.action_executions SET "
        "not_before = not_before - make_interval(secs => :s), "
        "dispatched_at = dispatched_at - make_interval(secs => :s)",
        {"s": seconds},
    )


async def _next_sample(stage: Stage, nomination_id: Any) -> dict[str, Any]:
    """Make the verification's earliest requested sample due and claim it."""

    pending = [
        r
        for r in await executions_of(nomination_id, "read")
        if r["state"] == "requested" and r["not_before"] is not None
    ]
    assert pending, "no verifier sample is scheduled"
    earliest = min(r["not_before"] for r in pending)
    await _shift_schedules((earliest - await _db_now() + timedelta(seconds=1)).total_seconds())
    claimed = await claim(stage)
    assert claimed.status_code == 200, claimed.text
    return dict(claimed.json())


async def run_forward(
    stage: Stage, row: dict[str, Any], forward: dict[str, Any], outcome: str
) -> None:
    """Drive a forward execution to ``outcome``.

    ``verified`` and ``not-recovered`` run the verifier (one healthy sample at
    settle; or every sample unhealthy to the deadline); ``failed`` and
    ``indeterminate`` end the forward execution itself.
    """

    claimed = await claim_one(stage, forward["id"])
    dispatched = await stage.client.post(
        f"/action-executions/{forward['id']}/dispatch",
        json=_fence(claimed),
        headers=worker_headers(),
    )
    assert dispatched.status_code == 200, dispatched.text
    if outcome in ("failed", "indeterminate"):
        code = "connector_error" if outcome == "failed" else "response_lost"
        ended = await end(stage, claimed, outcome, code)
        assert ended.status_code == 200, ended.text
        return
    ended = await end(stage, claimed, "confirmed")
    assert ended.status_code == 200, ended.text
    if outcome == "verified":
        sample = await _next_sample(stage, row["id"])
        assert (await report(stage, sample, HEALTHY)).status_code == 200
    else:
        while (await nomination(row["id"]))["verification_outcome"] is None:
            sample = await _next_sample(stage, row["id"])
            assert (await report(stage, sample, UNHEALTHY)).status_code == 200
    finished = await nomination(row["id"])
    assert finished["verification_outcome"] == outcome, finished


_AGEABLE_EXCLUDED = {"remediation_policy_generations", "remediation_policies"}


def _age(seconds: int) -> None:
    """As if ``seconds`` had passed: every remediation and execution timestamp moves back.

    Every timestamp column of ``action_executions``, ``agent_actions`` and every
    ``remediation_*`` table (other than the immutable policy history), whatever
    the implementation names them, so an incident window, a rolling hour or a
    reservation ages exactly as it would.
    """

    columns = sql_dicts(
        "SELECT table_name, column_name FROM information_schema.columns "
        "WHERE table_schema = 'curie' AND data_type LIKE 'timestamp%' "
        "AND (table_name LIKE 'remediation\\_%' OR table_name IN "
        "('action_executions', 'agent_actions'))"
    )
    by_table: dict[str, list[str]] = {}
    for column in columns:
        if column["table_name"] not in _AGEABLE_EXCLUDED:
            by_table.setdefault(column["table_name"], []).append(column["column_name"])
    for table, names in by_table.items():
        assignments = ", ".join(f'"{n}" = "{n}" - make_interval(secs => :s)' for n in names)
        sql_rows(f'UPDATE curie."{table}" SET {assignments}', {"s": seconds})


async def age(seconds: int) -> None:
    await asyncio.to_thread(_age, seconds)


def _approver() -> dict[str, str]:
    """The route's approver (``approvers.users``) as an operator principal, no platform key."""

    return {k: v for k, v in admin_headers().items() if k != "X-API-Key"}


async def assert_requested(row: dict[str, Any]) -> dict[str, Any]:
    """The nomination's ``remediation`` approval (task 10's request), bound to its call."""

    assert row["approval_id"] is not None, row
    (approval,) = await q(
        "SELECT * FROM curie.approvals WHERE id = :id", {"id": row["approval_id"]}
    )
    assert approval["purpose"] == "remediation", approval
    assert approval["route"] == ROUTE_NAME, approval
    assert approval["granted_tool"] == f"mcp__{ACT_CONNECTOR}__{ACT_TOOL}", approval
    assert approval["expires_at"] is not None, approval
    return approval


async def approved_forward(stage: Stage, row: dict[str, Any]) -> dict[str, Any]:
    """Approve the nomination's approval through the real resolve route (task 10).

    Admission raised the approval through ``request_remediation_approval``; the
    route's approver approves it, and the resolution creates the one
    approval-authority forward execution (AUTOMATED-REMEDIATION-16).
    """

    asked = await nomination(row["id"])
    assert asked["state"] == "approval_requested", asked
    await assert_requested(asked)
    resolved = await stage.client.post(
        f"/approvals/{asked['approval_id']}/resolve",
        json={"decision": "approved"},
        headers=_approver(),
    )
    assert resolved.status_code == 200, resolved.text
    rows = await q(
        "SELECT * FROM curie.action_executions WHERE kind = 'forward' AND idempotency_key = :key",
        {"key": f"remediation:{row['id']}:approval:{asked['approval_id']}"},
    )
    assert len(rows) == 1 and rows[0]["authority_kind"] == "approval", rows
    return rows[0]


# --------------------------------------------------------------------------- #
# Staging
# --------------------------------------------------------------------------- #


def _declaration(action: dict[str, Any]) -> str:
    """What one qualification record covers (AUTOMATED-REMEDIATION-22)."""

    return json.dumps(
        {key: action.get(key) for key in ("connector", "tool", "reversibility", "verifier")},
        sort_keys=True,
    )


def _qualifications(stage: Stage, qualification_id: str) -> str:
    return f"/agents/{stage.agent}/remediation-qualifications/{qualification_id}"


async def verifier_run(stage: Stage, qualification_id: str, hook: str, *, healthy: bool) -> str:
    """One qualification verifier run on ``DRILL_TARGET`` driven to its outcome."""

    started = await stage.client.post(
        f"{_qualifications(stage, qualification_id)}/verifier-runs",
        json={"hook": hook, "action": ACTION_NAME, "target": DRILL_TARGET},
        headers=admin_headers(),
    )
    assert started.status_code == 201, started.text
    run_id = str(started.json()["id"])
    outcome = None
    while outcome is None:
        pending = await q(
            "SELECT * FROM curie.action_executions WHERE kind = 'read' "
            "AND authority_ref = :ref AND state = 'requested' ORDER BY not_before",
            {"ref": run_id},
        )
        assert pending, f"verifier run {run_id} has no sample left and no outcome"
        due = pending[0]["not_before"] - await _db_now() + timedelta(seconds=1)
        seconds = due.total_seconds()
        await asyncio.to_thread(
            sql_rows,
            "UPDATE curie.action_executions SET not_before = not_before - "
            "make_interval(secs => :s) WHERE authority_ref = :ref",
            {"s": seconds, "ref": run_id},
        )
        await asyncio.to_thread(
            sql_rows,
            "UPDATE curie.remediation_qualification_verifier_runs SET started_at = "
            "started_at - make_interval(secs => :s) WHERE id = :id",
            {"s": seconds, "id": uuid.UUID(run_id)},
        )
        claimed = await claim_one(stage, pending[0]["id"])
        reported = await report(stage, claimed, HEALTHY if healthy else UNHEALTHY)
        assert reported.status_code == 200, reported.text
        read = await stage.client.get(
            f"{_qualifications(stage, qualification_id)}/verifier-runs/{run_id}",
            headers=_platform(),
        )
        assert read.status_code == 200, read.text
        outcome = read.json()["outcome"]
    assert outcome == ("verified" if healthy else "not-recovered"), outcome
    return run_id


class World:
    def __init__(self, stage: Stage, tmp_path: Path) -> None:
        self.stage = stage
        self.tmp_path = tmp_path
        self.hooks = Hooks()
        self.deliveries = Deliveries(stage)
        self.document: dict[str, Any] = policy()
        # The record id each qualified action declaration got (task 15).
        self.records: dict[str, str] = {}

    async def qualify(self, document: dict[str, Any], hook: str = HOOK) -> dict[str, Any]:
        """``document`` with each automatic action naming a valid record, made once.

        @spec AUTOMATED-REMEDIATION-22 @spec AUTOMATED-REMEDIATION-23: the drill
        order through the real routes (see the module docstring).
        """

        qualified = copy.deepcopy(document)
        for action in qualified["actions"]:
            if not action.get("automatic") or action.get("qualification") != QUALIFICATION:
                continue
            key = _declaration(action)
            if key not in self.records:
                self.records[key] = await self._record(document, action, hook)
            action["qualification"] = self.records[key]
        return qualified

    async def _record(self, document: dict[str, Any], action: dict[str, Any], hook: str) -> str:
        drill = copy.deepcopy(document)
        for declared in drill["actions"]:
            declared.update(automatic=False, qualification=None)
        generation = await write_policy(self.stage, self.hooks, drill, hook)
        qualification_id = str(uuid.uuid4())
        agent = str(self.stage.agent)
        evidence: dict[str, Any] = {}
        if action["reversibility"] == "reversible":
            evidence["restore_execution_id"] = str(
                await asyncio.to_thread(_confirmed_restore, agent, arguments=DRILL_ARGUMENTS)
            )
            evidence["conflict_execution_id"] = str(
                await asyncio.to_thread(_conflict_restore, agent, arguments=DRILL_ARGUMENTS)
            )
        else:
            evidence["forward_execution_ids"] = [
                str(await asyncio.to_thread(_forward, agent, arguments=DRILL_ARGUMENTS))
                for _ in range(2)
            ]
        evidence["not_recovered_run_id"] = await verifier_run(
            self.stage, qualification_id, hook, healthy=False
        )
        evidence["verified_run_id"] = await verifier_run(
            self.stage, qualification_id, hook, healthy=True
        )
        written = await self.stage.client.put(
            _qualifications(self.stage, qualification_id),
            json={
                "hook": hook,
                "action": action["name"],
                "generation": str(generation),
                "evidence": evidence,
                "worst_case": WORST_CASE,
            },
            headers=admin_headers(),
        )
        assert written.status_code == 200, written.text
        return qualification_id

    async def bind(
        self, document: dict[str, Any] | None = None, *, armed: bool = True, hook: str = HOOK
    ) -> int:
        self.document = await self.qualify(document or policy(), hook)
        await write_policy(self.stage, self.hooks, self.document, hook)
        if armed:
            await set_armed(self.stage, self.hooks, True, hook)
        if hook == HOOK:
            self.stage.generation = self.hooks.generation[hook]
        return self.hooks.generation[hook]

    async def nominate(self, *entries: Any, body: bytes | None = None) -> list[dict[str, Any]]:
        return await nominate(self.stage, self.deliveries, *entries, body=body)

    async def one(self, item: Any = None) -> dict[str, Any]:
        (row,) = await self.nominate(item or entry())
        return row

    async def second_hook(self, document: dict[str, Any] | None = None) -> None:
        await asyncio.to_thread(protect_second_hook, self.stage)
        await self.bind(document, hook=HOOK_TWO)

    async def nominate_second_hook(self, *entries: Any) -> list[dict[str, Any]]:
        template = await self.deliveries.event()
        event = await second_hook_event(self.stage, template, self.hooks.generation[HOOK_TWO])
        return await submit(self.stage, event, *entries)


@asynccontextmanager
async def world(
    broker: Any,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    connectors: str | None = None,
) -> AsyncIterator[World]:
    """Remediation and the executor on, a protected hook, both connectors in force."""

    remediation(monkeypatch, enabled=True)
    async with staged(broker, tmp_path, monkeypatch) as stage:
        await deploy(stage, tmp_path, connectors)
        yield World(stage, tmp_path)


def run(scenario: Callable[[], Any], timeout: float = TIMEOUT) -> None:
    asyncio.run(asyncio.wait_for(scenario(), timeout))


# --------------------------------------------------------------------------- #
# Fault injection at the database and Valkey boundaries
# --------------------------------------------------------------------------- #


# The per-agent admission lock (AUTOMATED-REMEDIATION-10): the advisory lock
# whose key names the remediation admission namespace and the agent. Only it is
# made to fail; other advisory locks (task 10's approval identity lock) still
# work, so the fail-closed approval can be raised.
ADMISSION_LOCK = "curie.remediation.admission:"


@contextmanager
def failing_statements(fragment: str) -> Iterator[list[str]]:
    """Every SQL statement containing ``fragment`` fails as an unreachable database would."""

    hits: list[str] = []

    def listener(
        _conn: Any, _cursor: Any, statement: str, params: Any, _context: Any, _many: bool
    ) -> None:
        if fragment in statement:
            hits.append(statement)
            raise OperationalError(statement, params, ConnectionError("injected read failure"))

    sa_event.listen(Engine, "before_cursor_execute", listener)
    try:
        yield hits
    finally:
        sa_event.remove(Engine, "before_cursor_execute", listener)


@contextmanager
def unreadable_kill_switch(monkeypatch: pytest.MonkeyPatch, agent: str) -> Iterator[None]:
    """The agent's kill key cannot be read: every EXISTS or GET of it fails."""

    key = kill_key(uuid.UUID(agent))
    names = {key, key.encode()}
    patched = pytest.MonkeyPatch()
    for method in ("exists", "get"):
        original = getattr(redis.asyncio.Redis, method)

        def failing(self: Any, *keys: Any, _original: Any = original, **kwargs: Any) -> Any:
            if any(k in names for k in keys):
                raise redis.exceptions.ConnectionError("injected kill switch read failure")
            return _original(self, *keys, **kwargs)

        patched.setattr(redis.asyncio.Redis, method, failing)
    try:
        yield
    finally:
        patched.undo()


# =========================================================================== #
# AUTOMATED-REMEDIATION-8: the order, check by check, through real producers
# =========================================================================== #


async def _killed(w: World) -> dict[str, Any]:
    await w.bind()
    await kill(w.stage)
    return await w.one()


async def _generation_absent(w: World) -> dict[str, Any]:
    event = await w.deliveries.event()  # admitted before any policy existed
    await w.bind()
    (row,) = await submit(w.stage, event, entry())
    assert row["admitted_generation"] is None, row
    return row


async def _generation_moved(w: World) -> dict[str, Any]:
    admitted = await w.bind()
    event = await w.deliveries.event()
    current = await write_policy(w.stage, w.hooks, w.document)  # N+1, still armed
    (row,) = await submit(w.stage, event, entry())
    assert (row["admitted_generation"], row["current_generation"]) == (admitted, current), row
    return row


async def _disarmed(w: World) -> dict[str, Any]:
    await w.bind(armed=False)
    return await w.one()


async def _not_automatic(w: World) -> dict[str, Any]:
    await w.bind(policy(automatic=False))
    return await w.one()


async def _prevent(w: World) -> dict[str, Any]:
    await w.bind(policy(kind="prevent", automatic=False))
    return await w.one()


async def _not_independent(w: World) -> dict[str, Any]:
    await w.bind()  # independent when written
    await deploy(w.stage, w.tmp_path, connectors_yaml(shared_secret=True))
    return await w.one()


async def _out_of_range(w: World) -> dict[str, Any]:
    await w.bind()
    return await w.one(entry(replicas=9))


async def _not_literal(w: World) -> dict[str, Any]:
    await w.bind()
    return await w.one(entry(NOT_LITERAL))


async def _not_reversible(w: World) -> dict[str, Any]:
    await w.bind(policy(reversibility="reversible"))  # no capability row at the digest
    return await w.one()


# (case id, producer, expected state, expected code)
ORDER: list[tuple[str, Callable[[World], Any], str, str]] = [
    ("2-killed", _killed, "refused", "agent_stopped"),
    (
        "3-admitted-before-any-policy",
        _generation_absent,
        "approval_requested",
        "generation_not_current",
    ),
    (
        "3-written-after-admission",
        _generation_moved,
        "approval_requested",
        "generation_not_current",
    ),
    ("4-disarmed", _disarmed, "approval_requested", "policy_disarmed"),
    ("5-not-automatic", _not_automatic, "approval_requested", "not_automatic"),
    ("5-prevent-kind", _prevent, "approval_requested", "not_automatic"),
    ("7-not-independent-now", _not_independent, "approval_requested", "verifier_not_independent"),
    ("8-out-of-range", _out_of_range, "approval_requested", "out_of_bounds"),
    ("8-target-not-a-literal-member", _not_literal, "approval_requested", "out_of_bounds"),
    (
        "9-reversible-without-capability",
        _not_reversible,
        "approval_requested",
        "not_reversible_now",
    ),
]


@pytest.mark.parametrize(
    "producer,state,code", [case[1:] for case in ORDER], ids=[case[0] for case in ORDER]
)
def test_each_admission_check_decides_through_its_real_producer(
    ingress_broker: Any,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    producer: Callable[[World], Any],
    state: str,
    code: str,
) -> None:
    """@spec AUTOMATED-REMEDIATION-8 @spec AUTOMATED-REMEDIATION-4 @spec AUTOMATED-REMEDIATION-10
    @spec AUTOMATED-REMEDIATION-17

    Each check failing alone, produced the way production produces it, decides
    the nomination: check 2 ends it ``agent_stopped`` with no approval; checks 3
    to 9 send it to an approval request naming the failed check. No read and no
    forward execution is created for it.
    """

    async def scenario() -> None:
        async with world(ingress_broker, tmp_path, monkeypatch) as w:
            row = await producer(w)
            if state == "refused":
                assert_stopped(row)
            else:
                assert_approval(row, code)
                await assert_requested(row)
            await assert_nothing_executes(row)

    run(scenario)


def test_without_its_qualification_record_an_automatic_action_asks_qualification_missing(
    ingress_broker: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """@spec AUTOMATED-REMEDIATION-8 (check 6) @spec AUTOMATED-REMEDIATION-22

    The real check 6: a policy write cannot bind an automatic action without a
    record (AUTOMATED-REMEDIATION-23), so the record is made unreadable after
    the write; admission fails closed to an approval request
    ``qualification_missing``, never an execution.
    """

    async def scenario() -> None:
        async with world(ingress_broker, tmp_path, monkeypatch) as w:
            await w.bind()
            await q(
                "DELETE FROM curie.remediation_qualifications WHERE agent_id = :a RETURNING id",
                {"a": uuid.UUID(str(w.stage.agent))},
            )
            row = await w.one()
            assert_approval(row, "qualification_missing")
            await assert_nothing_executes(row)

    run(scenario)


def test_a_connector_upgrade_after_qualification_asks_qualification_stale(
    ingress_broker: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """@spec AUTOMATED-REMEDIATION-22 (acceptance) @spec AUTOMATED-REMEDIATION-8 (check 6)

    "deploying a new connector digest makes the same nomination an approval
    request with ``qualification_stale``": the record must be fresh for the
    acting digest in force at admission. Before the upgrade the same nomination
    passes check 6.
    """

    async def scenario() -> None:
        async with world(ingress_broker, tmp_path, monkeypatch) as w:
            await w.bind()
            await assert_pending(await w.one(entry(TARGETS[0])))

            await deploy(w.stage, w.tmp_path, connectors_yaml(act_digest=UPGRADED_ACT_DIGEST))
            row = await w.one(entry(TARGETS[1]))

            assert_approval(row, "qualification_stale")
            await assert_requested(row)
            await assert_nothing_executes(row)

    run(scenario)


def test_a_record_gone_stale_while_precondition_pending_creates_no_execution(
    ingress_broker: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """@spec AUTOMATED-REMEDIATION-8 (checks 6 and 12) @spec AUTOMATED-REMEDIATION-22

    The transition from ``precondition_pending`` to ``admitted`` re-runs check 6
    against the digest in force then: a connector upgrade while the read is
    outstanding sends the nomination to approval ``qualification_stale`` and
    creates no forward execution, though the precondition holds.
    """

    async def scenario() -> None:
        async with world(ingress_broker, tmp_path, monkeypatch) as w:
            await w.bind()
            row = await w.one()
            await assert_pending(row)

            await deploy(w.stage, w.tmp_path, connectors_yaml(act_digest=UPGRADED_ACT_DIGEST))
            after = await take_precondition(w.stage, row, CONDITION_PRESENT)

            assert_approval(after, "qualification_stale")
            assert await forwards(row["id"]) == []
            assert await all_forwards() == []

    run(scenario)


def test_remediation_off_still_refuses_at_the_route_with_no_row(
    ingress_broker: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """@spec AUTOMATED-REMEDIATION-8 (check 1) @spec AUTOMATED-REMEDIATION-1"""

    async def scenario() -> None:
        async with world(ingress_broker, tmp_path, monkeypatch) as w:
            await w.bind()
            before = await q("SELECT id FROM curie.action_executions")
            event = await w.deliveries.event()
            remediation(monkeypatch, enabled=False)
            response = await w.stage.submit(event, block(entry()))
            assert response.status_code == 409, response.text
            assert response.json()["detail"]["code"] == "remediation_disabled"
            assert await q("SELECT id FROM curie.remediation_nominations") == []
            assert await q("SELECT id FROM curie.action_executions") == before

    run(scenario)


def test_with_every_check_passing_one_precondition_read_then_one_forward_execution(
    ingress_broker: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """@spec AUTOMATED-REMEDIATION-8 @spec AUTOMATED-REMEDIATION-9 @spec AUTOMATED-REMEDIATION-13

    Checks 1 to 11 pass: the nomination is ``precondition_pending`` with exactly
    one ``read`` execution of the declared precondition (its connector at the
    in-force digest, tool, canonical arguments and pointer) under the policy
    authority, due now. The read observing the condition admits it: one policy
    forward execution of the nominated call under the generation it was
    admitted under, named on the nomination, which is ``admitted``.
    """

    async def scenario() -> None:
        async with world(ingress_broker, tmp_path, monkeypatch) as w:
            generation = await w.bind()
            row = await w.one()
            assert row["admitted_generation"] == generation, row

            read = await assert_pending(row)
            assert read["kind"] == "read" and read["state"] == "requested", read
            assert read["connector"] == READ_CONNECTOR, read
            assert read["connector_digest"] == READ_DIGEST, read
            assert read["tool"] == READ_TOOL, read
            assert read["forward_arguments"] == PRECONDITION["arguments"], read
            assert read["pointer"] == PRECONDITION["pointer"], read
            assert read["authority_kind"] == "policy", read
            assert read["not_before"] is None or read["not_before"] <= await _db_now(), read

            forward = await admit(w.stage, row)
            assert forward["authority_kind"] == "policy", forward
            assert forward["authority_ref"] == (
                f"policy:{w.stage.agent}:{HOOK}:{generation}:{row['id']}"
            ), forward
            assert forward["idempotency_key"] == f"remediation:{row['id']}:policy", forward
            assert (forward["connector"], forward["tool"]) == (ACT_CONNECTOR, ACT_TOOL), forward
            assert forward["connector_digest"] == ACT_DIGEST, forward
            assert forward["arguments_sha256"] == row["arguments_sha256"], forward
            assert forward["state"] == "requested", forward

    run(scenario)


async def _killed_and_disarmed(w: World) -> dict[str, Any]:
    await w.bind(armed=False)
    await kill(w.stage)
    return await w.one(entry(replicas=9))


async def _absent_and_not_automatic(w: World) -> dict[str, Any]:
    event = await w.deliveries.event()
    await w.bind(policy(automatic=False), armed=False)
    (row,) = await submit(w.stage, event, entry(replicas=9))
    return row


async def _disarmed_and_out_of_bounds(w: World) -> dict[str, Any]:
    await w.bind(policy(automatic=False), armed=False)
    return await w.one(entry(replicas=9))


async def _not_automatic_and_out_of_bounds(w: World) -> dict[str, Any]:
    await w.bind(policy(automatic=False, reversibility="reversible"))
    return await w.one(entry(replicas=9))


async def _out_of_bounds_and_not_reversible(w: World) -> dict[str, Any]:
    await w.bind(policy(reversibility="reversible"))
    return await w.one(entry(replicas=9))


@pytest.mark.parametrize(
    "producer,state,code",
    [
        (_killed_and_disarmed, "refused", "agent_stopped"),
        (_absent_and_not_automatic, "approval_requested", "generation_not_current"),
        (_disarmed_and_out_of_bounds, "approval_requested", "policy_disarmed"),
        (_not_automatic_and_out_of_bounds, "approval_requested", "not_automatic"),
        (_out_of_bounds_and_not_reversible, "approval_requested", "out_of_bounds"),
    ],
    ids=["2-over-4-8", "3-over-4-5-8", "4-over-5-8", "5-over-8-9", "8-over-9"],
)
def test_the_first_failing_check_decides(
    ingress_broker: Any,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    producer: Callable[[World], Any],
    state: str,
    code: str,
) -> None:
    """@spec AUTOMATED-REMEDIATION-8: "the first failing check decides"."""

    async def scenario() -> None:
        async with world(ingress_broker, tmp_path, monkeypatch) as w:
            row = await producer(w)
            if state == "refused":
                assert_stopped(row)
            else:
                assert_approval(row, code)
                await assert_requested(row)
            await assert_nothing_executes(row)

    run(scenario)


def test_a_reversible_action_with_capability_and_custody_passes_check_9(
    ingress_broker: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """@spec AUTOMATED-REMEDIATION-8 (check 9): the capability row records the
    ``restore`` and ``observe_version`` pair at the in-force digest (the probe's
    row, ACTION-EXECUTOR-13) and the version declares the sealing key
    (ACTION-EXECUTOR-16): the reversible action reaches its precondition read.
    """

    async def scenario() -> None:
        async with world(ingress_broker, tmp_path, monkeypatch) as w:
            await asyncio.to_thread(
                sql_rows,
                "INSERT INTO curie.connector_capabilities "
                "(agent_id, connector, digest, restore_capable, observed_at) "
                "VALUES (:agent_id, :connector, :digest, true, now())",
                {
                    "agent_id": uuid.UUID(w.stage.agent),
                    "connector": ACT_CONNECTOR,
                    "digest": ACT_DIGEST,
                },
            )
            await w.bind(policy(reversibility="reversible"))
            await assert_pending(await w.one())

    run(scenario)


# =========================================================================== #
# AUTOMATED-REMEDIATION-9: the precondition read
# =========================================================================== #


def test_a_precondition_read_observing_the_condition_absent_asks_though_the_alert_claimed_it(
    ingress_broker: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """@spec AUTOMATED-REMEDIATION-9: the condition is the read's, never the alert
    body's or the nomination's ``reason``. Observed absent: an approval request
    ``precondition_not_met`` and no forward execution.
    """

    claim_body = b'{"message":"error ratio is 0.97, scale out example-api now"}'

    async def scenario() -> None:
        async with world(ingress_broker, tmp_path, monkeypatch) as w:
            await w.bind()
            (row,) = await w.nominate(
                entry(reason="the alert says the error ratio is 0.97"), body=claim_body
            )
            await assert_pending(row)
            after = await take_precondition(w.stage, row, CONDITION_ABSENT)
            assert_approval(after, "precondition_not_met")
            assert await forwards(row["id"]) == []

    run(scenario)


@pytest.mark.parametrize("refusal", ["sandbox_unavailable", "runner_unavailable"])
def test_a_precondition_read_that_fails_asks_precondition_unavailable(
    ingress_broker: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, refusal: str
) -> None:
    """@spec AUTOMATED-REMEDIATION-9: an unreachable read connector (the read
    execution refused) sends the nomination to approval
    ``precondition_unavailable``; nothing executes.
    """

    async def scenario() -> None:
        async with world(ingress_broker, tmp_path, monkeypatch) as w:
            await w.bind()
            row = await w.one()
            read = await assert_pending(row)
            claimed = await claim_one(w.stage, read["id"])
            ended = await end(w.stage, claimed, "refused", refusal)
            assert ended.status_code == 200, ended.text
            assert_approval(await nomination(row["id"]), "precondition_unavailable")
            assert await forwards(row["id"]) == []

    run(scenario)


async def _absent(w: World, row: dict[str, Any]) -> None:
    after = await take_precondition(w.stage, row, CONDITION_ABSENT)
    assert_approval(after, "precondition_not_met")


async def _unreachable(w: World, row: dict[str, Any]) -> None:
    read = await precondition_read(row["id"])
    claimed = await claim_one(w.stage, read["id"])
    assert (await end(w.stage, claimed, "refused", "sandbox_unavailable")).status_code == 200
    assert_approval(await nomination(row["id"]), "precondition_unavailable")


@pytest.mark.parametrize("ending", [_absent, _unreachable], ids=["not-met", "unavailable"])
def test_the_reservation_is_released_when_the_precondition_does_not_admit(
    ingress_broker: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, ending: Any
) -> None:
    """@spec AUTOMATED-REMEDIATION-9 @spec AUTOMATED-REMEDIATION-10: with a policy
    limit of one per hour, the pending nomination holds the only slot (a second
    target is ``policy_rate_limit``); once its precondition does not admit, the
    slot is free again and the next nomination reaches its precondition read.
    """

    async def scenario() -> None:
        async with world(ingress_broker, tmp_path, monkeypatch) as w:
            await w.bind(policy(limits={"per_policy_per_hour": 1}))
            first = await w.one(entry(TARGETS[0]))
            await assert_pending(first)
            held = await w.one(entry(TARGETS[1]))
            assert_approval(held, "policy_rate_limit")

            await ending(w, first)

            await assert_pending(await w.one(entry(TARGETS[2])))

    run(scenario)


# =========================================================================== #
# AUTOMATED-REMEDIATION-8: the re-check at precondition_pending -> admitted
# =========================================================================== #


async def _disarm(w: World) -> None:
    await set_armed(w.stage, w.hooks, False)


async def _rewrite(w: World) -> None:
    await write_policy(w.stage, w.hooks, w.document)


async def _deploy_shared_secret(w: World) -> None:
    await deploy(w.stage, w.tmp_path, connectors_yaml(shared_secret=True))


async def _kill(w: World) -> None:
    await kill(w.stage)


RECHECK = [
    ("disarmed", _disarm, "approval_requested", "generation_not_current"),
    ("new-generation", _rewrite, "approval_requested", "generation_not_current"),
    ("killed", _kill, "refused", "agent_stopped"),
    ("not-independent", _deploy_shared_secret, "approval_requested", "verifier_not_independent"),
]


@pytest.mark.parametrize("change,state,code", [c[1:] for c in RECHECK], ids=[c[0] for c in RECHECK])
def test_a_change_while_precondition_pending_makes_the_transition_create_no_execution(
    ingress_broker: Any,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    change: Any,
    state: str,
    code: str,
) -> None:
    """@spec AUTOMATED-REMEDIATION-8 @spec AUTOMATED-REMEDIATION-11

    "the transition from ``precondition_pending`` to ``admitted`` re-runs checks
    2 to 11": disarming (a new generation, so check 3 decides), any other policy
    write, killing the agent, or a new in-force version that makes the verifier
    share the acting credential, after admission and before the precondition
    read reports, leaves the read observing the condition and still creates no
    forward execution. The reservation is released (a one-per-hour policy admits
    the next target).
    """

    async def scenario() -> None:
        async with world(ingress_broker, tmp_path, monkeypatch) as w:
            await w.bind(policy(limits={"per_policy_per_hour": 1}))
            row = await w.one(entry(TARGETS[0]))
            await assert_pending(row)

            await change(w)
            after = await take_precondition(w.stage, row, CONDITION_PRESENT)

            if state == "refused":
                assert_stopped(after)
            else:
                assert_approval(after, code)
            assert await forwards(row["id"]) == []
            if change is _disarm or change is _rewrite:
                # The current generation still has to be armed for anything new.
                if change is _disarm:
                    await set_armed(w.stage, w.hooks, True)
                await assert_pending(await w.one(entry(TARGETS[1])))

    run(scenario)


def test_a_breaker_opened_while_precondition_pending_sends_the_transition_to_approval(
    ingress_broker: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """@spec AUTOMATED-REMEDIATION-8 @spec AUTOMATED-REMEDIATION-11: a breaker that
    opens on the target between admission and the precondition read reporting
    (an approved action on the same target ending ``not-recovered``) sends the
    transition to approval ``breaker_open``; no forward execution.
    """

    async def scenario() -> None:
        async with world(ingress_broker, tmp_path, monkeypatch) as w:
            await w.bind()
            asked = await w.one(entry(TARGETS[0], replicas=9))
            assert_approval(asked, "out_of_bounds")
            pending = await w.one(entry(TARGETS[0]))
            read = await assert_pending(pending)
            # Hold the precondition read while the approved action runs.
            await asyncio.to_thread(
                sql_rows,
                "UPDATE curie.action_executions SET not_before = now() + interval '1 hour' "
                "WHERE id = :id",
                {"id": read["id"]},
            )
            approved = await approved_forward(w.stage, asked)
            await run_forward(w.stage, asked, approved, "not-recovered")
            assert [b["closed_at"] for b in await breakers(TARGETS[0])] == [None]
            await asyncio.to_thread(
                sql_rows,
                "UPDATE curie.action_executions SET not_before = now() WHERE id = :id",
                {"id": read["id"]},
            )

            after = await take_precondition(w.stage, pending, CONDITION_PRESENT)

            assert_approval(after, "breaker_open")
            assert await forwards(pending["id"]) == []

    run(scenario)


# =========================================================================== #
# Fail closed: injected read failures never execute
# =========================================================================== #


async def _reversible_with_capability(w: World) -> None:
    await asyncio.to_thread(
        sql_rows,
        "INSERT INTO curie.connector_capabilities "
        "(agent_id, connector, digest, restore_capable, observed_at) "
        "VALUES (:agent_id, :connector, :digest, true, now())",
        {"agent_id": uuid.UUID(w.stage.agent), "connector": ACT_CONNECTOR, "digest": ACT_DIGEST},
    )
    await w.bind(policy(reversibility="reversible"))


# (id, statement fragment, reversible) for reads admission makes at submission.
INITIAL_FAULTS = [
    ("breaker", "remediation_breakers", False),
    ("limit-lock", ADMISSION_LOCK, False),
    ("capability", "connector_capabilities", True),
]


@pytest.mark.parametrize(
    "fragment,reversible", [f[1:] for f in INITIAL_FAULTS], ids=[f[0] for f in INITIAL_FAULTS]
)
def test_an_unreadable_breaker_limit_or_capability_row_at_admission_asks_and_never_executes(
    ingress_broker: Any,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    fragment: str,
    reversible: bool,
) -> None:
    """@spec AUTOMATED-REMEDIATION-8: "An unreadable policy, breaker, limit or
    capability row fails closed to the approval path, never to execution." A
    database error on the read (or the limit lock) yields an approval request
    ``admission_unreadable`` and no read or forward execution.
    """

    async def scenario() -> None:
        async with world(ingress_broker, tmp_path, monkeypatch) as w:
            if reversible:
                await _reversible_with_capability(w)
            else:
                await w.bind()
            event = await w.deliveries.event()
            with failing_statements(fragment) as hits:
                response = await w.stage.submit(event, block(entry()))
            assert hits, f"admission never read {fragment}"
            assert response.status_code == 200, response.text
            (row,) = [await nomination(i) for i in response.json()["nomination_ids"]]
            assert_approval(row, "admission_unreadable")
            await assert_nothing_executes(row)

    run(scenario)


# Reads the re-check makes at the transition (the policy too: it is read again).
TRANSITION_FAULTS = [
    ("policy", "remediation_policy_generations", False),
    ("breaker", "remediation_breakers", False),
    ("limit-lock", ADMISSION_LOCK, False),
    ("capability", "connector_capabilities", True),
]


@pytest.mark.parametrize(
    "fragment,reversible",
    [f[1:] for f in TRANSITION_FAULTS],
    ids=[f[0] for f in TRANSITION_FAULTS],
)
def test_an_unreadable_row_at_the_transition_asks_and_never_executes(
    ingress_broker: Any,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    fragment: str,
    reversible: bool,
) -> None:
    """@spec AUTOMATED-REMEDIATION-8: the re-check fails closed the same way; the
    precondition held, yet the nomination goes to approval and no forward
    execution exists.
    """

    async def scenario() -> None:
        async with world(ingress_broker, tmp_path, monkeypatch) as w:
            if reversible:
                await _reversible_with_capability(w)
            else:
                await w.bind()
            row = await w.one()
            read = await assert_pending(row)
            claimed = await claim_one(w.stage, read["id"])
            with failing_statements(fragment) as hits:
                await report(w.stage, claimed, CONDITION_PRESENT)
            assert hits, f"the transition never read {fragment}"
            after = await nomination(row["id"])
            assert after["state"] == "approval_requested", after
            if fragment == "remediation_policy_generations":
                assert after["approval_reason"] in {
                    "admission_unreadable",
                    "precondition_unavailable",
                }, after
            else:
                assert after["approval_reason"] == "admission_unreadable", after
            assert after["execution_id"] is None, after
            assert await forwards(row["id"]) == []

    run(scenario)


@pytest.mark.parametrize("phase", ["admission", "transition"])
def test_an_unreadable_kill_switch_ends_the_nomination_agent_stopped(
    ingress_broker: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, phase: str
) -> None:
    """@spec AUTOMATED-REMEDIATION-8 (check 2): the switch is read through the key
    the worker reads; unreadable is ``agent_stopped`` (no approval), at
    admission and at the re-check, and nothing executes.
    """

    async def scenario() -> None:
        async with world(ingress_broker, tmp_path, monkeypatch) as w:
            await w.bind()
            if phase == "admission":
                event = await w.deliveries.event()
                with unreadable_kill_switch(monkeypatch, w.stage.agent):
                    response = await w.stage.submit(event, block(entry()))
                assert response.status_code == 200, response.text
                row = await nomination(response.json()["nomination_ids"][0])
            else:
                pending = await w.one()
                read = await assert_pending(pending)
                claimed = await claim_one(w.stage, read["id"])
                with unreadable_kill_switch(monkeypatch, w.stage.agent):
                    await report(w.stage, claimed, CONDITION_PRESENT)
                row = await nomination(pending["id"])
            assert_stopped(row)
            assert await forwards(row["id"]) == []

    run(scenario)


# =========================================================================== #
# AUTOMATED-REMEDIATION-10: limits
# =========================================================================== #


def test_a_fourth_admissible_nomination_within_the_hour_asks_policy_rate_limit(
    ingress_broker: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """@spec AUTOMATED-REMEDIATION-10: at most three automatic actions per policy
    per rolling hour (ruling 9), reserved at admission: three deliveries on three
    targets reach their precondition reads, the fourth is ``policy_rate_limit``.
    """

    async def scenario() -> None:
        async with world(ingress_broker, tmp_path, monkeypatch) as w:
            await w.bind()
            for target in TARGETS[:3]:
                await assert_pending(await w.one(entry(target)))
            fourth = await w.one(entry(TARGETS[3]))
            assert_approval(fourth, "policy_rate_limit")
            await assert_nothing_executes(fourth)

    run(scenario)


def test_a_policy_limit_ages_out_after_the_rolling_hour(
    ingress_broker: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """@spec AUTOMATED-REMEDIATION-10: the hour is rolling. Three admitted
    actions hold the policy limit; once they are more than an hour old a fifth
    target is admissible again.
    """

    async def scenario() -> None:
        async with world(ingress_broker, tmp_path, monkeypatch) as w:
            await w.bind()
            # All three reach their precondition reads before any forward
            # exists, so the claim route (oldest due first) hands out each
            # read in turn rather than an earlier action's forward.
            rows = [await w.one(entry(target)) for target in TARGETS[:3]]
            for row in rows:
                await assert_pending(row)
            for row in rows:
                await admit(w.stage, row)
            assert_approval(await w.one(entry(TARGETS[3])), "policy_rate_limit")
            await age(3700)
            await assert_pending(await w.one(entry(TARGETS[4])))

    run(scenario)


def test_a_declared_per_action_limit_asks_action_rate_limit(
    ingress_broker: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """@spec AUTOMATED-REMEDIATION-10: "per-action hourly limits apply when declared"."""

    async def scenario() -> None:
        async with world(ingress_broker, tmp_path, monkeypatch) as w:
            await w.bind(policy(limits={"per_action_per_hour": 1}))
            await assert_pending(await w.one(entry(TARGETS[0])))
            second = await w.one(entry(TARGETS[1]))
            assert_approval(second, "action_rate_limit")

    run(scenario)


def test_two_nominations_in_one_turn_yield_one_admission_and_one_turn_limit(
    ingress_broker: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """@spec AUTOMATED-REMEDIATION-10: "at most one automatic action leaves one
    turn": two admissible entries of one block on two targets yield one
    precondition read (then one forward execution) and one approval request
    ``turn_limit``.
    """

    async def scenario() -> None:
        async with world(ingress_broker, tmp_path, monkeypatch) as w:
            await w.bind()
            first, second = await w.nominate(entry(TARGETS[0]), entry(TARGETS[1]))
            await assert_pending(first)
            assert_approval(second, "turn_limit")
            await admit(w.stage, first)
            assert len(await all_forwards()) == 1

    run(scenario)


def test_a_second_automatic_action_on_a_live_target_asks_target_live(
    ingress_broker: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """@spec AUTOMATED-REMEDIATION-10: "at most one automatic action per target is
    live (not finished verifying)": while the first is reserved and pending, a
    second delivery's nomination on the same target is ``target_live``.
    """

    async def scenario() -> None:
        async with world(ingress_broker, tmp_path, monkeypatch) as w:
            await w.bind()
            await assert_pending(await w.one(entry(TARGETS[0], replicas=3)))
            second = await w.one(entry(TARGETS[0], replicas=4))
            assert_approval(second, "target_live")

    run(scenario)


def test_two_hooks_racing_for_one_target_yield_exactly_one_execution(
    ingress_broker: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """@spec AUTOMATED-REMEDIATION-10: counts and the reservation are taken under
    one transaction-scoped advisory lock keyed by the agent, so two concurrent
    admissions from two hooks of one agent on one target (real Postgres) cannot
    both take it: exactly one reaches its precondition read and then the one
    forward execution, the other asks with a check 11 reason.
    """

    async def scenario() -> None:
        async with world(ingress_broker, tmp_path, monkeypatch) as w:
            await w.bind()
            await w.second_hook()
            template = await w.deliveries.event()
            hook_two_event = await second_hook_event(
                w.stage, template, w.hooks.generation[HOOK_TWO]
            )
            hook_one_event = await w.deliveries.event()

            results = await asyncio.gather(
                submit(w.stage, hook_one_event, entry(TARGETS[0], replicas=3)),
                submit(w.stage, hook_two_event, entry(TARGETS[0], replicas=4)),
            )
            rows = [rs[0] for rs in results]
            pending = [r for r in rows if r["state"] == "precondition_pending"]
            asked = [r for r in rows if r["state"] == "approval_requested"]
            assert len(pending) == 1 and len(asked) == 1, rows
            assert asked[0]["approval_reason"] in CHECK_11, asked
            await assert_nothing_executes(asked[0])
            await admit(w.stage, pending[0])
            assert len(await all_forwards()) == 1

    run(scenario, timeout=180)


def test_a_target_equivalent_to_but_not_literally_in_the_list_asks_and_the_literal_admits(
    ingress_broker: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """@spec AUTOMATED-REMEDIATION-10: the target value must be a literal member of
    the action's allowed list; the decomposed spelling of a listed name is out of
    bounds, the listed spelling is admitted, and their target keys differ.
    """

    async def scenario() -> None:
        async with world(ingress_broker, tmp_path, monkeypatch) as w:
            await w.bind()
            equivalent = await w.one(entry(NOT_LITERAL))
            assert_approval(equivalent, "out_of_bounds")
            literal = await w.one(entry(LISTED_COMPOSED))
            await assert_pending(literal)
            assert literal["target"] == target_key(LISTED_COMPOSED), literal
            assert equivalent["target"] != literal["target"], (equivalent, literal)

    run(scenario)


# =========================================================================== #
# AUTOMATED-REMEDIATION-10: the incident window (maintainer ruling, 2026-10-07)
# =========================================================================== #


async def _verified_on(w: World, target: str) -> dict[str, Any]:
    row = await w.one(entry(target))
    await assert_pending(row)
    forward = await admit(w.stage, row)
    await run_forward(w.stage, row, forward, "verified")
    return row


def test_a_second_automatic_action_on_a_target_within_the_window_asks_incident_limit(
    ingress_broker: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """@spec AUTOMATED-REMEDIATION-10: one automatic action per incident per
    target. After an action on a target verified, a later delivery with the
    same alert body asks ``incident_limit`` on that target, while another target
    is admitted: the incident is the target's window, not the alert's.
    """

    async def scenario() -> None:
        async with world(ingress_broker, tmp_path, monkeypatch) as w:
            await w.bind()
            await _verified_on(w, TARGETS[0])
            again = await w.one(entry(TARGETS[0]))
            assert_approval(again, "incident_limit")
            await assert_pending(await w.one(entry(TARGETS[1])))

    run(scenario)


@pytest.mark.parametrize(
    "window,still_open,closed",
    [(None, 3500, 3700), (7200, 7100, 7300)],
    ids=["default", "lengthened"],
)
def test_the_incident_stays_open_for_its_window_after_verification_finished(
    ingress_broker: Any,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    window: int | None,
    still_open: int,
    closed: int,
) -> None:
    """@spec AUTOMATED-REMEDIATION-10: the incident stays open for
    ``incident_window_seconds`` (default 3600; a policy may only lengthen it)
    after the action's verification finished; then the target is admissible.
    """

    async def scenario() -> None:
        async with world(ingress_broker, tmp_path, monkeypatch) as w:
            limits = {} if window is None else {"incident_window_seconds": window}
            await w.bind(policy(limits=limits))
            await _verified_on(w, TARGETS[0])

            await age(still_open)
            assert_approval(await w.one(entry(TARGETS[0], replicas=3)), "incident_limit")
            await age(closed - still_open)
            await assert_pending(await w.one(entry(TARGETS[0], replicas=4)))

    run(scenario)


def test_a_window_below_the_default_is_refused_at_write(
    ingress_broker: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """@spec AUTOMATED-REMEDIATION-10: "a policy declaring a window below the
    default is refused at write", and so is a looser rate limit.
    """

    async def scenario() -> None:
        async with world(ingress_broker, tmp_path, monkeypatch) as w:
            for limits in ({"incident_window_seconds": 1800}, {"per_policy_per_hour": 4}):
                response = await w.stage.client.put(
                    _base(w.stage),
                    json={
                        "expected_generation": "0",
                        "operation_id": str(uuid.uuid4()),
                        "policy": policy(limits=limits),
                    },
                    headers=admin_headers(),
                )
                assert response.status_code == 422, response.text
                assert response.json()["detail"]["code"] == "policy_limit_out_of_bounds"

    run(scenario)


def test_another_hook_cannot_act_inside_a_targets_incident_window(
    ingress_broker: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """@spec AUTOMATED-REMEDIATION-10: the window is looked up per agent and target
    key, never per policy or hook: after hook one's action verified on a target,
    hook two's own armed policy asks ``incident_limit`` there.
    """

    async def scenario() -> None:
        async with world(ingress_broker, tmp_path, monkeypatch) as w:
            await w.bind()
            await w.second_hook()
            await _verified_on(w, TARGETS[0])
            (row,) = await w.nominate_second_hook(entry(TARGETS[0]))
            assert row["hook"] == HOOK_TWO, row
            assert_approval(row, "incident_limit")
            (other,) = await w.nominate_second_hook(entry(TARGETS[1]))
            await assert_pending(other)

    run(scenario)


def test_an_approved_action_opens_the_incident_too(
    ingress_broker: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """@spec AUTOMATED-REMEDIATION-10: the incident "opens with the first action
    executed on it under this contract, automatic or approved": an approved
    action verified on a target makes the next automatic one there ask
    ``incident_limit``.
    """

    async def scenario() -> None:
        async with world(ingress_broker, tmp_path, monkeypatch) as w:
            await w.bind()
            asked = await w.one(entry(TARGETS[0], replicas=9))
            assert_approval(asked, "out_of_bounds")
            approved = await approved_forward(w.stage, asked)
            await run_forward(w.stage, asked, approved, "verified")

            assert_approval(await w.one(entry(TARGETS[0])), "incident_limit")

    run(scenario)


# =========================================================================== #
# AUTOMATED-REMEDIATION-11: the breaker
# =========================================================================== #


@pytest.mark.parametrize("outcome", ["not-recovered", "failed", "indeterminate"])
def test_an_outcome_other_than_verified_opens_a_breaker_that_sends_the_next_nomination_to_approval(
    ingress_broker: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, outcome: str
) -> None:
    """@spec AUTOMATED-REMEDIATION-11: a ``remediation_breakers`` row keyed by
    agent, connector, tool and target key opens on a ``not-recovered`` outcome
    and on an execution that ended ``failed`` or ``indeterminate``; the next
    nomination for that action and target, from any hook, asks ``breaker_open``.
    """

    async def scenario() -> None:
        async with world(ingress_broker, tmp_path, monkeypatch) as w:
            await w.bind()
            await w.second_hook()
            row = await w.one(entry(TARGETS[0]))
            await assert_pending(row)
            forward = await admit(w.stage, row)
            await run_forward(w.stage, row, forward, outcome)

            (breaker,) = await breakers()
            assert str(breaker["agent_id"]) == w.stage.agent, breaker
            assert (breaker["connector"], breaker["tool"]) == (ACT_CONNECTOR, ACT_TOOL), breaker
            assert breaker["target"] == target_key(TARGETS[0]), breaker
            assert breaker["opened_at"] is not None and breaker["closed_at"] is None, breaker

            assert_approval(await w.one(entry(TARGETS[0])), "breaker_open")
            (from_hook_two,) = await w.nominate_second_hook(entry(TARGETS[0]))
            assert_approval(from_hook_two, "breaker_open")
            # A nomination never closes it.
            assert [b["closed_at"] for b in await breakers()] == [None]

    run(scenario)


def test_a_verified_outcome_opens_no_breaker(
    ingress_broker: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """@spec AUTOMATED-REMEDIATION-11: only an outcome other than ``verified`` opens one."""

    async def scenario() -> None:
        async with world(ingress_broker, tmp_path, monkeypatch) as w:
            await w.bind()
            await _verified_on(w, TARGETS[0])
            assert await breakers() == []

    run(scenario)


def test_an_approved_action_that_does_not_recover_opens_the_breaker_too(
    ingress_broker: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """@spec AUTOMATED-REMEDIATION-11: "whether the action ran under the policy or
    under an approval"."""

    async def scenario() -> None:
        async with world(ingress_broker, tmp_path, monkeypatch) as w:
            await w.bind()
            asked = await w.one(entry(TARGETS[0], replicas=9))
            approved = await approved_forward(w.stage, asked)
            await run_forward(w.stage, asked, approved, "not-recovered")
            assert [b["closed_at"] for b in await breakers(TARGETS[0])] == [None]
            await age(3700)  # past the incident the approved action opened
            assert_approval(await w.one(entry(TARGETS[0])), "breaker_open")

    run(scenario)


def _close_url(stage: Stage, breaker_id: Any, hook: str = HOOK) -> str:
    return f"{_base(stage, hook)}/breakers/{breaker_id}/close"


def _hook_signed(agent: str, body: bytes) -> dict[str, str]:
    """A valid protected hook source signature, and no platform credential."""

    secret = hook_source_signing.derive(
        get_settings().api_key, agent_id=agent, hook=HOOK, generation=1
    )
    timestamp = str(int(time.time()))
    delivery = uuid.uuid4().hex
    return {
        hook_signing.TIMESTAMP_HEADER: timestamp,
        hook_signing.DELIVERY_HEADER: delivery,
        hook_signing.SIGNATURE_HEADER: hook_signing.sign(
            secret,
            timestamp=timestamp,
            delivery_id=delivery,
            hook=HOOK,
            tool_access="read-only",
            body=body,
        ),
        "Content-Type": "application/json",
    }


def test_only_the_administrative_route_with_an_operator_principal_closes_a_breaker(
    ingress_broker: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """@spec AUTOMATED-REMEDIATION-11 @spec AUTOMATED-REMEDIATION-3

    ``POST .../remediation-policy/breakers/{breaker_id}/close``: the hook source
    key and the worker token are ``401``; the platform key alone is refused
    ``operator_principal_required``; an unknown breaker is ``404``; each leaves
    the breaker open. With the platform key and an operator principal and a
    reason it closes, recording the principal as the closing actor and the
    reason, and (once the incident has passed) automatic admission resumes on
    that target.
    """

    reason = "connector fixed and redeployed"

    async def scenario() -> None:
        async with world(ingress_broker, tmp_path, monkeypatch) as w:
            await w.bind()
            row = await w.one(entry(TARGETS[0]))
            await assert_pending(row)
            await run_forward(w.stage, row, await admit(w.stage, row), "not-recovered")
            (breaker,) = await breakers()
            url = _close_url(w.stage, breaker["id"])
            body = json.dumps({"reason": reason}).encode()

            hook_key = await w.stage.client.post(
                url, content=body, headers=_hook_signed(w.stage.agent, body)
            )
            assert hook_key.status_code == 401, hook_key.text
            worker = await w.stage.client.post(
                url, json={"reason": reason}, headers=worker_headers()
            )
            assert worker.status_code == 401, worker.text
            platform_only = await w.stage.client.post(
                url, json={"reason": reason}, headers=_platform()
            )
            assert platform_only.status_code == 403, platform_only.text
            assert platform_only.json()["detail"]["code"] == "operator_principal_required"
            unknown = await w.stage.client.post(
                _close_url(w.stage, uuid.uuid4()), json={"reason": reason}, headers=admin_headers()
            )
            assert unknown.status_code == 404, unknown.text
            other_hook = await w.stage.client.post(
                _close_url(w.stage, breaker["id"], HOOK_TWO),
                json={"reason": reason},
                headers=admin_headers(),
            )
            assert other_hook.status_code == 404, other_hook.text
            no_reason = await w.stage.client.post(url, json={}, headers=admin_headers())
            assert no_reason.status_code == 422, no_reason.text
            assert [b["closed_at"] for b in await breakers()] == [None]

            closed = await w.stage.client.post(
                url, json={"reason": reason}, headers=admin_headers()
            )

            assert closed.status_code == 200, closed.text
            assert closed.headers.get("cache-control") == "no-store"
            assert closed.json()["id"] == str(breaker["id"]), closed.text
            assert closed.json()["closed_by"] == OPERATOR, closed.text
            (after,) = await breakers()
            assert after["closed_at"] is not None, after
            assert after["closed_by"] == OPERATOR, after
            assert after["close_reason"] == reason, after

            await age(3700)  # past the incident window of the failed action
            await assert_pending(await w.one(entry(TARGETS[0])))

    run(scenario)


# =========================================================================== #
# AUTOMATED-REMEDIATION-11, executor amendment E8: re-validated at claim
# =========================================================================== #


async def _admitted_and_held(w: World) -> tuple[dict[str, Any], dict[str, Any]]:
    row = await w.one(entry(TARGETS[0]))
    await assert_pending(row)
    forward = await admit(w.stage, row)
    return row, forward


async def assert_policy_changed(w: World, row: dict[str, Any], forward: dict[str, Any]) -> None:
    """Refused ``policy_changed`` before any sandbox claim; back to approval."""

    ledger_before = await ledger_rows()
    claimed = await claim(w.stage)
    assert claimed.status_code in (200, 204), claimed.text
    if claimed.status_code == 200:
        assert claimed.json()["id"] != str(forward["id"]), "a changed authority is never handed out"
    (after,) = await q(
        "SELECT * FROM curie.action_executions WHERE id = :id", {"id": forward["id"]}
    )
    assert after["state"] == "refused", after
    assert after["refusal_code"] == "policy_changed", after
    assert after["dispatched_at"] is None, after
    assert after["subject_action_id"] is None, after
    assert await ledger_rows() == ledger_before
    nominated = await nomination(row["id"])
    assert nominated["state"] == "approval_requested", nominated
    assert nominated["approval_reason"] == "policy_changed", nominated
    await assert_requested(nominated)
    assert nominated["verification_outcome"] is None, nominated
    assert nominated["execution_code"] is None, nominated
    assert [r for r in await executions_of(row["id"], "read") if r["state"] == "requested"] == []


@pytest.mark.parametrize("change", ["disarm", "new-generation"])
def test_a_policy_change_after_creation_refuses_the_execution_policy_changed_at_claim(
    ingress_broker: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, change: str
) -> None:
    """@spec AUTOMATED-REMEDIATION-11 (executor amendment E8): disarming (or any
    write) after the forward execution is created and before it is claimed:
    the claim route's remediation authority hook ends it ``refused``
    ``policy_changed`` with no lease, no dispatch and no ledger record, and the
    nomination goes to approval with no outcome.
    """

    async def scenario() -> None:
        async with world(ingress_broker, tmp_path, monkeypatch) as w:
            await w.bind()
            row, forward = await _admitted_and_held(w)
            if change == "disarm":
                await set_armed(w.stage, w.hooks, False)
            else:
                await write_policy(w.stage, w.hooks, w.document)
            await assert_policy_changed(w, row, forward)

    run(scenario)


def test_a_breaker_opened_after_creation_refuses_the_execution_policy_changed_at_claim(
    ingress_broker: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """@spec AUTOMATED-REMEDIATION-11 (executor amendment E8): "no breaker is open
    for its target key" at claim. An approved action on the same target ends
    ``not-recovered`` while the policy forward waits; its claim is refused
    ``policy_changed``.
    """

    async def scenario() -> None:
        async with world(ingress_broker, tmp_path, monkeypatch) as w:
            await w.bind()
            asked = await w.one(entry(TARGETS[0], replicas=9))
            row, forward = await _admitted_and_held(w)
            await asyncio.to_thread(
                sql_rows,
                "UPDATE curie.action_executions SET not_before = now() + interval '1 hour' "
                "WHERE id = :id",
                {"id": forward["id"]},
            )
            approved = await approved_forward(w.stage, asked)
            await run_forward(w.stage, asked, approved, "not-recovered")
            await asyncio.to_thread(
                sql_rows,
                "UPDATE curie.action_executions SET not_before = NULL WHERE id = :id",
                {"id": forward["id"]},
            )
            await assert_policy_changed(w, row, forward)

    run(scenario)


def test_an_approval_authority_execution_is_claimed_whatever_the_policy_state(
    ingress_broker: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """@spec AUTOMATED-REMEDIATION-11 (E8): "for ``authority_kind`` ``policy``
    only"; an approved execution is claimed exactly as AE-17 says.
    """

    async def scenario() -> None:
        async with world(ingress_broker, tmp_path, monkeypatch) as w:
            await w.bind()
            asked = await w.one(entry(TARGETS[0], replicas=9))
            approved = await approved_forward(w.stage, asked)
            await set_armed(w.stage, w.hooks, False)
            claimed = await claim_one(w.stage, approved["id"])
            assert claimed["state"] == "claimed", claimed

    run(scenario)


def test_with_the_policy_unchanged_the_policy_execution_is_claimed(
    ingress_broker: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """@spec AUTOMATED-REMEDIATION-11 (E8): the hook passes when the generation is
    current and armed and no breaker is open."""

    async def scenario() -> None:
        async with world(ingress_broker, tmp_path, monkeypatch) as w:
            await w.bind()
            _row, forward = await _admitted_and_held(w)
            claimed = await claim_one(w.stage, forward["id"])
            assert claimed["state"] == "claimed", claimed

    run(scenario)


# =========================================================================== #
# Review round 1: the approval's reply surface is the delivery's own
# (AUTOMATED-REMEDIATION-15), and owed approvals never starve
# =========================================================================== #

SECOND_REPLY = {"kind": "slack", "address": "C0EXAMPLE3"}
EMAIL_REPLY = {
    "kind": "email",
    "address": "support@example.test",
    "endpoint": "http://adapter.example.test",
    "adapter": "mail",
}
REPLY_COLUMNS = ("reply_kind", "reply_channel", "reply_endpoint", "reply_adapter")


async def add_reply_channel(stage: Stage) -> None:
    """A second reply channel: the hook route now needs the delivery to select one."""

    added = await stage.client.post(
        f"/agents/{stage.agent}/channels", json=dict(SECOND_REPLY), headers=_platform()
    )
    assert added.status_code in (200, 201), added.text


async def submission(event: str) -> dict[str, Any]:
    (row,) = await q(
        "SELECT * FROM curie.remediation_nomination_submissions WHERE event_id = :e", {"e": event}
    )
    return row


async def approval_of(row: dict[str, Any]) -> dict[str, Any]:
    fresh = await nomination(row["id"])
    assert fresh["approval_id"] is not None, fresh
    (approval,) = await q(
        "SELECT * FROM curie.approvals WHERE id = :id", {"id": fresh["approval_id"]}
    )
    return approval


@pytest.mark.parametrize(
    "selected", [SECOND_REPLY, EMAIL_REPLY], ids=["second-channel", "first-channel"]
)
def test_an_agent_with_two_reply_channels_asks_on_the_deliverys_own_surface(
    ingress_broker: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, selected: Any
) -> None:
    """@spec AUTOMATED-REMEDIATION-15: ``reply_kind`` and ``reply_channel`` (and the
    nullable reply columns) are "copied from the protected delivery's
    ``QueuedTurn``". With two reply channels the hook route makes the delivery
    select one (``?kind=&address=``); that surface is recorded on the
    submission row, next to ``conversation_id``, at nomination time
    (``reply_kind``, ``reply_channel``, ``reply_endpoint``, ``reply_adapter``),
    and the approval is raised on it, never guessed from the agent's channels.
    """

    async def scenario() -> None:
        async with world(ingress_broker, tmp_path, monkeypatch) as w:
            await add_reply_channel(w.stage)
            await w.bind(policy(automatic=False))
            params = {"kind": selected["kind"], "address": selected["address"]}
            if "adapter" in selected:
                params["adapter"] = selected["adapter"]
            event = await w.deliveries.event(params=params)
            (row,) = await submit(w.stage, event, entry())

            recorded = await submission(event)
            assert recorded["conversation_id"] is not None, recorded
            assert recorded["reply_kind"] == selected["kind"], recorded
            assert recorded["reply_channel"] == selected["address"], recorded
            assert recorded["reply_endpoint"] == selected.get("endpoint"), recorded
            assert recorded["reply_adapter"] in (selected.get("adapter"), "default"), recorded

            assert_approval(row, "not_automatic")
            approval = await approval_of(row)
            assert approval["reply_kind"] == selected["kind"], approval
            assert approval["reply_channel"] == selected["address"], approval
            assert approval["conversation_id"] == recorded["conversation_id"], approval

    run(scenario)


def test_the_surface_is_the_deliverys_even_after_the_agents_channels_change(
    ingress_broker: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """@spec AUTOMATED-REMEDIATION-15: the handle is the delivery's, recorded at
    nomination time. A delivery that selected the second channel is asked on
    it although that channel was removed before the block was submitted,
    leaving the agent one channel that a guess would pick.
    """

    async def scenario() -> None:
        async with world(ingress_broker, tmp_path, monkeypatch) as w:
            await add_reply_channel(w.stage)
            await w.bind(policy(automatic=False))
            event = await w.deliveries.event(
                params={"kind": SECOND_REPLY["kind"], "address": SECOND_REPLY["address"]}
            )
            removed = await w.stage.client.request(
                "DELETE",
                f"/agents/{w.stage.agent}/channels",
                params={"kind": SECOND_REPLY["kind"], "address": SECOND_REPLY["address"]},
                headers=_platform(),
            )
            assert removed.status_code in (200, 204), removed.text

            (row,) = await submit(w.stage, event, entry())

            assert_approval(row, "not_automatic")
            approval = await approval_of(row)
            assert (approval["reply_kind"], approval["reply_channel"]) == (
                SECOND_REPLY["kind"],
                SECOND_REPLY["address"],
            ), approval

    run(scenario)


async def _reconcile_once(w: World) -> int:
    """One pass of the API's admission reconciler, as the expiry sweeper runs it."""

    admission = importlib.import_module("curie_api.remediation_admission")
    from curie_api.killswitch import KillSwitch
    from curie_api.storage import BundleStore

    engine = create_async_engine(get_settings().database_url)
    valkey = redis.asyncio.from_url(get_settings().valkey_dsn())
    try:
        async with async_sessionmaker(engine, expire_on_commit=False)() as session:
            handled: int = await admission.reconcile_admissions(
                session, BundleStore(get_settings()), KillSwitch(valkey)
            )
            return handled
    finally:
        await valkey.aclose()
        await engine.dispose()


async def _owed(
    w: World,
    *,
    action: str = ACTION_NAME,
    reply: dict[str, Any] | None = EMAIL_REPLY,
    target: str = TARGETS[0],
    age_seconds: int = 0,
) -> uuid.UUID:
    """A nomination sent to approval whose approval is still owed.

    The state admission leaves when raising the approval failed after the
    decision (``approval_requested`` with its reason and no approval): its
    submission recorded ``reply`` (None: no surface), and ``action`` names a
    declared action or, otherwise, one no generation declares (never raisable).
    """

    event = f"hook-{w.stage.agent}-{HOOK}-{uuid.uuid4().hex[:16]}"
    columns = {
        "reply_kind": None,
        "reply_channel": None,
        "reply_endpoint": None,
        "reply_adapter": None,
    }
    if reply is not None:
        columns.update(
            reply_kind=reply["kind"],
            reply_channel=reply["address"],
            reply_endpoint=reply.get("endpoint"),
            reply_adapter=reply.get("adapter"),
        )
    await asyncio.to_thread(
        sql_rows,
        "INSERT INTO curie.remediation_nomination_submissions "
        "(event_id, agent_id, hook, block_sha256, conversation_id, reply_kind, reply_channel, "
        "reply_endpoint, reply_adapter) VALUES (:e, :agent, :hook, :sha, :conversation, "
        ":reply_kind, :reply_channel, :reply_endpoint, :reply_adapter)",
        {
            "e": event,
            "agent": uuid.UUID(w.stage.agent),
            "hook": HOOK,
            "sha": "cd" * 32,
            "conversation": hook_conversation_id(uuid.UUID(w.stage.agent), HOOK, None),
            **columns,
        },
    )
    arguments = {"deployment": target, "namespace": "example-ns", "replicas": 4}
    text = json.dumps(arguments, sort_keys=True, separators=(",", ":"))
    nomination_id = uuid.uuid4()
    await asyncio.to_thread(
        sql_rows,
        "INSERT INTO curie.remediation_nominations "
        "(id, agent_id, hook, event_id, admitted_generation, current_generation, action, kind, "
        "arguments, arguments_sha256, target, state, approval_reason, created_at, decided_at) "
        "VALUES (:id, :agent, :hook, :e, :g, :g, :action, 'remediate', :arguments, :sha, "
        ":target, 'approval_requested', 'not_automatic', "
        "now() - make_interval(secs => :age), now() - make_interval(secs => :age))",
        {
            "id": nomination_id,
            "agent": uuid.UUID(w.stage.agent),
            "hook": HOOK,
            "e": event,
            "g": w.hooks.generation[HOOK],
            "action": action,
            "arguments": text,
            "sha": hashlib.sha256(text.encode()).hexdigest(),
            "target": target_key(target),
            "age": age_seconds,
        },
    )
    return nomination_id


def test_a_nomination_whose_delivery_left_no_reply_surface_ends_visibly_with_a_named_code(
    ingress_broker: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """@spec AUTOMATED-REMEDIATION-15 @spec AUTOMATED-REMEDIATION-8: when the
    delivery's reply surface is genuinely missing there is nowhere to ask, so
    the nomination does not sit ``approval_requested`` with no approval, card
    or expiry: the next reconciler pass ends it ``refused`` with
    ``reply_surface_unavailable`` (a frozen nomination refusal), ``decided_at``
    set, no approval and no execution, and later passes leave it alone.
    """

    async def scenario() -> None:
        async with world(ingress_broker, tmp_path, monkeypatch) as w:
            await add_reply_channel(w.stage)  # two channels: nothing to guess from
            await w.bind(policy(automatic=False))
            stranded = await _owed(w, reply=None, age_seconds=600)

            await _reconcile_once(w)

            row = await nomination(stranded)
            assert row["state"] == "refused", row
            assert row["refusal_code"] == "reply_surface_unavailable", row
            assert row["approval_id"] is None and row["execution_id"] is None, row
            assert row["decided_at"] is not None, row
            assert await q("SELECT id FROM curie.approvals") == []
            await _reconcile_once(w)
            assert (await nomination(stranded))["state"] == "refused"

    run(scenario)


def test_reply_surface_unavailable_is_a_frozen_nomination_refusal() -> None:
    """@spec AUTOMATED-REMEDIATION-8: the code is in the closed vocabulary the
    API, worker and CLI share (``nomination_refusals``)."""

    codes = json.loads(
        (
            Path(__file__).resolve().parents[3] / "tests" / "vectors" / "remediation-codes.json"
        ).read_text("utf-8")
    )
    assert "reply_surface_unavailable" in codes["nomination_refusals"]


def test_unraisable_owed_approvals_do_not_starve_a_raisable_one(
    ingress_broker: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """@spec AUTOMATED-REMEDIATION-8 @spec AUTOMATED-REMEDIATION-15: 105 owed
    approvals that can never be raised (their action is declared by no
    generation), all older than one that can, do not stop the next reconciler
    pass from raising the newer one's approval.
    """

    async def scenario() -> None:
        async with world(ingress_broker, tmp_path, monkeypatch) as w:
            await w.bind(policy(automatic=False))
            for index in range(105):
                await _owed(w, action="withdrawn-action", age_seconds=7200 - index)
            raisable = await _owed(w, age_seconds=60)

            await _reconcile_once(w)

            row = await nomination(raisable)
            assert row["state"] == "approval_requested", row
            assert row["approval_id"] is not None, row
            approval = await approval_of(row)
            assert approval["purpose"] == "remediation", approval
            assert (approval["reply_kind"], approval["reply_channel"]) == (
                EMAIL_REPLY["kind"],
                EMAIL_REPLY["address"],
            ), approval

    run(scenario, timeout=240)


# =========================================================================== #
# Review round 2: the reply surface is recorded before the broker admits
# (AUTOMATED-REMEDIATION-15, -6), through the real signed hook route
# =========================================================================== #

SURFACE_INSERT = "INSERT INTO curie.remediation_delivery_surfaces"


async def surface_rows(event: str) -> list[dict[str, Any]]:
    return await q(
        "SELECT * FROM curie.remediation_delivery_surfaces WHERE event_id = :e", {"e": event}
    )


def _admitted(stage: Stage, event: str) -> bool:
    return bool(stage.broker.command("EXISTS", "protected:admission:binding:" + event))


async def _signed_delivery(stage: Stage, delivery: str) -> httpx.Response:
    headers = signed(scoped_secret(stage.agent), delivery=delivery, body=BODY)
    return await deliver(stage.client, stage.agent, headers, body=BODY)


def test_a_delivery_whose_reply_surface_cannot_be_recorded_is_503_and_never_admitted(
    ingress_broker: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """@spec AUTOMATED-REMEDIATION-15 @spec AUTOMATED-REMEDIATION-6: the reply
    surface a remediation approval is raised on is recorded before the broker
    admits the delivery. When that write fails, the signed hook route answers
    ``503`` and the broker holds no binding and no queued turn for it, so no
    turn runs and nothing can be nominated against a delivery with no surface.
    """

    async def scenario() -> None:
        async with world(ingress_broker, tmp_path, monkeypatch) as w:
            await w.bind()
            delivery = f"surface-fault-{uuid.uuid4().hex[:8]}"
            event = event_id_of(w.stage.agent, delivery)
            entries = len(private_entries(w.stage.broker))

            with failing_statements(SURFACE_INSERT) as hits:
                response = await _signed_delivery(w.stage, delivery)

            assert hits, "the route never recorded the delivery's reply surface"
            assert response.status_code == 503, response.text
            assert not _admitted(w.stage, event), "a delivery with no surface was admitted"
            assert len(private_entries(w.stage.broker)) == entries
            assert await surface_rows(event) == []

    run(scenario)


def test_a_redelivery_after_a_failed_surface_write_records_it_and_admits_once(
    ingress_broker: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """@spec AUTOMATED-REMEDIATION-15: the sender's retry of the same signed
    delivery records the surface and is admitted exactly once; a further
    retry is the broker's duplicate and queues nothing more.
    """

    async def scenario() -> None:
        async with world(ingress_broker, tmp_path, monkeypatch) as w:
            await w.bind()
            delivery = f"surface-retry-{uuid.uuid4().hex[:8]}"
            event = event_id_of(w.stage.agent, delivery)
            entries = len(private_entries(w.stage.broker))
            with failing_statements(SURFACE_INSERT):
                failed = await _signed_delivery(w.stage, delivery)
            assert failed.status_code == 503, failed.text

            retried = await _signed_delivery(w.stage, delivery)

            assert retried.status_code == 200, retried.text
            assert retried.json()["acceptance_status"] == "accepted", retried.text
            assert _admitted(w.stage, event)
            (surface,) = await surface_rows(event)
            assert (surface["reply_kind"], surface["reply_channel"]) == (
                EMAIL_REPLY["kind"],
                EMAIL_REPLY["address"],
            ), surface
            assert len(private_entries(w.stage.broker)) == entries + 1

            again = await _signed_delivery(w.stage, delivery)
            assert again.status_code == 200, again.text
            assert len(private_entries(w.stage.broker)) == entries + 1
            assert len(await surface_rows(event)) == 1

    run(scenario)


def test_the_surface_is_committed_before_the_broker_admits_so_a_fast_turn_finds_it(
    ingress_broker: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """@spec AUTOMATED-REMEDIATION-15 @spec AUTOMATED-REMEDIATION-6: once the
    broker admits, a protected worker may run the turn and submit its block
    before the hook route answers. At the moment of admission the surface row
    is already committed (observed from another connection), so a nomination
    submitted right after the fastest turn raises its approval on it.
    """

    hooks_router = importlib.import_module("curie_api.routers.hooks")
    original = hooks_router.admit
    seen: list[list[dict[str, Any]]] = []

    async def observing_admit(slot: Any, runtime: Any, admission: Any) -> Any:
        event = admission.turn.event_id if hasattr(admission, "turn") else None
        if event is None:
            event = json.loads(admission.queued_payload)["event_id"]
        seen.append(await surface_rows(event))
        return await original(slot, runtime, admission)

    async def scenario() -> None:
        async with world(ingress_broker, tmp_path, monkeypatch) as w:
            await w.bind(policy(automatic=False))
            monkeypatch.setattr(hooks_router, "admit", observing_admit)
            delivery = f"surface-fast-{uuid.uuid4().hex[:8]}"
            response = await _signed_delivery(w.stage, delivery)
            assert response.status_code == 200, response.text
            assert seen and len(seen[0]) == 1, f"no committed surface at admission: {seen}"

            event = str(response.json()["event_id"])
            (row,) = await submit(w.stage, event, entry())
            assert_approval(row, "not_automatic")
            approval = await approval_of(row)
            assert approval["reply_kind"] == EMAIL_REPLY["kind"], approval

    run(scenario)
