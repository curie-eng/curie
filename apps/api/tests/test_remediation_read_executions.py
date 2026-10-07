"""Read executions, sample scheduling and the installation-wide sandbox cap (plan task 7).

@spec AUTOMATED-REMEDIATION-12 (executor amendments E2, E5, E6 and E9)

AUTOMATED-REMEDIATION-12 (docs/superpowers/specs/2026-10-07-automated-remediation.md)
and the maintainer rulings of 2026-10-07 (M2, M3):

* a ``read`` execution is one sample: it never enters ``dispatched``, runs
  ``list`` and its one ``read`` in ``claimed``, and ends ``confirmed`` when the
  worker reports the sample to ``POST /action-executions/{id}/samples`` (worker
  token, fenced like the other transitions), or ``refused`` on any refusal or on
  lease expiry (``runner_unavailable``). It is never re-queued;
* the API creates each sample as its own execution with a ``not_before`` time;
  the claim route hands out only due executions, oldest ``not_before`` first,
  and never more than ``actionExecutor.maxConcurrentSandboxes`` live (claimed or
  dispatched) executions across the installation (default 2, at least 1); while
  two or more slots exist at most all but one are reads, so a write-kind
  execution never waits behind samples; the API's count, not a worker, is the
  authority, so worker replicas cannot exceed it;
* a sample that cannot be claimed before its next sample is due is recorded
  skipped, an unsuccessful sample, never a refusal.

Surface these tests fix (see ``.projects/plans/task-remediation-read.tests.md``):

* ``curie_api.remediation_reads.create_read_execution(session, *, agent_id,
  connector, connector_digest, tool, arguments, pointer, authority_kind,
  authority_ref, idempotency_key, not_before)`` -> ``ReadCreated(execution_id,
  state, created)``; raises ``ReadRefused`` (with ``code``) and writes nothing.
  The read producers (admission's precondition, the verifier, the
  qualification verifier run) call it; none is an HTTP route (E1).
* ``POST /action-executions/{id}/arguments`` answers a claimed read's
  ``{"tool", "arguments", "pointer"}`` to its holder.
* ``POST /action-executions/{id}/samples``: the fence plus exactly ``sample``
  and ``value`` (``remediation-predicate.json``'s ``sample_report``), stored on
  the execution's ``sample`` column; the receipt never carries the value.
* A sample series is the read executions of one agent with the same
  ``authority_ref``, connector, tool, arguments and pointer.
* Settings ``action_executor_max_concurrent_sandboxes``
  (``CURIE_ACTION_EXECUTOR_MAX_CONCURRENT_SANDBOXES``), default 2, at least 1.

Every identifier is a placeholder.
"""

from __future__ import annotations

import asyncio
import importlib
import json
import os
import threading
import uuid
from collections.abc import Iterator
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
from _migration_support import sql_dicts, sql_rows
from _sealed_actions import (
    executor_enabled,  # noqa: F401 - fixture, requested by name
    operator_headers,
    worker_headers,
)
from curie_api.config import get_settings
from curie_api.main import create_app
from fastapi.testclient import TestClient
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

pytestmark = pytest.mark.usefixtures("clean_db", "executor_enabled")

CAP_SETTING = "CURIE_ACTION_EXECUTOR_MAX_CONCURRENT_SANDBOXES"
DIGEST = "sha256:" + "ab" * 32
READ_CONNECTOR = "example-metrics"
READ_TOOL = "query_value"
READ_ARGUMENTS: dict[str, Any] = {"query": "example_error_ratio"}
POINTER = "/data/value"
# A sampled value distinctive enough to find in any body that leaked it.
SAMPLED = "0.0137"

_VECTORS = Path(__file__).resolve().parents[3] / "tests" / "vectors"
_PREDICATE = json.loads((_VECTORS / "remediation-predicate.json").read_text("utf-8"))
_ROUTE = json.loads((_VECTORS / "runner-execute.json").read_text("utf-8"))


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #


def _reads() -> Any:
    """The read producer seam, imported per test so each reports its own failure."""

    return importlib.import_module("curie_api.remediation_reads")


@pytest.fixture
def cap() -> Iterator[Any]:
    """Set the installation-wide cap for this test; ``cap(n)`` before the claims."""

    before = os.environ.get(CAP_SETTING)

    def set_cap(value: int) -> None:
        os.environ[CAP_SETTING] = str(value)
        get_settings.cache_clear()

    try:
        yield set_cap
    finally:
        if before is None:
            os.environ.pop(CAP_SETTING, None)
        else:
            os.environ[CAP_SETTING] = before
        get_settings.cache_clear()


def _agent() -> uuid.UUID:
    agent_id = uuid.uuid4()
    sql_rows(
        "INSERT INTO curie.agents (id, name) VALUES (:id, :name)",
        {"id": agent_id, "name": f"example-agent-{agent_id.hex[:8]}"},
    )
    return agent_id


def _db_now() -> datetime:
    now: datetime = sql_dicts("SELECT now() AS now")[0]["now"]
    return now


def _run(coro_factory: Any) -> Any:
    async def run() -> Any:
        engine = create_async_engine(get_settings().database_url)
        sessions = async_sessionmaker(engine, expire_on_commit=False)
        try:
            async with sessions() as session:
                return await coro_factory(session)
        finally:
            await engine.dispose()

    return asyncio.run(run())


def _create_read(
    agent_id: uuid.UUID,
    *,
    due_in_s: float = -60.0,
    ref: str | None = None,
    key: str | None = None,
    **overrides: Any,
) -> Any:
    """One sample, due ``due_in_s`` seconds from the database's now."""

    fields: dict[str, Any] = {
        "agent_id": agent_id,
        "connector": READ_CONNECTOR,
        "connector_digest": DIGEST,
        "tool": READ_TOOL,
        "arguments": READ_ARGUMENTS,
        "pointer": POINTER,
        "authority_kind": "policy",
        "authority_ref": ref or f"policy:{agent_id}:alerts:3:{uuid.uuid4()}",
        "idempotency_key": key or f"read:{uuid.uuid4()}",
        "not_before": _db_now() + timedelta(seconds=due_in_s),
    }
    fields.update(overrides)
    return _run(lambda session: _reads().create_read_execution(session, **fields))


def _forward(agent_id: uuid.UUID) -> uuid.UUID:
    """A requested forward execution, as the forward creation function writes it."""

    execution_id = uuid.uuid4()
    sql_rows(
        "INSERT INTO curie.action_executions (id, kind, agent_id, connector, tool, "
        "connector_digest, authority_kind, authority_ref, idempotency_key, "
        "forward_arguments, arguments_sha256) VALUES (:id, 'forward', :agent_id, 'example-scale', "
        "'scale', :digest, 'approval', :ref, :key, CAST(:arguments AS jsonb), :sha)",
        {
            "id": execution_id,
            "agent_id": agent_id,
            "digest": DIGEST,
            "ref": str(uuid.uuid4()),
            "key": f"forward:{execution_id}",
            "arguments": '{"replicas":3}',
            "sha": "cd" * 32,
        },
    )
    return execution_id


def _claim(client: Any, owner: str = "worker-a", lease_seconds: int = 60) -> Any:
    return client.post(
        "/action-executions/claim",
        json={"lease_owner": owner, "lease_seconds": lease_seconds},
        headers=worker_headers(),
    )


def _claimed(client: Any, owner: str = "worker-a") -> dict[str, Any]:
    answer = _claim(client, owner)
    assert answer.status_code == 200, answer.text
    return dict(answer.json())


def _fence(claimed: dict[str, Any]) -> dict[str, Any]:
    return {"lease_owner": claimed["lease_owner"], "attempt": claimed["attempt"]}


def _sample(
    client: Any, execution_id: Any, fence: dict[str, Any], sample: str = "value", value: Any = 0.02
) -> Any:
    return client.post(
        f"/action-executions/{execution_id}/samples",
        json={**fence, "sample": sample, "value": value},
        headers=worker_headers(),
    )


def _row(execution_id: Any) -> dict[str, Any]:
    rows = sql_dicts("SELECT * FROM curie.action_executions WHERE id = :id", {"id": execution_id})
    assert len(rows) == 1
    return rows[0]


def _live() -> int:
    rows = sql_dicts(
        "SELECT count(*) AS n FROM curie.action_executions WHERE state IN ('claimed','dispatched')"
    )
    return int(rows[0]["n"])


# --------------------------------------------------------------------------- #
# Creation (executor amendments E1, E2, E9)
# --------------------------------------------------------------------------- #


def test_a_read_execution_is_created_requested_with_its_not_before() -> None:
    """@spec AUTOMATED-REMEDIATION-12: kind ``read``, authority ``policy``, scheduled."""

    agent_id = _agent()
    not_before = _db_now() + timedelta(minutes=5)
    created = _create_read(agent_id, not_before=not_before, ref="policy:example:1")

    assert created.created is True
    assert created.state == "requested"
    row = _row(created.execution_id)
    assert row["kind"] == "read"
    assert row["agent_id"] == agent_id
    assert row["connector"] == READ_CONNECTOR
    assert row["tool"] == READ_TOOL
    assert row["authority_kind"] == "policy"
    assert row["authority_ref"] == "policy:example:1"
    assert row["not_before"] == not_before
    assert row["subject_action_id"] is None
    assert row["sample"] is None


@pytest.mark.parametrize("authority_kind", ["policy", "approval", "qualification"])
def test_each_read_authority_kind_is_accepted(authority_kind: str) -> None:
    """@spec AUTOMATED-REMEDIATION-12: ``policy``, ``approval`` or ``qualification``."""

    created = _create_read(_agent(), authority_kind=authority_kind)
    assert _row(created.execution_id)["authority_kind"] == authority_kind


@pytest.mark.parametrize("authority_kind", ["undo_ruling", "capability_probe", "example_unknown"])
def test_another_authority_kind_creates_no_read(authority_kind: str) -> None:
    """@spec AUTOMATED-REMEDIATION-12 (E1): reads come from the remediation producers only."""

    agent_id = _agent()
    with pytest.raises(_reads().ReadRefused):
        _create_read(agent_id, authority_kind=authority_kind)
    assert sql_dicts("SELECT id FROM curie.action_executions") == []


@pytest.mark.parametrize("pointer", _ROUTE["read"]["invalid_pointers"])
def test_an_invalid_pointer_creates_no_read(pointer: str) -> None:
    """@spec AUTOMATED-REMEDIATION-17: an RFC 6901 pointer, or nothing is created."""

    with pytest.raises(_reads().ReadRefused):
        _create_read(_agent(), pointer=pointer)
    assert sql_dicts("SELECT id FROM curie.action_executions") == []


def test_a_replayed_read_creation_adopts_the_existing_execution() -> None:
    """@spec AUTOMATED-REMEDIATION-12: one sample per key, however often it is created."""

    agent_id = _agent()
    first = _create_read(agent_id, key="read:example-1", ref="policy:example:1", due_in_s=30)
    replay = _create_read(agent_id, key="read:example-1", ref="policy:example:1", due_in_s=30)

    assert replay.execution_id == first.execution_id
    assert replay.created is False
    assert len(sql_dicts("SELECT id FROM curie.action_executions")) == 1


# --------------------------------------------------------------------------- #
# Scheduling: not_before (executor amendment E9)
# --------------------------------------------------------------------------- #


def test_a_read_that_is_not_yet_due_is_not_handed_out(client: Any) -> None:
    """@spec AUTOMATED-REMEDIATION-12: the claim route hands out only due executions."""

    _create_read(_agent(), due_in_s=3600)

    assert _claim(client).status_code == 204


def test_due_executions_are_handed_out_oldest_not_before_first(client: Any, cap: Any) -> None:
    """@spec AUTOMATED-REMEDIATION-12: oldest ``not_before`` first, not creation order."""

    cap(5)
    agent_id = _agent()
    later = _create_read(agent_id, due_in_s=-30)
    oldest = _create_read(agent_id, due_in_s=-90)
    middle = _create_read(agent_id, due_in_s=-60)

    order = [_claimed(client)["id"] for _ in range(3)]

    assert order == [str(oldest.execution_id), str(middle.execution_id), str(later.execution_id)]


def test_a_sample_that_misses_its_slot_is_recorded_skipped(client: Any) -> None:
    """@spec AUTOMATED-REMEDIATION-12: "A sample that cannot be claimed before its next
    sample is due is recorded skipped, an unsuccessful sample."

    It is never handed out, never claims a sandbox, and its record is the sample
    kind ``skipped``, never a refusal code; the due successor is claimed instead.
    """

    agent_id = _agent()
    ref = f"policy:{agent_id}:alerts:3:{uuid.uuid4()}"
    missed = _create_read(agent_id, ref=ref, due_in_s=-120)
    due = _create_read(agent_id, ref=ref, due_in_s=-60)
    _create_read(agent_id, ref=ref, due_in_s=3600)

    claimed = _claimed(client)

    assert claimed["id"] == str(due.execution_id)
    row = _row(missed.execution_id)
    assert row["attempt"] == 0
    assert row["lease_owner"] is None
    assert row["state"] in {"confirmed", "refused"}
    assert row["finished_at"] is not None
    assert row["sample"] == {"sample": "skipped", "value": None}
    assert row["refusal_code"] != "skipped"
    assert row["failure_code"] is None
    assert "skipped" in _ROUTE["remediation_codes"]["not_refusals"]


# --------------------------------------------------------------------------- #
# The installation-wide cap (executor amendment E9)
# --------------------------------------------------------------------------- #


def test_the_default_cap_is_two() -> None:
    """@spec AUTOMATED-REMEDIATION-12: ``actionExecutor.maxConcurrentSandboxes`` default 2."""

    assert get_settings().action_executor_max_concurrent_sandboxes == 2


def test_with_the_default_cap_a_second_read_waits_and_a_forward_is_claimed(client: Any) -> None:
    """@spec AUTOMATED-REMEDIATION-12: with the cap at 2, one slot stays for writes.

    Two due samples and a forward: the first sample is claimed, the second waits
    (all but one slot may be reads), the forward is claimed while the
    verification is in progress, and a third due execution waits for a slot.
    """

    agent_id = _agent()
    first = _create_read(agent_id, due_in_s=-90)
    second = _create_read(agent_id, due_in_s=-60)

    assert _claimed(client)["id"] == str(first.execution_id)
    forward = _forward(agent_id)
    assert _claimed(client)["id"] == str(forward)
    assert _claim(client).status_code == 204
    assert _row(second.execution_id)["state"] == "requested"
    assert _live() == 2


def test_a_read_alone_never_takes_the_write_slot(client: Any) -> None:
    """@spec AUTOMATED-REMEDIATION-12: "at most all but one are read executions"."""

    agent_id = _agent()
    _create_read(agent_id, due_in_s=-90)
    _create_read(agent_id, due_in_s=-60)

    _claimed(client)
    assert _claim(client).status_code == 204
    assert _live() == 1


def test_a_finished_sample_frees_its_slot(client: Any) -> None:
    """@spec AUTOMATED-REMEDIATION-12: the next sample is claimed once one ends."""

    agent_id = _agent()
    first = _create_read(agent_id, due_in_s=-90)
    second = _create_read(agent_id, due_in_s=-60)
    claimed = _claimed(client)
    assert _claim(client).status_code == 204

    assert _sample(client, first.execution_id, _fence(claimed)).status_code == 200

    assert _claimed(client)["id"] == str(second.execution_id)


def test_writes_are_capped_too(client: Any) -> None:
    """@spec AUTOMATED-REMEDIATION-12: never more than the cap live, of any kind."""

    agent_id = _agent()
    for _ in range(3):
        _forward(agent_id)

    _claimed(client)
    _claimed(client, "worker-b")
    assert _claim(client).status_code == 204
    assert _live() == 2


def test_a_cap_of_three_admits_two_reads_and_keeps_one_slot(client: Any, cap: Any) -> None:
    """@spec AUTOMATED-REMEDIATION-12: all but one slot may be reads."""

    cap(3)
    agent_id = _agent()
    for offset in (-90, -60, -30):
        _create_read(agent_id, due_in_s=offset)

    _claimed(client)
    _claimed(client)
    assert _claim(client).status_code == 204
    _forward(agent_id)
    claimed = _claimed(client)
    assert claimed["kind"] == "forward"


def test_a_cap_of_one_lets_a_read_take_the_only_slot(client: Any, cap: Any) -> None:
    """@spec AUTOMATED-REMEDIATION-12: the reserved slot exists only while two or more do."""

    cap(1)
    agent_id = _agent()
    read = _create_read(agent_id)
    _forward(agent_id)

    assert _claimed(client)["id"] == str(read.execution_id)
    assert _claim(client).status_code == 204


def test_two_replicas_claiming_at_once_never_exceed_the_cap(cap: Any) -> None:
    """@spec AUTOMATED-REMEDIATION-12: the API's count is the authority across replicas.

    Four API clients (each its own app, as replicas are) claim concurrently for
    two worker lease owners against many due reads and forwards; at no point are
    more than the cap live, and never more than all but one are reads.
    """

    cap(3)
    agent_id = _agent()
    for offset in range(12):
        _create_read(agent_id, due_in_s=-600 + offset)
    for _ in range(6):
        _forward(agent_id)

    barrier = threading.Barrier(4)
    errors: list[BaseException] = []

    def replica(owner: str) -> None:
        try:
            with TestClient(create_app()) as replica_client:
                barrier.wait(timeout=60)
                for _ in range(6):
                    answer = _claim(replica_client, owner)
                    assert answer.status_code in {200, 204}, answer.text
        except BaseException as exc:  # noqa: BLE001 - surfaced below
            errors.append(exc)

    threads = [
        threading.Thread(target=replica, args=(f"worker-{name}",)) for name in ("a", "b", "a", "b")
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=110)
    assert not errors, errors

    live = sql_dicts(
        "SELECT kind, count(*) AS n FROM curie.action_executions "
        "WHERE state IN ('claimed','dispatched') GROUP BY kind"
    )
    by_kind = {row["kind"]: int(row["n"]) for row in live}
    assert sum(by_kind.values()) == 3
    assert by_kind.get("read", 0) <= 2


# --------------------------------------------------------------------------- #
# Lifecycle (executor amendment E5)
# --------------------------------------------------------------------------- #


def test_a_claimed_read_hands_its_holder_the_bound_tool_arguments_and_pointer(
    client: Any,
) -> None:
    """@spec AUTOMATED-REMEDIATION-12: the read is the declaration's, never a caller's."""

    created = _create_read(_agent())
    claimed = _claimed(client)
    assert claimed["kind"] == "read"
    assert claimed["tool"] == READ_TOOL

    answer = client.post(
        f"/action-executions/{created.execution_id}/arguments",
        json=_fence(claimed),
        headers=worker_headers(),
    )

    assert answer.status_code == 200, answer.text
    assert answer.json() == {"tool": READ_TOOL, "arguments": READ_ARGUMENTS, "pointer": POINTER}


@pytest.mark.parametrize("case", _PREDICATE["extractions"], ids=lambda case: case["name"])
def test_a_reported_sample_is_stored_and_ends_the_read_confirmed(
    client: Any, case: dict[str, Any]
) -> None:
    """@spec AUTOMATED-REMEDIATION-12: every frozen sample kind, stored exactly."""

    created = _create_read(_agent())
    claimed = _claimed(client)
    sample = case["sample"]

    answer = _sample(
        client, created.execution_id, _fence(claimed), sample["sample"], sample["value"]
    )

    assert answer.status_code == 200, answer.text
    assert answer.json()["state"] == "confirmed"
    row = _row(created.execution_id)
    assert row["state"] == "confirmed"
    assert row["dispatched_at"] is None
    assert row["refusal_code"] is None and row["failure_code"] is None
    assert json.dumps(row["sample"], sort_keys=True) == json.dumps(sample, sort_keys=True)


def test_a_replayed_sample_is_idempotent_and_a_different_one_is_refused(client: Any) -> None:
    """@spec AUTOMATED-REMEDIATION-12: fenced and idempotent like the other transitions."""

    created = _create_read(_agent())
    fence = _fence(_claimed(client))

    assert _sample(client, created.execution_id, fence).status_code == 200
    assert _sample(client, created.execution_id, fence).status_code == 200
    assert _sample(client, created.execution_id, fence, value=0.5).status_code == 409
    assert _row(created.execution_id)["sample"] == {"sample": "value", "value": 0.02}


def test_a_sample_with_a_wrong_fence_is_refused(client: Any) -> None:
    """@spec AUTOMATED-REMEDIATION-12: "fenced like the other transitions"."""

    created = _create_read(_agent())
    claimed = _claimed(client)

    answer = _sample(client, created.execution_id, {**_fence(claimed), "lease_owner": "worker-z"})

    assert answer.status_code == 409
    assert _row(created.execution_id)["state"] == "claimed"


@pytest.mark.parametrize(
    "body",
    [
        pytest.param({"sample": "value", "value": 1, "result": {"data": 1}}, id="result"),
        pytest.param({"sample": "value", "value": 1, "index": 0}, id="index"),
        pytest.param({"sample": "example_unknown", "value": None}, id="unknown_kind"),
        pytest.param({"sample": "skipped", "value": None}, id="skipped_is_the_apis"),
        pytest.param({"sample": "value", "value": {"x": 1}}, id="not_a_scalar"),
        pytest.param({"sample": "value", "value": "x" * 257}, id="value_too_long"),
        pytest.param({"sample": "pointer_absent", "value": 1}, id="value_on_absent"),
        pytest.param({"sample": "value"}, id="missing_value"),
    ],
)
def test_a_sample_body_outside_the_frozen_report_is_rejected(
    client: Any, body: dict[str, Any]
) -> None:
    """@spec AUTOMATED-REMEDIATION-12: the API never receives more than the scalar."""

    created = _create_read(_agent())
    fence = _fence(_claimed(client))

    answer = client.post(
        f"/action-executions/{created.execution_id}/samples",
        json={**fence, **body},
        headers=worker_headers(),
    )

    assert answer.status_code == 422, answer.text
    assert _row(created.execution_id)["state"] == "claimed"
    assert _row(created.execution_id)["sample"] is None


def test_the_samples_route_requires_the_worker_token(client: Any) -> None:
    """@spec AUTOMATED-REMEDIATION-12: worker token, never the platform key."""

    created = _create_read(_agent())
    fence = _fence(_claimed(client))

    answer = client.post(
        f"/action-executions/{created.execution_id}/samples",
        json={**fence, "sample": "value", "value": 1},
        headers=operator_headers(),
    )

    assert answer.status_code in {401, 403}


def test_only_a_read_reports_a_sample(client: Any) -> None:
    """@spec AUTOMATED-REMEDIATION-12: a forward or restore has no sample."""

    forward = _forward(_agent())
    fence = _fence(_claimed(client))

    assert _sample(client, forward, fence).status_code == 409


def test_the_receipt_never_carries_the_sampled_value(client: Any) -> None:
    """@spec AUTOMATED-REMEDIATION-12: identity, state, fence and codes only."""

    created = _create_read(_agent())
    fence = _fence(_claimed(client))
    assert _sample(client, created.execution_id, fence, value=SAMPLED).status_code == 200

    receipt = client.get(f"/action-executions/{created.execution_id}", headers=operator_headers())

    assert receipt.status_code == 200, receipt.text
    assert SAMPLED not in receipt.text
    assert POINTER not in receipt.text


def test_a_read_never_dispatches(client: Any) -> None:
    """@spec AUTOMATED-REMEDIATION-12: "a read execution never enters ``dispatched``"."""

    created = _create_read(_agent())
    fence = _fence(_claimed(client))

    answer = client.post(
        f"/action-executions/{created.execution_id}/dispatch", json=fence, headers=worker_headers()
    )

    assert answer.status_code == 409
    assert _row(created.execution_id)["state"] == "claimed"
    assert sql_dicts("SELECT id FROM curie.agent_actions") == []


def test_a_read_refused_tool_not_read_only_ends_refused(client: Any) -> None:
    """@spec AUTOMATED-REMEDIATION-12 (E6): the runner's refusal is a pre-dispatch code."""

    created = _create_read(_agent())
    fence = _fence(_claimed(client))

    answer = client.post(
        f"/action-executions/{created.execution_id}/outcome",
        json={**fence, "state": "refused", "code": "tool_not_read_only"},
        headers=worker_headers(),
    )

    assert answer.status_code == 200, answer.text
    row = _row(created.execution_id)
    assert (row["state"], row["refusal_code"]) == ("refused", "tool_not_read_only")
    assert row["sample"] is None


@pytest.mark.parametrize(
    ("state", "code"),
    [("confirmed", None), ("failed", "connector_error"), ("indeterminate", "response_lost")],
)
def test_a_read_ends_confirmed_only_through_its_sample(
    client: Any, state: str, code: str | None
) -> None:
    """@spec AUTOMATED-REMEDIATION-12: ``confirmed`` with a sample, or ``refused``."""

    created = _create_read(_agent())
    fence = _fence(_claimed(client))

    answer = client.post(
        f"/action-executions/{created.execution_id}/outcome",
        json={**fence, "state": state, "code": code},
        headers=worker_headers(),
    )

    assert answer.status_code == 409, answer.text
    assert _row(created.execution_id)["state"] == "claimed"


def test_an_expired_read_lease_is_refused_and_never_requeued(client: Any) -> None:
    """@spec AUTOMATED-REMEDIATION-12: "``refused`` with ``runner_unavailable`` on lease
    expiry ... It is never re-queued", so a crashed worker's sample is unsuccessful.
    """

    created = _create_read(_agent())
    _claimed(client)
    sql_rows(
        "UPDATE curie.action_executions SET lease_expires_at = now() - interval '1 minute' "
        "WHERE id = :id",
        {"id": created.execution_id},
    )

    assert _claim(client, "worker-b").status_code == 204

    row = _row(created.execution_id)
    assert (row["state"], row["refusal_code"]) == ("refused", "runner_unavailable")
    assert row["attempt"] == 1
    assert row["finished_at"] is not None


def test_an_expired_read_frees_its_slot(client: Any) -> None:
    """@spec AUTOMATED-REMEDIATION-12: a crashed holder never holds the cap."""

    agent_id = _agent()
    first = _create_read(agent_id, due_in_s=-90)
    second = _create_read(agent_id, due_in_s=-60)
    _claimed(client)
    sql_rows(
        "UPDATE curie.action_executions SET lease_expires_at = now() - interval '1 minute' "
        "WHERE id = :id",
        {"id": first.execution_id},
    )

    assert _claimed(client, "worker-b")["id"] == str(second.execution_id)
