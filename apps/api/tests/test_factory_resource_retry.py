"""A limit failure retries only under a strictly larger delivered limit (#5427, ADR 0269).

Every scenario drives the real paths, as ``test_factory_retry.py`` does: the
real app from ``create_app()``, requests admitted through the internal
admissions route, the internal acquire, defer, start and finish routes posted
as the worker posts them, and the real ``WorkItemReconciler.run_once()``
against the per-test runs stream. Postgres and Valkey are real. Time is moved
only by SQL on the stored rows; nothing inside the API is patched.

A start reports the limits the worker read from the sandbox it claimed. The
API records them, with the history capacity and execution deadline it
enforces itself, on the request. A clean limit failure admits one successor
gated on the limit its own attempt had, which the reconciler offers every
probe interval and the start releases only for a strictly larger value.
"""

from __future__ import annotations

import json
import logging
import uuid
from collections.abc import Iterator
from datetime import timedelta
from typing import Any

import httpx
import pytest
import redis
from curie_api.config import get_settings
from curie_api.main import create_app
from fastapi.testclient import TestClient
from test_factory_retry import (
    OWNER,
    WORKER,
    Ref,
    _admit,
    _agent,
    _call,
    _configure,
    _execute,
    _expire_wait,
    _finish,
    _make_due,
    _now,
    _published_wake,
    _reconcile,
    _request,
    _requests,
    _successor,
    _used,
    _wake,
    _wake_ids,
)

PREFIX = "/v1/internal/work-items/requests"
GIB = 1024**3
MIB = 1024**2
# What the worker read from the sandbox each ordinary start claimed.
BASE: dict[str, Any] = {
    "memory_bytes": GIB,
    "ephemeral_storage_bytes": 2 * GIB,
    "scratch_bytes": {"workspace": GIB, "home": 512 * MIB},
    "max_turns": 40,
}


# -- harness -----------------------------------------------------------------


@pytest.fixture
def api(
    clean_db: None, runs_stream: str, valkey: redis.Redis, monkeypatch: pytest.MonkeyPatch
) -> Iterator[TestClient]:
    _configure(monkeypatch)
    with TestClient(create_app()) as client:
        yield client
    get_settings.cache_clear()


def _limits(**changes: Any) -> dict[str, Any]:
    limits = {**BASE, "scratch_bytes": dict(BASE["scratch_bytes"])}
    limits.update(changes)
    return limits


def _probe() -> timedelta:
    return timedelta(seconds=get_settings().work_item_resource_gate_probe_seconds)


def _acquire(api: TestClient, request_id: uuid.UUID, generation: int) -> httpx.Response:
    wake = _call(api, _wake, request_id)
    return api.post(
        f"{PREFIX}/{request_id}/acquire",
        json={
            "owner": OWNER,
            "generation": generation,
            "wake": wake.model_dump(mode="json"),
            "context": None,
        },
        headers=WORKER,
    )


def _offer(api: TestClient, request_id: uuid.UUID) -> tuple[int, dict[str, Any]]:
    """Make the request due, let the reconciler wake it, then acquire it."""

    _make_due(request_id)
    _reconcile()
    generation = _published_wake(request_id)
    acquired = _acquire(api, request_id, generation)
    assert acquired.status_code == 200, acquired.text
    return generation, acquired.json()


def _start(
    api: TestClient,
    request_id: uuid.UUID,
    generation: int,
    grant: dict[str, Any],
    limits: dict[str, Any],
) -> httpx.Response:
    return api.post(
        f"{PREFIX}/{request_id}/start",
        json={
            "owner": OWNER,
            "generation": generation,
            "claim_name": f"curie-thread-{request_id.hex[:12]}",
            "sandbox_name": f"sbx-curie-thread-{request_id.hex[:12]}",
            "context": grant["context"],
            "delivered_limits": limits,
        },
        headers=WORKER,
    )


def _defer(
    api: TestClient, request_id: uuid.UUID, generation: int, reason: str, *, capacity: bool
) -> httpx.Response:
    return api.post(
        f"{PREFIX}/{request_id}/defer",
        json={"owner": OWNER, "generation": generation, "reason": reason, "capacity": capacity},
        headers=WORKER,
    )


def _run(api: TestClient, request_id: uuid.UUID, limits: dict[str, Any] | None = None) -> int:
    """Offer, acquire and start the request with ``limits``; return its epoch."""

    generation, grant = _offer(api, request_id)
    started = _start(api, request_id, generation, grant, BASE if limits is None else limits)
    assert started.status_code == 200, started.text
    epoch = started.json()["runtime_epoch"]
    assert isinstance(epoch, int)
    return epoch


def _fail(
    api: TestClient,
    request_id: uuid.UUID,
    epoch: int,
    cause: str,
    *,
    safety: str = "no_effects",
    volume: str | None = None,
) -> None:
    finished = _finish(api, request_id, epoch, cause, safety, exhausted_volume=volume)
    assert finished.status_code == 200, finished.text


def _gated(
    api: TestClient,
    ref: Ref,
    cause: str = "sandbox_memory_exhausted",
    *,
    limits: dict[str, Any] | None = None,
    volume: str | None = None,
) -> dict[str, Any]:
    """Run ``ref``'s request, fail it on a limit, and return its gated successor."""

    epoch = _run(api, ref.request_id, limits)
    _fail(api, ref.request_id, epoch, cause, volume=volume)
    successor = _successor(ref.request_id)
    assert successor is not None
    assert successor["resource_gate"] is not None, successor
    return successor


def _refusal(response: httpx.Response) -> str:
    detail = response.json()["detail"]
    assert isinstance(detail, dict), detail
    code = detail["code"]
    assert isinstance(code, str)
    return code


def _successor_woken(request_id: uuid.UUID) -> bool:
    return any(i.startswith(f"work-item-{request_id}") for i in _wake_ids())


# -- admission -----------------------------------------------------------------


@pytest.mark.parametrize(
    ("cause", "volume", "condition", "dimension", "failed"),
    [
        ("sandbox_memory_exhausted", None, "memory_limit", "memory", GIB),
        (
            "sandbox_storage_exhausted",
            None,
            "ephemeral_storage_limit",
            "ephemeral_storage",
            2 * GIB,
        ),
        ("sandbox_scratch_exhausted", "workspace", "scratch_limit", "scratch", GIB),
        ("history_capacity", None, "history_capacity", "history_capacity", None),
        ("max_turns", None, "turn_limit", "max_turns", 40),
    ],
)
def test_clean_limit_failure_admits_one_gated_successor_with_the_delivered_limit(
    api: TestClient,
    auth_headers: dict[str, str],
    cause: str,
    volume: str | None,
    condition: str,
    dimension: str,
    failed: int | None,
) -> None:
    item = _admit(api, _agent(api, auth_headers), 54270)
    epoch = _run(api, item.request_id)
    _fail(api, item.request_id, epoch, cause, volume=volume)

    expected = get_settings().transcript_max_thread_bytes if failed is None else failed
    predecessor = _request(item.request_id)
    record = predecessor["delivered_limits"]
    assert record["runtime_epoch"] == epoch == predecessor["runtime_epoch"]
    assert record.get("exhausted_volume") == volume
    successor = _successor(item.request_id)
    assert successor is not None
    assert len(_requests(item.work_item_id)) == 2
    assert _used(item.work_item_id) == 1
    assert successor["status"] == "waiting"
    assert successor["blocked_on"] == condition
    assert successor["resource_gate"] == {
        "condition": condition,
        "dimension": dimension,
        "volume": volume,
        "failed_limit": expected,
        "source_request_id": str(item.request_id),
        "source_runtime_epoch": epoch,
    }
    # A gated wait is probed, not slept until its waiting deadline.
    terminal_at = predecessor["terminal_at"]
    due = successor["dispatch_not_before"]
    assert terminal_at + _probe() <= due <= terminal_at + _probe() + timedelta(seconds=5)
    assert due < successor["wait_deadline"]


@pytest.mark.parametrize(
    ("cause", "volume", "limits"),
    [
        ("sandbox_memory_exhausted", None, _limits(memory_bytes=None)),
        ("sandbox_storage_exhausted", None, _limits(ephemeral_storage_bytes=None)),
        ("sandbox_scratch_exhausted", "tmp", _limits()),
        ("max_turns", None, _limits(max_turns=None)),
        ("sandbox_memory_exhausted", None, None),
    ],
    ids=["no-memory", "no-storage", "volume-not-delivered", "no-turn-limit", "no-record"],
)
def test_predecessor_without_a_delivered_value_admits_no_successor(
    api: TestClient,
    auth_headers: dict[str, str],
    cause: str,
    volume: str | None,
    limits: dict[str, Any] | None,
) -> None:
    agent_id = _agent(api, auth_headers)
    item = _admit(api, agent_id, 54271)
    epoch = _run(api, item.request_id, limits or BASE)
    if limits is None:
        # A request started before the record existed has none.
        _execute(
            "UPDATE curie.execution_requests SET delivered_limits = NULL WHERE id = :id",
            {"id": item.request_id},
            guarded=True,
        )
    _fail(api, item.request_id, epoch, cause, volume=volume)
    assert _request(item.request_id)["status"] == "failed"
    assert _successor(item.request_id) is None
    assert _used(item.work_item_id) == 0

    # Liveness: the same failure with the value delivered admits one.
    control = _admit(api, agent_id, 54272)
    epoch = _run(api, control.request_id)
    _fail(api, control.request_id, epoch, cause, volume=volume and "workspace")
    assert _successor(control.request_id) is not None


@pytest.mark.parametrize("safety", ["flagged", "uncertain"])
def test_flagged_or_uncertain_limit_failure_admits_no_successor(
    api: TestClient, auth_headers: dict[str, str], safety: str
) -> None:
    agent_id = _agent(api, auth_headers)
    item = _admit(api, agent_id, 54273)
    epoch = _run(api, item.request_id)
    _fail(api, item.request_id, epoch, "sandbox_memory_exhausted", safety=safety)
    assert _request(item.request_id)["retry_safety"] == safety
    assert _successor(item.request_id) is None
    assert _used(item.work_item_id) == 0

    # Liveness: a clean run of the same failure is gated.
    control = _admit(api, agent_id, 54274)
    assert _gated(api, control)["blocked_on"] == "memory_limit"


def test_stale_runtime_epoch_finish_is_refused_and_admits_nothing(
    api: TestClient, auth_headers: dict[str, str]
) -> None:
    item = _admit(api, _agent(api, auth_headers), 54275)
    epoch = _run(api, item.request_id)
    stale = _finish(api, item.request_id, epoch + 1, "sandbox_memory_exhausted")
    assert stale.status_code == 409, stale.text
    assert _refusal(stale) == "stale_owner"
    assert _request(item.request_id)["status"] == "running"
    assert _successor(item.request_id) is None

    # Liveness: the current epoch settles and admits the gated successor.
    _fail(api, item.request_id, epoch, "sandbox_memory_exhausted")
    assert _successor(item.request_id) is not None


def test_slot_exhaustion_stops_at_two_gated_successors(
    api: TestClient, auth_headers: dict[str, str]
) -> None:
    item = _admit(api, _agent(api, auth_headers), 54276)
    first = _gated(api, item)
    assert first["resource_gate"]["failed_limit"] == GIB
    epoch = _run(api, first["id"], _limits(memory_bytes=2 * GIB))
    _fail(api, first["id"], epoch, "sandbox_memory_exhausted")
    second = _successor(first["id"])
    assert second is not None
    # The second gate carries the limit the retry itself had.
    assert second["resource_gate"]["failed_limit"] == 2 * GIB
    epoch = _run(api, second["id"], _limits(memory_bytes=4 * GIB))
    _fail(api, second["id"], epoch, "sandbox_memory_exhausted")
    assert _successor(second["id"]) is None
    assert _used(item.work_item_id) == 2
    assert len(_requests(item.work_item_id)) == 3


# -- dispatch ------------------------------------------------------------------


def test_gated_row_is_offered_only_after_its_probe_and_an_unmet_defer_is_uncounted(
    api: TestClient, auth_headers: dict[str, str]
) -> None:
    item = _admit(api, _agent(api, auth_headers), 54277)
    successor = _gated(api, item)

    # Before its probe time the reconciler leaves the gated row asleep.
    _reconcile()
    assert not _successor_woken(successor["id"])
    assert _request(successor["id"])["published_generation"] is None

    generation, grant = _offer(api, successor["id"])
    assert grant["resource_gate"] == {
        "condition": "memory_limit",
        "dimension": "memory",
        "volume": None,
        "failed_limit": GIB,
        # The worker resolves sandbox dimensions itself.
        "current_limit": None,
    }
    deferred = _defer(api, successor["id"], generation, "resource_gate_unmet", capacity=False)
    assert deferred.status_code == 200, deferred.text
    row = _request(successor["id"])
    now = _now()
    assert (row["status"], row["blocked_on"], row["resource_gate"]) == (
        "waiting",
        "memory_limit",
        successor["resource_gate"],
    )
    assert (row["start_deferrals"], row["capacity_deferrals"]) == (
        successor["start_deferrals"],
        successor["capacity_deferrals"],
    )
    assert row["last_deferral_reason"] == "resource_gate_unmet"
    assert now + _probe() - timedelta(seconds=5) <= row["dispatch_not_before"] <= now + _probe()
    assert row["acquire_owner"] is None

    # Not offered again before the next probe.
    published = row["published_generation"]
    _reconcile()
    assert _request(successor["id"])["published_generation"] == published

    # Liveness: at the next probe it is offered and a larger limit starts it.
    generation, grant = _offer(api, successor["id"])
    started = _start(api, successor["id"], generation, grant, _limits(memory_bytes=2 * GIB))
    assert started.status_code == 200, started.text


@pytest.mark.parametrize("memory", [GIB, GIB // 2], ids=["equal", "smaller"])
def test_start_without_a_larger_limit_is_refused_and_leaves_the_request_blocked(
    api: TestClient, auth_headers: dict[str, str], memory: int
) -> None:
    item = _admit(api, _agent(api, auth_headers), 54278)
    successor = _gated(api, item)
    generation, grant = _offer(api, successor["id"])
    before = _request(successor["id"])

    refused = _start(api, successor["id"], generation, grant, _limits(memory_bytes=memory))
    assert refused.status_code == 409, refused.text
    assert _refusal(refused) == "resource_gate_unmet"
    after = _request(successor["id"])
    assert after == before
    assert (after["status"], after["blocked_on"], after["delivered_limits"]) == (
        "waiting",
        "memory_limit",
        None,
    )
    # The acquire is still held, so the worker can release and defer.
    assert after["acquire_owner"] == OWNER

    deferred = _defer(api, successor["id"], generation, "resource_gate_unmet", capacity=False)
    assert deferred.status_code == 200, deferred.text
    assert _request(successor["id"])["start_deferrals"] == before["start_deferrals"]

    # Liveness: a strictly larger delivered limit starts it.
    generation, grant = _offer(api, successor["id"])
    started = _start(api, successor["id"], generation, grant, _limits(memory_bytes=memory * 4))
    assert started.status_code == 200, started.text


def test_larger_limit_starts_clears_the_block_and_records_the_new_envelope(
    api: TestClient, auth_headers: dict[str, str]
) -> None:
    item = _admit(api, _agent(api, auth_headers), 54279)
    successor = _gated(api, item, "sandbox_scratch_exhausted", volume="workspace")
    generation, grant = _offer(api, successor["id"])
    raised = _limits(scratch_bytes={"workspace": 2 * GIB, "home": 512 * MIB})
    started = _start(api, successor["id"], generation, grant, raised)
    assert started.status_code == 200, started.text

    row = _request(successor["id"])
    assert (row["status"], row["blocked_on"]) == ("running", None)
    # The gate stays on the row as the record of why this attempt waited.
    assert row["resource_gate"] == successor["resource_gate"]
    assert row["delivered_limits"] == {
        "runtime_epoch": started.json()["runtime_epoch"],
        **raised,
        "history_capacity_bytes": get_settings().transcript_max_thread_bytes,
        "execution_seconds": int((row["execution_deadline"] - row["started_at"]).total_seconds()),
    }


@pytest.mark.parametrize(
    ("cause", "volume", "unrelated", "related"),
    [
        (
            "sandbox_memory_exhausted",
            None,
            _limits(max_turns=400, ephemeral_storage_bytes=8 * GIB),
            _limits(memory_bytes=2 * GIB),
        ),
        (
            "sandbox_scratch_exhausted",
            "workspace",
            _limits(scratch_bytes={"workspace": GIB, "home": 8 * GIB}),
            _limits(scratch_bytes={"workspace": 2 * GIB, "home": 512 * MIB}),
        ),
        (
            "sandbox_scratch_exhausted",
            "workspace",
            _limits(scratch_bytes={"home": 8 * GIB}),
            _limits(scratch_bytes={"workspace": 2 * GIB}),
        ),
        (
            "max_turns",
            None,
            _limits(memory_bytes=8 * GIB),
            _limits(max_turns=41),
        ),
    ],
    ids=["memory-vs-others", "scratch-other-volume", "scratch-volume-absent", "turns-vs-memory"],
)
def test_an_unrelated_change_does_not_release_the_gate(
    api: TestClient,
    auth_headers: dict[str, str],
    cause: str,
    volume: str | None,
    unrelated: dict[str, Any],
    related: dict[str, Any],
) -> None:
    item = _admit(api, _agent(api, auth_headers), 54280)
    successor = _gated(api, item, cause, volume=volume)
    generation, grant = _offer(api, successor["id"])
    # The same limits as the failed run stand for a cpu, image or replica
    # change, which the delivered envelope does not carry.
    for limits in (BASE, unrelated):
        refused = _start(api, successor["id"], generation, grant, limits)
        assert refused.status_code == 409, refused.text
        assert _refusal(refused) == "resource_gate_unmet"
        assert _request(successor["id"])["blocked_on"] == successor["blocked_on"]

    # Liveness: raising the gated dimension releases it.
    started = _start(api, successor["id"], generation, grant, related)
    assert started.status_code == 200, started.text


def test_capacity_defers_on_a_gated_row_never_clear_the_gate(
    api: TestClient, auth_headers: dict[str, str]
) -> None:
    item = _admit(api, _agent(api, auth_headers), 54281)
    successor = _gated(api, item, "sandbox_storage_exhausted")
    for _ in range(2):
        generation, _grant = _offer(api, successor["id"])
        deferred = _defer(api, successor["id"], generation, "capacity", capacity=True)
        assert deferred.status_code == 200, deferred.text
    row = _request(successor["id"])
    assert row["capacity_deferrals"] == successor["capacity_deferrals"] + 2
    assert row["start_deferrals"] == successor["start_deferrals"]
    assert (row["blocked_on"], row["resource_gate"]) == (
        "ephemeral_storage_limit",
        successor["resource_gate"],
    )

    # The gate still holds at start, and a larger value still releases it.
    generation, grant = _offer(api, successor["id"])
    refused = _start(api, successor["id"], generation, grant, BASE)
    assert _refusal(refused) == "resource_gate_unmet"
    started = _start(
        api, successor["id"], generation, grant, _limits(ephemeral_storage_bytes=4 * GIB)
    )
    assert started.status_code == 200, started.text


def test_gate_wait_expires_as_retry_condition_expired_naming_the_condition(
    api: TestClient, auth_headers: dict[str, str], caplog: pytest.LogCaptureFixture
) -> None:
    item = _admit(api, _agent(api, auth_headers), 54282)
    successor = _gated(api, item)
    _expire_wait(successor["id"])
    with caplog.at_level(logging.WARNING, logger="curie_api.workitem_reconciler"):
        _reconcile()
    expected = (
        f"work item request {successor['id']} expired waiting for retry condition memory_limit"
    )
    assert expected in [record.getMessage() for record in caplog.records]
    expired = _request(successor["id"])
    assert (expired["status"], expired["terminal_cause"], expired["blocked_on"]) == (
        "expired",
        "retry_condition_expired",
        "memory_limit",
    )
    assert _successor(successor["id"]) is None
    assert _used(item.work_item_id) == 1


# -- the history, turn and execution time dimensions ----------------------------


def test_history_gate_carries_the_current_cap_and_a_raised_cap_releases_it(
    api: TestClient, auth_headers: dict[str, str], monkeypatch: pytest.MonkeyPatch
) -> None:
    item = _admit(api, _agent(api, auth_headers), 54283)
    successor = _gated(api, item, "history_capacity")
    cap = get_settings().transcript_max_thread_bytes
    generation, grant = _offer(api, successor["id"])
    assert grant["resource_gate"]["current_limit"] == cap
    refused = _start(api, successor["id"], generation, grant, _limits(memory_bytes=8 * GIB))
    assert _refusal(refused) == "resource_gate_unmet"

    # The API computes this dimension itself, so the operator's raised cap is
    # what releases it, not anything the worker submits.
    monkeypatch.setenv("TRANSCRIPT_MAX_THREAD_BYTES", str(cap * 2))
    get_settings.cache_clear()
    assert get_settings().transcript_max_thread_bytes == cap * 2
    started = _start(api, successor["id"], generation, grant, BASE)
    assert started.status_code == 200, started.text
    assert _request(successor["id"])["delivered_limits"]["history_capacity_bytes"] == cap * 2


def test_turn_gate_is_released_only_by_a_larger_turn_limit(
    api: TestClient, auth_headers: dict[str, str]
) -> None:
    item = _admit(api, _agent(api, auth_headers), 54284)
    successor = _gated(api, item, "max_turns")
    generation, grant = _offer(api, successor["id"])
    assert grant["resource_gate"]["current_limit"] is None
    for turns in (None, 40, 39):
        refused = _start(api, successor["id"], generation, grant, _limits(max_turns=turns))
        assert _refusal(refused) == "resource_gate_unmet"
    started = _start(api, successor["id"], generation, grant, _limits(max_turns=80))
    assert started.status_code == 200, started.text


def test_execution_time_gate_reads_the_agent_deadline_and_needs_a_larger_one(
    api: TestClient, auth_headers: dict[str, str]
) -> None:
    """The gate supports ``execution_time_limit``.

    The execution deadline API path admits no successor, because it carries no
    worker safety fact, so this seeds the gated successor row directly: a real
    gated successor whose gate is rewritten to the execution time dimension.
    """

    agent_id = _agent(api, auth_headers)
    item = _admit(api, agent_id, 54285)
    successor = _gated(api, item, "history_capacity")
    failed = _request(item.request_id)["delivered_limits"]["execution_seconds"]
    gate = {
        **successor["resource_gate"],
        "condition": "execution_time_limit",
        "dimension": "execution_seconds",
        "failed_limit": failed,
    }
    _execute(
        "UPDATE curie.execution_requests SET blocked_on = 'execution_time_limit', "
        "resource_gate = CAST(:gate AS jsonb) WHERE id = :id",
        {"gate": json.dumps(gate), "id": successor["id"]},
        guarded=True,
    )
    generation, grant = _offer(api, successor["id"])
    assert grant["resource_gate"]["dimension"] == "execution_seconds"
    assert grant["resource_gate"]["current_limit"] == failed
    refused = _start(api, successor["id"], generation, grant, BASE)
    assert _refusal(refused) == "resource_gate_unmet"

    _execute(
        "UPDATE curie.agents SET execution_deadline_seconds = :seconds WHERE id = :id",
        {"seconds": failed + 600, "id": agent_id},
    )
    # The worker still holds its acquire after the refused start; acquiring
    # again reads the agent's current deadline.
    acquired = _acquire(api, successor["id"], generation)
    assert acquired.status_code == 200, acquired.text
    assert acquired.json()["resource_gate"]["current_limit"] == failed + 600
    started = _start(api, successor["id"], generation, acquired.json(), BASE)
    assert started.status_code == 200, started.text
    row = _request(successor["id"])
    assert row["delivered_limits"]["execution_seconds"] == failed + 600


# -- node disruption and the predecessor fence ----------------------------------


def test_node_disruption_admits_a_scheduled_ungated_successor_fenced_on_its_predecessor(
    api: TestClient, auth_headers: dict[str, str]
) -> None:
    agent_id = _agent(api, auth_headers)
    item = _admit(api, agent_id, 54286)
    epoch = _run(api, item.request_id)
    _fail(api, item.request_id, epoch, "sandbox_node_disrupted")
    predecessor = _request(item.request_id)
    successor = _successor(item.request_id)
    assert successor is not None
    assert (successor["blocked_on"], successor["resource_gate"]) == (None, None)
    backoff = successor["dispatch_not_before"] - predecessor["terminal_at"]
    assert timedelta(seconds=24) <= backoff <= timedelta(seconds=37)

    generation, grant = _offer(api, successor["id"])
    assert grant["resource_gate"] is None
    assert grant["predecessor_claim_name"] == predecessor["runtime_claim_name"]
    assert grant["predecessor_claim_name"] == f"curie-thread-{item.request_id.hex[:12]}"
    deferred = _defer(
        api, successor["id"], generation, "predecessor_runtime_present", capacity=False
    )
    assert deferred.status_code == 200, deferred.text
    row = _request(successor["id"])
    now = _now()
    assert (row["start_deferrals"], row["capacity_deferrals"]) == (
        successor["start_deferrals"],
        successor["capacity_deferrals"],
    )
    assert now + _probe() - timedelta(seconds=5) <= row["dispatch_not_before"] <= now + _probe()

    # Liveness: once the claim is gone the ungated successor starts with any limits.
    generation, grant = _offer(api, successor["id"])
    started = _start(api, successor["id"], generation, grant, BASE)
    assert started.status_code == 200, started.text

    # A flagged node disruption admits nothing.
    flagged = _admit(api, agent_id, 54287)
    epoch = _run(api, flagged.request_id)
    _fail(api, flagged.request_id, epoch, "sandbox_node_disrupted", safety="flagged")
    assert _successor(flagged.request_id) is None


def test_gated_successor_grant_names_the_predecessor_claim(
    api: TestClient, auth_headers: dict[str, str]
) -> None:
    item = _admit(api, _agent(api, auth_headers), 54288)
    successor = _gated(api, item, "max_turns")
    _generation, grant = _offer(api, successor["id"])
    assert grant["predecessor_claim_name"] == _request(item.request_id)["runtime_claim_name"]


def test_fence_and_gate_defers_are_refused_without_a_predecessor_or_gate(
    api: TestClient, auth_headers: dict[str, str]
) -> None:
    item = _admit(api, _agent(api, auth_headers), 54289)
    generation, grant = _offer(api, item.request_id)
    assert (grant["resource_gate"], grant["predecessor_claim_name"]) == (None, None)
    before = _request(item.request_id)
    for reason in ("predecessor_runtime_present", "resource_gate_unmet"):
        for capacity in (False, True):
            refused = _defer(api, item.request_id, generation, reason, capacity=capacity)
            assert refused.status_code == 409, refused.text
            assert _refusal(refused) == "not_dispatchable"
    assert _request(item.request_id) == before

    # Liveness: the refusals left the acquire held, so the request starts.
    started = _start(api, item.request_id, generation, grant, BASE)
    assert started.status_code == 200, started.text


def test_ordinary_retry_successor_carries_no_fence(
    api: TestClient, auth_headers: dict[str, str]
) -> None:
    item = _admit(api, _agent(api, auth_headers), 54290)
    epoch = _run(api, item.request_id)
    _fail(api, item.request_id, epoch, "model_unreachable")
    successor = _successor(item.request_id)
    assert successor is not None and successor["resource_gate"] is None
    generation, grant = _offer(api, successor["id"])
    assert grant["predecessor_claim_name"] is None
    # Without a gate there is no gate to wait on.
    refused = _defer(api, successor["id"], generation, "resource_gate_unmet", capacity=False)
    assert _refusal(refused) == "not_dispatchable"
    # An automatic successor may still report its predecessor present.
    deferred = _defer(
        api, successor["id"], generation, "predecessor_runtime_present", capacity=False
    )
    assert deferred.status_code == 200, deferred.text


# -- the finish body -------------------------------------------------------------


def test_finish_body_ties_exhausted_volume_to_the_scratch_cause(
    api: TestClient, auth_headers: dict[str, str]
) -> None:
    item = _admit(api, _agent(api, auth_headers), 54291)
    epoch = _run(api, item.request_id)
    for cause, volume in (
        ("sandbox_memory_exhausted", "workspace"),
        ("model_unreachable", "workspace"),
        ("sandbox_scratch_exhausted", None),
        ("sandbox_scratch_exhausted", "Work_Space"),
        ("sandbox_scratch_exhausted", "-workspace"),
    ):
        refused = _finish(api, item.request_id, epoch, cause, exhausted_volume=volume)
        assert refused.status_code == 422, (cause, volume, refused.text)
    row = _request(item.request_id)
    assert (row["status"], row["delivered_limits"].get("exhausted_volume")) == ("running", None)

    # Liveness: the scratch cause with a valid volume settles and is recorded.
    _fail(api, item.request_id, epoch, "sandbox_scratch_exhausted", volume="workspace")
    assert _request(item.request_id)["delivered_limits"]["exhausted_volume"] == "workspace"
    assert _successor(item.request_id) is not None
