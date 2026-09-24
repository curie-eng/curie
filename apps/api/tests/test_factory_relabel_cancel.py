"""Relabel starts a new run, unlabel cancels, and a cancel always settles (#3072).

Payload shapes follow GitHub's webhook catalog:
https://docs.github.com/en/webhooks/webhook-events-and-payloads#issues
Machine fixtures drive the events. They are not human-authored GitHub proof.

The worker half of "unlabel deletes its sandbox claim" is covered by
apps/worker/tests/sandbox/test_substrate.py::
test_terminate_thread_observes_absence_of_labelled_and_sql_names.
"""

from __future__ import annotations

import asyncio
import json
import sys
import uuid
from pathlib import Path
from typing import Any

import pytest
import redis

sys.path.insert(0, str(Path(__file__).resolve().parent))

from aci_protocol import STREAM_PAYLOAD_FIELD
from curie_api.config import get_settings
from curie_api.factory_notices import comment_body
from curie_test_support.valkey import VALKEY_HOST, VALKEY_PORT, VALKEY_PW
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine
from test_factory_terminus import (  # noqa: F401  (fixtures)
    _notices,
    _observe_termination,
    _reconcile,
    _rows,
    _start_running,
    admitted,
    comments,
)
from test_github_factory_ingress import LABEL, REPO_ID, SENDER, SENDER_ID, _issue_event, _post

pytestmark = pytest.mark.usefixtures("clean_db")

WORKER = {"X-Curie-Worker-Token": "factory-terminus-worker"}


def _all(number: int) -> list[dict[str, Any]]:
    return _rows(
        "SELECT r.id, r.sequence, r.status, r.terminal_cause, r.requester, "
        "r.runtime_epoch, r.runtime_owner, r.termination_observation, "
        "r.cancellation_requested_at, w.id AS work_item_id, w.cancelled_at, "
        "w.readmit_request_id, w.readmit_requester, w.readmit_objective "
        "FROM curie.execution_requests r "
        "JOIN curie.work_items w ON w.id = r.work_item_id "
        "WHERE w.github_repository_id = :repo AND w.github_issue_number = :number "
        "ORDER BY r.sequence",
        {"repo": REPO_ID, "number": number},
    )


def _labelled(client: Any, github: Any, number: int) -> Any:
    github.issue_number = number
    github.labels = [LABEL]
    return _post(
        client,
        "issues",
        _issue_event("labeled", number, label={"name": LABEL}),
        delivery=str(uuid.uuid4()),
    )


def _unlabelled(client: Any, github: Any, number: int) -> Any:
    github.labels = []
    return _post(client, "issues", _issue_event("unlabeled", number, label={"name": LABEL}))


def _execute(statement: str, params: dict[str, Any]) -> None:
    async def go() -> None:
        engine = create_async_engine(get_settings().database_url)
        try:
            async with engine.begin() as connection:
                await connection.execute(text(statement), params)
        finally:
            await engine.dispose()

    asyncio.run(go())


def _stream_event_ids() -> list[str]:
    valkey = redis.Redis(host=VALKEY_HOST, port=VALKEY_PORT, password=VALKEY_PW or None)
    try:
        return [
            json.loads(fields[STREAM_PAYLOAD_FIELD.encode()])["event_id"]
            for _id, fields in valkey.xrange(get_settings().runs_stream)
        ]
    finally:
        valkey.close()


# AC5


def test_issue_cancelled_comment_is_a_plain_stop_notice() -> None:
    body = comment_body(uuid.uuid4(), "issue_cancelled", pr_url=None)
    assert "Stopped:" in body
    assert "Could not complete" not in body


# AC1


def test_unlabel_on_running_requests_cancellation_and_publishes_a_terminate_wake(
    admitted: Any,  # noqa: F811 (shared fixture)
) -> None:
    client, github, _sink = admitted
    number = 9601
    assert _labelled(client, github, number).json()["status"] == "factory_admitted"
    request_id = _all(number)[0]["id"]
    _start_running(request_id)
    removed = _unlabelled(client, github, number)
    assert removed.json()["status"] == "factory_cancellation_requested"
    row = _all(number)[0]
    assert (row["status"], row["terminal_cause"]) == ("cancellation_requested", "issue_cancelled")
    assert row["cancellation_requested_at"] is not None
    _reconcile()
    assert f"work-item-{request_id}-terminate" in _stream_event_ids()


# AC2


def test_relabel_after_a_terminal_request_admits_a_second_request(admitted: Any) -> None:  # noqa: F811 (shared fixture)
    client, github, _sink = admitted
    number = 9602
    assert _labelled(client, github, number).json()["status"] == "factory_admitted"
    first = _all(number)[0]
    epoch = _start_running(first["id"])
    failed = client.post(
        f"/v1/internal/work-items/requests/{first['id']}/finish",
        headers=WORKER,
        json={"runtime_epoch": epoch, "outcome": "failed", "cause": "runner_escalated"},
    )
    assert failed.status_code == 200, failed.text

    again = _labelled(client, github, number)

    assert again.status_code == 200, again.text
    assert again.json()["status"] == "factory_admitted"
    rows = _all(number)
    assert [r["sequence"] for r in rows] == [1, 2]
    assert rows[0]["work_item_id"] == rows[1]["work_item_id"]
    assert rows[1]["status"] == "waiting"


def test_unlabel_then_relabel_clears_cancelled_at_and_admits_a_new_request(
    admitted: Any,  # noqa: F811 (shared fixture)
) -> None:
    client, github, _sink = admitted
    number = 9603
    assert _labelled(client, github, number).json()["status"] == "factory_admitted"
    assert _unlabelled(client, github, number).json()["status"] == "factory_cancelled"
    assert _all(number)[0]["cancelled_at"] is not None

    again = _labelled(client, github, number)

    assert again.json()["status"] == "factory_admitted"
    rows = _all(number)
    assert [r["status"] for r in rows] == ["cancelled", "waiting"]
    assert rows[1]["cancelled_at"] is None


# AC3


def test_relabel_while_waiting_supersedes_the_waiting_request(admitted: Any) -> None:  # noqa: F811 (shared fixture)
    client, github, _sink = admitted
    number = 9604
    assert _labelled(client, github, number).json()["status"] == "factory_admitted"

    again = _labelled(client, github, number)

    assert again.json()["status"] == "factory_admitted"
    rows = _all(number)
    assert [(r["status"], r["terminal_cause"]) for r in rows] == [
        ("cancelled", "issue_cancelled"),
        ("waiting", None),
    ]
    assert rows[0]["work_item_id"] == rows[1]["work_item_id"]
    _reconcile()
    assert _notices(rows[0]["id"]) == []


def test_relabel_while_running_readmits_after_the_termination_is_observed(
    admitted: Any,  # noqa: F811 (shared fixture)
) -> None:
    client, github, sink = admitted
    number = 9605
    assert _labelled(client, github, number).json()["status"] == "factory_admitted"
    old_id = _all(number)[0]["id"]
    _start_running(old_id)

    again = _labelled(client, github, number)

    assert again.json()["status"] == "factory_readmit_pending"
    rows = _all(number)
    assert len(rows) == 1
    assert (rows[0]["status"], rows[0]["terminal_cause"]) == (
        "cancellation_requested",
        "issue_cancelled",
    )
    assert rows[0]["readmit_request_id"] is not None
    assert rows[0]["readmit_requester"] == f"github:{SENDER_ID}:{SENDER}"
    assert rows[0]["readmit_objective"]

    _observe_termination(client, old_id)
    _reconcile()

    rows = _all(number)
    assert [r["status"] for r in rows] == ["cancelled", "waiting"]
    assert rows[1]["requester"] == f"github:{SENDER_ID}:{SENDER}"
    assert rows[1]["readmit_request_id"] is None
    assert rows[1]["cancelled_at"] is None
    assert _notices(old_id) == []
    assert sink.posts == 0


# AC6


def test_unlabel_while_a_readmit_is_pending_clears_it(admitted: Any) -> None:  # noqa: F811 (shared fixture)
    client, github, _sink = admitted
    number = 9606
    assert _labelled(client, github, number).json()["status"] == "factory_admitted"
    old_id = _all(number)[0]["id"]
    _start_running(old_id)
    assert _labelled(client, github, number).json()["status"] == "factory_readmit_pending"

    _unlabelled(client, github, number)

    row = _all(number)[0]
    assert row["readmit_request_id"] is None
    assert row["readmit_requester"] is None
    assert row["readmit_objective"] is None
    _observe_termination(client, old_id)
    _reconcile()
    assert len(_all(number)) == 1


# AC4


def _unowned_cancel(client: Any, github: Any, number: int, age_seconds: int) -> dict[str, Any]:
    assert _labelled(client, github, number).json()["status"] == "factory_admitted"
    request_id = _all(number)[0]["id"]
    _start_running(request_id)
    assert _unlabelled(client, github, number).json()["status"] == (
        "factory_cancellation_requested"
    )
    _execute(
        "UPDATE curie.execution_requests SET runtime_owner = NULL, "
        "cancellation_requested_at = clock_timestamp() - make_interval(secs => :age) "
        "WHERE id = :id",
        {"id": request_id, "age": age_seconds},
    )
    return _all(number)[0]


def test_unowned_cancel_settles_after_the_window_and_fences_the_old_epoch(
    admitted: Any,  # noqa: F811 (shared fixture)
    monkeypatch: pytest.MonkeyPatch,  # noqa: F811 (shared fixture)
) -> None:
    monkeypatch.setenv("CURIE_WORK_ITEM_CANCEL_SETTLE_SECONDS", "1")
    get_settings.cache_clear()
    client, github, _sink = admitted
    before = _unowned_cancel(client, github, 9607, age_seconds=30)

    _reconcile()

    after = _all(9607)[0]
    assert (after["status"], after["terminal_cause"]) == ("cancelled", "issue_cancelled")
    assert after["termination_observation"]
    assert after["runtime_epoch"] > before["runtime_epoch"]
    late = client.post(
        f"/v1/internal/work-items/requests/{before['id']}/termination",
        headers=WORKER,
        json={"runtime_epoch": before["runtime_epoch"], "observation": "late teardown"},
    )
    assert late.status_code == 409, late.text
    assert _all(9607)[0]["termination_observation"] == after["termination_observation"]


def test_unowned_cancel_inside_the_window_is_left_alone(
    admitted: Any,  # noqa: F811 (shared fixture)
    monkeypatch: pytest.MonkeyPatch,  # noqa: F811 (shared fixture)
) -> None:
    monkeypatch.setenv("CURIE_WORK_ITEM_CANCEL_SETTLE_SECONDS", "3600")
    get_settings.cache_clear()
    client, github, _sink = admitted
    _unowned_cancel(client, github, 9608, age_seconds=5)

    _reconcile()

    row = _all(9608)[0]
    assert row["status"] == "cancellation_requested"
    assert row["termination_observation"] is None


def test_overdue_cancel_with_a_live_owner_heartbeat_is_not_settled(
    admitted: Any,  # noqa: F811 (shared fixture)
    monkeypatch: pytest.MonkeyPatch,  # noqa: F811 (shared fixture)
) -> None:
    monkeypatch.setenv("CURIE_WORK_ITEM_CANCEL_SETTLE_SECONDS", "1")
    get_settings.cache_clear()
    client, github, _sink = admitted
    number = 9609
    assert _labelled(client, github, number).json()["status"] == "factory_admitted"
    request_id = _all(number)[0]["id"]
    _start_running(request_id)
    assert _unlabelled(client, github, number).json()["status"] == (
        "factory_cancellation_requested"
    )
    _execute(
        "UPDATE curie.execution_requests SET runtime_owner = 'live-worker', "
        "runtime_heartbeat_expires_at = clock_timestamp() + interval '1 hour', "
        "cancellation_requested_at = clock_timestamp() - interval '30 seconds' "
        "WHERE id = :id",
        {"id": request_id},
    )

    _reconcile()

    row = _all(number)[0]
    assert row["status"] == "cancellation_requested"
    assert row["termination_observation"] is None
    published = _rows(
        "SELECT terminate_published_at FROM curie.execution_requests WHERE id = :id",
        {"id": request_id},
    )[0]["terminate_published_at"]
    assert published is not None


def test_unlabel_after_a_suppressed_termination_still_owes_the_stop_notice(
    admitted: Any,  # noqa: F811 (shared fixture)
) -> None:
    client, github, _sink = admitted
    number = 9610
    assert _labelled(client, github, number).json()["status"] == "factory_admitted"
    old_id = _all(number)[0]["id"]
    _start_running(old_id)
    assert _labelled(client, github, number).json()["status"] == "factory_readmit_pending"
    _observe_termination(client, old_id)
    assert _notices(old_id) == []

    assert _unlabelled(client, github, number).json()["status"] == "factory_cancelled"
    _unlabelled(client, github, number)

    rows = _all(number)
    assert len(rows) == 1
    assert rows[0]["readmit_request_id"] is None
    notices = _notices(old_id)
    assert len(notices) == 1


# P1: a forced settle must not strand the sandbox claim.


def _teardown_state(request_id: uuid.UUID) -> dict[str, Any]:
    return _rows(
        "SELECT status, teardown_unconfirmed_at, terminate_published_at, "
        "termination_observation, runtime_epoch "
        "FROM curie.execution_requests WHERE id = :id",
        {"id": request_id},
    )[0]


def _age_terminate_publish(request_id: uuid.UUID) -> None:
    _execute(
        "UPDATE curie.execution_requests "
        "SET terminate_published_at = clock_timestamp() - interval '1 hour' "
        "WHERE id = :id",
        {"id": request_id},
    )


def _terminate_wakes(request_id: uuid.UUID) -> int:
    return _stream_event_ids().count(f"work-item-{request_id}-terminate")


def test_forced_settle_keeps_publishing_teardown_until_a_worker_records_it(
    admitted: Any,  # noqa: F811 (shared fixture)
    monkeypatch: pytest.MonkeyPatch,  # noqa: F811 (shared fixture)
) -> None:
    monkeypatch.setenv("CURIE_WORK_ITEM_CANCEL_SETTLE_SECONDS", "1")
    get_settings.cache_clear()
    client, github, _sink = admitted
    before = _unowned_cancel(client, github, 9611, age_seconds=30)
    request_id = before["id"]

    _reconcile()
    settled = _teardown_state(request_id)
    assert settled["status"] == "cancelled"
    assert settled["teardown_unconfirmed_at"] is not None
    notices = _notices(request_id)
    assert len(notices) == 1

    _age_terminate_publish(request_id)
    wakes = _terminate_wakes(request_id)
    _reconcile()
    assert _terminate_wakes(request_id) == wakes + 1
    assert _teardown_state(request_id)["terminate_published_at"] > settled[
        "terminate_published_at"
    ]

    claimed = client.post(
        f"/v1/internal/work-items/requests/{request_id}/termination/claim",
        headers=WORKER,
        json={"owner": "late-worker"},
    )
    assert claimed.status_code == 200, claimed.text
    assert claimed.json()["runtime_epoch"] > settled["runtime_epoch"]
    recorded = client.post(
        f"/v1/internal/work-items/requests/{request_id}/termination",
        headers=WORKER,
        json={
            "runtime_epoch": claimed.json()["runtime_epoch"],
            "observation": "sandbox claim absent",
        },
    )
    assert recorded.status_code == 200, recorded.text

    done = _teardown_state(request_id)
    assert done["status"] == "cancelled"
    assert done["teardown_unconfirmed_at"] is None
    assert done["termination_observation"] == "sandbox claim absent"
    assert len(_notices(request_id)) == 1

    _age_terminate_publish(request_id)
    wakes = _terminate_wakes(request_id)
    _reconcile()
    assert _terminate_wakes(request_id) == wakes
    again = client.post(
        f"/v1/internal/work-items/requests/{request_id}/termination/claim",
        headers=WORKER,
        json={"owner": "late-worker"},
    )
    assert again.status_code == 409, again.text


def test_forced_settle_teardown_defers_to_an_active_relabeled_sibling(
    admitted: Any,  # noqa: F811 (shared fixture)
    monkeypatch: pytest.MonkeyPatch,  # noqa: F811 (shared fixture)
) -> None:
    monkeypatch.setenv("CURIE_WORK_ITEM_CANCEL_SETTLE_SECONDS", "1")
    get_settings.cache_clear()
    client, github, _sink = admitted
    number = 9612
    before = _unowned_cancel(client, github, number, age_seconds=30)
    request_id = before["id"]
    _reconcile()
    assert _teardown_state(request_id)["teardown_unconfirmed_at"] is not None

    assert _labelled(client, github, number).json()["status"] == "factory_admitted"
    assert [r["status"] for r in _all(number)] == ["cancelled", "waiting"]

    _age_terminate_publish(request_id)
    published = _teardown_state(request_id)["terminate_published_at"]
    wakes = _terminate_wakes(request_id)
    _reconcile()
    assert _terminate_wakes(request_id) == wakes
    assert _teardown_state(request_id)["terminate_published_at"] == published

    refused = client.post(
        f"/v1/internal/work-items/requests/{request_id}/termination/claim",
        headers=WORKER,
        json={"owner": "late-worker"},
    )
    assert refused.status_code == 409, refused.text
    # Left flagged: the teardown resumes once the sibling is terminal.
    assert _teardown_state(request_id)["teardown_unconfirmed_at"] is not None
