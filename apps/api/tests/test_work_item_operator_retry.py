"""An operator retries a failed factory WorkItem through the API (#5431, ADR 0269).

Every scenario drives the real paths: the real app from ``create_app()``, the
real ``POST /work-items/{id}/retry`` route under administrative authentication,
requests admitted through the internal admissions route, started through the
real reconciler pass and worker routes, and failed through the real settlement
entry points (the finish route, ``lifecycle.fail_execution`` and the observed
teardown), reusing the helpers of ``test_factory_retry.py`` and
``test_factory_publication_recovery.py``. Postgres and Valkey are real. Only
state outside the code under test is arranged by SQL: a publication of the same
conversation, a failure that settled before automatic recovery existed, an
unobserved teardown and a pending relabel.

Each refusal runs beside a liveness control on the same fixture shape that
admits, and asserts that the refusal wrote nothing.
"""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import Callable, Iterator
from datetime import timedelta
from typing import Any

import httpx
import pytest
import redis
from curie_api.config import get_settings
from curie_api.crud import console as crud_console
from curie_api.main import create_app
from curie_api.models import Principal
from curie_api.workitems import lifecycle
from curie_api.workitems.lifecycle import AUTOMATIC_RETRY_LIMIT
from fastapi.testclient import TestClient
from forge_fakes.github_comments import admitted, comments
from sqlalchemy.ext.asyncio import AsyncSession
from test_factory_publication_recovery import _candidate, _captured, _settle
from test_factory_retry import (
    TRACKER_FIXTURES,
    Ref,
    _admit,
    _agent,
    _call,
    _configure,
    _db,
    _dispatch,
    _execute,
    _expire_wait,
    _fail,
    _finish,
    _lose_owner,
    _reconcile,
    _request,
    _requests,
    _rows,
    _successor,
    _tracker_issue,
    _used,
    _versions,
)
from test_factory_terminus import _start_running
from test_publication_candidates import ARCHIVE_LIMIT
from test_workitems import _attach_own_publication

# The tracker fixtures pytest resolves by name for the tracker scenario below.
TRACKER = (TRACKER_FIXTURES, admitted, comments)
REASON = "The provider outage is over"
SESSION_COOKIE = "__Host-curie_console_session"
DEFAULT_TENANT_ID = uuid.UUID("00000000-0000-0000-0000-000000000001")


@pytest.fixture
def api(
    clean_db: None, runs_stream: str, valkey: redis.Redis, monkeypatch: pytest.MonkeyPatch
) -> Iterator[TestClient]:
    _configure(monkeypatch)
    monkeypatch.setenv("CURIE_PUBLICATION_CANDIDATES_ENABLED", "true")
    monkeypatch.setenv("CURIE_WORKSPACE_MAX_ARCHIVE_BYTES", str(ARCHIVE_LIMIT))
    monkeypatch.delenv("CURIE_PUBLICATION_CANDIDATE_MAX_BYTES", raising=False)
    get_settings.cache_clear()
    with TestClient(create_app()) as client:
        yield client
    get_settings.cache_clear()


# -- harness -------------------------------------------------------------------


def _admin() -> dict[str, str]:
    return {"X-API-Key": get_settings().api_key}


def _version(work_item_id: uuid.UUID) -> int:
    (row,) = _rows("SELECT version FROM curie.work_items WHERE id = :id", {"id": work_item_id})
    return int(row["version"])


def _retry(
    api: TestClient,
    work_item_id: uuid.UUID,
    *,
    request_id: uuid.UUID | None = None,
    expected_version: int | None = None,
    reason: str = REASON,
    dry_run: bool = False,
    headers: dict[str, str] | None = None,
) -> httpx.Response:
    body = {
        "request_id": str(request_id or uuid.uuid4()),
        "expected_version": (
            _version(work_item_id) if expected_version is None else expected_version
        ),
        "reason": reason,
        "dry_run": dry_run,
    }
    response = api.post(
        f"/work-items/{work_item_id}/retry",
        json=body,
        headers=_admin() if headers is None else headers,
    )
    assert response.headers.get("cache-control") == "no-store", response.headers
    return response


def _audits(work_item_id: uuid.UUID | None = None) -> list[dict[str, Any]]:
    if work_item_id is None:
        return _rows("SELECT * FROM curie.work_item_operator_retries ORDER BY created_at")
    return _rows(
        "SELECT * FROM curie.work_item_operator_retries WHERE work_item_id = :id",
        {"id": work_item_id},
    )


def _state(work_item_id: uuid.UUID) -> dict[str, Any]:
    """Everything a refusal must leave alone, for one WorkItem."""

    (item,) = _rows(
        "SELECT version, automatic_retries_used, next_sequence, cancelled_at, "
        "readmit_request_id FROM curie.work_items WHERE id = :id",
        {"id": work_item_id},
    )
    return {
        "item": item,
        "requests": _requests(work_item_id),
        "notices": _rows(
            "SELECT execution_request_id, applied_label FROM curie.factory_terminal_notices "
            "WHERE work_item_id = :id ORDER BY created_at",
            {"id": work_item_id},
        ),
        "audits": _audits(work_item_id),
    }


def _refused(response: httpx.Response, code: str, status: int = 409) -> dict[str, Any]:
    assert response.status_code == status, response.text
    detail = response.json()["detail"]
    assert isinstance(detail, dict)
    assert detail["code"] == code, detail
    if status == 409:
        assert isinstance(detail["message"], str) and detail["message"]
        assert REASON not in detail["message"]
    return detail


def _synthesize_old_failure(api: TestClient, ref: Ref, cause: str = "model_unreachable") -> None:
    """A failure settled before automatic recovery existed: failed, typed, no successor.

    The request is started through the real routes, then settled by SQL the way
    an upgraded install carries it, with the guard lifted for this write only.
    """

    _dispatch(api, ref.request_id)
    _execute(
        "UPDATE curie.execution_requests SET status = 'failed', "
        "terminal_at = clock_timestamp(), terminal_cause = :cause, "
        "retry_safety = 'no_effects', version = version + 1, updated_at = clock_timestamp() "
        "WHERE id = :id AND status = 'running'",
        {"id": ref.request_id, "cause": cause},
        guarded=True,
    )
    assert _request(ref.request_id)["status"] == "failed"
    assert _successor(ref.request_id) is None


def _eligible(api: TestClient, agent_id: uuid.UUID, issue: int) -> Ref:
    ref = _admit(api, agent_id, issue)
    _synthesize_old_failure(api, ref)
    return ref


def _conversation_publication(ref: Ref, status: str) -> uuid.UUID:
    """A publication of the WorkItem's conversation that no request of it owns."""

    async def body(session: AsyncSession) -> None:
        await _attach_own_publication(session, ref.work_item_id, ref.request_id, status=status)

    _db(body)
    (row,) = _rows(
        "SELECT id FROM curie.publications WHERE execution_request_id = :id",
        {"id": ref.request_id},
    )
    publication_id = row["id"]
    _execute(
        "UPDATE curie.publications SET execution_request_id = NULL WHERE id = :id",
        {"id": publication_id},
    )
    assert isinstance(publication_id, uuid.UUID)
    return publication_id


def _settle_publication(publication_id: uuid.UUID) -> None:
    _execute(
        "UPDATE curie.publications SET status = 'failed' WHERE id = :id",
        {"id": publication_id},
    )


def _assert_admitted(
    response: httpx.Response,
    ref: Ref,
    *,
    used: int,
    version_before: int,
    disposition: str = "implementation",
    cause: str = "model_unreachable",
) -> dict[str, Any]:
    assert response.status_code == 200, response.text
    body = response.json()
    successor = _successor(ref.request_id)
    assert successor is not None
    assert successor["status"] == "waiting"
    assert successor["retry_of_request_id"] == ref.request_id
    assert successor["execution_kind"] == disposition
    assert body["work_item_id"] == str(ref.work_item_id)
    assert (body["dry_run"], body["replayed"]) == (False, False)
    assert body["disposition"] == disposition
    assert body["predecessor_request_id"] == str(ref.request_id)
    assert body["predecessor_cause"] == cause
    assert body["successor_request_id"] == str(successor["id"])
    assert body["successor_blocked_on"] == successor["blocked_on"]
    assert body["retries_used"] == used == _used(ref.work_item_id)
    assert body["retry_limit"] == AUTOMATIC_RETRY_LIMIT
    assert body["work_item_version"] == _version(ref.work_item_id) > version_before
    assert body["recorded_at"] is not None
    # Exactly one new request, and its status comment row is the only new one;
    # no label was ever applied by this operation.
    notices = _rows(
        "SELECT applied_label FROM curie.factory_terminal_notices WHERE execution_request_id = :id",
        {"id": successor["id"]},
    )
    assert notices == [{"applied_label": None}]
    (audit,) = _audits(ref.work_item_id)
    assert audit["request_id"] == uuid.UUID(body["request_id"])
    assert audit["successor_request_id"] == successor["id"]
    assert audit["predecessor_request_id"] == ref.request_id
    assert audit["disposition"] == disposition
    assert audit["reason"] == REASON
    assert audit["result"] == body
    return body


def _assert_unchanged(ref: Ref, before: dict[str, Any]) -> None:
    assert _state(ref.work_item_id) == before


# -- AC1: admission ----------------------------------------------------------------


def test_clean_failure_refused_behind_a_publication_admits_once_it_settles(
    api: TestClient, auth_headers: dict[str, str]
) -> None:
    item = _admit(api, _agent(api, auth_headers), 54310)
    epoch = _dispatch(api, item.request_id)
    publication = _conversation_publication(item, "pending")
    finished = _finish(api, item.request_id, epoch, "model_error")
    assert finished.status_code == 200, finished.text
    # Settlement refused the automatic successor: the publication owned the terminus.
    assert _successor(item.request_id) is None
    assert _used(item.work_item_id) == 0

    before = _state(item.work_item_id)
    detail = _refused(_retry(api, item.work_item_id), "admission_refused")
    assert (detail["cause"], detail["disposition"]) == ("model_error", "retry_backoff")
    _assert_unchanged(item, before)

    _settle_publication(publication)
    version = _version(item.work_item_id)
    response = _retry(api, item.work_item_id)
    _assert_admitted(response, item, used=1, version_before=version, cause="model_error")
    assert len(_requests(item.work_item_id)) == 2


def test_failure_settled_before_automatic_recovery_admits_one_request(
    api: TestClient, auth_headers: dict[str, str]
) -> None:
    item = _eligible(api, _agent(api, auth_headers), 54311)
    version = _version(item.work_item_id)
    response = _retry(api, item.work_item_id)
    body = _assert_admitted(response, item, used=1, version_before=version)
    successor = _successor(item.request_id)
    assert successor is not None
    # The automatic backoff of slot 1, not an immediate start.
    due = successor["dispatch_not_before"] - _request(item.request_id)["terminal_at"]
    assert due >= timedelta(seconds=24)
    assert body["successor_dispatch_not_before"] is not None
    assert len(_requests(item.work_item_id)) == 2


def test_retained_candidate_admits_publication_recovery_and_no_implementation(
    api: TestClient, auth_headers: dict[str, str]
) -> None:
    run, reservation = _captured(api, _agent(api, auth_headers), 54312)
    publication = _conversation_publication(run.ref, "pending")
    _settle(run.ref, "publication_failed")
    assert _successor(run.ref.request_id) is None
    assert _candidate(reservation)["owner_request_id"] == run.ref.request_id

    before = _state(run.ref.work_item_id)
    detail = _refused(_retry(api, run.ref.work_item_id), "admission_refused")
    assert (detail["cause"], detail["disposition"]) == (
        "publication_failed",
        "publication_recovery",
    )
    _assert_unchanged(run.ref, before)

    _settle_publication(publication)
    version = _version(run.ref.work_item_id)
    response = _retry(api, run.ref.work_item_id)
    _assert_admitted(
        response,
        run.ref,
        used=1,
        version_before=version,
        disposition="publication_recovery",
        cause="publication_failed",
    )
    successor = _successor(run.ref.request_id)
    assert successor is not None
    candidate = _candidate(reservation)
    assert successor["recovery_candidate_id"] == candidate["id"]
    assert candidate["owner_request_id"] == successor["id"]
    assert candidate["previous_owner_request_id"] == run.ref.request_id
    assert [row["execution_kind"] for row in _requests(run.ref.work_item_id)] == [
        "implementation",
        "publication_recovery",
    ]


def test_owner_loss_admits_with_observed_teardown_and_refuses_while_unconfirmed(
    api: TestClient, auth_headers: dict[str, str]
) -> None:
    item = _admit(api, _agent(api, auth_headers), 54313)
    _dispatch(api, item.request_id)
    publication = _conversation_publication(item, "pending")
    _lose_owner(api, item.request_id, started=True)
    assert _successor(item.request_id) is None
    _settle_publication(publication)

    # The same failed owner loss, while its teardown is not observed, refuses.
    _execute(
        "UPDATE curie.execution_requests SET teardown_unconfirmed_at = clock_timestamp() "
        "WHERE id = :id",
        {"id": item.request_id},
        guarded=True,
    )
    before = _state(item.work_item_id)
    detail = _refused(_retry(api, item.work_item_id), "teardown_unconfirmed")
    assert detail["request_id"] == str(item.request_id)
    _assert_unchanged(item, before)

    _execute(
        "UPDATE curie.execution_requests SET teardown_unconfirmed_at = NULL WHERE id = :id",
        {"id": item.request_id},
        guarded=True,
    )
    assert _request(item.request_id)["termination_observation"]
    version = _version(item.work_item_id)
    response = _retry(api, item.work_item_id)
    _assert_admitted(response, item, used=1, version_before=version, cause="owner_lost")
    successor = _successor(item.request_id)
    assert successor is not None and successor["owner_lost_retry"] is True


# -- idempotency ---------------------------------------------------------------------


def test_replay_answers_the_stored_result_and_changed_facts_conflict(
    api: TestClient, auth_headers: dict[str, str]
) -> None:
    agent_id = _agent(api, auth_headers)
    item = _eligible(api, agent_id, 54314)
    other = _eligible(api, agent_id, 54315)
    key = uuid.uuid4()
    version = _version(item.work_item_id)
    first = _retry(api, item.work_item_id, request_id=key, expected_version=version)
    body = _assert_admitted(first, item, used=1, version_before=version)
    assert _version(item.work_item_id) > version

    # The same facts after the version advanced: the stored result, replayed.
    # Surrounding whitespace is stripped before hashing, so it is the same reason.
    for reason in (REASON, f"  {REASON}\n"):
        again = _retry(
            api, item.work_item_id, request_id=key, expected_version=version, reason=reason
        )
        assert again.status_code == 200, again.text
        assert again.json() == {**body, "replayed": True}
    after = _state(item.work_item_id)

    session = _console_session(api, auth_headers)
    for changed in (
        {"reason": "A different reason"},
        {"expected_version": version + 1},
        {"headers": session},
    ):
        response = _retry(
            api,
            item.work_item_id,
            request_id=key,
            **{"expected_version": version, **changed},  # type: ignore[arg-type]
        )
        _refused(response, "idempotency_conflict")
    # The same UUID on another WorkItem conflicts too.
    _refused(_retry(api, other.work_item_id, request_id=key), "idempotency_conflict")
    assert _state(item.work_item_id) == after
    assert _successor(other.request_id) is None
    # A fresh UUID on the other WorkItem still admits.
    other_version = _version(other.work_item_id)
    _assert_admitted(_retry(api, other.work_item_id), other, used=1, version_before=other_version)


async def _race(app: Any, work_item_id: uuid.UUID, body: dict[str, Any]) -> list[dict[str, Any]]:
    url = f"/work-items/{work_item_id}/retry"
    headers = _admin()
    async with (
        httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://api") as one,
        httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://api") as two,
    ):
        responses = await asyncio.gather(
            one.post(url, json=body, headers=headers), two.post(url, json=body, headers=headers)
        )
    for response in responses:
        assert response.status_code == 200, response.text
    return [response.json() for response in responses]


def test_simultaneous_identical_retries_create_exactly_one_request(
    api: TestClient, auth_headers: dict[str, str]
) -> None:
    item = _eligible(api, _agent(api, auth_headers), 54316)
    body = {
        "request_id": str(uuid.uuid4()),
        "expected_version": _version(item.work_item_id),
        "reason": REASON,
    }
    answers = _call(api, _race, api.app, item.work_item_id, body)
    assert sorted(answer["replayed"] for answer in answers) == [False, True]
    first, second = answers
    assert {**first, "replayed": False} == {**second, "replayed": False}
    assert len(_requests(item.work_item_id)) == 2
    assert _successor(item.request_id) is not None
    assert _used(item.work_item_id) == 1
    assert len(_audits(item.work_item_id)) == 1


# -- AC2: refusals ---------------------------------------------------------------------


def _stale(api: TestClient, ref: Ref) -> dict[str, Any]:
    return {"expected_version": _version(ref.work_item_id) - 1}


def _cancelled(api: TestClient, ref: Ref) -> dict[str, Any]:
    work_version, _ = _versions(ref.request_id)

    async def cancel(session: AsyncSession) -> lifecycle.WorkItemResult:
        return await lifecycle.request_cancellation(
            session, work_item_id=ref.work_item_id, expected_work_item_version=work_version
        )

    assert isinstance(_db(cancel), lifecycle.WorkItemOutcome)
    return {}


def _relabel_pending(api: TestClient, ref: Ref) -> dict[str, Any]:
    _execute(
        "UPDATE curie.work_items SET readmit_request_id = :rid, readmit_requester = 'U0REQUEST1', "
        "readmit_objective = 'Implement the admitted work item' WHERE id = :id",
        {"rid": uuid.uuid4(), "id": ref.work_item_id},
    )
    return {}


SETUPS: dict[str, Callable[[TestClient, Ref], dict[str, Any]]] = {
    "stale_version": _stale,
    "work_item_cancelled": _cancelled,
    "readmit_pending": _relabel_pending,
}


@pytest.mark.parametrize("code", sorted(SETUPS))
def test_work_item_fences_refuse_and_write_nothing(
    api: TestClient, auth_headers: dict[str, str], code: str
) -> None:
    agent_id = _agent(api, auth_headers)
    fenced = _eligible(api, agent_id, 54320)
    control = _eligible(api, agent_id, 54321)
    overrides = SETUPS[code](api, fenced)
    before = _state(fenced.work_item_id)
    detail = _refused(_retry(api, fenced.work_item_id, **overrides), code)
    if code == "stale_version":
        assert detail["current_version"] == _version(fenced.work_item_id)
    _assert_unchanged(fenced, before)
    assert _audits() == []
    version = _version(control.work_item_id)
    _assert_admitted(_retry(api, control.work_item_id), control, used=1, version_before=version)


def test_active_execution_and_unfailed_newest_request_refuse(
    api: TestClient, auth_headers: dict[str, str]
) -> None:
    agent_id = _agent(api, auth_headers)
    # Automatic recovery already admitted a successor: it is the active request.
    retried = _admit(api, agent_id, 54322)
    _fail(api, retried.request_id)
    successor = _successor(retried.request_id)
    assert successor is not None
    # A request that expired while waiting never authorizes a replay.
    expired = _admit(api, agent_id, 54323)
    _expire_wait(expired.request_id)
    _reconcile()
    assert _request(expired.request_id)["status"] == "expired"
    control = _eligible(api, agent_id, 54324)

    before = _state(retried.work_item_id)
    detail = _refused(_retry(api, retried.work_item_id), "active_execution")
    assert (detail["request_id"], detail["status"]) == (str(successor["id"]), "waiting")
    _assert_unchanged(retried, before)

    before = _state(expired.work_item_id)
    detail = _refused(_retry(api, expired.work_item_id), "no_failed_request")
    assert (detail["latest_status"], detail["cause"]) == ("expired", "capacity_wait_expired")
    _assert_unchanged(expired, before)

    assert _audits() == []
    version = _version(control.work_item_id)
    _assert_admitted(_retry(api, control.work_item_id), control, used=1, version_before=version)


def test_exhausted_allowance_names_the_used_count_and_cause(
    api: TestClient, auth_headers: dict[str, str]
) -> None:
    agent_id = _agent(api, auth_headers)
    item = _admit(api, agent_id, 54325)
    control = _eligible(api, agent_id, 54326)
    current = item.request_id
    for _ in range(AUTOMATIC_RETRY_LIMIT):
        _fail(api, current)
        successor = _successor(current)
        assert successor is not None
        current = successor["id"]
    _fail(api, current)
    assert _successor(current) is None

    before = _state(item.work_item_id)
    detail = _refused(_retry(api, item.work_item_id), "allowance_exhausted")
    assert (detail["retries_used"], detail["retry_limit"], detail["cause"]) == (
        AUTOMATIC_RETRY_LIMIT,
        AUTOMATIC_RETRY_LIMIT,
        "model_unreachable",
    )
    assert detail["message"].startswith(
        f"All {AUTOMATIC_RETRY_LIMIT} of {AUTOMATIC_RETRY_LIMIT} automatic retries"
    )
    assert "model_unreachable" in detail["message"]
    _assert_unchanged(item, before)
    version = _version(control.work_item_id)
    _assert_admitted(_retry(api, control.work_item_id), control, used=1, version_before=version)


@pytest.mark.parametrize(
    ("cause", "safety", "code", "fact"),
    [
        ("no_pull_request", "no_effects", "not_retryable", ("disposition", "terminal")),
        ("budget_exceeded", "no_effects", "not_retryable", ("disposition", "terminal")),
        ("model_unreachable", "flagged", "side_effects_unproven", ("retry_safety", "flagged")),
        ("model_unreachable", "uncertain", "side_effects_unproven", ("retry_safety", "uncertain")),
    ],
)
def test_unretryable_or_unproven_failures_refuse(
    api: TestClient,
    auth_headers: dict[str, str],
    cause: str,
    safety: str,
    code: str,
    fact: tuple[str, str],
) -> None:
    agent_id = _agent(api, auth_headers)
    item = _admit(api, agent_id, 54327)
    control = _admit(api, agent_id, 54328)
    epoch = _dispatch(api, item.request_id)
    finished = _finish(api, item.request_id, epoch, cause, safety)
    assert finished.status_code == 200, finished.text
    assert _request(item.request_id)["status"] == "failed"
    assert _successor(item.request_id) is None

    before = _state(item.work_item_id)
    detail = _refused(_retry(api, item.work_item_id), code)
    assert detail["cause"] == cause
    assert detail[fact[0]] == fact[1]
    _assert_unchanged(item, before)

    # The same shape with a proved clean, retryable failure admits.
    _synthesize_old_failure(api, control)
    version = _version(control.work_item_id)
    _assert_admitted(_retry(api, control.work_item_id), control, used=1, version_before=version)


def test_unknown_work_item_is_not_found(api: TestClient) -> None:
    missing = uuid.uuid4()
    response = _retry(api, missing, expected_version=1)
    assert response.status_code == 404, response.text
    assert response.json() == {"detail": {"code": "not_found"}}


def test_invalid_bodies_are_422_and_write_nothing(
    api: TestClient, auth_headers: dict[str, str]
) -> None:
    item = _eligible(api, _agent(api, auth_headers), 54329)
    before = _state(item.work_item_id)
    version = _version(item.work_item_id)
    base = {"request_id": str(uuid.uuid4()), "expected_version": version, "reason": REASON}
    for body in (
        {**base, "reason": "   "},
        {**base, "reason": "x" * 501},
        {**base, "expected_version": 0},
        {**base, "request_id": "not-a-uuid"},
        {**base, "actor_kind": "platform_key"},
        {key: value for key, value in base.items() if key != "reason"},
    ):
        response = api.post(f"/work-items/{item.work_item_id}/retry", json=body, headers=_admin())
        assert response.status_code == 422, (body, response.text)
        assert response.headers.get("cache-control") == "no-store"
    _assert_unchanged(item, before)
    ok = {**base, "reason": "x" * 500}
    response = api.post(f"/work-items/{item.work_item_id}/retry", json=ok, headers=_admin())
    assert response.status_code == 200, response.text


# -- dry run -----------------------------------------------------------------------------


def test_dry_run_reports_the_same_plan_and_persists_nothing(
    api: TestClient, auth_headers: dict[str, str]
) -> None:
    item = _eligible(api, _agent(api, auth_headers), 54330)
    key = uuid.uuid4()
    before = _state(item.work_item_id)
    version = _version(item.work_item_id)
    planned = _retry(api, item.work_item_id, request_id=key, dry_run=True)
    assert planned.status_code == 200, planned.text
    plan = planned.json()
    assert (plan["dry_run"], plan["replayed"]) == (True, False)
    assert (plan["successor_request_id"], plan["recorded_at"]) == (None, None)
    assert plan["disposition"] == "implementation"
    assert plan["retries_used"] == 1
    assert plan["work_item_version"] > version
    # No request, slot, version, status comment or audit row; the UUID is free.
    _assert_unchanged(item, before)
    assert _audits() == []

    real = _retry(api, item.work_item_id, request_id=key)
    body = _assert_admitted(real, item, used=1, version_before=version)
    for field in (
        "disposition",
        "predecessor_request_id",
        "predecessor_cause",
        "successor_blocked_on",
        "work_item_version",
        "retries_used",
        "retry_limit",
        "actor_kind",
    ):
        assert body[field] == plan[field], field


def test_dry_run_refuses_exactly_as_the_real_call(
    api: TestClient, auth_headers: dict[str, str]
) -> None:
    item = _eligible(api, _agent(api, auth_headers), 54331)
    _relabel_pending(api, item)
    before = _state(item.work_item_id)
    _refused(_retry(api, item.work_item_id, dry_run=True), "readmit_pending")
    _refused(_retry(api, item.work_item_id), "readmit_pending")
    _assert_unchanged(item, before)


# -- authentication ------------------------------------------------------------------------


def _console_session(api: TestClient, auth_headers: dict[str, str]) -> dict[str, str]:
    minted = api.post("/console/login-codes", json={"subject": "U0EXAMPLE1"}, headers=auth_headers)
    assert minted.status_code == 201, minted.text
    exchanged = api.post("/console/session", json={"code": minted.json()["code"]})
    assert exchanged.status_code == 200, exchanged.text
    token = api.cookies.get(SESSION_COOKIE)
    assert isinstance(token, str) and token
    api.cookies.clear()
    return {"Cookie": f"{SESSION_COOKIE}={token}", "Origin": "http://testserver"}


def _principal_session(api: TestClient, monkeypatch: pytest.MonkeyPatch) -> tuple[str, uuid.UUID]:
    """A live session bound to an active principal of the configured issuer."""

    issuer = "https://idp.example.test"
    for key, value in {
        "CURIE_OIDC_ISSUER": issuer,
        "CURIE_OIDC_AUDIENCE": "curie-console",
        "CURIE_OIDC_JWKS_URL": f"{issuer}/jwks",
        "CURIE_OIDC_CLIENT_SECRET": "placeholder-secret",
        "CURIE_OIDC_REDIRECT_URI": "http://testserver/console/oidc/callback",
    }.items():
        monkeypatch.setenv(key, value)
    get_settings.cache_clear()

    async def mint(session: AsyncSession) -> tuple[str, uuid.UUID]:
        principal = Principal(
            tenant_id=DEFAULT_TENANT_ID,
            idp_issuer=issuer,
            idp_subject=f"subject-{uuid.uuid4().hex[:8]}",
            type="human",
            email="operator@example.com",
        )
        session.add(principal)
        await session.flush()
        token, _row = await crud_console.create_principal_console_session(session, principal)
        return token, principal.id

    return _db(mint)


def test_unauthenticated_and_revoked_callers_are_401_and_write_nothing(
    api: TestClient, auth_headers: dict[str, str]
) -> None:
    item = _eligible(api, _agent(api, auth_headers), 54332)
    session = _console_session(api, auth_headers)
    _execute("UPDATE curie.console_sessions SET revoked_at = now()")
    before = _state(item.work_item_id)
    for headers in ({}, {"X-API-Key": "wrong-key"}, session):
        response = _retry(api, item.work_item_id, headers=headers)
        assert response.status_code == 401, response.text
    _assert_unchanged(item, before)
    assert _audits() == []


def test_actor_is_derived_from_the_credential_and_reads_still_pass(
    api: TestClient, auth_headers: dict[str, str], monkeypatch: pytest.MonkeyPatch
) -> None:
    agent_id = _agent(api, auth_headers)
    keyed = _eligible(api, agent_id, 54333)
    by_session = _eligible(api, agent_id, 54334)
    by_principal = _eligible(api, agent_id, 54335)

    session = _console_session(api, auth_headers)
    (row,) = _rows("SELECT id FROM curie.console_sessions WHERE revoked_at IS NULL")
    token, principal_id = _principal_session(api, monkeypatch)
    principal = {"Cookie": f"{SESSION_COOKIE}={token}", "Origin": "http://testserver"}

    # The require_api_key read routes still take all three credentials.
    for headers in (_admin(), session, principal):
        listed = api.get("/work-items", headers=headers)
        assert listed.status_code == 200, listed.text

    for ref, headers, kind, subject in (
        (keyed, _admin(), "platform_key", None),
        (by_session, session, "console_session", f"session:{row['id']}"),
        (by_principal, principal, "console_session", f"principal:{principal_id}"),
    ):
        response = _retry(api, ref.work_item_id, headers=headers)
        assert response.status_code == 200, response.text
        assert response.json()["actor_kind"] == kind
        (audit,) = _audits(ref.work_item_id)
        assert (audit["actor_kind"], audit["actor_subject"]) == (kind, subject)

    # A disabled principal closes both the read and the retry route.
    _execute("UPDATE curie.principals SET status = 'disabled' WHERE id = :id", {"id": principal_id})
    assert api.get("/work-items", headers=principal).status_code == 401
    other = _eligible(api, agent_id, 54336)
    assert _retry(api, other.work_item_id, headers=principal).status_code == 401
    assert _audits(other.work_item_id) == []


# -- tracker -------------------------------------------------------------------------------


def test_a_tracker_backed_retry_writes_nothing_to_the_tracker(admitted: Any) -> None:
    client, github, sink = admitted
    refused = _tracker_issue(client, github, 54340)
    item = _tracker_issue(client, github, 54341)
    for ref in (refused, item):
        _start_running(client, ref.request_id)
        _execute(
            "UPDATE curie.execution_requests SET status = 'failed', "
            "terminal_at = clock_timestamp(), terminal_cause = 'model_unreachable', "
            "retry_safety = :safety, version = version + 1 WHERE id = :id",
            {"id": ref.request_id, "safety": "flagged" if ref is refused else "no_effects"},
            guarded=True,
        )
    calls = len(sink.requests)
    labels = {number: set(found) for number, found in sink.issue_labels.items()}

    before = _state(refused.work_item_id)
    _refused(_retry(client, refused.work_item_id), "side_effects_unproven")
    _assert_unchanged(refused, before)
    version = _version(item.work_item_id)
    _assert_admitted(_retry(client, item.work_item_id), item, used=1, version_before=version)

    # No label, comment or any other tracker call from either answer.
    assert len(sink.requests) == calls
    assert {number: set(found) for number, found in sink.issue_labels.items()} == labels
