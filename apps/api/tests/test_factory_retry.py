"""Failed factory requests share two automatic fresh attempts (#5424, ADR 0269).

Every scenario drives the real paths: the real app from ``create_app()``,
requests admitted through the internal admissions route, the worker's real
``WorkItemDispatchClient`` over ``httpx.ASGITransport`` to acquire, start and
record teardown, the real finish route for the worker's typed facts, and the
real ``WorkItemReconciler.run_once()`` against the per-test runs stream.
Postgres and Valkey are real. Time is moved only by SQL on the stored rows,
inside one transaction that disables the immutable-field guard, as
``test_workitem_dispatch.py`` does; nothing inside the API is patched.

The shared vector file ``tests/vectors/factory-retry-dispositions.json`` is
read by the pure policy test and by the boundary tests here, and by the
worker suite.
"""

from __future__ import annotations

import asyncio
import json
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable, Iterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import httpx
import personal_execution_support as support
import pytest
import redis
import redis.asyncio as aredis
from aci_protocol import STREAM_PAYLOAD_FIELD, QueuedTurn
from channel_protocol.work_item_events import execute_event_id
from curie_api.config import get_settings
from curie_api.factory_comment_text import marker_for
from curie_api.factory_notices import utc_text
from curie_api.factory_retry_policy import RetryFacts, condition_text, decide
from curie_api.main import create_app
from curie_api.workitem_reconciler import WorkItemReconciler
from curie_api.workitems import lifecycle
from curie_worker.binding import FactoryWake
from curie_worker.workitem_dispatch import TerminationObservation, WorkItemDispatchClient
from fastapi.testclient import TestClient
from forge_fakes import factory_wake
from forge_fakes.github_comments import _LABELS, admitted, comments
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from test_factory_terminus import _label as _label_issue
from test_factory_terminus import _notices as _status_rows
from test_factory_terminus import _reconcile as _reconcile_and_sync
from test_factory_terminus import _request as _issue_request
from test_factory_terminus import _start_running
from test_workitems import _attach_own_publication, _seed_transcript, _transcript_threads

WORKER_TOKEN = "factory-retry-worker-token"
WORKER = {"X-Curie-Worker-Token": WORKER_TOKEN}
OWNER = "curie-workers-retry-a"
PEER = "curie-workers-retry-b"
REPO = "acme-corp/acme-bot"
ADDRESS = "C0EXAMPLE1"
OBJECTIVE = "Implement the admitted work item"
REQUESTER = "U0REQUEST1"
VECTORS_PATH = Path(__file__).resolve().parents[3] / "tests/vectors/factory-retry-dispositions.json"
VECTORS: dict[str, Any] = json.loads(VECTORS_PATH.read_text(encoding="utf-8"))
CASES: list[dict[str, Any]] = VECTORS["cases"]
PLACEHOLDER_AT = "2030-01-01T00:00:00Z"
_ABSENT = object()
LIMIT = VECTORS["allowance"]["limit"]


# -- harness -----------------------------------------------------------------


def _configure(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("INTERNAL_WORKER_TOKEN", WORKER_TOKEN)
    monkeypatch.setenv("RESUME_RECONCILER_ENABLED", "false")
    # Each test drives the real reconciler pass itself.
    monkeypatch.setenv("CURIE_WORK_ITEM_RECONCILER_ENABLED", "false")
    monkeypatch.setenv("APPROVAL_SWEEP_INTERVAL_S", "0")
    monkeypatch.setenv("DEAD_LETTER_WATCH_INTERVAL_S", "0")
    monkeypatch.setenv("GITHUB_REPO_ALLOWLIST", '["acme-corp/*"]')
    get_settings.cache_clear()


@pytest.fixture
def retry_env(
    clean_db: None, runs_stream: str, valkey: redis.Redis, monkeypatch: pytest.MonkeyPatch
) -> Iterator[None]:
    _configure(monkeypatch)
    yield
    get_settings.cache_clear()


@pytest.fixture
def api(retry_env: None) -> Iterator[TestClient]:
    with TestClient(create_app()) as client:
        yield client


@dataclass(frozen=True)
class Ref:
    request_id: uuid.UUID
    work_item_id: uuid.UUID


def _db[T](body: Callable[[AsyncSession], Awaitable[T]]) -> T:
    async def go() -> T:
        engine = create_async_engine(get_settings().database_url)
        try:
            async with AsyncSession(engine, expire_on_commit=False) as session:
                return await body(session)
        finally:
            await engine.dispose()

    return asyncio.run(go())


def _rows(statement: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    async def body(session: AsyncSession) -> list[dict[str, Any]]:
        result = await session.execute(text(statement), params or {})
        return [dict(row) for row in result.mappings().all()]

    return _db(body)


def _execute(
    statement: str, params: dict[str, Any] | None = None, *, guarded: bool = False
) -> None:
    """Write rows directly; ``guarded`` lifts the immutable-field guard for this write only."""

    async def body(session: AsyncSession) -> None:
        if guarded:
            await session.execute(text("ALTER TABLE curie.execution_requests DISABLE TRIGGER USER"))
        await session.execute(text(statement), params or {})
        if guarded:
            await session.execute(text("ALTER TABLE curie.execution_requests ENABLE TRIGGER USER"))
        await session.commit()

    _db(body)


def _now() -> datetime:
    (row,) = _rows("SELECT clock_timestamp() AS now")
    value = row["now"]
    assert isinstance(value, datetime)
    return value


def _async_valkey() -> aredis.Redis:
    s = get_settings()
    return aredis.Redis(
        host=s.valkey_host,
        port=s.valkey_port,
        password=s.valkey_password or None,
        decode_responses=True,
    )


def _reconcile() -> None:
    """One real reconciler pass against the per-test runs stream."""

    async def go() -> None:
        engine = create_async_engine(get_settings().database_url)
        valkey = _async_valkey()
        try:
            reconciler = WorkItemReconciler(
                async_sessionmaker(engine, expire_on_commit=False), valkey, get_settings()
            )
            await reconciler.run_once()
        finally:
            await valkey.aclose()
            await engine.dispose()

    asyncio.run(go())


def _wakes() -> list[QueuedTurn]:
    async def go() -> list[QueuedTurn]:
        valkey = _async_valkey()
        try:
            entries = await valkey.xrange(get_settings().runs_stream)
        finally:
            await valkey.aclose()
        return [
            QueuedTurn.model_validate_json(fields[STREAM_PAYLOAD_FIELD]) for _id, fields in entries
        ]

    return asyncio.run(go())


def _wake_ids() -> set[str]:
    return {turn.event_id for turn in _wakes()}


@asynccontextmanager
async def _worker(app: Any) -> AsyncIterator[WorkItemDispatchClient]:
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://api"
    ) as http:
        yield WorkItemDispatchClient(
            api_base_url="http://api", worker_token=WORKER_TOKEN, client=http
        )


def _agent(api: TestClient, auth_headers: dict[str, str]) -> uuid.UUID:
    created = api.post(
        "/agents",
        json={
            "name": f"acme-bot-{uuid.uuid4().hex[:8]}",
            "channel": {"kind": "slack", "address": ADDRESS},
            "repo_full_name": REPO,
        },
        headers=auth_headers,
    )
    assert created.status_code == 201, created.text
    return uuid.UUID(created.json()["id"])


def _admit(api: TestClient, agent_id: uuid.UUID, issue: int) -> Ref:
    admitted = api.post(
        "/v1/internal/work-items/admissions",
        json={
            "agent_id": str(agent_id),
            "kind": "slack",
            "address": ADDRESS,
            # One conversation per issue, so one WorkItem's history is its own.
            "reply_conversation_id": f"1700000000.{issue:06d}",
            "tracker": {
                "kind": "github",
                "host": "github.com",
                "scope_id": "101",
                "issue_id": str(issue),
            },
            "repository": {
                "code_host_kind": "github",
                "host": "github.com",
                "project_id": "101",
                "path": REPO,
            },
            "code_host_installation_id": 202,
            "objective": OBJECTIVE,
            "requester": REQUESTER,
            "request_id": str(uuid.uuid4()),
        },
        headers=WORKER,
    )
    assert admitted.status_code == 200, admitted.text
    body = admitted.json()
    return Ref(uuid.UUID(body["request"]["id"]), uuid.UUID(body["work_item"]["id"]))


def _request(request_id: uuid.UUID) -> dict[str, Any]:
    (row,) = _rows("SELECT * FROM curie.execution_requests WHERE id = :id", {"id": request_id})
    return row


def _requests(work_item_id: uuid.UUID) -> list[dict[str, Any]]:
    return _rows(
        "SELECT * FROM curie.execution_requests WHERE work_item_id = :id ORDER BY sequence",
        {"id": work_item_id},
    )


def _used(work_item_id: uuid.UUID) -> int:
    (row,) = _rows(
        "SELECT automatic_retries_used FROM curie.work_items WHERE id = :id", {"id": work_item_id}
    )
    value = row["automatic_retries_used"]
    assert isinstance(value, int)
    return value


def _successor(request_id: uuid.UUID) -> dict[str, Any] | None:
    found = _rows(
        "SELECT * FROM curie.execution_requests WHERE retry_of_request_id = :id", {"id": request_id}
    )
    assert len(found) <= 1, found
    return found[0] if found else None


def _make_due(request_id: uuid.UUID) -> None:
    _execute(
        "UPDATE curie.execution_requests "
        "SET dispatch_not_before = clock_timestamp() - interval '1 second' "
        "WHERE id = :id AND status = 'waiting'",
        {"id": request_id},
        guarded=True,
    )


def _expire_wait(request_id: uuid.UUID) -> None:
    _execute(
        "UPDATE curie.execution_requests "
        "SET wait_deadline = clock_timestamp() - interval '1 second', "
        "dispatch_not_before = clock_timestamp() - interval '2 seconds' "
        "WHERE id = :id AND status = 'waiting'",
        {"id": request_id},
        guarded=True,
    )


async def _wake(request_id: uuid.UUID) -> FactoryWake:
    engine = create_async_engine(get_settings().database_url)
    try:
        async with AsyncSession(engine) as session:
            wake = await factory_wake(session, request_id)
            return FactoryWake.model_validate(wake.model_dump())
    finally:
        await engine.dispose()


async def _acquire_and_start(app: Any, request_id: uuid.UUID, generation: int) -> int:
    wake = await _wake(request_id)
    async with _worker(app) as client:
        grant = await client.acquire(
            request_id, owner=OWNER, generation=generation, wake=wake, context=None
        )
        started = await client.start(
            request_id,
            owner=OWNER,
            generation=generation,
            claim_name=f"curie-thread-{request_id.hex[:12]}",
            sandbox_name=f"sbx-curie-thread-{request_id.hex[:12]}",
            context=grant.context,
        )
        return started.runtime_epoch


def _published_wake(request_id: uuid.UUID) -> int:
    """The generation the reconciler published an execute wake for."""

    row = _request(request_id)
    generation = row["dispatch_generation"]
    assert isinstance(generation, int)
    assert row["published_generation"] == generation, row
    assert execute_event_id(request_id, generation) in _wake_ids()
    return generation


def _dispatch(api: TestClient, request_id: uuid.UUID) -> int:
    """Make the request due, let the reconciler wake it, then acquire and start it."""

    _make_due(request_id)
    _reconcile()
    generation = _published_wake(request_id)
    return _call(api, _acquire_and_start, api.app, request_id, generation)


def _finish(
    api: TestClient,
    request_id: uuid.UUID,
    epoch: int,
    cause: str,
    safety: str | None | object = "no_effects",
    *,
    condition: object = _ABSENT,
    outcome: str = "failed",
) -> httpx.Response:
    body: dict[str, Any] = {"runtime_epoch": epoch, "outcome": outcome, "cause": cause}
    if safety is not _ABSENT:
        body["retry_safety"] = safety
    if condition is not _ABSENT:
        body["retry_condition"] = condition
    return api.post(
        f"/v1/internal/work-items/requests/{request_id}/finish", json=body, headers=WORKER
    )


def _fail(api: TestClient, request_id: uuid.UUID, cause: str = "model_unreachable") -> None:
    epoch = _dispatch(api, request_id)
    finished = _finish(api, request_id, epoch, cause)
    assert finished.status_code == 200, finished.text


async def _record_teardown(app: Any, request_id: uuid.UUID, owner: str) -> None:
    async with _worker(app) as client:
        claim = await client.claim_termination(request_id, owner=owner)
        await client.record_termination(
            request_id,
            runtime_epoch=claim.runtime_epoch,
            expected_request_version=claim.request_version,
            observation=TerminationObservation(
                claims=(f"curie-thread-{request_id.hex[:12]}",),
                sandboxes=(f"sbx-curie-thread-{request_id.hex[:12]}",),
                observed_at=datetime.now(UTC),
                observer=owner,
            ).render(),
        )


def _lapse_heartbeat(request_id: uuid.UUID) -> None:
    elapsed = timedelta(seconds=get_settings().work_item_runtime_ttl_seconds + 5)
    _execute(
        "UPDATE curie.execution_requests "
        "SET runtime_heartbeat_expires_at = clock_timestamp() - CAST(:elapsed AS interval) "
        "WHERE id = :id AND status IN ('running', 'cancellation_requested')",
        {"id": request_id, "elapsed": elapsed},
    )


def _lose_owner(api: TestClient, request_id: uuid.UUID, *, started: bool = False) -> None:
    """Start the request, lapse its heartbeat and record the observed teardown."""

    if not started:
        _dispatch(api, request_id)
    _lapse_heartbeat(request_id)
    _reconcile()
    row = _request(request_id)
    assert (row["status"], row["terminal_cause"]) == ("cancellation_requested", "owner_lost")
    _call(api, _record_teardown, api.app, request_id, PEER)
    row = _request(request_id)
    assert (row["status"], row["terminal_cause"]) == ("failed", "owner_lost")


def _portal(api: TestClient) -> Any:
    portal = api.portal
    assert portal is not None
    return portal


def _call[T](api: TestClient, fn: Callable[..., Awaitable[T]], *args: Any) -> T:
    result: T = _portal(api).call(fn, *args)
    return result


def _conversation(work_item_id: uuid.UUID) -> tuple[uuid.UUID, str]:
    (row,) = _rows(
        "SELECT agent_id, conversation_id FROM curie.work_items WHERE id = :id",
        {"id": work_item_id},
    )
    return row["agent_id"], row["conversation_id"]


def _seed_history(work_item_id: uuid.UUID) -> None:
    agent_id, conversation = _conversation(work_item_id)

    async def body(session: AsyncSession) -> None:
        await _seed_transcript(session, agent_id, conversation)

    _db(body)


def _history(work_item_id: uuid.UUID) -> bool:
    agent_id, conversation = _conversation(work_item_id)

    async def body(session: AsyncSession) -> set[str]:
        return await _transcript_threads(session, agent_id)

    return conversation in _db(body)


def _carrier(case: dict[str, Any], base: datetime) -> tuple[dict[str, Any] | None, Any]:
    """The case's carrier with a placeholder time replaced, and the time it names."""

    condition = case["finish"]["retry_condition"]
    if condition is None:
        return None, None
    wire = dict(case.get("finish_wire_retry_condition") or condition)
    offset = case.get("retry_at_from_now_seconds")
    at = None
    if offset is not None:
        at = (base + timedelta(seconds=offset)).astimezone(UTC).replace(microsecond=0)
        wire["retry_at"] = at.strftime("%Y-%m-%dT%H:%M:%SZ")
    elif wire.get("retry_at") is not None:
        at = datetime.fromisoformat(str(wire["retry_at"]).replace("Z", "+00:00"))
    return {key: value for key, value in wire.items() if value is not None}, at


def _assert_timing(
    case_timing: str | None,
    predecessor: dict[str, Any],
    successor: dict[str, Any],
    *,
    provider_due: datetime | None,
    blocked_on: str | None,
) -> None:
    budget = timedelta(seconds=get_settings().work_item_wait_budget_seconds)
    assert successor["blocked_on"] == blocked_on, successor
    wait = successor["wait_deadline"]
    terminal_at = predecessor["terminal_at"]
    assert terminal_at <= wait - budget <= terminal_at + timedelta(seconds=5)
    due = successor["dispatch_not_before"]
    if case_timing == "blocked":
        assert due == wait
    elif case_timing == "backoff":
        low, high = {1: (24, 36), 2: (96, 144)}[_slot_of(successor)]
        assert timedelta(seconds=low) <= due - terminal_at <= timedelta(seconds=high + 1)
    elif case_timing == "provider_time":
        assert provider_due is not None
        assert due == provider_due
    elif case_timing == "immediate":
        assert due <= terminal_at + timedelta(seconds=5)
    else:
        raise AssertionError(f"unexpected timing {case_timing}")


def _slot_of(successor: dict[str, Any]) -> int:
    found = _rows(
        "SELECT count(*) AS n FROM curie.execution_requests "
        "WHERE work_item_id = :w AND retry_of_request_id IS NOT NULL AND sequence <= :s",
        {"w": successor["work_item_id"], "s": successor["sequence"]},
    )
    value = found[0]["n"]
    assert isinstance(value, int)
    return value


# -- AC1: the allowance --------------------------------------------------------


def test_flag_clean_model_unreachable_admits_attempts_two_and_three(
    api: TestClient, auth_headers: dict[str, str]
) -> None:
    first = _admit(api, _agent(api, auth_headers), 54240)
    _fail(api, first.request_id)
    second = _successor(first.request_id)
    assert second is not None
    assert (second["status"], second["retry_safety"], second["owner_lost_retry"]) == (
        "waiting",
        None,
        False,
    )
    assert (second["retry_of_request_id"], second["retry_origin_request_id"]) == (
        first.request_id,
        first.request_id,
    )
    assert _request(first.request_id)["retry_safety"] == "no_effects"
    assert _used(first.work_item_id) == 1

    _fail(api, second["id"])
    third = _successor(second["id"])
    assert third is not None and third["status"] == "waiting"
    # Every successor names the original authorized request.
    assert (third["retry_of_request_id"], third["retry_origin_request_id"]) == (
        second["id"],
        first.request_id,
    )
    assert _used(first.work_item_id) == 2
    assert [r["sequence"] for r in _requests(first.work_item_id)] == [1, 2, 3]


def test_third_failure_admits_no_fourth_attempt(
    api: TestClient, auth_headers: dict[str, str]
) -> None:
    item = _admit(api, _agent(api, auth_headers), 54241)
    current = item.request_id
    for _ in range(LIMIT):
        _fail(api, current)
        successor = _successor(current)
        assert successor is not None
        current = successor["id"]
    _fail(api, current)
    assert _successor(current) is None
    rows = _requests(item.work_item_id)
    assert [(r["status"], r["terminal_cause"]) for r in rows] == [
        ("failed", "model_unreachable")
    ] * 3
    assert _used(item.work_item_id) == LIMIT


@pytest.mark.parametrize("third", ["loss", "fail"])
@pytest.mark.parametrize(
    "order", [("loss", "loss"), ("loss", "fail"), ("fail", "loss"), ("fail", "fail")]
)
def test_mixed_owner_lost_and_transient_sequence_never_admits_a_fourth(
    api: TestClient, auth_headers: dict[str, str], order: tuple[str, str], third: str
) -> None:
    item = _admit(api, _agent(api, auth_headers), 54242)
    current = item.request_id
    for slot, kind in enumerate(order, start=1):
        if kind == "loss":
            _lose_owner(api, current)
        else:
            _fail(api, current)
        successor = _successor(current)
        assert successor is not None, (slot, kind)
        assert successor["owner_lost_retry"] is (kind == "loss")
        assert _used(item.work_item_id) == slot
        current = successor["id"]
    if third == "loss":
        _lose_owner(api, current)
    else:
        _fail(api, current)
    assert _successor(current) is None
    assert len(_requests(item.work_item_id)) == 3
    assert _used(item.work_item_id) == LIMIT


async def _race_finish(app: Any, request_id: uuid.UUID, body: dict[str, Any]) -> list[int]:
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://api"
    ) as http:
        url = f"/v1/internal/work-items/requests/{request_id}/finish"
        responses = await asyncio.gather(
            http.post(url, json=body, headers=WORKER), http.post(url, json=body, headers=WORKER)
        )
    return sorted(response.status_code for response in responses)


def test_two_concurrent_finish_settlements_admit_one_successor(
    api: TestClient, auth_headers: dict[str, str]
) -> None:
    item = _admit(api, _agent(api, auth_headers), 54243)
    epoch = _dispatch(api, item.request_id)
    body = {
        "runtime_epoch": epoch,
        "outcome": "failed",
        "cause": "model_unreachable",
        "retry_safety": "no_effects",
    }
    assert _call(api, _race_finish, api.app, item.request_id, body) == [200, 409]
    assert len(_requests(item.work_item_id)) == 2
    assert _successor(item.request_id) is not None
    assert _used(item.work_item_id) == 1


def test_duplicate_finish_delivery_is_not_running_and_admits_nothing(
    api: TestClient, auth_headers: dict[str, str]
) -> None:
    item = _admit(api, _agent(api, auth_headers), 54244)
    epoch = _dispatch(api, item.request_id)
    first = _finish(api, item.request_id, epoch, "model_unreachable")
    assert first.status_code == 200, first.text
    settled = _request(item.request_id)
    again = _finish(api, item.request_id, epoch, "model_unreachable")
    assert again.status_code == 409, again.text
    assert again.json()["detail"]["code"] == "not_running"
    assert _request(item.request_id) == settled
    assert len(_requests(item.work_item_id)) == 2
    assert _used(item.work_item_id) == 1


def test_reconstructed_app_and_session_do_not_replenish_the_allowance(
    retry_env: None, auth_headers: dict[str, str]
) -> None:
    with TestClient(create_app()) as api:
        agent_id = _agent(api, auth_headers)
        item = _admit(api, agent_id, 54245)
    current = item.request_id
    for attempt in range(1, LIMIT + 2):
        # A fresh app, engine and worker client for every attempt.
        with TestClient(create_app()) as api:
            epoch = _dispatch(api, current)
            finished = _finish(api, current, epoch, "model_unreachable")
            assert finished.status_code == 200, finished.text
            replay = _finish(api, current, epoch, "model_unreachable")
            assert replay.status_code == 409, replay.text
        assert _used(item.work_item_id) == min(attempt, LIMIT)
        successor = _successor(current)
        if attempt <= LIMIT:
            assert successor is not None
            current = successor["id"]
        else:
            assert successor is None
    assert len(_requests(item.work_item_id)) == LIMIT + 1


def _versions(request_id: uuid.UUID) -> tuple[int, int]:
    (row,) = _rows(
        "SELECT w.version AS w, r.version AS r FROM curie.execution_requests r "
        "JOIN curie.work_items w ON w.id = r.work_item_id WHERE r.id = :id",
        {"id": request_id},
    )
    return int(row["w"]), int(row["r"])


def _settle(ref: Ref, *, work_version: int, request_version: int) -> lifecycle.WorkItemResult:
    async def body(session: AsyncSession) -> lifecycle.WorkItemResult:
        return await lifecycle.fail_execution(
            session,
            work_item_id=ref.work_item_id,
            request_id=ref.request_id,
            cause="model_unreachable",
            expected_work_item_version=work_version,
            expected_request_version=request_version,
            retry_safety="no_effects",
            retry_condition=None,
        )

    return _db(body)


@pytest.mark.parametrize("stale", ["work_item", "request"])
def test_stale_versions_conflict_and_valid_neighbor_settles(
    api: TestClient, auth_headers: dict[str, str], stale: str
) -> None:
    """Tests 7 and 8: either stale version writes nothing; the current one admits."""

    item = _admit(api, _agent(api, auth_headers), 54246)
    _dispatch(api, item.request_id)
    work_version, request_version = _versions(item.request_id)
    before = _request(item.request_id)
    refused = _settle(
        item,
        work_version=work_version - (stale == "work_item"),
        request_version=request_version - (stale == "request"),
    )
    assert isinstance(refused, lifecycle.WorkItemConflict), refused
    assert refused.code == "stale_version"
    assert _request(item.request_id) == before
    assert _used(item.work_item_id) == 0
    assert len(_requests(item.work_item_id)) == 1

    settled = _settle(item, work_version=work_version, request_version=request_version)
    assert isinstance(settled, lifecycle.WorkItemOutcome), settled
    assert _successor(item.request_id) is not None
    assert _used(item.work_item_id) == 1


def test_stale_runtime_epoch_is_stale_owner_and_valid_neighbor_settles(
    api: TestClient, auth_headers: dict[str, str]
) -> None:
    item = _admit(api, _agent(api, auth_headers), 54247)
    epoch = _dispatch(api, item.request_id)
    before = _request(item.request_id)
    stale = _finish(api, item.request_id, epoch + 1, "model_unreachable")
    assert stale.status_code == 409, stale.text
    assert stale.json()["detail"]["code"] == "stale_owner"
    assert _request(item.request_id) == before
    assert len(_requests(item.work_item_id)) == 1

    valid = _finish(api, item.request_id, epoch, "model_unreachable")
    assert valid.status_code == 200, valid.text
    assert _successor(item.request_id) is not None


@pytest.mark.parametrize("used", [2, 3])
def test_work_item_already_at_or_above_two_admits_nothing(
    api: TestClient, auth_headers: dict[str, str], used: int
) -> None:
    agent_id = _agent(api, auth_headers)
    spent = _admit(api, agent_id, 54248)
    neighbor = _admit(api, agent_id, 54249)
    _execute(
        "UPDATE curie.work_items SET automatic_retries_used = :used WHERE id = :id",
        {"used": used, "id": spent.work_item_id},
    )
    _execute(
        "UPDATE curie.work_items SET automatic_retries_used = 1 WHERE id = :id",
        {"id": neighbor.work_item_id},
    )
    _fail(api, spent.request_id)
    assert _successor(spent.request_id) is None
    assert _used(spent.work_item_id) == used
    _fail(api, neighbor.request_id)
    assert _successor(neighbor.request_id) is not None
    assert _used(neighbor.work_item_id) == 2


# -- AC2: identity, deadlines, history and fences -------------------------------


def test_successor_has_new_identity_sequence_and_fresh_wait_deadline(
    api: TestClient, auth_headers: dict[str, str]
) -> None:
    item = _admit(api, _agent(api, auth_headers), 54250)
    original = _request(item.request_id)
    epoch = _dispatch(api, item.request_id)
    before = _now()
    assert _finish(api, item.request_id, epoch, "model_unreachable").status_code == 200
    after = _now()
    predecessor = _request(item.request_id)
    successor = _successor(item.request_id)
    assert successor is not None
    budget = timedelta(seconds=get_settings().work_item_wait_budget_seconds)
    assert successor["id"] != item.request_id
    assert successor["sequence"] == predecessor["sequence"] + 1
    assert before + budget <= successor["wait_deadline"] <= after + budget
    assert (successor["started_at"], successor["execution_deadline"]) == (None, None)
    assert (successor["objective"], successor["requester"]) == (OBJECTIVE, REQUESTER)
    assert predecessor["wait_deadline"] == original["wait_deadline"]


def test_predecessor_row_is_byte_unchanged_after_successor_runs(
    api: TestClient, auth_headers: dict[str, str]
) -> None:
    item = _admit(api, _agent(api, auth_headers), 54251)
    _fail(api, item.request_id)
    snapshot = "SELECT to_jsonb(r)::text AS row FROM curie.execution_requests r WHERE id = :id"
    (before,) = _rows(snapshot, {"id": item.request_id})
    successor = _successor(item.request_id)
    assert successor is not None
    epoch = _dispatch(api, successor["id"])
    assert _finish(api, successor["id"], epoch, "runner_escalated").status_code == 200
    (after,) = _rows(snapshot, {"id": item.request_id})
    assert after == before


def test_successor_execution_deadline_is_set_once_at_its_own_start(
    api: TestClient, auth_headers: dict[str, str]
) -> None:
    item = _admit(api, _agent(api, auth_headers), 54252)
    _fail(api, item.request_id)
    predecessor = _request(item.request_id)
    successor = _successor(item.request_id)
    assert successor is not None and successor["execution_deadline"] is None
    epoch = _dispatch(api, successor["id"])
    started = _request(successor["id"])
    assert started["execution_deadline"] > started["started_at"]
    assert started["execution_deadline"] != predecessor["execution_deadline"]

    async def beat(app: Any) -> None:
        async with _worker(app) as client:
            await client.heartbeat(successor["id"], runtime_epoch=epoch)

    _call(api, beat, api.app)
    assert _request(successor["id"])["execution_deadline"] == started["execution_deadline"]
    assert _request(item.request_id)["execution_deadline"] == predecessor["execution_deadline"]


def test_backoff_due_time_is_deterministic_and_inside_the_jitter_window(
    api: TestClient, auth_headers: dict[str, str]
) -> None:
    agent_id = _agent(api, auth_headers)
    item = _admit(api, agent_id, 54253)
    current = item.request_id
    for slot, (low, high) in enumerate(((24, 36), (96, 144)), start=1):
        _fail(api, current)
        predecessor = _request(current)
        successor = _successor(current)
        assert successor is not None
        delay = successor["dispatch_not_before"] - predecessor["terminal_at"]
        assert timedelta(seconds=low) <= delay <= timedelta(seconds=high + 1)
        expected = lifecycle.automatic_retry_delay_seconds(item.work_item_id, slot)
        assert expected == lifecycle.automatic_retry_delay_seconds(item.work_item_id, slot)
        assert abs(delay.total_seconds() - expected) < 1.0
        if slot == 1:
            # A pass before the due time wakes a due neighbor, not the successor.
            neighbor = _admit(api, agent_id, 54254)
            _reconcile()
            ids = _wake_ids()
            assert any(i.startswith(f"work-item-{neighbor.request_id}") for i in ids), ids
            assert not any(i.startswith(f"work-item-{successor['id']}") for i in ids), ids
            assert _request(successor["id"])["published_generation"] is None
        current = successor["id"]


def test_history_survives_while_a_successor_exists_and_expires_at_the_final_bound(
    api: TestClient, auth_headers: dict[str, str]
) -> None:
    item = _admit(api, _agent(api, auth_headers), 54255)
    _seed_history(item.work_item_id)
    current = item.request_id
    for _ in range(LIMIT):
        _fail(api, current)
        assert _history(item.work_item_id)
        successor = _successor(current)
        assert successor is not None
        current = successor["id"]
    _fail(api, current)
    assert _successor(current) is None
    assert not _history(item.work_item_id)


def test_no_recovery_checkpoint_is_read_for_a_successor(
    api: TestClient, auth_headers: dict[str, str]
) -> None:
    item = _admit(api, _agent(api, auth_headers), 54256)
    _fail(api, item.request_id)
    successor = _successor(item.request_id)
    assert successor is not None
    epoch = _dispatch(api, successor["id"])
    assert _finish(api, successor["id"], epoch, "runner_escalated").status_code == 200
    assert _rows("SELECT count(*) AS n FROM curie.workspace_recovery_readers") == [{"n": 0}]


def _suppress(api: TestClient, ref: Ref, epoch: int, fence: str) -> None:
    """Apply one fence to a running request and settle it; it must admit nothing."""

    if fence in ("cancellation", "issue_deletion"):
        work_version, _ = _versions(ref.request_id)

        async def cancel(session: AsyncSession) -> lifecycle.WorkItemResult:
            return await lifecycle.request_cancellation(
                session,
                work_item_id=ref.work_item_id,
                expected_work_item_version=work_version,
                cause="issue_cancelled" if fence == "cancellation" else "issue_deleted",
            )

        assert isinstance(_db(cancel), lifecycle.WorkItemOutcome)
        refused = _finish(api, ref.request_id, epoch, "model_unreachable")
        assert refused.status_code == 409, refused.text
        assert refused.json()["detail"]["code"] == "work_item_cancelled"
        return
    if fence == "queued_revision":
        work_version, _ = _versions(ref.request_id)
        conversation = _request(ref.request_id)["reply_conversation_id"]

        async def revise(session: AsyncSession) -> lifecycle.WorkItemResult:
            return await lifecycle.create_revision_request(
                session,
                work_item_id=ref.work_item_id,
                request_id=uuid.uuid4(),
                wait_deadline=datetime.now(UTC) + timedelta(hours=1),
                expected_work_item_version=work_version,
                snapshot={
                    "objective": "Address the follow up comment",
                    "requester": REQUESTER,
                    "reply_kind": "slack",
                    "reply_address": ADDRESS,
                    "reply_conversation_id": conversation,
                },
            )

        revised = _db(revise)
        assert isinstance(revised, lifecycle.WorkItemOutcome), revised
        assert revised.request is not None and revised.request.status == "queued"
    elif fence == "closed_lineage":
        _execute(
            "UPDATE curie.execution_requests SET reply_target_kind = 'review_thread', "
            "reply_target_pr_number = '123', reply_target_comment_id = '88201', "
            "reply_target_url = 'https://github.com/acme-corp/acme-bot/pull/123' "
            "WHERE id = :id",
            {"id": ref.request_id},
        )
    elif fence == "publication":

        async def publish(session: AsyncSession) -> None:
            await _attach_own_publication(
                session, ref.work_item_id, ref.request_id, status="failed"
            )

        _db(publish)
    if fence == "absent_safety":
        work_version, request_version = _versions(ref.request_id)

        async def settle(session: AsyncSession) -> lifecycle.WorkItemResult:
            return await lifecycle.fail_execution(
                session,
                work_item_id=ref.work_item_id,
                request_id=ref.request_id,
                cause="model_unreachable",
                expected_work_item_version=work_version,
                expected_request_version=request_version,
                retry_safety=None,
                retry_condition=None,
            )

        assert isinstance(_db(settle), lifecycle.WorkItemOutcome)
        return
    cause = "budget_exceeded" if fence == "budget_exceeded" else "model_unreachable"
    safety = fence if fence in ("flagged", "uncertain") else "no_effects"
    finished = _finish(api, ref.request_id, epoch, cause, safety)
    assert finished.status_code == 200, finished.text


@pytest.mark.parametrize(
    "fence",
    [
        "cancellation",
        "issue_deletion",
        "queued_revision",
        "closed_lineage",
        "publication",
        "budget_exceeded",
        "flagged",
        "uncertain",
        "absent_safety",
    ],
)
def test_ordinary_retry_is_suppressed(
    api: TestClient, auth_headers: dict[str, str], fence: str
) -> None:
    agent_id = _agent(api, auth_headers)
    fenced = _admit(api, agent_id, 54257)
    control = _admit(api, agent_id, 54258)
    epoch = _dispatch(api, fenced.request_id)
    _suppress(api, fenced, epoch, fence)
    assert _successor(fenced.request_id) is None
    assert _used(fenced.work_item_id) == 0
    assert not any(r["retry_of_request_id"] for r in _requests(fenced.work_item_id))
    # The valid neighbor in the same state still admits.
    _fail(api, control.request_id)
    assert _successor(control.request_id) is not None
    assert _used(control.work_item_id) == 1


@pytest.mark.parametrize("safety", ["flagged", "uncertain"])
def test_model_unreachable_after_flagged_or_uncertain_effect_admits_nothing(
    api: TestClient, auth_headers: dict[str, str], safety: str
) -> None:
    agent_id = _agent(api, auth_headers)
    effected = _admit(api, agent_id, 54259)
    clean = _admit(api, agent_id, 54260)
    carrier = {"reason": "transient_service"}
    epoch = _dispatch(api, effected.request_id)
    finished = _finish(
        api, effected.request_id, epoch, "model_unreachable", safety, condition=carrier
    )
    assert finished.status_code == 200, finished.text
    assert _request(effected.request_id)["retry_safety"] == safety
    assert _successor(effected.request_id) is None
    assert _used(effected.work_item_id) == 0
    epoch = _dispatch(api, clean.request_id)
    finished = _finish(api, clean.request_id, epoch, "model_unreachable", condition=carrier)
    assert finished.status_code == 200, finished.text
    assert _successor(clean.request_id) is not None


def test_missing_or_malformed_finish_facts_are_422_and_request_stays_running(
    api: TestClient, auth_headers: dict[str, str]
) -> None:
    item = _admit(api, _agent(api, auth_headers), 54261)
    epoch = _dispatch(api, item.request_id)
    before = _request(item.request_id)
    for refused in VECTORS["refused_finish_bodies"]:
        body = {"runtime_epoch": epoch, **refused["body"]}
        response = api.post(
            f"/v1/internal/work-items/requests/{item.request_id}/finish", json=body, headers=WORKER
        )
        assert response.status_code == 422, (refused["id"], response.text)
        assert _request(item.request_id) == before, refused["id"]
    assert len(_requests(item.work_item_id)) == 1
    valid = _finish(api, item.request_id, epoch, "model_unreachable")
    assert valid.status_code == 200, valid.text
    assert _successor(item.request_id) is not None


def test_owner_loss_admits_without_a_safety_fact_inside_the_shared_count(
    api: TestClient, auth_headers: dict[str, str]
) -> None:
    """Storage half of test 20: owner loss is marked distinct and spends a slot."""

    agent_id = _agent(api, auth_headers)
    lost = _admit(api, agent_id, 54262)
    failed = _admit(api, agent_id, 54263)
    _lose_owner(api, lost.request_id)
    predecessor = _request(lost.request_id)
    assert predecessor["retry_safety"] is None
    successor = _successor(lost.request_id)
    assert successor is not None
    assert successor["owner_lost_retry"] is True
    assert (successor["retry_of_request_id"], successor["retry_origin_request_id"]) == (
        lost.request_id,
        lost.request_id,
    )
    assert successor["blocked_on"] is None
    assert successor["dispatch_not_before"] <= predecessor["terminal_at"] + timedelta(seconds=5)
    assert _used(lost.work_item_id) == 1

    _fail(api, failed.request_id)
    ordinary = _successor(failed.request_id)
    assert ordinary is not None
    assert ordinary["owner_lost_retry"] is False
    assert ordinary["retry_of_request_id"] == failed.request_id
    assert _used(failed.work_item_id) == 1


def _blocked_successor(api: TestClient, ref: Ref) -> dict[str, Any]:
    epoch = _dispatch(api, ref.request_id)
    assert _finish(api, ref.request_id, epoch, "model_usage_limited").status_code == 200
    successor = _successor(ref.request_id)
    assert successor is not None
    assert successor["blocked_on"] == "provider_usage_reset"
    assert successor["dispatch_not_before"] == successor["wait_deadline"]
    return successor


def test_blocked_successor_expires_as_retry_condition_expired_naming_the_condition(
    api: TestClient, auth_headers: dict[str, str]
) -> None:
    item = _admit(api, _agent(api, auth_headers), 54264)
    _seed_history(item.work_item_id)
    successor = _blocked_successor(api, item)
    assert _history(item.work_item_id)
    _expire_wait(successor["id"])
    _reconcile()
    expired = _request(successor["id"])
    assert (expired["status"], expired["terminal_cause"], expired["blocked_on"]) == (
        "expired",
        "retry_condition_expired",
        "provider_usage_reset",
    )
    assert _successor(successor["id"]) is None
    assert _used(item.work_item_id) == 1
    assert not _history(item.work_item_id)


def test_unblocked_wait_expiry_stays_capacity_wait_expired(
    api: TestClient, auth_headers: dict[str, str]
) -> None:
    agent_id = _agent(api, auth_headers)
    fresh = _admit(api, agent_id, 54265)
    retried = _admit(api, agent_id, 54266)
    _fail(api, retried.request_id)
    scheduled = _successor(retried.request_id)
    assert scheduled is not None and scheduled["blocked_on"] is None
    _expire_wait(fresh.request_id)
    _expire_wait(scheduled["id"])
    _reconcile()
    for request_id in (fresh.request_id, scheduled["id"]):
        row = _request(request_id)
        assert (row["status"], row["terminal_cause"], row["blocked_on"]) == (
            "expired",
            "capacity_wait_expired",
            None,
        )


@pytest.mark.parametrize(("offset", "blocked"), [(600, False), (86370, True)])
def test_time_condition_inside_the_deadline_is_scheduled_and_past_it_is_blocked(
    api: TestClient, auth_headers: dict[str, str], offset: int, blocked: bool
) -> None:
    item = _admit(api, _agent(api, auth_headers), 54267)
    epoch = _dispatch(api, item.request_id)
    reset = (_now() + timedelta(seconds=offset)).astimezone(UTC).replace(microsecond=0)
    carrier = {"reason": "usage_reset", "retry_at": reset.strftime("%Y-%m-%dT%H:%M:%SZ")}
    finished = _finish(api, item.request_id, epoch, "model_usage_limited", condition=carrier)
    assert finished.status_code == 200, finished.text
    predecessor = _request(item.request_id)
    assert (predecessor["retry_condition_reason"], predecessor["retry_condition_retry_at"]) == (
        "usage_reset",
        reset,
    )
    successor = _successor(item.request_id)
    assert successor is not None
    if blocked:
        assert successor["blocked_on"] == "provider_usage_reset"
        assert successor["dispatch_not_before"] == successor["wait_deadline"]
    else:
        assert successor["blocked_on"] is None
        assert successor["dispatch_not_before"] == reset + timedelta(seconds=60)


def test_blocked_row_is_never_claimed_and_a_due_neighbor_is(
    api: TestClient, auth_headers: dict[str, str]
) -> None:
    agent_id = _agent(api, auth_headers)
    blocked = _blocked_successor(api, _admit(api, agent_id, 54268))
    retried = _admit(api, agent_id, 54269)
    _fail(api, retried.request_id)
    scheduled = _successor(retried.request_id)
    assert scheduled is not None
    # Both are past their due time; only the typed block keeps one asleep.
    _make_due(blocked["id"])
    _make_due(scheduled["id"])
    _reconcile()
    ids = _wake_ids()
    assert any(i.startswith(f"work-item-{scheduled['id']}") for i in ids), ids
    assert not any(i.startswith(f"work-item-{blocked['id']}") for i in ids), ids
    assert _request(blocked["id"])["published_generation"] is None
    assert _request(blocked["id"])["status"] == "waiting"


@pytest.mark.parametrize("kind", ["blocked", "scheduled"])
def test_relabel_supersedes_a_blocked_or_scheduled_successor(
    api: TestClient, auth_headers: dict[str, str], kind: str
) -> None:
    item = _admit(api, _agent(api, auth_headers), 54270)
    if kind == "blocked":
        successor = _blocked_successor(api, item)
    else:
        _fail(api, item.request_id)
        found = _successor(item.request_id)
        assert found is not None
        successor = found
    relabel_id = uuid.uuid4()

    async def relabel(session: AsyncSession) -> lifecycle.WorkItemResult:
        return await lifecycle.readmit(
            session,
            work_item_id=item.work_item_id,
            request_id=relabel_id,
            wait_deadline=datetime.now(UTC) + timedelta(hours=1),
            objective=OBJECTIVE,
            requester=REQUESTER,
            trigger_label=None,
        )

    assert isinstance(_db(relabel), lifecycle.WorkItemOutcome)
    superseded = _request(successor["id"])
    assert (superseded["status"], superseded["terminal_cause"]) == ("cancelled", "issue_cancelled")
    fresh = _request(relabel_id)
    assert (fresh["status"], fresh["blocked_on"], fresh["retry_of_request_id"]) == (
        "waiting",
        None,
        None,
    )
    # A separately authorized readmission neither spends nor refunds a slot.
    assert _used(item.work_item_id) == 1


@pytest.mark.parametrize("cause", ["issue_cancelled", "issue_deleted"])
def test_cancellation_or_deletion_cancels_a_waiting_successor_and_expires_history(
    api: TestClient, auth_headers: dict[str, str], cause: str
) -> None:
    item = _admit(api, _agent(api, auth_headers), 54271)
    _seed_history(item.work_item_id)
    _fail(api, item.request_id)
    successor = _successor(item.request_id)
    assert successor is not None
    assert _history(item.work_item_id)
    work_version, _ = _versions(successor["id"])

    async def cancel(session: AsyncSession) -> lifecycle.WorkItemResult:
        return await lifecycle.request_cancellation(
            session,
            work_item_id=item.work_item_id,
            expected_work_item_version=work_version,
            cause=cause,  # type: ignore[arg-type]
        )

    assert isinstance(_db(cancel), lifecycle.WorkItemOutcome)
    row = _request(successor["id"])
    assert (row["status"], row["terminal_cause"]) == ("cancelled", cause)
    assert not _history(item.work_item_id)
    assert _used(item.work_item_id) == 1


@pytest.fixture
def world(clean_db: None) -> Iterator[support.World]:
    support.cleanup_owned()
    yield support.seed_world()
    assert support.cleanup_owned() == 0, "an owned tenant, principal or enrollment survived"


@pytest.fixture
def personal_api(
    world: support.World, runs_stream: str, valkey: redis.Redis, monkeypatch: pytest.MonkeyPatch
) -> Iterator[TestClient]:
    support.configure_api(monkeypatch, worker_token=WORKER_TOKEN)
    with TestClient(create_app()) as client:
        yield client
    get_settings.cache_clear()


def test_personal_mode_successor_snapshots_the_current_owner_tuple(
    personal_api: TestClient, world: support.World
) -> None:
    agent_id = support.insert_agent(world.tenant_id)
    first = support.admit_personal(personal_api, WORKER, world, agent_id, issue_number=54272)
    original = world.personal()
    epoch = support.start_request(first.request_id)
    finished = _finish(personal_api, first.request_id, epoch, "model_unreachable")
    assert finished.status_code == 200, finished.text
    successor = _successor(first.request_id)
    assert successor is not None
    support.assert_owner_tuples(
        first.work_item_id,
        work_item=original,
        requests={first.request_id: original, successor["id"]: original},
    )


# -- AC3: waits, counted starts and CI rounds ----------------------------------


async def _acquire(app: Any, request_id: uuid.UUID, generation: int) -> None:
    wake = await _wake(request_id)
    async with _worker(app) as client:
        await client.acquire(
            request_id, owner=OWNER, generation=generation, wake=wake, context=None
        )


async def _defer(
    app: Any, request_id: uuid.UUID, generation: int, reason: str, capacity: bool
) -> str | None:
    async with _worker(app) as client:
        return await client.defer(
            request_id,
            owner=OWNER,
            generation=generation,
            reason=reason,
            capacity=capacity,
            start_failure_detail=None,
        )


def _acquire_and_defer(
    api: TestClient, request_id: uuid.UUID, reason: str, *, capacity: bool
) -> str | None:
    _make_due(request_id)
    _reconcile()
    generation = _published_wake(request_id)
    _call(api, _acquire, api.app, request_id, generation)
    return _call(api, _defer, api.app, request_id, generation, reason, capacity)


def test_capacity_and_workspace_timeout_defers_keep_request_deadline_and_count(
    api: TestClient, auth_headers: dict[str, str]
) -> None:
    item = _admit(api, _agent(api, auth_headers), 54273)
    _fail(api, item.request_id)
    successor = _successor(item.request_id)
    assert successor is not None
    for reason, capacity in (("capacity", True), ("workspace_timeout", False)):
        assert _acquire_and_defer(api, successor["id"], reason, capacity=capacity) is None
        row = _request(successor["id"])
        assert (row["status"], row["wait_deadline"], row["version"], row["start_deferrals"]) == (
            "waiting",
            successor["wait_deadline"],
            successor["version"],
            successor["start_deferrals"],
        )
    assert len(_requests(item.work_item_id)) == 2
    assert _used(item.work_item_id) == 1


def test_five_counted_starts_span_the_automatic_chain(
    api: TestClient, auth_headers: dict[str, str]
) -> None:
    item = _admit(api, _agent(api, auth_headers), 54274)
    for n in range(1, 4):
        reason = f"not_started:classified_failure-{n}"
        assert _acquire_and_defer(api, item.request_id, reason, capacity=False) is None
    assert _request(item.request_id)["start_deferrals"] == 3
    _fail(api, item.request_id)
    successor = _successor(item.request_id)
    assert successor is not None and successor["start_deferrals"] == 3
    reason = "not_started:classified_failure-4"
    assert _acquire_and_defer(api, successor["id"], reason, capacity=False) is None
    reason = "not_started:classified_failure-5"
    assert _acquire_and_defer(api, successor["id"], reason, capacity=False) == "start_failed"
    row = _request(successor["id"])
    assert (row["status"], row["terminal_cause"], row["start_deferrals"]) == (
        "failed",
        "start_failed",
        5,
    )
    assert _successor(successor["id"]) is None
    assert _used(item.work_item_id) == 1


def test_ci_round_is_derived_from_publications_and_no_successor_resets_it(
    api: TestClient, auth_headers: dict[str, str]
) -> None:
    agent_id = _agent(api, auth_headers)
    published = _admit(api, agent_id, 54275)
    control = _admit(api, agent_id, 54276)
    epoch = _dispatch(api, published.request_id)

    async def publish(session: AsyncSession) -> None:
        await _attach_own_publication(
            session, published.work_item_id, published.request_id, status="succeeded"
        )

    _db(publish)
    rounds = (
        "SELECT count(*) AS n FROM curie.publications "
        "WHERE execution_request_id = :id AND status = 'succeeded'"
    )
    assert _rows(rounds, {"id": published.request_id}) == [{"n": 1}]
    finished = _finish(api, published.request_id, epoch, "model_unreachable")
    assert finished.status_code == 200, finished.text
    assert _successor(published.request_id) is None
    assert _used(published.work_item_id) == 0
    assert _rows(rounds, {"id": published.request_id}) == [{"n": 1}]
    _fail(api, control.request_id)
    assert _successor(control.request_id) is not None


# -- AC4: the shared vectors ---------------------------------------------------


def _case_ids(cases: list[dict[str, Any]]) -> list[str]:
    return [case["id"] for case in cases]


def _parse_at(value: str | None) -> datetime | None:
    return None if value is None else datetime.fromisoformat(value.replace("Z", "+00:00"))


@pytest.mark.parametrize("case", CASES, ids=_case_ids(CASES))
def test_policy_matches_every_vector(case: dict[str, Any]) -> None:
    facts = case["finish"]
    condition = facts["retry_condition"]
    disposition = decide(
        RetryFacts(
            cause=facts["cause"],
            safety=facts["retry_safety"],
            condition_reason=None if condition is None else condition["reason"],
            condition_retry_at=None if condition is None else _parse_at(condition["retry_at"]),
        )
    )
    expected = case["expected"]
    assert (
        disposition.kind.value,
        disposition.condition,
        disposition.not_before,
        disposition.successor,
    ) == (
        expected["disposition"],
        expected["condition"],
        _parse_at(expected["not_before"]),
        expected["policy_successor"],
    )


def _expected_provider_due(case: dict[str, Any], at: datetime | None) -> datetime | None:
    expected = _parse_at(case["expected"]["not_before"])
    if expected is None or at is None:
        return None
    placeholder = _parse_at(PLACEHOLDER_AT)
    assert placeholder is not None
    return at + (expected - placeholder)


def _assert_boundary(case: dict[str, Any], predecessor: dict[str, Any], used_before: int) -> None:
    expected = case["expected"]
    successor = _successor(predecessor["id"])
    if not expected["successor"]:
        assert successor is None, (case["id"], successor)
        assert _used(predecessor["work_item_id"]) == used_before
        return
    assert successor is not None, case["id"]
    assert _used(predecessor["work_item_id"]) == used_before + 1
    at = predecessor["retry_condition_retry_at"]
    _assert_timing(
        expected["timing"],
        predecessor,
        successor,
        provider_due=_expected_provider_due(case, at),
        blocked_on=expected["blocked_on"],
    )


FINISH_CASES = [case for case in CASES if case["boundary"] == "finish"]


@pytest.mark.parametrize("case", FINISH_CASES, ids=_case_ids(FINISH_CASES))
def test_finish_boundary_matches_every_vector(
    api: TestClient, auth_headers: dict[str, str], case: dict[str, Any]
) -> None:
    item = _admit(api, _agent(api, auth_headers), 54300)
    epoch = _dispatch(api, item.request_id)
    carrier, at = _carrier(case, _now())
    facts = case["finish"]
    finished = _finish(
        api,
        item.request_id,
        epoch,
        facts["cause"],
        facts["retry_safety"],
        condition=_ABSENT if carrier is None else carrier,
    )
    assert finished.status_code == 200, finished.text
    predecessor = _request(item.request_id)
    assert (predecessor["status"], predecessor["terminal_cause"], predecessor["retry_safety"]) == (
        "failed",
        facts["cause"],
        facts["retry_safety"],
    )
    stored = facts["retry_condition"]
    assert predecessor["retry_condition_reason"] == (None if stored is None else stored["reason"])
    assert predecessor["retry_condition_retry_at"] == at
    assert predecessor["retry_condition_scope_key"] == (
        None if stored is None else stored["scope_key"]
    )
    _assert_boundary(case, predecessor, 0)


def _defer_boundary(api: TestClient, item: Ref, case: dict[str, Any]) -> dict[str, Any]:
    setup = case.get("setup", {})
    if case["finish"]["cause"] == "start_failed":
        limit = get_settings().work_item_start_deferral_limit
        for n in range(1, limit + 1):
            result = _acquire_and_defer(
                api, item.request_id, f"not_started:classified_failure-{n}", capacity=False
            )
        assert result == "start_failed"
    else:
        before = _request(item.request_id)
        result = _acquire_and_defer(
            api, item.request_id, setup["defer_reason"], capacity=setup["defer_capacity"]
        )
        assert result is None
        after = _request(item.request_id)
        assert (after["wait_deadline"], after["start_deferrals"]) == (
            before["wait_deadline"],
            before["start_deferrals"],
        )
    return _request(item.request_id)


def _runtime_termination_boundary(api: TestClient, item: Ref, case: dict[str, Any]) -> None:
    used = case.get("setup", {}).get("automatic_retries_used_before")
    if used is not None:
        _execute(
            "UPDATE curie.work_items SET automatic_retries_used = :used WHERE id = :id",
            {"used": used, "id": item.work_item_id},
        )
    if case["finish"]["cause"] == "owner_lost":
        _lose_owner(api, item.request_id)
        return
    _dispatch(api, item.request_id)
    _execute(
        "UPDATE curie.execution_requests SET "
        "started_at = clock_timestamp() - interval '2 hours', "
        "execution_deadline = clock_timestamp() - interval '1 hour' WHERE id = :id",
        {"id": item.request_id},
        guarded=True,
    )
    # The heartbeat stays live, so the deadline is the only reason to stop.
    _reconcile()
    row = _request(item.request_id)
    assert (row["status"], row["terminal_cause"]) == (
        "cancellation_requested",
        "execution_deadline",
    )
    # The live owner stops its own run at the deadline and observes the teardown.
    _call(api, _record_teardown, api.app, item.request_id, OWNER)


def _wait_expiry_boundary(api: TestClient, item: Ref, case: dict[str, Any]) -> uuid.UUID:
    target = item.request_id
    if case["setup"]["blocked_before"]:
        target = _blocked_successor(api, item)["id"]
    _expire_wait(target)
    _reconcile()
    return target


NON_FINISH_CASES = [
    case
    for case in CASES
    if case["boundary"] in ("defer", "runtime_termination", "wait_expiry", "api_settlement")
]


@pytest.mark.parametrize("case", NON_FINISH_CASES, ids=_case_ids(NON_FINISH_CASES))
def test_non_finish_boundary_vectors_match(
    api: TestClient, auth_headers: dict[str, str], case: dict[str, Any]
) -> None:
    item = _admit(api, _agent(api, auth_headers), 54301)
    expected = case["expected"]
    boundary = case["boundary"]
    target = item.request_id
    used_before = case.get("setup", {}).get("automatic_retries_used_before", 0)
    if boundary == "defer":
        row = _defer_boundary(api, item, case)
        assert row["status"] == expected["status"]
        assert len(_requests(item.work_item_id)) == 1
        assert _used(item.work_item_id) == 0
        return
    if boundary == "runtime_termination":
        _runtime_termination_boundary(api, item, case)
    elif boundary == "wait_expiry":
        target = _wait_expiry_boundary(api, item, case)
        used_before = _used(item.work_item_id)
    else:
        _dispatch(api, item.request_id)
        work_version, request_version = _versions(item.request_id)

        async def settle(session: AsyncSession) -> lifecycle.WorkItemResult:
            return await lifecycle.fail_execution(
                session,
                work_item_id=item.work_item_id,
                request_id=item.request_id,
                cause=case["finish"]["cause"],
                expected_work_item_version=work_version,
                expected_request_version=request_version,
                retry_safety=None,
                retry_condition=None,
            )

        assert isinstance(_db(settle), lifecycle.WorkItemOutcome)
    row = _request(target)
    if "status" in expected:
        assert (row["status"], row["terminal_cause"]) == (
            expected["status"],
            expected["terminal_cause"],
        )
    assert row["retry_safety"] is None
    _assert_boundary(case, row, used_before)


def test_unknown_causes_stop_and_valid_transient_advances(
    api: TestClient, auth_headers: dict[str, str]
) -> None:
    agent_id = _agent(api, auth_headers)
    carrier = {"reason": "transient_service"}
    for issue, cause in (
        (54277, "model_unreachable_maybe"),
        (54278, "model-unreachable-maybe"),
        (54279, "Model_Unreachable"),
    ):
        unknown = _admit(api, agent_id, issue)
        epoch = _dispatch(api, unknown.request_id)
        finished = _finish(api, unknown.request_id, epoch, cause, condition=carrier)
        assert finished.status_code == 200, finished.text
        assert _successor(unknown.request_id) is None, cause
        assert _used(unknown.work_item_id) == 0
    valid = _admit(api, agent_id, 54281)
    epoch = _dispatch(api, valid.request_id)
    finished = _finish(api, valid.request_id, epoch, "model_unreachable", condition=carrier)
    assert finished.status_code == 200, finished.text
    assert _successor(valid.request_id) is not None
    assert _used(valid.work_item_id) == 1
    # The hyphenated token is an explicit alias, so it admits exactly as the cause does.
    alias = _admit(api, agent_id, 54280)
    epoch = _dispatch(api, alias.request_id)
    finished = _finish(api, alias.request_id, epoch, "model-unreachable", condition=carrier)
    assert finished.status_code == 200, finished.text
    successor = _successor(alias.request_id)
    assert successor is not None
    assert successor["blocked_on"] is None
    assert successor["owner_lost_retry"] is False
    assert _used(alias.work_item_id) == 1


# -- AC2: what operators see ---------------------------------------------------
#
# The tracker scenarios run the real notice consumer (the reconciler pass and
# its status comment sync) against the threaded GitHub stand-in, which records
# every comment and label write it receives.

# The tracker fixtures pytest resolves by name for the scenarios below.
TRACKER_FIXTURES = (admitted, comments)
TRACKER_WORKER = {"X-Curie-Worker-Token": "factory-terminus-worker"}
NEEDS_HUMAN = "curie-factory:needs-human"
QUEUED_LABEL = "curie-factory:queued"
USED_PHRASE = "automatic retries used"


def _admin() -> dict[str, str]:
    return {"X-API-Key": get_settings().api_key}


def _outcome(client: TestClient, work_item_id: uuid.UUID) -> dict[str, Any]:
    response = client.get(f"/work-items/{work_item_id}", headers=_admin())
    assert response.status_code == 200, response.text
    body = response.json()
    assert isinstance(body, dict)
    return body


def _marked_body(sink: Any, request_id: uuid.UUID) -> str:
    everything = list(sink.comments) + [c for listed in sink.lists.values() for c in listed]
    (comment,) = [c for c in everything if marker_for(request_id) in (c.get("body") or "")]
    body = comment["body"]
    assert isinstance(body, str)
    return body


def _state_labels(sink: Any, number: int) -> set[str]:
    labels: set[str] = sink.issue_labels.get(number, set())
    return {label for label in labels if label.startswith("curie-factory:")}


def _needs_human_applications(sink: Any) -> int:
    return sum(
        1
        for method, path, body in sink.requests
        if method == "POST" and _LABELS.match(path) and body and NEEDS_HUMAN in body
    )


def _tracker_issue(client: TestClient, github: Any, number: int) -> Ref:
    _label_issue(client, github, number)
    row = _issue_request(number)
    _reconcile_and_sync()
    return Ref(row["id"], row["work_item_id"])


def _tracker_fail(client: TestClient, request_id: uuid.UUID, cause: str) -> None:
    """Start the request and finish it flag clean through the real finish route."""

    epoch = _start_running(client, request_id)
    finished = client.post(
        f"/v1/internal/work-items/requests/{request_id}/finish",
        headers=TRACKER_WORKER,
        json={
            "runtime_epoch": epoch,
            "outcome": "failed",
            "cause": cause,
            "retry_safety": "no_effects",
        },
    )
    assert finished.status_code == 200, finished.text
    _reconcile_and_sync()


def test_owner_loss_and_failure_successors_report_their_retry_kind(
    api: TestClient, auth_headers: dict[str, str]
) -> None:
    """Display half of test 20, through the public outcome route."""

    agent_id = _agent(api, auth_headers)
    lost = _admit(api, agent_id, 54290)
    failed = _admit(api, agent_id, 54291)
    _lose_owner(api, lost.request_id)
    _fail(api, failed.request_id)

    for ref, kind, safety in (
        (lost, "owner_loss", None),
        (failed, "failure", "no_effects"),
    ):
        successor = _successor(ref.request_id)
        assert successor is not None
        view = _outcome(api, ref.work_item_id)
        assert (view["automatic_retries_used"], view["automatic_retry_limit"]) == (1, LIMIT)
        assert view["state"] == "waiting"
        first, second = view["requests"]
        assert (first["retry_kind"], first["retry_of_request_id"]) == (None, None)
        assert first["retry_safety"] == safety
        assert second["retry_kind"] == kind
        assert second["retry_of_request_id"] == str(ref.request_id)
        assert second["blocked_on"] is None
        assert second["retry_safety"] is None
        due = datetime.fromisoformat(second["dispatch_not_before"])
        assert due == successor["dispatch_not_before"]
    # The ordinary successor waits out its backoff, so the cause names the due time.
    cause = _outcome(api, failed.work_item_id)["actionable_cause"]
    assert cause.startswith("automatic retry scheduled for "), cause
    assert "capacity" not in cause


def test_scheduled_successor_shows_queued_label_and_due_time_not_needs_human(
    admitted: Any,
) -> None:
    client, github, sink = admitted
    number = 54292
    item = _tracker_issue(client, github, number)
    _tracker_fail(client, item.request_id, "model_unreachable")

    successor = _successor(item.request_id)
    assert successor is not None
    assert (successor["status"], successor["blocked_on"]) == ("waiting", None)
    scheduled = (
        "Scheduled: the automatic retry starts at or after "
        f"{utc_text(successor['dispatch_not_before'])}."
    )
    predecessor = _marked_body(sink, item.request_id)
    assert "Status: RETRYING" in predecessor
    assert (
        "Retrying: this run failed with no side effects, so Curie admitted automatic retry 1 of 2."
        in predecessor
    )
    assert scheduled in predecessor
    assert "Cause: model_unreachable" in predecessor
    own = _marked_body(sink, successor["id"])
    assert "Status: QUEUED" in own
    assert scheduled in own
    for body in (predecessor, own):
        assert "NEEDS HUMAN" not in body
        assert "Could not complete:" not in body
        assert USED_PHRASE not in body
    assert _state_labels(sink, number) == {QUEUED_LABEL}
    assert _needs_human_applications(sink) == 0

    token = _status_rows(item.request_id)[0]["card_token"]
    card = client.get(f"/v1/factory/cards/{token}.svg")
    assert card.status_code == 200, card.text
    assert "RETRYING" in card.text
    assert "NEEDS HUMAN" not in card.text

    view = _outcome(client, item.work_item_id)
    assert view["state"] == "waiting"
    assert view["actionable_cause"].startswith("automatic retry scheduled for ")
    assert view["requests"][-1]["retry_kind"] == "failure"


def test_blocked_successor_shows_queued_label_and_named_condition_not_needs_human(
    admitted: Any,
) -> None:
    client, github, sink = admitted
    number = 54293
    item = _tracker_issue(client, github, number)
    _tracker_fail(client, item.request_id, "model_usage_limited")

    successor = _successor(item.request_id)
    assert successor is not None
    assert successor["blocked_on"] == "provider_usage_reset"
    assert successor["dispatch_not_before"] == successor["wait_deadline"]
    blocked = (
        f"Blocked: the automatic retry waits for {condition_text('provider_usage_reset')} "
        "(condition `provider_usage_reset`). If that does not happen by "
        f"{utc_text(successor['wait_deadline'])}, the retry expires."
    )
    predecessor = _marked_body(sink, item.request_id)
    own = _marked_body(sink, successor["id"])
    assert "Status: RETRYING" in predecessor
    assert "Status: QUEUED" in own
    for body in (predecessor, own):
        assert blocked in body
        assert "NEEDS HUMAN" not in body
        assert "capacity" not in body
        assert USED_PHRASE not in body
    assert _state_labels(sink, number) == {QUEUED_LABEL}
    assert _needs_human_applications(sink) == 0

    view = _outcome(client, item.work_item_id)
    assert view["state"] == "waiting"
    cause = view["actionable_cause"]
    assert cause.startswith(
        "automatic retry blocked on provider_usage_reset: it waits for "
        f"{condition_text('provider_usage_reset')}; waiting deadline "
    ), cause
    assert "capacity" not in cause
    assert view["requests"][-1]["blocked_on"] == "provider_usage_reset"


def test_final_bound_writes_exactly_one_needs_human_with_cause_and_used_count(
    admitted: Any,
) -> None:
    client, github, sink = admitted
    number = 54294
    item = _tracker_issue(client, github, number)
    chain = [item.request_id]
    _tracker_fail(client, item.request_id, "model_unreachable")
    for slot in range(1, LIMIT + 1):
        successor = _successor(chain[-1])
        assert successor is not None
        assert _state_labels(sink, number) == {QUEUED_LABEL}
        assert _needs_human_applications(sink) == 0
        assert f"automatic retry {slot} of {LIMIT}" in _marked_body(sink, chain[-1])
        chain.append(successor["id"])
        _make_due(successor["id"])
        _reconcile_and_sync()
        _tracker_fail(client, successor["id"], "model_unreachable")
    assert _successor(chain[-1]) is None
    assert _used(item.work_item_id) == LIMIT

    final = _marked_body(sink, chain[-1])
    assert "Status: NEEDS HUMAN" in final
    assert "Could not complete: " in final
    assert f"\n{LIMIT} of {LIMIT} {USED_PHRASE}.\n" in final
    assert "Cause: model_unreachable" in final
    assert "Retrying:" not in final
    # Across the whole chain the used count is shown once, on the final bound.
    bodies = [_marked_body(sink, request_id) for request_id in chain]
    assert sum(body.count(USED_PHRASE) for body in bodies) == 1
    for body in bodies[:-1]:
        assert "Status: RETRYING" in body
        assert "NEEDS HUMAN" not in body
    assert _needs_human_applications(sink) == 1
    assert _state_labels(sink, number) == {NEEDS_HUMAN}

    view = _outcome(client, item.work_item_id)
    assert (view["state"], view["automatic_retries_used"]) == ("failed", LIMIT)
    assert [r["retry_kind"] for r in view["requests"]] == [None, "failure", "failure"]


def test_blocked_successor_expiry_outcome_names_the_condition(
    api: TestClient, auth_headers: dict[str, str]
) -> None:
    """Outcome sentence half of test 24, through the public outcome route."""

    item = _admit(api, _agent(api, auth_headers), 54295)
    successor = _blocked_successor(api, item)
    _expire_wait(successor["id"])
    _reconcile()
    expired = _request(successor["id"])
    view = _outcome(api, item.work_item_id)
    assert view["state"] == "expired"
    assert view["actionable_cause"] == (
        "expired: retry_condition_expired, the automatic retry was still blocked on "
        f"provider_usage_reset ({condition_text('provider_usage_reset')}) when its waiting "
        f"deadline {expired['wait_deadline'].isoformat()} elapsed"
    )
    assert view["requests"][-1]["blocked_on"] == "provider_usage_reset"
