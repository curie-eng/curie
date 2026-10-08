"""The acquire grant carries the WorkItem repository (#2992)."""

from __future__ import annotations

import asyncio
import json
import logging
import uuid

import httpx
import pytest
from curie_worker import workitem_dispatch
from curie_worker.workitem_dispatch import (
    WorkItemAcquireGrant,
    WorkItemDispatchClient,
    WorkItemRun,
    WorkItemTransportError,
)

REQUEST_ID = uuid.UUID("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")
WORK_ITEM_ID = uuid.UUID("bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb")


def _acquire(body: dict[str, object]) -> object:
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=body)

    async def go() -> object:
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
            client = WorkItemDispatchClient(
                api_base_url="http://api.example", worker_token="example-token", client=http
            )
            return await client.acquire(REQUEST_ID, owner="worker-1", generation=1)

    return asyncio.run(go())


BASE = {
    "generation": 1,
    "work_item_id": str(WORK_ITEM_ID),
    "conversation_id": "work-item-thread",
    "wait_deadline": "2026-09-23T02:00:00+00:00",
}


def test_acquire_grant_carries_the_work_item_repository() -> None:
    grant = _acquire({**BASE, "repo_full_name": "acme-corp/widgets"})
    assert grant.repo_full_name == "acme-corp/widgets"  # type: ignore[attr-defined]


@pytest.mark.parametrize("body", [BASE, {**BASE, "repo_full_name": None}])
def test_acquire_from_an_api_without_the_field_still_grants(body: dict[str, object]) -> None:
    # A worker rolled out ahead of its API replica must not fail an acquisition
    # the API has already committed.
    grant = _acquire(body)
    assert grant.repo_full_name is None  # type: ignore[attr-defined]


# --- #3076 orphan recovery verbs -------------------------------------------

from curie_worker.workitem_dispatch import WorkItemConflict  # noqa: E402


def _run(handler, call):  # type: ignore[no-untyped-def]
    async def go() -> object:
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
            client = WorkItemDispatchClient(
                api_base_url="http://api.example", worker_token="example-token", client=http
            )
            return await call(client)

    return asyncio.run(go())


def test_runtime_owners_parses_the_running_list() -> None:
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(
            200,
            json={
                "requests": [
                    {"request_id": str(REQUEST_ID), "runtime_owner": "w-old", "runtime_epoch": 2}
                ]
            },
        )

    owners = _run(handler, lambda c: c.runtime_owners())
    assert seen[0].method == "GET"
    assert seen[0].url.path == "/v1/internal/work-items/runtime-owners"
    assert seen[0].headers["X-Curie-Worker-Token"] == "example-token"
    assert [(o.request_id, o.runtime_owner, o.runtime_epoch) for o in owners] == [  # type: ignore[attr-defined]
        (REQUEST_ID, "w-old", 2)
    ]


def test_declare_owner_lost_posts_owner_and_epoch() -> None:
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(
            200, json={"status": "cancellation_requested", "terminal_cause": "owner_lost"}
        )

    _run(handler, lambda c: c.declare_owner_lost(REQUEST_ID, owner="w-old", runtime_epoch=2))
    assert seen[0].method == "POST"
    assert seen[0].url.path == f"/v1/internal/work-items/requests/{REQUEST_ID}/owner-lost"
    import json

    assert json.loads(seen[0].content) == {"owner": "w-old", "runtime_epoch": 2}


def test_declare_owner_lost_raises_the_conflict_code_on_409() -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(409, json={"detail": {"code": "stale_owner"}})

    with pytest.raises(WorkItemConflict) as caught:
        _run(handler, lambda c: c.declare_owner_lost(REQUEST_ID, owner="w-old", runtime_epoch=2))
    assert caught.value.code == "stale_owner"


def test_runtime_owners_sends_the_after_cursor() -> None:
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(200, json={"requests": []})

    _run(handler, lambda c: c.runtime_owners())
    _run(handler, lambda c: c.runtime_owners(after=REQUEST_ID))
    assert "after" not in seen[0].url.params
    assert seen[1].url.params["after"] == str(REQUEST_ID)


# Conflict response contract: apps/api/src/curie_api/workitem_dispatch.py::_map_finish_conflict.
@pytest.mark.parametrize("operation", ["finish", "hold_for_approval", "defer"])
@pytest.mark.parametrize("refused", [False, True])
def test_run_settlement_retries_transports_but_not_conflicts(
    monkeypatch, operation, refused
) -> None:
    from curie_worker import api_retry

    async def go() -> None:
        now = 0.0
        sleeps: list[float] = []
        seen: list[httpx.Request] = []

        async def sleep(delay: float) -> None:
            nonlocal now
            sleeps.append(delay)
            now += delay

        monkeypatch.setattr(api_retry, "_clock", lambda: now)
        monkeypatch.setattr(api_retry, "_sleep", sleep)

        def handler(request: httpx.Request) -> httpx.Response:
            seen.append(request)
            if refused:
                return httpx.Response(409, json={"detail": {"code": "not_running"}})
            if len(seen) == 1:
                raise httpx.ConnectError("API restarting", request=request)
            return httpx.Response(503 if len(seen) == 2 else 200, json={})

        async def stop(_thread: str, _run: WorkItemRun) -> None:
            raise AssertionError("No heartbeat was started")

        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
            run = WorkItemRun(
                client=WorkItemDispatchClient(
                    api_base_url="http://api.example", worker_token="example-token", client=http
                ),
                request_id=REQUEST_ID,
                owner="worker-1",
                grant=WorkItemAcquireGrant(1, WORK_ITEM_ID, "thread", "2099-01-01T00:00:00Z", None),
                event_id="event",
                thread_key="slack:C1:thread",
                on_stop=stop,
                on_stale=stop,
            )
            run.started = True
            run.runtime_epoch = 1

            async def settle() -> None:
                if operation == "finish":
                    await run.finish(outcome="delivered", cause="completed", detail="done")
                elif operation == "hold_for_approval":
                    await run.hold_for_approval()
                else:
                    await run.defer("capacity", capacity=True)

            if refused:
                with pytest.raises(WorkItemConflict) as error:
                    await settle()
                assert error.value.code == "not_running"
                assert len(seen) == 1
                assert sleeps == []
                assert not run.finished
            else:
                await settle()
                assert len(seen) == 3
                assert sleeps == [0.5, 1.0]
                assert all(request.content == seen[0].content for request in seen)
                assert run.finished is (operation == "finish")

    asyncio.run(go())


def test_run_finish_clamps_retry_budget_to_execution_deadline(monkeypatch) -> None:
    from datetime import UTC, datetime, timedelta

    from curie_worker import api_retry

    async def go() -> None:
        now = 0.0
        sleeps: list[float] = []

        async def sleep(delay: float) -> None:
            nonlocal now
            sleeps.append(delay)
            now += delay

        monkeypatch.setattr(api_retry, "_clock", lambda: now)
        monkeypatch.setattr(api_retry, "_sleep", sleep)

        def handler(request: httpx.Request) -> httpx.Response:
            raise httpx.ConnectError("API absent", request=request)

        async def stop(_thread: str, _run: WorkItemRun) -> None:
            pass

        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
            run = WorkItemRun(
                client=WorkItemDispatchClient(
                    api_base_url="http://api.example", worker_token="example-token", client=http
                ),
                request_id=REQUEST_ID,
                owner="worker-1",
                grant=WorkItemAcquireGrant(1, WORK_ITEM_ID, "thread", "2099-01-01T00:00:00Z", None),
                event_id="event",
                thread_key="slack:C1:thread",
                on_stop=stop,
                on_stale=stop,
            )
            run.runtime_epoch = 1
            run.execution_deadline = datetime.now(UTC) + timedelta(seconds=3)
            with pytest.raises(WorkItemTransportError):
                await run.finish(outcome="delivered", cause="completed", detail="done")
        assert 2.9 <= sum(sleeps) <= 3

    asyncio.run(go())


@pytest.mark.parametrize("error_type", [httpx.ReadTimeout, httpx.ConnectError])
def test_settlement_empty_transport_message_retains_the_exception_type(
    monkeypatch, error_type
) -> None:
    from curie_worker import api_retry

    async def go() -> None:
        now = 0.0
        sleeps: list[float] = []
        seen: list[httpx.Request] = []

        async def sleep(delay: float) -> None:
            nonlocal now
            sleeps.append(delay)
            now += delay

        monkeypatch.setattr(api_retry, "_clock", lambda: now)
        monkeypatch.setattr(api_retry, "_sleep", sleep)

        def handler(request: httpx.Request) -> httpx.Response:
            seen.append(request)
            raise error_type("", request=request)

        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
            client = WorkItemDispatchClient(
                api_base_url="http://api.example",
                worker_token="example-token",
                client=http,
            )
            with pytest.raises(WorkItemTransportError, match=error_type.__name__) as caught:
                await client.finish(
                    REQUEST_ID,
                    runtime_epoch=1,
                    outcome="delivered",
                    cause="completed",
                    detail="done",
                    budget_s=3,
                )

        assert isinstance(caught.value.__cause__, error_type)
        assert str(caught.value.__cause__) == ""
        assert str(caught.value) == (
            f"work-item dispatch endpoint is unreachable: {error_type.__name__}"
        )
        assert sleeps == [0.5, 1.0, 1.5]
        assert len(seen) == 4
        assert all(attempt.content == seen[0].content for attempt in seen)

    asyncio.run(go())


# --- #4170 a start deferral can end the request ------------------------------

# Response shape: apps/api/src/curie_api/routers/work_items.py defer route.
_TERMINAL_DEFER = {"dispatch_generation": 6, "not_before": None, "terminal_cause": "start_failed"}
_NONTERMINAL_DEFER = {
    "dispatch_generation": 2,
    "not_before": "2026-10-07T12:00:30+00:00",
    "terminal_cause": None,
}


def _deferring_run(body: object) -> tuple[str | None, WorkItemRun]:
    async def go() -> tuple[str | None, WorkItemRun]:
        def handler(_request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, json=body)

        async def stop(_thread: str, _run: WorkItemRun) -> None:
            raise AssertionError("No heartbeat was started")

        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
            client = WorkItemDispatchClient(
                api_base_url="http://api.example", worker_token="example-token", client=http
            )
            cause = await client.defer(
                REQUEST_ID, owner="worker-1", generation=1, reason="thread_busy", capacity=False
            )
            run = WorkItemRun(
                client=client,
                request_id=REQUEST_ID,
                owner="worker-1",
                grant=WorkItemAcquireGrant(1, WORK_ITEM_ID, "thread", "2099-01-01T00:00:00Z", None),
                event_id="event",
                thread_key="slack:C1:thread",
                on_stop=stop,
                on_stale=stop,
            )
            await run.defer("not_started:classified_failure", capacity=False)
            return cause, run

    return asyncio.run(go())


def test_a_terminal_start_deferral_finishes_the_run() -> None:
    # The fifth non-capacity deferral ends the request start_failed; the run
    # must count as settled so the kernel releases the claim this delivery made.
    cause, run = _deferring_run(_TERMINAL_DEFER)
    assert cause == "start_failed"
    assert run.finished is True
    assert run.started is False


@pytest.mark.parametrize(
    "body",
    [_NONTERMINAL_DEFER, {"dispatch_generation": 2, "not_before": None}],
    ids=["terminal_cause_null", "terminal_cause_absent"],
)
def test_a_nonterminal_deferral_keeps_the_run_open(body: dict[str, object]) -> None:
    # The request stays waiting and the next acquire adopts this thread's
    # route, so the claim must stay standing.
    cause, run = _deferring_run(body)
    assert cause is None
    assert run.finished is False


@pytest.mark.parametrize("terminal_cause", [5, "", ["start_failed"]])
def test_a_deferral_with_an_unusable_terminal_cause_is_a_transport_error(
    terminal_cause: object,
) -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={**_TERMINAL_DEFER, "terminal_cause": terminal_cause})

    with pytest.raises(WorkItemTransportError):
        _run(
            handler,
            lambda c: c.defer(
                REQUEST_ID, owner="worker-1", generation=1, reason="thread_busy", capacity=False
            ),
        )


class _AcquireRenewClock:
    def __init__(self) -> None:
        self.now = 0.0
        self._waiters: list[tuple[float, asyncio.Future[None]]] = []

    async def sleep(self, delay: float) -> None:
        assert delay == 20.0
        future = asyncio.get_running_loop().create_future()
        waiter = (self.now + delay, future)
        self._waiters.append(waiter)
        try:
            await future
        finally:
            self._waiters.remove(waiter)

    async def advance(self, now: float) -> None:
        # Let newly constructed tasks register their first timer at the old time.
        await asyncio.sleep(0)
        self.now = now
        for deadline, future in tuple(self._waiters):
            if deadline <= now and not future.done():
                future.set_result(None)
        await asyncio.sleep(0)


def _renewing_run(client: WorkItemDispatchClient) -> WorkItemRun:
    async def unexpected_stop(_thread: str, _run: WorkItemRun) -> None:
        raise AssertionError("Acquire renewal must not stop the running turn")

    return WorkItemRun(
        client=client,
        request_id=REQUEST_ID,
        owner="worker-1",
        grant=WorkItemAcquireGrant(1, WORK_ITEM_ID, "thread", "2099-01-01T00:00:00Z", None),
        event_id="event",
        thread_key="slack:C0EXAMPLE1:thread",
        on_stop=unexpected_stop,
        on_stale=unexpected_stop,
    )


@pytest.fixture
def acquire_renew_clock(monkeypatch: pytest.MonkeyPatch) -> _AcquireRenewClock:
    clock = _AcquireRenewClock()
    monkeypatch.setattr(workitem_dispatch, "_renew_sleep", clock.sleep)
    return clock


def _renewal_client(
    http: httpx.AsyncClient,
) -> WorkItemDispatchClient:
    return WorkItemDispatchClient(
        api_base_url="http://api.example", worker_token="example-token", client=http
    )


def test_acquire_renewal_keeps_the_same_owner_and_generation_every_twenty_seconds(
    acquire_renew_clock: _AcquireRenewClock,
) -> None:
    clock = acquire_renew_clock
    seen: list[tuple[float, httpx.Request]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append((clock.now, request))
        # Grant shape is defined by the API's internal acquire route.
        return httpx.Response(200, json={**BASE, "repo_full_name": "acme-corp/acme-bot"})

    async def go() -> None:
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
            run = _renewing_run(_renewal_client(http))
            try:
                for now in (20.0, 40.0, 60.0):
                    await clock.advance(now)
                assert [now for now, _ in seen] == [20.0, 40.0, 60.0]
                assert all(request.method == "POST" for _, request in seen)
                assert all(
                    request.url.path == f"/v1/internal/work-items/requests/{REQUEST_ID}/acquire"
                    for _, request in seen
                )
                assert all(
                    json.loads(request.content) == {"owner": "worker-1", "generation": 1}
                    for _, request in seen
                )
            finally:
                await run.close()

    asyncio.run(go())


@pytest.mark.parametrize("operation", ["start", "defer", "close"])
def test_acquire_renewal_stops_after_start_defer_or_close_at_thirty_seconds(
    acquire_renew_clock: _AcquireRenewClock, operation: str
) -> None:
    clock = acquire_renew_clock
    seen: list[tuple[float, str]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        verb = request.url.path.rsplit("/", 1)[-1]
        seen.append((clock.now, verb))
        # Response shapes are the internal acquire/start/defer route contracts.
        if verb == "start":
            return httpx.Response(
                200,
                json={
                    "runtime_epoch": 1,
                    "execution_deadline": "2099-01-01T00:00:00+00:00",
                    "remaining_s": 600,
                    "heartbeat_interval_s": 1000,
                },
            )
        if verb == "defer":
            return httpx.Response(200, json=_NONTERMINAL_DEFER)
        return httpx.Response(200, json=BASE)

    async def go() -> None:
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
            run = _renewing_run(_renewal_client(http))
            try:
                await clock.advance(20.0)
                await clock.advance(30.0)
                if operation == "start":
                    await run.start(claim_name="acme-claim", sandbox_name="acme-sandbox")
                elif operation == "defer":
                    await run.defer("thread_busy", capacity=False)
                else:
                    await run.close()
                await clock.advance(40.0)
                await clock.advance(60.0)
                expected = [(20.0, "acquire")]
                if operation != "close":
                    expected.append((30.0, operation))
                assert seen == expected
            finally:
                await run.close()

    asyncio.run(go())


@pytest.mark.parametrize("state", ["started", "finished"])
def test_acquire_renewal_never_posts_for_a_run_ended_before_the_first_tick(
    acquire_renew_clock: _AcquireRenewClock, state: str
) -> None:
    clock = acquire_renew_clock
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(200, json=BASE)

    async def go() -> None:
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
            run = _renewing_run(_renewal_client(http))
            setattr(run, state, True)
            try:
                for now in (20.0, 40.0, 60.0):
                    await clock.advance(now)
                assert seen == []
            finally:
                await run.close()

    asyncio.run(go())


@pytest.mark.parametrize(
    "code",
    [
        "duplicate",
        "not_published",
        "not_dispatchable",
        "waiting_deadline_elapsed",
        "work_item_cancelled",
    ],
)
def test_acquire_renewal_stops_on_any_conflict_without_other_actions(
    acquire_renew_clock: _AcquireRenewClock, caplog: pytest.LogCaptureFixture, code: str
) -> None:
    clock = acquire_renew_clock
    seen: list[tuple[float, str]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append((clock.now, request.url.path.rsplit("/", 1)[-1]))
        # HTTP 409 shape is defined by the API's internal acquire route.
        return httpx.Response(409, json={"detail": {"code": code}})

    async def go() -> None:
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
            run = _renewing_run(_renewal_client(http))
            try:
                for now in (20.0, 40.0, 60.0):
                    await clock.advance(now)
                assert seen == [(20.0, "acquire")]
                assert not run.started
                assert not run.finished
            finally:
                await run.close()

    with caplog.at_level(logging.INFO, logger=workitem_dispatch.__name__):
        asyncio.run(go())
    records = [record for record in caplog.records if record.name == workitem_dispatch.__name__]
    assert len(records) == 1
    assert records[0].levelno == logging.INFO
    assert str(REQUEST_ID) in records[0].getMessage()
    assert code in records[0].getMessage()


def test_acquire_renewal_continues_after_a_transport_failure(
    acquire_renew_clock: _AcquireRenewClock, caplog: pytest.LogCaptureFixture
) -> None:
    clock = acquire_renew_clock
    seen: list[tuple[float, str]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append((clock.now, request.url.path.rsplit("/", 1)[-1]))
        if clock.now == 20:
            raise httpx.ConnectError("API restarting", request=request)
        return httpx.Response(200, json=BASE)

    async def go() -> None:
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
            run = _renewing_run(_renewal_client(http))
            try:
                for now in (20.0, 40.0, 60.0):
                    await clock.advance(now)
                assert seen == [(20.0, "acquire"), (40.0, "acquire"), (60.0, "acquire")]
                assert not run.started
                assert not run.finished
            finally:
                await run.close()

    asyncio.run(go())
    records = [record for record in caplog.records if record.name == workitem_dispatch.__name__]
    assert len(records) == 1
    assert records[0].levelno == logging.WARNING
    assert str(REQUEST_ID) in records[0].getMessage()


def test_failed_start_keeps_renewing_the_acquisition(
    acquire_renew_clock: _AcquireRenewClock,
) -> None:
    clock = acquire_renew_clock
    seen: list[tuple[float, str]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        verb = request.url.path.rsplit("/", 1)[-1]
        seen.append((clock.now, verb))
        if verb == "start":
            raise httpx.ConnectError("API restarting", request=request)
        return httpx.Response(200, json=BASE)

    async def go() -> None:
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
            run = _renewing_run(_renewal_client(http))
            try:
                await clock.advance(20.0)
                await clock.advance(30.0)
                with pytest.raises(WorkItemTransportError):
                    await run.start(claim_name="acme-claim", sandbox_name="acme-sandbox")
                await clock.advance(40.0)
                assert seen == [(20.0, "acquire"), (30.0, "start"), (40.0, "acquire")]
                assert not run.started
            finally:
                await run.close()

    asyncio.run(go())


def test_defer_stops_renewal_before_its_post_returns_or_raises(
    acquire_renew_clock: _AcquireRenewClock,
) -> None:
    clock = acquire_renew_clock
    seen: list[tuple[float, str]] = []

    async def go() -> None:
        defer_entered = asyncio.Event()
        release_defer = asyncio.Event()

        async def handler(request: httpx.Request) -> httpx.Response:
            verb = request.url.path.rsplit("/", 1)[-1]
            seen.append((clock.now, verb))
            if verb == "defer":
                defer_entered.set()
                await release_defer.wait()
                return httpx.Response(409, json={"detail": {"code": "not_dispatchable"}})
            return httpx.Response(200, json=BASE)

        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
            run = _renewing_run(_renewal_client(http))
            pending: asyncio.Task[None] | None = None
            try:
                await clock.advance(20.0)
                await clock.advance(30.0)
                pending = asyncio.create_task(run.defer("thread_busy", capacity=False))
                await defer_entered.wait()
                await clock.advance(40.0)
                assert seen == [(20.0, "acquire"), (30.0, "defer")]
                release_defer.set()
                with pytest.raises(WorkItemConflict):
                    await pending
                await clock.advance(60.0)
                assert seen == [(20.0, "acquire"), (30.0, "defer")]
            finally:
                release_defer.set()
                if pending is not None and not pending.done():
                    pending.cancel()
                    with pytest.raises(asyncio.CancelledError):
                        await pending
                await run.close()

    asyncio.run(go())
