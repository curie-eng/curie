"""The acquire grant carries the WorkItem repository (#2992)."""

from __future__ import annotations

import asyncio
import uuid

import httpx
import pytest
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
