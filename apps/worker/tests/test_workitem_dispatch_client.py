"""The acquire grant carries the WorkItem repository (#2992)."""

from __future__ import annotations

import asyncio
import dataclasses
import hashlib
import json
import logging
import uuid
from datetime import UTC, datetime

import httpx
import pytest
from curie_worker import workitem_dispatch
from curie_worker.binding import FactoryContext, FactoryWake
from curie_worker.sandbox.types import DeliveredLimits
from curie_worker.workitem_dispatch import (
    WorkItemAcquireGrant,
    WorkItemDispatchClient,
    WorkItemRun,
    WorkItemRunning,
    WorkItemStartRefused,
    WorkItemTransportError,
)

UNOBSERVED_LIMITS = DeliveredLimits(
    memory_bytes=None, ephemeral_storage_bytes=None, scratch_bytes={}, max_turns=None
).to_wire()


REQUEST_ID = uuid.UUID("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")
WORK_ITEM_ID = uuid.UUID("bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb")

AGENT_ID = uuid.UUID("11111111-1111-4111-8111-111111111111")
CONTEXT = FactoryContext(
    agent_id=AGENT_ID,
    tenant_id=uuid.UUID("00000000-0000-0000-0000-000000000001"),
    binding_id=uuid.UUID("33333333-3333-4333-8333-333333333333"),
    kind="slack",
    address="C0EXAMPLE1",
    adapter="default",
)
WAKE = FactoryWake(
    kind="slack", address="C0EXAMPLE1", adapter="default", conversation_id="thread"
)


def _grant() -> WorkItemAcquireGrant:
    return WorkItemAcquireGrant(
        generation=1,
        work_item_id=WORK_ITEM_ID,
        conversation_id="slack:C0EXAMPLE1:thread",
        wait_deadline=datetime(2099, 1, 1, tzinfo=UTC),
        repository_path="acme-corp/widgets",
        execution_kind="implementation",
        recovery_candidate_id=None,
        context=CONTEXT,
        resource_gate=None,
        predecessor_claim_name=None,
    )


def _acquire(body: dict[str, object]) -> WorkItemAcquireGrant:
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=body)

    async def go() -> WorkItemAcquireGrant:
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
            client = WorkItemDispatchClient(
                api_base_url="http://api.example", worker_token="example-token", client=http
            )
            return await client.acquire(
                REQUEST_ID, owner="worker-1", generation=1, wake=WAKE, context=None
            )

    return asyncio.run(go())


BASE = {
    "generation": 1,
    "work_item_id": str(WORK_ITEM_ID),
    "conversation_id": "work-item-thread",
    "wait_deadline": "2026-09-23T02:00:00+00:00",
    "repository_path": "acme-corp/widgets",
    "context": CONTEXT.model_dump(mode="json"),
    "execution_kind": "implementation",
    "recovery_candidate_id": None,
    "resource_gate": None,
    "predecessor_claim_name": None,
}


def test_acquire_grant_carries_the_work_item_repository() -> None:
    grant = _acquire({**BASE, "repository_path": "acme-corp/widgets"})
    assert grant.repository_path == "acme-corp/widgets"  # type: ignore[attr-defined]


@pytest.mark.parametrize(
    "field",
    [
        "generation", "work_item_id", "conversation_id",
        "wait_deadline", "repository_path", "context",
        "execution_kind", "recovery_candidate_id",
        "resource_gate", "predecessor_claim_name",
    ],
)
def test_acquire_refuses_a_missing_required_field(field: str) -> None:
    body = dict(BASE)
    del body[field]
    with pytest.raises(WorkItemTransportError):
        _acquire(body)


def test_acquire_grant_carries_the_publication_recovery_candidate() -> None:
    candidate = uuid.uuid4()
    grant = _acquire(
        {
            **BASE,
            "execution_kind": "publication_recovery",
            "recovery_candidate_id": str(candidate),
        }
    )
    assert grant.execution_kind == "publication_recovery"
    assert grant.recovery_candidate_id == candidate
    plain = _acquire(BASE)
    assert (plain.execution_kind, plain.recovery_candidate_id) == ("implementation", None)


@pytest.mark.parametrize("running", [False, True])
@pytest.mark.parametrize("sha_length", [40, 64])
def test_conflict_workspace_pins_are_a_pair_on_the_authenticated_request(
    running: bool, sha_length: int
) -> None:
    def read(fields: dict[str, object]) -> WorkItemAcquireGrant | WorkItemRunning:
        if running:
            return _run(
                lambda _: httpx.Response(200, json={**_running_body(), **fields}),
                lambda client: client.running_for_conversation("thread", wake=WAKE),
            )
        return _acquire({**BASE, **fields})

    head, base = "a" * sha_length, "b" * sha_length
    fields = {"conflict_head_sha": head, "conflict_base_sha": base, "conflict_paths": ["rules"]}
    pins = read(fields).conflict_workspace
    assert pins is not None and (pins.head_sha, pins.base_sha) == (head, base)
    assert pins.paths == ("rules",)
    assert read({}).conflict_workspace is None
    for invalid in (
        {**fields, "conflict_paths": []},
        {**fields, "conflict_paths": None},
        {**fields, "conflict_paths": ["rules", "rules"]},
        {"conflict_head_sha": head},
        {"conflict_base_sha": base},
        {**fields, "conflict_base_sha": head},
        {**fields, "conflict_head_sha": "main"},
        {**fields, "conflict_base_sha": "release"},
    ):
        with pytest.raises(WorkItemTransportError):
            read(invalid)


@pytest.mark.parametrize(
    ("kind", "candidate"),
    [
        ("publication_recovery", None),
        ("implementation", "cccccccc-cccc-4ccc-8ccc-cccccccccccc"),
        ("recovery", "cccccccc-cccc-4ccc-8ccc-cccccccccccc"),
        (None, None),
        ("publication_recovery", "not-a-uuid"),
    ],
)
def test_acquire_refuses_an_unusable_execution_kind(kind: object, candidate: object) -> None:
    with pytest.raises(WorkItemTransportError):
        _acquire({**BASE, "execution_kind": kind, "recovery_candidate_id": candidate})


def test_only_an_acquired_recovery_run_is_a_publication_recovery() -> None:
    candidate = uuid.uuid4()

    async def go() -> tuple[WorkItemRun, WorkItemRun]:
        async with httpx.AsyncClient(
            transport=httpx.MockTransport(lambda _r: httpx.Response(500))
        ) as http:
            client = WorkItemDispatchClient(
                api_base_url="http://api.example", worker_token="example-token", client=http
            )
            runs = []
            for grant in (
                dataclasses.replace(
                    _grant(),
                    execution_kind="publication_recovery",
                    recovery_candidate_id=candidate,
                ),
                _grant(),
            ):
                run = WorkItemRun(
                    client=client,
                    request_id=REQUEST_ID,
                    owner="worker-1",
                    grant=grant,
                    wake=WAKE,
                    event_id=f"work-item-{REQUEST_ID}-execute-1",
                    thread_key="slack:C0EXAMPLE1:thread",
                    on_stop=_noop_stop,
                    on_stale=_noop_stop,
                )
                await run.close()
                runs.append(run)
            return runs[0], runs[1]

    recovery, implementation = asyncio.run(go())
    assert recovery.is_publication_recovery
    assert recovery.recovery_candidate_id == candidate
    assert not implementation.is_publication_recovery
    assert implementation.recovery_candidate_id is None


async def _noop_stop(_thread_key: str, _run: WorkItemRun) -> None:
    return None


@pytest.mark.parametrize("repository", [None, "", " ", 42])
def test_acquire_refuses_an_unusable_repository(repository: object) -> None:
    with pytest.raises(WorkItemTransportError):
        _acquire({**BASE, "repository_path": repository})


@pytest.mark.parametrize(
    "field", ["agent_id", "tenant_id", "binding_id", "kind", "address", "adapter"]
)
def test_acquire_refuses_incomplete_authority(field: str) -> None:
    context = CONTEXT.model_dump(mode="json")
    del context[field]
    with pytest.raises(WorkItemTransportError):
        _acquire({**BASE, "context": context})


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


def test_acquire_sends_wake_and_initial_authority_explicitly() -> None:
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(200, json=BASE)

    granted = _run(
        handler,
        lambda client: client.acquire(
            REQUEST_ID, owner="worker-1", generation=1, wake=WAKE, context=None
        ),
    )
    assert json.loads(seen[0].content) == {
        "owner": "worker-1",
        "generation": 1,
        "wake": WAKE.model_dump(mode="json"),
        "context": None,
    }
    assert granted.context == CONTEXT  # type: ignore[attr-defined]
    assert granted.agent_id == AGENT_ID  # type: ignore[attr-defined]
    assert granted.wait_deadline == datetime(2026, 9, 23, 2, tzinfo=UTC)  # type: ignore[attr-defined]


def test_only_matching_execute_delivery_attests_to_its_received_text() -> None:
    from channel_protocol.work_item_events import ci_event_id, execute_event_id

    received = "Preserve this line exactly.\n\tRésumé "
    observed = []

    def handler(request: httpx.Request) -> httpx.Response:
        observed.append(json.loads(request.content).get("input_sha256"))
        return httpx.Response(200, json=BASE)

    async def acquire_for_delivery(client: WorkItemDispatchClient) -> None:
        async def acquire_once(request_id: uuid.UUID, generation: int) -> None:
            await client.acquire(
                request_id, owner="reader", generation=generation, wake=WAKE, context=None
            )

        with workitem_dispatch.received_execution_input(execute_event_id(REQUEST_ID, 4), received):
            await acquire_once(REQUEST_ID, 4)
            await acquire_once(REQUEST_ID, 3)
            await acquire_once(uuid.uuid4(), 4)
        with workitem_dispatch.received_execution_input(ci_event_id(REQUEST_ID, 2), received):
            await acquire_once(REQUEST_ID, 2)
        await acquire_once(REQUEST_ID, 4)

    _run(handler, acquire_for_delivery)
    assert observed == [hashlib.sha256(received.encode()).hexdigest(), None, None, None, None]


def _running_body() -> dict[str, object]:
    # The running lookup carries no execution kind, gate or fence: only acquire
    # grants them, and a running request is past its start.
    acquire_only = {
        "execution_kind",
        "recovery_candidate_id",
        "resource_gate",
        "predecessor_claim_name",
    }
    return {
        **{key: value for key, value in BASE.items() if key not in acquire_only},
        "request_id": str(REQUEST_ID),
        "agent_id": str(AGENT_ID),
        "runtime_epoch": 7,
        "execution_deadline": "2099-01-01T00:00:00+00:00",
        "lease_held_to_deadline": False,
        "refusal": None,
    }


@pytest.mark.parametrize("lease_held_to_deadline", [False, True])
def test_running_sends_the_wake_and_preserves_persisted_execution_facts(
    lease_held_to_deadline: bool,
) -> None:
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(
            200, json={**_running_body(), "lease_held_to_deadline": lease_held_to_deadline},
        )

    found = _run(
        handler, lambda client: client.running_for_conversation("thread", wake=WAKE)
    )
    assert isinstance(found, WorkItemRunning)
    assert dict(seen[0].url.params) == {
        "conversation_id": "thread",
        "wake_kind": WAKE.kind,
        "wake_address": WAKE.address,
        "wake_adapter": "default",
        "wake_conversation_id": WAKE.conversation_id,
    }
    assert found.agent_id == AGENT_ID
    assert found.generation == 1
    assert found.runtime_epoch == 7
    assert found.conversation_id == BASE["conversation_id"]
    assert found.wait_deadline == datetime(2026, 9, 23, 2, tzinfo=UTC)
    assert found.repository_path == "acme-corp/widgets"
    assert found.context == CONTEXT
    assert found.refusal is None
    assert found.lease_held_to_deadline is lease_held_to_deadline


# An API older than the single-path dispatch body may still send a completion
# field. The worker reads none: every grant is an implementation or a
# publication recovery run by its execution kind alone, and a stale value, of
# any spelling, neither fails the body nor changes the run.
_STALE_COMPLETION_VALUES = ["implementation", "an-unknown-contract", 7]


@pytest.mark.parametrize("stale", _STALE_COMPLETION_VALUES)
def test_a_stale_completion_field_in_the_dispatch_body_is_ignored(stale: object) -> None:
    stale_body = {"completion_contract": stale}
    acquired = _acquire({**BASE, **stale_body})
    assert acquired == _acquire(BASE)
    assert {field.name for field in dataclasses.fields(acquired)} == {
        "generation",
        "work_item_id",
        "conversation_id",
        "wait_deadline",
        "repository_path",
        "context",
        "execution_kind",
        "recovery_candidate_id",
        "resource_gate",
        "predecessor_claim_name",
        "conflict_workspace",
    }

    found = _run(
        lambda _request: httpx.Response(200, json={**_running_body(), **stale_body}),
        lambda client: client.running_for_conversation("thread", wake=WAKE),
    )
    assert isinstance(found, WorkItemRunning)
    assert found == _run(
        lambda _request: httpx.Response(200, json=_running_body()),
        lambda client: client.running_for_conversation("thread", wake=WAKE),
    )

    start_body = {
        "runtime_epoch": 1,
        "execution_deadline": "2099-01-01T00:00:00+00:00",
        "remaining_s": 600,
        "heartbeat_interval_s": 1000,
    }
    started = _run(
        lambda _request: httpx.Response(200, json={**start_body, **stale_body}),
        lambda client: client.start(
            REQUEST_ID,
            owner="worker-1",
            generation=1,
            claim_name="acme-claim",
            sandbox_name="acme-sandbox",
            context=CONTEXT,
            delivered_limits=UNOBSERVED_LIMITS,
        ),
    )
    assert {field.name for field in dataclasses.fields(started)} == set(start_body)


@pytest.mark.parametrize("field", list(_running_body()))
def test_running_refuses_missing_required_facts(field: str) -> None:
    body = _running_body()
    del body[field]

    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=body)

    with pytest.raises(WorkItemTransportError):
        _run(handler, lambda client: client.running_for_conversation("thread", wake=WAKE))


def test_running_preserves_the_refusal_without_runnable_authority() -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={**_running_body(), "context": None, "refusal": "factory_binding_missing"},
        )

    found = _run(handler, lambda client: client.running_for_conversation("thread", wake=WAKE))
    assert isinstance(found, WorkItemRunning)
    assert found.request_id == REQUEST_ID
    assert found.agent_id == AGENT_ID
    assert found.runtime_epoch == 7
    assert found.repository_path == "acme-corp/widgets"
    assert found.context is None
    assert found.refusal == "factory_binding_missing"


@pytest.mark.parametrize(
    "change",
    [
        {"context": None, "refusal": None},
        {"refusal": "factory_binding_missing"},
        {"context": None, "refusal": "unrecognized_refusal"},
        {"agent_id": str(REQUEST_ID)},
        {"lease_held_to_deadline": None},
        {"lease_held_to_deadline": "false"},
        {"lease_held_to_deadline": 0},
        {"lease_held_to_deadline": 1},
        {"lease_held_to_deadline": []},
        {"lease_held_to_deadline": {}},
    ],
)
def test_running_refuses_inconsistent_authority(change: dict[str, object]) -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={**_running_body(), **change})

    with pytest.raises(WorkItemTransportError):
        _run(handler, lambda client: client.running_for_conversation("thread", wake=WAKE))


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
                grant=_grant(),
                wake=WAKE,
                event_id="event",
                thread_key="slack:C1:thread",
                on_stop=stop,
                on_stale=stop,
            )
            run.started = True
            run.runtime_epoch = 1

            async def settle() -> None:
                if operation == "finish":
                    await run.finish(
                        outcome="delivered",
                        cause="completed",
                        detail="done",
                        ci_fix_round=None,
                        retry_safety="uncertain",
                        retry_condition=None,
                        exhausted_volume=None,
                    )
                elif operation == "hold_for_approval":
                    await run.hold_for_approval()
                else:
                    await run.defer("capacity", capacity=True, start_failure_detail=None)

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
                grant=_grant(),
                wake=WAKE,
                event_id="event",
                thread_key="slack:C1:thread",
                on_stop=stop,
                on_stale=stop,
            )
            run.runtime_epoch = 1
            run.execution_deadline = datetime.now(UTC) + timedelta(seconds=3)
            with pytest.raises(WorkItemTransportError):
                await run.finish(
                    outcome="delivered",
                    cause="completed",
                    detail="done",
                    ci_fix_round=None,
                    retry_safety="uncertain",
                    retry_condition=None,
                    exhausted_volume=None,
                )
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
                    ci_fix_round=None,
                    retry_safety="uncertain",
                    retry_condition=None,
                    exhausted_volume=None,
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
                REQUEST_ID,
                owner="worker-1",
                generation=1,
                reason="thread_busy",
                capacity=False,
                start_failure_detail=None,
            )
            run = WorkItemRun(
                client=client,
                request_id=REQUEST_ID,
                owner="worker-1",
                grant=_grant(),
                wake=WAKE,
                event_id="event",
                thread_key="slack:C1:thread",
                on_stop=stop,
                on_stale=stop,
            )
            await run.defer(
                "not_started:classified_failure", capacity=False, start_failure_detail=None
            )
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
                REQUEST_ID,
                owner="worker-1",
                generation=1,
                reason="thread_busy",
                capacity=False,
                start_failure_detail=None,
            ),
        )


# --- #4930 the pre-start workspace diagnostic rides the defer ---------------


def _captured_defer_body(
    call,  # type: ignore[no-untyped-def]
) -> dict[str, object]:
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(200, json=_NONTERMINAL_DEFER)

    async def go() -> None:
        async def stop(_thread: str, _run: WorkItemRun) -> None:
            raise AssertionError("No heartbeat was started")

        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
            client = WorkItemDispatchClient(
                api_base_url="http://api.example", worker_token="example-token", client=http
            )
            run = WorkItemRun(
                client=client,
                request_id=REQUEST_ID,
                owner="worker-1",
                grant=_grant(),
                wake=WAKE,
                event_id="event",
                thread_key="slack:C1:thread",
                on_stop=stop,
                on_stale=stop,
            )
            await call(run)
            await run.close()

    asyncio.run(go())
    defers = [r for r in seen if r.url.path.endswith("/defer")]
    assert len(defers) == 1, [r.url.path for r in seen]
    assert defers[0].url.path == f"/v1/internal/work-items/requests/{REQUEST_ID}/defer"
    return json.loads(defers[0].content)


def test_defer_posts_the_start_failure_detail() -> None:
    detail = "workspace archive-size failed: archive exceeds 1048576 bytes"
    body = _captured_defer_body(
        lambda run: run.defer(
            "not_started:classified_failure", capacity=False, start_failure_detail=detail
        )
    )
    assert body == {
        "owner": "worker-1",
        "generation": 1,
        "reason": "not_started:classified_failure",
        "capacity": False,
        "start_failure_detail": detail,
    }


def test_defer_posts_a_null_start_failure_detail() -> None:
    # Explicit null, never an omitted key: the API reads both as "no
    # diagnostic", but the wire shape is pinned so a new worker always states it.
    body = _captured_defer_body(
        lambda run: run.defer("capacity", capacity=True, start_failure_detail=None)
    )
    assert body == {
        "owner": "worker-1",
        "generation": 1,
        "reason": "capacity",
        "capacity": True,
        "start_failure_detail": None,
    }


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
        grant=_grant(),
        wake=WAKE,
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
        return httpx.Response(200, json=BASE)

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
                    json.loads(request.content) == {
                        "owner": "worker-1", "generation": 1,
                        "wake": WAKE.model_dump(mode="json"),
                        "context": CONTEXT.model_dump(mode="json"),
                    }
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
                    await run.start(
                        claim_name="acme-claim",
                        sandbox_name="acme-sandbox",
                        delivered_limits=UNOBSERVED_LIMITS,
                    )
                elif operation == "defer":
                    await run.defer("thread_busy", capacity=False, start_failure_detail=None)
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
                    await run.start(
                        claim_name="acme-claim",
                        sandbox_name="acme-sandbox",
                        delivered_limits=UNOBSERVED_LIMITS,
                    )
                await clock.advance(40.0)
                assert seen == [(20.0, "acquire"), (30.0, "start"), (40.0, "acquire")]
                assert not run.started
            finally:
                await run.close()

    asyncio.run(go())


def test_a_refused_start_still_records_the_claim_and_sandbox_names(
    acquire_renew_clock: _AcquireRenewClock,
) -> None:
    # #4331: a run whose start is refused on a terminal code (#3208) must still
    # know the claim it made, so its sandbox release stays fenced to that claim.
    seen: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        verb = request.url.path.rsplit("/", 1)[-1]
        seen.append(verb)
        if verb == "start":
            # HTTP 409 shape is defined by the API's internal start route.
            return httpx.Response(409, json={"detail": {"code": "not_dispatchable"}})
        return httpx.Response(200, json=BASE)

    async def go() -> None:
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
            run = _renewing_run(_renewal_client(http))
            try:
                with pytest.raises(WorkItemConflict) as refused:
                    await run.start(
                        claim_name="acme-claim",
                        sandbox_name="acme-sandbox",
                        delivered_limits=UNOBSERVED_LIMITS,
                    )
                assert refused.value.code == "not_dispatchable"
                assert seen == ["start"]
                assert run.claim_name == "acme-claim"
                assert run.sandbox_name == "acme-sandbox"
                assert not run.started
                assert run.runtime_epoch is None
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
                pending = asyncio.create_task(
                    run.defer("thread_busy", capacity=False, start_failure_detail=None)
                )
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


@pytest.mark.parametrize("code", ["factory_binding_missing", "factory_start_failed"])
def test_factory_renewal_refusal_fences_subsequent_start_without_another_http_write(
    acquire_renew_clock: _AcquireRenewClock, code: str,
) -> None:
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(409, json={"detail": {"code": code}})

    async def go() -> None:
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
            run = _renewing_run(_renewal_client(http))
            try:
                run.ensure_acquired()
                await acquire_renew_clock.advance(20)
                with pytest.raises(WorkItemStartRefused) as refused:
                    run.ensure_acquired()
                assert refused.value.code == code
                with pytest.raises(WorkItemStartRefused) as start_refused:
                    await run.start(
                        claim_name="owned-claim",
                        sandbox_name="owned-sandbox",
                        delivered_limits=UNOBSERVED_LIMITS,
                    )
                assert start_refused.value.code == code
                assert run.claim_name == "owned-claim"
                assert run.sandbox_name == "owned-sandbox"
                assert not run.started
                assert run.finished is (code == "factory_start_failed")
                await acquire_renew_clock.advance(40)
                assert len(seen) == 1
                assert seen[0].url.path.endswith("/acquire")
                assert json.loads(seen[0].content) == {
                    "owner": "worker-1", "generation": 1,
                    "wake": WAKE.model_dump(mode="json"),
                    "context": CONTEXT.model_dump(mode="json"),
                }
            finally:
                await run.close()

    asyncio.run(go())


# --- #5427 resource gate, predecessor fence, delivered limits ------------------

_MEMORY_GATE = {
    "condition": "memory_limit",
    "dimension": "memory",
    "volume": None,
    "failed_limit": 50331648,
    "current_limit": None,
}


def test_acquire_grant_carries_the_resource_gate_and_predecessor_claim() -> None:
    grant = _acquire(
        {**BASE, "resource_gate": _MEMORY_GATE, "predecessor_claim_name": "curie-thread-old"}
    )
    assert grant.resource_gate == workitem_dispatch.ResourceGate(
        condition="memory_limit",
        dimension="memory",
        volume=None,
        failed_limit=50331648,
        current_limit=None,
    )
    assert grant.predecessor_claim_name == "curie-thread-old"
    scratch = _acquire(
        {
            **BASE,
            "resource_gate": {
                **_MEMORY_GATE,
                "condition": "scratch_limit",
                "dimension": "scratch",
                "volume": "workspace",
                "failed_limit": 8388608,
            },
        }
    )
    assert scratch.resource_gate is not None
    assert scratch.resource_gate.volume == "workspace"
    history = _acquire(
        {
            **BASE,
            "resource_gate": {
                **_MEMORY_GATE,
                "condition": "history_capacity",
                "dimension": "history_capacity",
                "current_limit": 2048,
            },
        }
    )
    assert history.resource_gate is not None
    assert history.resource_gate.current_limit == 2048


@pytest.mark.parametrize(
    "gate",
    [
        "memory_limit",
        {**_MEMORY_GATE, "dimension": "cpu"},
        {**_MEMORY_GATE, "volume": "workspace"},
        {**_MEMORY_GATE, "dimension": "scratch"},
        {**_MEMORY_GATE, "failed_limit": 0},
        {**_MEMORY_GATE, "failed_limit": True},
        {**_MEMORY_GATE, "failed_limit": "50331648"},
        {**_MEMORY_GATE, "current_limit": -1},
        {**_MEMORY_GATE, "condition": ""},
        {key: value for key, value in _MEMORY_GATE.items() if key != "current_limit"},
        {**_MEMORY_GATE, "source_request_id": str(REQUEST_ID)},
    ],
)
def test_acquire_refuses_a_malformed_resource_gate(gate: object) -> None:
    with pytest.raises(WorkItemTransportError):
        _acquire({**BASE, "resource_gate": gate})


@pytest.mark.parametrize("claim", ["", 7, ["curie-thread-old"]])
def test_acquire_refuses_an_unusable_predecessor_claim(claim: object) -> None:
    with pytest.raises(WorkItemTransportError):
        _acquire({**BASE, "predecessor_claim_name": claim})


def test_start_and_finish_send_delivered_limits_and_exhausted_volume() -> None:
    seen: list[httpx.Request] = []
    limits = DeliveredLimits(
        memory_bytes=67108864,
        ephemeral_storage_bytes=12582912,
        scratch_bytes={"workspace": 8388608},
        max_turns=1000,
    ).to_wire()

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        if request.url.path.endswith("/start"):
            return httpx.Response(
                200,
                json={
                    "runtime_epoch": 1,
                    "execution_deadline": "2099-01-01T00:00:00+00:00",
                    "remaining_s": 60.0,
                    "heartbeat_interval_s": 15.0,
                },
            )
        return httpx.Response(200, json={})

    async def call(client: WorkItemDispatchClient) -> None:
        await client.start(
            REQUEST_ID,
            owner="worker-1",
            generation=1,
            claim_name="claim-1",
            sandbox_name="sandbox-1",
            context=CONTEXT,
            delivered_limits=limits,
        )
        await client.finish(
            REQUEST_ID,
            runtime_epoch=1,
            outcome="failed",
            cause="sandbox_scratch_exhausted",
            detail=None,
            ci_fix_round=None,
            retry_safety="no_effects",
            retry_condition=None,
            exhausted_volume="workspace",
        )

    _run(handler, call)
    start, finish = (json.loads(request.content) for request in seen)
    assert start["delivered_limits"] == {
        "memory_bytes": 67108864,
        "ephemeral_storage_bytes": 12582912,
        "scratch_bytes": {"workspace": 8388608},
        "max_turns": 1000,
    }
    assert finish["exhausted_volume"] == "workspace"


@pytest.mark.parametrize(
    "order",
    ["beat_answers_first", "finish_answers_first", "finish_refused_stale", "no_finish"],
)
def test_a_stale_beat_is_only_a_lost_epoch_when_this_runs_finish_did_not_commit(
    order: str,
) -> None:
    """A beat in flight when this run's own finish commits is not a stale owner (#5840).

    The API refuses a heartbeat for any row that is no longer running, so the
    beat that crosses a committed finish comes back ``stale_owner`` whichever
    answer arrives first. Abandoning on it skips the failed run's recovery
    capture. A finish the API refuses as ``stale_owner`` (a stolen epoch), or no
    finish at all, still abandons.
    """

    async def go() -> None:
        finish_committed = asyncio.Event()
        beat_answered = asyncio.Event()
        finish_returned = asyncio.Event()
        beat_sent = asyncio.Event()
        beats: list[str] = []
        stale: list[str] = []

        async def handler(request: httpx.Request) -> httpx.Response:
            verb = request.url.path.rsplit("/", 1)[-1]
            if verb == "heartbeat":
                beats.append(order)
                beat_sent.set()
                if order == "finish_answers_first":
                    await finish_returned.wait()
                elif order != "no_finish":
                    await finish_committed.wait()
                beat_answered.set()
                # The internal heartbeat route's 409 for a row no longer running.
                return httpx.Response(409, json={"detail": {"code": "stale_owner"}})
            assert verb == "finish"
            if order == "finish_refused_stale":
                finish_committed.set()
                await beat_answered.wait()
                return httpx.Response(409, json={"detail": {"code": "stale_owner"}})
            finish_committed.set()
            if order == "beat_answers_first":
                await beat_answered.wait()
                # Let the heartbeat task act on its answer before this one lands.
                for _ in range(50):
                    await asyncio.sleep(0)
            return httpx.Response(200, json={})

        async def on_stop(_thread: str, _run: WorkItemRun) -> None:
            raise AssertionError("no cancellation was requested")

        async def on_stale(_thread: str, run: WorkItemRun) -> None:
            stale.append(order)
            run.abandoned = True

        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
            run = WorkItemRun(
                client=WorkItemDispatchClient(
                    api_base_url="http://api.example", worker_token="example-token", client=http
                ),
                request_id=REQUEST_ID,
                owner="worker-1",
                grant=_grant(),
                wake=WAKE,
                event_id="event",
                thread_key="slack:C1:thread",
                on_stop=on_stop,
                on_stale=on_stale,
            )
            run.started = True
            run.runtime_epoch = 1
            beat = asyncio.create_task(run._heartbeat_loop(0.01))
            run._heartbeat_task = beat
            try:
                if order != "no_finish":
                    await asyncio.wait_for(beat_sent.wait(), 5)
                    try:
                        await run.finish(
                            outcome="failed",
                            cause="runner_failed",
                            detail=None,
                            ci_fix_round=None,
                            retry_safety="uncertain",
                            retry_condition=None,
                            exhausted_volume=None,
                        )
                    except WorkItemConflict as exc:
                        assert order == "finish_refused_stale"
                        assert exc.code == "stale_owner"
                    finish_returned.set()
                await asyncio.wait_for(asyncio.shield(beat), 5)
            finally:
                await run.close()

        committed = order in {"beat_answers_first", "finish_answers_first"}
        assert beats == [order]
        assert stale == ([] if committed else [order])
        assert run.abandoned is (not committed)
        assert run.finished is committed

    asyncio.run(go())
