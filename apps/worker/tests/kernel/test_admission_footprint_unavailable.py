"""Transient footprint observation uses durable admission waiting (#5388)."""

from __future__ import annotations

import asyncio
import uuid
from types import SimpleNamespace
from typing import Any

import pytest
from aci_protocol import Final, SessionStatus
from curie_worker.sandbox import CapacityExhaustedError
from kubernetes.client import ApiException
from urllib3.exceptions import MaxRetryError, ReadTimeoutError

from .test_consumer import (
    _capacity_consumer,
    _inventory_timeout_client,
    _pending_local_entry,
    _qevent,
)
from .test_work_item_workspace import _Binding, _DeferRecordingWorkItems, _turn, _Workspace


def _footprint_fault_client(sync_redis, names, monkeypatch, plural: str, failure: str):
    client, api, admission = _inventory_timeout_client(
        sync_redis,
        names,
        monkeypatch,
        failure="healthy",
        restore=True,
        failures=0,
    )
    # https://urllib3.readthedocs.io/en/stable/reference/urllib3.exceptions.html
    timeout = ReadTimeoutError(None, "/apis/example", "read timed out")
    fault = {
        "timeout": timeout,
        "sdk-timeout": MaxRetryError(None, timeout.url, timeout),
        "http-503": ApiException(status=503),
    }[failure]
    state = SimpleNamespace(fault=fault, reads=[])
    get = api.get_namespaced_custom_object

    def observe(*args: Any, **kwargs: Any) -> Any:
        if args[3] == plural and kwargs.get("_request_timeout") == 1.0:
            state.reads.append(args[4])
            if state.fault is not None:
                raise state.fault
        return get(*args, **kwargs)

    monkeypatch.setattr(api, "get_namespaced_custom_object", observe)
    return client, api, admission, state


@pytest.mark.parametrize("plural", ["sandboxwarmpools", "sandboxtemplates"])
@pytest.mark.parametrize("failure", ["timeout", "sdk-timeout", "http-503"])
def test_footprint_outage_waits_and_recovery_starts_once(
    make_harness,
    sync_redis,
    names,
    monkeypatch,
    plural: str,
    failure: str,
) -> None:
    client, api, admission, fault = _footprint_fault_client(
        sync_redis,
        names,
        monkeypatch,
        plural,
        failure,
    )

    async def go() -> None:
        async with make_harness(
            sandbox_client=client,
            substrate_env={"CURIE_SANDBOX_ADMISSION": admission},
            slack_no_edit_streaming=True,
            max_delivery=2,
        ) as h:
            consumer = _capacity_consumer(h)
            await consumer.ensure_group()
            event = _qevent("run after recovery", event_id="footprint-original")
            entry, fields = await _pending_local_entry(h, event)
            await consumer._sem.acquire()
            await consumer._handle(entry, fields)
            waiting = await consumer._waits.get(event.event_id)
            assert waiting is not None and waiting.state == "waiting"
            assert len(fault.reads) == 1
            assert api.created == [] and api.secrets == {}
            assert h.runner.opened == h.sink.completions == []
            assert not await h.kernel._markers.is_terminal(event.event_id)
            assert (await h.async_redis.xpending(h.config.stream, h.config.consumer_group))[
                "pending"
            ] == 0

            fault.fault = None
            h.runner.default_script = [Final(text="footprint recovered", status=SessionStatus.DONE)]
            await h.async_redis.zadd(consumer._waits._due, {event.event_id: 0})
            assert await consumer._waits.wake_due() == 1
            rows = await h.async_redis.xreadgroup(
                h.config.consumer_group,
                h.config.consumer_name,
                {h.config.stream: ">"},
                count=1,
            )
            entry, fields = rows[0][1][0]
            await consumer._sem.acquire()
            await consumer._handle(entry, fields)
            finished = await consumer._waits.get(event.event_id)
            assert finished is not None and finished.state == "done"
            assert finished.deadline_ms == waiting.deadline_ms
            assert h.runner.opened == h.runner.queried == ["run after recovery"]
            assert h.sink.last_text == "footprint recovered"
            assert len(h.sink.completions) == 1
            assert not await h.async_redis.exists(f"{h.config.stream}:dead")

    asyncio.run(go())


@pytest.mark.parametrize("plural", ["sandboxwarmpools", "sandboxtemplates"])
@pytest.mark.parametrize("failure", ["timeout", "sdk-timeout", "http-503"])
def test_factory_footprint_outage_defers_without_start_accounting(
    make_harness,
    sync_redis,
    names,
    monkeypatch,
    plural: str,
    failure: str,
) -> None:
    client, api, admission, fault = _footprint_fault_client(
        sync_redis,
        names,
        monkeypatch,
        plural,
        failure,
    )

    async def go() -> None:
        work_items = _DeferRecordingWorkItems()
        async with make_harness(
            sandbox_client=client,
            substrate_env={"CURIE_SANDBOX_ADMISSION": admission},
            binding=_Binding(),
            workspace_factory=_Workspace,
            work_items=work_items,
            slack_no_edit_streaming=True,
        ) as h:
            consumer = _capacity_consumer(h)
            await consumer.ensure_group()
            request_id = uuid.uuid4()
            event = _turn(f"work-item-{request_id}-execute-1", "Implement the admitted change")
            entry, fields = await _pending_local_entry(h, event)
            await consumer._sem.acquire()
            await consumer._handle(entry, fields)
            assert len(fault.reads) == 1
            assert len(work_items.defers) == 1
            deferred_id, deferred = work_items.defers[0]
            assert deferred_id == request_id
            assert deferred["reason"] == "admission_unavailable"
            assert deferred["capacity"] is False
            assert deferred["start_failure_detail"] is None
            assert "start" not in work_items.calls
            assert work_items.finishes == h.runner.opened == []
            assert api.created == [] and api.secrets == {}
            assert h.sink.updates == []
            assert (await h.async_redis.xpending(h.config.stream, h.config.consumer_group))[
                "pending"
            ] == 0

    asyncio.run(go())


@pytest.mark.parametrize(
    "failure",
    [
        "missing-pool",
        "malformed-pool",
        "missing-template",
        "malformed-template",
        "forbidden",
        "quota",
    ],
)
def test_footprint_structural_errors_and_capacity_keep_their_classification(
    make_harness,
    sync_redis,
    names,
    monkeypatch,
    failure: str,
) -> None:
    client, api, admission = _inventory_timeout_client(
        sync_redis,
        names,
        monkeypatch,
        failure="healthy",
        restore=True,
        failures=0,
    )
    pool = ("sandboxwarmpools", "test-pool")
    template = ("sandboxtemplates", "curie-agent-acme-a-runner")
    expected = ValueError
    if failure == "missing-pool":
        del api.objects[pool]
    elif failure == "malformed-pool":
        api.objects[pool]["spec"]["sandboxTemplateRef"] = {}
    elif failure == "missing-template":
        del api.objects[template]
    elif failure == "malformed-template":
        api.objects[template]["spec"] = {}
        expected = KeyError
    elif failure == "forbidden":
        api.failures[("get", "sandboxtemplates")] = ApiException(status=403)
        expected = ApiException
    else:
        api.quota.status.used = dict(api.quota.status.hard)
        expected = CapacityExhaustedError

    async def go() -> None:
        async with make_harness(
            sandbox_client=client,
            substrate_env={"CURIE_SANDBOX_ADMISSION": admission},
        ) as h:
            await _capacity_consumer(h).ensure_group()
            with pytest.raises(expected) as raised:
                await asyncio.to_thread(h.substrate.claim, "structural-control")
            if failure == "forbidden":
                assert raised.value.status == 403
            assert api.created == [] and api.secrets == {}
            assert h.runner.opened == []

    asyncio.run(go())
