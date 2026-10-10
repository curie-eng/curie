"""API outage ownership through the real kernel, clients, and orphan sweeper.

The external API uses httpx MockTransport; Valkey, the substrate, runner transport,
and the kernel are the existing integration harness. Fake time affects only API
retry waits and settlement cadence, preserving the unchanged heartbeat loop.
"""

from __future__ import annotations

import asyncio
import json
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx
import pytest
from aci_protocol import ErrorEvent, Final, SessionStatus, SideEffectFlag
from curie_worker.actions import ActionClient
from curie_worker.workitem_dispatch import WorkItemDispatchClient, WorkItemTransportError
from curie_worker.workitem_orphans import WorkItemOrphanSweeper

from .test_work_item_early_stop import _Binding, _PublicationApi, _turn, _Workspace


class ApiClock:
    def __init__(self) -> None:
        self.now = 0.0
        self.sleeps: list[float] = []
        self.on_sleep: Any = None

    def __call__(self) -> float:
        return self.now

    async def sleep(self, delay: float) -> None:
        self.sleeps.append(delay)
        self.now += delay
        if self.on_sleep is not None:
            await self.on_sleep()
        await asyncio.sleep(0)


class Api:
    """HTTP response shapes follow routers/work_items.py and actions.py."""

    def __init__(self, request_id: uuid.UUID, clock: ApiClock) -> None:
        self.request_id = request_id
        self.clock = clock
        self.owner = ""
        self.running = False
        self.fail_finish = False
        self.finish_status = 200
        self.conflict_code = "stale_owner"
        self.lose_committed_finish_response = False
        self.committed_finish_unseen = False
        self.outage_until = 0.0
        self.begin_outage_on_action = False
        self.outage_started = asyncio.Event()
        self.posts: list[tuple[str, dict[str, Any]]] = []
        self.finished_posts: list[dict[str, Any]] = []
        self.owner_lost_posts: list[dict[str, Any]] = []
        self.listed_owner_rows: list[tuple[float, int]] = []
        self.run: Any = None
        self.kernel: Any = None

    def __call__(self, request: httpx.Request) -> httpx.Response:
        path = request.url.path
        body = json.loads(request.content) if request.content else {}
        if self.begin_outage_on_action and path == "/actions":
            self.begin_outage_on_action = False
            self.outage_until = self.clock.now + 15
            self.outage_started.set()
        if self.clock.now < self.outage_until:
            raise httpx.ConnectError("API restart outage", request=request)
        if request.method == "POST":
            self.posts.append((path, body))
        if path.endswith("/acquire"):
            self.owner = body["owner"]
            return httpx.Response(
                200,
                json={
                    "generation": 1,
                    "work_item_id": str(self.request_id),
                    "conversation_id": f"work-item-{self.request_id}",
                    "wait_deadline": (datetime.now(UTC) + timedelta(hours=1)).isoformat(),
                    "repo_full_name": "acme-corp/widgets",
                },
            )
        if path.endswith("/start"):
            self.running = True
            self.run = self.kernel._work_item_runs[self.request_id]
            return httpx.Response(
                200,
                json={
                    "runtime_epoch": 1,
                    "execution_deadline": (datetime.now(UTC) + timedelta(hours=1)).isoformat(),
                    "remaining_s": 3600,
                    "heartbeat_interval_s": 60,
                },
            )
        if path.endswith("/issue-read/context"):
            return httpx.Response(
                200,
                json={
                    "execution_request_id": str(self.request_id),
                    "repo_full_name": "acme-corp/widgets",
                    "issue_number": 123,
                    "capability": "example-capability",
                },
            )
        if path.endswith("/heartbeat"):
            return httpx.Response(
                200,
                json={"status": "running", "terminal_cause": None, "work_item_cancelled": False},
            )
        if path.endswith("/finish"):
            # API replay after an accepted finish is not_running, per
            # apps/api/src/curie_api/workitem_dispatch.py::_map_finish_conflict.
            if self.committed_finish_unseen:
                return httpx.Response(409, json={"detail": {"code": "not_running"}})
            if self.fail_finish:
                raise httpx.ConnectError("API finish unavailable", request=request)
            if self.finish_status == 409:
                return httpx.Response(409, json={"detail": {"code": self.conflict_code}})
            self.running = False
            self.finished_posts.append(body)
            if self.lose_committed_finish_response:
                self.committed_finish_unseen = True
                raise httpx.ConnectError("Finish committed but response lost", request=request)
            return httpx.Response(200, json={})
        if path.endswith("/runtime-owners"):
            rows = []
            if self.running and "after" not in request.url.params:
                rows = [
                    {
                        "request_id": str(self.request_id),
                        "runtime_owner": self.owner,
                        "runtime_epoch": 1,
                    }
                ]
            if rows:
                self.listed_owner_rows.append((self.clock.now, len(self.finished_posts)))
            return httpx.Response(200, json={"requests": rows})
        if path.endswith("/owner-lost"):
            self.owner_lost_posts.append(body)
            self.running = False
            return httpx.Response(
                200, json={"status": "cancellation_requested", "terminal_cause": "owner_lost"}
            )
        if path == "/actions":
            return httpx.Response(201, json={"id": "action-1", "status": "pending"})
        if path == "/actions/action-1/complete":
            return httpx.Response(
                200,
                json={"id": "action-1", "tool": "deploy", "status": "succeeded", "undoable": False},
            )
        raise AssertionError(f"Unexpected fake API request {request.method} {path}")


def _sweeper(
    client: WorkItemDispatchClient,
    h: Any,
    clock: ApiClock,
    *,
    observed_ownership: list[tuple[float, bool]] | None = None,
) -> WorkItemOrphanSweeper:
    async def alive(_owner: str) -> bool:
        return True

    def locally_owned(request_id: uuid.UUID) -> bool:
        owned = h.kernel.owns_work_item(request_id)
        if observed_ownership is not None:
            observed_ownership.append((clock.now, owned))
        return owned

    return WorkItemOrphanSweeper(
        client,
        alive,
        self_name=h.config.consumer_name,
        locally_owned=locally_owned,
        absence_proof_s=30,
        interval_s=15,
        clock=clock,
    )


def _event(request_id: uuid.UUID):
    return _turn(
        f"work-item-{request_id}-execute-1",
        "Resolve https://github.com/acme-corp/widgets/issues/123",
    )


def _ledger_script():
    return [
        SideEffectFlag(tool="deploy", call_id="call-1", arguments={}),
        SideEffectFlag(tool="deploy", call_id="call-1", failed=False, result={"ok": True}),
        ErrorEvent(classification="server-error", message="Provider turn ended"),
        Final(text="Provider turn ended", status=SessionStatus.CLASSIFIED_FAILURE),
    ]


@pytest.mark.parametrize(
    "exit_reason",
    ["recovery", "conflict", "publication_pending", "heartbeat", "deadline", "shutdown"],
)
def test_failed_finish_keeps_ownership_until_settlement_or_liveness_end(
    make_harness, monkeypatch, exit_reason
) -> None:
    from curie_worker import api_retry
    from curie_worker import workitem_dispatch as dispatch_module
    from curie_worker.kernel import work_items as kernel_work_items

    async def go() -> None:
        clock = ApiClock()
        wall_offset = 0.0

        class WallClock(datetime):
            @classmethod
            def now(cls, tz=None):
                return datetime.now(tz) + timedelta(seconds=wall_offset)

        monkeypatch.setattr(kernel_work_items, "datetime", WallClock)
        monkeypatch.setattr(dispatch_module, "datetime", WallClock)
        request_id = uuid.uuid4()
        api = Api(request_id, clock)
        api.fail_finish = True
        waiting = asyncio.Event()
        retry = asyncio.Event()
        settle_sleeps: list[float] = []

        async def settle_sleep(delay: float) -> None:
            nonlocal wall_offset
            settle_sleeps.append(delay)
            waiting.set()
            await retry.wait()
            clock.now += delay
            if exit_reason == "deadline":
                wall_offset = 7200

        monkeypatch.setattr(api_retry, "_clock", clock)
        monkeypatch.setattr(api_retry, "_sleep", clock.sleep)
        monkeypatch.setattr(kernel_work_items, "_settle_sleep", settle_sleep)
        async with httpx.AsyncClient(transport=httpx.MockTransport(api)) as http:
            client = WorkItemDispatchClient(
                api_base_url="http://api.example", worker_token="example", client=http
            )
            actions = ActionClient(
                api_base_url="http://api.example", api_key="example", client=http
            )
            async with make_harness(
                binding=_Binding(),
                workspace_factory=_Workspace,
                publication_creator=_PublicationApi(),
                actions=actions,
            ) as h:
                api.kernel = h.kernel
                h.kernel._work_items = client
                h.runner.default_script = _ledger_script()
                releases: list[str] = []
                release = h.kernel._release_work_item_sandbox

                async def observed_release(thread: str) -> None:
                    releases.append(thread)
                    await release(thread)

                monkeypatch.setattr(h.kernel, "_release_work_item_sandbox", observed_release)
                event = _event(request_id)
                await h.kernel.process_event(event)
                await asyncio.wait_for(waiting.wait(), timeout=2)
                run = api.run
                assert sum(clock.sleeps) == 120
                assert request_id in h.kernel._settling_work_items
                assert h.kernel.owns_work_item(request_id)
                assert run.heartbeat_running
                assert not run.finished
                assert releases == []
                assert await h.kernel._markers.is_terminal(event.event_id)
                assert await _sweeper(client, h, clock).sweep() == 0
                assert api.owner_lost_posts == []
                assert len(h.runner.opened) == 1
                finish_attempts = sum(path.endswith("/finish") for path, _ in api.posts)
                task = next(
                    task
                    for task in asyncio.all_tasks()
                    if not task.done()
                    and "settle" in task.get_name()
                    and str(request_id) in task.get_name()
                )

                if exit_reason == "shutdown":
                    await h.kernel.close()
                else:
                    api.fail_finish = False
                    if exit_reason in {"conflict", "publication_pending"}:
                        api.finish_status = 409
                        if exit_reason == "publication_pending":
                            api.conflict_code = "publication_pending"
                    elif exit_reason == "heartbeat":
                        await run.close()
                    retry.set()
                    await asyncio.wait_for(task, timeout=2)
                assert request_id not in h.kernel._settling_work_items
                assert not h.kernel.owns_work_item(request_id)
                assert not run.heartbeat_running
                if exit_reason in {"recovery", "publication_pending"}:
                    assert run.finished
                    if exit_reason == "recovery":
                        assert len(api.finished_posts) == 1
                        assert api.finished_posts[0]["cause"] == "model_error"
                    else:
                        assert api.finished_posts == []
                    assert releases == [run.thread_key]
                    assert h.substrate._affinity.get(run.thread_key) is None
                else:
                    assert api.finished_posts == []
                    assert releases == []
                    if exit_reason in {"heartbeat", "deadline", "shutdown"}:
                        assert (
                            sum(path.endswith("/finish") for path, _ in api.posts)
                            == finish_attempts
                        )
                assert settle_sleeps == [15]

    asyncio.run(go())


@pytest.mark.parametrize("old_finalizer", [False, True])
def test_fifteen_second_outage_repro_and_old_finalizer_negative_control(
    make_harness, monkeypatch, old_finalizer
) -> None:
    from curie_worker import actions as actions_module
    from curie_worker import api_retry
    from curie_worker import workitem_dispatch as dispatch_module

    async def go() -> None:
        clock = ApiClock()
        request_id = uuid.uuid4()
        api = Api(request_id, clock)
        api.begin_outage_on_action = True
        monkeypatch.setattr(api_retry, "_clock", clock)
        monkeypatch.setattr(api_retry, "_sleep", clock.sleep)
        if old_finalizer:

            async def single_attempt(client, url, *, budget_s=120, **kwargs):
                return await client.post(url, **kwargs)

            monkeypatch.setattr(actions_module, "post_with_retry", single_attempt)
            monkeypatch.setattr(dispatch_module, "post_with_retry", single_attempt)

        async with httpx.AsyncClient(transport=httpx.MockTransport(api)) as http:
            client = WorkItemDispatchClient(
                api_base_url="http://api.example", worker_token="example", client=http
            )
            actions = ActionClient(
                api_base_url="http://api.example", api_key="example", client=http
            )
            async with make_harness(
                binding=_Binding(),
                workspace_factory=_Workspace,
                publication_creator=_PublicationApi(),
                actions=actions,
            ) as h:
                api.kernel = h.kernel
                h.kernel._work_items = client
                h.runner.default_script = _ledger_script()
                if old_finalizer:

                    def no_settling(*args, **kwargs):
                        raise WorkItemTransportError("old finalizer: failed finish raises")

                    monkeypatch.setattr(h.kernel, "_begin_settling", no_settling)
                observed_ownership: list[tuple[float, bool]] = []
                sweeper = _sweeper(client, h, clock, observed_ownership=observed_ownership)
                sweeps: list[int] = []

                async def sweep_during_outage() -> None:
                    await api.outage_started.wait()
                    sweeps.append(await sweeper.sweep())

                sweep_task = asyncio.create_task(sweep_during_outage())

                async def sweep_between_attempts() -> None:
                    sweeps.append(await sweeper.sweep())

                clock.on_sleep = sweep_between_attempts
                event = _event(request_id)
                if old_finalizer:
                    with pytest.raises(WorkItemTransportError):
                        await h.kernel.process_event(event)
                    assert not h.kernel.owns_work_item(request_id)
                    assert request_id not in h.kernel._settling_work_items
                    assert not api.run.heartbeat_running
                    clock.now = 15
                else:
                    await h.kernel.process_event(event)
                    assert clock.now >= 15
                await sweep_task
                sweeps.append(await sweeper.sweep())
                assert len(h.runner.opened) == 1
                # At recovery, a successful API list still carries the running
                # row before finish clears it. The actual sweeper must consult
                # local ownership on this row, so a zero result cannot be inert.
                assert any(at >= 15 and finishes == 0 for at, finishes in api.listed_owner_rows)
                assert any(
                    at >= 15 and owned is (not old_finalizer) for at, owned in observed_ownership
                )
                if old_finalizer:
                    assert api.finished_posts == []
                    assert len(api.owner_lost_posts) == 1
                    assert sum(sweeps) == 1
                    assert not await h.kernel._markers.is_terminal(event.event_id)
                else:
                    assert api.owner_lost_posts == []
                    assert sum(sweeps) == 0
                    assert len(api.finished_posts) == 1
                    assert api.finished_posts[0]["outcome"] == "failed"
                    assert api.finished_posts[0]["cause"] == "model_error"
                    assert api.finished_posts[0]["detail"] == "Provider turn ended"
                    assert await h.kernel._markers.is_terminal(event.event_id)
                    assert [completion.outcome for completion in h.sink.completions] == [
                        "escalated"
                    ]
                    assert not api.run.heartbeat_running

    asyncio.run(go())


def test_settlement_finishing_before_event_finally_releases_the_sandbox_once(
    make_harness, monkeypatch
) -> None:
    from curie_worker import api_retry
    from curie_worker.kernel import work_items as kernel_work_items

    async def go() -> None:
        clock = ApiClock()
        request_id = uuid.uuid4()
        api = Api(request_id, clock)
        api.fail_finish = True
        released = asyncio.Event()
        releases: list[str] = []

        async def recover_on_settle(delay: float) -> None:
            clock.now += delay
            api.fail_finish = False

        monkeypatch.setattr(api_retry, "_clock", clock)
        monkeypatch.setattr(api_retry, "_sleep", clock.sleep)
        monkeypatch.setattr(kernel_work_items, "_settle_sleep", recover_on_settle)
        async with httpx.AsyncClient(transport=httpx.MockTransport(api)) as http:
            client = WorkItemDispatchClient(
                api_base_url="http://api.example", worker_token="example", client=http
            )
            actions = ActionClient(
                api_base_url="http://api.example", api_key="example", client=http
            )
            async with make_harness(
                binding=_Binding(),
                workspace_factory=_Workspace,
                publication_creator=_PublicationApi(),
                actions=actions,
            ) as h:
                api.kernel = h.kernel
                h.kernel._work_items = client
                h.runner.default_script = _ledger_script()
                release = h.kernel._release_work_item_sandbox
                mark_done = h.kernel._markers.mark_done

                async def observed_release(thread: str) -> None:
                    releases.append(thread)
                    await release(thread)
                    released.set()

                async def mark_after_settling(*args, **kwargs) -> None:
                    # Schedule only: the actual Valkey marker write is unchanged.
                    # Force finish and release before process_event reaches finally.
                    await asyncio.wait_for(released.wait(), timeout=2)
                    await mark_done(*args, **kwargs)

                monkeypatch.setattr(h.kernel, "_release_work_item_sandbox", observed_release)
                monkeypatch.setattr(h.kernel._markers, "mark_done", mark_after_settling)
                event = _event(request_id)
                await h.kernel.process_event(event)
                assert api.run.finished
                assert releases == [api.run.thread_key]
                assert h.substrate._affinity.get(api.run.thread_key) is None
                assert not h.kernel.owns_work_item(request_id)
                assert request_id not in h.kernel._work_item_runs
                assert request_id not in h.kernel._settling_work_items
                assert not api.run.heartbeat_running
                assert len(api.finished_posts) == 1
                assert await h.kernel._markers.is_terminal(event.event_id)

    asyncio.run(go())


@pytest.mark.parametrize("deferred", [False, True])
def test_finish_committed_before_response_loss_settles_without_rerunning(
    make_harness, monkeypatch, deferred
) -> None:
    from curie_worker import api_retry
    from curie_worker.kernel import work_items as kernel_work_items

    async def go() -> None:
        clock = ApiClock()
        request_id = uuid.uuid4()
        api = Api(request_id, clock)
        api.fail_finish = deferred
        api.lose_committed_finish_response = True
        waiting = asyncio.Event()
        resume = asyncio.Event()
        releases: list[str] = []

        async def settle_sleep(delay: float) -> None:
            waiting.set()
            await resume.wait()
            clock.now += delay

        monkeypatch.setattr(api_retry, "_clock", clock)
        monkeypatch.setattr(api_retry, "_sleep", clock.sleep)
        monkeypatch.setattr(kernel_work_items, "_settle_sleep", settle_sleep)
        async with httpx.AsyncClient(transport=httpx.MockTransport(api)) as http:
            client = WorkItemDispatchClient(
                api_base_url="http://api.example", worker_token="example", client=http
            )
            actions = ActionClient(
                api_base_url="http://api.example", api_key="example", client=http
            )
            async with make_harness(
                binding=_Binding(),
                workspace_factory=_Workspace,
                publication_creator=_PublicationApi(),
                actions=actions,
            ) as h:
                api.kernel = h.kernel
                h.kernel._work_items = client
                h.runner.default_script = _ledger_script()
                release = h.kernel._release_work_item_sandbox

                async def observed_release(thread: str) -> None:
                    releases.append(thread)
                    await release(thread)

                monkeypatch.setattr(h.kernel, "_release_work_item_sandbox", observed_release)
                event = _event(request_id)
                await h.kernel.process_event(event)
                if deferred:
                    await asyncio.wait_for(waiting.wait(), timeout=2)
                    assert h.kernel.owns_work_item(request_id)
                    assert api.run.heartbeat_running
                    assert releases == []
                    task = next(
                        task
                        for task in asyncio.all_tasks()
                        if not task.done()
                        and "settle" in task.get_name()
                        and str(request_id) in task.get_name()
                    )
                    failed_attempts = sum(path.endswith("/finish") for path, _ in api.posts)
                    api.fail_finish = False
                    resume.set()
                    await asyncio.wait_for(task, timeout=2)
                assert api.committed_finish_unseen
                assert len(api.finished_posts) == 1
                assert api.finished_posts[0] == {
                    "runtime_epoch": 1,
                    "outcome": "failed",
                    "cause": "model_error",
                    "detail": "Provider turn ended",
                }
                finish_attempts = [body for path, body in api.posts if path.endswith("/finish")]
                assert len(finish_attempts) == (failed_attempts + 2 if deferred else 2)
                assert all(body == api.finished_posts[0] for body in finish_attempts)
                assert api.run.finished
                assert releases == [api.run.thread_key]
                assert h.substrate._affinity.get(api.run.thread_key) is None
                assert not api.run.heartbeat_running
                assert not h.kernel.owns_work_item(request_id)
                assert request_id not in h.kernel._settling_work_items
                assert await h.kernel._markers.is_terminal(event.event_id)
                assert len(h.runner.opened) == 1
                assert [completion.outcome for completion in h.sink.completions] == ["escalated"]
                assert await _sweeper(client, h, clock).sweep() == 0
                assert api.owner_lost_posts == []

    asyncio.run(go())
