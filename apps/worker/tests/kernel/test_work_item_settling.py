"""API outage ownership through the real kernel, clients, and orphan sweeper.

The external API uses httpx MockTransport; Valkey, the substrate, runner transport,
and the kernel are the existing integration harness. Fake time affects only API
retry waits and settlement cadence, preserving the unchanged heartbeat loop.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx
import pytest
from aci_protocol import ErrorEvent, Final, SessionStatus, SideEffectFlag
from curie_worker.actions import ActionClient
from curie_worker.approvals import (
    ApprovalBackendError,
    ApprovalRequest,
    CreatedApproval,
    CreatedPublication,
)
from curie_worker.sandbox import RouteState, SandboxHandle
from curie_worker.workitem_dispatch import WorkItemDispatchClient, WorkItemTransportError
from curie_worker.workitem_orphans import WorkItemOrphanSweeper

from .test_work_item_early_stop import (
    PUBLISH_TOOL,
    _Binding,
    _patch_snapshot,
    _PublicationApi,
    _publish_final,
    _tool,
    _turn,
    _Workspace,
)


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
        # #4331: the heartbeat cadence the start grant hands out, and a switch
        # that makes every heartbeat a transport failure (a store outage).
        self.heartbeat_interval_s = 60.0
        self.fail_heartbeat = False
        self.heartbeat_failures = 0
        self.holds: list[dict[str, Any]] = []
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
                    "heartbeat_interval_s": self.heartbeat_interval_s,
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
            if self.fail_heartbeat:
                self.heartbeat_failures += 1
                raise httpx.ConnectError("Postgres outage behind the API", request=request)
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
        if path.endswith("/hold-approval"):
            self.holds.append(body)
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


def test_settling_empty_transport_error_is_named_in_the_log(
    make_harness, monkeypatch, caplog
) -> None:
    from curie_worker import api_retry
    from curie_worker import kernel as kernel_module

    async def go() -> None:
        clock = ApiClock()
        request_id = uuid.uuid4()
        api = Api(request_id, clock)
        api.fail_finish = True
        empty_failure_pending = True
        waiting = asyncio.Event()
        resume = asyncio.Event()

        def handler(request: httpx.Request) -> httpx.Response:
            nonlocal empty_failure_pending
            if request.url.path.endswith("/finish") and not api.fail_finish:
                if empty_failure_pending:
                    empty_failure_pending = False
                    # The API-facing seam can raise an empty wrapper, independently
                    # of the underlying httpx errors exercised by client tests.
                    raise WorkItemTransportError("")
            return api(request)

        async def settle_sleep(delay: float) -> None:
            waiting.set()
            await resume.wait()
            clock.now += delay
            api.fail_finish = False

        monkeypatch.setattr(api_retry, "_clock", clock)
        monkeypatch.setattr(api_retry, "_sleep", clock.sleep)
        monkeypatch.setattr(kernel_module, "_settle_sleep", settle_sleep)
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
            client = WorkItemDispatchClient(
                api_base_url="http://api.example", worker_token="example", client=http
            )
            async with make_harness(
                binding=_Binding(),
                workspace_factory=_Workspace,
                publication_creator=_PublicationApi(),
            ) as h:
                api.kernel = h.kernel
                h.kernel._work_items = client
                h.runner.default_script = _ledger_script()
                with caplog.at_level(logging.WARNING, logger="curie_worker.kernel"):
                    await h.kernel.process_event(_event(request_id))
                    await asyncio.wait_for(waiting.wait(), timeout=2)
                    task = next(
                        task
                        for task in asyncio.all_tasks()
                        if not task.done()
                        and "settle" in task.get_name()
                        and str(request_id) in task.get_name()
                    )
                    resume.set()
                    await asyncio.wait_for(task, timeout=2)

                records = [
                    record
                    for record in caplog.records
                    if record.getMessage().startswith("work-item finish still unavailable for ")
                ]
                assert len(records) == 1
                assert records[0].levelno == logging.WARNING
                assert records[0].getMessage() == (
                    f"work-item finish still unavailable for {request_id}: WorkItemTransportError: "
                )
                assert api.run.finished
                assert not h.kernel.owns_work_item(request_id)
                assert len(h.runner.opened) == 1
                assert len(api.finished_posts) == 1

    asyncio.run(go())


@pytest.mark.parametrize(
    "exit_reason",
    ["recovery", "conflict", "publication_pending", "heartbeat", "deadline", "shutdown"],
)
def test_failed_finish_keeps_ownership_until_settlement_or_liveness_end(
    make_harness, monkeypatch, exit_reason
) -> None:
    from curie_worker import api_retry
    from curie_worker import kernel as kernel_module
    from curie_worker import workitem_dispatch as dispatch_module

    async def go() -> None:
        clock = ApiClock()
        wall_offset = 0.0

        class WallClock(datetime):
            @classmethod
            def now(cls, tz=None):
                return datetime.now(tz) + timedelta(seconds=wall_offset)

        monkeypatch.setattr(kernel_module, "datetime", WallClock)
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
        monkeypatch.setattr(kernel_module, "_settle_sleep", settle_sleep)
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

                async def observed_release(run: Any) -> None:
                    releases.append(run.thread_key)
                    await release(run)

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
    from curie_worker import kernel as kernel_module

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
        monkeypatch.setattr(kernel_module, "_settle_sleep", recover_on_settle)
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

                async def observed_release(run: Any) -> None:
                    releases.append(run.thread_key)
                    await release(run)
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
    from curie_worker import kernel as kernel_module

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
        monkeypatch.setattr(kernel_module, "_settle_sleep", settle_sleep)
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

                async def observed_release(run: Any) -> None:
                    releases.append(run.thread_key)
                    await release(run)

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


# --- #4331: a run's cleanup only ever deletes the claim it recorded -------------
#
# ADR 0206 decision 1 gives teardown of a lost request to the API termination
# chain. A successor admitted on the same thread claims a fresh sandbox under the
# same route key, so a late cleanup from the lost owner must not read "whatever
# the route names now" and delete it.


class _RecordingWorkspace(_Workspace):
    """The workspace seam, recording each thread whose workspace was released."""

    def __init__(self, substrate: object) -> None:
        super().__init__(substrate)
        self.releases: list[str] = []

    def release(self, thread_key: str) -> None:
        self.releases.append(thread_key)


class _RecordingApprovals:
    """Approvals backend double: records every create."""

    def __init__(self) -> None:
        self.creates: list[ApprovalRequest] = []

    async def create(self, request: ApprovalRequest, *, budget_s: float = 120) -> CreatedApproval:
        self.creates.append(request)
        return CreatedApproval(id="appr-1", status="pending")


def _provider_failure() -> list[Any]:
    # A flag-clean classified failure: escalates without a retry or a continuation.
    return [
        ErrorEvent(classification="server-error", message="Provider turn ended"),
        Final(text="Provider turn ended", status=SessionStatus.CLASSIFIED_FAILURE),
    ]


def _permission_gate() -> list[Any]:
    return [
        _tool("Bash"),
        Final(
            text="Requesting approval.",
            status=SessionStatus.AWAITING_APPROVAL,
            approval_summary="Run the requested command",
            approval_gate_kind="permission",
            approval_granted_tool="Bash",
        ),
    ]


def _publication_gate() -> list[Any]:
    return [_tool(PUBLISH_TOOL), _publish_final()]


async def _until(predicate: Any, *, timeout: float = 5.0) -> None:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while not predicate():
        if loop.time() > deadline:
            raise AssertionError("condition never held")
        await asyncio.sleep(0.01)


async def _successor_claims_the_thread(h: Any, thread_key: str, own_claim: str) -> SandboxHandle:
    """What ADR 0206 does next: the lost route goes, a successor claims fresh.

    The termination chain drops the lost owner's route; the successor request's
    claim on the same thread then creates a new SandboxClaim and routes the
    thread to it. The lost owner's own claim object is left to that chain.
    """

    assert h.substrate._affinity.delete_if_claim(thread_key, own_claim)
    successor = await asyncio.to_thread(h.substrate.claim, thread_key, env={})
    assert successor.claim_name != own_claim
    assert successor.claim_name in h.fake_k8s.claims
    return successor


@contextlib.asynccontextmanager
async def _held_execution(make_harness: Any, monkeypatch: Any, **harness_kwargs: Any):
    """A started execute turn the runner holds open, heartbeating every 10 ms."""

    from curie_worker import api_retry
    from curie_worker import workitem_dispatch as dispatch_module

    clock = ApiClock()
    request_id = uuid.uuid4()
    api = Api(request_id, clock)
    api.heartbeat_interval_s = 0.01
    monkeypatch.setattr(dispatch_module, "_MIN_HEARTBEAT_INTERVAL_S", 0.01)
    monkeypatch.setattr(api_retry, "_clock", clock)
    monkeypatch.setattr(api_retry, "_sleep", clock.sleep)
    harness_kwargs.setdefault("publication_creator", _PublicationApi())
    async with httpx.AsyncClient(transport=httpx.MockTransport(api)) as http:
        client = WorkItemDispatchClient(
            api_base_url="http://api.example", worker_token="example", client=http
        )
        async with make_harness(
            binding=_Binding(), workspace_factory=_RecordingWorkspace, **harness_kwargs
        ) as h:
            api.kernel = h.kernel
            h.kernel._work_items = client
            h.runner.hold = asyncio.Event()
            h.runner.default_script = []
            yield h, api, _event(request_id)


async def _abandon_by_heartbeat_outage(api: Api) -> None:
    """Three heartbeat transport failures run the real stale callback."""

    await _until(lambda: api.run is not None and api.run.heartbeat_running)
    api.fail_heartbeat = True
    await _until(lambda: api.run.finished)
    assert api.heartbeat_failures == 3


def test_stale_owner_cleanup_never_deletes_the_successor_claim(make_harness, monkeypatch) -> None:
    """The chaos-run repro at the event handler's finally (#4331).

    A started run is abandoned by a heartbeat outage while its turn is still
    open, a successor claims a fresh sandbox on the same thread, and only then
    does the lost owner's handler reach its cleanup. The successor's claim, its
    route, and its workspace must survive.
    """

    async def go() -> None:
        async with _held_execution(make_harness, monkeypatch) as (h, api, event):
            h.runner.tail = _provider_failure()
            turn = asyncio.create_task(h.kernel.process_event(event))
            await _abandon_by_heartbeat_outage(api)
            run = api.run
            own_claim = run.claim_name
            assert own_claim is not None
            successor = await _successor_claims_the_thread(h, run.thread_key, own_claim)

            h.runner.hold.set()
            await asyncio.wait_for(turn, timeout=10)

            assert successor.claim_name in h.fake_k8s.claims
            assert successor.claim_name not in h.fake_k8s.deleted_claims
            record = h.substrate._affinity.get(run.thread_key)
            assert record is not None
            assert record.handle.claim_name == successor.claim_name
            assert record.state is RouteState.LIVE
            assert h.kernel._workspace.releases == []
            assert api.finished_posts == []
            assert not h.kernel.owns_work_item(api.request_id)

    asyncio.run(go())


def test_stale_owner_cleanup_leaves_its_own_claim_to_the_termination_chain(
    make_harness, monkeypatch
) -> None:
    """Abandoned with the route still naming its own claim: deletes nothing."""

    async def go() -> None:
        async with _held_execution(make_harness, monkeypatch) as (h, api, event):
            h.runner.tail = _provider_failure()
            turn = asyncio.create_task(h.kernel.process_event(event))
            await _abandon_by_heartbeat_outage(api)
            run = api.run
            own_claim = run.claim_name
            assert own_claim is not None

            h.runner.hold.set()
            await asyncio.wait_for(turn, timeout=10)

            assert own_claim in h.fake_k8s.claims
            assert h.fake_k8s.deleted_claims == []
            record = h.substrate._affinity.get(run.thread_key)
            assert record is not None
            assert record.handle.claim_name == own_claim
            assert h.kernel._workspace.releases == []
            assert api.finished_posts == []

    asyncio.run(go())


def test_settlement_cleanup_never_deletes_the_successor_claim(make_harness, monkeypatch) -> None:
    """The fence at the settlement cleanup: a run that finished through settlement
    after a successor took the thread releases nothing of the successor's."""

    from curie_worker import api_retry
    from curie_worker import kernel as kernel_module

    async def go() -> None:
        clock = ApiClock()
        request_id = uuid.uuid4()
        api = Api(request_id, clock)
        api.fail_finish = True
        waiting = asyncio.Event()
        resume = asyncio.Event()

        async def settle_sleep(delay: float) -> None:
            waiting.set()
            await resume.wait()
            clock.now += delay

        monkeypatch.setattr(api_retry, "_clock", clock)
        monkeypatch.setattr(api_retry, "_sleep", clock.sleep)
        monkeypatch.setattr(kernel_module, "_settle_sleep", settle_sleep)
        async with httpx.AsyncClient(transport=httpx.MockTransport(api)) as http:
            client = WorkItemDispatchClient(
                api_base_url="http://api.example", worker_token="example", client=http
            )
            actions = ActionClient(
                api_base_url="http://api.example", api_key="example", client=http
            )
            async with make_harness(
                binding=_Binding(),
                workspace_factory=_RecordingWorkspace,
                publication_creator=_PublicationApi(),
                actions=actions,
            ) as h:
                api.kernel = h.kernel
                h.kernel._work_items = client
                h.runner.default_script = _ledger_script()
                await h.kernel.process_event(_event(request_id))
                await asyncio.wait_for(waiting.wait(), timeout=2)
                run = api.run
                assert request_id in h.kernel._settling_work_items
                assert not run.finished
                own_claim = run.claim_name
                assert own_claim is not None
                task = next(
                    task
                    for task in asyncio.all_tasks()
                    if not task.done()
                    and "settle" in task.get_name()
                    and str(request_id) in task.get_name()
                )

                successor = await _successor_claims_the_thread(h, run.thread_key, own_claim)
                api.fail_finish = False
                resume.set()
                await asyncio.wait_for(task, timeout=2)

                assert run.finished
                assert len(api.finished_posts) == 1
                assert request_id not in h.kernel._settling_work_items
                assert successor.claim_name in h.fake_k8s.claims
                assert successor.claim_name not in h.fake_k8s.deleted_claims
                record = h.substrate._affinity.get(run.thread_key)
                assert record is not None
                assert record.handle.claim_name == successor.claim_name
                assert h.kernel._workspace.releases == []

    asyncio.run(go())


@pytest.mark.parametrize("site", ["event_finally", "settlement"])
def test_a_normal_finish_releases_exactly_its_own_claim_and_route(
    make_harness, monkeypatch, site
) -> None:
    """Negative control for the fence: a settled run on its own claim still
    deletes that claim, its route, and its workspace, at both release sites."""

    from curie_worker import api_retry
    from curie_worker import kernel as kernel_module

    async def go() -> None:
        clock = ApiClock()
        request_id = uuid.uuid4()
        api = Api(request_id, clock)
        api.fail_finish = site == "settlement"

        async def recover_on_settle(delay: float) -> None:
            clock.now += delay
            api.fail_finish = False

        monkeypatch.setattr(api_retry, "_clock", clock)
        monkeypatch.setattr(api_retry, "_sleep", clock.sleep)
        monkeypatch.setattr(kernel_module, "_settle_sleep", recover_on_settle)
        async with httpx.AsyncClient(transport=httpx.MockTransport(api)) as http:
            client = WorkItemDispatchClient(
                api_base_url="http://api.example", worker_token="example", client=http
            )
            actions = ActionClient(
                api_base_url="http://api.example", api_key="example", client=http
            )
            async with make_harness(
                binding=_Binding(),
                workspace_factory=_RecordingWorkspace,
                publication_creator=_PublicationApi(),
                actions=actions,
            ) as h:
                api.kernel = h.kernel
                h.kernel._work_items = client
                h.runner.default_script = _ledger_script()
                await h.kernel.process_event(_event(request_id))
                settling = [
                    task
                    for task in asyncio.all_tasks()
                    if not task.done()
                    and "settle" in task.get_name()
                    and str(request_id) in task.get_name()
                ]
                await asyncio.wait_for(asyncio.gather(*settling), timeout=2)
                run = api.run

                assert run.finished
                assert len(api.finished_posts) == 1
                assert h.fake_k8s.claims == {}
                assert run.claim_name in h.fake_k8s.deleted_claims
                assert h.substrate._affinity.get(run.thread_key) is None
                assert h.kernel._workspace.releases == [run.thread_key]

    asyncio.run(go())


async def _approval_turn(
    make_harness: Any, monkeypatch: Any, caplog: Any, *, gate: str, abandon: bool
) -> dict[str, Any]:
    """Drive one execute turn that ends AWAITING_APPROVAL; report what it did."""

    approvals = _RecordingApprovals()
    publications = _PublicationApi()
    async with _held_execution(
        make_harness,
        monkeypatch,
        approvals=approvals,
        publication_creator=publications,
    ) as (h, api, event):
        if gate == "publication":
            _patch_snapshot(h, monkeypatch)
        h.runner.tail = _publication_gate() if gate == "publication" else _permission_gate()
        with caplog.at_level(logging.WARNING, logger="curie_worker.kernel"):
            turn = asyncio.create_task(h.kernel.process_event(event))
            if abandon:
                await _abandon_by_heartbeat_outage(api)
            else:
                await _until(lambda: api.run is not None and api.run.heartbeat_running)
            run = api.run
            h.runner.hold.set()
            await asyncio.wait_for(turn, timeout=10)
        return {
            "approval_creates": len(approvals.creates),
            "publication_creates": len(publications.creates),
            "finish_posts": [path for path, _ in api.posts if path.endswith("/finish")],
            "holds": len(api.holds),
            "escalations": [
                record.getMessage()
                for record in caplog.records
                if record.getMessage().startswith("escalating event ")
            ],
            "completions": [completion.outcome for completion in h.sink.completions],
            "route": h.substrate._affinity.get(run.thread_key),
            "own_claim": run.claim_name,
            "claims": dict(h.fake_k8s.claims),
            "workspace_releases": list(h.kernel._workspace.releases),
        }


@pytest.mark.parametrize("gate", ["permission", "publication"])
def test_an_abandoned_run_creates_no_approval_and_drops_the_delivery(
    make_harness, monkeypatch, caplog, gate
) -> None:
    """The observed case: the abandonment lands while the turn is still open, so
    the gate reaches the pause on a run this worker no longer drives."""

    seen = asyncio.run(_approval_turn(make_harness, monkeypatch, caplog, gate=gate, abandon=True))

    assert seen["approval_creates"] == 0
    assert seen["publication_creates"] == 0
    assert seen["finish_posts"] == []
    assert seen["holds"] == 0
    assert seen["escalations"] == []
    assert seen["completions"] == ["dropped"]
    # No suspend and no release: the route still names the lost owner's own
    # claim, live, for the termination chain to tear down.
    route = seen["route"]
    assert route is not None
    assert route.handle.claim_name == seen["own_claim"]
    assert route.state is RouteState.LIVE
    assert seen["own_claim"] in seen["claims"]
    assert seen["workspace_releases"] == []


@pytest.mark.parametrize("gate", ["permission", "publication"])
def test_a_live_run_still_creates_its_approval(make_harness, monkeypatch, caplog, gate) -> None:
    """Control: the same turn on a run that was not abandoned pauses as before."""

    seen = asyncio.run(_approval_turn(make_harness, monkeypatch, caplog, gate=gate, abandon=False))

    if gate == "publication":
        assert seen["publication_creates"] == 1
        assert seen["approval_creates"] == 0
    else:
        assert seen["approval_creates"] == 1
        assert seen["publication_creates"] == 0
    assert seen["finish_posts"] == []
    assert seen["holds"] == 1
    assert seen["escalations"] == []
    assert seen["completions"] == ["awaiting-approval"]


def test_a_run_resumed_onto_a_fresh_claim_releases_that_claim_when_it_settles(
    make_harness, monkeypatch
) -> None:
    """Liveness control for #3075 under the fence: a started run that parks for
    approval is resumed onto a fresh claim (resume retires the suspended claim
    and cold-claims a new one), so the claim its finishing turn ran on is not
    the one it started on. Settling must still release that latest claim."""

    from curie_worker import api_retry

    async def go() -> None:
        clock = ApiClock()
        request_id = uuid.uuid4()
        api = Api(request_id, clock)
        monkeypatch.setattr(api_retry, "_clock", clock)
        monkeypatch.setattr(api_retry, "_sleep", clock.sleep)
        approvals = _RecordingApprovals()
        async with httpx.AsyncClient(transport=httpx.MockTransport(api)) as http:
            client = WorkItemDispatchClient(
                api_base_url="http://api.example", worker_token="example", client=http
            )
            async with make_harness(
                binding=_Binding(),
                workspace_factory=_RecordingWorkspace,
                approvals=approvals,
                publication_creator=_PublicationApi(),
            ) as h:
                api.kernel = h.kernel
                h.kernel._work_items = client
                h.runner.turn_scripts = [
                    _permission_gate(),
                    [Final(text="Resumed. Done.", status=SessionStatus.DONE)],
                ]

                await h.kernel.process_event(_event(request_id))
                run = api.run
                started_claim = run.claim_name
                assert started_claim is not None
                assert len(approvals.creates) == 1
                assert len(api.holds) == 1
                parked = h.substrate._affinity.get(run.thread_key)
                assert parked is not None
                assert parked.state is RouteState.SUSPENDED

                await h.kernel.process_event(
                    _turn(
                        f"approval-{uuid.uuid4()}-resolved",
                        "[approval resolved] approved",
                        placeholder="approval-placeholder",
                    )
                )

                assert len(h.runner.opened) == 2
                assert len(api.finished_posts) == 1
                assert run.finished
                # The resume really ran on a different claim than the start.
                assert h.fake_k8s.claims == {}
                assert started_claim in h.fake_k8s.deleted_claims
                assert len(h.fake_k8s.deleted_claims) == 2
                assert h.substrate._affinity.get(run.thread_key) is None
                assert h.kernel._workspace.releases == [run.thread_key]

    asyncio.run(go())


class _AbandoningApprovals(_RecordingApprovals):
    """Approvals backend whose create lets the run be abandoned in flight."""

    def __init__(self, during: Any, *, fail: bool) -> None:
        super().__init__()
        self.during = during
        self.fail = fail

    async def create(self, request: ApprovalRequest, *, budget_s: float = 120) -> CreatedApproval:
        self.creates.append(request)
        await self.during()
        if self.fail:
            raise ApprovalBackendError("approval API unreachable")
        return CreatedApproval(id="appr-1", status="pending")


class _AbandoningPublications(_PublicationApi):
    """Publication creator whose create lets the run be abandoned in flight."""

    def __init__(self, during: Any, *, fail: bool) -> None:
        super().__init__()
        self.during = during
        self.fail = fail

    async def create_publication(self, request: object, *, budget_s: float = 120) -> object:
        self.creates.append(request)
        await self.during()
        if self.fail:
            raise ApprovalBackendError("publication API unreachable")
        return CreatedPublication(id="publication-1", approval_id="approval-1", status="pending")


@pytest.mark.parametrize("gate", ["permission", "publication"])
@pytest.mark.parametrize("create_fails", [True, False], ids=["create_raises", "create_succeeds"])
def test_abandonment_during_the_approval_create_drops_without_escalating_or_suspending(
    make_harness, monkeypatch, caplog, gate, create_fails
) -> None:
    """The observed race: the run is live when the pause reaches the create, and
    the heartbeat outage abandons it while the create is in flight. Whether the
    create then fails or succeeds, the stale owner escalates nothing, writes no
    finish, does not suspend the thread (whose route now names a successor's
    claim), and drops the delivery."""

    async def go() -> dict[str, Any]:
        state: dict[str, Any] = {}

        async def abandon_and_hand_over() -> None:
            api: Api = state["api"]
            h: Any = state["h"]
            run = api.run
            assert not run.abandoned and not run.finished
            api.fail_heartbeat = True
            await _until(lambda: run.finished)
            assert run.abandoned
            assert api.heartbeat_failures == 3
            own_claim = run.claim_name
            assert own_claim is not None
            state["own_claim"] = own_claim
            state["successor"] = await _successor_claims_the_thread(h, run.thread_key, own_claim)

        approvals = _AbandoningApprovals(abandon_and_hand_over, fail=create_fails)
        publications = _AbandoningPublications(abandon_and_hand_over, fail=create_fails)
        async with _held_execution(
            make_harness,
            monkeypatch,
            approvals=approvals,
            publication_creator=publications,
        ) as (h, api, event):
            state["h"] = h
            state["api"] = api
            if gate == "publication":
                _patch_snapshot(h, monkeypatch)
            h.runner.tail = _publication_gate() if gate == "publication" else _permission_gate()
            with caplog.at_level(logging.WARNING, logger="curie_worker.kernel"):
                turn = asyncio.create_task(h.kernel.process_event(event))
                await _until(lambda: api.run is not None and api.run.heartbeat_running)
                run = api.run
                h.runner.hold.set()
                await asyncio.wait_for(turn, timeout=10)
            successor: SandboxHandle = state["successor"]
            return {
                "approval_creates": len(approvals.creates),
                "publication_creates": len(publications.creates),
                "finish_posts": [path for path, _ in api.posts if path.endswith("/finish")],
                "holds": len(api.holds),
                "escalations": [
                    record.getMessage()
                    for record in caplog.records
                    if record.getMessage().startswith("escalating event ")
                ],
                "completions": [completion.outcome for completion in h.sink.completions],
                "route": h.substrate._affinity.get(run.thread_key),
                "successor": successor,
                "successor_mode": h.fake_k8s.sandboxes[successor.sandbox_name].operating_mode,
                "claims": dict(h.fake_k8s.claims),
                "deleted_claims": list(h.fake_k8s.deleted_claims),
                "own_claim": state["own_claim"],
                "workspace_releases": list(h.kernel._workspace.releases),
            }

    seen = asyncio.run(go())

    # The create really was in flight when the abandonment landed.
    if gate == "publication":
        assert seen["publication_creates"] == 1
        assert seen["approval_creates"] == 0
    else:
        assert seen["approval_creates"] == 1
        assert seen["publication_creates"] == 0
    assert seen["escalations"] == []
    assert seen["finish_posts"] == []
    assert seen["holds"] == 0
    assert seen["completions"] == ["dropped"]
    # No suspend, no release: the successor's claim and route stay live.
    successor = seen["successor"]
    route = seen["route"]
    assert route is not None
    assert route.handle.claim_name == successor.claim_name
    assert route.state is RouteState.LIVE
    assert seen["successor_mode"] != "Suspended"
    assert successor.claim_name in seen["claims"]
    assert successor.claim_name not in seen["deleted_claims"]
    assert seen["own_claim"] not in seen["deleted_claims"]
    assert seen["workspace_releases"] == []


def test_an_unrelated_turn_on_the_thread_never_rebinds_the_runs_claim(
    make_harness, monkeypatch
) -> None:
    """A plain chat turn on a run's thread is not that run's turn (#4331).

    Both events go through the real process_event path. The work item turn has
    ended and its finish call is still in flight, so the run is started,
    unfinished, and still the thread's active run (a settlement handoff would
    have removed it from the candidates). Its route then goes, as the
    termination chain or an expired route would drop it, and an unrelated chat
    message on the same thread cold-claims a fresh sandbox and runs a turn
    there. The kernel's candidate match finds the run by thread key alone, so
    only the event identity keeps the chat's claim from being recorded as the
    run's. When the finish then lands, the run's cleanup must not delete the
    chat turn's claim, route, or workspace.
    """

    from curie_worker import api_retry

    async def go() -> None:
        clock = ApiClock()
        request_id = uuid.uuid4()
        api = Api(request_id, clock)
        finish_reached = asyncio.Event()
        finish_gate = asyncio.Event()

        async def transport(request: httpx.Request) -> httpx.Response:
            if request.url.path.endswith("/finish") and not finish_gate.is_set():
                finish_reached.set()
                await finish_gate.wait()
            return api(request)

        monkeypatch.setattr(api_retry, "_clock", clock)
        monkeypatch.setattr(api_retry, "_sleep", clock.sleep)
        async with httpx.AsyncClient(transport=httpx.MockTransport(transport)) as http:
            client = WorkItemDispatchClient(
                api_base_url="http://api.example", worker_token="example", client=http
            )
            actions = ActionClient(
                api_base_url="http://api.example", api_key="example", client=http
            )
            async with make_harness(
                binding=_Binding(),
                workspace_factory=_RecordingWorkspace,
                publication_creator=_PublicationApi(),
                actions=actions,
            ) as h:
                api.kernel = h.kernel
                h.kernel._work_items = client
                h.runner.turn_scripts = [
                    _ledger_script(),
                    [Final(text="Hello back.", status=SessionStatus.DONE)],
                ]
                turn = asyncio.create_task(h.kernel.process_event(_event(request_id)))
                await asyncio.wait_for(finish_reached.wait(), timeout=5)
                run = api.run
                assert h.kernel._work_item_runs.get(request_id) is run
                assert run.started and not run.finished
                own_claim = run.claim_name
                assert own_claim is not None

                assert h.substrate._affinity.delete_if_claim(run.thread_key, own_claim)
                await asyncio.wait_for(
                    h.kernel.process_event(_turn(f"slack-{uuid.uuid4()}", "hello there")),
                    timeout=5,
                )
                assert len(h.runner.opened) == 2
                chat_route = h.substrate._affinity.get(run.thread_key)
                assert chat_route is not None
                chat_claim = chat_route.handle.claim_name
                assert chat_claim != own_claim
                assert chat_claim in h.fake_k8s.claims
                assert run.claim_name == own_claim

                finish_gate.set()
                await asyncio.wait_for(turn, timeout=5)

                assert run.finished
                assert len(api.finished_posts) == 1
                assert chat_claim in h.fake_k8s.claims
                assert chat_claim not in h.fake_k8s.deleted_claims
                record = h.substrate._affinity.get(run.thread_key)
                assert record is not None
                assert record.handle.claim_name == chat_claim
                assert h.kernel._workspace.releases == []

    asyncio.run(go())
