"""A tracker_result execution runs one session and never publishes (#5586, ADR 0277).

The request's completion contract arrives at acquire, before the capability mint
and the sandbox claim. For a tracker_result request the kernel:

1. binds no publication context and refuses a publish request before any
   publication API call (AC4), while an implementation run still publishes;
2. finishes a delivered turn ``completed`` with cause ``tracker_result`` and no
   pull request, the API deciding from its receipt (AC5), while an
   implementation run that delivers without a pull request still fails;
3. opens exactly one model turn: no unpublished-turn continuation, no further
   turn after a dollar halt or an execution deadline stop, and no retry of a
   flag-clean transient failure (AC6);
4. fails closed on a failed capability mint: no sandbox claim, no runner turn,
   and the acquisition is deferred rather than started or failed.

Driven through ``Kernel.process_event`` on the kernel harness (real Valkey, the
real substrate over a fake Kubernetes client, an in-process fake runner). The
WorkItems dispatch and publication API doubles are the boundary outside the box.
"""

from __future__ import annotations

import asyncio
import json
import uuid
from dataclasses import replace
from datetime import UTC, datetime
from typing import Any

import httpx
import pytest
from aci_protocol import ErrorEvent, Final, SessionStatus, TextDelta
from curie_worker import workitem_dispatch as dispatch_module
from curie_worker.binding import FactoryContext, FactoryWake
from curie_worker.kernel import lifecycle as lifecycle_module
from curie_worker.workitem_dispatch import (
    CONTRACT_IMPLEMENTATION,
    CONTRACT_TRACKER_RESULT,
    CompletionContract,
    TerminationClaim,
    WorkItemAcquireGrant,
    WorkItemConflict,
    WorkItemDispatchClient,
    WorkItemHeartbeat,
    WorkItemRequestView,
    WorkItemRun,
    WorkItemRunning,
    WorkItemStartGrant,
    WorkItemTransportError,
)

from apps.worker.tests.kernel.test_work_item_early_stop import (
    ISSUE_PROMPT,
    PUBLISH_TOOL,
    WORK_ITEM_REPO,
    WORKED_TURN,
    ZERO_WORK_TURN,
    _Binding,
    _done,
    _patch_snapshot,
    _PublicationApi,
    _publish_final,
    _tool,
    _turn,
    _WorkItems,
    _Workspace,
)

TRACKER_RESULT = CONTRACT_TRACKER_RESULT
IMPLEMENTATION = CONTRACT_IMPLEMENTATION
MUST_NOT_RUN = "a further model turn must never open"


class _ContractWorkItems(_WorkItems):
    """Dispatch double whose requests carry one completion contract.

    ``mint`` is what the issue read mint does: None answers, an exception is
    raised, an ``asyncio.Event`` is awaited (a mint that hangs). ``stop_when``
    turns the next heartbeat into the API's deadline teardown
    (``cancellation_requested`` / ``execution_deadline``); a stopped request
    then refuses any finish ``not_running``, as the API does.
    """

    def __init__(
        self,
        contract: CompletionContract,
        *,
        mint: BaseException | asyncio.Event | None = None,
        defer_refusal: str | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.contract = contract
        self.mint = mint
        self.defer_refusal = defer_refusal
        self.defers: list[dict[str, object]] = []
        self.heartbeat_interval_s = 60.0
        self.stop_when: Any = None
        self.stopping: set[uuid.UUID] = set()
        self.refused_finishes: list[dict[str, object]] = []
        self.terminations: list[dict[str, object]] = []

    async def acquire(self, request_id: uuid.UUID, **kwargs: Any) -> WorkItemAcquireGrant:
        grant = await super().acquire(request_id, **kwargs)
        if grant.completion_contract != self.contract:
            grant = replace(grant, completion_contract=self.contract)
            self.acquisitions[request_id] = grant
        return grant

    async def start(self, request_id: uuid.UUID, **kwargs: Any) -> WorkItemStartGrant:
        started = await super().start(request_id, **kwargs)
        started = replace(
            started,
            heartbeat_interval_s=self.heartbeat_interval_s,
            completion_contract=self.contract,
        )
        self.starts[request_id] = started
        return started

    async def issue_read_context(self, request_id: uuid.UUID) -> tuple[str, str]:
        self.calls.append("issue_read_context")
        if isinstance(self.mint, asyncio.Event):
            await self.mint.wait()
        elif isinstance(self.mint, BaseException):
            raise self.mint
        return await super().issue_read_context(request_id)

    async def defer(
        self, request_id: uuid.UUID, *, reason: str, capacity: bool, **_: object
    ) -> str | None:
        self.calls.append("defer")
        self.defers.append({"reason": reason, "capacity": capacity})
        if self.defer_refusal is not None:
            raise WorkItemConflict(self.defer_refusal)
        return None

    async def heartbeat(self, request_id: uuid.UUID, *, runtime_epoch: int) -> WorkItemHeartbeat:
        self.calls.append("heartbeat")
        assert runtime_epoch == self.starts[request_id].runtime_epoch
        if self.stop_when is not None and self.stop_when():
            self.stopping.add(request_id)
            return WorkItemHeartbeat(
                status="cancellation_requested",
                terminal_cause="execution_deadline",
                work_item_cancelled=False,
            )
        return WorkItemHeartbeat(status="running", terminal_cause=None, work_item_cancelled=False)

    async def finish(self, request_id: uuid.UUID, **kwargs: object) -> None:
        if request_id in self.stopping:
            self.calls.append("finish")
            self.refused_finishes.append(kwargs)
            raise WorkItemConflict("not_running")
        await super().finish(request_id, **kwargs)

    async def claim_termination(self, request_id: uuid.UUID, *, owner: str) -> TerminationClaim:
        self.calls.append("claim_termination")
        return TerminationClaim(
            runtime_epoch=self.starts[request_id].runtime_epoch, request_version=1
        )

    async def record_termination(self, request_id: uuid.UUID, **kwargs: object) -> None:
        self.calls.append("record_termination")
        self.terminations.append(kwargs)

    async def get_request(self, request_id: uuid.UUID) -> WorkItemRequestView:
        view = await super().get_request(request_id)
        if request_id in self.stopping:
            return replace(view, status="cancellation_requested")
        return view


class _CountingPublicationApi(_PublicationApi):
    """Publication API double that also counts precheck-context reads."""

    def __init__(self) -> None:
        super().__init__()
        self.prechecks = 0

    async def get_publication_precheck_context(self, **_kwargs: object) -> None:
        self.prechecks += 1
        return None


def _record_snapshots(h: Any, monkeypatch: pytest.MonkeyPatch) -> list[object]:
    """Patch the runner snapshot as the publication tests do, and record each read."""

    _patch_snapshot(h, monkeypatch)
    taken: list[object] = []
    patched = h.kernel._runner.snapshot

    async def snapshot(*args: object, **kwargs: object) -> object:
        taken.append(kwargs.get("base_sha"))
        return await patched(*args, **kwargs)

    monkeypatch.setattr(h.kernel._runner, "snapshot", snapshot)
    return taken


async def _drive(h: Any, items: _ContractWorkItems, scripts: list[list[Any]]) -> str:
    """Run one factory execute event to its end; return its event id."""

    h.kernel._work_items = items
    h.runner.turn_scripts = [list(script) for script in scripts]
    h.runner.default_script = [_done(MUST_NOT_RUN)]
    event = _turn(f"work-item-{uuid.uuid4()}-execute-1", ISSUE_PROMPT)
    await asyncio.wait_for(h.kernel.process_event(event), timeout=30)
    return event.event_id


def _harness(make_harness: Any, publications: _PublicationApi, **overrides: object) -> Any:
    return make_harness(
        binding=_Binding(),
        workspace_factory=_Workspace,
        publication_creator=publications,
        **overrides,
    )


# --- AC4: publication is refused before the publication API ----------------------


def test_a_tracker_result_publish_request_is_refused_before_any_publication_call(
    make_harness, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def exercise() -> None:
        publications = _CountingPublicationApi()
        async with _harness(make_harness, publications) as h:
            items = _ContractWorkItems(TRACKER_RESULT)
            snapshots = _record_snapshots(h, monkeypatch)

            event_id = await _drive(h, items, [[_tool(PUBLISH_TOOL), _publish_final()]])

            assert h.runner.opened == [ISSUE_PROMPT]
            # No publication authority was minted and no publication requested,
            # so nothing can have been pushed and no pull request opened.
            assert publications.prechecks == 0
            assert publications.creates == []
            assert snapshots == []
            assert "hold_for_approval" not in items.calls
            [finish] = items.finishes
            assert finish["outcome"] == "failed"
            assert finish["cause"] == "approval_create_failed"
            assert isinstance(finish["detail"], str)
            assert finish["detail"].startswith("publication.tracker_result_request")
            assert await h.kernel._markers.is_terminal(event_id)
            assert h.fake_k8s.claims == {}

    asyncio.run(exercise())


def test_the_same_publish_request_on_an_implementation_run_still_publishes(
    make_harness, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def exercise() -> None:
        publications = _CountingPublicationApi()
        async with _harness(make_harness, publications) as h:
            items = _ContractWorkItems(IMPLEMENTATION)
            snapshots = _record_snapshots(h, monkeypatch)

            await _drive(h, items, [[_tool(PUBLISH_TOOL), _publish_final()]])

            assert h.runner.opened == [ISSUE_PROMPT]
            assert publications.prechecks == 1
            assert len(publications.creates) == 1
            assert snapshots == ["a1" * 20]
            assert "hold_for_approval" in items.calls
            assert items.finishes == []

    asyncio.run(exercise())


# --- AC5: a delivered turn completes without a pull request -----------------------


@pytest.mark.parametrize("first_turn", [WORKED_TURN, ZERO_WORK_TURN], ids=["worked", "zero_work"])
def test_a_delivered_tracker_result_turn_completes_with_no_pull_request_and_no_continuation(
    make_harness, first_turn: list[Any]
) -> None:
    async def exercise() -> None:
        publications = _CountingPublicationApi()
        async with _harness(make_harness, publications) as h:
            items = _ContractWorkItems(TRACKER_RESULT)

            event_id = await _drive(h, items, [first_turn, [_done(MUST_NOT_RUN)]])

            # One model turn: neither the unpublished nor the early-stop
            # continuation prompt was sent.
            assert h.runner.opened == [ISSUE_PROMPT]
            [finish] = items.finishes
            assert finish["outcome"] == "completed"
            assert finish["cause"] == "tracker_result"
            assert finish["detail"] is None
            assert finish["ci_fix_round"] is None
            assert publications.prechecks == 0
            assert publications.creates == []
            # The contract was known before the capability mint and the start.
            assert items.calls.index("acquire") < items.calls.index("issue_read_context")
            assert items.calls.index("issue_read_context") < items.calls.index("start")
            assert await h.kernel._markers.is_terminal(event_id)
            assert h.fake_k8s.claims == {}

    asyncio.run(exercise())


@pytest.mark.parametrize(
    ("first_turn", "cause"),
    [(WORKED_TURN, "no_pull_request"), (ZERO_WORK_TURN, "early_stop")],
)
def test_an_implementation_run_that_delivers_without_a_pull_request_still_fails(
    make_harness, first_turn: list[Any], cause: str
) -> None:
    async def exercise() -> None:
        async with _harness(make_harness, _CountingPublicationApi()) as h:
            items = _ContractWorkItems(IMPLEMENTATION)

            await _drive(h, items, [first_turn, [_done("Still not publishing.")]])

            assert len(h.runner.opened) == 2
            assert "publish_changes" in h.runner.opened[1]
            [finish] = items.finishes
            assert finish["outcome"] == "failed"
            assert finish["cause"] == cause
            assert finish["detail"] == "Still not publishing."

    asyncio.run(exercise())


# --- AC6: one session --------------------------------------------------------------


def test_a_dollar_halt_finishes_budget_exceeded_with_no_further_turn(make_harness) -> None:
    async def exercise() -> None:
        async with _harness(make_harness, _CountingPublicationApi()) as h:
            items = _ContractWorkItems(TRACKER_RESULT)
            halted = [
                _tool("Bash"),
                ErrorEvent(
                    message="the session allowance was reached", classification="budget-exceeded"
                ),
                Final(text="", status=SessionStatus.CLASSIFIED_FAILURE),
            ]

            await _drive(h, items, [halted])

            assert h.runner.opened == [ISSUE_PROMPT]
            [finish] = items.finishes
            assert finish["outcome"] == "failed"
            assert finish["cause"] == "budget_exceeded"

    asyncio.run(exercise())


@pytest.mark.parametrize(
    ("contract", "turns"),
    [(TRACKER_RESULT, 1), (IMPLEMENTATION, 3)],
)
def test_a_transient_failure_is_retried_only_for_implementation(
    make_harness, contract: CompletionContract, turns: int
) -> None:
    async def exercise() -> None:
        async with _harness(make_harness, _CountingPublicationApi(), max_attempts=3) as h:
            items = _ContractWorkItems(contract)
            h.kernel._work_items = items
            h.runner.default_script = [
                ErrorEvent(
                    message="model endpoint refused the connection",
                    classification="model-unreachable",
                ),
                Final(text="", status=SessionStatus.CLASSIFIED_FAILURE),
            ]
            event = _turn(f"work-item-{uuid.uuid4()}-execute-1", ISSUE_PROMPT)

            await asyncio.wait_for(h.kernel.process_event(event), timeout=30)

            assert h.runner.opened == [ISSUE_PROMPT] * turns
            assert items.calls.count("start") == 1
            [finish] = items.finishes
            assert finish["outcome"] == "failed"
            assert finish["cause"] == "model_unreachable"

    asyncio.run(exercise())


def test_an_execution_deadline_stop_starts_no_further_turn(
    make_harness, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def exercise() -> None:
        monkeypatch.setattr(dispatch_module, "_MIN_HEARTBEAT_INTERVAL_S", 0.01)
        async with _harness(make_harness, _CountingPublicationApi()) as h:
            items = _ContractWorkItems(TRACKER_RESULT)
            items.heartbeat_interval_s = 0.01
            # The API's deadline teardown lands while the one turn is live.
            items.stop_when = lambda: h.runner.turn_active
            h.runner.hold = asyncio.Event()
            # What the runner streams once its turn is interrupted.
            h.runner.tail = [
                Final(text="run interrupted", status=SessionStatus.IDLE_AWAITING_INPUT)
            ]

            await _drive(h, items, [[TextDelta(text="Investigating.")]])

            assert h.runner.opened == [ISSUE_PROMPT]
            assert h.runner.interrupts >= 1
            assert items.stopping
            # Any finish the interrupted turn posted met the stopped request.
            assert items.finishes == []
            assert all(f["cause"] == "tracker_result" for f in items.refused_finishes)

    asyncio.run(exercise())


# --- Capability mint fails closed --------------------------------------------------


@pytest.mark.parametrize(
    "failure",
    ["conflict", "transport", "deadline", "unexpected"],
)
def test_a_failed_capability_mint_defers_without_a_sandbox_or_a_turn(
    make_harness, monkeypatch: pytest.MonkeyPatch, failure: str
) -> None:
    async def exercise() -> None:
        mint: BaseException | asyncio.Event
        if failure == "conflict":
            mint = WorkItemConflict("invalid_context")
        elif failure == "transport":
            mint = WorkItemTransportError("work-item dispatch endpoint is unreachable")
        elif failure == "deadline":
            # A mint that never answers meets the gate's own deadline.
            monkeypatch.setattr(lifecycle_module, "_TRACKER_RESULT_MINT_TIMEOUT_S", 0.05)
            mint = asyncio.Event()
        else:
            mint = RuntimeError("unexpected mint fault")
        async with _harness(make_harness, _CountingPublicationApi()) as h:
            items = _ContractWorkItems(TRACKER_RESULT, mint=mint)

            event_id = await _drive(h, items, [[_done(MUST_NOT_RUN)]])

            assert h.runner.opened == []
            assert "start" not in items.calls
            assert items.finishes == []
            assert items.defers == [{"reason": "issue_read_unavailable", "capacity": False}]
            assert h.fake_k8s.claims == {}
            assert h.fake_k8s.deleted_claims == []
            assert await h.kernel._markers.is_terminal(event_id)

    asyncio.run(exercise())


def test_a_stale_acquisitions_refused_defer_still_starts_nothing(make_harness) -> None:
    async def exercise() -> None:
        async with _harness(make_harness, _CountingPublicationApi()) as h:
            items = _ContractWorkItems(
                TRACKER_RESULT,
                mint=WorkItemTransportError("work-item dispatch endpoint is unreachable"),
                defer_refusal="not_dispatchable",
            )

            await _drive(h, items, [[_done(MUST_NOT_RUN)]])

            assert items.defers == [{"reason": "issue_read_unavailable", "capacity": False}]
            assert h.runner.opened == []
            assert "start" not in items.calls
            assert h.fake_k8s.claims == {}

    asyncio.run(exercise())


def test_an_implementation_run_with_a_failed_mint_still_boots(make_harness) -> None:
    async def exercise() -> None:
        async with _harness(make_harness, _CountingPublicationApi()) as h:
            items = _ContractWorkItems(
                IMPLEMENTATION,
                mint=WorkItemTransportError("work-item dispatch endpoint is unreachable"),
            )

            await _drive(h, items, [WORKED_TURN, [_done("Not publishing.")]])

            assert h.runner.opened[0] == ISSUE_PROMPT
            assert "start" in items.calls
            assert items.defers == []
            [finish] = items.finishes
            assert finish["cause"] == "no_pull_request"

    asyncio.run(exercise())


# --- The contract on the wire --------------------------------------------------------

_AGENT_ID = uuid.UUID("11111111-1111-4111-8111-111111111111")
_REQUEST_ID = uuid.UUID("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")
_CONTEXT = FactoryContext(
    agent_id=_AGENT_ID,
    tenant_id=uuid.UUID("00000000-0000-0000-0000-000000000001"),
    binding_id=uuid.UUID("33333333-3333-4333-8333-333333333333"),
    kind="slack",
    address="C0EXAMPLE1",
    adapter="default",
)
_WAKE = FactoryWake(kind="slack", address="C0EXAMPLE1", adapter="default", conversation_id="thread")
_ACQUIRE_BODY: dict[str, object] = {
    "generation": 1,
    "work_item_id": str(uuid.UUID("bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb")),
    "conversation_id": "work-item-thread",
    "wait_deadline": "2099-01-01T00:00:00+00:00",
    "repository_path": WORK_ITEM_REPO,
    "context": _CONTEXT.model_dump(mode="json"),
}
_START_BODY: dict[str, object] = {
    "runtime_epoch": 3,
    "execution_deadline": "2099-01-01T00:00:00+00:00",
    "remaining_s": 1800.0,
    "heartbeat_interval_s": 15.0,
}
_RUNNING_BODY: dict[str, object] = {
    **_ACQUIRE_BODY,
    "request_id": str(_REQUEST_ID),
    "agent_id": str(_AGENT_ID),
    "runtime_epoch": 3,
    "execution_deadline": "2099-01-01T00:00:00+00:00",
    "lease_held_to_deadline": False,
    "refusal": None,
}


_ABSENT = object()


def _with_contract(body: dict[str, object], contract: object) -> dict[str, object]:
    if contract is _ABSENT:
        return dict(body)
    return {**body, "completion_contract": contract}


async def _call(body: dict[str, object], verb: str) -> object:
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=json.dumps(body).encode())

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        client = WorkItemDispatchClient(
            api_base_url="http://api.example", worker_token="example-token", client=http
        )
        if verb == "acquire":
            return await client.acquire(
                _REQUEST_ID, owner="worker-1", generation=1, wake=_WAKE, context=None
            )
        if verb == "start":
            return await client.start(
                _REQUEST_ID,
                owner="worker-1",
                generation=1,
                claim_name="claim-1",
                sandbox_name="sandbox-1",
                context=_CONTEXT,
            )
        return await client.running_for_conversation("thread", wake=_WAKE)


_BODIES = {"acquire": _ACQUIRE_BODY, "start": _START_BODY, "running": _RUNNING_BODY}


@pytest.mark.parametrize("verb", list(_BODIES))
@pytest.mark.parametrize(
    ("sent", "parsed"),
    [
        (TRACKER_RESULT, TRACKER_RESULT),
        (IMPLEMENTATION, IMPLEMENTATION),
        (_ABSENT, IMPLEMENTATION),
        (None, IMPLEMENTATION),
    ],
    ids=["tracker_result", "implementation", "absent", "null"],
)
def test_every_dispatch_answer_carries_the_completion_contract(
    verb: str, sent: object, parsed: str
) -> None:
    answer = asyncio.run(_call(_with_contract(_BODIES[verb], sent), verb))

    assert answer.completion_contract == parsed  # type: ignore[attr-defined]


@pytest.mark.parametrize("verb", list(_BODIES))
@pytest.mark.parametrize("sent", ["triage", "TRACKER_RESULT", "", 1, ["tracker_result"]])
def test_an_unknown_completion_contract_is_an_unusable_answer(verb: str, sent: object) -> None:
    with pytest.raises(WorkItemTransportError):
        asyncio.run(_call(_with_contract(_BODIES[verb], sent), verb))


class _StartClient:
    def __init__(self, contract: CompletionContract) -> None:
        self.contract = contract

    async def start(self, _request_id: uuid.UUID, **_: object) -> WorkItemStartGrant:
        return WorkItemStartGrant(
            runtime_epoch=3,
            execution_deadline=datetime(2099, 1, 1, tzinfo=UTC),
            remaining_s=1800.0,
            heartbeat_interval_s=60.0,
            completion_contract=self.contract,
        )


async def _noop_stop(_thread_key: str, _run: WorkItemRun) -> None:
    return None


def _run_for(grant: WorkItemAcquireGrant | WorkItemRunning, client: object) -> WorkItemRun:
    return WorkItemRun(
        client=client,  # type: ignore[arg-type]
        request_id=_REQUEST_ID,
        owner="worker-1",
        grant=grant,
        wake=_WAKE,
        event_id=f"work-item-{_REQUEST_ID}-execute-1",
        thread_key="slack:C0EXAMPLE1:thread",
        on_stop=_noop_stop,
        on_stale=_noop_stop,
    )


def test_a_run_adopted_from_the_running_row_keeps_its_contract() -> None:
    async def go() -> tuple[bool, bool]:
        found = await _call(_with_contract(_RUNNING_BODY, TRACKER_RESULT), "running")
        assert isinstance(found, WorkItemRunning)
        adopted = _run_for(found, _StartClient(TRACKER_RESULT))
        plain = _run_for(
            replace(found, completion_contract=IMPLEMENTATION), _StartClient(IMPLEMENTATION)
        )
        try:
            return adopted.is_tracker_result, plain.is_tracker_result
        finally:
            await adopted.close()
            await plain.close()

    assert asyncio.run(go()) == (True, False)


@pytest.mark.parametrize(
    ("acquired", "started", "expected"),
    [
        (TRACKER_RESULT, TRACKER_RESULT, True),
        (TRACKER_RESULT, IMPLEMENTATION, True),
        (IMPLEMENTATION, TRACKER_RESULT, True),
        (IMPLEMENTATION, IMPLEMENTATION, False),
    ],
)
def test_the_start_answer_only_ever_widens_the_contract_to_tracker_result(
    acquired: CompletionContract, started: CompletionContract, expected: bool
) -> None:
    async def go() -> bool:
        grant = await _call(_with_contract(_ACQUIRE_BODY, acquired), "acquire")
        assert isinstance(grant, WorkItemAcquireGrant)
        run = _run_for(grant, _StartClient(started))
        try:
            await run.start(claim_name="claim-1", sandbox_name="sandbox-1")
            return run.is_tracker_result
        finally:
            await run.close()

    assert asyncio.run(go()) is expected
