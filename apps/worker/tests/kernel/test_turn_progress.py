"""A person's turn carries a progress capability, and its pump applies the inbox (ADR 0130).

The kernel hands an eligible turn a per-turn capability in two runner control
headers on ``POST /v1/event``: a ``turn.progress`` sandbox token bound to the
turn chain's ``progress_id`` and the API route it is good for. While the turn's
stream is consumed, a pump applies what the API appended to that chain's inbox
to the durable record, and with rendering off nothing reaches the sink.

Real Valkey, the real substrate over a fake Kubernetes client and the
in-process fake runner, as everywhere in this suite. The API is not in the
loop: a test writes the inbox entries the API would, in the shape frozen in
``tests/vectors/turn-progress-capability.json``.
"""

from __future__ import annotations

import asyncio
import json
import sys
import uuid
from pathlib import Path
from typing import Any

import pytest
from aci_protocol import Final, SessionStatus, TextDelta, TurnSource
from curie_internal import sandbox_token
from curie_worker.approvals import ApprovalRequest, CreatedApproval
from curie_worker.config import WorkerConfig
from curie_worker.kernel.claim import _boots_differently
from curie_worker.kernel.routing import _thread_key_for
from curie_worker.progress import ProgressStore, progress_id_for, sweep_pending_progress
from curie_worker.sandbox.types import SandboxHandle
from pydantic import ValidationError

sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from queue_fixtures import qevent, wait_until  # noqa: E402

_VECTOR = json.loads(
    (
        Path(__file__).resolve().parents[4] / "tests" / "vectors" / "turn-progress-capability.json"
    ).read_text()
)
URL_HEADER = _VECTOR["url_header"]
TOKEN_HEADER = _VECTOR["token_header"]
GENERATION_HEADER = _VECTOR["generation_header"]
SCOPE = _VECTOR["token_scope"]
ELIGIBILITY_ENV = _VECTOR["eligibility_env"]
DONE = SessionStatus.DONE


def _progress(redis: Any, config: WorkerConfig) -> ProgressStore:
    return ProgressStore(redis, config)


class _Approvals:
    """An ``ApprovalCreator`` that records requests and mints stable ids."""

    def __init__(self) -> None:
        self.requests: list[ApprovalRequest] = []

    async def create(self, request: ApprovalRequest) -> CreatedApproval:
        self.requests.append(request)
        return CreatedApproval(id=f"appr-{len(self.requests)}", status="pending")


def _header(headers: dict[str, str], name: str) -> str | None:
    lowered = {key.lower(): value for key, value in headers.items()}
    return lowered.get(name.lower())


def _capability(h: Any, index: int = -1) -> tuple[str | None, str | None, str | None]:
    headers = h.runner.event_headers[index]
    return (
        _header(headers, URL_HEADER),
        _header(headers, TOKEN_HEADER),
        _header(headers, GENERATION_HEADER),
    )


def _route_for(h: Any, progress_id: str) -> str:
    base = h.config.runner_facing_api_base_url.rstrip("/")
    return base + _VECTOR["route"].format(progress_id=progress_id)


def _entry(
    update_id: str,
    state: str,
    summary: str,
    *,
    generation: int,
    seq: int,
    milestone: str | None = None,
) -> dict[str, str]:
    command: dict[str, Any] = {
        "version": "1.0",
        "update_id": update_id,
        "state": state,
        "summary": summary,
    }
    if milestone is not None:
        command["milestone"] = milestone
    fields = {
        "command": json.dumps(command),
        "generation": str(generation),
        "seq": str(seq),
    }
    assert set(fields) == set(_VECTOR["inbox_entry_example"])
    return fields


async def _append(h: Any, progress_id: str, fields: dict[str, str]) -> str:
    """Write one inbox entry the way the API does."""

    return str(await h.async_redis.xadd(h.config.progress_inbox_key(progress_id), fields))


def test_a_persons_slack_turn_carries_a_chain_bound_capability(make_harness) -> None:
    async def go() -> None:
        async with make_harness(progress_factory=_progress) as h:
            ev = qevent("check the build", thread="t-cap")
            await h.kernel.process_event(ev)

            progress_id = progress_id_for(_thread_key_for(ev), ev.event_id)
            url, token, generation = _capability(h)
            assert url == _route_for(h, progress_id)
            assert token is not None and generation == "1"
            assert sandbox_token.verify(
                token, h.config.api_key, agent=f"{progress_id}:1", scope=SCOPE
            )
            # Bound to this chain and this scope only.
            assert not sandbox_token.verify(
                token, h.config.api_key, agent=f"{uuid.uuid4()}:1", scope=SCOPE
            )
            assert not sandbox_token.verify(
                token, h.config.api_key, agent=f"{progress_id}:1", scope="work_item.progress"
            )
            assert not sandbox_token.verify(
                token, h.config.api_key, agent=f"{progress_id}:1", scope="state"
            )
            # The chain was opened while the turn ran.
            record = await ProgressStore(h.async_redis, h.config).read(progress_id)
            assert record is not None
            assert record.update_count == 0
            assert (record.turn_generation, record.active_generation) == (1, 0)
            assert any(
                env is not None and env.get(ELIGIBILITY_ENV) == "1" for env in h.fake_k8s.claim_envs
            )
            route_record = h.substrate._affinity.get(_thread_key_for(ev))  # noqa: SLF001
            assert route_record is not None
            assert route_record.handle.carries_turn_progress

    asyncio.run(go())


def test_runner_start_delay_does_not_expire_the_turn_capability(make_harness) -> None:
    """@spec ADR-0130 d1: renewal covers runner response-header delay."""

    async def go() -> None:
        async with make_harness(progress_factory=_progress) as h:
            h.runner.accept = asyncio.Event()
            ev = qevent("check the build", thread="t-delayed-start")
            turn = asyncio.create_task(h.kernel.process_event(ev))
            try:
                await wait_until(lambda: h.runner.opened == ["check the build"])
                progress_id = progress_id_for(_thread_key_for(ev), ev.event_id)

                # RunnerClient permits ten seconds for response headers. A valid
                # response after the five-second lease must still have authority.
                await asyncio.sleep(5.25)
                record = await ProgressStore(h.async_redis, h.config).read(progress_id)
                assert record is not None
                assert record.active_generation == 1
                assert record.active_until_ms > 0
                assert await ProgressStore(h.async_redis, h.config).renew_turn(progress_id, 1)
            finally:
                h.runner.accept.set()
                await turn

    asyncio.run(go())


@pytest.mark.parametrize(
    "overrides",
    [
        pytest.param({"source": TurnSource.WEBHOOK}, id="webhook-job"),
        pytest.param({"kind": "email", "channel": "agent@example.test"}, id="email"),
        pytest.param({"adapter": "curie-cluster-message"}, id="cluster-message-relay"),
    ],
)
def test_an_ineligible_turn_carries_no_capability(make_harness, overrides: dict[str, Any]) -> None:
    async def go() -> None:
        async with make_harness(progress_factory=_progress) as h:
            ev = qevent("hello", thread=f"t-inel-{uuid.uuid4().hex[:6]}", **overrides)
            await h.kernel.process_event(ev)

            assert h.runner.event_headers, "the turn must have reached the runner"
            for headers in h.runner.event_headers:
                assert _header(headers, URL_HEADER) is None
                assert _header(headers, TOKEN_HEADER) is None
                assert _header(headers, GENERATION_HEADER) is None
            assert all(env is None or ELIGIBILITY_ENV not in env for env in h.fake_k8s.claim_envs)
            progress_id = progress_id_for(_thread_key_for(ev), ev.event_id)
            assert await ProgressStore(h.async_redis, h.config).read(progress_id) is None
            route_record = h.substrate._affinity.get(_thread_key_for(ev))  # noqa: SLF001
            assert route_record is not None
            assert not route_record.handle.carries_turn_progress

    asyncio.run(go())


def test_a_factory_execution_carries_no_turn_progress_capability(make_harness) -> None:
    from test_work_item_workspace import (
        ISSUE_URL,
        _Binding,
        _NoExistingPublication,
        _turn,
        _WorkItems,
        _Workspace,
    )

    async def go() -> None:
        async with make_harness(
            binding=_Binding(),
            workspace_factory=_Workspace,
            publication_creator=_NoExistingPublication(),
            progress_factory=_progress,
        ) as h:
            h.kernel._work_items = _WorkItems()
            h.runner.default_script = [Final(text="Done.", status=DONE)]
            request_id = uuid.uuid4()

            await h.kernel.process_event(
                _turn(f"work-item-{request_id}-execute-1", f"Resolve {ISSUE_URL}")
            )

            assert h.runner.event_headers, "the execution must have reached the runner"
            for headers in h.runner.event_headers:
                assert _header(headers, URL_HEADER) is None
                assert _header(headers, TOKEN_HEADER) is None
                assert _header(headers, GENERATION_HEADER) is None
            assert all(env is None or ELIGIBILITY_ENV not in env for env in h.fake_k8s.claim_envs)

    asyncio.run(go())


def test_a_kernel_without_a_progress_store_sends_no_capability(make_harness) -> None:
    async def go() -> None:
        async with make_harness() as h:
            await h.kernel.process_event(qevent("hello", thread="t-unwired"))

            assert h.runner.event_headers
            assert _capability(h) == (None, None, None)
            assert all(env is None or ELIGIBILITY_ENV not in env for env in h.fake_k8s.claim_envs)

    asyncio.run(go())


def test_a_retry_of_the_same_event_names_the_same_chain(make_harness) -> None:
    async def go() -> None:
        async with make_harness(progress_factory=_progress) as h:
            # The first /v1/event answers 500, a flag-clean runner error the
            # kernel retries in the same delivery.
            h.runner.event_fail_times = 1
            ev = qevent("check the build", thread="t-retry")
            await h.kernel.process_event(ev)

            assert len(h.runner.event_headers) == 2
            progress_id = progress_id_for(_thread_key_for(ev), ev.event_id)
            for index in (0, 1):
                url, token, generation = _capability(h, index)
                assert url == _route_for(h, progress_id)
                assert token is not None and generation == str(index + 1)
                assert sandbox_token.verify(
                    token,
                    h.config.api_key,
                    agent=f"{progress_id}:{index + 1}",
                    scope=SCOPE,
                )

    asyncio.run(go())


def test_a_steer_opens_no_chain(make_harness) -> None:
    async def go() -> None:
        async with make_harness(progress_factory=_progress) as h:
            hold = asyncio.Event()
            h.runner.hold = hold
            h.runner.default_script = [TextDelta(text="working")]
            h.runner.tail = [Final(text="done", status=DONE)]
            store = ProgressStore(h.async_redis, h.config)

            first = qevent("first", thread="t-steer")
            turn = asyncio.create_task(h.kernel.process_event(first))
            await wait_until(lambda: h.runner.turn_active)

            follow_up = qevent("second", thread="t-steer")
            await h.kernel.process_event(follow_up)
            assert h.runner.steers == ["second"]

            hold.set()
            await turn

            # One turn, one capability, one chain: the follow-up rode the live
            # turn's capability and minted no record of its own.
            assert len(h.runner.event_headers) == 1
            thread_key = _thread_key_for(first)
            assert await store.read(progress_id_for(thread_key, first.event_id)) is not None
            assert await store.read(progress_id_for(thread_key, follow_up.event_id)) is None

    asyncio.run(go())


def test_the_pump_applies_the_turns_commands_while_it_runs(make_harness) -> None:
    async def go() -> None:
        async with make_harness(progress_factory=_progress) as h:
            hold = asyncio.Event()
            h.runner.hold = hold
            h.runner.default_script = [TextDelta(text="working")]
            h.runner.tail = [Final(text="done", status=DONE)]
            store = ProgressStore(h.async_redis, h.config)

            ev = qevent("check the build", thread="t-pump")
            turn = asyncio.create_task(h.kernel.process_event(ev))
            await wait_until(lambda: h.runner.turn_active)
            progress_id = progress_id_for(_thread_key_for(ev), ev.event_id)

            await _append(
                h,
                progress_id,
                _entry("u1", "investigating", "Reading the failing test", generation=1, seq=1),
            )
            await _append(
                h,
                progress_id,
                _entry(
                    "u2",
                    "investigating",
                    "Found the regression",
                    generation=1,
                    seq=2,
                    milestone="evidence",
                ),
            )

            async def applied(count: int) -> None:
                for _ in range(500):
                    record = await store.read(progress_id)
                    if record is not None and record.update_count >= count:
                        return
                    await asyncio.sleep(0.01)
                raise AssertionError(f"the pump did not apply {count} commands mid-turn")

            # Applied while the turn is still live, not only when it ends.
            await applied(2)
            assert h.runner.turn_active

            # A malformed entry is skipped; an older epoch's command is refused
            # by the record's order; the last one lands.
            await _append(
                h,
                progress_id,
                {"command": "not json", "generation": "1", "seq": "3"},
            )
            await _append(
                h,
                progress_id,
                _entry("stale", "publishing", "From an older turn", generation=0, seq=9),
            )
            last = await _append(
                h,
                progress_id,
                _entry(
                    "u3",
                    "testing",
                    "Verified the fix",
                    generation=1,
                    seq=4,
                    milestone="verification",
                ),
            )
            hold.set()
            await turn

            record = await store.read(progress_id)
            assert record is not None
            assert record.state is not None and record.state.value == "testing"
            assert record.summary == "Verified the fix"
            assert record.revision == 3
            assert record.milestones_used == 2
            assert record.update_count == 3
            assert (record.epoch, record.last_seq) == (1, 4)
            # The cursor stands past the last entry, so a resume of this chain
            # starts after it.
            assert record.inbox_cursor == last

    asyncio.run(go())


def test_rendering_off_records_progress_and_emits_nothing(make_harness) -> None:
    async def go() -> None:
        async with make_harness(progress_factory=_progress) as h:
            assert h.config.progress_render is False
            hold = asyncio.Event()
            h.runner.hold = hold
            h.runner.default_script = [TextDelta(text="working")]
            h.runner.tail = [Final(text="done", status=DONE)]
            store = ProgressStore(h.async_redis, h.config)

            ev = qevent("check the build", thread="t-dark")
            turn = asyncio.create_task(h.kernel.process_event(ev))
            await wait_until(lambda: h.runner.turn_active)
            progress_id = progress_id_for(_thread_key_for(ev), ev.event_id)
            await _append(
                h,
                progress_id,
                _entry(
                    "u1",
                    "investigating",
                    "Reading the logs",
                    generation=1,
                    seq=1,
                    milestone="evidence",
                ),
            )
            hold.set()
            await turn

            record = await store.read(progress_id)
            assert record is not None and record.revision == 1
            assert record.milestones_used == 1
            # No reply event carries progress, and the kernel posted nothing.
            assert all(getattr(event, "progress", None) is None for event, _r, _b in h.sink.events)
            assert h.sink.posts == []
            # Nothing is left owed for a later deliverer to replay.
            assert await h.async_redis.smembers(h.config.progress_pending_key()) == set()
            leftovers = [
                key
                async for key in h.async_redis.scan_iter(match=h.config.progress_delivery_key("*"))
            ]
            assert leftovers == []
            delivered: list[object] = []

            async def deliver(stored: object) -> str | None:
                delivered.append(stored)
                return None

            await sweep_pending_progress(store, deliver=deliver, grace_s=0.0)
            assert delivered == []

    asyncio.run(go())


def test_rendering_on_is_refused_at_startup(monkeypatch: pytest.MonkeyPatch) -> None:
    with pytest.raises(ValidationError, match="CURIE_PROGRESS_RENDER"):
        WorkerConfig(progress_render=True)
    monkeypatch.setenv("CURIE_PROGRESS_RENDER", "true")
    with pytest.raises(ValidationError, match="CURIE_PROGRESS_RENDER"):
        WorkerConfig()
    monkeypatch.setenv("CURIE_PROGRESS_RENDER", "false")
    assert WorkerConfig().progress_render is False


def test_a_pause_links_its_resume_to_the_chain(make_harness) -> None:
    async def go() -> None:
        approvals = _Approvals()
        async with make_harness(approvals=approvals, progress_factory=_progress) as h:
            h.runner.default_script = [
                TextDelta(text="Requesting sign-off"),
                Final(
                    text="Requesting sign-off",
                    status=SessionStatus.AWAITING_APPROVAL,
                    approval_summary="Give ACME a 20% discount",
                ),
            ]
            ev = qevent("please discount", thread="t-pause")
            await h.kernel.process_event(ev)

            assert approvals.requests, "the turn must have paused for approval"
            progress_id = progress_id_for(_thread_key_for(ev), ev.event_id)
            store = ProgressStore(h.async_redis, h.config)
            assert await store.resolve_chain("approval-appr-1-resolved") == progress_id

    asyncio.run(go())


def test_an_approval_resume_continues_its_chain(make_harness) -> None:
    async def go() -> None:
        async with make_harness(progress_factory=_progress) as h:
            store = ProgressStore(h.async_redis, h.config)
            original = qevent("please discount", thread="t-resume")
            progress_id = await store.open_chain(_thread_key_for(original), original.event_id)
            approval_id = str(uuid.uuid4())
            resume_event_id = f"approval-{approval_id}-resolved"
            assert await store.link_resume(resume_event_id, progress_id)

            resume = qevent(
                "[approval resolved] approved", thread="t-resume", event_id=resume_event_id
            )
            await h.kernel.process_event(resume)

            url, token, generation = _capability(h)
            assert url == _route_for(h, progress_id)
            assert token is not None and generation == "1"
            assert sandbox_token.verify(
                token, h.config.api_key, agent=f"{progress_id}:1", scope=SCOPE
            )
            # The resume never derives a chain of its own.
            own = progress_id_for(_thread_key_for(resume), resume_event_id)
            assert await store.read(own) is None

    asyncio.run(go())


def test_a_resume_whose_pointer_expired_gets_no_capability(make_harness) -> None:
    async def go() -> None:
        async with make_harness(progress_factory=_progress) as h:
            resume_event_id = f"approval-{uuid.uuid4()}-resolved"
            resume = qevent(
                "[approval resolved] approved", thread="t-expired", event_id=resume_event_id
            )
            await h.kernel.process_event(resume)

            assert h.runner.event_headers
            assert _capability(h) == (None, None, None)
            store = ProgressStore(h.async_redis, h.config)
            own = progress_id_for(_thread_key_for(resume), resume_event_id)
            assert await store.read(own) is None

    asyncio.run(go())


def test_eligibility_is_a_persons_slack_turn_only() -> None:
    from aci_protocol import HookRunRef, QueuedTurn
    from curie_worker.turn_progress import progress_eligible

    person = qevent("hello")
    assert progress_eligible(person, factory_work_item=False)
    assert not progress_eligible(person, factory_work_item=True)
    assert not progress_eligible(qevent("hi", source=TurnSource.CRON), factory_work_item=False)
    assert not progress_eligible(qevent("hi", source=TurnSource.WEBHOOK), factory_work_item=False)
    assert not progress_eligible(qevent("hi", kind="email"), factory_work_item=False)
    assert not progress_eligible(
        qevent("hi", adapter="curie-cluster-message"), factory_work_item=False
    )
    # A named Slack identity is still a person's Slack turn.
    assert progress_eligible(qevent("hi", adapter="second-bot"), factory_work_item=False)
    targetless = QueuedTurn(
        event_id="hook-1",
        conversation_id="hook-thread",
        author="system",
        text="nightly",
        reply_handle=None,
        received_at="2026-07-05T00:00:00+00:00",
        source=TurnSource.CRON,
        hook_run=HookRunRef(
            agent_id=str(uuid.uuid4()), name="nightly", slot_utc="2026-07-05T00:00:00+00:00"
        ),
    )
    assert not progress_eligible(targetless, factory_work_item=False)


@pytest.mark.parametrize(
    ("booted", "requested", "must_replace"),
    [
        (False, False, False),
        (False, True, True),
        (True, False, True),
        (True, True, False),
    ],
)
def test_warm_sandbox_adoption_fences_progress_eligibility_in_both_directions(
    booted: bool, requested: bool, must_replace: bool
) -> None:
    """@spec ADR-0130 d1: model-surface eligibility is immutable per sandbox boot."""

    handle = SandboxHandle(
        thread_key="slack:C0EXAMPLE1:t",
        claim_name="claim",
        sandbox_name="sandbox",
        namespace="curie",
        service_fqdn="sandbox.curie.svc",
        port=8080,
        session_id="session",
        carries_turn_progress=booted,
    )
    env = {ELIGIBILITY_ENV: "1"} if requested else {}
    assert _boots_differently(handle, env) is must_replace


def test_a_runner_booted_for_another_run_is_replaced() -> None:
    """@spec ADR-0178 d3: a sandbox belongs to one run."""

    run_a = "11111111-1111-4111-8111-111111111111"
    run_b = "33333333-3333-4333-8333-333333333333"

    def handle(caller_run: str | None) -> SandboxHandle:
        return SandboxHandle(
            thread_key="slack:C0EXAMPLE1:t",
            claim_name="claim",
            sandbox_name="sandbox",
            namespace="curie",
            service_fqdn="sandbox.curie.svc",
            port=8080,
            session_id="session",
            carries_caller_token=True,
            caller_run=caller_run,
        )

    env = {"CURIE_CONNECTOR_CALLER_TOKEN": "cct.present"}
    assert _boots_differently(handle(run_a), env, caller_run=run_b) is True
    assert _boots_differently(handle(run_a), env, caller_run=None) is True
    assert _boots_differently(handle(run_a), env, caller_run=run_a) is False
    assert _boots_differently(handle(None), env, caller_run=run_a) is True
    assert _boots_differently(handle(None), {}, caller_run=None) is False
