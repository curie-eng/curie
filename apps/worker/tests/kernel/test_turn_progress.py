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
import base64
import hashlib
import inspect
import json
import sys
import time
import uuid
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from aci_protocol import Final, SessionStatus, TextDelta, TurnSource
from curie_internal import sandbox_token
from curie_worker import caller_token
from curie_worker.approvals import ApprovalRequest, CreatedApproval
from curie_worker.binding import BOOT_TOKEN_GRACE_SECONDS
from curie_worker.config import WorkerConfig
from curie_worker.kernel import claim as kernel_claim
from curie_worker.kernel import clock as kernel_clock
from curie_worker.kernel import failures as kernel_failures
from curie_worker.kernel import hooks as kernel_hooks
from curie_worker.kernel.claim import _boots_differently
from curie_worker.kernel.core import Kernel
from curie_worker.kernel.routing import _thread_key_for
from curie_worker.progress import ProgressStore, progress_id_for, sweep_pending_progress
from curie_worker.runner_client import RunnerError
from curie_worker.sandbox.types import SandboxHandle
from nacl.signing import SigningKey
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

    async def create(self, request: ApprovalRequest, *, budget_s: float = 120) -> CreatedApproval:
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


def test_an_expired_boot_token_replaces_the_warm_sandbox() -> None:
    """#3823: a follow-up replaces a sandbox whose token cannot cover that turn."""

    from curie_internal.sandbox_token import mint
    from curie_worker.binding import HISTORY_TOKEN_ENV

    base = dict(
        thread_key="slack:C0EXAMPLE1:t",
        claim_name="claim",
        sandbox_name="sandbox",
        namespace="curie",
        service_fqdn="sandbox.curie.svc",
        port=8080,
        session_id="session",
    )
    exp = int(time.time()) + 100
    handle = SandboxHandle(**base, state_token_exp=exp)
    later = mint(
        "api-key",
        agent="22222222-2222-4222-8222-222222222222",
        scope="state",
        exp=exp + 500,
        claims={"cred": "ab" * 16, "binding": None, "memory": "read"},
    )
    sooner = mint(
        "api-key",
        agent="22222222-2222-4222-8222-222222222222",
        scope="state",
        exp=exp - 10,
        claims={"cred": "cd" * 16, "binding": None, "memory": "read"},
    )
    assert _boots_differently(handle, {HISTORY_TOKEN_ENV: later}) is True
    assert _boots_differently(handle, {HISTORY_TOKEN_ENV: sooner}) is False
    dead = SandboxHandle(**base, state_token_exp=int(time.time()) - 1)
    assert _boots_differently(dead, {}) is True
    assert _boots_differently(SandboxHandle(**base), {}) is False


# -- ADR 0270 decision 3: requester-aware adoption, coverage and window ---------------

_CALLER_TOKEN_ENV = "CURIE_CONNECTOR_CALLER_TOKEN"
_REQUESTER_DIGEST_DOMAIN = b"curie-cct-principal-v1\n"
_REQUESTER_TENANT = "33333333-3333-4333-8333-333333333333"
_REQUESTER_ISSUER = "https://identity.example.test/directory"
_ALICE = {
    "tenant_id": _REQUESTER_TENANT,
    "issuer": _REQUESTER_ISSUER,
    "subject": "SyntheticAliceSubject",
}
_BOB = {
    "tenant_id": _REQUESTER_TENANT,
    "issuer": _REQUESTER_ISSUER,
    "subject": "SyntheticBobSubject",
}
_REQUESTER_RUN = "11111111-1111-4111-8111-111111111111"
_REQUESTER_WORK_ITEM = "22222222-2222-4222-8222-222222222222"
# A frozen wall clock, far from the real one so no assertion passes by accident.
_T = 1_900_000_000
_REQUESTER_TTL = 3600


def _requester_digest(principal: Any) -> str:
    """The requester fact, computed independently of the worker. @spec CONNECTOR-REQUESTER-3."""
    canonical = json.dumps(principal, separators=(",", ":"), sort_keys=True, ensure_ascii=True)
    digest = hashlib.sha256(_REQUESTER_DIGEST_DOMAIN + canonical.encode("ascii")).hexdigest()
    return "sha256:" + digest


_A = _requester_digest(_ALICE)
_B = _requester_digest(_BOB)


def _requester_seed() -> str:
    """A fresh worker Ed25519 seed, minted at runtime. @spec CONNECTOR-REQUESTER-3."""
    return base64.b64encode(bytes(SigningKey.generate())).decode()


def _b64url(raw: bytes) -> str:
    """@spec CONNECTOR-REQUESTER-3."""
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii")


def _cct(
    seed: str,
    *,
    exp: int,
    principal: dict[str, str] | None = None,
    run: str | None = None,
    work_item: str | None = None,
) -> str:
    """@spec CONNECTOR-REQUESTER-3."""
    return caller_token.mint(
        seed,
        agent="test-agent",
        exp=exp,
        run=run,
        work_item=work_item,
        principal=None if principal is None else caller_token.CallerPrincipal(**principal),
    )


def _signed_cct(seed: str, body: dict[str, Any]) -> str:
    """A cct over any payload: shapes the minter refuses. @spec CONNECTOR-REQUESTER-3."""
    payload = json.dumps(body, separators=(",", ":"), sort_keys=True).encode("ascii")
    head = f"cct.{_b64url(payload)}"
    signature = SigningKey(base64.b64decode(seed)).sign(head.encode("ascii")).signature
    return f"{head}.{_b64url(signature)}"


def _verified_cct(seed: str, token: str) -> dict[str, Any]:
    """Signature-checked claims of a cct. @spec CONNECTOR-REQUESTER-3."""
    head, payload, signature = token.split(".")
    assert head == "cct"
    SigningKey(base64.b64decode(seed)).verify_key.verify(
        f"{head}.{payload}".encode("ascii"),
        base64.urlsafe_b64decode(signature + "=" * (-len(signature) % 4)),
    )
    raw = base64.urlsafe_b64decode(payload + "=" * (-len(payload) % 4))
    claims: dict[str, Any] = json.loads(raw)
    return claims


def _requester_handle(**overrides: Any) -> SandboxHandle:
    """@spec CONNECTOR-REQUESTER-3."""
    base: dict[str, Any] = {
        "thread_key": "slack:C0EXAMPLE1:t",
        "claim_name": "claim",
        "sandbox_name": "sandbox",
        "namespace": "curie",
        "service_fqdn": "sandbox.curie.svc",
        "port": 8080,
        "session_id": "session",
    }
    base.update(overrides)
    return SandboxHandle(**base)


def _freeze_kernel_clock(monkeypatch: pytest.MonkeyPatch, now: float) -> None:
    """Patch the kernel's one clock binding. @spec CONNECTOR-REQUESTER-3."""
    frozen = SimpleNamespace(**{**vars(time), "time": lambda: now})
    monkeypatch.setattr(kernel_clock, "time", frozen)


def _requester_change() -> Callable[..., bool]:
    """``claim._requester_change``, resolved at call time. @spec CONNECTOR-REQUESTER-3."""
    rule = getattr(kernel_claim, "_requester_change", None)
    assert callable(rule), "kernel.claim has no _requester_change"
    return rule  # type: ignore[no-any-return]


_RECORDED = {"unrecorded": None, "absent": "absent", "none": "none", "A": _A, "B": _B}
_DESIRED = {"absent": "absent", "none": "none", "A": _A, "B": _B}
# Every (recorded, desired) cell: "always" replaces, "never" adopts, and
# "enforced" replaces only for an opted-in turn. Written out, not derived.
_REQUESTER_RULE = {
    ("unrecorded", "absent"): "enforced",
    ("unrecorded", "none"): "enforced",
    ("unrecorded", "A"): "always",
    ("unrecorded", "B"): "always",
    ("absent", "absent"): "never",
    ("absent", "none"): "never",
    ("absent", "A"): "always",
    ("absent", "B"): "always",
    ("none", "absent"): "never",
    ("none", "none"): "never",
    ("none", "A"): "always",
    ("none", "B"): "always",
    ("A", "absent"): "always",
    ("A", "none"): "always",
    ("A", "A"): "never",
    ("A", "B"): "always",
    ("B", "absent"): "always",
    ("B", "none"): "always",
    ("B", "A"): "always",
    ("B", "B"): "never",
}


def test_the_requester_rule_table_covers_every_cell() -> None:
    """@spec CONNECTOR-REQUESTER-3: the table below is the whole cross product."""
    assert set(_REQUESTER_RULE) == {(r, d) for r in _RECORDED for d in _DESIRED}
    assert _A != _B and _A.startswith("sha256:") and len(_A) == len("sha256:") + 64


@pytest.mark.parametrize("enforced", [False, True], ids=["flag-off", "enforced"])
@pytest.mark.parametrize(("recorded", "desired"), sorted(_REQUESTER_RULE))
def test_requester_change_rule_table(recorded: str, desired: str, enforced: bool) -> None:
    """@spec CONNECTOR-REQUESTER-3: a digest on either side must match exactly.

    Principal-less facts (``"none"``, ``"absent"``) match each other both
    ways. A route written before recording (None) cannot prove a match for an
    opted-in turn, and is adopted by a flag-off one as today. The digest rule
    holds with the flag off: that is what makes disabling the flag, and a
    machine turn, replace a human's sandbox.
    """
    rule = _requester_change()
    parameter = inspect.signature(rule).parameters.get("enforced")
    assert parameter is not None and parameter.kind is inspect.Parameter.KEYWORD_ONLY
    cell = _REQUESTER_RULE[(recorded, desired)]
    expected = cell == "always" or (cell == "enforced" and enforced)
    assert rule(_RECORDED[recorded], _DESIRED[desired], enforced=enforced) is expected


def _env_for(booted: str, seed: str) -> dict[str, str]:
    """The boot env a delivery brings: A's, B's, principal-less, or no cct.

    @spec CONNECTOR-REQUESTER-3.
    """
    if booted == "absent":
        return {}
    principal = {"A": _ALICE, "B": _BOB, "none": None}[booted]
    return {_CALLER_TOKEN_ENV: _cct(seed, exp=_T + _REQUESTER_TTL, principal=principal)}


# @spec CONNECTOR-REQUESTER-3
@pytest.mark.parametrize(
    ("recorded", "booted", "enforced", "replaced"),
    [
        ("A", "B", False, True),
        ("A", "B", True, True),
        ("A", "none", False, True),
        ("A", "absent", False, True),
        ("unrecorded", "A", False, True),
        ("unrecorded", "none", True, True),
        ("none", "A", False, True),
        # Controls: the same human, or two principal-less boots, adopt.
        ("A", "A", False, False),
        ("A", "A", True, False),
        ("none", "none", True, False),
        ("unrecorded", "none", False, False),
    ],
)
def test_boots_differently_applies_the_requester_rule(
    monkeypatch: pytest.MonkeyPatch, recorded: str, booted: str, enforced: bool, replaced: bool
) -> None:
    """A warm runner booted for another requester is replaced, never adopted.

    The handle's caller expiry covers the turn, so only the requester decides.
    """
    _freeze_kernel_clock(monkeypatch, float(_T))
    seed = _requester_seed()
    handle = _requester_handle(
        carries_caller_token=True,
        caller_requester=_RECORDED[recorded],
        caller_token_exp=_T + 5000,
    )
    env = _env_for(booted, seed)
    assert (
        _boots_differently(handle, env, requester_enforced=enforced, caller_needed=_T + 700)
        is replaced
    )


# @spec CONNECTOR-REQUESTER-3
@pytest.mark.parametrize("enforced", [False, True], ids=["flag-off", "enforced"])
def test_a_requester_handle_below_caller_needed_is_replaced(
    monkeypatch: pytest.MonkeyPatch, enforced: bool
) -> None:
    """A warm requester cct that cannot cover the next turn is replaced.

    ``caller_needed`` is computed for this turn; the env's own cct expiry is
    not the comparison. Covering exactly is enough (admission accepts it).
    """
    _freeze_kernel_clock(monkeypatch, float(_T))
    seed = _requester_seed()
    env = _env_for("A", seed)
    needed = _T + 700

    def replaced(exp: int | None) -> bool:
        handle = _requester_handle(
            carries_caller_token=True, caller_requester=_A, caller_token_exp=exp
        )
        return _boots_differently(handle, env, requester_enforced=enforced, caller_needed=needed)

    assert replaced(_T + 100) is True
    assert replaced(None) is True
    assert replaced(needed - 1) is True
    # Controls: a covering token on the same requester is adopted.
    assert replaced(needed) is False
    assert replaced(_T + 5000) is False


# @spec CONNECTOR-REQUESTER-3
@pytest.mark.parametrize("enforced", [False, True], ids=["flag-off", "enforced"])
def test_principal_less_handles_ignore_caller_expiry(
    monkeypatch: pytest.MonkeyPatch, enforced: bool
) -> None:
    """Coverage touches only principal-bearing tokens; principal-less boots adopt as today."""
    _freeze_kernel_clock(monkeypatch, float(_T))
    seed = _requester_seed()
    plain = _env_for("none", seed)
    cases: list[tuple[str | None, bool, dict[str, str]]] = [
        ("none", True, plain),
        ("none", True, {}),
        ("absent", False, {}),
    ]
    if not enforced:
        # A legacy route on a flag-off turn adopts as today, whatever its expiry.
        cases.append((None, True, plain))
    for requester, carries, env in cases:
        for exp in (_T + 100, _T - 1, None):
            handle = _requester_handle(
                carries_caller_token=carries, caller_requester=requester, caller_token_exp=exp
            )
            assert (
                _boots_differently(handle, env, requester_enforced=enforced, caller_needed=_T + 700)
                is False
            ), (requester, carries, exp)


def _kernel_helper(name: str) -> Callable[..., Any]:
    """A kernel helper wherever the change put it: claim, hooks, or the Kernel class.

    @spec CONNECTOR-REQUESTER-3.
    """
    for owner in (kernel_claim, kernel_hooks, Kernel):
        helper = getattr(owner, name, None)
        if callable(helper):
            return helper  # type: ignore[no-any-return]
    raise AssertionError(f"no kernel helper named {name}")


class _WindowRunner:
    """``RunnerClient.turn_deadline_s`` with a 600 s session total. @spec CONNECTOR-REQUESTER-3."""

    def turn_deadline_s(self, remaining_s: float | None) -> float:
        if remaining_s is None:
            return 600.0
        return max(1.0, min(600.0, remaining_s))


def _window_kernel() -> SimpleNamespace:
    """A kernel double with no owned work item. @spec CONNECTOR-REQUESTER-3."""
    needed_exp = _kernel_helper("_requester_needed_exp")
    kernel = SimpleNamespace(_runner=_WindowRunner(), _work_item_runs={})
    kernel._requester_needed_exp = lambda remaining_s, run: needed_exp(kernel, remaining_s, run)
    return kernel


def _call(helper: Callable[..., Any], *args: Any) -> Any:
    """Call a helper that may be sync or async. @spec CONNECTOR-REQUESTER-3."""
    result = helper(*args)
    if inspect.isawaitable(result):

        async def wait() -> Any:
            return await result

        return asyncio.run(wait())
    return result


# @spec CONNECTOR-REQUESTER-3
def test_requester_window_rule(monkeypatch: pytest.MonkeyPatch) -> None:
    """A requester turn starts only while its cct outlives the turn's bound plus grace.

    Needed is now plus the turn's runtime bound plus the boot grace, lowered to
    the run's execution deadline when the delivery owns one. Only a digest
    handle is checked; ``"none"``, ``"absent"`` and an unrecorded route skip.
    """
    _freeze_kernel_clock(monkeypatch, _T + 0.75)
    needed_exp = _kernel_helper("_requester_needed_exp")
    kernel = _window_kernel()
    grace = BOOT_TOKEN_GRACE_SECONDS
    assert needed_exp(kernel, None, None) == _T + 600 + grace
    assert needed_exp(kernel, 120.4, None) == _T + 121 + grace

    def run(deadline: int | None) -> SimpleNamespace:
        return SimpleNamespace(
            execution_deadline=(
                None if deadline is None else datetime.fromtimestamp(deadline, UTC)
            ),
            request_id=uuid.UUID(_REQUESTER_RUN),
            work_item_id=uuid.UUID(_REQUESTER_WORK_ITEM),
        )

    assert needed_exp(kernel, None, run(_T + 300)) == _T + 300
    assert needed_exp(kernel, None, run(_T + 5000)) == _T + 600 + grace
    assert needed_exp(kernel, None, run(None)) == _T + 600 + grace

    window = _kernel_helper("_require_requester_window")
    short = getattr(kernel_failures, "RequesterTokenWindowShort", None)
    assert isinstance(short, type) and issubclass(short, RunnerError)
    needed = _T + 600 + grace
    _call(window, kernel, _requester_handle(caller_requester=_A, caller_token_exp=needed), None)
    with pytest.raises(short):
        _call(
            window,
            kernel,
            _requester_handle(caller_requester=_A, caller_token_exp=needed - 1),
            None,
        )
    with pytest.raises(short):
        _call(window, kernel, _requester_handle(caller_requester=_A, caller_token_exp=None), None)
    for requester in ("none", "absent", None):
        for exp in (_T + 1, None):
            _call(
                window,
                kernel,
                _requester_handle(caller_requester=requester, caller_token_exp=exp),
                None,
            )


_DEADLINE_RESIGN_CASES = [
    # (principal, signed exp offset, deadline offset, expected exp offset or outcome)
    ("alice", 3600, 1200, 1200),
    ("alice", 86400, 5000, 3600),
    ("alice", 3600, 5000, "untouched"),
    ("plain", 86400, 1200, 1200),
    ("malformed", 3600, 1200, "raises"),
]


# @spec CONNECTOR-REQUESTER-3
@pytest.mark.parametrize(
    ("principal", "signed", "deadline", "outcome"),
    _DEADLINE_RESIGN_CASES,
    ids=["shortened", "one-hour-cap", "untouched", "principal-less", "malformed"],
)
def test_deadline_resign_keeps_the_exact_principal_and_only_shortens(
    monkeypatch: pytest.MonkeyPatch,
    principal: str,
    signed: int,
    deadline: int,
    outcome: int | str,
) -> None:
    """The factory deadline correction keeps the requester and never lengthens.

    ``exp = min(signed exp, deadline, now + (3600 if principal else 86400))``
    with the run pair kept and one ``replace_handle`` claim. A malformed
    principal raises: no principal-less fallback, no longer token, no claim.
    """
    _freeze_kernel_clock(monkeypatch, float(_T))
    seed = _requester_seed()
    if principal == "malformed":
        token = _signed_cct(
            seed,
            {
                "agent": "test-agent",
                "exp": _T + signed,
                "run": _REQUESTER_RUN,
                "work_item": _REQUESTER_WORK_ITEM,
                "principal": {"subject": _ALICE["subject"]},
            },
        )
    else:
        token = _cct(
            seed,
            exp=_T + signed,
            principal=_ALICE if principal == "alice" else None,
            run=_REQUESTER_RUN,
            work_item=_REQUESTER_WORK_ITEM,
        )
    boot_env = {_CALLER_TOKEN_ENV: token}
    claims: list[tuple[str, dict[str, str], dict[str, Any]]] = []
    replacement = _requester_handle(claim_name="claim-2", sandbox_name="sandbox-2")

    async def claim_or_resume(
        thread_key: str, env: dict[str, str] | None, **kwargs: Any
    ) -> SandboxHandle:
        claims.append((thread_key, dict(env or {}), kwargs))
        return replacement

    kernel = SimpleNamespace(
        _config=SimpleNamespace(connector_caller_signing_key=seed),
        _claim_or_resume=claim_or_resume,
    )
    run = SimpleNamespace(
        execution_deadline=datetime.fromtimestamp(_T + deadline, UTC),
        request_id=uuid.UUID(_REQUESTER_RUN),
        work_item_id=uuid.UUID(_REQUESTER_WORK_ITEM),
        claim_name="claim",
        sandbox_name="sandbox",
    )
    # Only the fields the deadline cap reads today, so the controls run green
    # before the requester fields exist.
    handle = _requester_handle(carries_caller_token=True, caller_run=_REQUESTER_RUN)

    async def resign() -> SandboxHandle:
        result: SandboxHandle = await kernel_claim._cap_caller_token_to_deadline(
            kernel,  # type: ignore[arg-type]
            "slack:C0EXAMPLE1:t",
            boot_env,
            handle,
            run,  # type: ignore[arg-type]
            agent_name="test-agent",
            workspace_deployment_id=None,
            workspace_repo=None,
            lineage_branch=None,
            lineage_head=None,
            lineage_base_sha=None,
            publication_visible_outcome_revision=None,
            runner_resources=None,
            remaining_s=None,
        )
        return result

    if outcome == "raises":
        with pytest.raises(ValueError):
            asyncio.run(resign())
        assert claims == []
        assert boot_env[_CALLER_TOKEN_ENV] == token
        return
    result = asyncio.run(resign())
    if outcome == "untouched":
        assert result is handle
        assert claims == []
        assert boot_env[_CALLER_TOKEN_ENV] == token
        return

    assert isinstance(outcome, int)
    assert result is replacement
    assert len(claims) == 1
    _thread, claim_env, kwargs = claims[0]
    assert kwargs.get("replace_handle") is handle
    assert kwargs.get("caller_run") == _REQUESTER_RUN
    resigned = boot_env[_CALLER_TOKEN_ENV]
    assert claim_env[_CALLER_TOKEN_ENV] == resigned
    payload = _verified_cct(seed, resigned)
    expected: dict[str, Any] = {
        "agent": "test-agent",
        "exp": _T + outcome,
        "run": _REQUESTER_RUN,
        "work_item": _REQUESTER_WORK_ITEM,
    }
    if principal == "alice":
        expected["principal"] = _ALICE
    assert payload == expected
    # Only ever shorter than what was signed.
    assert payload["exp"] < _T + signed
    if principal == "plain":
        # Principal-less: byte for byte today's re-sign.
        assert resigned == _cct(
            seed, exp=_T + outcome, run=_REQUESTER_RUN, work_item=_REQUESTER_WORK_ITEM
        )
    assert run.claim_name == "claim-2" and run.sandbox_name == "sandbox-2"
