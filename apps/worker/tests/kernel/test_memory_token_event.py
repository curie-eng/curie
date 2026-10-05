"""ADR-0188 (#3623): each turn carries its own memory write credential on the Event.

For an agent with memory writes on, the worker mints a short-lived ``state``
credential per turn (``BindingResolver.turn_memory_token``) and sends it as
``Event.memory_token`` on the runner POST, never in the boot env. These tests
drive the real ``Kernel.process_event`` against real Valkey and the scriptable
HTTP runner in ``conftest.py``; the binding double resolves a real
``ResolvedDeployment`` and delegates ``boot_env`` and ``turn_memory_token`` to
the real resolver, so the credentials under test are the ones the worker would
really mint.
"""

from __future__ import annotations

import asyncio
import base64
import functools
import json
import sys
import time
import uuid
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from aci_protocol import Final, OutboundEvent, QueuedTurn, SessionStatus, TextDelta, ToolNote
from curie_internal.sandbox_token import verify
from curie_worker import binding as binding_module
from curie_worker.behaviorpacks import BehaviorPacks
from curie_worker.binding import BindingResolver, ResolvedDeployment
from curie_worker.config import WorkerConfig

# importlib import mode does not add the test root to sys.path.
sys.path.insert(0, str(Path(__file__).parent.parent))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from queue_fixtures import qevent  # noqa: E402
from queue_fixtures import wait_until as _wait_until  # noqa: E402
from test_work_item_early_stop import (  # noqa: E402
    ISSUE_PROMPT,
    _PublicationApi,
    _WorkItems,
    _Workspace,
)
from test_work_item_early_stop import _turn as _work_item_turn  # noqa: E402

AGENT_ID = uuid.UUID("11111111-1111-4111-8111-111111111111")
DEPLOYMENT_ID = uuid.UUID("22222222-2222-4222-8222-222222222222")
CHANNEL = "C0EXAMPLE1"
_qevent = functools.partial(qevent, channel=CHANNEL, received_at="2026-10-01T00:00:00+00:00")


class _MemoryBinding:
    """Resolves one agent with memory writes on; real boot env and real mints."""

    def __init__(self, *, memory_writes: bool = True) -> None:
        self.memory_writes = memory_writes
        self.envs: list[dict[str, str]] = []
        self.turn_token_calls: list[dict[str, Any]] = []
        self._real = BindingResolver.__new__(BindingResolver)
        self._real._config = WorkerConfig()  # type: ignore[attr-defined]

    async def resolve(self, kind: str, adapter: str | None, channel: str) -> object:
        return ResolvedDeployment(
            agent_id=AGENT_ID,
            agent_name="acme-bot",
            deployment_id=DEPLOYMENT_ID,
            version_id=uuid.UUID("33333333-3333-4333-8333-333333333333"),
            version_label="v1",
            bundle_ref=None,
            max_usd_per_day=None,
            max_output_tokens_per_run=None,
        )

    async def memory_writes_for(self, _agent_id: uuid.UUID) -> bool:
        return self.memory_writes

    def boot_env(self, resolved: Any, thread_key: str, **kwargs: Any) -> dict[str, str]:
        env = self._real.boot_env(resolved, thread_key, **kwargs)
        self.envs.append(dict(env))
        return env

    def turn_memory_token(self, resolved: Any, **kwargs: Any) -> str | None:
        self.turn_token_calls.append(dict(kwargs))
        return self._real.turn_memory_token(resolved, **kwargs)  # type: ignore[attr-defined]

    def packs_for(self, _resolved: object) -> BehaviorPacks:
        return BehaviorPacks()


def _claims(token: object) -> dict[str, Any]:
    assert isinstance(token, str) and token, f"no memory_token on the event: {token!r}"
    assert verify(token, WorkerConfig().api_key, agent=str(AGENT_ID), scope="state") is True
    seg = token.split(".")[1]
    payload = json.loads(base64.urlsafe_b64decode(seg + "=" * (-len(seg) % 4)))
    assert isinstance(payload, dict)
    return payload


def _as(turn: QueuedTurn, author: str) -> QueuedTurn:
    return turn.model_copy(update={"author": author})


def test_runner_event_carries_turn_token(make_harness) -> None:
    async def go() -> None:
        binding = _MemoryBinding()
        async with make_harness(binding=binding) as h:
            turn = _as(_qevent("remember the deploy window", thread="th-mt-1"), "U0ALICE01")

            await h.kernel.process_event(turn)

            assert h.sink.last_text == "ok"
            body = h.runner.event_bodies[0]
            claims = _claims(body["memory_token"])
            assert claims["agent"] == str(AGENT_ID)
            assert claims["scope"] == "state"
            assert claims["memory"] == "write"
            assert claims["binding"] == f"slack:{CHANNEL}"
            assert claims["sender"] == "U0ALICE01"
            # The run identity: the queued event id, plus a per-attempt suffix
            # so closing one attempt's credential cannot refuse a retry's (#3776).
            assert claims["turn"].startswith(f"{turn.event_id}#"), claims["turn"]

    asyncio.run(go())


def test_writes_off_turn_carries_no_token(make_harness) -> None:
    async def go() -> None:
        binding = _MemoryBinding(memory_writes=False)
        async with make_harness(binding=binding) as h:
            await h.kernel.process_event(_qevent("hello", thread="th-mt-2"))

            assert h.runner.event_bodies[0]["memory_token"] is None
            # The boot env still carries the long-lived read credential.
            claims = _claims(binding.envs[0]["CURIE_MEMORY_TOKEN"])
            assert claims["memory"] == "read"

    asyncio.run(go())


def test_steer_carries_steering_senders_token(
    make_harness, monkeypatch: pytest.MonkeyPatch
) -> None:
    # A steer reuses the event built for the steering turn, so the steering
    # sender writes under their own name, not the live turn's sender.
    async def go() -> None:
        binding = _MemoryBinding()
        async with make_harness(binding=binding) as h:
            steered: list[Any] = []
            real_steer = h.kernel._runner.steer

            async def spy(base_url: str, event: Any, **kwargs: Any) -> Any:
                steered.append(event)
                return await real_steer(base_url, event, **kwargs)

            monkeypatch.setattr(h.kernel._runner, "steer", spy)
            hold = asyncio.Event()
            h.runner.hold = hold
            h.runner.default_script = [TextDelta(text="working")]
            h.runner.tail = [Final(text="done", status=SessionStatus.DONE)]

            first = _as(
                _qevent("first question", thread="th-mt-3", placeholder="ph-1"), "U0ALICE01"
            )
            task = asyncio.create_task(h.kernel.process_event(first))
            try:
                await _wait_until(lambda: h.runner.turn_active, "the first turn to be live")
                second = _as(
                    _qevent("and also this", thread="th-mt-3", placeholder="ph-2"), "U0BOB0001"
                )
                await h.kernel.process_event(second)
            finally:
                hold.set()
                await asyncio.gather(task, return_exceptions=True)

            assert h.runner.steers == ["and also this"]
            # Only the follow-up's steer landed (a steer the kernel tries
            # before the first turn is live is refused by the runner).
            steered = [e for e in steered if e.text == "and also this"]
            assert len(steered) == 1
            first_claims = _claims(h.runner.event_bodies[0]["memory_token"])
            assert first_claims["sender"] == "U0ALICE01"
            assert first_claims["turn"].startswith(f"{first.event_id}#")
            steer_claims = _claims(steered[0].memory_token)
            assert steer_claims["sender"] == "U0BOB0001"
            assert steer_claims["turn"].startswith(f"{second.event_id}#")
            assert steer_claims["binding"] == f"slack:{CHANNEL}"

    asyncio.run(go())


def _tool(name: str) -> ToolNote:
    return ToolNote(text=f"running tool {name}", tool=name)


def test_work_item_continuation_carries_token(make_harness) -> None:
    # A factory execute turn that ends without publishing is re-prompted once
    # in the same session; that continuation opens a turn too, so it carries a
    # write credential minted for it.
    async def go() -> None:
        binding = _MemoryBinding()
        scripts: list[list[OutboundEvent]] = [
            [
                _tool("mcp__github__get_issue"),
                TextDelta(text="I read the issue."),
                Final(text="I read the issue.", status=SessionStatus.DONE),
            ],
            [Final(text="I will not continue.", status=SessionStatus.DONE)],
        ]
        async with make_harness(
            binding=binding, workspace_factory=_Workspace, publication_creator=_PublicationApi()
        ) as h:
            h.kernel._work_items = _WorkItems()
            h.runner.turn_scripts = [list(s) for s in scripts]
            h.runner.default_script = [Final(text="unexpected", status=SessionStatus.DONE)]
            turn = _work_item_turn(f"work-item-{uuid.uuid4()}-execute-1", ISSUE_PROMPT)

            await h.kernel.process_event(turn)

            assert len(h.runner.event_bodies) == 2, h.runner.opened
            assert h.runner.opened[0] == ISSUE_PROMPT
            for body in h.runner.event_bodies:
                claims = _claims(body["memory_token"])
                assert claims["memory"] == "write"
                assert claims["sender"] == turn.author
                assert claims["turn"].startswith(f"{turn.event_id}#")
                assert claims["binding"] == f"slack:{turn.reply_handle.channel}"

    asyncio.run(go())


def test_turn_token_never_in_boot_env(make_harness) -> None:
    # MEMORY-TOKEN-3 / ADR-0188 decision 5: the per-turn credential rides the
    # event only. No boot-env value is it, and no boot-env token can write.
    async def go() -> None:
        binding = _MemoryBinding()
        async with make_harness(binding=binding) as h:
            await h.kernel.process_event(_as(_qevent("hi there", thread="th-mt-5"), "U0ALICE01"))

            token = h.runner.event_bodies[0]["memory_token"]
            assert _claims(token)["memory"] == "write"
            envs = [dict(env or {}) for env in h.fake_k8s.claim_envs] + binding.envs
            assert envs, "the turn must claim a sandbox"
            for env in envs:
                assert token not in env.values()
                assert all(not (isinstance(v, str) and token in v) for v in env.values())
                for name in ("CURIE_MEMORY_TOKEN", "CURIE_HISTORY_TOKEN"):
                    if name in env:
                        assert _claims(env[name])["memory"] == "read", name
            # Not in the runner request headers either.
            for headers in h.runner.event_headers:
                assert token not in " ".join(headers.values())

    asyncio.run(go())


def test_retry_mints_fresh_expiry(make_harness, monkeypatch: pytest.MonkeyPatch) -> None:
    # Each attempt mints its own credential: a retried turn does not reuse the
    # first attempt's expiry. The binding module's clock is advanced between
    # mints so two mints in the same wall-clock second still differ.
    async def go() -> None:
        binding = _MemoryBinding()
        offset = {"s": 0}
        real_time = time.time

        def clock() -> float:
            return real_time() + offset["s"]

        monkeypatch.setattr(
            binding_module, "time", SimpleNamespace(time=clock, monotonic=time.monotonic)
        )
        real_mint = binding.turn_memory_token

        def advancing(resolved: Any, **kwargs: Any) -> str | None:
            offset["s"] += 1000
            return real_mint(resolved, **kwargs)

        binding.turn_memory_token = advancing  # type: ignore[method-assign]
        async with make_harness(binding=binding, max_attempts=3) as h:
            h.runner.event_fail_times = 1

            await h.kernel.process_event(_as(_qevent("retry me", thread="th-mt-6"), "U0ALICE01"))

            assert len(h.runner.event_bodies) == 2, h.runner.opened
            first, second = (_claims(b["memory_token"]) for b in h.runner.event_bodies)
            assert second["exp"] >= first["exp"] + 1000
            # Each attempt is its own turn for the closed-turn check (#3776).
            assert second["turn"] != first["turn"]
            assert (
                h.runner.event_bodies[0]["memory_token"] != h.runner.event_bodies[1]["memory_token"]
            )
            assert len(binding.turn_token_calls) >= 2

    asyncio.run(go())


# --------------------------------------------------------------------------- #
# Security review of #3623
# --------------------------------------------------------------------------- #

# The most a turn credential may outlive the turn's own deadline: clock skew only.
_CLOCK_SKEW_S = 5


@pytest.mark.parametrize("runner_total_timeout_s", [30.0, 120.0])
def test_turn_token_expires_by_the_turn_deadline(
    make_harness, monkeypatch: pytest.MonkeyPatch, runner_total_timeout_s: float
) -> None:
    # Review M2: the turn's deadline is the runner request's stream timeout,
    # ``min(runner_total_timeout_s, remaining delivery budget)`` (RunnerClient
    # ``start_turn``). The credential must be dead by then, give or take clock
    # skew: no extra grace, and not the whole delivery budget.
    async def go() -> None:
        binding = _MemoryBinding()
        async with make_harness(
            binding=binding, runner_total_timeout_s=runner_total_timeout_s
        ) as h:
            starts: list[tuple[float, float | None]] = []
            real_start = h.kernel._runner.start_turn

            async def spy(base_url: str, event: Any, **kwargs: Any) -> Any:
                starts.append((time.time(), kwargs.get("remaining_s")))
                return await real_start(base_url, event, **kwargs)

            monkeypatch.setattr(h.kernel._runner, "start_turn", spy)
            await h.kernel.process_event(_as(_qevent("short one", thread="th-mt-7"), "U0ALICE01"))

            assert h.sink.last_text == "ok"
            assert starts, "the turn never started"
            started_at, remaining_s = starts[0]
            stream_s = h.kernel._runner._total_timeout_s
            if remaining_s is not None:
                stream_s = min(stream_s, remaining_s)
            deadline = started_at + stream_s
            exp = _claims(h.runner.event_bodies[0]["memory_token"])["exp"]
            assert exp <= deadline + _CLOCK_SKEW_S, (
                f"credential outlives the turn by {exp - deadline:.0f}s "
                f"(stream timeout {stream_s:.0f}s)"
            )

    asyncio.run(go())


def _conftest_runner_class(make_harness: Any) -> Any:
    # The harness's FakeRunner, from the conftest module pytest loaded (an
    # ``import conftest`` here would load a second copy under importlib mode).
    return make_harness.__globals__["FakeRunner"]


def _old_runner_400(frames: list[dict[str, Any]]) -> Any:
    # A runner from before ``server._frame_error``: pydantic's default 400
    # repeats the rejected input, which for a model-level error is the frame.
    from aiohttp import web

    async def handler(self: Any, request: Any) -> Any:
        body = await request.json()
        frames.append(body)
        if request.path == "/v1/event":
            self.opened.append(body["text"])
            self.event_bodies.append(body)
        elif not self.turn_active:
            # A steer with no live turn is refused as before, so the kernel
            # opens the first turn normally.
            frames.pop()
            return web.json_response({"error": "no active turn"}, status=409)
        detail = [{"type": "value_error", "loc": ["body"], "msg": "bad frame", "input": body}]
        return web.json_response({"detail": detail}, status=400)

    return handler


def _assert_not_logged(caplog: pytest.LogCaptureFixture, token: str) -> None:
    assert caplog.records, "nothing was logged"
    assert token not in caplog.text
    for record in caplog.records:
        assert token not in record.getMessage(), record.name
        if record.exc_info is not None:
            assert token not in repr(record.exc_info[1]), record.name
            assert token not in str(record.exc_info[1]), record.name


def test_old_runner_event_400_does_not_log_the_token(
    make_harness, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    # Review L2 (MEMORY-TOKEN-3): an older runner's 400 body echoes the frame,
    # memory_token included. The worker must not put that body in a log line.
    frames: list[dict[str, Any]] = []
    monkeypatch.setattr(_conftest_runner_class(make_harness), "_event", _old_runner_400(frames))
    caplog.set_level("DEBUG")

    async def go() -> None:
        binding = _MemoryBinding()
        async with make_harness(binding=binding, max_attempts=1) as h:
            await h.kernel.process_event(_as(_qevent("hello", thread="th-mt-8"), "U0ALICE01"))

    asyncio.run(go())
    assert frames, "the runner never got the frame"
    token = frames[0]["memory_token"]
    assert _claims(token)["memory"] == "write"
    _assert_not_logged(caplog, token)


def test_old_runner_steer_400_does_not_log_the_token(
    make_harness, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    # The same for /v1/steer: a follow-up steered into a live turn.
    steers: list[dict[str, Any]] = []
    monkeypatch.setattr(_conftest_runner_class(make_harness), "_steer", _old_runner_400(steers))
    caplog.set_level("DEBUG")

    async def go() -> None:
        binding = _MemoryBinding()
        async with make_harness(binding=binding) as h:
            hold = asyncio.Event()
            h.runner.hold = hold
            h.runner.default_script = [TextDelta(text="working")]
            h.runner.tail = [Final(text="done", status=SessionStatus.DONE)]
            first = _as(_qevent("first", thread="th-mt-9", placeholder="ph-1"), "U0ALICE01")
            task = asyncio.create_task(h.kernel.process_event(first))
            try:
                await _wait_until(lambda: h.runner.turn_active, "the first turn to be live")
                second = _as(_qevent("follow", thread="th-mt-9", placeholder="ph-2"), "U0BOB0001")
                await asyncio.wait_for(h.kernel.process_event(second), timeout=30)
            finally:
                hold.set()
                await asyncio.gather(task, return_exceptions=True)

    asyncio.run(go())
    tokens = [s["memory_token"] for s in steers if s.get("memory_token")]
    assert tokens, f"no steer carried a token: {steers!r}"
    for token in tokens:
        _assert_not_logged(caplog, token)


# --------------------------------------------------------------------------- #
# Re-review of #3623: R1 (the credential's expiry is the stream deadline) and
# R2 (no part of an old runner's 400 body reaches a log line)
# --------------------------------------------------------------------------- #


class _ShiftedTime:
    """The ``time`` module with a wall clock the test can move forward.

    Patched into both the kernel and the binding module, so the kernel's view
    of "now" and the credential's ``exp`` read the same clock."""

    def __init__(self) -> None:
        self.offset_s = 0.0

    def time(self) -> float:
        return time.time() + self.offset_s

    def __getattr__(self, name: str) -> Any:
        return getattr(time, name)


def _shift_clocks(monkeypatch: pytest.MonkeyPatch) -> _ShiftedTime:
    from curie_worker.kernel import clock as kernel_clock

    clock = _ShiftedTime()
    monkeypatch.setattr(binding_module, "time", clock)
    monkeypatch.setattr(kernel_clock, "time", clock)
    return clock


def _stream_s(h: Any, remaining_s: float | None) -> float:
    # ``RunnerClient.start_turn``'s own bound on the stream.
    total = float(h.kernel._runner._total_timeout_s)
    return total if remaining_s is None else max(0.05, min(total, remaining_s))


def _grant(thread_key: str) -> Any:
    from curie_worker.kernel.memory import TurnMemoryGrant

    resolved = ResolvedDeployment(
        agent_id=AGENT_ID,
        agent_name="acme-bot",
        deployment_id=DEPLOYMENT_ID,
        version_id=uuid.UUID("33333333-3333-4333-8333-333333333333"),
        version_label="v1",
        bundle_ref=None,
        max_usd_per_day=None,
        max_output_tokens_per_run=None,
        memory_writes=True,
    )
    return TurnMemoryGrant(resolved=resolved, kind="slack", address=CHANNEL, thread_key=thread_key)


@pytest.mark.parametrize("spent", [0.0, -3.0])
def test_spent_budget_gets_no_turn_token(make_harness, spent: float) -> None:
    # R1. A spent budget (``remaining_s`` of exactly 0, or below) is not "no
    # budget". The stream gets only ``_MIN_REQUEST_TIMEOUT_S`` (50 ms) to fail
    # fast, so there is no turn for a credential to cover: the worker mints
    # none, and the runner falls back to the read-only boot-env token. An
    # already-expired token would be the same to the API, but it would still put
    # a signed credential on the wire for nothing. It must never fall back to
    # the whole delivery budget, which a falsy ``0.0`` check does.
    async def go() -> None:
        binding = _MemoryBinding()
        async with make_harness(binding=binding) as h:
            turn = _as(_qevent("late", thread="th-mt-r1a"), "U0ALICE01")
            event = h.kernel._to_event(turn)
            out = h.kernel._with_memory_token(event, turn, _grant("th-mt-r1a"), spent)
            if out.memory_token is not None:
                exp = _claims(out.memory_token)["exp"]
                assert exp <= int(time.time()), f"a spent turn's credential lives to {exp}"
            assert out.memory_token is None, "a spent budget must mint no credential"
            # No budget in hand at all (None) is the ceiling, not "spent".
            live = h.kernel._with_memory_token(event, turn, _grant("th-mt-r1a"), None)
            assert live.memory_token is not None

    asyncio.run(go())


def test_turn_token_exp_is_the_stream_deadline_after_a_slow_claim(
    make_harness, monkeypatch: pytest.MonkeyPatch
) -> None:
    # R1. The sandbox claim (a cold boot) can take tens of seconds, and nothing
    # takes that off the ``remaining_s`` passed to ``start_turn``. So the stream
    # deadline is ``start + bound(remaining)``. A credential minted before the
    # claim expires that much earlier than the stream, and a memory write near
    # the end of the turn gets a 403. Its ``exp`` must be the stream deadline,
    # within ``_CLOCK_SKEW_S`` either way.
    claim_s = 120.0

    async def go() -> None:
        binding = _MemoryBinding()
        clock = _shift_clocks(monkeypatch)
        async with make_harness(binding=binding, runner_total_timeout_s=300.0) as h:
            real_claim = h.fake_k8s.create_claim

            def slow_claim(*args: Any, **kwargs: Any) -> Any:
                clock.offset_s += claim_s
                return real_claim(*args, **kwargs)

            monkeypatch.setattr(h.fake_k8s, "create_claim", slow_claim)
            starts: list[tuple[float, float | None]] = []
            real_start = h.kernel._runner.start_turn

            async def spy(base_url: str, event: Any, **kwargs: Any) -> Any:
                starts.append((clock.time(), kwargs.get("remaining_s")))
                return await real_start(base_url, event, **kwargs)

            monkeypatch.setattr(h.kernel._runner, "start_turn", spy)
            await h.kernel.process_event(_as(_qevent("cold one", thread="th-mt-r1b"), "U0ALICE01"))

            assert h.sink.last_text == "ok"
            assert clock.offset_s == claim_s, "the turn never claimed a sandbox"
            started_at, remaining_s = starts[0]
            deadline = started_at + _stream_s(h, remaining_s)
            exp = _claims(h.runner.event_bodies[0]["memory_token"])["exp"]
            assert abs(exp - deadline) <= _CLOCK_SKEW_S, (
                f"credential exp is {exp - deadline:+.0f}s from the stream deadline"
            )

    asyncio.run(go())


def test_steer_token_does_not_outlive_the_live_turn(
    make_harness, monkeypatch: pytest.MonkeyPatch
) -> None:
    # R1. A steer joins the live turn and ends when it ends. Its credential is
    # minted later than the live turn's, from the steering delivery's own
    # budget, so left alone it expires after the live turn's stream deadline.
    # It must be capped at that deadline.
    later_s = 100.0

    async def go() -> None:
        binding = _MemoryBinding()
        clock = _shift_clocks(monkeypatch)
        async with make_harness(binding=binding, runner_total_timeout_s=300.0) as h:
            starts: list[tuple[float, float | None]] = []
            steered: list[Any] = []
            real_start = h.kernel._runner.start_turn
            real_steer = h.kernel._runner.steer

            async def start_spy(base_url: str, event: Any, **kwargs: Any) -> Any:
                starts.append((clock.time(), kwargs.get("remaining_s")))
                return await real_start(base_url, event, **kwargs)

            async def steer_spy(base_url: str, event: Any, **kwargs: Any) -> Any:
                steered.append(event)
                return await real_steer(base_url, event, **kwargs)

            monkeypatch.setattr(h.kernel._runner, "start_turn", start_spy)
            monkeypatch.setattr(h.kernel._runner, "steer", steer_spy)
            hold = asyncio.Event()
            h.runner.hold = hold
            h.runner.default_script = [TextDelta(text="working")]
            h.runner.tail = [Final(text="done", status=SessionStatus.DONE)]

            first = _as(_qevent("first", thread="th-mt-r1c", placeholder="ph-1"), "U0ALICE01")
            task = asyncio.create_task(h.kernel.process_event(first))
            try:
                await _wait_until(lambda: h.runner.turn_active, "the first turn to be live")
                # The fake runner marks the turn live as its headers go out,
                # before the kernel records the turn's deadline. Shifting the
                # clock in that gap would record the deadline late and void the
                # cap this test pins.
                await _wait_until(
                    lambda: bool(h.kernel._turn_deadlines), "the live turn's deadline recorded"
                )
                clock.offset_s += later_s
                second = _as(
                    _qevent("and this", thread="th-mt-r1c", placeholder="ph-2"), "U0BOB0001"
                )
                await h.kernel.process_event(second)
            finally:
                hold.set()
                await asyncio.gather(task, return_exceptions=True)

            assert h.runner.steers == ["and this"]
            started_at, remaining_s = starts[0]
            live_deadline = started_at + _stream_s(h, remaining_s)
            landed = [e for e in steered if e.text == "and this"]
            assert len(landed) == 1
            claims = _claims(landed[0].memory_token)
            assert claims["sender"] == "U0BOB0001"
            # ``exp`` is a whole second, rounded up, hence the 1 s.
            assert claims["exp"] <= live_deadline + 1, (
                f"steer credential outlives the live turn by {claims['exp'] - live_deadline:.0f}s"
            )

    asyncio.run(go())


_BODY_MARKER = "OLD-RUNNER-400-BODY"
_FRAGMENT_LEN = 8


def _old_runner_truncated_400(frames: list[dict[str, Any]]) -> Any:
    # A runner from before ``server._frame_error`` answers a bad frame with
    # ``{"error": f"invalid event frame: {exc}"}``. ``str(ValidationError)``
    # truncates ``input_value``, so the body never holds the whole token, only
    # pieces of it. This builds that real message, and adds a prefix and a
    # suffix of the token so the fixture does not depend on where pydantic cuts.
    from aci_protocol import Event
    from aiohttp import web
    from pydantic import ValidationError

    async def handler(self: Any, request: Any) -> Any:
        body = await request.json()
        frames.append(body)
        if request.path == "/v1/event":
            self.opened.append(body["text"])
            self.event_bodies.append(body)
        elif not self.turn_active:
            frames.pop()
            return web.json_response({"error": "no active turn"}, status=409)
        try:
            Event.model_validate({**body, "zz_newer_field": 1})
            exc_text = "validated"
        except ValidationError as exc:
            exc_text = str(exc)
        token = body.get("memory_token") or ""
        message = (
            f"{_BODY_MARKER} invalid event frame: {exc_text} "
            f"[prefix {token[:24]}...] [suffix ...{token[-24:]}]"
        )
        return web.json_response({"error": message}, status=400)

    return handler


def _assert_no_token_fragment(caplog: pytest.LogCaptureFixture, token: str) -> None:
    assert caplog.records, "nothing was logged"
    texts = [caplog.text]
    for record in caplog.records:
        texts.append(record.getMessage())
        if record.exc_info is not None:
            texts += [repr(record.exc_info[1]), str(record.exc_info[1])]
    logged = "\n".join(texts)
    assert _BODY_MARKER not in logged, "the runner's 400 body reached a log line"
    for i in range(len(token) - _FRAGMENT_LEN + 1):
        fragment = token[i : i + _FRAGMENT_LEN]
        assert fragment not in logged, f"token fragment at offset {i} was logged"


def test_old_runner_event_truncated_400_logs_no_token_fragment(
    make_harness, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    # R2 (MEMORY-TOKEN-3): an exact-string replace cannot redact a truncated
    # echo. The worker logs the status and the body's length, never the body.
    frames: list[dict[str, Any]] = []
    monkeypatch.setattr(
        _conftest_runner_class(make_harness), "_event", _old_runner_truncated_400(frames)
    )
    caplog.set_level("DEBUG")

    async def go() -> None:
        binding = _MemoryBinding()
        async with make_harness(binding=binding, max_attempts=1) as h:
            await h.kernel.process_event(_as(_qevent("hello", thread="th-mt-r2a"), "U0ALICE01"))

    asyncio.run(go())
    assert frames, "the runner never got the frame"
    token = frames[0]["memory_token"]
    assert _claims(token)["memory"] == "write"
    _assert_no_token_fragment(caplog, token)


def test_old_runner_steer_truncated_400_logs_no_token_fragment(
    make_harness, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    # The same for /v1/steer.
    steers: list[dict[str, Any]] = []
    monkeypatch.setattr(
        _conftest_runner_class(make_harness), "_steer", _old_runner_truncated_400(steers)
    )
    caplog.set_level("DEBUG")

    async def go() -> None:
        binding = _MemoryBinding()
        async with make_harness(binding=binding) as h:
            hold = asyncio.Event()
            h.runner.hold = hold
            h.runner.default_script = [TextDelta(text="working")]
            h.runner.tail = [Final(text="done", status=SessionStatus.DONE)]
            first = _as(_qevent("first", thread="th-mt-r2b", placeholder="ph-1"), "U0ALICE01")
            task = asyncio.create_task(h.kernel.process_event(first))
            try:
                await _wait_until(lambda: h.runner.turn_active, "the first turn to be live")
                second = _as(_qevent("follow", thread="th-mt-r2b", placeholder="ph-2"), "U0BOB0001")
                await asyncio.wait_for(h.kernel.process_event(second), timeout=30)
            finally:
                hold.set()
                await asyncio.gather(task, return_exceptions=True)

    asyncio.run(go())
    tokens = [s["memory_token"] for s in steers if s.get("memory_token")]
    assert tokens, f"no steer carried a token: {steers!r}"
    for token in tokens:
        _assert_no_token_fragment(caplog, token)
