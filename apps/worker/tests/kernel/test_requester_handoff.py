"""A different requester never inherits, steers or interrupts another person's runner.

ADR 0270 decision 3, the fail-closed core. Every sandbox records the requester
its ``cct`` booted with (a digest, ``"none"`` or ``"absent"``) and that token's
expiry. A delivery whose requester differs from the route's replaces the
sandbox at the durable handoff boundary when the runner is idle, and defers
without steering or interrupting when it is live. A reclaimed redelivery never
interrupts another requester's turn, and its interrupt is fenced to the exact
route it checked. A requester ``cct`` is re-minted at claim time and a turn is
admitted only when that token covers it.

The kernel harness runs real Valkey, the real substrate and the real runner
client against in-process fake runners. The identity core and the binding are
the labelled doubles of ``test_requester_claim``; ``MintingRequesterBinding``
adds a real signed ``cct`` and wires the production claim-time minter. Every
key and token is minted at runtime. No test sleeps: waits are events, bounded
polls and a movable wall clock.

@spec CONNECTOR-REQUESTER-3.
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import logging
import secrets
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from types import SimpleNamespace
from typing import Any

import pytest
from aci_protocol import Final, SessionStatus, TextDelta, TurnSource
from curie_internal.identity_authority import TurnAuthorityScopeV1
from curie_worker import binding as binding_module
from curie_worker import caller_token
from curie_worker.binding import (
    CONNECTOR_CALLER_TOKEN_ENV,
    RUNNER_TOKEN_ENV,
    SANDBOX_TOKEN_TTL_SECONDS,
    BindingResolver,
)
from curie_worker.config import WorkerConfig
from curie_worker.consumer import Consumer
from curie_worker.delivery_lease import DeliveryLeaseStore
from curie_worker.kernel import failures, identity
from curie_worker.kernel import routing as kernel_routing
from curie_worker.runner_client import RunnerError

from ..queue_fixtures import qevent, wait_until
from .test_http_identity_authority import (
    CAPSULE,
    ORIGINAL_HASH,
    pending_entry,
    process,
    require_execution_interface,
)
from .test_kernel import _leased_entry
from .test_requester_claim import (
    ISSUER,
    SCOPE,
    RequesterBinding,
    RequesterCore,
    assert_refused_before_boot,
    principal_for,
    require_requester_interface,
)

BOB_SUBJECT = "SyntheticBobSubject"
# ADR 0270 decision 3: a requester-bearing cct lives at most one hour from mint.
_REQUESTER_TTL_S = 3600
_DIGEST_DOMAIN = b"curie-cct-principal-v1\n"
_UNRECORDED = "<SandboxHandle has no such field>"
_PENDING_LOG = "requester handoff pending"
# Deliveries driven straight into the kernel with a lease and no consumer
# heartbeat: a TTL far past any test, so fenced terminal writes never meet an
# expired lease key.
_LEASE_KNOBS: dict[str, object] = {
    "delivery_budget_s": 60.0,
    "delivery_lease_ttl_s": 30.0,
    "delivery_lease_heartbeat_s": 1.0,
    "runner_total_timeout_s": 30.0,
}
# A turn whose runtime bound nearly fills the one-hour requester window.
_LONG_TURN_KNOBS: dict[str, object] = {
    "runner_total_timeout_s": 3400.0,
    "delivery_budget_s": 3600.0,
}


# -- The new kernel surface, asserted inside test bodies ------------------------------------


def handoff_pending() -> type[Exception]:
    """The deferral a live different requester meets. @spec CONNECTOR-REQUESTER-3."""
    cls = getattr(failures, "RequesterHandoffPending", None)
    assert cls is not None, "kernel failures has no RequesterHandoffPending"
    assert issubclass(cls, failures.LiveSessionBusy), (
        "RequesterHandoffPending must keep LiveSessionBusy's deferral handling"
    )
    return cls


def window_short() -> type[Exception]:
    """The admission refusal for a requester token that cannot cover the turn.

    @spec CONNECTOR-REQUESTER-3.
    """
    cls = getattr(failures, "RequesterTokenWindowShort", None)
    assert cls is not None, "kernel failures has no RequesterTokenWindowShort"
    assert issubclass(cls, RunnerError), "a short requester window must be a retryable RunnerError"
    return cls


# -- Tokens and digests -----------------------------------------------------------------------


def principal_dict(requester: dict[str, str]) -> dict[str, str]:
    """The ``principal`` object a cct carries for ``requester``. @spec CONNECTOR-REQUESTER-2."""
    return {
        "issuer": requester["issuer"],
        "subject": requester["subject"],
        "tenant_id": requester["tenant_id"],
    }


def requester_digest(requester: dict[str, str]) -> str:
    """The route's requester fact, computed independently of the worker.

    sha256 over the domain line and the compact sorted-key ASCII JSON of the
    payload's principal value. @spec CONNECTOR-REQUESTER-3.
    """
    body = json.dumps(
        principal_dict(requester), separators=(",", ":"), sort_keys=True, ensure_ascii=True
    )
    return "sha256:" + hashlib.sha256(_DIGEST_DOMAIN + body.encode("ascii")).hexdigest()


def _unpadded(segment: str) -> bytes:
    return base64.urlsafe_b64decode(segment + "=" * (-len(segment) % 4))


def cct_of(env: dict[str, str] | None) -> str:
    """The connector caller token a claim env carries. @spec CONNECTOR-REQUESTER-2."""
    assert env is not None, "the claim carried no env"
    token = env.get(CONNECTOR_CALLER_TOKEN_ENV)
    assert isinstance(token, str), "the claim env carries no connector caller token"
    return token


def cct_claims(token: str) -> dict[str, Any]:
    """The unsigned payload of a cct. @spec CONNECTOR-REQUESTER-2."""
    prefix, body, _signature = token.split(".")
    assert prefix == caller_token.PREFIX
    claims = json.loads(_unpadded(body))
    assert isinstance(claims, dict)
    return claims


def canonical(value: object) -> bytes:
    """Byte form for a byte-equal principal comparison. @spec CONNECTOR-REQUESTER-3."""
    return json.dumps(value, separators=(",", ":"), sort_keys=True).encode("ascii")


def assert_signed_by(token: str, signing_key_text: str) -> None:
    """The cct verifies under the install's caller key. @spec CONNECTOR-REQUESTER-2."""
    prefix, body, signature = token.split(".")
    verify_key = caller_token.signing_key(signing_key_text).verify_key
    verify_key.verify(f"{prefix}.{body}".encode("ascii"), _unpadded(signature))


# -- The binding double ---------------------------------------------------------------------


@dataclass(frozen=True)
class MinterRun:
    """One pass of the substrate's claim-time minter, with the fake time around it."""

    before: float
    after: float
    cct: str | None


class MintingRequesterBinding(RequesterBinding):
    """``RequesterBinding`` whose boots carry a real signed ``cct`` and a runner token.

    ``boot_env`` mints the cct as ``BindingResolver.boot_env`` does under ADR
    0270 decision 3: one hour with a principal, ``principal_less_ttl_s`` (24
    hours unless a test shortens it) without. ``fresh_boot_credential`` is the
    production ``BindingResolver.fresh_boot_credential`` over a resolver that
    holds only the caller signing key, so ``Kernel.__init__`` wires it as the
    substrate's per-claim minter and every claim passes through the real
    claim-time re-mint. The runner token makes the handoff fences and the
    authenticated ``/v1/status`` (which reports the turn epoch) reachable.

    @spec CONNECTOR-REQUESTER-2 CONNECTOR-REQUESTER-3.
    """

    def __init__(
        self,
        *,
        flag: bool | BaseException = False,
        principal_less_ttl_s: int = SANDBOX_TOKEN_TTL_SECONDS,
        **kwargs: Any,
    ) -> None:
        """@spec CONNECTOR-REQUESTER-3: a fresh signing key per binding, never committed."""
        super().__init__(flag=flag, **kwargs)
        self.signing_key = base64.b64encode(secrets.token_bytes(32)).decode("ascii")
        self.principal_less_ttl_s = principal_less_ttl_s
        self.boot_ccts: list[str] = []
        self.minter_runs: list[MinterRun] = []
        resolver = BindingResolver.__new__(BindingResolver)
        resolver._config = WorkerConfig(  # type: ignore[attr-defined]
            api_key="", connector_caller_signing_key=self.signing_key
        )
        self._resolver = resolver

    def boot_env(self, resolved: Any, thread_key: str, **kwargs: Any) -> dict[str, str]:
        """@spec CONNECTOR-REQUESTER-3: the delivery's boot cct, read from the patched clock."""
        env = super().boot_env(resolved, thread_key, **kwargs)
        principal = kwargs.get("caller_principal")
        now = int(binding_module.time.time())
        ttl = _REQUESTER_TTL_S if principal is not None else self.principal_less_ttl_s
        token = caller_token.mint(
            self.signing_key, agent=resolved.agent_name, exp=now + ttl, principal=principal
        )
        self.boot_ccts.append(token)
        return {
            **env,
            CONNECTOR_CALLER_TOKEN_ENV: token,
            RUNNER_TOKEN_ENV: secrets.token_urlsafe(24),
        }

    def fresh_boot_credential(self, env: Any) -> dict[str, str]:
        """@spec CONNECTOR-REQUESTER-3: the production claim-time minter, timed."""
        before = binding_module.time.time()
        out = self._resolver.fresh_boot_credential(env)
        after = binding_module.time.time()
        self.minter_runs.append(MinterRun(before, after, out.get(CONNECTOR_CALLER_TOKEN_ENV)))
        return out


# -- Clock and seams ------------------------------------------------------------------------


class WallClock:
    """``time`` with a movable wall clock; ``monotonic`` and the rest stay real.

    Patched into the kernel clock and the binding module, so a token's ``exp``
    and the kernel's "now" read one clock. @spec CONNECTOR-REQUESTER-3.
    """

    def __init__(self) -> None:
        self.offset_s = 0.0

    def time(self) -> float:
        return time.time() + self.offset_s

    def advance(self, seconds: float) -> None:
        self.offset_s += seconds

    def __getattr__(self, name: str) -> Any:
        return getattr(time, name)


def install_clock(monkeypatch: pytest.MonkeyPatch) -> WallClock:
    """@spec CONNECTOR-REQUESTER-3: one fake wall clock for the kernel and the binding."""
    from curie_worker.kernel import clock as kernel_clock

    clock = WallClock()
    monkeypatch.setattr(binding_module, "time", clock)
    monkeypatch.setattr(kernel_clock, "time", clock)
    return clock


def delay_claims(h: Any, clock: WallClock, delay_for: Callable[[], float]) -> list[float]:
    """Advance the wall clock inside ``create_claim``, after the minter ran.

    @spec CONNECTOR-REQUESTER-3.
    """
    real_create = h.fake_k8s.create_claim
    applied: list[float] = []

    def create_claim(name: str, **kwargs: Any) -> None:
        seconds = delay_for()
        clock.advance(seconds)
        applied.append(seconds)
        real_create(name, **kwargs)

    h.fake_k8s.create_claim = create_claim
    return applied


def observe_starts(h: Any) -> list[object]:
    """Record each turn open: ``"started"`` or the exception class it raised.

    @spec CONNECTOR-REQUESTER-3.
    """
    real_start = h.kernel._start_turn_under_hook_control
    outcomes: list[object] = []

    async def start(handle: Any, event: Any, remaining_s: Any, **kwargs: Any) -> Any:
        try:
            turn = await real_start(handle, event, remaining_s, **kwargs)
        except BaseException as exc:
            outcomes.append(type(exc))
            raise
        outcomes.append("started")
        return turn

    h.kernel._start_turn_under_hook_control = start
    return outcomes


def advance_on_first_backoff(h: Any, clock: WallClock, seconds: float) -> list[float]:
    """The retry backoff spends ``seconds`` of wall clock. @spec CONNECTOR-REQUESTER-3."""
    real_backoff = h.kernel._backoff
    advanced: list[float] = []

    def backoff(attempt: int) -> float:
        if not advanced:
            clock.advance(seconds)
            advanced.append(seconds)
        return float(real_backoff(attempt))

    h.kernel._backoff = backoff
    return advanced


# -- Routes and runners ---------------------------------------------------------------------


def thread_key(event: Any) -> str:
    """@spec CONNECTOR-REQUESTER-3."""
    return kernel_routing._thread_key_for(event)


def route(h: Any, key: str) -> Any:
    """The thread's live route. @spec CONNECTOR-REQUESTER-3."""
    handle = h.substrate.lookup(key)
    assert handle is not None, f"thread {key} has no live route"
    return handle


def recorded(handle: Any) -> tuple[object, object]:
    """The requester fact and caller-token expiry a route records. @spec CONNECTOR-REQUESTER-3."""
    return (
        getattr(handle, "caller_requester", _UNRECORDED),
        getattr(handle, "caller_token_exp", _UNRECORDED),
    )


def route_payload(h: Any, key: str) -> dict[str, Any]:
    """The durable affinity record, as stored. @spec CONNECTOR-REQUESTER-3."""
    affinity = h.substrate._affinity
    raw = affinity._redis.get(affinity._key(key))
    assert raw is not None, f"thread {key} has no route record"
    payload = json.loads(raw.decode("utf-8") if isinstance(raw, bytes) else raw)
    assert isinstance(payload, dict)
    return payload


def write_route_payload(h: Any, key: str, payload: dict[str, Any]) -> None:
    """@spec CONNECTOR-REQUESTER-3."""
    affinity = h.substrate._affinity
    affinity._redis.set(affinity._key(key), json.dumps(payload, sort_keys=True), ex=60)


def hand_route(h: Any, *, source_key: str, target_key: str) -> None:
    """Point ``target_key`` at the sandbox ``source_key`` routes to, as a concurrent
    handoff by another worker would. @spec CONNECTOR-REQUESTER-3."""
    payload = route_payload(h, source_key)
    payload["thread_key"] = target_key
    write_route_payload(h, target_key, payload)


def strip_requester_record(h: Any, key: str) -> None:
    """Rewrite the route as a worker before this change wrote it. @spec CONNECTOR-REQUESTER-3."""
    payload = route_payload(h, key)
    payload.pop("caller_requester", None)
    payload.pop("caller_token_exp", None)
    write_route_payload(h, key, payload)


def runner_at(h: Any, handle: Any) -> Any:
    """The fake runner serving one sandbox. @spec CONNECTOR-REQUESTER-3."""
    return h.runners[handle.port]


def bearer(headers: dict[str, str]) -> str | None:
    """The Authorization a runner request carried, header case aside.

    @spec CONNECTOR-REQUESTER-3.
    """
    return {name.lower(): value for name, value in headers.items()}.get("authorization")


def all_opened(h: Any) -> list[str]:
    """@spec CONNECTOR-REQUESTER-3."""
    return [text for runner in h.runners.values() for text in runner.opened]


def all_steers(h: Any) -> list[str]:
    """@spec CONNECTOR-REQUESTER-3."""
    return [text for runner in h.runners.values() for text in runner.steers]


def all_interrupts(h: Any) -> int:
    """@spec CONNECTOR-REQUESTER-3."""
    return sum(runner.interrupts for runner in h.runners.values())


def any_turn_active(h: Any) -> bool:
    """@spec CONNECTOR-REQUESTER-3."""
    return any(runner.turn_active for runner in h.runners.values())


def hold_turns(h: Any) -> list[asyncio.Event]:
    """Every runner holds its next turn live until released or interrupted.

    One event per runner, so an interrupt on one never releases another.
    @spec CONNECTOR-REQUESTER-3.
    """
    holds: list[asyncio.Event] = []
    for runner in h.runners.values():
        hold = asyncio.Event()
        runner.hold = hold
        runner.default_script = [TextDelta(text="working")]
        runner.tail = [Final(text="done", status=SessionStatus.DONE)]
        holds.append(hold)
    return holds


def release(holds: list[asyncio.Event]) -> None:
    """@spec CONNECTOR-REQUESTER-3."""
    for hold in holds:
        hold.set()


def complete_turns(h: Any) -> None:
    """Later turns finish at once. Call only after releasing the holds.

    @spec CONNECTOR-REQUESTER-3.
    """
    for runner in h.runners.values():
        runner.hold = None
        runner.default_script = [Final(text="ok", status=SessionStatus.DONE)]
        runner.tail = []


async def lease_store(h: Any) -> DeliveryLeaseStore:
    """@spec CONNECTOR-REQUESTER-3."""
    await h.async_redis.xgroup_create(
        h.config.stream, h.config.consumer_group, id="0", mkstream=True
    )
    return DeliveryLeaseStore(h.async_redis, h.config)


async def process_leased(h: Any, event: Any, lease: Any) -> None:
    """One HTTP-context delivery under ``lease``. @spec CONNECTOR-REQUESTER-3."""
    await h.kernel.process_event(
        event, lease=lease, http_identity_context=CAPSULE, original_payload_sha256=ORIGINAL_HASH
    )


async def is_done(h: Any, event: Any) -> bool:
    """@spec CONNECTOR-REQUESTER-3."""
    return bool(await h.async_redis.exists(h.config.done_key(event.event_id)))


async def pending_count(h: Any) -> int:
    """@spec CONNECTOR-REQUESTER-3."""
    summary = await h.async_redis.xpending(h.config.stream, h.config.consumer_group)
    return int(summary["pending"])


def logged(caplog: pytest.LogCaptureFixture, text: str) -> list[str]:
    """Captured records mentioning ``text``, with their exception. @spec CONNECTOR-REQUESTER-3."""
    found: list[str] = []
    for record in caplog.records:
        message = record.getMessage()
        if record.exc_info and record.exc_info[1] is not None:
            message += " " + repr(record.exc_info[1])
        if text in message:
            found.append(message)
    return found


def parties() -> tuple[MintingRequesterBinding, dict[str, str], dict[str, str], RequesterCore]:
    """An opted-in agent, its two verified humans, and a core answering for Alice.

    @spec CONNECTOR-REQUESTER-3.
    """
    binding = MintingRequesterBinding(flag=True)
    alice = binding.requester_wire()
    bob = binding.requester_wire(BOB_SUBJECT)
    return binding, alice, bob, RequesterCore(alice)


# -- requester_enforced ---------------------------------------------------------------------


def test_requester_enforced_is_true_only_for_a_live_opted_in_http_delivery() -> None:
    """@spec CONNECTOR-REQUESTER-3: plan 1.1 item 2.

    ``requester_principal`` stores its validated flag read on the carry even
    when no requester was retained, so an opted-in V2-null turn is enforced.
    A flag-off read, an ended carry and a context-free carry never are.
    """
    enforced = getattr(identity, "requester_enforced", None)
    assert callable(enforced), "kernel identity has no requester_enforced()"
    assert "requester_enabled" in identity.HttpTurnCarry.__dataclass_fields__, (
        "HttpTurnCarry does not keep the requester flag read"
    )
    agent_id = uuid.UUID(str(SCOPE["agent_id"]))

    def kernel(flag: bool) -> Any:
        async def read(_agent: uuid.UUID) -> bool:
            return flag

        return SimpleNamespace(_binding=SimpleNamespace(connector_requester_identity_for=read))

    def carry(context: str | None = CAPSULE) -> Any:
        return identity.HttpTurnCarry(
            qevent("enforcement probe"),
            context,
            ORIGINAL_HASH,
            scope=TurnAuthorityScopeV1.model_validate(SCOPE),
        )

    async def go() -> None:
        assert enforced() is False
        opted_in = carry()
        token = identity.CURRENT.set(opted_in)
        try:
            assert opted_in.requester_enabled is False and enforced() is False
            assert await identity.requester_principal(kernel(True), agent_id) is None
            assert opted_in.requester_enabled is True
            assert enforced() is True
            opted_in.active = False
            assert enforced() is False
        finally:
            identity.CURRENT.reset(token)
        assert enforced() is False

        opted_out = carry()
        token = identity.CURRENT.set(opted_out)
        try:
            assert await identity.requester_principal(kernel(False), agent_id) is None
            assert opted_out.requester_enabled is False and enforced() is False
        finally:
            identity.CURRENT.reset(token)

        legacy = carry(context=None)
        legacy.requester_enabled = True
        token = identity.CURRENT.set(legacy)
        try:
            assert enforced() is False
        finally:
            identity.CURRENT.reset(token)

    asyncio.run(go())


# -- 1, 2: idle replacement, and the same-human steering control ---------------------------


def test_a_then_b_on_an_idle_thread_replaces_with_bobs_principal(make_harness) -> None:
    """@spec CONNECTOR-REQUESTER-3: Bob never adopts the sandbox Alice's token booted.

    The idle route is handed off through the existing handoff: a new claim
    whose cct names Bob, recorded on the route as Bob's digest and expiry.
    """
    require_execution_interface()
    require_requester_interface()

    async def run() -> None:
        binding, alice, bob, core = parties()
        async with make_harness(
            binding=binding, identity_authority=core, per_sandbox_runners=2
        ) as h:
            first = qevent("alice asks", thread="handoff-idle")
            await process(h, first)
            key = thread_key(first)
            alice_handle = route(h, key)
            assert recorded(alice_handle)[0] == requester_digest(alice)

            core.requester = bob
            second = qevent("bob asks", thread="handoff-idle")
            await process(h, second)

            assert len(h.fake_k8s.claim_envs) == 2, "Bob adopted Alice's sandbox"
            bob_cct = cct_of(h.fake_k8s.claim_envs[1])
            claims = cct_claims(bob_cct)
            assert claims["principal"] == principal_dict(bob)
            assert_signed_by(bob_cct, binding.signing_key)
            bob_handle = route(h, key)
            assert bob_handle.claim_name != alice_handle.claim_name
            assert recorded(bob_handle) == (requester_digest(bob), claims["exp"])
            assert runner_at(h, alice_handle).opened == ["alice asks"]
            assert runner_at(h, bob_handle).opened == ["bob asks"]
            assert all_steers(h) == []
            assert all_interrupts(h) == 0
            assert await is_done(h, second)

    asyncio.run(run())


def test_the_same_human_still_steers_a_live_turn(make_harness) -> None:
    """@spec CONNECTOR-REQUESTER-3 (control): same-requester steering continues."""
    require_execution_interface()
    require_requester_interface()

    async def run() -> None:
        binding, _alice, _bob, core = parties()
        async with make_harness(
            binding=binding, identity_authority=core, per_sandbox_runners=2
        ) as h:
            holds = hold_turns(h)
            opener = asyncio.create_task(process(h, qevent("alice opens", thread="same-human")))
            try:
                await wait_until(lambda: any_turn_active(h), "alice's turn is live")
                await process(h, qevent("alice follows up", thread="same-human"))
                assert all_steers(h) == ["alice follows up"]
                assert all_opened(h) == ["alice opens"]
                assert len(h.fake_k8s.claim_envs) == 1
                assert all_interrupts(h) == 0
            finally:
                release(holds)
                await opener

    asyncio.run(run())


# -- 3, 4, 5: a live turn of another requester -------------------------------------------


def test_a_then_b_during_a_live_turn_defers_and_never_steers(
    make_harness, caplog: pytest.LogCaptureFixture
) -> None:
    """@spec CONNECTOR-REQUESTER-3: Bob waits; he never enters Alice's live runner.

    The deferral raises before any steer or start and logs the event id and
    thread key only. Liveness: once Alice's turn ends, Bob's redelivery at
    generation 2 replaces the sandbox with his own principal and runs once,
    and nobody is interrupted on the way.
    """
    require_execution_interface()
    require_requester_interface()
    pending_cls = handoff_pending()
    caplog.set_level(logging.DEBUG, logger="curie_worker")

    async def run() -> None:
        binding, alice, bob, core = parties()
        async with make_harness(
            binding=binding, identity_authority=core, per_sandbox_runners=2, **_LEASE_KNOBS
        ) as h:
            store = await lease_store(h)
            holds = hold_turns(h)
            alice_turn = qevent("alice is working", thread="live-defer")
            key = thread_key(alice_turn)
            opener = asyncio.create_task(process(h, alice_turn))
            bob_turn = qevent("bob asks", thread="live-defer")
            try:
                await wait_until(lambda: any_turn_active(h), "alice's turn is live")
                alice_handle = route(h, key)
                alice_runner = runner_at(h, alice_handle)

                core.requester = bob
                with pytest.raises(pending_cls):
                    await process(h, bob_turn)

                assert all_steers(h) == [], "Bob's message was steered into Alice's turn"
                assert all_interrupts(h) == 0
                assert alice_runner.turn_active and not opener.done()
                assert all_opened(h) == ["alice is working"]
                assert len(h.fake_k8s.claim_envs) == 1
                assert route(h, key) == alice_handle
                assert not await is_done(h, bob_turn)

                messages = logged(caplog, _PENDING_LOG)
                assert messages, f"no {_PENDING_LOG!r} log line for the deferral"
                assert any(bob_turn.event_id in m and key in m for m in messages)
                tenant = str(binding.identity.tenant_id)
                for message in messages:
                    for private in (
                        "SyntheticAliceSubject",
                        BOB_SUBJECT,
                        ISSUER,
                        tenant,
                        "sha256:",
                    ):
                        assert private not in message, "the deferral log names the requester"
            finally:
                release(holds)
                await opener

            complete_turns(h)
            lease = await _leased_entry(h, store, event_id=bob_turn.event_id, generation=2)
            await process_leased(h, bob_turn, lease)

            bob_handle = route(h, key)
            assert bob_handle.claim_name != alice_handle.claim_name
            assert recorded(bob_handle)[0] == requester_digest(bob)
            assert cct_claims(cct_of(h.fake_k8s.claim_envs[-1]))["principal"] == (
                principal_dict(bob)
            )
            assert runner_at(h, bob_handle).opened == ["bob asks"]
            assert all_opened(h).count("bob asks") == 1
            assert all_steers(h) == []
            assert all_interrupts(h) == 0
            assert await is_done(h, bob_turn)
            assert recorded(alice_handle)[0] == requester_digest(alice)

    asyncio.run(run())


def test_a_reclaimed_delivery_never_interrupts_another_requesters_turn(
    make_harness, caplog: pytest.LogCaptureFixture
) -> None:
    """@spec CONNECTOR-REQUESTER-3: the reclaim preflight defers, never interrupts.

    Today's preflight interrupts any live turn before a generation 2 retry.
    Bob's reclaimed delivery meets Alice's live turn: no interrupt, no
    ``interrupting before retry`` line, a deferral. Control: Alice's own
    generation 2 redelivery still fences her live turn and then runs.
    """
    require_execution_interface()
    require_requester_interface()
    pending_cls = handoff_pending()
    caplog.set_level(logging.DEBUG, logger="curie_worker")

    async def run() -> None:
        binding, alice, bob, core = parties()
        async with make_harness(
            binding=binding, identity_authority=core, per_sandbox_runners=2, **_LEASE_KNOBS
        ) as h:
            store = await lease_store(h)
            holds = hold_turns(h)
            alice_turn = qevent("alice is working", thread="reclaim-guard")
            key = thread_key(alice_turn)
            opener = asyncio.create_task(process(h, alice_turn))
            try:
                await wait_until(lambda: any_turn_active(h), "alice's turn is live")
                alice_runner = runner_at(h, route(h, key))

                core.requester = bob
                bob_turn = qevent("bob retried", thread="reclaim-guard")
                lease = await _leased_entry(h, store, event_id=bob_turn.event_id, generation=2)
                with pytest.raises(pending_cls):
                    await process_leased(h, bob_turn, lease)

                assert alice_runner.interrupts == 0, "Bob's reclaim interrupted Alice's turn"
                assert alice_runner.turn_active and not opener.done()
                assert all_opened(h) == ["alice is working"]
                assert all_steers(h) == []
                assert logged(caplog, "interrupting before retry") == []
                assert not await is_done(h, bob_turn)

                core.requester = alice
                alice_retry = qevent("alice retried", thread="reclaim-guard")
                retry_lease = await _leased_entry(
                    h, store, event_id=alice_retry.event_id, generation=2
                )
                await process_leased(h, alice_retry, retry_lease)
                assert alice_runner.interrupts == 1
                assert alice_runner.opened == ["alice is working", "alice retried"]
                assert len(h.fake_k8s.claim_envs) == 1
                assert await is_done(h, alice_retry)
            finally:
                release(holds)
                await opener

    asyncio.run(run())


def test_unreadable_liveness_defers_a_different_requester(make_harness) -> None:
    """@spec CONNECTOR-REQUESTER-3: an unreadable runner counts as busy.

    Liveness: once the runner answers idle, the next delivery replaces.
    """
    require_execution_interface()
    require_requester_interface()
    pending_cls = handoff_pending()

    async def run() -> None:
        binding, _alice, bob, core = parties()
        async with make_harness(
            binding=binding, identity_authority=core, per_sandbox_runners=2
        ) as h:
            alice_turn = qevent("alice asks", thread="unreadable")
            await process(h, alice_turn)
            key = thread_key(alice_turn)
            alice_handle = route(h, key)
            alice_runner = runner_at(h, alice_handle)

            alice_runner.status_fails = True
            core.requester = bob
            bob_turn = qevent("bob asks", thread="unreadable")
            with pytest.raises(pending_cls):
                await process(h, bob_turn)
            assert all_opened(h) == ["alice asks"]
            assert all_steers(h) == []
            assert len(h.fake_k8s.claim_envs) == 1
            assert route(h, key) == alice_handle

            alice_runner.status_fails = False
            await process(h, bob_turn)
            bob_handle = route(h, key)
            assert len(h.fake_k8s.claim_envs) == 2
            assert recorded(bob_handle)[0] == requester_digest(bob)
            assert runner_at(h, bob_handle).opened == ["bob asks"]
            assert all_interrupts(h) == 0

    asyncio.run(run())


# -- 6: the post-claim check ----------------------------------------------------------------


def test_a_route_adopted_after_an_empty_snapshot_is_refused(
    make_harness, monkeypatch: pytest.MonkeyPatch
) -> None:
    """@spec CONNECTOR-REQUESTER-3: the claimed handle is checked, not only the snapshot.

    The route snapshot under the lock reads no route (``lookup`` answers None
    until the claim starts), so the claim adopts Alice's live sandbox. The
    post-claim comparison refuses it before any steer or start. Liveness: the
    next delivery sees the route and replaces.
    """
    require_execution_interface()
    require_requester_interface()
    pending_cls = handoff_pending()

    async def run() -> None:
        binding, _alice, bob, core = parties()
        async with make_harness(
            binding=binding, identity_authority=core, per_sandbox_runners=2
        ) as h:
            alice_turn = qevent("alice asks", thread="empty-snapshot")
            await process(h, alice_turn)
            key = thread_key(alice_turn)
            alice_handle = route(h, key)

            real_lookup = h.substrate.lookup
            real_claim = h.substrate.claim
            hidden = {"on": True, "count": 0, "claims": 0}

            def lookup(thread: str) -> Any:
                if hidden["on"] and thread == key:
                    hidden["count"] += 1
                    return None
                return real_lookup(thread)

            def claim(thread: str, **kwargs: Any) -> Any:
                hidden["on"] = False
                hidden["claims"] += 1
                return real_claim(thread, **kwargs)

            monkeypatch.setattr(h.substrate, "lookup", lookup)
            monkeypatch.setattr(h.substrate, "claim", claim)
            core.requester = bob
            bob_turn = qevent("bob asks", thread="empty-snapshot")
            with pytest.raises(pending_cls):
                await process(h, bob_turn)
            assert hidden["count"] >= 1 and hidden["claims"] == 1, "the claim never adopted"
            monkeypatch.setattr(h.substrate, "lookup", real_lookup)
            monkeypatch.setattr(h.substrate, "claim", real_claim)

            assert all_opened(h) == ["alice asks"], "a turn started on Alice's sandbox"
            assert all_steers(h) == []
            assert len(h.fake_k8s.claim_envs) == 1
            assert route(h, key) == alice_handle

            await process(h, bob_turn)
            bob_handle = route(h, key)
            assert len(h.fake_k8s.claim_envs) == 2
            assert recorded(bob_handle)[0] == requester_digest(bob)
            assert runner_at(h, bob_handle).opened == ["bob asks"]

    asyncio.run(run())


# -- 7: transitions -------------------------------------------------------------------------


def test_human_principal_less_human_replaces_each_time(make_harness) -> None:
    """@spec CONNECTOR-REQUESTER-3: human to principal-less and back each replace."""
    require_execution_interface()
    require_requester_interface()

    async def run() -> None:
        binding, alice, _bob, core = parties()
        async with make_harness(
            binding=binding, identity_authority=core, per_sandbox_runners=3
        ) as h:
            first = qevent("alice one", thread="transitions")
            key = thread_key(first)
            await process(h, first)
            human = route(h, key)
            assert recorded(human)[0] == requester_digest(alice)

            core.requester = None
            second = qevent("no human", thread="transitions")
            await process(h, second)
            assert len(h.fake_k8s.claim_envs) == 2, "a principal-less turn adopted a human's"
            assert "principal" not in cct_claims(cct_of(h.fake_k8s.claim_envs[1]))
            nobody = route(h, key)
            assert recorded(nobody)[0] == "none"

            core.requester = alice
            third = qevent("alice two", thread="transitions")
            await process(h, third)
            assert len(h.fake_k8s.claim_envs) == 3, "a human adopted a principal-less sandbox"
            again = route(h, key)
            assert recorded(again)[0] == requester_digest(alice)
            assert len({human.claim_name, nobody.claim_name, again.claim_name}) == 3
            assert runner_at(h, human).opened == ["alice one"]
            assert runner_at(h, nobody).opened == ["no human"]
            assert runner_at(h, again).opened == ["alice two"]
            assert all_steers(h) == []

    asyncio.run(run())


def test_disabling_the_flag_replaces_a_principal_bearing_sandbox(make_harness) -> None:
    """@spec CONNECTOR-REQUESTER-3: the digest rule applies without the flag."""
    require_execution_interface()
    require_requester_interface()

    async def run() -> None:
        binding, alice, _bob, core = parties()
        async with make_harness(
            binding=binding, identity_authority=core, per_sandbox_runners=2
        ) as h:
            first = qevent("alice asks", thread="flag-off")
            key = thread_key(first)
            await process(h, first)
            assert recorded(route(h, key))[0] == requester_digest(alice)

            binding.flag = False
            core.version = 1
            core.requester = None
            second = qevent("after opt-out", thread="flag-off")
            await process(h, second)
            assert len(h.fake_k8s.claim_envs) == 2, "a flag-off turn adopted a human's sandbox"
            assert "principal" not in cct_claims(cct_of(h.fake_k8s.claim_envs[1]))
            handle = route(h, key)
            assert recorded(handle)[0] == "none"
            assert runner_at(h, handle).opened == ["after opt-out"]

    asyncio.run(run())


def test_a_job_turn_never_adopts_a_humans_sandbox(make_harness) -> None:
    """@spec CONNECTOR-REQUESTER-3: a machine turn (here a webhook job) replaces."""
    require_execution_interface()
    require_requester_interface()

    async def run() -> None:
        binding, alice, _bob, core = parties()
        async with make_harness(
            binding=binding, identity_authority=core, per_sandbox_runners=2
        ) as h:
            first = qevent("alice asks", thread="job-turn")
            key = thread_key(first)
            await process(h, first)
            human = route(h, key)
            assert recorded(human)[0] == requester_digest(alice)

            core.requester = None
            job = qevent("webhook payload", thread="job-turn", source=TurnSource.WEBHOOK)
            await process(h, job)
            assert len(h.fake_k8s.claim_envs) == 2, "a job adopted a human's sandbox"
            machine = route(h, key)
            assert recorded(machine)[0] == "none"
            assert runner_at(h, human).opened == ["alice asks"]
            assert runner_at(h, machine).opened == ["webhook payload"]

    asyncio.run(run())


# -- 8, 9: unrecorded and token-free routes -------------------------------------------------


def test_an_unrecorded_route_cannot_prove_a_match_when_enforced(make_harness) -> None:
    """@spec CONNECTOR-REQUESTER-3: missing old metadata cannot prove a match.

    A route record written before this change (no requester keys) is
    replaced once by an opted-in turn, even a V2-null one; the record it
    writes then adopts. A flag-off turn adopts a legacy record as today.
    """
    require_execution_interface()
    require_requester_interface()

    async def run() -> None:
        binding, _alice, _bob, _core = parties()
        core = RequesterCore(None)
        async with make_harness(
            binding=binding, identity_authority=core, per_sandbox_runners=2
        ) as h:
            first = qevent("opted in, no human", thread="legacy-route")
            key = thread_key(first)
            await process(h, first)
            legacy = route(h, key)
            assert recorded(legacy)[0] == "none"
            strip_requester_record(h, key)
            assert recorded(route(h, key)) == (None, None)

            await process(h, qevent("enforced", thread="legacy-route"))
            assert len(h.fake_k8s.claim_envs) == 2, "an unrecorded route proved a match"
            replaced = route(h, key)
            assert replaced.claim_name != legacy.claim_name
            assert recorded(replaced)[0] == "none"

            await process(h, qevent("enforced again", thread="legacy-route"))
            assert len(h.fake_k8s.claim_envs) == 2, "a recorded route was replaced again"

            strip_requester_record(h, key)
            binding.flag = False
            core.version = 1
            await process(h, qevent("flag off", thread="legacy-route"))
            assert len(h.fake_k8s.claim_envs) == 2, "a flag-off turn replaced a legacy route"
            assert route(h, key).claim_name == replaced.claim_name
            assert runner_at(h, legacy).opened == ["opted in, no human"]
            assert runner_at(h, replaced).opened == ["enforced", "enforced again", "flag off"]

    asyncio.run(run())


def test_an_opted_in_turn_without_a_caller_token_starts_and_is_adopted(make_harness) -> None:
    """@spec CONNECTOR-REQUESTER-3 [D1]: a token-free boot records ``"absent"``.

    The slice 1 binding boots no cct (a keyless install). Every boot records a
    fact, never None, so the same requester's next turn adopts.
    """
    require_execution_interface()
    require_requester_interface()

    async def run() -> None:
        binding = RequesterBinding(flag=True)
        core = RequesterCore(binding.requester_wire())
        async with make_harness(binding=binding, identity_authority=core) as h:
            first = qevent("alice one", thread="no-cct")
            key = thread_key(first)
            await process(h, first)
            assert recorded(route(h, key)) == ("absent", None)

            await process(h, qevent("alice two", thread="no-cct"))
            assert h.runner.opened == ["alice one", "alice two"]
            assert len(h.fake_k8s.claim_envs) == 1
            assert CONNECTOR_CALLER_TOKEN_ENV not in (h.fake_k8s.claim_envs[0] or {})
            assert recorded(route(h, key)) == ("absent", None)

    asyncio.run(run())


# -- 10: the consumer keeps a deferred delivery ---------------------------------------------


def test_a_deferred_delivery_stays_pending_then_runs_once(make_harness) -> None:
    """@spec CONNECTOR-REQUESTER-3: deferred, never lost, never run twice.

    The actual consumer handles Bob's entry while Alice's turn is live: it
    stays in the PEL with no done marker. After her turn ends the entry is
    reclaimed (generation 2), replaces the sandbox, runs once and is acked.
    """
    require_execution_interface()
    require_requester_interface()

    async def run() -> None:
        binding, _alice, bob, core = parties()
        async with make_harness(
            binding=binding, identity_authority=core, per_sandbox_runners=2, **_LEASE_KNOBS
        ) as h:
            leases = DeliveryLeaseStore(h.async_redis, h.config)
            consumer = Consumer(
                redis=h.async_redis,
                kernel=h.kernel,
                config=h.config,
                leases=leases,
            )
            holds = hold_turns(h)
            alice_turn = qevent("alice is working", thread="consumer-defer")
            key = thread_key(alice_turn)
            opener = asyncio.create_task(process(h, alice_turn))
            bob_turn = qevent("bob asks", thread="consumer-defer")
            try:
                await wait_until(lambda: any_turn_active(h), "alice's turn is live")
                alice_handle = route(h, key)
                core.requester = bob
                fields = {
                    "payload": bob_turn.model_dump_json(),
                    "curie_identity_context_v1": CAPSULE,
                }
                entry_id, observed = await pending_entry(h, consumer, fields)
                await consumer._handle(entry_id, observed)

                assert "bob asks" not in all_opened(h)
                assert all_steers(h) == []
                assert all_interrupts(h) == 0
                assert await pending_count(h) == 1
                assert not await is_done(h, bob_turn)
                assert route(h, key) == alice_handle
            finally:
                release(holds)
                await opener

            complete_turns(h)
            # Idle time zero claims the pending entry at once; the test needs
            # the redelivery, not a reclaim delay.
            await h.async_redis.xclaim(
                h.config.stream,
                h.config.consumer_group,
                h.config.consumer_name,
                min_idle_time=0,
                message_ids=[entry_id],
            )
            await consumer._handle(entry_id, observed)

            assert all_opened(h).count("bob asks") == 1
            bob_handle = route(h, key)
            assert runner_at(h, bob_handle).opened == ["bob asks"]
            assert recorded(bob_handle)[0] == requester_digest(bob)
            assert all_interrupts(h) == 0
            assert await is_done(h, bob_turn)
            assert await pending_count(h) == 0

    asyncio.run(run())


# -- 11: the boot guard ---------------------------------------------------------------------


# 3600 s requester window, less the 60 s boot grace and the 90 s claim reserve.
_REQUESTER_CAP_S = 3450.0


@pytest.mark.parametrize(
    ("runner_total_s", "human", "capped"),
    [
        (10800.0, True, True),
        (3500.0, True, True),
        (600.0, True, False),
        (10800.0, False, False),
    ],
    ids=["human-chart-default-10800s", "human-3500s", "control-600s", "principal-less-10800s"],
)
def test_an_opted_in_turn_is_capped_at_the_requester_window_and_boots(
    make_harness,
    caplog: pytest.LogCaptureFixture,
    runner_total_s: float,
    human: bool,
    capped: bool,
) -> None:
    """@spec CONNECTOR-REQUESTER-3: bound the opted-in execution to the window.

    The chart's default runner bound is 10800 s. An opted-in human turn under
    it boots with its principal, and its boot token and runner stream deadline
    are capped at 3600 - 60 grace - 90 claim = 3450 s instead of refusing. A
    600 s bound already fits and is unchanged; a principal-less turn is not
    capped at all.
    """
    require_execution_interface()
    require_requester_interface()
    caplog.set_level(logging.INFO)

    async def run() -> None:
        binding = MintingRequesterBinding(flag=True)
        core = RequesterCore(binding.requester_wire() if human else None)
        async with make_harness(
            binding=binding,
            identity_authority=core,
            claim_timeout_seconds=90.0,
            runner_total_timeout_s=runner_total_s,
            delivery_budget_s=runner_total_s,
        ) as h:
            event = qevent("a long turn", thread="window-cap")
            await process(h, event)
            assert binding.flag_reads
            assert len(binding.boot_kwargs) == 1, "the turn was refused"
            expected = principal_for(binding.requester_wire()) if human else None
            assert binding.boot_kwargs[0].get("caller_principal") == expected
            ttl = float(binding.boot_kwargs[0]["token_ttl_s"])
            bounded = logged(caplog, "bounded to 3450s by the requester token window")
            if capped:
                assert ttl == _REQUESTER_CAP_S
                assert bounded, "the cap was applied silently"
                assert logged(caplog, "effective timeout 3450.000s")
            else:
                assert runner_total_s - 30 < ttl <= runner_total_s
                assert not bounded
            assert len(h.fake_k8s.claim_envs) == 1
            assert h.runner.opened == [event.text]
            assert await is_done(h, event)

    asyncio.run(run())


def test_a_claim_reserve_that_leaves_no_window_refuses_before_boot(make_harness) -> None:
    """@spec CONNECTOR-REQUESTER-3: a bound that cannot be lowered still refuses.

    A claim reserve of 3545 s plus the 60 s boot grace leaves no part of the
    one-hour window, and the runner's minimum request time cannot go below
    zero, so the opted-in turn refuses before any token is minted. No opted-in
    turn runs longer than the window.
    """
    require_execution_interface()
    require_requester_interface()

    async def run() -> None:
        binding = MintingRequesterBinding(flag=True)
        core = RequesterCore(binding.requester_wire())
        async with make_harness(
            binding=binding,
            identity_authority=core,
            claim_timeout_seconds=3545.0,
            runner_total_timeout_s=600.0,
            delivery_budget_s=600.0,
        ) as h:
            event = qevent("no window left", thread="window-none")
            await process(h, event)
            assert binding.flag_reads
            assert binding.boot_kwargs == [], "a token was minted for a refused turn"
            assert_refused_before_boot(h, binding)
            assert h.sink.events == []
            assert await is_done(h, event)

    asyncio.run(run())


def test_the_window_ends_a_long_turn_visibly_and_never_retries(
    make_harness, caplog: pytest.LogCaptureFixture
) -> None:
    """@spec CONNECTOR-REQUESTER-3: the cap ends the turn like a deadline, visibly.

    A 3539.5 s claim reserve leaves a 0.5 s window, so a turn that keeps
    running is stopped there. The outcome is ``requester-window``: shown to the
    requester with its guidance, logged at INFO, and not retried, because a
    retry would run the same request past the window on a fresh token.
    """
    require_execution_interface()
    require_requester_interface()
    caplog.set_level(logging.INFO)

    async def run() -> None:
        binding = MintingRequesterBinding(flag=True)
        core = RequesterCore(binding.requester_wire())
        async with make_harness(
            binding=binding,
            identity_authority=core,
            claim_timeout_seconds=3539.5,
            runner_total_timeout_s=600.0,
            delivery_budget_s=600.0,
            max_attempts=3,
        ) as h:
            hold = asyncio.Event()
            h.runner.hold = hold
            h.runner.timeout_status = 200
            # No Final: the turn is still working when the window ends it.
            h.runner.default_script = [TextDelta(text="working")]
            event = qevent("keep working", thread="window-end")
            try:
                await process(h, event)
            finally:
                hold.set()
            assert len(binding.boot_kwargs) == 1
            assert h.runner.opened == [event.text], "the capped turn was retried"
            assert h.runner.timeout_calls == 1
            assert logged(caplog, "ended at the requester token window")
            assert h.sink.last_text is not None
            assert "requester-window" in h.sink.last_text
            assert "one-hour limit" in h.sink.last_text
            assert await is_done(h, event)

    asyncio.run(run())


# -- 13: the reclaim interrupt is fenced to the checked route [D3] --------------------------


async def _two_live_requesters(h: Any, core: Any, alice: Any, bob: Any) -> tuple[Any, ...]:
    """Bob live on one sandbox (thread B), Alice live on another (thread A).

    @spec CONNECTOR-REQUESTER-3.
    """
    core.requester = bob
    bob_turn = qevent("bob is working", thread="fence-b")
    bob_task = asyncio.create_task(process(h, bob_turn))
    await wait_until(lambda: any_turn_active(h), "bob's turn is live")
    bob_key = thread_key(bob_turn)
    bob_handle = route(h, bob_key)

    core.requester = alice
    alice_turn = qevent("alice is working", thread="fence-a")
    alice_task = asyncio.create_task(process(h, alice_turn))
    await wait_until(
        lambda: sum(runner.turn_active for runner in h.runners.values()) == 2,
        "alice's turn is live beside bob's",
    )
    alice_key = thread_key(alice_turn)
    alice_handle = route(h, alice_key)
    assert alice_handle.port != bob_handle.port
    return bob_task, alice_task, bob_key, bob_handle, alice_key, alice_handle


def test_reclaim_preflight_never_interrupts_a_route_handed_to_another_requester(
    make_harness,
) -> None:
    """@spec CONNECTOR-REQUESTER-3 [D3]: the interrupt is fenced to the checked route.

    Bob's reclaimed delivery checks his own live turn on runner 1, then,
    between that check and the interrupt, the route is handed to Alice's
    live sandbox on runner 2. The fenced interrupt sees the changed route and
    signals nothing; the restarted check meets Alice and defers.
    """
    require_execution_interface()
    require_requester_interface()
    pending_cls = handoff_pending()

    async def run() -> None:
        binding, alice, bob, core = parties()
        async with make_harness(
            binding=binding, identity_authority=core, per_sandbox_runners=2, **_LEASE_KNOBS
        ) as h:
            store = await lease_store(h)
            holds = hold_turns(h)
            tasks: list[asyncio.Task[None]] = []
            try:
                (
                    bob_task,
                    alice_task,
                    bob_key,
                    bob_handle,
                    alice_key,
                    alice_handle,
                ) = await _two_live_requesters(h, core, alice, bob)
                tasks = [bob_task, alice_task]
                bob_runner, alice_runner = runner_at(h, bob_handle), runner_at(h, alice_handle)

                core.requester = bob
                retried = qevent("bob retried", thread="fence-b")
                lease = await _leased_entry(h, store, event_id=retried.event_id, generation=2)
                real_turn_active = h.kernel._turn_active
                checked: list[Any] = []

                async def turn_active(handle: Any, **kwargs: Any) -> bool:
                    live = bool(await real_turn_active(handle, **kwargs))
                    if live and not checked:
                        checked.append(handle)
                        hand_route(h, source_key=alice_key, target_key=bob_key)
                    return live

                h.kernel._turn_active = turn_active
                with pytest.raises(pending_cls):
                    await process_leased(h, retried, lease)

                assert checked == [bob_handle], "the preflight never checked Bob's live turn"
                assert bob_runner.interrupts == 0
                assert alice_runner.interrupts == 0, "the interrupt followed a fresh lookup"
                assert bob_runner.turn_active and alice_runner.turn_active
                assert "bob retried" not in all_opened(h)
                assert all_steers(h) == []
            finally:
                release(holds)
                await asyncio.gather(*tasks, return_exceptions=True)

    asyncio.run(run())


def test_reclaim_preflight_interrupts_the_checked_route_when_it_is_unchanged(
    make_harness,
) -> None:
    """@spec CONNECTOR-REQUESTER-3 [D3] (control): no swap, the checked turn is fenced.

    Bob's reclaimed delivery interrupts his own live turn on runner 1, with
    that sandbox's token, once; then runs once after it goes idle. Alice's
    turn on runner 2 is untouched.
    """
    require_execution_interface()
    require_requester_interface()

    async def run() -> None:
        binding, alice, bob, core = parties()
        async with make_harness(
            binding=binding, identity_authority=core, per_sandbox_runners=2, **_LEASE_KNOBS
        ) as h:
            store = await lease_store(h)
            holds = hold_turns(h)
            tasks: list[asyncio.Task[None]] = []
            try:
                (
                    bob_task,
                    alice_task,
                    _bob_key,
                    bob_handle,
                    _alice_key,
                    alice_handle,
                ) = await _two_live_requesters(h, core, alice, bob)
                tasks = [bob_task, alice_task]
                bob_runner, alice_runner = runner_at(h, bob_handle), runner_at(h, alice_handle)

                core.requester = bob
                retried = qevent("bob retried", thread="fence-b")
                lease = await _leased_entry(h, store, event_id=retried.event_id, generation=2)
                await process_leased(h, retried, lease)

                assert bob_runner.interrupts == 1
                assert bearer(bob_runner.interrupt_headers[-1]) == f"Bearer {bob_handle.token}"
                assert alice_runner.interrupts == 0
                assert alice_runner.turn_active
                assert bob_runner.opened == ["bob is working", "bob retried"]
                assert all_opened(h).count("bob retried") == 1
                assert await is_done(h, retried)
            finally:
                release(holds)
                await asyncio.gather(*tasks, return_exceptions=True)

    asyncio.run(run())


def test_interrupt_thread_with_expected_refuses_a_changed_route(make_harness) -> None:
    """@spec CONNECTOR-REQUESTER-3 [D3]: ``interrupt_thread(..., expected=handle)``.

    An equal route signals the expected sandbox with its own token. A route
    naming another sandbox, a later generation, or another requester returns
    False and sends nothing.
    """
    require_execution_interface()
    require_requester_interface()

    async def run() -> None:
        binding, _alice, bob, core = parties()
        async with make_harness(
            binding=binding, identity_authority=core, per_sandbox_runners=2
        ) as h:
            one = qevent("one", thread="expected-one")
            two = qevent("two", thread="expected-two")
            await process(h, one)
            await process(h, two)
            key_one, key_two = thread_key(one), thread_key(two)
            first, second = route(h, key_one), route(h, key_two)
            runner_one, runner_two = runner_at(h, first), runner_at(h, second)
            assert runner_one is not runner_two

            assert await h.kernel.interrupt_thread(key_one, "unit", expected=first) is True
            assert runner_one.interrupts == 1 and runner_two.interrupts == 0
            assert bearer(runner_one.interrupt_headers[-1]) == f"Bearer {first.token}"

            original = route_payload(h, key_one)
            other_sandbox = {**route_payload(h, key_two), "thread_key": key_one}
            later_generation = {**original, "generation": int(original["generation"]) + 1}
            other_requester = {**original, "caller_requester": requester_digest(bob)}
            for changed in (other_sandbox, later_generation, other_requester):
                write_route_payload(h, key_one, changed)
                assert await h.kernel.interrupt_thread(key_one, "unit", expected=first) is False
                assert runner_one.interrupts == 1 and runner_two.interrupts == 0
                write_route_payload(h, key_one, original)
            assert route(h, key_one) == first

    asyncio.run(run())


# -- 14, 15: the admission window [D4] ------------------------------------------------------


@pytest.mark.parametrize(
    ("claim_delay_s", "capped"),
    [(200.0, True), (30.0, False)],
    ids=["slow-claim", "control-30s-claim"],
)
def test_a_slow_first_claim_caps_the_turn_instead_of_refusing(
    make_harness,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    claim_delay_s: float,
    capped: bool,
) -> None:
    """@spec CONNECTOR-REQUESTER-3 [D4]: admission caps at what the token has left.

    Runner bound 3400 s. The first claim spends 200 s after the minter ran, so
    the claim-time token (exp mint + 3600) leaves 3340 s after the 60 s grace,
    beyond the 90 s claim reserve. The turn starts on attempt 1, on that one
    sandbox, bounded to the token's remaining window rather than refused.
    Control: a 30 s claim leaves more than the bound, so nothing is capped.
    """
    require_execution_interface()
    require_requester_interface()
    clock = install_clock(monkeypatch)
    caplog.set_level(logging.DEBUG, logger="curie_worker")

    async def run() -> None:
        binding, alice, _bob, core = parties()
        async with make_harness(
            binding=binding, identity_authority=core, per_sandbox_runners=2, **_LONG_TURN_KNOBS
        ) as h:
            delays = [claim_delay_s]
            delay_claims(h, clock, lambda: delays.pop(0) if delays else 0.0)
            outcomes = observe_starts(h)
            event = qevent("alice's long turn", thread="slow-claim")
            await process(h, event)

            envs = h.fake_k8s.claim_envs
            assert len(binding.boot_ccts) == 1, "the boot env was rebuilt per attempt"
            assert len(envs) == 1, "a slow claim was replaced instead of capped"
            first = cct_claims(cct_of(envs[0]))
            assert first["principal"] == principal_dict(alice)
            assert cct_of(envs[0]) == binding.minter_runs[0].cct
            assert outcomes == ["started"]
            assert not logged(caplog, "requester_token_window_short")
            bounded = logged(caplog, "by the requester token window")
            if capped:
                assert bounded, "the slow claim's cap was applied silently"
                # 3600 - 200 claim - 60 grace, less the little the start takes.
                assert any("bounded to 33" in line for line in bounded), bounded
            else:
                assert not bounded
            handle = route(h, thread_key(event))
            assert recorded(handle) == (requester_digest(alice), first["exp"])
            assert runner_at(h, handle).opened == [event.text]
            assert all_opened(h) == [event.text]
            assert await is_done(h, event)

    asyncio.run(run())


def test_a_claim_leaving_under_the_minimum_attempt_never_starts(
    make_harness, monkeypatch: pytest.MonkeyPatch
) -> None:
    """@spec CONNECTOR-REQUESTER-3 [D4]: a window too small to lower into refuses.

    Every claim spends 3537 s of its token's hour, leaving 3 s after the 60 s
    grace, under the 5 s minimum attempt budget. Each attempt is refused before
    any runner request (``RequesterTokenWindowShort``) and replaces the
    sandbox; no turn ever starts and the delivery ends visibly.
    """
    require_execution_interface()
    require_requester_interface()
    short_cls = window_short()
    clock = install_clock(monkeypatch)

    async def run() -> None:
        binding, _alice, _bob, core = parties()
        async with make_harness(
            binding=binding, identity_authority=core, per_sandbox_runners=3, **_LONG_TURN_KNOBS
        ) as h:
            delay_claims(h, clock, lambda: 3537.0)
            outcomes = observe_starts(h)
            event = qevent("never fits", thread="always-short")
            await process(h, event)

            attempts = h.config.max_attempts
            assert all_opened(h) == [], "a turn started on a token with no window left"
            assert outcomes == [short_cls] * attempts
            assert len(h.fake_k8s.claim_envs) == attempts
            assert await is_done(h, event)
            assert any("attempt(s)" in text for _address, _ref, text in h.sink.updates), (
                "the delivery did not escalate"
            )

    asyncio.run(run())


@pytest.mark.parametrize(
    ("backoff_advance_s", "reboots"),
    [(3000.0, True), (5.0, False)],
    ids=["retry-after-3000s", "control-5s-retry"],
)
def test_a_retry_long_after_mint_reboots_with_a_fresh_requester_token(
    make_harness, monkeypatch: pytest.MonkeyPatch, backoff_advance_s: float, reboots: bool
) -> None:
    """@spec CONNECTOR-REQUESTER-3 [D4]: the retry compares against a fresh need.

    Attempt 1's start fails (HTTP 500 before any turn). The backoff spends
    3000 s, so the warm token (mint + 3600) cannot cover the default 600 s
    turn: attempt 2 replaces the sandbox with a claim-time token and runs once.
    Control: a 5 s backoff adopts the same sandbox.
    """
    require_execution_interface()
    require_requester_interface()
    short_cls = window_short()
    clock = install_clock(monkeypatch)

    async def run() -> None:
        binding, alice, _bob, core = parties()
        async with make_harness(
            binding=binding, identity_authority=core, per_sandbox_runners=2
        ) as h:
            first_runner = next(iter(h.runners.values()))
            first_runner.event_fail_times = 1
            advanced = advance_on_first_backoff(h, clock, backoff_advance_s)
            outcomes = observe_starts(h)
            event = qevent("alice retries", thread="late-retry")
            await process(h, event)

            assert advanced == [backoff_advance_s], "the delivery never retried"
            assert outcomes[-1] == "started" and short_cls not in outcomes
            envs = h.fake_k8s.claim_envs
            handle = route(h, thread_key(event))
            assert len(binding.boot_ccts) == 1
            if reboots:
                assert len(envs) == 2, "a token minted 3000 s ago was reused"
                first, second = cct_claims(cct_of(envs[0])), cct_claims(cct_of(envs[1]))
                assert canonical(second["principal"]) == canonical(principal_dict(alice))
                assert canonical(second["principal"]) == canonical(first["principal"])
                assert second["exp"] >= first["exp"] + int(backoff_advance_s) - 1
                run = binding.minter_runs[1]
                assert run.cct == cct_of(envs[1])
                assert int(run.before) + _REQUESTER_TTL_S <= second["exp"]
                assert second["exp"] <= int(run.after) + _REQUESTER_TTL_S
                assert runner_at(h, handle) is not first_runner
                assert first_runner.queried == []
                assert runner_at(h, handle).queried == [event.text]
            else:
                assert len(envs) == 1
                assert runner_at(h, handle) is first_runner
                assert first_runner.opened == [event.text, event.text]
                assert first_runner.queried == [event.text]
            assert sum(len(runner.queried) for runner in h.runners.values()) == 1
            assert await is_done(h, event)

    asyncio.run(run())


def test_principal_less_turns_skip_the_window_check(
    make_harness, monkeypatch: pytest.MonkeyPatch
) -> None:
    """@spec CONNECTOR-REQUESTER-3 [D4]: only a digest handle is window-checked.

    Flag off. The double mints the principal-less cct with a one-hour expiry,
    so after a 200 s claim it really is shorter than start + 3400 + 60; the
    turn still starts on attempt 1 because a ``"none"`` handle skips the check.
    """
    require_execution_interface()
    require_requester_interface()
    clock = install_clock(monkeypatch)

    async def run() -> None:
        binding = MintingRequesterBinding(flag=False, principal_less_ttl_s=_REQUESTER_TTL_S)
        core = RequesterCore(None, version=1)
        async with make_harness(
            binding=binding, identity_authority=core, per_sandbox_runners=2, **_LONG_TURN_KNOBS
        ) as h:
            delay_claims(h, clock, lambda: 200.0)
            outcomes = observe_starts(h)
            event = qevent("no human, long turn", thread="principal-less")
            await process(h, event)

            assert outcomes == ["started"]
            assert len(h.fake_k8s.claim_envs) == 1
            claims = cct_claims(cct_of(h.fake_k8s.claim_envs[0]))
            assert "principal" not in claims
            assert claims["exp"] < int(clock.time()) + 3400 + 60, "the window was not short"
            assert binding.boot_kwargs[0].get("caller_principal") is None
            assert recorded(route(h, thread_key(event)))[0] == "none"
            assert all_opened(h) == [event.text]

    asyncio.run(run())
