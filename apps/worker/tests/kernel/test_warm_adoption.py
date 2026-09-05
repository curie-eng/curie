"""Warm bind through the kernel: claim env-free, establish authority, adopt, settle.

ADR-0116 d2 / ADR-0122 realized end to end inside the worker: with a warm-bind
policy wired, a non-workspace claim binds a pool pod with NO identity env, the
per-conversation credential is on the route as PENDING before the first event,
the runner must prove by behavior that it is a bootstrap-mode adopter before
that event is sent, the adopting turn is acked on every frame, and the route is
settled through the store's fenced predicate. Real Valkey, the real substrate
over the fake control plane, and either the REAL runner app (its model seam
faked) or a scriptable adopting fake for the failure shapes.

Without a policy the kernel is byte-for-byte the cold path, proven here too.

The "old or open runner" negative uses the suite's ordinary ``FakeRunner``,
which ignores adoption entirely and answers 200 to any status bearer. That is an
open-mode SURROGATE on current code, not an execution of a pre-adoption runner
image; the actual old-server negative remains an activation obligation.
"""

from __future__ import annotations

import asyncio
import uuid
from typing import Any

import pytest
from aci_protocol import Final, QueuedTurn, ReplyHandle, SessionStatus, TextDelta
from aiohttp import web
from curie_runner import RunTracer, SideEffectClassifier
from curie_runner.fake import FakeModelSession
from curie_runner.history import ConversationReplay, NullTranscriptStore
from curie_runner.server import bind_status_attestation, create_app
from curie_runner.session import SessionRunner
from curie_worker.behaviorpacks import BehaviorPacks
from curie_worker.binding import BUDGET_ENV, PLUGIN_DIR_ENV, ResolvedDeployment
from curie_worker.runner_client import RunnerClient
from curie_worker.sandbox import HISTORY_ENV, SESSION_ENV
from curie_worker.sandbox.types import AdoptionState, RouteRecord, RouteState
from curie_worker.warm_bind import ADOPTION_UNCONFIRMED

from .conftest import FakeRunner

DONE = SessionStatus.DONE
_BOOT = "pool-bootstrap-credential-0123456789"
_BOOTSTRAP_ONLY = "bootstrap credential permits adoption only"
_CHANNEL = "C1"
_THREAD = "th-warm"
_THREAD_KEY = f"slack:{_CHANNEL}:{_THREAD}"
_SESSION = f"agent-test-{_THREAD_KEY}"
_HISTORY = "http://api.example.com/agents/acme/state/transcript/th-warm"


def _qevent(text: str, *, thread: str = _THREAD) -> QueuedTurn:
    return QueuedTurn(
        event_id=uuid.uuid4().hex,
        conversation_id=thread,
        author="U1",
        text=text,
        reply_handle=ReplyHandle(kind="slack", channel=_CHANNEL, placeholder="p-1"),
        received_at="2026-07-05T00:00:00+00:00",
    )


class _Binding:
    """A resolver whose boot env carries the bound identity the cold path injects."""

    def __init__(self) -> None:
        self.agent_id = uuid.uuid4()

    async def resolve(self, kind: str, address: str) -> ResolvedDeployment | None:
        return ResolvedDeployment(
            agent_id=self.agent_id,
            agent_name="test-agent",
            version_id=uuid.uuid4(),
            version_label="v1",
            bundle_ref="bundles/x.zip",
            max_usd_per_day=None,
            max_output_tokens_per_run=None,
        )

    async def undeployed_binding(self, kind: str, address: str) -> Any | None:
        return None

    def packs_for(self, resolved: ResolvedDeployment) -> BehaviorPacks:
        return BehaviorPacks.from_config(resolved.behavior_packs)

    def boot_env(self, resolved: ResolvedDeployment, thread_key: str, **_: Any) -> dict[str, str]:
        return {
            BUDGET_ENV: '{"max_output_tokens_per_run":100000,"max_usd_per_day":10.0}',
            PLUGIN_DIR_ENV: "/bundles/current",
            SESSION_ENV: f"agent-test-{thread_key}",
            HISTORY_ENV: _HISTORY,
        }


class _Pool:
    """The warm-bind policy: this pool is booted in bootstrap mode."""

    def __init__(self, bootstrap: str | None = _BOOT) -> None:
        self.bootstrap = bootstrap
        self.asked: list[str] = []

    def bootstrap_for(
        self, thread_key: str, *, boot_env: dict[str, str], agent_name: str | None
    ) -> str | None:
        self.asked.append(thread_key)
        return self.bootstrap


async def _wait_applied(h: Any, *, thread_key: str = _THREAD_KEY) -> None:
    """Wait until the route reads APPLIED: the kernel writes it after the runner's
    200, on a worker thread, so ``turn_active`` on the fake can lead it."""

    for _ in range(300):
        live = h.substrate.lookup(thread_key)
        if live is not None and live.adoption_state is AdoptionState.APPLIED:
            return
        await asyncio.sleep(0.01)
    raise AssertionError("route never read APPLIED")


class _RealRunnerApp:
    """The real runner app in bootstrap mode over the fake model seam."""

    def __init__(self) -> None:
        self.sessions: list[FakeModelSession] = []
        self.loads: list[tuple[str, str | None]] = []
        app_self = self

        class Binder:
            async def load(self, session_id: str, history_ref: str | None) -> tuple[Any, Any, Any]:
                app_self.loads.append((session_id, history_ref))
                return NullTranscriptStore(), ConversationReplay(), None

            def rebind(self, session_id: str, replay: ConversationReplay) -> None:
                return None

        def factory() -> FakeModelSession:
            fake = FakeModelSession()
            self.sessions.append(fake)
            return fake

        self.runner = SessionRunner(
            session_factory=factory,
            ceiling=0,
            tracer=RunTracer(None),
            classifier=SideEffectClassifier(),
            trace_name="curie-run:warm-unbound",
            session_id="warm-unbound",
            conversation_binder=Binder(),
        )
        bind_status_attestation(self.runner, session_id="warm-unbound", sandbox_id="sbx", cwd=None)
        self.app = create_app(self.runner, token=None, bootstrap_token=_BOOT)


class _AdoptingFake(FakeRunner):
    """A scriptable bootstrap-mode adopter for the failure shapes.

    ``apply`` controls whether the adopting event actually binds: when True the
    frames carry ``adoption_applied: true`` and the status route attests the
    adopted session under the conversation credential; when False the frames
    carry no ack and the conversation credential is refused (401), the shape of
    a pod whose binding never became active.
    """

    def __init__(self, *, apply: bool = True, stamp: bool = True) -> None:
        super().__init__()
        self.apply = apply
        self.stamp = stamp
        self.conversation: str | None = None
        self.adopted: str | None = None
        self.adopting_bodies: list[dict[str, Any]] = []

    @staticmethod
    def _bearer(request: web.Request) -> str | None:
        auth = request.headers.get("Authorization", "")
        return auth.removeprefix("Bearer ").strip() or None

    async def _status(self, request: web.Request) -> web.Response:
        bearer = self._bearer(request)
        if self.adopted is None and bearer == _BOOT:
            return web.json_response({"error": _BOOTSTRAP_ONLY}, status=403)
        if self.adopted is not None and bearer == self.conversation:
            return web.json_response(
                {"status": "idle-awaiting-input", "turn_active": False, "session_id": self.adopted}
            )
        return web.json_response({"error": "unauthorized"}, status=401)

    async def _event(self, request: web.Request) -> web.StreamResponse:
        body = await request.json()
        if body.get("adoption_credential") is not None:
            self.adopting_bodies.append(body)
            if self.apply:
                self.conversation = body["adoption_credential"]
                self.adopted = body["session_id"]
            if self.apply and self.stamp:
                self.default_script = [
                    frame.model_copy(update={"adoption_applied": True})
                    for frame in self.default_script
                ]
                self.tail = [
                    frame.model_copy(update={"adoption_applied": True}) for frame in self.tail
                ]
        return await super()._event(request)


def test_warm_bind_adopts_over_the_aci_and_settles_the_route(make_harness) -> None:
    """Positive path against the REAL runner app in bootstrap mode."""

    real = _RealRunnerApp()
    pool = _Pool()

    async def go() -> None:
        await real.runner.start()
        async with make_harness(binding=_Binding(), runner_app=real.app) as h:
            h.kernel.attach_warm_bind(pool)
            await h.kernel.process_event(_qevent("hello warm"))

            # The claim carried no identity env; the identity travelled over the ACI.
            assert h.fake_k8s.claim_envs == [None]
            assert real.loads == [(_SESSION, _HISTORY)]
            assert real.runner.session_id == _SESSION
            # The route settled through the fenced predicate with the minted credential.
            handle = h.substrate.lookup(_THREAD_KEY)
            assert handle is not None
            assert handle.adoption_state is AdoptionState.APPLIED
            assert handle.session_id == _SESSION and handle.token
            # The adopting-event marker was written at PENDING and cleared by
            # the terminal answer: a later retry of this event opens normally.
            assert handle.adopting_event_id is None
            assert h.sink.last_text
            # The bootstrap is retired on the pod; the conversation credential is live.
            client = RunnerClient(total_timeout_s=10.0)
            try:
                base = handle.base_url
                assert await client.adoption_authority(base, bootstrap_token=_BOOT) is False
                assert (
                    await client.adoption_applied(
                        base, conversation_token=handle.token, session_id=_SESSION
                    )
                    is True
                )
            finally:
                await client.close()
            # A second message on the thread is an ordinary turn under the
            # conversation credential on the same, now-bound, pod.
            await h.kernel.process_event(_qevent("and again"))
            assert h.fake_k8s.claim_envs == [None]
            assert len(real.sessions) >= 1 and sum(len(s.queries) for s in real.sessions) == 2
            assert h.substrate.lookup(_THREAD_KEY) == handle or (
                h.substrate.lookup(_THREAD_KEY).adoption_state is AdoptionState.APPLIED  # type: ignore[union-attr]
            )

    asyncio.run(go())


def test_without_a_policy_the_cold_path_is_unchanged(make_harness) -> None:
    async def go() -> None:
        async with make_harness(binding=_Binding()) as h:
            await h.kernel.process_event(_qevent("hello cold"))
            assert len(h.fake_k8s.claim_envs) == 1
            env = h.fake_k8s.claim_envs[0]
            assert env is not None and env[SESSION_ENV] == _SESSION
            handle = h.substrate.lookup(_THREAD_KEY)
            assert handle is not None and handle.adoption_state is AdoptionState.NONE
            assert h.runner.opened == ["hello cold"]

    asyncio.run(go())


def test_a_non_adopting_runner_is_refused_before_any_event(make_harness) -> None:
    """Open-mode surrogate (see the module docstring): 200 to the bootstrap bearer."""

    pool = _Pool()

    async def go() -> None:
        async with make_harness(binding=_Binding(), max_attempts=2) as h:
            h.kernel.attach_warm_bind(pool)
            await h.kernel.process_event(_qevent("hello old"))
            # No adopting event, no plain event, no model turn: the pod never
            # established authority, so nothing was ever sent to /v1/event.
            assert h.runner.opened == []
            # Each attempt claimed env-free, failed the probe, and released the
            # pod; the bounded attempt budget then escalated to a human.
            assert h.fake_k8s.claim_envs == [None, None]
            assert h.fake_k8s.claims == {}
            assert h.substrate.lookup(_THREAD_KEY) is None
            assert "Flagging for a human" in (h.sink.last_text or "")
            assert "runner-error" in (h.sink.last_text or "")

    asyncio.run(go())


def test_lost_response_after_an_applied_adoption_is_unconfirmed_not_retried(
    make_harness,
) -> None:
    fake = _AdoptingFake(apply=True)
    # The stream ends without a Final: a dropped response after the event landed.
    fake.default_script = [TextDelta(text="partial")]
    pool = _Pool()

    async def go() -> None:
        async with make_harness(binding=_Binding(), runner_app=fake.app, max_attempts=3) as h:
            h.kernel.attach_warm_bind(pool)
            await h.kernel.process_event(_qevent("hello lost"))
            # Exactly one adopting event; the lost turn was NOT replayed.
            assert len(fake.adopting_bodies) == 1 and fake.opened == ["hello lost"]
            body = fake.adopting_bodies[0]
            assert body["session_id"] == _SESSION and body["history_ref"] == _HISTORY
            # The attestation proved the binding: the route is APPLIED and kept...
            handle = h.substrate.lookup(_THREAD_KEY)
            assert handle is not None
            assert handle.adoption_state is AdoptionState.APPLIED
            assert handle.token == fake.conversation
            # ...and the turn is escalated as unconfirmed rather than retried.
            assert ADOPTION_UNCONFIRMED in (h.sink.last_text or "")
            assert "1 attempt(s)" in (h.sink.last_text or "")

    asyncio.run(go())


def test_lost_response_with_no_active_binding_releases_and_escalates(make_harness) -> None:
    fake = _AdoptingFake(apply=False)
    fake.default_script = [TextDelta(text="partial")]
    pool = _Pool()

    async def go() -> None:
        async with make_harness(binding=_Binding(), runner_app=fake.app, max_attempts=3) as h:
            h.kernel.attach_warm_bind(pool)
            await h.kernel.process_event(_qevent("hello gone"))
            assert len(fake.adopting_bodies) == 1 and fake.opened == ["hello gone"]
            # The credential is not active on the answering pod: fail closed,
            # the route is released, nothing is re-adopted, a human is flagged.
            assert h.substrate.lookup(_THREAD_KEY) is None
            assert h.fake_k8s.claims == {}
            assert ADOPTION_UNCONFIRMED in (h.sink.last_text or "")
            assert "1 attempt(s)" in (h.sink.last_text or "")

    asyncio.run(go())


def test_acked_turn_whose_route_was_replaced_logs_a_lost_fence(make_harness, caplog) -> None:
    fake = _AdoptingFake(apply=True)
    hold = asyncio.Event()
    fake.default_script = []
    fake.tail = [Final(text="done late", status=DONE)]
    pool = _Pool()

    async def go() -> None:
        fake.hold = hold
        async with make_harness(binding=_Binding(), runner_app=fake.app) as h:
            h.kernel.attach_warm_bind(pool)
            task = asyncio.create_task(h.kernel.process_event(_qevent("hello fence")))
            await _wait_applied(h)
            assert fake.turn_active
            # Settled APPLIED at the runner's 200, while the turn still streams.
            live = h.substrate.lookup(_THREAD_KEY)
            assert live is not None and live.adoption_state is AdoptionState.APPLIED
            # Another owner ends the session while the adopting turn streams:
            # the post-stream settle finds no route to re-affirm and says so.
            await asyncio.to_thread(h.substrate.release, _THREAD_KEY)
            hold.set()
            await task
            assert "adoption fence lost" in caplog.text
            assert h.sink.last_text == "done late"

    asyncio.run(go())


def test_follow_up_during_the_adopting_turn_steers_instead_of_releasing(make_harness) -> None:
    """Router finding 1: the route is APPLIED at the runner's 200, under the lock.

    A second message arriving while the adopting turn streams must take the
    cold path's steer (rule 1), never the PENDING branch, and must never
    release the live pod.
    """

    fake = _AdoptingFake(apply=True)
    hold = asyncio.Event()
    fake.default_script = [TextDelta(text="working")]
    fake.tail = [Final(text="first done", status=DONE)]
    pool = _Pool()

    async def go() -> None:
        fake.hold = hold
        async with make_harness(binding=_Binding(), runner_app=fake.app) as h:
            h.kernel.attach_warm_bind(pool)
            first = asyncio.create_task(h.kernel.process_event(_qevent("first")))
            await _wait_applied(h)
            assert fake.turn_active
            # Settled before the route lock was released: APPLIED while streaming.
            live = h.substrate.lookup(_THREAD_KEY)
            assert live is not None and live.adoption_state is AdoptionState.APPLIED
            claim_before = live.claim_name
            # The follow-up steers under the conversation credential.
            await h.kernel.process_event(_qevent("second"))
            assert fake.steers == ["second"]
            assert len(fake.adopting_bodies) == 1 and fake.opened == ["first"]
            after = h.substrate.lookup(_THREAD_KEY)
            assert after is not None and after.claim_name == claim_before
            hold.set()
            await first
            assert h.sink.last_text == "first done" or "Folded" in (h.sink.last_text or "")
            assert pool.asked.count(_THREAD_KEY) >= 1

    asyncio.run(go())


def test_lost_owner_binding_continues_a_new_message_and_escalates_the_original(
    make_harness,
) -> None:
    """Router findings 2/3 and Fable's retry-vs-new: the marker tells them apart.

    A lost owner adopted the pod (runner 200) but died before the route write.
    The route is PENDING and names the ORIGINAL adopting event. A NEW message
    settles the route and continues on the ordinary steer-or-open path (rule
    1); a redelivery of the ORIGINAL event sends nothing and is escalated as
    unconfirmed, because its turn may have run.
    """

    fake = _AdoptingFake(apply=True)
    fake.default_script = [Final(text="answer", status=DONE)]
    pool = _Pool()

    async def go() -> None:
        async with make_harness(binding=_Binding(), runner_app=fake.app, max_attempts=3) as h:
            h.kernel.attach_warm_bind(pool)
            await h.kernel.process_event(_qevent("first"))
            applied = h.substrate.lookup(_THREAD_KEY)
            assert applied is not None and applied.adoption_state is AdoptionState.APPLIED
            # Rewind the ROUTE to the lost-owner shape; the pod stays bound.
            from dataclasses import replace

            h.substrate._affinity.replace(  # noqa: SLF001 - shaping the store for the test
                _THREAD_KEY,
                RouteRecord(
                    handle=replace(
                        applied,
                        adoption_state=AdoptionState.PENDING,
                        adopting_event_id="original-adopting-event",
                    ),
                    state=RouteState.LIVE,
                ),
                600,
            )
            # A NEW message: no adopting event, the route settles APPLIED, and
            # the message opens an ordinary turn on the bound pod.
            await h.kernel.process_event(_qevent("new message"))
            assert len(fake.adopting_bodies) == 1
            assert fake.opened == ["first", "new message"]
            assert h.sink.last_text == "answer"
            settled = h.substrate.lookup(_THREAD_KEY)
            assert settled is not None
            assert settled.adoption_state is AdoptionState.APPLIED
            assert settled.claim_name == applied.claim_name
            assert settled.adopting_event_id == "original-adopting-event"
            # The ORIGINAL event, redelivered: nothing is sent, it is escalated
            # as unconfirmed on one attempt, the pod is kept.
            original = _qevent("first")
            original = original.model_copy(update={"event_id": "original-adopting-event"})
            await h.kernel.process_event(original)
            assert fake.opened == ["first", "new message"]
            assert ADOPTION_UNCONFIRMED in (h.sink.last_text or "")
            assert "1 attempt(s)" in (h.sink.last_text or "")
            assert list(h.fake_k8s.claims) == [applied.claim_name]
            kept = h.substrate.lookup(_THREAD_KEY)
            assert kept is not None and kept.adopting_event_id == "original-adopting-event"

    asyncio.run(go())


def test_crash_after_the_runner_200_makes_the_redelivery_unconfirmed(make_harness) -> None:
    """Router 8's blocking finding: cancel between the 200 and the settle.

    The route is APPLIED with this event's marker; the redelivered SAME event
    must not open a plain turn on the bound pod.
    """

    fake = _AdoptingFake(apply=True)
    hold = asyncio.Event()
    fake.default_script = [TextDelta(text="working")]
    fake.tail = [Final(text="late", status=DONE)]
    pool = _Pool()

    async def go() -> None:
        fake.hold = hold
        async with make_harness(binding=_Binding(), runner_app=fake.app, max_attempts=3) as h:
            h.kernel.attach_warm_bind(pool)
            event = _qevent("first")
            task = asyncio.create_task(h.kernel.process_event(event))
            await _wait_applied(h)
            assert fake.turn_active
            live = h.substrate.lookup(_THREAD_KEY)
            assert live is not None
            assert live.adoption_state is AdoptionState.APPLIED
            assert live.adopting_event_id == event.event_id
            # The worker dies mid-stream.
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
            hold.set()
            for _ in range(200):
                if not fake.turn_active:
                    break
                await asyncio.sleep(0.01)
            # Redelivery of the SAME event: no event is sent, the delivery is
            # escalated unconfirmed, the pod and the marker are kept.
            await h.kernel.process_event(event)
            assert fake.opened == ["first"] and len(fake.adopting_bodies) == 1
            assert ADOPTION_UNCONFIRMED in (h.sink.last_text or "")
            kept = h.substrate.lookup(_THREAD_KEY)
            assert kept is not None and kept.adopting_event_id == event.event_id
            assert list(h.fake_k8s.claims) == [live.claim_name]
            # A different message on the thread routes normally.
            await h.kernel.process_event(_qevent("second"))
            assert fake.opened == ["first", "second"]

    asyncio.run(go())


def test_terminal_answer_without_the_per_frame_ack_keeps_the_session(make_harness) -> None:
    """Router 8's should-fix: after settle-at-200 a missing ack is a contract log, not a release."""

    fake = _AdoptingFake(apply=True, stamp=False)
    fake.default_script = [Final(text="unstamped answer", status=DONE)]
    pool = _Pool()

    async def go() -> None:
        async with make_harness(binding=_Binding(), runner_app=fake.app) as h:
            h.kernel.attach_warm_bind(pool)
            await h.kernel.process_event(_qevent("first"))
            assert h.sink.last_text == "unstamped answer"
            kept = h.substrate.lookup(_THREAD_KEY)
            assert kept is not None and kept.adoption_state is AdoptionState.APPLIED
            assert list(h.fake_k8s.claims) == [kept.claim_name]

    asyncio.run(go())
