"""A cancelled resume does not execute (#2753), observed at the kernel.

The companion to ``tests/binding/test_resume_tombstone.py``: that file proves
the worker can READ ``approvals.resume_cancelled_at`` off the durable row, this
one proves the worker ACTS on it -- no model run, no sandbox claim, no reply,
and the delivery settled rather than redelivered forever or dead-lettered.

Why a tombstone rather than a ``resumed_at`` compare-and-set. The API enqueues
the resume turn BEFORE it marks ``resumed_at`` (the inline resolver, the expiry
sweeper and the reconciler all do). A crash in that window leaves a runnable
entry in Valkey that no CAS can retract, because the entry already exists. An
administrator then cancels the owed resume, the entry is delivered anyway, and
without a veto at EXECUTION the cancelled resume runs. The tombstone is that
veto, and ``test_crash_window_entry_is_vetoed_at_execution`` below is its proof.

Harness discipline is the file's neighbours': real Valkey, the real substrate
over a fake Kubernetes client, an in-process fake ACI runner, and a binding
double at the ``approval_resume_cancelled`` seam -- exactly the shape
``GrantBinding`` uses in ``test_approval_lifecycle.py`` for the #430 grant.
"""

from __future__ import annotations

import asyncio
import uuid

from aci_protocol import Final, QueuedTurn, ReplyHandle, SessionStatus
from channel_protocol.reply import TurnStatus
from curie_api.resumequeue import resume_event_id

DONE = SessionStatus.DONE

_REASON = "cancelled during the 0.9.2 upgrade drill"
_ACTOR = "U-OPERATOR-2753"


def _qevent(text: str, *, thread: str, event_id: str) -> QueuedTurn:
    return QueuedTurn(
        event_id=event_id,
        conversation_id=thread,
        author="U1",
        text=text,
        reply_handle=ReplyHandle(kind="slack", channel="C1", placeholder="p-1"),
        received_at="2026-09-18T00:00:00+00:00",
    )


class TombstoneBinding:
    """A binding stand-in that answers the tombstone read by event id.

    ``resolve``/``boot_env``/``packs_for``/``budget_for`` behave like the routed
    double in ``test_approval_lifecycle.py``. ``approval_resume_cancelled``
    answers a refusal for the one resume event id it was configured with,
    whatever agent the channel currently resolves to -- mirroring the real
    resolver, whose veto is keyed on the cancelled approval the event names and
    is deliberately NOT agent-bound (binding a veto would make a NULL or
    rebound agent into permission to run a cancelled resume).

    ``cancelled`` is mutable so a test can commit the tombstone AFTER the entry
    was enqueued, which is the crash window the column exists for. ``probes``
    counts every call, so a test can prove an ordinary turn pays nothing.
    """

    def __init__(
        self,
        *,
        cancelled_event_id: str | None = None,
        cancelled: bool = True,
        owner_agent_id: uuid.UUID | None = None,
        resolves_agent_id: uuid.UUID | None = None,
    ) -> None:
        self.agent_id = resolves_agent_id or owner_agent_id or uuid.uuid4()
        self.owner_agent_id = owner_agent_id or self.agent_id
        self.cancelled_event_id = cancelled_event_id
        self.cancelled = cancelled
        self.probes: list[str] = []
        self.records: list[tuple[str | None, str | None, int | None]] = []

    async def resolve(self, kind: str, channel: str):  # noqa: ANN201
        from curie_worker.binding import ResolvedDeployment

        return ResolvedDeployment(
            agent_id=self.agent_id,
            agent_name="test-agent",
            version_id=uuid.uuid4(),
            version_label="v1",
            bundle_ref=None,
            max_usd_per_day=None,
            max_output_tokens_per_run=None,
        )

    def packs_for(self, resolved):  # noqa: ANN001, ANN201
        from curie_worker.behaviorpacks import BehaviorPacks

        return BehaviorPacks.from_config(None)

    def budget_for(self, resolved):  # noqa: ANN001, ANN201
        from aci_protocol import Budget

        return Budget(max_output_tokens_per_run=1000, max_usd_per_day=1.0)

    def boot_env(self, resolved, thread_key, *, kind=None, address=None):  # noqa: ANN001, ANN201
        return {"CURIE_SESSION_ID": f"s-{thread_key}"}

    async def approval_resume_cancelled(  # noqa: ANN201
        self,
        event_id: str,
        *,
        lease_key: str | None,
        owner: str | None,
        generation: int | None,
    ):
        self.probes.append(event_id)
        # The real resolver fast-returns on a non-approval event id with no DB
        # round trip; the double mirrors the shape so a kernel change that
        # stopped gating this call is still visible in ``probes``.
        if not (event_id.startswith("approval-") and event_id.endswith("-resolved")):
            return None
        if not self.cancelled or event_id != self.cancelled_event_id:
            self.records.append((lease_key, owner, generation))
            return None
        # No agent gate: the veto belongs to the approval the event names, not
        # to whoever the address happens to be bound to now.
        return f"resume cancelled by {_ACTOR}: {_REASON}"

    async def approval_decision(self, event_id: str, agent_id):  # noqa: ANN001, ANN201
        return "approved" if event_id == self.cancelled_event_id else None


def _assert_did_not_execute(h) -> None:  # noqa: ANN001
    """No model run, no sandbox claim, no reply to the human -- the observable
    effects.

    Deliberately the OUTCOMES rather than a log line or an internal field: a
    veto that let the runner open a turn and only suppressed the reply would
    still have run the approved action, which is the entire failure this closes.

    "No reply" is scoped to the reply SURFACE, not to the event count. The
    terminal ``finally`` lowers the shimmer unconditionally on every exit path
    (pinned by ``test_the_already_done_skip_still_lowers_the_shimmer``), so a
    vetoed turn still emits one empty ``TurnStatus``. That is a caption clear on
    an already-settled placeholder -- it carries no text, posts nothing and
    tells the human nothing -- and buying its absence would mean parking
    cross-delivery state on the kernel's terminal path. Every OTHER event, and
    any non-empty status, is forbidden: ``status_sets`` empty proves the veto
    landed before the shimmer was ever raised, and it is non-empty on an
    ordinary turn, so this catches the veto's removal.
    """
    assert h.runner.opened == [], f"the model ran anyway: {h.runner.opened}"
    assert h.fake_k8s.claim_envs == [], "a sandbox was claimed for a cancelled resume"
    assert h.sink.updates == [], f"a cancelled resume replied: {h.sink.updates}"
    assert h.sink.text_posts == [], f"a cancelled resume posted: {h.sink.text_posts}"
    assert h.sink.posts == [], f"a cancelled resume posted a message: {h.sink.posts}"
    assert h.sink.card_updates == [], f"a cancelled resume rewrote a card: {h.sink.card_updates}"
    assert h.sink.completions == [], f"a vetoed turn owed a completion: {h.sink.completions}"
    assert h.sink.status_sets == [], f"a cancelled resume raised a shimmer: {h.sink.status_sets}"
    beyond_clear = [
        event
        for event, _, _ in h.sink.events
        if not (isinstance(event, TurnStatus) and not event.status)
    ]
    assert beyond_clear == [], (
        f"a cancelled resume emitted more than a shimmer clear: {beyond_clear}"
    )


def test_tombstoned_resume_turn_does_not_run(make_harness) -> None:
    """A resume turn whose approval carries ``resume_cancelled_at`` produces no
    side effect, no model run and no reply -- and is SETTLED, so the delivery is
    acked rather than redelivered until the cap and dead-lettered."""

    async def go() -> None:
        event = resume_event_id(uuid.uuid4())
        binding = TombstoneBinding(cancelled_event_id=event)
        async with make_harness(binding=binding) as h:
            h.runner.default_script = [Final(text="Issue created.", status=DONE)]
            await h.kernel.process_event(
                _qevent("proceed with the approved action", thread="th-tomb", event_id=event)
            )

            _assert_did_not_execute(h)
            # The done marker is this suite's proof of terminal handling: it is
            # written only once the turn is settled, so its presence is what
            # separates "refused" from "silently dropped and redelivered".
            assert await h.async_redis.exists(h.config.done_key(event))

    asyncio.run(go())


def test_untombstoned_resume_turn_still_runs(make_harness) -> None:
    """The negative control. An ordinary, uncancelled resume turn runs exactly
    as it does today, so a tombstone check written to refuse everything -- the
    cheapest wrong implementation -- fails here instead of passing the file."""

    async def go() -> None:
        event = resume_event_id(uuid.uuid4())
        binding = TombstoneBinding(cancelled_event_id=event, cancelled=False)
        async with make_harness(binding=binding) as h:
            h.runner.default_script = [Final(text="Issue created.", status=DONE)]
            await h.kernel.process_event(
                _qevent("proceed with the approved action", thread="th-live", event_id=event)
            )

            assert h.runner.opened == ["proceed with the approved action"]
            assert h.fake_k8s.claim_envs, "the resume turn never claimed a sandbox"
            assert any("Issue created." in u[2] for u in h.sink.updates)
            assert await h.async_redis.exists(h.config.done_key(event))

    asyncio.run(go())


def test_crash_window_entry_is_vetoed_at_execution(make_harness) -> None:
    """AC3's exactly-once-under-crashes proof, and the reason the tombstone
    exists rather than a ``resumed_at`` CAS alone.

    The sequence is the real one: the API enqueued the resume turn and crashed
    before marking ``resumed_at``, so a runnable entry exists that no CAS can
    retract. An administrator then cancels the owed resume -- the tombstone
    commits AFTER the entry was already appended. The entry is then delivered.
    It must not execute."""

    async def go() -> None:
        event = resume_event_id(uuid.uuid4())
        # cancelled=False at first: this is the state the entry was appended in.
        binding = TombstoneBinding(cancelled_event_id=event, cancelled=False)
        async with make_harness(binding=binding) as h:
            h.runner.default_script = [Final(text="Issue created.", status=DONE)]
            qevent = _qevent(
                "proceed with the approved action", thread="th-crash", event_id=event
            )

            # The entry is now on the stream, resumed_at is still NULL, and
            # nothing has run. The administrator cancels.
            binding.cancelled = True

            # Delivery happens after the cancellation committed.
            await h.kernel.process_event(qevent)

            _assert_did_not_execute(h)
            assert await h.async_redis.exists(h.config.done_key(event))

    asyncio.run(go())


def test_redelivering_a_tombstoned_resume_is_idempotent(make_harness) -> None:
    """The same cancelled entry delivered twice still produces no effect and
    accumulates no errors. A one-shot latch on the refusal would let the SECOND
    delivery -- the ordinary consequence of an unacked entry being reclaimed --
    execute the action the first one refused."""

    async def go() -> None:
        event = resume_event_id(uuid.uuid4())
        binding = TombstoneBinding(cancelled_event_id=event)
        async with make_harness(binding=binding) as h:
            h.runner.default_script = [Final(text="Issue created.", status=DONE)]
            qevent = _qevent(
                "proceed with the approved action", thread="th-redeliver", event_id=event
            )

            await h.kernel.process_event(qevent)
            _assert_did_not_execute(h)
            await h.kernel.process_event(qevent)
            _assert_did_not_execute(h)

            assert await h.async_redis.exists(h.config.done_key(event))

    asyncio.run(go())


def test_ordinary_turn_pays_nothing_for_the_tombstone(make_harness) -> None:
    """A non-approval event id costs no tombstone lookup, matching the existing
    fast-return idiom the grant and the decision already keep. Asserted at the
    call, not only inside the resolver: the kernel must not start paying a
    per-turn database round trip for a column only resume turns can carry."""

    async def go() -> None:
        binding = TombstoneBinding(cancelled_event_id=resume_event_id(uuid.uuid4()))
        async with make_harness(binding=binding) as h:
            h.runner.default_script = [Final(text="Hello.", status=DONE)]
            await h.kernel.process_event(
                _qevent("hello there", thread="th-ordinary", event_id="ev-fresh-1")
            )

            assert h.runner.opened == ["hello there"]
            assert [p for p in binding.probes if not p.startswith("approval-")] == []

    asyncio.run(go())


def test_tombstone_survives_a_rebind_of_the_address(make_harness) -> None:
    """A rebind must not convert a cancellation into permission to run.

    The inverse of ``approval_grant_tool``'s #430 rebind guard, and the reason
    it cannot be copied. There, a mismatch withholds AUTHORITY, which is
    fail-safe. Here a mismatch would withhold a REFUSAL, so an approval whose
    address was rebound (or whose ``agent_id`` is NULL on an older row) would
    run the very resume an administrator cancelled. The queued event names the
    cancelled approval directly, so the veto holds whoever is bound now."""

    async def go() -> None:
        event = resume_event_id(uuid.uuid4())
        owner = uuid.uuid4()
        other = uuid.uuid4()
        binding = TombstoneBinding(
            cancelled_event_id=event, owner_agent_id=owner, resolves_agent_id=other
        )
        async with make_harness(binding=binding) as h:
            h.runner.default_script = [Final(text="Issue created.", status=DONE)]
            await h.kernel.process_event(
                _qevent("proceed with the approved action", thread="th-other", event_id=event)
            )

            # The channel now resolves to a DIFFERENT agent than the one that
            # owned the tombstoned approval, and the resume is STILL refused.
            _assert_did_not_execute(h)

    asyncio.run(go())


_LEASE_KNOBS: dict[str, object] = {
    "delivery_budget_s": 60.0,
    "delivery_lease_ttl_s": 2.0,
    "delivery_lease_heartbeat_s": 0.3,
    "runner_total_timeout_s": 30.0,
}


def test_crashed_delivery_is_redelivered_re_recorded_and_run(make_harness) -> None:
    """The lost-continuation regression, at the kernel with real delivery leases.

    Delivery 1 holds a real lease and dies before its first side effect (its
    lease released, as expiry would). The redelivery acquires the next fencing
    generation and must RECORD under it and RUN, and each record must name the
    lease key the platform actually uses, because that key is what the API
    names when it refuses to cancel a started resume."""

    async def go() -> None:
        from curie_worker.delivery_lease import DeliveryLeaseStore

        event = resume_event_id(uuid.uuid4())
        binding = TombstoneBinding(cancelled_event_id=event, cancelled=False)
        async with make_harness(binding=binding, **_LEASE_KNOBS) as h:
            h.runner.default_script = [Final(text="Issue created.", status=DONE)]
            store = DeliveryLeaseStore(h.async_redis, h.config)
            stream, group = f"s-2753-{uuid.uuid4().hex[:8]}", "g-2753"
            await h.async_redis.xgroup_create(stream, group, id="0", mkstream=True)
            raw_id = await h.async_redis.xadd(stream, {"event_id": event})
            entry_id = raw_id.decode() if isinstance(raw_id, bytes) else raw_id
            await h.async_redis.xreadgroup(group, "c1", {stream: ">"}, count=1)
            key = h.config.delivery_lease_key(stream, group, entry_id)

            crashed = await store.acquire(stream, group, entry_id, consumer="c1")
            # The crash: delivery 1 recorded execution and never finished. Its
            # record is what the double saw first; its lease is then gone.
            binding.records.append((key, crashed.owner, crashed.generation))
            await h.async_redis.delete(key)
            # Lease recovery hands the pending entry to the next consumer.
            await h.async_redis.xclaim(stream, group, "c2", 0, [entry_id])

            retry = await store.acquire(stream, group, entry_id, consumer="c2")
            assert retry.generation > crashed.generation
            await h.kernel.process_event(
                _qevent("proceed with the approved action", thread="th-crash2", event_id=event),
                lease=retry,
            )

            assert h.runner.opened == ["proceed with the approved action"], (
                "the redelivered resume did not run; the owed continuation was lost"
            )
            assert binding.records[-1] == (key, retry.owner, retry.generation)

    asyncio.run(go())


def test_an_unfenced_delivery_records_a_null_lease(make_harness) -> None:
    """A caller with no fenced lease records a NULL key rather than inventing
    an owner; the API refuses to cancel it like any recorded execution."""

    async def go() -> None:
        event = resume_event_id(uuid.uuid4())
        binding = TombstoneBinding(cancelled_event_id=event, cancelled=False)
        async with make_harness(binding=binding) as h:
            h.runner.default_script = [Final(text="Issue created.", status=DONE)]
            await h.kernel.process_event(
                _qevent("proceed with the approved action", thread="th-nf", event_id=event)
            )
            assert binding.records == [(None, None, None)]
            assert h.runner.opened == ["proceed with the approved action"]

    asyncio.run(go())


class _UnboundTombstoneBinding(TombstoneBinding):
    """The tombstoned resume's address is no longer bound to any agent."""

    async def resolve(self, kind: str, channel: str):  # noqa: ANN201
        return None


class _AlwaysKilled:
    async def is_killed(self, _agent_id: uuid.UUID) -> bool:
        return True


def _assert_settled_silently(h) -> None:  # noqa: ANN001
    """The drop paths post a reply through the original transport; a vetoed
    resume must never reach them, so no reply of any shape is emitted."""
    _assert_did_not_execute(h)
    assert h.sink.events == [] or all(
        isinstance(event, TurnStatus) and not event.status for event, _, _ in h.sink.events
    ), f"a vetoed resume reached the drop path: {h.sink.events}"


def test_tombstoned_resume_on_an_unbound_address_settles_silently(make_harness) -> None:
    """An unbound address used to route the vetoed resume into the "no agent is
    configured" drop reply BEFORE the veto, which needs the very transport a
    broken card may have lost (#2753): retries, then a dead-letter."""

    async def go() -> None:
        event = resume_event_id(uuid.uuid4())
        binding = _UnboundTombstoneBinding(cancelled_event_id=event)
        async with make_harness(binding=binding) as h:
            qevent = _qevent("proceed", thread="th-unbound", event_id=event)
            await h.kernel.process_event(qevent)
            _assert_settled_silently(h)
            assert await h.async_redis.exists(h.config.done_key(event))
            await h.kernel.process_event(qevent)
            _assert_settled_silently(h)
            assert binding.probes == [event], "the redelivery was not a no-op"

    asyncio.run(go())


def test_tombstoned_resume_for_a_paused_agent_settles_silently(make_harness) -> None:
    """Same defect through the killswitch drop ("paused by an operator")."""

    async def go() -> None:
        event = resume_event_id(uuid.uuid4())
        binding = TombstoneBinding(cancelled_event_id=event)
        async with make_harness(binding=binding) as h:
            h.kernel.attach_killswitch(_AlwaysKilled())  # type: ignore[arg-type]
            qevent = _qevent("proceed", thread="th-paused", event_id=event)
            await h.kernel.process_event(qevent)
            _assert_settled_silently(h)
            assert await h.async_redis.exists(h.config.done_key(event))
            await h.kernel.process_event(qevent)
            _assert_settled_silently(h)
            assert binding.probes == [event], "the redelivery was not a no-op"

    asyncio.run(go())
