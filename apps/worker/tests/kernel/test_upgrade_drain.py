"""A routine chart upgrade must not turn accepted work into an escalation (#2010).

The incident this file is written against, in full:

    11:15:23  the worker claimed a real AgentMail turn
    11:15:44  `helm upgrade` (atomic) began; the backing services and the worker
              rolled together
    11:16:31  the REPLACEMENT worker reclaimed the delivery, saw the durable
              side-effect marker, and emitted the designed escalation --
              "A prior attempt started an action before the worker restarted;
              not retrying automatically. Flagging for a human."
    11:16:41  that reply was delivered

Nothing was lost and no side effect ran twice. The requested task simply did not
happen. That escalation is the correct answer to an unsafe situation; the bug is
that a supported, routine upgrade CREATES the situation.

Two tests carry the property, and they are a matched pair:

- ``test_a_rollout_over_an_accepted_side_effecting_turn_escalates_it`` is the
  NEGATIVE CONTROL. It reproduces the incident with the gate bypassed. Without
  it, "the fixed path completed the turn" would be indistinguishable from a
  harness that cannot produce the failure at all.
- ``test_the_pre_upgrade_gate_refuses_the_roll_and_the_turn_completes_once`` is
  the regression. The gate refuses the upgrade while the accepted turn is still
  in flight, nothing is rolled, and the turn reaches its intended terminal
  outcome exactly once.

Reverts that turn this red, each named against the property it breaks:

- Return ``()`` unconditionally from ``UpgradeDrainGate.unsettled_deliveries``
  (or gate on pending-ness rather than lease liveness) -> the gate reports a
  clean drain over a live delivery and the regression's refusal assertion fails.
- Drop the ``_claims_paused`` check from ``StreamConsumer._consume`` or from
  ``Consumer._maintenance_loop``'s reclaim -> the replacement claims during the
  drain and the suppression test fails.
- Clear the quiesce flag inside ``await_drained``'s success path -> the
  replacement pods reclaim mid-roll, which is the incident itself.
"""

from __future__ import annotations

import asyncio
import contextlib
import functools
import sys
import time
import uuid
from dataclasses import replace
from pathlib import Path
from typing import Any, cast

from aci_protocol import (
    Final,
    SessionStatus,
    SideEffectFlag,
    TextDelta,
)
from curie_dispatcher.queue import to_stream_fields
from curie_worker.config import WorkerConfig
from curie_worker.consumer import Consumer
from curie_worker.consumer_liveness import ConsumerLivenessStore
from curie_worker.delivery_lease import DeliveryLeaseStore
from curie_worker.eval import EvalStreamConsumer
from curie_worker.upgrade_drain import UpgradeDrainGate, run_gate

# importlib import mode does not add the test root to sys.path.
sys.path.insert(0, str(Path(__file__).parent.parent))

from queue_fixtures import qevent, wait_until  # noqa: E402

_qevent = functools.partial(qevent, received_at="2026-08-28T11:15:23+00:00")
_wait_until = functools.partial(wait_until, timeout=20.0, interval=0.02)

DONE = SessionStatus.DONE

# The escalation the incident produced, verbatim from ``Kernel._process_one``.
# Byte-identical on purpose: these tests assert the FAILURE is reproduced in the
# control and absent after the fix, and a paraphrase would pass both ways.
ESCALATION = (
    "A prior attempt started an action before the worker restarted; "
    "not retrying automatically. Flagging for a human."
)

# Compressed lease clocks (the ``test_delivery_lease.py`` shape) so a rollout
# window is seconds rather than a minute. Every config ratio is preserved:
# TTL >= 3x heartbeat, reclaim interval < TTL, runner ceiling <= budget, and the
# quiesce TTL strictly outlasts the drain wait.
_KNOBS: dict[str, object] = {
    "delivery_budget_s": 60.0,
    "delivery_lease_ttl_s": 1.0,
    "delivery_lease_heartbeat_s": 0.3,
    "runner_total_timeout_s": 30.0,
    "reclaim_interval_s": 0.05,
    "reclaim_min_idle_ms": 50,
    "upgrade_drain_timeout_s": 0.5,
    "upgrade_drain_poll_interval_s": 0.05,
    "upgrade_quiesce_ttl_s": 5.0,
}


async def _stop(task: asyncio.Task[None], consumer: Consumer | None = None) -> None:
    if consumer is not None:
        consumer.request_stop()
    task.cancel()
    with contextlib.suppress(asyncio.CancelledError, Exception):
        await task


async def _pending_row(
    h, *, stream: str, group: str, entry_id: str
) -> dict[str, Any]:  # noqa: ANN001
    rows = await h.async_redis.xpending_range(
        stream,
        group,
        min=entry_id,
        max=entry_id,
        count=1,
    )
    assert len(rows) == 1
    return rows[0]


def _recording_eval_consumer(
    h,  # noqa: ANN001
    config: WorkerConfig,
    gate: UpgradeDrainGate,
    seen: list[tuple[str, dict[str, str]]],
    *,
    leases: DeliveryLeaseStore | None = None,
) -> EvalStreamConsumer:
    """An actual eval consumer with only its external execution collaborators inert."""
    consumer = EvalStreamConsumer(
        redis=h.async_redis,
        config=config,
        bundle_store=cast(Any, object()),
        substrate=cast(Any, object()),
        reporter=cast(Any, object()),
        recorder=cast(Any, object()),
        repo_lookup=cast(Any, object()),
        leases=leases,
        drain=gate,
    )

    async def record(entry_id: str, fields: dict[str, str]) -> None:
        seen.append((entry_id, dict(fields)))

    consumer._delivery = replace(consumer._delivery, handler=record)
    return consumer


async def _kill_replica(task: asyncio.Task[None], consumer: Consumer) -> None:
    """Model the Pod going away mid-turn, which is what a roll does.

    Cancelling only ``run()`` is not that: the per-message handlers are separate
    tasks and would keep executing (and keep holding the thread lock), so the
    replacement would block on a replica that in reality no longer exists. The
    handler tasks go too, exactly as a SIGKILLed process's would.
    """
    inflight = list(consumer._inflight)
    await _stop(task, consumer)
    for handler in inflight:
        handler.cancel()
    for handler in inflight:
        with contextlib.suppress(asyncio.CancelledError, Exception):
            await handler


def _texts(h) -> list[str]:  # noqa: ANN001
    """Every reply body the sink saw, whether edited into a placeholder or posted."""
    return [text for _addr, _ref, text in h.sink.updates] + [
        text for _addr, _ref, text in h.sink.text_posts
    ]


def _admit_side_effecting_turn(h, consumer: Consumer):  # noqa: ANN001, ANN201
    """Script the runner so the turn flags a side effect and then hangs.

    This is the incident's "deliberate real AgentMail turn": the durable
    no-retry marker is written the instant the frame is seen, and the turn is
    still open. Returns the hold event the test releases.
    """
    hold = asyncio.Event()
    h.runner.hold = hold
    h.runner.default_script = [
        SideEffectFlag(tool="agentmail.send", call_id="call-1"),
        TextDelta(text="sending the mail"),
    ]
    h.runner.tail = [Final(text="mail sent", status=DONE)]
    assert consumer is not None
    return hold


# --- the negative control: the incident, reproduced ---------------------------


def test_a_rollout_over_an_accepted_side_effecting_turn_escalates_it(make_harness) -> None:  # noqa: ANN001
    """Reproduce #2010 with the gate bypassed.

    The old replica accepts the turn and flags a side effect; the roll takes it
    away mid-flight; the replacement reclaims, refuses to re-run the action, and
    escalates. Exactly what happened at 11:16:31, and exactly what the gate
    exists to stop happening on a routine upgrade.
    """
    thread = "drain-control-1"
    event_id = uuid.uuid4().hex

    async def go() -> None:
        async with make_harness(**_KNOBS) as h:
            store = DeliveryLeaseStore(h.async_redis, h.config)
            old_cfg = h.config.model_copy(update={"consumer_name": "worker-old"})
            new_cfg = h.config.model_copy(update={"consumer_name": "worker-new"})
            old = Consumer(redis=h.async_redis, kernel=h.kernel, config=old_cfg, leases=store)
            new = Consumer(redis=h.async_redis, kernel=h.kernel, config=new_cfg, leases=store)
            _admit_side_effecting_turn(h, old)

            # The group is created at ``$``, so it must exist before the enqueue.
            await old.ensure_group()
            old_task = asyncio.create_task(old.run())
            new_task: asyncio.Task[None] | None = None
            try:
                await h.async_redis.xadd(
                    h.config.stream,
                    to_stream_fields(_qevent("send the mail", thread=thread, event_id=event_id)),
                )
                # Admitted: the turn is open and the durable marker is written.
                await _wait_until(lambda: h.runner.turn_active)
                deadline = time.monotonic() + 20.0
                while time.monotonic() < deadline:
                    if await h.async_redis.exists(h.config.side_effect_key(event_id)):
                        break
                    await asyncio.sleep(0.02)
                assert await h.async_redis.exists(h.config.side_effect_key(event_id)), (
                    "the side-effect marker was never written; nothing was admitted"
                )

                # The roll: the old worker goes away mid-turn, exactly as a Pod
                # being replaced does. NO gate runs.
                await _kill_replica(old_task, old)

                new_task = asyncio.create_task(new.run())
                await _wait_until(lambda: any(ESCALATION in t for t in _texts(h)), timeout=30.0)

                # The incident's shape, asserted: escalated, delivered, and the
                # requested task never happened.
                assert any(ESCALATION in t for t in _texts(h))
                assert not any("mail sent" in t for t in _texts(h)), (
                    "the control did not actually reproduce the failure"
                )
            finally:
                if new_task is not None:
                    await _stop(new_task, new)
                if not old_task.done():
                    await _stop(old_task, old)

    asyncio.run(go())


# --- the regression: refuse the roll, finish the work -------------------------


def test_the_pre_upgrade_gate_refuses_the_roll_and_the_turn_completes_once(make_harness) -> None:  # noqa: ANN001
    """The same accepted turn, with the pre-upgrade gate in front of the roll.

    The gate finds a live-leased delivery, refuses, and `helm upgrade` fails
    with it -- so nothing is rolled, the turn keeps running on the workers that
    are already there, and it reaches its intended terminal outcome exactly
    once. The no-duplicate-side-effect invariant is asserted on the far side: a
    redelivery of the same event after the turn is done runs no second action.
    """
    thread = "drain-fixed-1"
    event_id = uuid.uuid4().hex

    async def go() -> None:
        async with make_harness(**_KNOBS) as h:
            store = DeliveryLeaseStore(h.async_redis, h.config)
            gate = UpgradeDrainGate(h.async_redis, h.config)
            old_cfg = h.config.model_copy(update={"consumer_name": "worker-old"})
            old = Consumer(
                redis=h.async_redis,
                kernel=h.kernel,
                config=old_cfg,
                leases=store,
                drain=gate,
            )
            hold = _admit_side_effecting_turn(h, old)

            await old.ensure_group()
            old_task = asyncio.create_task(old.run())
            try:
                await h.async_redis.xadd(
                    h.config.stream,
                    to_stream_fields(_qevent("send the mail", thread=thread, event_id=event_id)),
                )
                await _wait_until(lambda: h.runner.turn_active)
                deadline = time.monotonic() + 20.0
                while time.monotonic() < deadline:
                    if await h.async_redis.exists(h.config.side_effect_key(event_id)):
                        break
                    await asyncio.sleep(0.02)
                assert await h.async_redis.exists(h.config.side_effect_key(event_id))

                # The chart's pre-upgrade hook, before a single manifest is
                # applied. It must refuse: an accepted, side-effecting delivery
                # is live.
                outcome = await gate.await_drained()
                assert outcome.drained is False, (
                    "the gate cleared an upgrade over a live side-effecting delivery"
                )
                assert outcome.remaining, "a refusal must name what held it back"
                assert all(
                    r.startswith(f"{h.config.stream}/{h.config.consumer_group}/")
                    for r in outcome.remaining
                )

                # `helm upgrade` fails on that exit code, so NOTHING rolls. The
                # postponed upgrade puts the fleet back the way it found it.
                await gate.clear_quiesce()

                # The turn finishes on the workers that were already there.
                hold.set()
                await _wait_until(lambda: any("mail sent" in t for t in _texts(h)), timeout=30.0)
                await _wait_until(lambda: len(h.sink.completions) >= 1, timeout=30.0)

                assert not any(ESCALATION in t for t in _texts(h)), (
                    "accepted work was still converted into a human escalation"
                )
                assert len(h.runner.opened) == 1, (
                    f"the turn ran more than once: {h.runner.opened!r}"
                )
                assert len(h.sink.completions) == 1, (
                    f"expected exactly one terminal completion, saw {h.sink.completions!r}"
                )

                # The invariant this fix may not trade away: a redelivery of the
                # same event after it is terminally done executes no second
                # action. The kernel's ``is_terminal`` short-circuit is what
                # holds it, and it must still hold with the gate in the path.
                await h.async_redis.xadd(
                    h.config.stream,
                    to_stream_fields(_qevent("send the mail", thread=thread, event_id=event_id)),
                )
                await asyncio.sleep(1.0)
                assert len(h.runner.opened) == 1, (
                    "a redelivery re-ran the side-effecting turn"
                )
            finally:
                hold.set()
                await _stop(old_task, old)

    asyncio.run(go())


# --- new claims are suppressed for the whole drain ----------------------------


def test_a_leftover_install_flag_cannot_pause_a_fresh_install(
    make_harness,
) -> None:  # noqa: ANN001
    """The #2374 fix pin: quiesce authority belongs to one installation.

    The queued turn first proves that its own installation's marker pauses the
    real consumer gate.  That consumer is then stopped before markers are
    switched, avoiding a claim in the clear/set gap.  A fresh consumer for the
    same installation must claim and reply while only the removed
    installation's marker remains.

    ``installation_id`` is supplied as ordinary config input.  The pre-fix
    ``WorkerConfig`` ignores it, so this test still collects and reaches the
    behavioral failure: both gates address the one global key and the reply
    never arrives.
    """
    thread = "drain-fresh-install-1"
    event_id = uuid.uuid4().hex

    async def go() -> None:
        knobs = {**_KNOBS, "upgrade_quiesce_ttl_s": 30.0}
        async with make_harness(
            **knobs,
            installation_id="install-fresh",
            upgrade_revision=10,
            upgrade_legacy_quiesce=False,
        ) as h:
            h.runner.default_script = [Final(text="handled by fresh install", status=DONE)]
            store = DeliveryLeaseStore(h.async_redis, h.config)
            fresh_gate = UpgradeDrainGate(h.async_redis, h.config)
            current = Consumer(
                redis=h.async_redis,
                kernel=h.kernel,
                config=h.config,
                leases=store,
                drain=fresh_gate,
            )
            await current.ensure_group()
            await fresh_gate.request_quiesce()
            current_task = asyncio.create_task(current.run())
            replacement_task: asyncio.Task[None] | None = None
            old_gate: UpgradeDrainGate | None = None
            try:
                await h.async_redis.xadd(
                    h.config.stream,
                    to_stream_fields(
                        _qevent("queued for fresh install", thread=thread, event_id=event_id)
                    ),
                )
                await asyncio.sleep(1.0)
                assert h.runner.opened == [], (
                    "the current installation's marker did not pause its queued turn"
                )

                await _stop(current_task, current)
                await fresh_gate.clear_quiesce()

                old_config = WorkerConfig.model_validate(
                    {
                        **h.config.model_dump(),
                        "consumer_name": "worker-removed",
                        "installation_id": "install-removed",
                        "upgrade_revision": 9,
                        "upgrade_legacy_quiesce": False,
                    }
                )
                fresh_replacement_config = WorkerConfig.model_validate(
                    {
                        **h.config.model_dump(),
                        "consumer_name": "worker-fresh-replacement",
                        "installation_id": "install-fresh",
                        "upgrade_revision": 10,
                        "upgrade_legacy_quiesce": False,
                    }
                )
                old_gate = UpgradeDrainGate(h.async_redis, old_config)
                await old_gate.request_quiesce()

                replacement = Consumer(
                    redis=h.async_redis,
                    kernel=h.kernel,
                    config=fresh_replacement_config,
                    leases=store,
                    drain=UpgradeDrainGate(h.async_redis, fresh_replacement_config),
                )
                replacement_task = asyncio.create_task(replacement.run())
                await _wait_until(
                    lambda: h.sink.last_text == "handled by fresh install",
                    timeout=5.0,
                )
                assert h.runner.opened == ["queued for fresh install"]
                assert h.sink.last_text == "handled by fresh install"
            finally:
                if replacement_task is not None:
                    await _stop(replacement_task, replacement)
                elif not current_task.done():
                    await _stop(current_task, current)
                if old_gate is not None:
                    await old_gate.clear_quiesce()
                await fresh_gate.clear_quiesce()

    asyncio.run(go())


def test_current_marker_pauses_eval_new_reads_until_owned_release(
    make_harness,
) -> None:  # noqa: ANN001
    """The eval sibling reaches the same shared new-read gate as runs."""

    from aci_protocol import EvalJob

    async def go() -> None:
        async with make_harness(
            **_KNOBS,
            installation_id="install-current",
            upgrade_revision=10,
            upgrade_legacy_quiesce=False,
        ) as h:
            eval_config = WorkerConfig.model_validate(
                {
                    **h.config.model_dump(),
                    "eval_stream": f"{h.config.stream}:evals",
                    "eval_consumer_group": f"{h.config.consumer_group}-evals",
                    "eval_consumer_name": "eval-current",
                    "installation_id": "install-current",
                    "upgrade_revision": 10,
                    "upgrade_legacy_quiesce": False,
                }
            )
            gate = UpgradeDrainGate(h.async_redis, eval_config)
            seen: list[tuple[str, dict[str, str]]] = []
            consumer = _recording_eval_consumer(h, eval_config, gate, seen)
            job = EvalJob(
                agent_id=uuid.uuid4(),
                version_id=uuid.uuid4(),
                sha="deadbeef",
                suite="smoke",
                bundle_ref="bundles/acme-eval.tgz",
                requested_at="2026-09-08T00:00:00+00:00",
            )
            payload = job.model_dump_json()
            hold = asyncio.Event()

            async def record_parsed_job(item: EvalJob, claimed_id: str) -> Any:
                assert item == job
                seen.append((claimed_id, {"payload": item.model_dump_json()}))
                await hold.wait()

            consumer._run_and_report = record_parsed_job  # type: ignore[method-assign,assignment]
            await consumer.ensure_group()
            await gate.request_quiesce()
            task = asyncio.create_task(consumer._read_loop())
            entry_id = await h.async_redis.xadd(
                eval_config.eval_stream, {"payload": payload}
            )
            try:
                await asyncio.sleep(0.6)
                assert seen == []
                assert (
                    await h.async_redis.xpending(
                        eval_config.eval_stream, eval_config.eval_consumer_group
                    )
                )["pending"] == 0, "the eval row entered a PEL while claims were paused"

                await gate.clear_quiesce()
                await _wait_until(lambda: len(seen) == 1, timeout=5.0)
                assert seen == [(entry_id, {"payload": payload})]
                row = await _pending_row(
                    h,
                    stream=eval_config.eval_stream,
                    group=eval_config.eval_consumer_group,
                    entry_id=entry_id,
                )
                assert row["consumer"] == "eval-current"
                assert int(row["times_delivered"]) == 1
            finally:
                await _stop(task)
                inflight = list(consumer._inflight)
                for handler in inflight:
                    handler.cancel()
                for handler in inflight:
                    with contextlib.suppress(asyncio.CancelledError, Exception):
                        await handler
                await gate.clear_quiesce()
                await h.async_redis.delete(
                    eval_config.eval_stream,
                    eval_config.eval_dead_letter_stream_name(),
                )

    asyncio.run(go())


def test_current_marker_pauses_runs_prompt_reclaim_without_charging_delivery(
    make_harness,
) -> None:  # noqa: ANN001
    """A capable peer transfer is selection, so it stops before the PEL moves."""

    async def go() -> None:
        knobs = {
            **_KNOBS,
            "reclaim_min_idle_ms": 5000,
            "dead_consumer_idle_ms": 0,
            "consumer_heartbeat_ttl_ms": 30,
            "consumer_capability_ttl_ms": 6000,
            "upgrade_quiesce_ttl_s": 30.0,
        }
        async with make_harness(
            **knobs,
            installation_id="install-current",
            upgrade_revision=10,
            upgrade_legacy_quiesce=False,
        ) as h:
            hold = asyncio.Event()
            h.runner.hold = hold
            h.runner.default_script = [TextDelta(text="reclaimed")]
            h.runner.tail = [Final(text="prompt handled", status=DONE)]
            store = DeliveryLeaseStore(h.async_redis, h.config)
            gate = UpgradeDrainGate(h.async_redis, h.config)
            consumer_config = h.config.model_copy(
                update={"consumer_name": "runs-replacement"}
            )
            consumer = Consumer(
                redis=h.async_redis,
                kernel=h.kernel,
                config=consumer_config,
                leases=store,
                drain=gate,
            )
            await consumer.ensure_group()
            entry_id = await h.async_redis.xadd(
                h.config.stream,
                to_stream_fields(
                    _qevent(
                        "prompt recovery",
                        thread="drain-prompt-runs",
                        event_id=uuid.uuid4().hex,
                    )
                ),
            )
            assert await h.async_redis.xreadgroup(
                h.config.consumer_group,
                "runs-departed",
                {h.config.stream: ">"},
                count=1,
            )
            liveness = ConsumerLivenessStore(h.async_redis)
            await liveness.publish(
                stream=h.config.stream,
                group=h.config.consumer_group,
                consumer="runs-departed",
                heartbeat_ttl_ms=1,
                capability_ttl_ms=h.config.consumer_capability_ttl_ms,
            )
            await asyncio.sleep(0.02)
            await gate.request_quiesce()
            try:
                assert await consumer._prompt_reclaim_once() == 0
                await asyncio.sleep(
                    h.config.consumer_heartbeat_ttl_ms / 1000 + 0.02
                )
                assert await consumer._prompt_reclaim_once() == 0
                paused = await _pending_row(
                    h,
                    stream=h.config.stream,
                    group=h.config.consumer_group,
                    entry_id=entry_id,
                )
                assert paused["consumer"] == "runs-departed"
                assert int(paused["times_delivered"]) == 1
                assert h.runner.opened == []

                await gate.clear_quiesce()
                assert await consumer._prompt_reclaim_once() == 0
                await asyncio.sleep(
                    h.config.consumer_heartbeat_ttl_ms / 1000 + 0.02
                )
                assert await consumer._prompt_reclaim_once() == 1
                await _wait_until(lambda: h.runner.turn_active, timeout=5.0)
                transferred = await _pending_row(
                    h,
                    stream=h.config.stream,
                    group=h.config.consumer_group,
                    entry_id=entry_id,
                )
                assert transferred["consumer"] == "runs-replacement"
                assert int(transferred["times_delivered"]) == 2
                assert h.runner.opened == ["prompt recovery"]
            finally:
                hold.set()
                if consumer._inflight:
                    await asyncio.gather(*list(consumer._inflight))
                await gate.clear_quiesce()

    asyncio.run(go())


def test_current_marker_pauses_eval_prompt_reclaim_until_owned_release(
    make_harness,
) -> None:  # noqa: ANN001
    async def go() -> None:
        knobs = {
            **_KNOBS,
            "reclaim_min_idle_ms": 5000,
            "dead_consumer_idle_ms": 0,
            "consumer_heartbeat_ttl_ms": 30,
            "consumer_capability_ttl_ms": 6000,
            "upgrade_quiesce_ttl_s": 30.0,
        }
        async with make_harness(
            **knobs,
            installation_id="install-current",
            upgrade_revision=10,
            upgrade_legacy_quiesce=False,
        ) as h:
            eval_config = WorkerConfig.model_validate(
                {
                    **h.config.model_dump(),
                    "eval_stream": f"{h.config.stream}:evals",
                    "eval_consumer_group": f"{h.config.consumer_group}-evals",
                    "eval_consumer_name": "eval-replacement",
                    "installation_id": "install-current",
                    "upgrade_revision": 10,
                    "upgrade_legacy_quiesce": False,
                }
            )
            gate = UpgradeDrainGate(h.async_redis, eval_config)
            seen: list[tuple[str, dict[str, str]]] = []
            consumer = _recording_eval_consumer(h, eval_config, gate, seen)
            await consumer.ensure_group()
            entry_id = await h.async_redis.xadd(
                eval_config.eval_stream, {"payload": "prompt-eval"}
            )
            assert await h.async_redis.xreadgroup(
                eval_config.eval_consumer_group,
                "eval-departed",
                {eval_config.eval_stream: ">"},
                count=1,
            )
            liveness = ConsumerLivenessStore(h.async_redis)
            await liveness.publish(
                stream=eval_config.eval_stream,
                group=eval_config.eval_consumer_group,
                consumer="eval-departed",
                heartbeat_ttl_ms=1,
                capability_ttl_ms=eval_config.consumer_capability_ttl_ms,
            )
            await asyncio.sleep(0.02)
            await gate.request_quiesce()
            try:
                assert await consumer._prompt_reclaim_once() == 0
                await asyncio.sleep(
                    eval_config.consumer_heartbeat_ttl_ms / 1000 + 0.02
                )
                assert await consumer._prompt_reclaim_once() == 0
                paused = await _pending_row(
                    h,
                    stream=eval_config.eval_stream,
                    group=eval_config.eval_consumer_group,
                    entry_id=entry_id,
                )
                assert paused["consumer"] == "eval-departed"
                assert int(paused["times_delivered"]) == 1
                assert seen == []

                await gate.clear_quiesce()
                assert await consumer._prompt_reclaim_once() == 0
                await asyncio.sleep(
                    eval_config.consumer_heartbeat_ttl_ms / 1000 + 0.02
                )
                assert await consumer._prompt_reclaim_once() == 1
                assert seen == [(entry_id, {"payload": "prompt-eval"})]
                transferred = await _pending_row(
                    h,
                    stream=eval_config.eval_stream,
                    group=eval_config.eval_consumer_group,
                    entry_id=entry_id,
                )
                assert transferred["consumer"] == "eval-replacement"
                assert int(transferred["times_delivered"]) == 2
            finally:
                await gate.clear_quiesce()
                await h.async_redis.delete(
                    eval_config.eval_stream,
                    eval_config.eval_dead_letter_stream_name(),
                )

    asyncio.run(go())


def test_current_marker_pauses_eval_heavy_reclaim_before_pending_selection(
    make_harness,
) -> None:  # noqa: ANN001
    async def go() -> None:
        knobs = {
            **_KNOBS,
            "reclaim_min_idle_ms": 50,
            "upgrade_quiesce_ttl_s": 30.0,
        }
        async with make_harness(
            **knobs,
            installation_id="install-current",
            upgrade_revision=10,
            upgrade_legacy_quiesce=False,
        ) as h:
            eval_config = WorkerConfig.model_validate(
                {
                    **h.config.model_dump(),
                    "eval_stream": f"{h.config.stream}:evals",
                    "eval_consumer_group": f"{h.config.consumer_group}-evals",
                    "eval_consumer_name": "eval-heavy-replacement",
                    "installation_id": "install-current",
                    "upgrade_revision": 10,
                    "upgrade_legacy_quiesce": False,
                }
            )
            gate = UpgradeDrainGate(h.async_redis, eval_config)
            seen: list[tuple[str, dict[str, str]]] = []
            consumer = _recording_eval_consumer(h, eval_config, gate, seen)
            await consumer.ensure_group()
            entry_id = await h.async_redis.xadd(
                eval_config.eval_stream, {"payload": "heavy-eval"}
            )
            assert await h.async_redis.xreadgroup(
                eval_config.eval_consumer_group,
                "eval-old-owner",
                {eval_config.eval_stream: ">"},
                count=1,
            )
            await asyncio.sleep(eval_config.reclaim_min_idle_ms / 1000 + 0.05)
            await gate.request_quiesce()
            try:
                assert await consumer._reclaim_once() == 0
                paused = await _pending_row(
                    h,
                    stream=eval_config.eval_stream,
                    group=eval_config.eval_consumer_group,
                    entry_id=entry_id,
                )
                assert paused["consumer"] == "eval-old-owner"
                assert int(paused["times_delivered"]) == 1
                assert seen == []

                await gate.clear_quiesce()
                assert await consumer._reclaim_once() == 1
                assert seen == [(entry_id, {"payload": "heavy-eval"})]
                transferred = await _pending_row(
                    h,
                    stream=eval_config.eval_stream,
                    group=eval_config.eval_consumer_group,
                    entry_id=entry_id,
                )
                assert transferred["consumer"] == "eval-heavy-replacement"
                assert int(transferred["times_delivered"]) == 2
            finally:
                await gate.clear_quiesce()
                await h.async_redis.delete(
                    eval_config.eval_stream,
                    eval_config.eval_dead_letter_stream_name(),
                )

    asyncio.run(go())


def test_mixed_version_refusal_allows_a_pre_fix_consumer_to_claim_again(
    make_harness,
) -> None:  # noqa: ANN001
    """Clearing the bridge on refusal restores an old replica's real read path."""

    async def go() -> None:
        knobs = {
            **_KNOBS,
            "upgrade_drain_timeout_s": 0.2,
            "upgrade_quiesce_ttl_s": 30.0,
        }
        async with make_harness(**knobs) as h:
            current_config = WorkerConfig.model_validate(
                {
                    **h.config.model_dump(),
                    "eval_stream": f"{h.config.stream}:evals",
                    "eval_consumer_group": f"{h.config.consumer_group}-evals",
                    "installation_id": "install-adopted",
                    "upgrade_revision": 9,
                    "upgrade_legacy_quiesce": True,
                }
            )
            legacy_config = WorkerConfig.model_validate(
                {
                    **current_config.model_dump(),
                    "consumer_name": "worker-pre-fix",
                    "installation_id": "",
                    "upgrade_revision": None,
                    "upgrade_legacy_quiesce": False,
                }
            )
            store = DeliveryLeaseStore(h.async_redis, current_config)
            legacy_gate = UpgradeDrainGate(h.async_redis, legacy_config)
            consumer = Consumer(
                redis=h.async_redis,
                kernel=h.kernel,
                config=legacy_config,
                leases=store,
                drain=legacy_gate,
            )
            h.runner.default_script = [Final(text="pre-fix worker resumed", status=DONE)]
            await consumer.ensure_group()

            busy_id = await h.async_redis.xadd(
                current_config.stream,
                to_stream_fields(
                    _qevent(
                        "already in flight",
                        thread="drain-bridge-busy",
                        event_id=uuid.uuid4().hex,
                    )
                ),
            )
            assert await h.async_redis.xreadgroup(
                current_config.consumer_group,
                "worker-busy",
                {current_config.stream: ">"},
                count=1,
            )
            await store.acquire(
                current_config.stream,
                current_config.consumer_group,
                busy_id,
                consumer="worker-busy",
            )
            await h.async_redis.xadd(
                current_config.stream,
                to_stream_fields(
                    _qevent(
                        "claim after refusal",
                        thread="drain-bridge-ready",
                        event_id=uuid.uuid4().hex,
                    )
                ),
            )

            task: asyncio.Task[None] | None = None
            try:
                assert await run_gate(current_config, mode="drain") == 1
                assert await legacy_gate.is_quiescing() is False

                task = asyncio.create_task(consumer.run())
                await _wait_until(
                    lambda: h.sink.last_text == "pre-fix worker resumed",
                    timeout=5.0,
                )
                assert h.runner.opened == ["claim after refusal"]
            finally:
                if task is not None:
                    await _stop(task, consumer)
                await UpgradeDrainGate(h.async_redis, current_config).clear_quiesce()
                await legacy_gate.clear_quiesce()
                await h.async_redis.delete(current_config.eval_stream)

    asyncio.run(go())


def test_a_quiesced_consumer_claims_nothing_and_resumes_when_released(make_harness) -> None:  # noqa: ANN001
    """"...while preventing new claims during the drain."

    A wait that kept admitting work could never terminate under load, and a
    replacement Pod that comes up mid-roll must not reclaim the delivery a
    still-draining replica is settling. Both are the same suppression, so both
    are asserted here: nothing is claimed while the flag is set, and the very
    same entry is claimed once it clears.
    """
    thread = "drain-quiesce-1"
    event_id = uuid.uuid4().hex

    async def go() -> None:
        async with make_harness(**_KNOBS) as h:
            store = DeliveryLeaseStore(h.async_redis, h.config)
            gate = UpgradeDrainGate(h.async_redis, h.config)
            consumer = Consumer(
                redis=h.async_redis,
                kernel=h.kernel,
                config=h.config,
                leases=store,
                drain=gate,
            )
            h.runner.default_script = [Final(text="handled", status=DONE)]

            await consumer.ensure_group()
            await gate.request_quiesce()
            task = asyncio.create_task(consumer.run())
            try:
                await h.async_redis.xadd(
                    h.config.stream,
                    to_stream_fields(_qevent("do the thing", thread=thread, event_id=event_id)),
                )
                # Long enough for several read-loop cycles (read_block_ms is
                # 100ms in the harness) and several maintenance ticks.
                await asyncio.sleep(1.5)
                assert h.runner.opened == [], (
                    "a quiesced consumer claimed new work during the drain"
                )

                await gate.clear_quiesce()
                await _wait_until(lambda: len(h.runner.opened) == 1, timeout=30.0)
                assert h.runner.opened == ["do the thing"]
            finally:
                await _stop(task, consumer)

    asyncio.run(go())


def test_a_quiesced_consumer_does_not_reclaim_a_departed_replicas_entry(make_harness) -> None:  # noqa: ANN001
    """The incident's own mechanism, blocked.

    A reclaim IS a new claim: it moves a peer's pending entry into this consumer
    and dispatches it. That is precisely how the replacement Pod took the
    delivery at 11:16:31 and escalated it. The read-loop suppression above
    cannot cover this -- a reclaimed entry never goes through ``XREADGROUP`` --
    so it is asserted separately, against an entry left pending by a replica
    that is already gone (no live lease, idle past the reclaim threshold).

    Red on removing the ``_claims_paused`` guard in ``Consumer._maintenance_loop``.
    """
    thread = "drain-reclaim-1"
    event_id = uuid.uuid4().hex

    async def go() -> None:
        async with make_harness(**_KNOBS) as h:
            store = DeliveryLeaseStore(h.async_redis, h.config)
            gate = UpgradeDrainGate(h.async_redis, h.config)
            consumer = Consumer(
                redis=h.async_redis,
                kernel=h.kernel,
                config=h.config,
                leases=store,
                drain=gate,
            )
            h.runner.default_script = [Final(text="handled", status=DONE)]

            await consumer.ensure_group()
            await h.async_redis.xadd(
                h.config.stream,
                to_stream_fields(_qevent("finish the thing", thread=thread, event_id=event_id)),
            )
            # Read it into a consumer name that no longer exists: the departed
            # replica's PEL row, holding no lease.
            await h.async_redis.xreadgroup(
                h.config.consumer_group,
                "worker-departed",
                {h.config.stream: ">"},
                count=1,
            )
            await asyncio.sleep(h.config.reclaim_min_idle_ms / 1000 + 0.1)

            await gate.request_quiesce()
            task = asyncio.create_task(consumer.run())
            try:
                await asyncio.sleep(1.5)
                assert h.runner.opened == [], (
                    "a quiesced consumer reclaimed a departed replica's delivery"
                )

                await gate.clear_quiesce()
                await _wait_until(lambda: len(h.runner.opened) == 1, timeout=30.0)
                assert h.runner.opened == ["finish the thing"]
            finally:
                await _stop(task, consumer)

    asyncio.run(go())
