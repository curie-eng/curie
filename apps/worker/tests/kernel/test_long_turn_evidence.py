"""The controlled long-run EVIDENCE harness for ADR-0131's Phase-4 exit gate.

The gate: *"A controlled 12-15 minute test and a configured 30-minute budget
produce regular progress, accept steering, survive the tested recovery event,
and create exactly one terminal result."*

This module is **opt-in** and skipped by the normal suite, following the same
convention ``apps/worker/CLAUDE.md`` documents for ``CURIE_SANDBOX_E2E=1``: a
13-minute wall-clock run has no business in the per-commit gate, and a gate that
takes 13 minutes stops being run. Set ``CURIE_LONG_TURN_EVIDENCE=1`` to collect
the evidence.

Env knobs (all optional):

- ``CURIE_LONG_TURN_SECONDS`` -- how long the turn is held open. Default 780
  (13 minutes), the gate's window. Set ~1700 for the configured-30-minute-budget
  run; set 45-90 to smoke the harness itself without burning the wall clock.
- ``CURIE_LONG_TURN_SAMPLE_S`` -- sampling cadence. Default 15.
- ``CURIE_LONG_TURN_EVIDENCE_LOG`` -- JSONL destination. Defaults to a tmp path,
  which is printed.
- ``CURIE_LONG_TURN_STEER_AT_S`` -- when the mid-run steer is issued. Default is
  620 (just past the old runner deadline) when the run is long enough to reach
  it, otherwise 60% of the hold.
- ``CURIE_LONG_TURN_RUNNER_CEILING_S`` -- the runner client's per-request
  ceiling. Defaults to the larger of the production ``runner_total_timeout_s``
  (600) and ``CURIE_LONG_TURN_SECONDS + 60`` -- i.e. it tracks whatever hold
  you configure, so the harness passes at its own defaults AND stays correct
  for a short smoke run. Override it explicitly to rehearse the
  request-recycle boundary (a ceiling BELOW the hold) instead.

**A continuous long turn needs two knobs raised together, not one.** The
delivery budget (``delivery_budget_s``, ADR-0039) bounds the WHOLE delivery,
retries included; the runner ceiling (``runner_total_timeout_s``) bounds a
SINGLE request within it. Raising only the budget does not buy one continuous
turn past the ceiling -- it buys retries: the request is cut at the ceiling,
the kernel retries, the retry steers the still-live turn, and the delivery
settles there instead of running continuously. A genuinely continuous hold
therefore requires raising the per-request ceiling to at least the hold
length, in addition to the delivery budget. Assertion 0 below is what
originally taught us this relationship, and its message names both knobs.

**Why the assertions are conditional on the elapsed time.** The 600s and 900s
boundaries are the two clocks ADR-0131 replaced (the runner's flat HTTP deadline
and the PEL-idle steal window). A run configured shorter than a boundary never
reaches it, and asserting a crossing that did not happen would be a green test
about nothing. Each boundary assertion therefore fires only when the run
actually got there, and the summary block reports which ones were exercised.

**The per-request ceiling is a real boundary this run can hit.** A runner
request is bounded by ``min(runner_total_timeout_s, remaining budget)`` and that
bound is aiohttp's ``total``, which streamed progress does not reset. A hold
longer than the ceiling therefore does NOT produce one continuous 13-minute
runner request: the request is cut, the kernel retries, the retry steers the
retained live turn, and the delivery settles there. Assertion 0 below refuses to
let that pass as evidence, and its message names the knob.

**Nothing here is mocked.** Real Valkey, the real ``DeliveryLeaseStore`` Lua,
two real ``Consumer`` replicas with distinct names on one group running their own
read and maintenance loops, and the in-process fake runner from ``conftest``.
The negative control below is driven through the same real machinery: the whole
point of a falsifiable control is that it would be worthless if the failure it
demonstrates were produced by a stub.
"""

from __future__ import annotations

import asyncio
import dataclasses
import functools
import json
import os
import resource
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

import aiohttp
import pytest
from aci_protocol import Final, SessionStatus, TextDelta
from curie_dispatcher.queue import to_stream_fields
from curie_worker.consumer import Consumer
from curie_worker.delivery_lease import DeliveryLeaseStore

from .conftest import _ProcessEventSpy

# importlib import mode does not add the test root to sys.path.
sys.path.insert(0, str(Path(__file__).parent.parent))

from queue_fixtures import qevent, wait_until  # noqa: E402

_qevent = functools.partial(qevent, received_at="2026-08-28T00:00:00+00:00")
_wait_until = functools.partial(wait_until, timeout=30.0, interval=0.05)

pytestmark = pytest.mark.skipif(
    "CURIE_LONG_TURN_EVIDENCE" not in os.environ,
    reason=(
        "long-run ADR-0131 evidence harness; opt in with CURIE_LONG_TURN_EVIDENCE=1 "
        "(a 13-minute wall-clock run does not belong in the per-commit suite)"
    ),
)

DONE = SessionStatus.DONE

# The two clocks ADR-0131 replaced. Named here rather than inlined because every
# conditional assertion below is about one of them.
OLD_RUNNER_DEADLINE_S = 600.0
OLD_PEL_IDLE_RECLAIM_S = 900.0

# Production-shaped knobs, exactly as the exit gate specifies them. Nothing is
# compressed: this run is about real-time behavior at real settings, which is
# what separates it from the compressed regressions in
# ``test_delivery_ownership.py``.
#
# ``reclaim_min_idle_ms`` is pinned to its PRODUCTION default (900000) rather
# than the harness's 50ms. That is load-bearing: ``_reclaim_once`` runs
# ``XAUTOCLAIM`` BEFORE its liveness check, so a compressed idle window would
# bump ``times_delivered`` on a healthy peer's entry every maintenance tick and
# make the JUSTID assertion below unprovable. At the production window the
# heartbeat's ``XCLAIM ... JUSTID`` keeps PEL idle near the heartbeat interval
# and the entry never becomes a reclaim candidate at all -- which is the
# property under test.
_PROD_KNOBS: dict[str, object] = {
    "delivery_budget_s": 1800.0,
    "delivery_lease_ttl_s": 45.0,
    "delivery_lease_heartbeat_s": 10.0,
    "reclaim_interval_s": 30.0,
    "runner_total_timeout_s": 600.0,
    "reclaim_min_idle_ms": 900000,
}

# The negative control's compressed clocks. Every ratio the WorkerConfig
# validators enforce is preserved (TTL >= 3x heartbeat, reclaim interval < TTL,
# runner ceiling <= budget); the budget sits at its configurable floor because
# nothing in the control waits a budget out. ``reclaim_min_idle_ms`` IS
# compressed here on purpose -- the control needs the reclaim path to fire, and
# it costs seconds rather than the 15 minutes of PEL idle production would need.
_CONTROL_TTL_S = 1.0
_CONTROL_KNOBS: dict[str, object] = {
    "delivery_budget_s": 60.0,
    "delivery_lease_ttl_s": _CONTROL_TTL_S,
    "delivery_lease_heartbeat_s": 0.3,
    "reclaim_interval_s": 0.25,
    "runner_total_timeout_s": 30.0,
    "reclaim_min_idle_ms": 300,
    "dead_consumer_idle_ms": 300,
}


def _env_float(name: str, default: float) -> float:
    raw = os.environ.get(name)
    return default if raw in (None, "") else float(str(raw))


class _DispatchSpy:
    """Counts the entries one replica's OWN loops handed to its handler.

    Both entry points have to be wrapped. ``_read_loop`` resolves
    ``self._dispatch`` at call time, but ``DeliverySpec.handler`` was bound to
    the original method in ``Consumer.__init__``, so the reclaim path -- the
    one that actually matters here, because cross-replica takeover arrives
    through reclaim and never through a read -- would otherwise slip past the
    spy and make this recorder silently blind to the single case it exists to
    observe. ``DeliverySpec`` is frozen, hence ``dataclasses.replace``.
    """

    def __init__(self, consumer: Consumer) -> None:
        self._inner = consumer._dispatch
        self.ids: list[str] = []
        consumer._dispatch = self  # type: ignore[method-assign]
        consumer._delivery = dataclasses.replace(consumer._delivery, handler=self)

    async def __call__(self, entry_id: str, fields: dict[str, str]) -> None:
        self.ids.append(entry_id)
        await self._inner(entry_id, fields)


def _proc_usage() -> dict[str, float]:
    """Process CPU seconds, RSS, and swap, with no new dependency.

    ``ru_maxrss`` is a high-water mark, so it can only grow; ``VmRSS`` from
    ``/proc/self/status`` is the instantaneous value and is what makes a leak
    across a 13-minute hold visible. Both are recorded because a run whose peak
    and current diverge tells a different story from one where they track.
    ``VmSwap`` is read from the same file for free: a long hold that starts
    swapping is a distinct failure shape from one that just grows RSS.
    """
    ru = resource.getrusage(resource.RUSAGE_SELF)
    rss_kb = 0.0
    swap_kb = 0.0
    try:
        for line in Path("/proc/self/status").read_text().splitlines():
            if line.startswith("VmRSS:"):
                rss_kb = float(line.split()[1])
            elif line.startswith("VmSwap:"):
                swap_kb = float(line.split()[1])
    except OSError:  # pragma: no cover - /proc absent
        pass
    return {
        "cpu_s": round(ru.ru_utime + ru.ru_stime, 3),
        "rss_kb": rss_kb,
        "peak_rss_kb": float(ru.ru_maxrss),
        "swap_kb": swap_kb,
    }


def _container_count() -> int | None:
    """Running container count via ``docker ps``, read defensively.

    This harness must not become dependent on Docker being present: a missing
    binary, no daemon, or no permissions all record ``None`` instead of
    failing the run.
    """
    try:
        result = subprocess.run(
            ["docker", "ps", "-q"],
            capture_output=True,
            text=True,
            timeout=5,
            check=True,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    return len([line for line in result.stdout.splitlines() if line.strip()])


async def _pel_row(h: Any, entry_id: str) -> dict[str, Any] | None:
    rows = await h.async_redis.xpending_range(
        h.config.stream, h.config.consumer_group, min=entry_id, max=entry_id, count=1
    )
    return dict(rows[0]) if rows else None


def _evidence_log_path(tmp_path: Path) -> Path:
    configured = os.environ.get("CURIE_LONG_TURN_EVIDENCE_LOG")
    if configured:
        path = Path(configured)
        path.parent.mkdir(parents=True, exist_ok=True)
        return path
    return tmp_path / "long-turn-evidence.jsonl"


# --- Test 1: the positive run -------------------------------------------------


def test_a_long_delivery_survives_both_old_clocks_and_settles_exactly_once(
    make_harness, tmp_path: Path
) -> None:
    """The Phase-4 exit-gate evidence run (ADR-0131).

    Two real replicas on one group, production knobs, one turn held open for
    ``CURIE_LONG_TURN_SECONDS`` (default 780s). Samples every 15s to JSONL, then
    asserts the ADR's five delivery-level claims.

    Reverts that turn this red, each named against the property it breaks:

    - Remove the background heartbeat task in
      ``StreamConsumer._delivery_lease`` -> the lease expires mid-turn, the peer
      becomes eligible, and both the second-replica and the terminal-result
      assertions fail.
    - Drop ``JUSTID`` from the same-owner ``XCLAIM`` in ``_HEARTBEAT_LUA`` ->
      ``times_delivered`` climbs one per heartbeat and the ADR-0039 budget is
      dead-lettered out from under a healthy turn.
    - Remove the ``EXISTS`` refusal from ``_ACQUIRE_LUA``, or the
      ``_lease_is_live`` guards in ``_reclaim_once`` /
      ``_dead_letter_over_cap`` -> the peer replica enters the handler.
    - Revert the ``HSETNX`` on ``deadline_ms`` to ``HSET`` -> the remaining
      budget stops declining monotonically across the run.
    - Revert C8's ``remaining_s`` plumbing into ``RunnerClient.steer`` in a way
      that derives a non-positive per-request budget -> the mid-run steer is
      refused, breaking kernel rule 1 ("steering is never blocked").

    The negative control for the whole run lives in the companion test below:
    without it, "nothing bad happened in 13 minutes" would be indistinguishable
    from a harness that cannot observe anything bad at all.
    """
    hold_s = _env_float("CURIE_LONG_TURN_SECONDS", 780.0)
    sample_s = _env_float("CURIE_LONG_TURN_SAMPLE_S", 15.0)
    # Self-consistent default: assertion 0 requires the ceiling to accommodate
    # the hold (a request cut short of the hold makes the delivery settle
    # early), so the default tracks whatever hold is configured rather than
    # pinning to the production value alone.
    ceiling_s = _env_float(
        "CURIE_LONG_TURN_RUNNER_CEILING_S",
        max(float(_PROD_KNOBS["runner_total_timeout_s"]), hold_s + 60.0),  # type: ignore[arg-type]
    )
    default_steer_at = (
        OLD_RUNNER_DEADLINE_S + 20.0 if hold_s > OLD_RUNNER_DEADLINE_S + 60.0 else hold_s * 0.6
    )
    steer_at_s = _env_float("CURIE_LONG_TURN_STEER_AT_S", default_steer_at)
    log_path = _evidence_log_path(tmp_path)
    thread = "evidence-long-1"
    long_event_id = "evidence-long-1"

    async def go() -> None:
        async with make_harness(**_PROD_KNOBS) as h:
            store = DeliveryLeaseStore(h.async_redis, h.config)
            cfg_a = h.config.model_copy(update={"consumer_name": "replica-a"})
            cfg_b = h.config.model_copy(update={"consumer_name": "replica-b"})
            consumer_a = Consumer(redis=h.async_redis, kernel=h.kernel, config=cfg_a, leases=store)
            consumer_b = Consumer(redis=h.async_redis, kernel=h.kernel, config=cfg_b, leases=store)
            dispatch_a = _DispatchSpy(consumer_a)
            dispatch_b = _DispatchSpy(consumer_b)
            spy = _ProcessEventSpy(h.kernel)

            # The harness's RunnerClient is built with a 30s ceiling, which no
            # existing test outgrows and this one does within half a minute.
            # Raised to the CONFIGURED per-request ceiling so the run measures
            # ADR-0131's real shape -- a long delivery made of bounded requests
            # -- rather than a harness artifact. Both the session default and the
            # budget-derived override are set: the override is what a leased
            # delivery uses, the default is the fallback, and leaving them
            # inconsistent would make a plumbing regression look like a timeout.
            runner_client = h.kernel._runner
            runner_client._total_timeout_s = ceiling_s
            runner_client._session._timeout = aiohttp.ClientTimeout(
                total=ceiling_s, connect=runner_client._connect_timeout_s, sock_read=ceiling_s
            )

            hold = asyncio.Event()
            h.runner.hold = hold
            h.runner.default_script = [TextDelta(text="working on the long run")]
            h.runner.tail = [Final(text="long run complete", status=DONE)]

            # The group is created at ``$`` (see ``Consumer.ensure_group``), so it
            # MUST exist before the turn is enqueued. Leaving it to the replicas'
            # own startup is a race that loses the entry outright whenever the
            # XADD wins -- a silently empty run, not a failure.
            await consumer_a.ensure_group()
            task_a = asyncio.create_task(consumer_a.run())
            task_b = asyncio.create_task(consumer_b.run())
            samples: list[dict[str, Any]] = []
            started = time.monotonic()
            try:
                await h.async_redis.xadd(
                    h.config.stream,
                    to_stream_fields(
                        _qevent("start the long run", thread=thread, event_id=long_event_id)
                    ),
                )
                await _wait_until(lambda: h.runner.turn_active, timeout=60.0)
                started = time.monotonic()

                # Whichever replica won the read is "the owner"; the other is the
                # peer whose intrusion the fence must prevent. Deriving it rather
                # than assigning it keeps the race real: two replicas on one
                # group genuinely compete for the read.
                inflight = set(consumer_a._inflight_ids) | set(consumer_b._inflight_ids)
                assert len(inflight) == 1, f"expected one in-flight entry, saw {inflight!r}"
                entry_id = inflight.pop()
                owner = "replica-a" if entry_id in consumer_a._inflight_ids else "replica-b"
                peer_dispatch = dispatch_b if owner == "replica-a" else dispatch_a

                lease = spy.leases_for(long_event_id)[0]
                assert lease is not None, "the kernel was handed no lease"
                first_remaining = lease.remaining_s()

                steered_at: float | None = None
                steer_accepted: str | None = None
                with log_path.open("w", encoding="utf-8") as log:

                    async def emit(note: str) -> dict[str, Any]:
                        elapsed = time.monotonic() - started
                        row = await _pel_row(h, entry_id)
                        state = await store.peek(h.config.stream, h.config.consumer_group, entry_id)
                        sample: dict[str, Any] = {
                            "note": note,
                            "elapsed_s": round(elapsed, 2),
                            "owner": owner,
                            "entry_id": entry_id,
                            "pel_present": row is not None,
                            "times_delivered": int(row["times_delivered"]) if row else None,
                            "pel_idle_ms": int(row["time_since_delivered"]) if row else None,
                            "pel_consumer": str(row["consumer"]) if row else None,
                            "lease_live": await store.is_live(
                                h.config.stream, h.config.consumer_group, entry_id
                            ),
                            "lease_lost": lease.lost.is_set(),
                            "generation": int(state["gen"]) if "gen" in state else None,
                            "remaining_budget_s": round(lease.remaining_s(), 2),
                            "handler_entries": spy.entries_for(long_event_id),
                            "peer_dispatches": len(peer_dispatch.ids),
                            "peer_entered_handler": spy.entries_for(long_event_id) > 1,
                            "runner_turns_opened": len(h.runner.opened),
                            "runner_steers": len(h.runner.steers),
                            "runner_turn_active": h.runner.turn_active,
                            "sink_completions": len(h.sink.completions),
                            "container_count": _container_count(),
                            **_proc_usage(),
                        }
                        samples.append(sample)
                        log.write(json.dumps(sample) + "\n")
                        log.flush()
                        return sample

                    await emit("start")
                    while True:
                        elapsed = time.monotonic() - started
                        if elapsed >= hold_s:
                            break
                        if steered_at is None and elapsed >= steer_at_s:
                            # Kernel rule 1: a follow-up on a live thread is a
                            # STEER and is NEVER blocked. Rule 2: if the turn
                            # finished underneath us the steer gets a 409 and the
                            # kernel opens a fresh turn instead of retrying it.
                            # Either outcome is "accepted"; a raise or a hang is
                            # not, which is why it is bounded here.
                            steers_before = len(h.runner.steers)
                            opened_before = len(h.runner.opened)
                            steer_started = time.monotonic()
                            await asyncio.wait_for(
                                h.kernel.process_event(
                                    _qevent("also check the disk usage", thread=thread)
                                ),
                                timeout=120.0,
                            )
                            steered_at = time.monotonic() - started
                            if len(h.runner.steers) > steers_before:
                                steer_accepted = "steered"
                            elif len(h.runner.opened) > opened_before:
                                steer_accepted = "409-fallback-new-turn"
                            await emit(
                                f"steer:{steer_accepted or 'rejected'}"
                                f":{round(time.monotonic() - steer_started, 2)}s"
                            )
                            continue
                        await asyncio.sleep(min(sample_s, max(0.1, hold_s - elapsed)))
                        if time.monotonic() - started >= hold_s:
                            break
                        await emit("sample")

                    final_sample = await emit("hold-release")
                    ran_for = final_sample["elapsed_s"]

                    hold.set()
                    await asyncio.gather(
                        *list(consumer_a._inflight),
                        *list(consumer_b._inflight),
                        return_exceptions=True,
                    )
                    await emit("settled")

                # -- the ADR's claims -------------------------------------------
                delivered = [s["times_delivered"] for s in samples if s["times_delivered"]]
                idles = [s["pel_idle_ms"] for s in samples if s["pel_idle_ms"] is not None]
                remaining = [s["remaining_budget_s"] for s in samples]

                pre_release = [s for s in samples if s["note"] != "settled"]
                settled_at = next(
                    (s["elapsed_s"] for s in pre_release if not s["pel_present"]), None
                )

                summary = {
                    "log": str(log_path),
                    "delivery_settled_at_s": settled_at,
                    "samples": len(samples),
                    "ran_for_s": ran_for,
                    "configured_hold_s": hold_s,
                    "runner_request_ceiling_s": ceiling_s,
                    "owner": owner,
                    "crossed_600s": ran_for > OLD_RUNNER_DEADLINE_S,
                    "crossed_900s": ran_for > OLD_PEL_IDLE_RECLAIM_S,
                    "times_delivered": sorted(set(delivered)),
                    "max_pel_idle_ms": max(idles) if idles else None,
                    "handler_entries": spy.entries_for(long_event_id),
                    "peer_dispatches": len(peer_dispatch.ids),
                    "runner_turns_opened": h.runner.opened,
                    "runner_steers": h.runner.steers,
                    "steered_at_s": steered_at,
                    "steer_outcome": steer_accepted,
                    "budget_first_remaining_s": round(first_remaining, 2),
                    "budget_last_remaining_s": remaining[-1],
                    "completions": [(c.event_id, c.outcome) for c in h.sink.completions],
                    "container_count": _container_count(),
                    **_proc_usage(),
                }
                print("\n=== ADR-0131 LONG-TURN EVIDENCE ===")
                print(json.dumps(summary, indent=2, sort_keys=True))
                print("=== END EVIDENCE ===\n")

                # 0. The delivery was STILL IN FLIGHT when the hold was released.
                #    Without this the whole run is unfalsifiable: every assertion
                #    below is also satisfied by a delivery that quietly settled
                #    at the per-request ceiling and left the harness measuring an
                #    idle system for the remaining minutes.
                assert final_sample["pel_present"] and final_sample["lease_live"], (
                    f"the delivery settled at ~{settled_at}s, well before the hold was "
                    f"released at {ran_for}s, so this run measured an idle system for the "
                    "rest of its window. One runner request is hard-capped at "
                    "min(runner_total_timeout_s, remaining budget) = "
                    f"{ceiling_s}s -- aiohttp's ``total``, which model progress does NOT "
                    "reset. Past it the kernel retries, the retry finds the retained turn "
                    "still live and STEERS it (kernel rule 1), and the steer's attempt "
                    "returns terminal_ok, so the delivery settles 'delivered' there. The "
                    "1800s budget therefore does not extend one CONTINUOUS turn beyond "
                    "that ceiling. Raise CURIE_LONG_TURN_RUNNER_CEILING_S above "
                    "CURIE_LONG_TURN_SECONDS to evidence the single-request shape, or "
                    "report this as the gate's finding."
                )

                # 1. The delivery outlived the clocks ADR-0131 replaced. Both are
                #    conditional: a run configured shorter than a boundary never
                #    reached it, and asserting a crossing that did not happen
                #    would be a green assertion about nothing.
                if ran_for > OLD_RUNNER_DEADLINE_S:
                    assert h.sink.completions, (
                        "the delivery produced no terminal result after living past the "
                        f"old {OLD_RUNNER_DEADLINE_S}s runner deadline"
                    )
                if ran_for > OLD_PEL_IDLE_RECLAIM_S:
                    assert spy.entries_for(long_event_id) == 1, (
                        "a second handler entry appeared after the old "
                        f"{OLD_PEL_IDLE_RECLAIM_S}s PEL-idle reclaim threshold: the "
                        "lease did not keep the healthy turn un-reclaimable"
                    )
                # The always-available form of the same property: the heartbeat's
                # XCLAIM ... JUSTID keeps PEL idle pinned near the heartbeat
                # interval, so the entry never even approaches the reclaim
                # window, whatever the run's length.
                if idles:
                    assert max(idles) < float(_PROD_KNOBS["delivery_lease_ttl_s"]) * 1000, (  # type: ignore[arg-type]
                        f"PEL idle reached {max(idles)}ms, past the lease TTL: the "
                        "heartbeat is not resetting same-owner idle with JUSTID"
                    )

                # 2. XCLAIM ... JUSTID: not one delivery of the ADR-0039 budget
                #    was burned across the whole run.
                assert len(set(delivered)) <= 1, (
                    f"times_delivered moved across the run ({sorted(set(delivered))}): the "
                    "heartbeat's same-owner XCLAIM is burning the ADR-0039 delivery budget"
                )

                # 3. One delivery, one execution. The peer replica ran its own
                #    read and maintenance loops for the entire hold and never
                #    entered the handler.
                assert spy.entries_for(long_event_id) == 1, (
                    f"the handler was entered {spy.entries_for(long_event_id)} times for one "
                    "delivery: the fence let a second replica run the same turn"
                )
                assert entry_id not in peer_dispatch.ids, (
                    f"the peer replica dispatched entry {entry_id}: reclaim did not "
                    "check the live lease before dispatching"
                )

                # 4. Steering was accepted mid-run (kernel rule 1, with rule 2's
                #    409 fallback as the other legal outcome).
                assert steered_at is not None, "the mid-run steer was never issued"
                assert steer_accepted is not None, (
                    "the mid-run steer was neither delivered to the live turn nor "
                    "converted into a fresh turn by the 409 finish-race fallback: "
                    "steering was blocked, which kernel rule 1 forbids"
                )
                if ran_for > OLD_RUNNER_DEADLINE_S:
                    assert steered_at > OLD_RUNNER_DEADLINE_S, (
                        f"the steer landed at {steered_at}s, before the old "
                        f"{OLD_RUNNER_DEADLINE_S}s boundary this run exists to cross"
                    )

                # 5. Exactly ONE user-visible terminal effect, and the delivery
                #    is settled off the group.
                terminal = [c for c in h.sink.completions if c.event_id == long_event_id]
                assert len(terminal) == 1, (
                    f"expected exactly one terminal result for {long_event_id}, got "
                    f"{[(c.event_id, c.outcome) for c in h.sink.completions]}"
                )
                assert terminal[0].outcome == "delivered", (
                    f"the long delivery settled as {terminal[0].outcome!r}, not delivered"
                )
                assert await _pel_row(h, entry_id) is None, (
                    "the delivery was never acked: it is still pending after settling"
                )
                assert not await store.is_live(
                    h.config.stream, h.config.consumer_group, entry_id
                ), "the lease outlived the settled delivery"

                # 6. The budget was CONSUMED, never restarted. A reverted HSETNX
                #    shows up here as a remaining budget that stopped declining.
                assert remaining[-1] < first_remaining, (
                    "the remaining delivery budget did not decline across the run: "
                    "something re-minted the deadline instead of inheriting it"
                )
            finally:
                consumer_a.request_stop()
                consumer_b.request_stop()
                hold.set()
                for task in (task_a, task_b):
                    task.cancel()
                await asyncio.gather(task_a, task_b, return_exceptions=True)

    asyncio.run(go())


# --- Test 2: the falsifiable negative control ---------------------------------


def test_control_without_heartbeat_renewal_a_second_replica_does_take_the_turn(
    make_harness, tmp_path: Path
) -> None:
    """THE CONTROL. The pre-fix failure, reproduced and asserted POSITIVELY.

    The positive run above proves nothing on its own: "no duplicate execution in
    13 minutes" is exactly what a harness that cannot see a duplicate execution
    also reports. So the same machinery -- real Valkey, the real Lua, two real
    ``Consumer`` replicas, the real reclaim loop -- is run with the owner's
    heartbeat effectively disabled (its store is handed a heartbeat interval far
    longer than the lease TTL, which is precisely what deleting the renewal does
    to a live turn). The owner's lease then expires UNDER a still-running turn,
    and the test passes only when the bad thing is observed: the peer replica
    enters the handler for the same delivery and ``times_delivered`` increases.

    Clocks are compressed so this costs seconds, not minutes. Real-time scale is
    what the positive run is for; falsification does not need it.

    Nothing is stubbed. The heartbeat is disabled by CONFIGURATION, so every
    Valkey round trip, every Lua guard, and the reclaim path itself are the
    production ones. A mocked store here would assert only that the mock did
    what the mock was told, which is the failure mode this control exists to
    rule out for the run above.

    What turns THIS test red is over-fencing: if lease expiry stopped working,
    or a reclaim path grew an unconditional skip, an abandoned delivery would
    never be recovered by anyone and the fence would have traded duplicate
    execution for permanent stranding. That is the opposite regression, and it
    is exactly as bad.
    """
    log_path = tmp_path / "long-turn-control.jsonl"
    thread = "evidence-control-1"
    control_event_id = "evidence-control-1"

    async def go() -> None:
        async with make_harness(**_CONTROL_KNOBS) as h:
            cfg_a = h.config.model_copy(update={"consumer_name": "control-a"})
            cfg_b = h.config.model_copy(update={"consumer_name": "control-b"})
            # The whole mutation, in one line: the OWNER's store believes it must
            # renew once an hour, so the 1s lease expires under a live turn. The
            # peer's store is untouched, so recovery runs at production fidelity.
            # ``model_copy`` deliberately bypasses the config validator that
            # forbids this ratio -- a config this broken is not reachable through
            # the constructor, which is the point of the validator and the reason
            # the mutation has to be applied here.
            store_a = DeliveryLeaseStore(
                h.async_redis, cfg_a.model_copy(update={"delivery_lease_heartbeat_s": 3600.0})
            )
            store_b = DeliveryLeaseStore(h.async_redis, cfg_b)
            consumer_a = Consumer(
                redis=h.async_redis, kernel=h.kernel, config=cfg_a, leases=store_a
            )
            consumer_b = Consumer(
                redis=h.async_redis, kernel=h.kernel, config=cfg_b, leases=store_b
            )
            dispatch_b = _DispatchSpy(consumer_b)
            spy = _ProcessEventSpy(h.kernel)
            await consumer_a.ensure_group()

            hold = asyncio.Event()
            h.runner.hold = hold
            h.runner.default_script = [TextDelta(text="working")]
            h.runner.tail = [Final(text="control done", status=DONE)]

            task_b: asyncio.Task[None] | None = None
            try:
                await h.async_redis.xadd(
                    h.config.stream,
                    to_stream_fields(
                        _qevent("control run", thread=thread, event_id=control_event_id)
                    ),
                )
                rows = await h.async_redis.xreadgroup(
                    h.config.consumer_group, "control-a", {h.config.stream: ">"}, count=1
                )
                entry_id, fields = rows[0][1][0]
                before = int((await _pel_row(h, entry_id))["times_delivered"])  # type: ignore[index]

                await consumer_a._dispatch(entry_id, dict(fields))
                await _wait_until(lambda: h.runner.turn_active, timeout=30.0)
                assert spy.entries_for(control_event_id) == 1
                assert await store_b.is_live(h.config.stream, h.config.consumer_group, entry_id), (
                    "the owner never took a lease at all; the control would be vacuous"
                )

                # The lease expires under the still-running turn.
                await asyncio.sleep(_CONTROL_TTL_S + 0.5)
                assert h.runner.turn_active, (
                    "the turn ended before the lease expired; the control never "
                    "reached the state it exists to demonstrate"
                )
                expired = not await store_b.is_live(
                    h.config.stream, h.config.consumer_group, entry_id
                )
                assert expired, (
                    "the un-renewed lease never expired, so the fence was never "
                    "actually open and anything observed below proves nothing"
                )

                # Now the peer's real reclaim loop is the only actor.
                task_b = asyncio.create_task(consumer_b.run())
                await _wait_until(lambda: spy.entries_for(control_event_id) >= 2, timeout=30.0)

                after_row = await _pel_row(h, entry_id)
                after = int(after_row["times_delivered"]) if after_row else before
                record = {
                    "entry_id": entry_id,
                    "handler_entries": spy.entries_for(control_event_id),
                    "peer_dispatches": dispatch_b.ids,
                    "times_delivered_before": before,
                    "times_delivered_after": after,
                    "pel_consumer": str(after_row["consumer"]) if after_row else None,
                    "runner_turn_active_after_takeover": h.runner.turn_active,
                    "generations": (
                        await DeliveryLeaseStore(h.async_redis, cfg_b).peek(
                            h.config.stream, h.config.consumer_group, entry_id
                        )
                    ),
                }
                log_path.write_text(json.dumps(record) + "\n", encoding="utf-8")
                print("\n=== ADR-0131 NEGATIVE CONTROL (pre-fix failure) ===")
                print(json.dumps(record, indent=2, sort_keys=True))
                print("=== END CONTROL ===\n")

                # Asserted POSITIVELY: this test passes because the bad thing
                # happened. Both halves of the observed defect are named.
                assert record["handler_entries"] >= 2, (
                    "the peer replica never entered the handler even with the owner's "
                    "lease expired under a live turn -- this control cannot falsify "
                    "the positive run, so the positive run proves nothing"
                )
                assert entry_id in dispatch_b.ids, (
                    "the second handler entry did not come from the peer replica's "
                    "own reclaim loop, so it is not the cross-replica defect"
                )
                assert after > before, (
                    "times_delivered did not increase on takeover: without the "
                    "heartbeat's XCLAIM ... JUSTID the ADR-0039 budget must be burned "
                    "by the reclaim, and it was not"
                )
            finally:
                hold.set()
                consumer_a.request_stop()
                consumer_b.request_stop()
                await asyncio.gather(
                    *list(consumer_a._inflight),
                    *list(consumer_b._inflight),
                    return_exceptions=True,
                )
                if task_b is not None:
                    task_b.cancel()
                    await asyncio.gather(task_b, return_exceptions=True)

    asyncio.run(go())
