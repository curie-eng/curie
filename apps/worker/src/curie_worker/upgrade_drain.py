"""The pre-upgrade drain gate: finish accepted work, or refuse the roll (#2010).

ADR-0131 made one worker's *own* shutdown safe. Platform grace covers the
delivery budget plus the shutdown reserve, so a SIGTERMed replica can settle the
delivery it owns, and the fencing lease stops a replacement stealing a healthy
long turn. None of that governs the roll happening *around* it. A `helm upgrade`
applies the worker and its backing services in one pass, and an accepted,
side-effecting turn whose owner is interrupted mid-flight is reclaimed by the
replacement, which correctly refuses to re-run the action and escalates to a
human:

    A prior attempt started an action before the worker restarted; not retrying
    automatically. Flagging for a human.

That is the *safe* answer to an unsafe situation, and it is the right thing to
emit once the situation exists. Issue #2010 is that a routine upgrade CREATES
the situation: no duplicate effect, no silent loss, and the requested task still
does not complete.

This module closes the gap ahead of the roll rather than behind it, with the two
outcomes the issue asks for and nothing in between:

1. **Drain.** Set a fleet-wide quiesce flag so no replica takes new work, then
   wait for every delivery that currently holds a live ownership lease to reach
   its terminal outcome. When they all do, the upgrade proceeds and each of
   those turns completed exactly once.
2. **Refuse.** If unsafe work is still in flight when the wait expires, the gate
   fails, `helm upgrade` fails with it, and NOTHING is rolled. The turn keeps
   running on the workers that are already there.

Three properties are load-bearing.

**"Unsafe in flight" is a live LEASE, not a pending entry.** The gate reads each
lane's pending list and keeps only the entries some owner is currently holding
(``DeliveryLeaseStore.is_live``). An unleased pending entry is not work in
progress -- it is work waiting to be reclaimed, and reclaim after the roll is
exactly what the existing machinery is for. Gating on pending-ness instead would
block every upgrade behind a dead-lettered backlog nobody is working, which is a
gate that gets disabled the first week it ships.

**No keyspace scan.** The pending list is bounded (``max_concurrency`` per
consumer, capped again by ADR-0039's delivery budget) and is paged, and the
liveness reads for one page go out in a single pipeline. ``markers.py`` states
the rule this follows: the maintenance path must not ``SCAN`` a production
Valkey, and this runs against exactly the release an operator is upgrading.

**A refusal must not wedge the fleet.** The quiesce flag is always written with
a TTL, and :func:`main` clears it explicitly when the drain is refused. A
postponed upgrade leaves the cluster exactly as it found it -- still serving,
still claiming -- which is what makes "refuse" an acceptable normal-path answer
rather than an outage.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
import time
from dataclasses import dataclass

from redis.asyncio import Redis
from redis.exceptions import ResponseError

from .config import WorkerConfig

logger = logging.getLogger(__name__)

# How many pending entries are read per round trip. The whole list is paged, so
# this bounds one round trip rather than the answer; it is deliberately larger
# than any single consumer's in-flight cap so the common case is one page.
_PEL_SCAN_PAGE = 256

# The separator in a delivery's human-readable id. The delivery triple is
# ``(stream, group, entry_id)`` (ADR-0131) and the gate reports it whole: an
# operator reading a refusal needs to know WHICH lane is still busy, and a bare
# entry id is ambiguous across the runs and eval groups.
_DELIVERY_SEP = "/"


@dataclass(frozen=True)
class DrainOutcome:
    """What the wait concluded, and the evidence for it.

    ``remaining`` is empty exactly when ``drained`` is true. It is carried on the
    refusal so the operator is told which deliveries held the upgrade back --
    "the gate refused" with no names is a message that gets the gate turned off.
    """

    drained: bool
    remaining: tuple[str, ...]
    waited_s: float


class UpgradeDrainGate:
    """Quiesce the fleet and wait for accepted work to settle.

    Takes the concrete ``redis.asyncio.Redis`` for the same reason ``Markers``
    and ``DeliveryLeaseStore`` do: it needs plain string-key verbs (the quiesce
    flag) next to the stream verbs, and the ``StreamBroker`` port deliberately
    carries only the latter.
    """

    def __init__(self, redis: Redis, config: WorkerConfig) -> None:
        self._redis = redis
        self._config = config

    # -- the quiesce flag -----------------------------------------------------

    async def request_quiesce(self, *, ttl_s: float | None = None) -> None:
        """Ask every replica to stop taking new work.

        ALWAYS with an expiry. A permanent flag turns any upgrade that dies
        between this call and the post-upgrade release into a fleet that has
        stopped answering and looks perfectly healthy while doing it.
        """
        ttl = self._config.upgrade_quiesce_ttl_s if ttl_s is None else ttl_s
        await self._redis.set(
            self._config.upgrade_quiesce_key(), "1", ex=max(1, int(ttl))
        )

    async def clear_quiesce(self) -> None:
        """Let the fleet claim again. Idempotent."""
        await self._redis.delete(self._config.upgrade_quiesce_key())

    async def is_quiescing(self) -> bool:
        """Is a drain in progress? The read every consumer makes before a claim."""
        return bool(await self._redis.exists(self._config.upgrade_quiesce_key()))

    # -- what is still in flight ----------------------------------------------

    def _lanes(self) -> tuple[tuple[str, str], ...]:
        """The (stream, group) pairs whose deliveries a roll would interrupt.

        Both lanes, not just runs: an eval delivery holds a sandbox and a lease
        exactly as a turn does, and it is settled by the same fenced write. The
        killswitch consumer is deliberately absent -- it holds no delivery lease
        and must keep answering while an upgrade drains.
        """
        return (
            (self._config.stream, self._config.consumer_group),
            (self._config.eval_stream, self._config.eval_consumer_group),
        )

    async def unsettled_deliveries(self) -> tuple[str, ...]:
        """Deliveries some owner is actively holding, across every lane.

        A pending entry with no live lease is NOT returned: nobody is working
        it, so rolling does not interrupt anything, and the reclaim machinery
        picks it up on the other side. Returned sorted so a refusal message is
        stable across polls.
        """
        found: list[str] = []
        for stream, group in self._lanes():
            found.extend(await self._unsettled_in_lane(stream, group))
        return tuple(sorted(found))

    async def _unsettled_in_lane(self, stream: str, group: str) -> list[str]:
        cursor = "-"
        live: list[str] = []
        while True:
            try:
                pending = await self._redis.xpending_range(
                    stream, group, min=cursor, max="+", count=_PEL_SCAN_PAGE
                )
            except ResponseError as exc:
                # NOGROUP -- a lane that has never been used on this release --
                # is no work, not unsafe work, and a release whose eval group
                # does not exist yet must still be upgradable.
                #
                # Nothing else is caught. Every other failure means this lane
                # could not be READ, and a gate that answers "nothing in flight"
                # about a lane it cannot see is worse than no gate: it clears an
                # upgrade over deliveries it never looked at. The exception
                # leaves ``run_gate`` non-zero, so an unreadable lane refuses the
                # upgrade -- fail closed, like every other authority read in this
                # subsystem.
                if "NOGROUP" not in str(exc):
                    raise
                logger.debug("lane %s/%s has no consumer group yet", stream, group)
                return live
            if not pending:
                return live
            entry_ids = [str(row["message_id"]) for row in pending]
            # One pipeline for the page's liveness reads. Not a transaction:
            # these are plain EXISTS, and MULTI would only add a blocking window
            # on the Valkey the fleet is mid-delivery against.
            async with self._redis.pipeline(transaction=False) as pipe:
                for entry_id in entry_ids:
                    pipe.exists(self._config.delivery_lease_key(stream, group, entry_id))
                flags = await pipe.execute()
            for entry_id, flag in zip(entry_ids, flags, strict=True):
                if flag:
                    live.append(_DELIVERY_SEP.join((stream, group, entry_id)))
            if len(pending) < _PEL_SCAN_PAGE:
                return live
            cursor = f"({entry_ids[-1]}"

    # -- the gate itself ------------------------------------------------------

    async def await_drained(
        self, *, timeout_s: float | None = None, poll_interval_s: float | None = None
    ) -> DrainOutcome:
        """Quiesce, then wait for the in-flight deliveries to settle.

        Sets the flag FIRST and unconditionally: the wait is only meaningful
        while nothing new is being admitted, and a wait that admitted new work
        could never terminate under load.

        The flag is deliberately left set on BOTH outcomes. On success it is what
        keeps the replacement pods from reclaiming while the roll is in progress,
        and the post-upgrade release clears it; on refusal, clearing it is the
        caller's decision (see :func:`main`), because a caller that wants to
        retry the gate immediately should not have to re-quiesce a fleet that
        just resumed.
        """
        timeout = self._config.upgrade_drain_timeout_s if timeout_s is None else timeout_s
        interval = (
            self._config.upgrade_drain_poll_interval_s
            if poll_interval_s is None
            else poll_interval_s
        )
        await self.request_quiesce()
        started = time.monotonic()
        deadline = started + timeout
        while True:
            remaining = await self.unsettled_deliveries()
            if not remaining:
                return DrainOutcome(
                    drained=True, remaining=(), waited_s=time.monotonic() - started
                )
            now = time.monotonic()
            if now >= deadline:
                return DrainOutcome(
                    drained=False, remaining=remaining, waited_s=now - started
                )
            logger.info(
                "upgrade drain waiting on %d in-flight deliver%s: %s",
                len(remaining),
                "y" if len(remaining) == 1 else "ies",
                ", ".join(remaining),
            )
            await asyncio.sleep(min(interval, max(0.0, deadline - now)))


def _client(config: WorkerConfig) -> Redis:
    return Redis(**config.valkey_client_kwargs())


async def run_gate(config: WorkerConfig, *, mode: str) -> int:
    """The chart hook's body, factored out so tests drive it without a process.

    ``drain`` is the pre-upgrade hook: quiesce, wait, and answer with the exit
    code Helm reads as "proceed" (0) or "do not roll" (1). ``release`` is the
    post-upgrade hook: clear the flag so the new pods start claiming.
    """
    redis = _client(config)
    try:
        if mode == "release":
            await UpgradeDrainGate(redis, config).clear_quiesce()
            logger.info("upgrade quiesce cleared; the fleet is claiming again")
            return 0
        gate = UpgradeDrainGate(redis, config)
        outcome = await gate.await_drained()
        if outcome.drained:
            logger.info(
                "upgrade drain complete after %.1fs; no delivery is in flight",
                outcome.waited_s,
            )
            return 0
        # Postpone, and put the cluster back exactly as it was found. Leaving
        # the flag set here would keep a fleet that is NOT being upgraded from
        # claiming until the TTL lapsed -- turning a refused upgrade into the
        # outage the refusal exists to avoid.
        await gate.clear_quiesce()
        logger.error(
            "refusing the upgrade: %d deliver%s still in flight after %.1fs (%s). "
            "Nothing was rolled and the fleet is claiming again; retry once these "
            "settle, or raise worker.upgradeDrain.timeoutSeconds.",
            len(outcome.remaining),
            "y is" if len(outcome.remaining) == 1 else "ies are",
            outcome.waited_s,
            ", ".join(outcome.remaining),
        )
        return 1
    finally:
        await redis.aclose()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="curie-worker-upgrade-drain",
        description="Drain accepted in-flight deliveries before a chart upgrade (#2010).",
    )
    parser.add_argument(
        "--mode",
        choices=("drain", "release"),
        default="drain",
        help="drain: pre-upgrade gate. release: post-upgrade quiesce clear.",
    )
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    return asyncio.run(run_gate(WorkerConfig(), mode=args.mode))


if __name__ == "__main__":  # pragma: no cover - process entry point
    sys.exit(main())
