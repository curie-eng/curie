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

1. **Drain.** Set an installation-scoped quiesce marker so no replica in that
   installation takes new work, then wait for every delivery that currently
   holds a live ownership lease to reach its terminal outcome. When they all
   do, the upgrade proceeds and each of those turns completed exactly once.
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

**A refusal must not wedge the fleet.** The quiesce marker is always written
with a TTL, and :func:`main` clears it explicitly when the drain is refused. A
postponed upgrade leaves the cluster exactly as it found it -- still serving,
still claiming -- which is what makes "refuse" an acceptable normal-path answer
rather than an outage.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import sys
import time
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Literal, TypedDict

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

# One atomic write owns every marker applicable to this hook invocation. KEYS
# contains the installation scoped key first and, only during the first mixed
# version upgrade, the legacy key second. ARGV is revision, candidate JSON, and
# TTL milliseconds. A same revision retry refreshes the TTL while retaining the
# first marker byte for byte, including its original ``since`` value. A lower
# numeric revision changes nothing.
_WRITE_OWNED_MARKER_LUA = """
local function marker_revision(raw)
  local ok, marker = pcall(cjson.decode, raw)
  if not ok or type(marker) ~= 'table' then return nil end
  local revision = marker['revision']
  if type(revision) ~= 'number' then return nil end
  if revision < 0 or revision ~= math.floor(revision) then return nil end
  return revision
end

local requested = tonumber(ARGV[1])
local marker = ARGV[2]
local retained = nil

for _, key in ipairs(KEYS) do
  local raw = redis.call('GET', key)
  if raw then
    local current = marker_revision(raw)
    if current and current > requested then return 0 end
    if current and current == requested and not retained then retained = raw end
  end
end

if retained then marker = retained end
for _, key in ipairs(KEYS) do
  redis.call('SET', key, marker, 'PX', tonumber(ARGV[3]))
end
return 1
"""

# Delete keys owned by this revision without splitting a newer mixed-version
# pair. KEYS[1] is the authoritative scoped marker; later keys are the shared
# legacy bridge. The owned scoped marker is safe to delete unilaterally when
# it matches, so a foreign marker on the shared key cannot strand it. A
# delayed release must still not clear the shared key while the scoped key
# holds a newer revision -- that would resume legacy workers during the newer
# drain. The bare ``1`` is recognized only as the unversioned standalone
# predecessor (revision zero), so a local release can still recover a marker
# written by the previous version.
_CLEAR_OWNED_MARKER_LUA = """
local function marker_revision(raw, requested)
  if raw == '1' and requested == 0 then return 0 end
  local ok, marker = pcall(cjson.decode, raw)
  if not ok or type(marker) ~= 'table' then return nil end
  local revision = marker['revision']
  if type(revision) ~= 'number' then return nil end
  if revision < 0 or revision ~= math.floor(revision) then return nil end
  return revision
end

local requested = tonumber(ARGV[1])
local auth_raw = redis.call('GET', KEYS[1])
local auth_rev = nil
if auth_raw then
  auth_rev = marker_revision(auth_raw, requested)
end
local may_clear_siblings = (not auth_raw) or (auth_rev == requested)

local deleted = 0
for i, key in ipairs(KEYS) do
  local raw = redis.call('GET', key)
  if raw then
    local current = marker_revision(raw, requested)
    if current == requested and (i == 1 or may_clear_siblings) then
      deleted = deleted + redis.call('DEL', key)
    end
  end
end
return deleted
"""


class ClaimStatus(TypedDict):
    """Safe status shape emitted to the operator-side observer."""

    state: Literal["claims_enabled", "quiescing", "unknown"]
    since: str | None
    revision: int | None


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


class QuiesceWriteRefused(Exception):
    """The fenced Lua write did not take pause authority.

    Raised when an applicable key already holds a higher revision. The gate
    must not report a drain over a fleet it never quiesced.
    """


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

    def _revision(self) -> int:
        # Ordinary worker processes do not receive a hook revision. Revision
        # zero is reserved for explicit standalone legacy mode and can never
        # supersede a positive Helm revision.
        return self._config.upgrade_revision or 0

    def _marker_keys(self) -> tuple[str, ...]:
        authoritative = self._config.upgrade_quiesce_key()
        if self._config.installation_id and self._config.upgrade_legacy_quiesce:
            # Scoped first so a same revision retry repairs a legacy key that an
            # old hook rewrote while retaining the authoritative marker's time.
            return (authoritative, self._config.upgrade_legacy_quiesce_key())
        return (authoritative,)

    async def request_quiesce(self, *, ttl_s: float | None = None) -> None:
        """Ask every replica to stop taking new work.

        ALWAYS with an expiry. A permanent flag turns any upgrade that dies
        between this call and the post-upgrade release into a fleet that has
        stopped answering and looks perfectly healthy while doing it.

        Raises :class:`QuiesceWriteRefused` when the fenced write does not take
        effect, so a caller cannot proceed as if the fleet had paused.
        """
        ttl = self._config.upgrade_quiesce_ttl_s if ttl_s is None else ttl_s
        revision = self._revision()
        marker = json.dumps(
            {
                "since": datetime.now(UTC).isoformat(),
                "revision": revision,
            },
            separators=(",", ":"),
        )
        keys = self._marker_keys()
        written = await self._redis.eval(
            _WRITE_OWNED_MARKER_LUA,
            len(keys),
            *keys,
            str(revision),
            marker,
            max(1, int(ttl * 1000)),
        )
        if written != 1:
            raise QuiesceWriteRefused(
                "refusing to quiesce: a higher revision marker already holds "
                "pause authority"
            )

    async def clear_quiesce(self) -> None:
        """Clear only markers owned by this revision. Idempotent."""

        keys = self._marker_keys()
        await self._redis.eval(
            _CLEAR_OWNED_MARKER_LUA,
            len(keys),
            *keys,
            str(self._revision()),
        )

    async def is_quiescing(self) -> bool:
        """Is a drain in progress? The read every consumer makes before a claim."""
        return bool(await self._redis.exists(self._config.upgrade_quiesce_key()))

    async def claim_status(self) -> ClaimStatus:
        """Read the safe claim state without exposing marker identity or bytes."""

        try:
            raw = await self._redis.get(self._config.upgrade_quiesce_key())
        except Exception:
            # Status is diagnostic. An unreadable authority is unknown, never
            # permission to claim and never an exception containing a key or
            # credential copied onto stdout.
            logger.warning("worker claim state is unknown: marker read failed")
            return {"state": "unknown", "since": None, "revision": None}
        if raw is None:
            return {"state": "claims_enabled", "since": None, "revision": None}
        try:
            marker = json.loads(raw)
            if not isinstance(marker, dict):
                raise ValueError("marker is not an object")
            since = marker.get("since")
            revision = marker.get("revision")
            if not isinstance(since, str):
                raise ValueError("marker since is not a string")
            parsed_since = datetime.fromisoformat(since)
            if (
                parsed_since.tzinfo is None
                or parsed_since.utcoffset() != timedelta(0)
            ):
                raise ValueError("marker since is not UTC")
            if (
                not isinstance(revision, int)
                or isinstance(revision, bool)
                or revision < 0
            ):
                raise ValueError("marker revision is not an integer")
        except (OverflowError, TypeError, ValueError):
            # Existence is pause authority. Bad metadata is intentionally not
            # echoed because its contents are untrusted diagnostic input.
            return {"state": "quiescing", "since": None, "revision": None}
        return {"state": "quiescing", "since": since, "revision": revision}

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

        Sets the flag FIRST: the wait is only meaningful while nothing new is
        being admitted, and a wait that admitted new work could never terminate
        under load. A fenced write that does not take effect raises
        :class:`QuiesceWriteRefused` rather than proceeding into the wait --
        reporting drained over a fleet that is still claiming is the failure
        the gate exists to prevent.

        The flag is deliberately left set on BOTH outcomes of a write that
        succeeded. On success it is what keeps the replacement pods from
        reclaiming while the roll is in progress, and the post-upgrade release
        clears it; on refusal, clearing it is the caller's decision (see
        :func:`main`), because a caller that wants to retry the gate immediately
        should not have to re-quiesce a fleet that just resumed.
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


async def _read_claim_status(config: WorkerConfig) -> ClaimStatus:
    redis = _client(config)
    try:
        return await UpgradeDrainGate(redis, config).claim_status()
    finally:
        await redis.aclose()


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
            logger.info(
                "upgrade quiesce release processing completed; run with --mode status "
                "to confirm the current claim state"
            )
            return 0
        gate = UpgradeDrainGate(redis, config)
        try:
            outcome = await gate.await_drained()
        except QuiesceWriteRefused:
            logger.error(
                "refusing the upgrade: quiesce marker write was fenced by a "
                "higher revision; workers were never asked to stop claiming. "
                "Nothing was rolled."
            )
            return 1
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


def _observed_arg(value: str) -> bool:
    normalized = value.strip().lower()
    if normalized == "true":
        return True
    if normalized == "false":
        return False
    raise argparse.ArgumentTypeError("expected true or false")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="curie-worker-upgrade-drain",
        description="Drain accepted in-flight deliveries before a chart upgrade (#2010).",
    )
    parser.add_argument(
        "--mode",
        choices=("drain", "release", "status"),
        default="drain",
        help=(
            "drain: pre-upgrade gate. release: post-upgrade quiesce clear. "
            "status: read worker claim state."
        ),
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="write the status result as exactly one JSON object",
    )
    parser.add_argument(
        "--installation-id-observed",
        type=_observed_arg,
        default=True,
        metavar="true|false",
        help="whether Helm observed the live installation identity",
    )
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    if args.mode != "status" and not args.installation_id_observed:
        # This render-time bit is hook-only. Refuse before WorkerConfig builds a
        # Redis client, so an unobserved upgrade cannot mutate either marker or
        # pretend a failed lookup was an installation identity.
        logger.error(
            "refusing %s: installation ID was not observed during the upgrade "
            "render; no Valkey client was created and no marker was mutated",
            args.mode,
        )
        return 1

    config = WorkerConfig()
    if args.mode == "status":
        status = asyncio.run(_read_claim_status(config))
        if args.json:
            sys.stdout.write(json.dumps(status, separators=(",", ":")) + "\n")
        elif status["state"] == "quiescing" and status["revision"] is not None:
            logger.info(
                "worker claims are quiescing since %s for upgrade revision %d",
                status["since"],
                status["revision"],
            )
        else:
            logger.info("worker claim state: %s", status["state"])
        return 0
    return asyncio.run(run_gate(config, mode=args.mode))


if __name__ == "__main__":  # pragma: no cover - process entry point
    sys.exit(main())
