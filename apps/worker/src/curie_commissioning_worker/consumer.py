"""Campaign verification and optional one-shot execution, @spec PROTECTED-COMMISSIONING-4.

Without an executor the composition remains verification-only. With an executor,
terminal claims settle under the shared delivery lease. Execution consumes a
permanent campaign-lane scalar before forwarding, so even a crash before the
callback cannot cause replay. Uncertain dispatch is never acceptance evidence.
Parking uses XPENDING/XRANGE without charged XCLAIM or dead-lettering.
"""

from __future__ import annotations

import asyncio
import logging
from collections import OrderedDict
from typing import Literal, cast

from curie_protected_hooks.commissioning_acl import derive_commissioning_lane
from curie_protected_hooks.commissioning_worker import (
    CampaignDispatchable,
    CampaignEntryGate,
    CampaignParked,
    CampaignRefused,
)
from curie_protected_hooks.worker_transport import (
    PROTECTED_WORKER_MAX_READ_BLOCK_MS,
    AuthenticatedWorkerClient,
)
from curie_worker.config import WorkerConfig
from curie_worker.consumer import Consumer
from curie_worker.delivery_lease import DeliveryLeaseStore
from curie_worker.kernel.core import Kernel
from curie_worker.protected_lane import _UnqualifiedKernel

from .claim_execution import CONSUMER_NAME, ExecuteClaim, ReplicaLivenessStore, consume_claim

Verdict = CampaignDispatchable | CampaignParked | CampaignRefused
ExecutionOutcome = Literal["completed", "execution-refused", "uncertain"]
# The most recent claim verdicts kept for the operator; bounded, never authority.
_VERDICTS = 1024
# The cadence of the parked-claim re-check while the worker runs.
RECHECK_INTERVAL_S = 5.0

logger = logging.getLogger(__name__)


class CommissioningConsumer(Consumer):
    """Verify claims and optionally forward once, @spec PROTECTED-COMMISSIONING-4."""

    def __init__(
        self,
        *,
        client: AuthenticatedWorkerClient,
        gate: CampaignEntryGate,
        config: WorkerConfig,
        recheck_interval_s: float = RECHECK_INTERVAL_S,
        execute_claim: ExecuteClaim | None = None,
    ) -> None:
        """@spec PROTECTED-COMMISSIONING-4."""
        if type(client) is not AuthenticatedWorkerClient or type(gate) is not CampaignEntryGate:
            raise ValueError("commissioning consumer configuration is invalid")
        if type(recheck_interval_s) not in (int, float) or not 0 < recheck_interval_s <= 60:
            raise ValueError("commissioning consumer configuration is invalid")
        if execute_claim is not None and (
            not callable(execute_claim) or config.consumer_name != CONSUMER_NAME
        ):
            raise ValueError("commissioning execution configuration is invalid")
        lane = derive_commissioning_lane(gate.permit)
        if (
            config.stream != lane.stream
            or config.consumer_group != lane.group
            or config.key_prefix != lane.prefix
            or config.dead_letter_stream_name() != lane.dead
            or type(config.read_block_ms) is not int
            or not 0 < config.read_block_ms <= PROTECTED_WORKER_MAX_READ_BLOCK_MS
        ):
            raise ValueError("commissioning consumer configuration is invalid")
        runs = client.lane().runs
        super().__init__(
            redis=runs,
            kernel=cast(Kernel, _UnqualifiedKernel()),
            config=config,
            leases=DeliveryLeaseStore(runs, config),
        )
        if execute_claim is not None:
            self._liveness_store = ReplicaLivenessStore(runs)
        self._execute_claim = execute_claim
        self._execution_outcomes: OrderedDict[str, ExecutionOutcome] = OrderedDict()
        self._unsettled: set[str] = set()
        self._gate = gate
        self._recheck_interval_s = float(recheck_interval_s)
        self._verdicts: OrderedDict[str, Verdict] = OrderedDict()
        # Pending claims whose latest verdict is parked. Bounded by the
        # campaign's charged delivery limit: each is one admitted lane entry.
        self._parked: set[str] = set()

    @property
    def verdicts(self) -> dict[str, Verdict]:
        """The latest verdict per claimed entry, newest last."""
        return dict(self._verdicts)

    @property
    def execution_outcomes(self) -> dict[str, ExecutionOutcome]:
        """Bounded observations; only completed denotes a confirmed callback and ACK."""
        return dict(self._execution_outcomes)

    async def ensure_group(self) -> None:
        """Accepted campaign backlog survives the first worker boot."""
        await self._ensure_group(self._config.stream, self._config.consumer_group, start_id="0")

    async def run(self) -> None:
        """Read new campaign entries and re-check parked ones; no reclaim or sweep."""
        await self.ensure_group()
        await self._run_consumer_generation(
            {"read": self._read_loop, "recheck": self._recheck_loop}
        )

    async def _recover_local_pending_once(self) -> None:
        """Recover the fixed consumer's claims without spending a delivery attempt."""
        if self._execute_claim is not None:
            # A fixed identity is the execution contract. Never silently skip a
            # prior name's PEL or guess that a differently named peer is dead.
            cursor = "-"
            while True:
                rows = await self._redis.xpending_range(
                    self._config.stream, self._config.consumer_group,
                    min=cursor, max="+", count=self._config.read_count,
                )
                if any(str(row["consumer"]) != CONSUMER_NAME for row in rows):
                    raise RuntimeError("commissioning execution refused: foreign pending owner")
                if len(rows) < self._config.read_count:
                    break
                cursor = "(" + str(rows[-1]["message_id"])
        await self._recheck_pending(parked_only=False)

    async def _recheck_loop(self) -> None:
        while not self._should_stop():
            await self._sleep_or_stop(self._recheck_interval_s)
            if self._should_stop():
                return
            try:
                await self._recheck_pending(parked_only=True)
            except Exception:  # noqa: BLE001  A transport fault waits for the next pass.
                logger.warning("parked claim re-check failed; retrying", exc_info=True)

    async def _recheck_pending(self, *, parked_only: bool) -> None:
        """Hand this consumer's pending claims to the gate again, without a claim."""
        config = self._config
        if parked_only:
            # Iterate the bounded work set, not every terminal verification-only
            # row. Membership is checked afresh so ACK/XDEL by a peer prunes it.
            for entry_id in tuple(self._parked | self._unsettled):
                if self._should_stop():
                    return
                rows = await self._redis.xpending_range(
                    config.stream, config.consumer_group, min=entry_id, max=entry_id,
                    count=1, consumername=config.consumer_name,
                )
                if not rows:
                    self._parked.discard(entry_id)
                    self._unsettled.discard(entry_id)
                    continue
                await self._recheck_entry(entry_id)
            return
        cursor = "-"
        while not self._should_stop():
            rows = await self._redis.xpending_range(
                config.stream,
                config.consumer_group,
                min=cursor,
                max="+",
                count=config.read_count,
                consumername=config.consumer_name,
            )
            for row in rows:
                entry_id = str(row["message_id"])
                if self._should_stop():
                    return
                await self._recheck_entry(entry_id)
            if len(rows) < config.read_count:
                return
            cursor = "(" + str(rows[-1]["message_id"])

    async def _recheck_entry(self, entry_id: str) -> None:
        found = await self._redis.xrange(self._config.stream, min=entry_id, max=entry_id)
        if not found:
            # Empty fields fail the gate at step 7; execution mode also ACKs
            # deleted pending entries under the ordinary delivery fence.
            await self._dispatch(entry_id, {})
            return
        await self._dispatch(entry_id, dict(found[0][1]))

    def _outcome(self, entry_id: str, outcome: ExecutionOutcome) -> None:
        self._execution_outcomes[entry_id] = outcome
        self._execution_outcomes.move_to_end(entry_id)
        while len(self._execution_outcomes) > _VERDICTS:
            self._execution_outcomes.popitem(last=False)

    def _record(self, entry_id: str, verdict: Verdict) -> None:
        if type(verdict) is CampaignParked:
            self._parked.add(entry_id)
        else:
            self._parked.discard(entry_id)
        self._verdicts[entry_id] = verdict
        self._verdicts.move_to_end(entry_id)
        while len(self._verdicts) > _VERDICTS:
            self._verdicts.popitem(last=False)

    async def _handle(self, entry_id: str, fields: dict[str, str]) -> None:
        try:
            verdict = await self._gate.verify_entry(entry_id, fields)
            self._record(entry_id, verdict)
            if self._execute_claim is None or type(verdict) is CampaignParked:
                return
            self._unsettled.add(entry_id)
            async with self._delivery_lease(entry_id, fields) as lease:
                if lease is None:
                    return
                if type(verdict) is CampaignDispatchable:
                    delivery_count = await self._pending_delivery_count(entry_id)
                    if delivery_count >= self._config.max_delivery or lease.remaining_s() <= 0:
                        self._outcome(entry_id, "execution-refused")
                        await self._dead_letter(
                            entry_id, fields,
                            reason=("max-delivery-exceeded"
                                    if delivery_count >= self._config.max_delivery
                                    else "delivery-budget-exhausted"),
                            delivery_count=delivery_count,
                        )
                        # The shared graveyard can refuse a lost fence without
                        # ACKing. Keep this id until a later PEL read proves it
                        # settled, or retries it under a fresh delivery lease.
                        return
                    if not await consume_claim(self._valkey, self._config, entry_id, lease):
                        self._outcome(entry_id, "execution-refused")
                    else:
                        # Budget/lease loss cancels a cooperative executor. Join
                        # its cleanup even beyond that deadline; this is not a
                        # hard process-kill boundary for an untrusted coroutine.
                        remaining_s = lease.remaining_s()
                        lease.raise_if_lost()
                        if remaining_s <= 0:
                            raise TimeoutError("commissioning execution budget exhausted")
                        work = asyncio.ensure_future(self._execute_claim(verdict))
                        lost = asyncio.create_task(lease.lost.wait())
                        try:
                            async with asyncio.timeout(remaining_s):
                                done, _ = await asyncio.wait(
                                    {work, lost}, return_when=asyncio.FIRST_COMPLETED
                                )
                                if lost in done:
                                    lease.raise_if_lost()
                                await work
                        finally:
                            work.cancel()
                            lost.cancel()
                            await asyncio.gather(work, lost, return_exceptions=True)
                await self._ack(entry_id)
                await self._settle_delivery_best_effort(entry_id)
                if (
                    type(verdict) is CampaignDispatchable
                    and entry_id not in self._execution_outcomes
                ):
                    self._outcome(entry_id, "completed")
                self._unsettled.discard(entry_id)
        except asyncio.CancelledError:
            if self._execute_claim is not None:
                self._outcome(entry_id, "uncertain")
            raise
        except Exception:  # noqa: BLE001  A consumed claim can never be replayed.
            if self._execute_claim is not None:
                self._outcome(entry_id, "uncertain")
            logger.warning("commissioning claim execution or settlement unconfirmed")
        finally:
            self._inflight_ids.discard(entry_id)
            self._sem.release()

    async def _interrupt_on_lease_lost(self, entry_id: str, fields: dict[str, str]) -> None:
        # _handle observes lease.lost and cancels/joins its executor. Campaign
        # payloads are not ordinary QueuedTurns and never enter the kernel.
        self._outcome(entry_id, "uncertain")

    async def _notice_vanished_entry(self, entry_id: str, fields: dict[str, str]) -> None:
        self._outcome(entry_id, "uncertain")

    async def _dead_letter(
        self,
        entry_id: str,
        fields: dict[str, str] | None,
        *,
        reason: str,
        delivery_count: int,
    ) -> None:
        if self._execute_claim is not None:
            await super()._dead_letter(
                entry_id, fields, reason=reason, delivery_count=delivery_count
            )
