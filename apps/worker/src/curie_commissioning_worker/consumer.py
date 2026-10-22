"""Parking-only candidate consumer on the campaign lane, @spec PROTECTED-COMMISSIONING-4.

The unchanged shared stream transport reads the campaign lane: stream, group,
dead-letter stream and every worker key come from the permit's derived lane, so
the candidate worker's ACL confines it there. Each claim is verified afresh by
the campaign gate and then left pending, as the production protected intake
does: guarded execution, the launch mediator and the provider gateway are
composed by later slices. Nothing is acknowledged, leased, dead-lettered,
retried or handed to a kernel, and the ordinary maintenance loop does not run
(it would touch the ordinary thread-reset keys outside the lane).

A parked claim waits only for the control to commit its admission, so it is
re-checked by the campaign gate rather than retired (#4900). Boot recovery
re-verifies every claim this consumer name still has pending, and a bounded
pass re-verifies each parked claim every ``recheck_interval_s`` while the
worker runs. Both read the entry with XRANGE instead of XCLAIM: parking is not
a delivery attempt, so no restart spends the transport's delivery cap or
reaches the dead-letter path. A refused or dispatchable verdict is not
re-checked while the worker runs; the gate decides each claim afresh after a
restart, and a refusal stays the recorded verdict. A claim whose lane entry
is gone is refused at step 7. Pending rows of another consumer name are not
transferred.
"""

from __future__ import annotations

import logging
from collections import OrderedDict
from typing import Any, cast

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

Verdict = CampaignDispatchable | CampaignParked | CampaignRefused
# The most recent claim verdicts kept for the operator; bounded, never authority.
_VERDICTS = 1024
# The cadence of the parked-claim re-check while the worker runs.
RECHECK_INTERVAL_S = 5.0

logger = logging.getLogger(__name__)


class CommissioningConsumer(Consumer):
    """Verify every campaign lane claim and park it, @spec PROTECTED-COMMISSIONING-4."""

    def __init__(
        self,
        *,
        client: AuthenticatedWorkerClient,
        gate: CampaignEntryGate,
        config: WorkerConfig,
        recheck_interval_s: float = RECHECK_INTERVAL_S,
    ) -> None:
        """@spec PROTECTED-COMMISSIONING-4."""
        if type(client) is not AuthenticatedWorkerClient or type(gate) is not CampaignEntryGate:
            raise ValueError("commissioning consumer configuration is invalid")
        if type(recheck_interval_s) not in (int, float) or not 0 < recheck_interval_s <= 60:
            raise ValueError("commissioning consumer configuration is invalid")
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
        """Re-verify every claim this consumer name left pending; never dead-letter."""
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
        cursor = "-"
        while not self._should_stop():
            rows: list[Any] = await self._redis.xpending_range(
                config.stream,
                config.consumer_group,
                min=cursor,
                max="+",
                count=config.read_count,
                consumername=config.consumer_name,
            )
            for row in rows:
                entry_id = str(row["message_id"])
                if parked_only and entry_id not in self._parked:
                    continue
                if self._should_stop():
                    return
                found = await self._redis.xrange(config.stream, min=entry_id, max=entry_id)
                if not found:
                    self._record(entry_id, CampaignRefused(7))
                    continue
                _, fields = found[0]
                await self._dispatch(entry_id, dict(fields))
            if len(rows) < config.read_count:
                return
            cursor = "(" + str(rows[-1]["message_id"])

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
            self._record(entry_id, await self._gate.verify_entry(entry_id, fields))
        finally:
            self._inflight_ids.discard(entry_id)
            self._sem.release()

    async def _dead_letter(
        self,
        entry_id: str,
        fields: dict[str, str] | None,
        *,
        reason: str,
        delivery_count: int,
    ) -> None:
        # No campaign verdict is terminal settlement authority in this slice.
        return None
