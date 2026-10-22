"""Parking-only candidate consumer on the campaign lane, @spec PROTECTED-COMMISSIONING-4.

The unchanged shared stream transport reads the campaign lane: stream, group,
dead-letter stream and every worker key come from the permit's derived lane, so
the candidate worker's ACL confines it there. Each claim is verified afresh by
the campaign gate and then left pending, as the production protected intake
does: guarded execution, the launch mediator and the provider gateway are
composed by later slices. Nothing is acknowledged, leased, dead-lettered,
retried or handed to a kernel, and no maintenance loop runs (it would touch the
ordinary thread-reset keys outside the lane).
"""

from __future__ import annotations

from collections import OrderedDict
from typing import cast

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


class CommissioningConsumer(Consumer):
    """Verify every campaign lane claim and park it, @spec PROTECTED-COMMISSIONING-4."""

    def __init__(
        self,
        *,
        client: AuthenticatedWorkerClient,
        gate: CampaignEntryGate,
        config: WorkerConfig,
    ) -> None:
        """@spec PROTECTED-COMMISSIONING-4."""
        if type(client) is not AuthenticatedWorkerClient or type(gate) is not CampaignEntryGate:
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
        self._verdicts: OrderedDict[str, Verdict] = OrderedDict()

    @property
    def verdicts(self) -> dict[str, Verdict]:
        """The latest verdict per claimed entry, newest last."""
        return dict(self._verdicts)

    async def ensure_group(self) -> None:
        """Accepted campaign backlog survives the first worker boot."""
        await self._ensure_group(self._config.stream, self._config.consumer_group, start_id="0")

    async def run(self) -> None:
        """Read new campaign entries only; no reclaim, reset drain or outbox sweep."""
        await self.ensure_group()
        await self._run_consumer_generation({"read": self._read_loop})

    async def _handle(self, entry_id: str, fields: dict[str, str]) -> None:
        try:
            verdict = await self._gate.verify_entry(entry_id, fields)
            self._verdicts[entry_id] = verdict
            self._verdicts.move_to_end(entry_id)
            while len(self._verdicts) > _VERDICTS:
                self._verdicts.popitem(last=False)
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
