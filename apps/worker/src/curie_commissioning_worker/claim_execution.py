"""One-shot lane-local execution bookkeeping, @spec PROTECTED-COMMISSIONING-4.

The scalar is deliberately permanent for this disposable campaign lane. A crash
or ambiguous broker reply after consumption can lose a dispatch, but can never
refund it. It is not a successful execution record or acceptance evidence.
The candidate's existing lane ACL grants these commands; no campaign authority
record, production key or side-effect marker is written.

ExecuteClaim is a trusted, cooperative async boundary: it must respect
cancellation, bound each external call, and join all child work before returning
or propagating cancellation. Consumer budget/lease loss requests cancellation
and joins cleanup. It cannot impose a hard wall-clock stop on a coroutine that
suppresses cancellation. Unfinished cleanup must keep supervisor acceptance
blocked; a callback returning alone does not prove campaign acceptance.
"""

from __future__ import annotations

import uuid
from collections.abc import Awaitable, Callable

from curie_protected_hooks.commissioning_worker import CampaignDispatchable
from curie_worker.config import WorkerConfig
from curie_worker.consumer_liveness import ConsumerLivenessStore
from curie_worker.delivery_lease import DeliveryLease, LeaseLostError
from redis.asyncio import Redis

ExecuteClaim = Callable[[CampaignDispatchable], Awaitable[None]]
CONSUMER_NAME = "commissioning-consumer-1"

# Check live delivery authority and consume the one-shot in the same operation.
# No expiry or deletion: lease/state cleanup must never refund execution.
_CONSUME = """
local pending = redis.call('XPENDING', KEYS[1], ARGV[1], ARGV[2], ARGV[2], 1)
if #pending ~= 1 or pending[1][2] ~= ARGV[3] then return -1 end
if redis.call('GET', KEYS[2]) ~= ARGV[4] then return -1 end
if redis.call('HGET', KEYS[3], 'gen') ~= ARGV[5] then return -1 end
if redis.call('SET', KEYS[4], 'consumed', 'NX') then return 1 end
return 0
"""


async def consume_claim(
    redis: Redis, config: WorkerConfig, entry_id: str, lease: DeliveryLease
) -> bool:
    """Confirm first consumption only; any uncertain reply raises before forwarding."""
    lease.raise_if_lost()
    result = await redis.eval(
        _CONSUME,
        4,
        config.stream,
        config.delivery_lease_key(config.stream, config.consumer_group, entry_id),
        config.delivery_state_key(config.stream, config.consumer_group, entry_id),
        config.key_prefix + ":claim-execution:" + entry_id,
        config.consumer_group,
        entry_id,
        config.consumer_name,
        lease.owner,
        str(lease.generation),
    )
    if type(result) is not int:
        raise ValueError("commissioning execution consumption unconfirmed")
    if result == -1:
        raise LeaseLostError("commissioning execution authority lost")
    if result not in (0, 1):
        raise ValueError("commissioning execution consumption unconfirmed")
    return result == 1


class ReplicaLivenessStore(ConsumerLivenessStore):
    """Separate process liveness from the fixed logical commissioning PEL owner.

    Commissioning never runs peer reclaim. These markers supervise only their
    own process generation; they do not advertise a separately reclaimable PEL.
    All publication order, token comparison, TTL and deadline behavior remains
    in the shared store and stream-consumer generation lifecycle.
    """

    def __init__(self, redis: Redis) -> None:
        super().__init__(redis)
        self._replica = CONSUMER_NAME + "-replica-" + uuid.uuid4().hex

    async def publish(
        self, *, stream: str, group: str, consumer: str,
        heartbeat_ttl_ms: int, capability_ttl_ms: int,
    ) -> str:
        return await super().publish(
            stream=stream, group=group, consumer=self._replica,
            heartbeat_ttl_ms=heartbeat_ttl_ms, capability_ttl_ms=capability_ttl_ms,
        )

    async def renew(
        self, *, stream: str, group: str, consumer: str,
        heartbeat_ttl_ms: int, capability_ttl_ms: int, token: str,
    ) -> bool:
        return await super().renew(
            stream=stream, group=group, consumer=self._replica,
            heartbeat_ttl_ms=heartbeat_ttl_ms, capability_ttl_ms=capability_ttl_ms, token=token,
        )

    async def cleanup_alive(
        self, *, stream: str, group: str, consumer: str, timeout_s: float,
    ) -> None:
        await super().cleanup_alive(
            stream=stream, group=group, consumer=self._replica, timeout_s=timeout_s,
        )
