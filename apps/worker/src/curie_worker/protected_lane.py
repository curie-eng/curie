"""Protected entry proof and parking-only intake, @spec PROTECTED-HOOK-LANE-6/7.

The entry gate grants no execution by selection. The Consumer parks accepted
work, and can settle only immutable ordinary-row poison under its own lease.
Guarded model execution, protected recovery and terminal receipts remain absent.
"""

from __future__ import annotations

import json
from collections.abc import AsyncIterator, Mapping
from contextlib import asynccontextmanager
from dataclasses import replace
from datetime import UTC, datetime
from typing import cast

from curie_protected_hooks.authority_records import Manifest
from curie_protected_hooks.worker_transport import (
    PROTECTED_WORKER_MAX_READ_BLOCK_MS,
    AuthenticatedWorkerClient,
)
from curie_protected_hooks.worker_verification import (
    Dispatchable,
    Parked,
    Refused,
    verify_delivery,
)

from .config import WorkerConfig
from .consumer import Consumer
from .delivery_lease import DeliveryLeaseStore
from .kernel.core import Kernel
from .protected_binding import protected_delivery_scope

_PROTECTED_STREAM = "curie:runs"


class ProtectedEntryGate:
    """Read authoritative proof through the pinned, non-reconnecting client."""

    def __init__(
        self,
        client: AuthenticatedWorkerClient,
        *,
        trusted_manifest: Manifest,
        backlog_limit: int = 64,
    ) -> None:
        if (
            type(client) is not AuthenticatedWorkerClient
            or type(trusted_manifest) is not Manifest
            or type(backlog_limit) is not int
            or not 0 < backlog_limit <= 64
        ):
            raise ValueError("protected entry gate configuration is invalid")
        self._client = client
        self._manifest = trusted_manifest
        self._backlog_limit = backlog_limit

    async def _verify(self, fields: Mapping[str, str]) -> Dispatchable | Parked | Refused:
        return await verify_delivery(
            self._client,
            fields,
            trusted_manifest=self._manifest,
            backlog_limit=self._backlog_limit,
        )

    async def settlement_or_transfer_eligible(
        self, entry_id: str, fields: dict[str, str] | None
    ) -> bool:
        """Defer preparing proof before PEL transfer or delivery-cap settlement.

        True means only that proof is not preparing. It authorizes neither
        dispatch nor terminal settlement: a delivery fence cannot establish
        immutable terminal authority. The parking-only consumer uses its own
        stricter selector and retains every protected envelope. A
        subsequent handler must independently enter delivery_scope; no verdict
        is cached across selection and execution.
        """
        verdict = await self.verify_entry(entry_id, fields)
        return type(verdict) is not Parked

    async def verify_entry(
        self, entry_id: str, fields: dict[str, str] | None
    ) -> Dispatchable | Parked | Refused:
        """Read live proof without granting delivery context or changing PEL state."""
        if fields is None:
            rows = await self._client.lane().runs.xrange(
                _PROTECTED_STREAM, min=entry_id, max=entry_id
            )
            raw = rows[0][1] if rows else None
            fields = {}
            if isinstance(raw, dict) and all(
                type(key) is str and type(value) is str for key, value in raw.items()
            ):
                fields = cast(dict[str, str], dict(raw))
        return await self._verify(fields)

    @asynccontextmanager
    async def delivery_scope(
        self, fields: Mapping[str, str]
    ) -> AsyncIterator[Dispatchable | Parked | Refused]:
        """Reverify now and expose delivery context only for Dispatchable.

        Rejected and preparing deliveries explicitly mask any inherited outer
        context. Every exit, including cancellation, restores the prior scope.
        This does not attest a later model start or durable outbox restoration.
        """
        verdict = await self._verify(fields)
        delivery = verdict if type(verdict) is Dispatchable else None
        async with protected_delivery_scope(delivery):
            yield verdict


class ProtectedExecutionUnavailable(RuntimeError):
    """Guarded execution and durable recovery are not implemented by this slice."""


class _UnqualifiedKernel:
    """Explicit deny-all facade; no ordinary method forwarding or dynamic lookup."""

    @staticmethod
    async def _unavailable(*args: object, **kwargs: object) -> None:
        raise ProtectedExecutionUnavailable("protected guarded execution is unavailable")

    @staticmethod
    def _is_approval_resume(*args: object, **kwargs: object) -> bool:
        raise ProtectedExecutionUnavailable("protected approval resume is unavailable")

    process_event = _unavailable
    notify_capacity_queued = _unavailable
    notify_capacity_expired = _unavailable
    resolve_capacity_grant = _unavailable
    expire_capacity_wait = _unavailable
    notify_turn_not_started = _unavailable
    interrupt_thread = _unavailable
    notify_broker_entry_vanished = _unavailable
    reap_orphans = _unavailable
    sweep_pending_completions = _unavailable
    release_thread = _unavailable


# Only owned rows without a protected envelope may reach this script. All reads
# and validation precede XADD/XACK. It checks the exact count and consumer, current
# lease token and generation, and copies the source row rather than caller data.
_REFUSE_AND_SETTLE_LUA = """
if #KEYS ~= 4 or #ARGV ~= 10 then return 0 end
local function integer(value)
  local n = tonumber(value)
  if not n or n < 1 or n ~= math.floor(n) or tostring(n) ~= value then return nil end
  return n
end
local generation = integer(ARGV[5])
local deliveries = integer(ARGV[6])
local limit = integer(ARGV[7])
if not generation or not deliveries or not limit then return 0 end
if ARGV[4] == '' or ARGV[8] == '' or ARGV[9] == '' then return 0 end
if KEYS[1] == KEYS[2] then return 0 end
local ok, expected = pcall(cjson.decode, ARGV[10])
if not ok or type(expected) ~= 'table' or #expected == 0 or #expected % 2 ~= 0 then return 0 end
for _, value in ipairs(expected) do if type(value) ~= 'string' then return 0 end end
local source_length = redis.pcall('XLEN', KEYS[1])
local dead_length = redis.pcall('XLEN', KEYS[2])
if type(source_length) ~= 'number' or source_length < 1
  or type(dead_length) ~= 'number' then return 0 end
local pending = redis.pcall('XPENDING', KEYS[1], ARGV[1], ARGV[2], ARGV[2], 1)
if pending.err then return 0 end
if #pending ~= 1 or pending[1][2] ~= ARGV[3] or pending[1][4] ~= deliveries then return 0 end
if redis.pcall('GET', KEYS[3]) ~= ARGV[4] then return 0 end
if redis.pcall('HGET', KEYS[4], 'gen') ~= ARGV[5] then return 0 end
local source = redis.call('XRANGE', KEYS[1], ARGV[2], ARGV[2], 'COUNT', 1)
if #source ~= 1 then return 0 end
local fields = source[1][2]
if #fields ~= #expected then return 0 end
for i = 1, #fields do if fields[i] ~= expected[i] then return 0 end end
local copied = {}
for i = 1, #fields, 2 do
  local key = fields[i]
  if key == 'protected_envelope' then return 0 end
  if string.sub(key, 1, 3) == 'dl_' then key = 'dl_' .. key end
  table.insert(copied, key)
  table.insert(copied, fields[i + 1])
end
for _, value in ipairs({
  'dl_original_id', ARGV[2], 'dl_delivery_count', ARGV[6],
  'dl_reason', ARGV[8], 'dl_dead_lettered_at', ARGV[9]
}) do table.insert(copied, value) end
redis.call('XADD', KEYS[2], 'MAXLEN', '~', limit, '*', unpack(copied))
redis.call('XACK', KEYS[1], ARGV[1], ARGV[2])
redis.call('DEL', KEYS[3], KEYS[4])
return 1
"""


class ProtectedConsumer(Consumer):
    """Parking-only protected intake; accepted execution is deliberately unavailable.

    Both Parked and Dispatchable remain pending without a lease or new retry
    charge until strict runner/substrate/kernel composition exists. Only Refused
    work with no protected envelope, owned by this consumer, can be atomically
    settled. Refused with an envelope is not monotonic and remains parked: a
    missing control/binding/commit can become valid. Cross-owner over-cap poison
    also stays pending; protected terminal authority and cap transfer are absent.
    The provisioner must supply an exclusive private protected broker and a
    dedicated protected- group. Settlement ACKs only that group and never XDELs
    the shared physical row. This cannot establish ordinary-worker separation
    without the eventual runtime composition and deployment checks.
    No ordinary decode, capacity recovery, outbox sweep, progress or reset runs.
    """

    def __init__(
        self,
        *,
        client: AuthenticatedWorkerClient,
        trusted_manifest: Manifest,
        kernel: Kernel,
        config: WorkerConfig,
    ) -> None:
        self._gate = ProtectedEntryGate(client, trusted_manifest=trusted_manifest)
        if (
            type(kernel) is not Kernel
            or type(config.read_block_ms) is not int
            or not 0 < config.read_block_ms <= PROTECTED_WORKER_MAX_READ_BLOCK_MS
            or config.stream != _PROTECTED_STREAM
            or not config.consumer_group.startswith("protected-")
            or not (
                config.key_prefix == "protected:lane:worker"
                or config.key_prefix.startswith("protected:lane:worker:")
            )
            or not config.dead_letter_stream_name().startswith("protected:lane:")
        ):
            raise ValueError("protected consumer configuration is invalid")
        runs = client.lane().runs
        # Keep the unqualified Kernel outside every inherited call path. There
        # is no activation flag that bypasses this facade; future execution must
        # supply its independently implemented strict composition.
        super().__init__(
            redis=runs,
            kernel=cast(Kernel, _UnqualifiedKernel()),
            config=config,
            leases=DeliveryLeaseStore(runs, config),
        )
        self._delivery = replace(self._spec, entry_eligibility=self._refusal_settlement_eligible)

    async def ensure_group(self) -> None:
        # Accepted protected backlog must survive the very first worker boot.
        await self._ensure_group(self._config.stream, self._config.consumer_group, start_id="0")

    async def run(self) -> None:
        await self.ensure_group()
        await self._run_consumer_generation(
            {
                "read": self._read_loop,
                "maintenance": self._maintenance_loop,
                "prompt-reclaim": self._prompt_reclaim_loop,
            }
        )

    async def _refusal_settlement_eligible(
        self, entry_id: str, fields: dict[str, str] | None
    ) -> bool:
        # A Refused carrying an envelope can become valid after missing mutable
        # proof is restored. Only immutable ordinary-row poison is terminal here.
        if fields is None:
            fields = await self._entry_fields(entry_id)
        if fields is None or "protected_envelope" in fields:
            return False
        return type(await self._gate.verify_entry(entry_id, fields)) is Refused

    async def _handle(self, entry_id: str, fields: dict[str, str]) -> None:
        try:
            async with protected_delivery_scope(None):
                verdict = await self._gate.verify_entry(entry_id, fields)
                if type(verdict) is not Refused or "protected_envelope" in fields:
                    return
                async with self._delivery_lease(entry_id, fields) as lease:
                    if lease is None:
                        return
                    await self._dead_letter(
                        entry_id,
                        fields,
                        reason="protected-proof-refused",
                        delivery_count=await self._pending_delivery_count(entry_id),
                    )
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
        # Never reuse generic absence-of-live-lease as settlement authority.
        # A peer-owned exhausted row is deliberately not transferred here.
        if fields is None or "protected_envelope" in fields:
            return
        lease = self._held_leases.get(entry_id)
        if lease is None:
            async with self._delivery_lease(entry_id, fields or {}) as acquired:
                if acquired is not None:
                    await self._dead_letter(
                        entry_id, fields, reason=reason, delivery_count=delivery_count
                    )
            return
        actual = await self._entry_fields(entry_id)
        if actual is None or fields != actual:
            return
        if type(await self._gate.verify_entry(entry_id, actual)) is not Refused:
            return
        if reason not in {"protected-proof-refused", "max-delivery-exceeded"}:
            return
        lease.raise_if_lost()
        snapshot = json.dumps(
            [value for item in actual.items() for value in item],
            ensure_ascii=True,
            separators=(",", ":"),
        )
        async with lease.settlement_lock:
            result = await self._valkey.eval(
                _REFUSE_AND_SETTLE_LUA,
                4,
                self._config.stream,
                self._config.dead_letter_stream_name(),
                self._config.delivery_lease_key(
                    self._config.stream, self._config.consumer_group, entry_id
                ),
                self._config.delivery_state_key(
                    self._config.stream, self._config.consumer_group, entry_id
                ),
                self._config.consumer_group,
                entry_id,
                self._config.consumer_name,
                lease.owner,
                str(lease.generation),
                str(delivery_count),
                str(self._config.dead_letter_maxlen),
                reason,
                datetime.now(UTC).isoformat(),
                snapshot,
            )
        if int(result) == 1:
            self._spec.logger.critical(
                self._spec.dead_letter_log,
                entry_id,
                delivery_count,
                reason,
                self._config.dead_letter_stream_name(),
            )

    async def _maintenance_loop(self) -> None:
        while not self._should_stop():
            try:
                await self._reclaim_once()
            except Exception:
                self._spec.logger.warning(
                    "protected reclaim failed; deliveries remain pending", exc_info=True
                )
            await self._sleep_generation(self._config.reclaim_interval_s)

    async def _interrupt_on_lease_lost(self, entry_id: str, fields: dict[str, str]) -> None:
        # This consumer never opened a model turn or capacity grant.
        return

    async def _notice_vanished_entry(self, entry_id: str, fields: dict[str, str]) -> None:
        # Unverified reply data cannot authorize a terminal message.
        return

    async def _unavailable(self, *args: object, **kwargs: object) -> None:
        raise ProtectedExecutionUnavailable("protected durable recovery is unavailable")

    _startup_completion_sweep = _unavailable
    _capacity_wake_loop = _unavailable
    _capacity_notice_loop = _unavailable
    _drain_thread_reset_requests = _unavailable
    _sweep_pending_progress = _unavailable
    _drain_pending_progress_inboxes = _unavailable
    _send_wait_notice = _unavailable
    _repair_wait_notices = _unavailable
    _repair_expiry_notices = _unavailable
    _expire_wait_delivery = _unavailable
