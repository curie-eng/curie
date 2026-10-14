"""Scoped protected dispatch and closed copy intake, @spec PROTECTED-HOOK-LANE-6/7.

No-start terminal copies remain pending; original caps retain diagnostic proof.
There is no ordinary decoder, dynamic kernel forwarding or execution flag.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager, nullcontext
from dataclasses import replace
from datetime import UTC, datetime
from typing import Any, NoReturn, cast

from aci_protocol import QueuedTurn, TurnSource
from channel_protocol.reply import ReplyAck
from curie_protected_hooks.admission_records import Envelope
from curie_protected_hooks.authority_records import Manifest
from curie_protected_hooks.execution_namespace import derive_lane_namespace
from curie_protected_hooks.worker_transport import AuthenticatedWorkerClient
from curie_protected_hooks.worker_verification import (
    Dispatchable,
    RetainedProof,
    restore_committed_event,
)

from .capacity_wait import CapacityWaitExpired, CapacityWaitRequested, wait_delivery_scope
from .config import WorkerConfig
from .consumer import Consumer
from .delivery_lease import DeliveryLease, DeliveryLeaseStore
from .kernel.core import Kernel
from .protected_binding import ProtectedMediatedBindingResolver, current_protected_delivery
from .protected_completion import ProtectedReplySink, sweep_protected_completions
from .protected_execution import (
    _CURRENT,
    _locally_valid,
    protected_execution_scope,
    require_current_execution,
)
from .protected_lane import ProtectedEntryGate
from .protected_markers import OwedCompletion, ProtectedMarkers
from .protected_nomination import HttpNominationSubmitter, NominationCapturingRunner
from .protected_recovery import (
    ProtectedCapacityWaitStore,
    ProtectedRecoveryAdapter,
    RetainedCopyCleanup,
    current_protected_recovery,
    recovery_delivery_scope,
)
from .protected_runner import ProtectedRunnerClient
from .protected_substrate import ProtectedSandboxSubstrate, protected_claim_scope
from .reply_sink import ObservedReplySink
from .upgrade_drain import UpgradeDrainGate

logger = logging.getLogger(__name__)


class ProtectedDispatchUnavailable(RuntimeError):
    """Fixed refusal, without original turn, ownership token or credentials."""


def _unavailable() -> NoReturn:
    raise ProtectedDispatchUnavailable("protected primary dispatch unavailable") from None


class GuardedProtectedKernel:
    """Only the actual scoped protected process and protected cold sweep are exposed."""

    def __init__(
        self,
        kernel: Kernel,
        *,
        broker: AuthenticatedWorkerClient,
        manifest: Manifest,
        markers: ProtectedMarkers,
        sink: ProtectedReplySink,
        substrate: ProtectedSandboxSubstrate,
    ) -> None:
        if (
            type(kernel) is not Kernel
            or type(broker) is not AuthenticatedWorkerClient
            or type(manifest) is not Manifest
            or type(markers) is not ProtectedMarkers
            or type(sink) is not ProtectedReplySink
            or type(substrate) is not ProtectedSandboxSubstrate
            or kernel._markers is not markers
            or type(kernel._sink) is not ObservedReplySink
            or kernel._sink._sink is not sink
            or cast(object, kernel._substrate) is not substrate
            or markers._broker is not broker
            or markers._manifest.canonical_bytes != manifest.canonical_bytes
            or sink._markers is not markers
        ):
            _unavailable()
        runner: object = kernel._runner
        binding: object = kernel._binding
        if type(runner) is NominationCapturingRunner:
            if type(runner._submitter) is not HttpNominationSubmitter:
                _unavailable()
            runner = runner._runner
        if (
            type(runner) is not ProtectedRunnerClient
            or runner._substrate is not substrate
            or runner._client is not substrate._client
            or substrate._client._broker is not broker
            or substrate._client._manifest.canonical_bytes != manifest.canonical_bytes
            or type(binding) is not ProtectedMediatedBindingResolver
            or binding._manifest.canonical_bytes != manifest.canonical_bytes
            or binding._execution.canonical_bytes != substrate._client._execution.canonical_bytes
            or kernel._config.max_attempts != 1
            or any(
                value is not None
                for value in (
                    kernel._approvals,
                    kernel._publication_creator,
                    kernel._approval_reader,
                    kernel._work_items,
                    kernel._workspace,
                    kernel._attachments,
                    kernel._attachment_ledger,
                    kernel._killswitch,
                    kernel._actions,
                    kernel._card_store,
                    kernel._hook_runs,
                    kernel._sweep,
                    kernel._progress,
                    kernel._bundles,
                    kernel._sibling_limit,
                )
            )
        ):
            _unavailable()
        self._kernel, self._broker, self._manifest = kernel, broker, manifest
        self._markers, self._sink, self._substrate = markers, sink, substrate

    async def process_event(
        self, qevent: QueuedTurn, *, lease: DeliveryLease | None = None
    ) -> None:
        delivery = current_protected_delivery()
        scope = _CURRENT.get()
        if (
            type(delivery) is not Dispatchable
            or scope is None
            or lease is None
            or type(lease) is not DeliveryLease
            or qevent is not delivery.turn
            or scope.delivery is not delivery
            or scope.lease is not lease
            or not _locally_valid(scope, self._broker, self._manifest)
            or qevent.source not in {TurnSource.WEBHOOK}
            or qevent.tool_access != "read-only"
            or qevent.reply_handle is None
            or qevent.attachments
            or qevent.hook_run is not None
            or self._kernel._is_approval_resume(qevent.event_id)
        ):
            _unavailable()
        await require_current_execution(self._broker, self._manifest)
        copy = current_protected_recovery()
        if copy is not None and copy.metadata.reply_ref is not None:
            # The atomic execution gate above validated this privately issued wait
            # snapshot. Adopt only its message ref; keep the original turn and route.
            self._kernel._adopt_ref(qevent, ReplyAck(ref=copy.metadata.reply_ref))
        await self._kernel.process_event(qevent, lease=lease)
        if (
            lease.lost.is_set()
            or lease.entry_vanished.is_set()
            or await self._markers.restore_owed(qevent.event_id) is None
        ):
            _unavailable()

    async def sweep_pending_completions(self) -> None:
        await sweep_protected_completions(self._markers, self._sink)

    async def stop_delivery(self, delivery: Dispatchable) -> None:
        # Owned stop remains possible without current positive execution proof.
        await self._substrate.retire_delivery(delivery)

    async def _unsupported(self, *args: object, **kwargs: object) -> None:
        _unavailable()

    release_thread = _unsupported
    reap_orphans = _unsupported
    resolve_capacity_grant = _unsupported
    notify_capacity_expired = _unsupported
    notify_capacity_queued = _unsupported
    expire_capacity_wait = _unsupported
    notify_turn_not_started = _unsupported
    notify_broker_entry_vanished = _unsupported
    interrupt_thread = _unsupported


# Witness, context, resolution, lease, delivery, stream, repair set, original snapshots.
# Every type and snapshot read, live epoch and actual PEL fence precedes XACK.
_ACK_TERMINAL = """
local types={'string','string','string','string','hash','stream','set'}
for n=1,7 do
 local t=redis.call('TYPE',KEYS[n]).ok
 if t~='none' and t~=types[n] then return 0 end
end
if string.match(redis.call('INFO','server'),'run_id:([0-9a-f]+)')~=ARGV[1] then return 0 end
if redis.call('GET',KEYS[1])~=ARGV[2] then return 0 end
if ARGV[4]=='1' then
 if redis.call('GET',KEYS[2])~=ARGV[3] then return 0 end
 if redis.call('SISMEMBER',KEYS[7],ARGV[7])~=1 then return 0 end
elseif redis.call('GET',KEYS[2]) then return 0 end
if ARGV[6]=='1' then
 if redis.call('GET',KEYS[3])~=ARGV[5] then return 0 end
elseif redis.call('GET',KEYS[3]) then return 0 end
if ARGV[4]~='1' and ARGV[6]~='1' then return 0 end
for n=8,#KEYS do
 local t=redis.call('TYPE',KEYS[n]).ok
 if t~='none' and t~='string' then return 0 end
 local actual=redis.call('GET',KEYS[n])
 local raw=ARGV[13+(n-8)*2]
 local present=ARGV[14+(n-8)*2]
 if present=='0' then if actual then return 0 end
 elseif actual~=raw then return 0 end
end
if redis.call('GET',KEYS[4])~=ARGV[8] or redis.call('PTTL',KEYS[4])<=0 then return 0 end
if redis.call('HGET',KEYS[5],'gen')~=ARGV[9] then return 0 end
local pending=redis.call('XPENDING',KEYS[6],ARGV[11],ARGV[10],ARGV[10],1)
if #pending~=1 or pending[1][2]~=ARGV[12] then return 0 end
return redis.call('XACK',KEYS[6],ARGV[11],ARGV[10])
"""


# Fixed copied-terminal transport cleanup. Keep the original witness immutable;
# the extra final key is the current wait hash, not an original string proof.
_ACK_CLEANUP_COPY = """
local proof_count=#KEYS-8
local row=cjson.decode(ARGV[13+proof_count*2])
local wait=cjson.decode(ARGV[14+proof_count*2])
if redis.call('TYPE',KEYS[6]).ok~='stream'
 or redis.call('TYPE',KEYS[#KEYS]).ok~='hash' then return 0 end
local rows=redis.call('XRANGE',KEYS[6],ARGV[10],ARGV[10],'COUNT',1)
if #rows~=1 or #rows[1][2]~=#row*2 then return 0 end
local expected={}
for _,pair in ipairs(row) do expected[pair[1]]=pair[2] end
for index=1,#rows[1][2],2 do
 if expected[rows[1][2][index]]~=rows[1][2][index+1] then return 0 end
 expected[rows[1][2][index]]=nil
end
if next(expected)~=nil then return 0 end
if #wait~=11 then return 0 end
for _,pair in ipairs(wait) do
 local actual=redis.call('HGET',KEYS[#KEYS],pair[1])
 if pair[2]==cjson.null then if actual then return 0 end
 elseif actual~=pair[2] then return 0 end
end
""" + _ACK_TERMINAL.replace("for n=8,#KEYS do", "for n=8,#KEYS-1 do")


# Diagnostic retention is transport exhaustion, never terminal retirement authority.
_CAP_ORIGINAL = """
local st=redis.call('TYPE',KEYS[1]).ok
local dt=redis.call('TYPE',KEYS[2]).ok
local lt=redis.call('TYPE',KEYS[3]).ok
if st~='stream' or (dt~='none' and dt~='stream')
 or (lt~='none' and lt~='string') then return 0 end
if string.match(redis.call('INFO','server'),'run_id:([0-9a-f]+)')~=ARGV[1] then return 0 end
if redis.call('GET',KEYS[3]) then return 0 end
for n=4,#KEYS do
 local t=redis.call('TYPE',KEYS[n]).ok
 if t~='none' and t~='string' then return 0 end
 local raw=redis.call('GET',KEYS[n])
 if ARGV[12+(n-4)*2]=='0' then if raw then return 0 end
 elseif raw~=ARGV[11+(n-4)*2] then return 0 end
end
local row=cjson.decode(ARGV[9])
local rows=redis.call('XRANGE',KEYS[1],ARGV[2],ARGV[2],'COUNT',1)
if #row~=2 or #rows~=1 or #rows[1][2]~=4 then return 0 end
local expected={}
for _,pair in ipairs(row) do expected[pair[1]]=pair[2] end
for n=1,4,2 do
 if expected[rows[1][2][n]]~=rows[1][2][n+1] then return 0 end
 expected[rows[1][2][n]]=nil
end
if next(expected)~=nil then return 0 end
local pending=redis.call('XPENDING',KEYS[1],ARGV[3],ARGV[2],ARGV[2],1)
if #pending~=1 or pending[1][2]~=ARGV[4]
 or pending[1][4]~=tonumber(ARGV[5]) or pending[1][4]<tonumber(ARGV[7])
 or pending[1][3]<tonumber(ARGV[6]) then return 0 end
local diagnostic=cjson.decode(ARGV[10])
local flat={}
for _,pair in ipairs(diagnostic) do
 flat[#flat+1]=pair[1]; flat[#flat+1]=pair[2]
end
redis.call('XADD',KEYS[2],'MAXLEN','~',ARGV[8],'*',unpack(flat))
return redis.call('XACK',KEYS[1],ARGV[3],ARGV[2])
"""


class ExecutingProtectedConsumer(Consumer):
    """Separate scoped intake; parking-only ProtectedConsumer stays unchanged.

    Original rows and closed copies use shared live-owner/reclaim fences. No-start
    terminal copies retain pending proof. Original caps retain diagnostics. No row
    reaches ordinary decoding, and copy metadata never grants model authority.
    """

    _waits: ProtectedCapacityWaitStore

    def __init__(
        self,
        *,
        client: AuthenticatedWorkerClient,
        trusted_manifest: Manifest,
        kernel: GuardedProtectedKernel,
        config: WorkerConfig,
    ) -> None:
        ns = derive_lane_namespace(trusted_manifest)
        if (
            type(client) is not AuthenticatedWorkerClient
            or type(kernel) is not GuardedProtectedKernel
            or type(config) is not WorkerConfig
            or kernel._broker is not client
            or kernel._manifest.canonical_bytes != trusted_manifest.canonical_bytes
            or (config.key_prefix, config.stream, config.consumer_group)
            != (ns.prefix, ns.stream, ns.group)
            or config.dead_letter_stream_name() != ns.dead
            or config.max_attempts != 1
            or not 1 <= config.read_count <= 64
            or not 1 <= config.max_concurrency <= 64
            or not 2 <= config.max_delivery <= 64
            or config.upgrade_legacy_quiesce
            or not 0 < config.read_block_ms <= 1000
        ):
            _unavailable()
        self._client, self._manifest, self._facade = client, trusted_manifest, kernel
        self._gate = ProtectedEntryGate(client, trusted_manifest=trusted_manifest)
        self._recovery = ProtectedRecoveryAdapter(
            client,
            trusted_manifest,
            config,
            markers=kernel._markers,
        )
        self._active: dict[str, Dispatchable] = {}
        runs = client.lane().runs
        super().__init__(
            redis=runs,
            kernel=cast(Kernel, kernel),
            config=config,
            leases=DeliveryLeaseStore(runs, config),
            drain=UpgradeDrainGate(runs, config),
        )
        self._waits = ProtectedCapacityWaitStore(client, trusted_manifest, config)
        self._delivery = replace(
            self._spec,
            entry_eligibility=self._recovery_eligible,
            cap_scan_page=64,
        )

    async def ensure_group(self) -> None:
        await self._ensure_group(self._config.stream, self._config.consumer_group, start_id="0")

    async def _recovery_eligible(self, entry_id: str, fields: dict[str, str] | None) -> bool:
        if fields is None:
            rows = await self._client.lane().runs.xrange(
                self._config.stream,
                min=entry_id,
                max=entry_id,
                count=1,
            )
            if not rows:
                return False
            raw = rows[0][1]
            if not isinstance(raw, dict) or any(
                type(k) is not str or type(v) is not str for k, v in raw.items()
            ):
                return False
            fields = cast(dict[str, str], dict(raw))
        if set(fields) == {"payload", "protected_envelope"}:
            delivery = await self._gate.verify_entry(entry_id, fields)
            return (
                type(delivery) is Dispatchable
                and entry_id == delivery.intent.as_dict()["reserved_stream_id"]
            )
        return await self._recovery.cleanup_eligible(
            entry_id, fields
        ) or await self._recovery.eligible(entry_id, fields)

    @asynccontextmanager
    async def _verified_scope(
        self,
        entry_id: str,
        fields: dict[str, str],
        lease: DeliveryLease,
    ) -> AsyncIterator[Dispatchable | None]:
        if set(fields) == {"payload", "protected_envelope"}:
            async with self._gate.delivery_scope(fields) as delivery:
                yield delivery if type(delivery) is Dispatchable else None
        else:
            copy = await self._recovery.verify(entry_id, fields, lease=lease)
            if copy is None:
                yield None
            else:
                async with recovery_delivery_scope(copy):
                    yield copy.delivery

    async def run(self) -> None:
        await self.ensure_group()
        await self._run_consumer_generation(
            {
                "read": self._read_loop,
                "completion": self._protected_completion_loop,
                "reclaim": self._protected_reclaim_loop,
                "capacity-wait": self._protected_wait_loop,
            }
        )

    async def _is_quiescing(self) -> bool:
        if self._drain is None:
            return True
        try:
            return await self._drain.is_quiescing()
        except Exception:  # noqa: BLE001  No fail-open read on this protected connection.
            return True

    async def _protected_completion_loop(self) -> None:
        while not self._should_stop():
            try:
                await self._facade.sweep_pending_completions()
            except Exception:  # noqa: BLE001  Owed records remain retained.
                logger.warning("protected completion pass remains pending")
            await self._sleep_generation(self._config.reclaim_interval_s)

    async def _protected_wait_loop(self) -> None:
        while not self._should_stop():
            # A poisoned/closed pinned session terminates this supervised generation;
            # it cannot become an unbounded reconnect or retry campaign.
            await self._client.observe()
            try:
                async with asyncio.timeout(5):
                    if not await self._is_quiescing():
                        await self._waits.wake_due(limit=64, wall_budget_s=2)
                        await self._waits.reconcile_lost_wakes(limit=64, wall_budget_s=2)
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001  Preserve durable wait proof on bounded refusal.
                logger.warning("protected capacity pass remains pending")
            await self._sleep_generation(min(5.0, self._config.reclaim_interval_s))

    async def _protected_reclaim_loop(self) -> None:
        while not self._should_stop():
            try:
                async with asyncio.timeout(5):
                    await self._reclaim_once()
            except Exception:  # noqa: BLE001  Preserve pending rows after bounded selection failure.
                logger.warning("protected recovery pass remains pending")
            await self._sleep_generation(self._config.reclaim_interval_s)

    async def _park_owned_wait(
        self,
        delivery: Dispatchable,
        lease: DeliveryLease,
        entry_id: str,
        fields: dict[str, str],
    ) -> None:
        scope = _CURRENT.get()
        if scope is None or not _locally_valid(scope, self._client, self._manifest):
            return
        substrate = self._facade._substrate
        proof = await substrate.retire_delivery(delivery)
        if (
            not _locally_valid(scope, self._client, self._manifest)
            or proof is None
            or not substrate._client.is_owned_retirement(proof, delivery=delivery)
        ):
            return
        await self._waits.park(entry_id, fields, delivery.turn.event_id, lease)
        lease.acknowledged.set()
        await self._settle_delivery(entry_id)

    async def _handle(self, entry_id: str, fields: dict[str, str]) -> None:
        try:
            if not await self._recovery_eligible(entry_id, fields):
                return
            if await self._pending_delivery_count(entry_id) > self._config.max_delivery:
                return
            async with self._delivery_lease(entry_id, fields) as lease:
                if lease is None:
                    return
                cleanup = await self._recovery.verify_cleanup(entry_id, fields, lease)
                if cleanup is not None:
                    await self._ack_cleanup_copy(cleanup, lease)
                    return
                async with self._verified_scope(entry_id, fields, lease) as delivery:
                    if delivery is None:
                        return
                    copy = current_protected_recovery()
                    if copy is not None and (copy.expired or copy.metadata.terminal_only):
                        return  # No absence/retirement evidence means no terminal settlement.
                    if await self._facade._markers.is_terminal(delivery.turn.event_id):
                        if copy is None:
                            await self._ack_retained_terminal(delivery, lease)
                        return
                    self._active[entry_id] = delivery
                    try:
                        async with protected_execution_scope(
                            self._client,
                            trusted_manifest=self._manifest,
                            config=self._config,
                            lease=lease,
                        ):
                            budget = lease.budget
                            deadline = (
                                budget.anchor_monotonic
                                + (budget.deadline_ms - budget.anchor_server_ms) / 1000
                            )
                            async with protected_claim_scope(deadline):
                                with (
                                    wait_delivery_scope(
                                        self._waits,
                                        delivery.turn.event_id,
                                        copy.metadata.generation,
                                        lease,
                                    )
                                    if copy is not None
                                    else nullcontext()
                                ):
                                    try:
                                        await self._facade.process_event(delivery.turn, lease=lease)
                                    except CapacityWaitExpired:
                                        return  # Never manufacture no-start owned evidence.
                                    except CapacityWaitRequested:
                                        await self._park_owned_wait(
                                            delivery,
                                            lease,
                                            entry_id,
                                            fields,
                                        )
                                        return
                            await self._ack_terminal(delivery, lease)
                    finally:
                        # Incomplete execution/retirement can never release quota or ACK.
                        try:
                            await self._facade.stop_delivery(delivery)
                        finally:
                            self._active.pop(entry_id, None)
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001  Never log untrusted payload or transport exceptions.
            logger.warning("protected primary delivery remains pending")
        finally:
            self._inflight_ids.discard(entry_id)
            self._sem.release()

    def _cleanup_operation(
        self,
        issued: RetainedCopyCleanup,
        lease: DeliveryLease,
    ) -> tuple[list[str], list[Any]]:
        if (
            not self._recovery.cleanup_is_issued(issued, lease)
            or self._held_leases.get(lease.entry_id) is not lease
        ):
            _unavailable()
        keys, args = self._ack_operation(issued.owed, lease)
        event_id = issued.owed.witness.as_dict()["event_id"]
        keys.append(self._recovery._wait_key(event_id))
        args.extend(
            [
                json.dumps(issued.entry_fields, separators=(",", ":")),
                json.dumps(issued.wait_snapshot, separators=(",", ":")),
            ]
        )
        return keys, args

    async def _ack_cleanup_copy(self, issued: RetainedCopyCleanup, lease: DeliveryLease) -> None:
        keys, args = self._cleanup_operation(issued, lease)
        timeout = min(2.0, lease.local_deadline_monotonic - time.monotonic())
        if timeout <= 0:
            _unavailable()
        async with asyncio.timeout(timeout), lease.settlement_lock:
            if (
                not self._recovery.cleanup_is_issued(issued, lease)
                or self._held_leases.get(lease.entry_id) is not lease
            ):
                _unavailable()
            removed = await self._client.lane().runs.eval(
                _ACK_CLEANUP_COPY, len(keys), *keys, *args
            )
            if removed != 1:
                lease.lost.set()
                _unavailable()
            lease.acknowledged.set()
        await self._settle_delivery(lease.entry_id)

    def _terminal_lease_valid(self, delivery: Dispatchable, lease: DeliveryLease) -> bool:
        # Terminal cleanup cannot borrow positive model authority or extend its budget.
        return (
            current_protected_delivery() is delivery
            and current_protected_recovery() is None
            and self._held_leases.get(lease.entry_id) is lease
            and (lease.stream, lease.group) == (self._config.stream, self._config.consumer_group)
            and lease.entry_id == delivery.intent.as_dict()["reserved_stream_id"]
            and bool(lease.owner)
            and lease.generation > 0
            and not lease.lost.is_set()
            and not lease.acknowledged.is_set()
            and not lease.entry_vanished.is_set()
            and lease.local_deadline_monotonic > time.monotonic()
        )

    async def _ack_retained_terminal(self, delivery: Dispatchable, lease: DeliveryLease) -> None:
        if not self._terminal_lease_valid(delivery, lease):
            _unavailable()
        owed = await self._facade._markers.restore_owed(delivery.turn.event_id)
        if owed is None:
            _unavailable()
        assert owed is not None
        witness = owed.witness.as_dict()
        if (
            witness["delivery_stream"],
            witness["delivery_group"],
            witness["delivery_entry_id"],
        ) != (lease.stream, lease.group, lease.entry_id):
            _unavailable()
        keys, args = self._ack_operation(owed, lease)
        timeout = min(2.0, lease.local_deadline_monotonic - time.monotonic())
        if timeout <= 0:
            _unavailable()
        async with asyncio.timeout(timeout), lease.settlement_lock:
            if not self._terminal_lease_valid(delivery, lease):
                _unavailable()
            removed = await self._client.lane().runs.eval(_ACK_TERMINAL, len(keys), *keys, *args)
            if removed != 1:
                lease.lost.set()
                _unavailable()
            lease.acknowledged.set()
        await self._settle_delivery(lease.entry_id)

    async def _ack_terminal(self, delivery: Dispatchable, lease: DeliveryLease) -> None:
        scope = _CURRENT.get()
        if scope is None or not _locally_valid(scope, self._client, self._manifest):
            _unavailable()
        owed = await self._facade._markers.restore_owed(delivery.turn.event_id)
        if owed is None:
            _unavailable()
        assert owed is not None
        w = owed.witness.as_dict()
        if (
            w["delivery_stream"],
            w["delivery_group"],
            w["delivery_entry_id"],
            w["delivery_owner"],
            w["delivery_generation"],
        ) != (lease.stream, lease.group, lease.entry_id, lease.owner, str(lease.generation)):
            _unavailable()
        keys, args = self._ack_operation(owed, lease)
        async with lease.settlement_lock:
            if not _locally_valid(scope, self._client, self._manifest):
                _unavailable()
            async with asyncio.timeout(min(2, lease.remaining_s())):
                result = await self._client.lane().runs.eval(_ACK_TERMINAL, len(keys), *keys, *args)
            if result != 1:
                lease.lost.set()
                _unavailable()
            lease.acknowledged.set()
        await self._settle_delivery(lease.entry_id)

    def _ack_operation(
        self, owed: OwedCompletion, lease: DeliveryLease
    ) -> tuple[list[str], list[Any]]:
        markers = self._facade._markers
        w = owed.witness.as_dict()
        gen = w["completion_generation"]
        event_id = w["event_id"]
        keys = [
            markers.settlement_key(event_id),
            markers.context_key(event_id, gen),
            markers.resolution_key(event_id, gen),
            self._config.delivery_lease_key(lease.stream, lease.group, lease.entry_id),
            self._config.delivery_state_key(lease.stream, lease.group, lease.entry_id),
            lease.stream,
            markers.repair_pending_key(),
        ]
        args: list[Any] = [
            w["broker_run_id"],
            owed.witness.canonical_bytes,
            owed.context.canonical_bytes if owed.context else b"",
            "1" if owed.context else "0",
            owed.resolution.canonical_bytes if owed.resolution else b"",
            "1" if owed.resolution else "0",
            event_id,
            lease.owner,
            str(lease.generation),
            lease.entry_id,
            lease.group,
            self._config.consumer_name,
        ]
        for key, raw in owed.proof.proof_snapshots:
            keys.append(key)
            args.extend([raw if raw is not None else b"", "1" if raw is not None else "0"])
        return keys, args

    async def _interrupt_on_lease_lost(self, entry_id: str, fields: dict[str, str]) -> None:
        delivery = self._active.get(entry_id)
        if delivery is not None:
            await self._facade.stop_delivery(delivery)

    async def _notice_vanished_entry(self, entry_id: str, fields: dict[str, str]) -> None:
        await self._interrupt_on_lease_lost(entry_id, fields)

    def _cap_operation(
        self,
        entry_id: str,
        fields: dict[str, str],
        proof: RetainedProof,
        pending: dict[str, Any],
    ) -> tuple[list[str], list[Any]]:
        keys = [
            self._config.stream,
            self._config.dead_letter_stream_name(),
            self._config.delivery_lease_key(
                self._config.stream, self._config.consumer_group, entry_id
            ),
        ]
        diagnostic = {
            **fields,
            "dl_original_id": entry_id,
            "dl_reason": "max-delivery-exceeded",
            "dl_delivery_count": str(pending["times_delivered"]),
            "dl_dead_lettered_at": datetime.now(UTC).isoformat(),
        }
        args: list[Any] = [
            self._manifest.as_dict()["broker_identity"]["run_id"],
            entry_id,
            self._config.consumer_group,
            str(pending["consumer"]),
            int(pending["times_delivered"]),
            self._spec.reclaim_min_idle_ms,
            self._spec.max_delivery,
            self._config.dead_letter_maxlen,
            json.dumps(sorted(fields.items()), separators=(",", ":")),
            json.dumps(sorted(diagnostic.items()), separators=(",", ":")),
        ]
        for key, raw in proof.proof_snapshots:
            keys.append(key)
            args.extend([raw if raw is not None else b"", "1" if raw is not None else "0"])
        return keys, args

    async def _dead_letter(
        self,
        entry_id: str,
        fields: dict[str, str] | None,
        *,
        reason: str,
        delivery_count: int,
    ) -> None:
        # A copied/no-start row cannot borrow original diagnostic authority.
        if (
            reason != "max-delivery-exceeded"
            or delivery_count < self._spec.max_delivery
            or fields is None
            or set(fields) != {"payload", "protected_envelope"}
            or entry_id in self._held_leases
        ):
            return
        try:
            event_id = Envelope(fields["protected_envelope"].encode()).as_dict()["event_id"]
            proof = await restore_committed_event(
                self._client, event_id, trusted_manifest=self._manifest
            )
            if (
                proof is None
                or proof.intent.as_dict()["reserved_stream_id"] != entry_id
                or proof.envelope.canonical_bytes != fields["protected_envelope"].encode()
                or proof.envelope.as_dict()["payload_sha256"]
                != hashlib.sha256(fields["payload"].encode()).hexdigest()
            ):
                return
            pending = await self._pending_row(entry_id)
            if (
                pending is None
                or int(pending["times_delivered"]) != delivery_count
                or int(pending["time_since_delivered"]) < self._spec.reclaim_min_idle_ms
            ):
                return
            keys, args = self._cap_operation(entry_id, fields, proof, pending)
            async with asyncio.timeout(2):
                await self._client.lane().runs.eval(_CAP_ORIGINAL, len(keys), *keys, *args)
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001  Missing proof or diagnostic failure retains the PEL.
            return
