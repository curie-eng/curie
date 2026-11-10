"""Permanent protected settlement and owed completion, @spec PROTECTED-HOOK-LANE-6.

Only the injected subclass owns these writes. Cold restoration cannot dispatch.
"""

from __future__ import annotations

import hashlib
import json
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, cast

from curie_protected_hooks.admission_records import DeliveryIdentity, delivery_digest
from curie_protected_hooks.authority_records import Manifest
from curie_protected_hooks.execution_namespace import derive_lane_namespace
from curie_protected_hooks.settlement_records import (
    CompletionContext,
    CompletionResolution,
    TerminalWitness,
)
from curie_protected_hooks.worker_transport import AuthenticatedWorkerClient
from curie_protected_hooks.worker_verification import RetainedProof, restore_committed_event
from redis.asyncio import Redis

from . import protected_execution
from .config import WorkerConfig
from .markers import CompletionRecord, DoneMarkerValue, Markers
from .protected_launch_client import ProtectedLaunchClient
from .protected_substrate import ProtectedSandboxSubstrate


class ProtectedSettlementError(RuntimeError):
    """Safe refusal without proof data."""


def _refuse() -> None:
    raise ProtectedSettlementError("protected settlement unavailable") from None


def _encode(value: dict[str, Any]) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode()


def _digest(raw: bytes | str) -> str:
    return hashlib.sha256(raw.encode() if isinstance(raw, str) else raw).hexdigest()


# Fixed keys: witness, context, resolution, repair, outbox, pending, done,
# lease, delivery state, source stream. Remaining keys are original snapshots.
# All data-dependent commands and key types are checked before the first write.
_GUARDS = """
local types = {'string','string','string','set','hash','set','string','string','hash','stream'}
for n=1,10 do
  local t = redis.call('TYPE', KEYS[n]).ok
  if t ~= 'none' and t ~= types[n] then return 0 end
end
if string.match(redis.call('INFO','server'), 'run_id:([0-9a-f]+)') ~= ARGV[1] then return 0 end
for n=11,#KEYS do
  local t = redis.call('TYPE', KEYS[n]).ok
  if t ~= 'none' and t ~= 'string' then return 0 end
  local expected = ARGV[14 + (n-11)*2]
  local present = ARGV[15 + (n-11)*2]
  local actual = redis.call('GET', KEYS[n])
  if present == '0' then
    if actual then return 0 end
  elseif actual ~= expected then return 0 end
end
local witness = redis.call('GET',KEYS[1])
local context = redis.call('GET',KEYS[2])
local resolution = redis.call('GET',KEYS[3])
if witness and witness ~= ARGV[2] then return 0 end
if context and context ~= ARGV[3] then return 0 end
local outgen = redis.call('HGET',KEYS[5],'gen')
local outrecord = redis.call('HGET',KEYS[5],'record')
if redis.call('EXISTS',KEYS[5]) == 1 and
  (outgen ~= ARGV[5] or outrecord ~= ARGV[4] or
   redis.call('HGET',KEYS[5],'done') ~= '1') then return 0 end
"""
_OUTBOX = """
redis.call('HDEL',KEYS[5],'cause')
redis.call('HSET',KEYS[5],'record',ARGV[4],'done','1','gen',ARGV[5])
redis.call('SADD',KEYS[6],ARGV[6])
if ARGV[7] == 'history_capacity' then redis.call('SET',KEYS[7],ARGV[7])
else redis.call('SET',KEYS[7],ARGV[7],'EX',ARGV[8]) end
return 1
"""
_SETTLE = (
    _GUARDS
    + """
if resolution then return 0 end
if redis.call('GET',KEYS[8]) ~= ARGV[9] or redis.call('PTTL',KEYS[8]) <= 0 then return 0 end
if redis.call('HGET',KEYS[9],'gen') ~= ARGV[10] then return 0 end
local pending = redis.call('XPENDING',KEYS[10],ARGV[11],ARGV[12],ARGV[12],1)
if #pending ~= 1 or pending[1][2] ~= ARGV[13] then return 0 end
if not witness and (outgen or redis.call('EXISTS',KEYS[7]) == 1) then return 0 end
redis.call('SADD',KEYS[4],ARGV[6])
redis.call('SET',KEYS[2],ARGV[3],'NX')
redis.call('SET',KEYS[1],ARGV[2],'NX')
"""
    + _OUTBOX
)
_REPAIR = (
    _GUARDS
    + """
if not witness then return 0 end
if resolution then
  if resolution ~= ARGV[9] and resolution ~= ARGV[10] then return 0 end
  redis.call('DEL',KEYS[5],KEYS[2])
  redis.call('SREM',KEYS[6],ARGV[6])
  redis.call('SREM',KEYS[4],ARGV[6])
  return 2
end
if not context or redis.call('SISMEMBER',KEYS[4],ARGV[6]) ~= 1 then return 0 end
"""
    + _OUTBOX
)
_CLEAR = (
    _GUARDS
    + """
if not witness or not context or not outgen then return 0 end
if redis.call('SISMEMBER',KEYS[4],ARGV[6]) ~= 1 then return 0 end
if resolution and resolution ~= ARGV[9] then return 0 end
redis.call('SET',KEYS[3],ARGV[9],'NX')
redis.call('DEL',KEYS[5],KEYS[2])
redis.call('SREM',KEYS[6],ARGV[6])
redis.call('SREM',KEYS[4],ARGV[6])
return 1
"""
)
_GRAVEYARD = _CLEAR.replace(
    "redis.call('SET',KEYS[3],ARGV[9],'NX')",
    """local deadtype = redis.call('TYPE',KEYS[#KEYS]).ok
if deadtype ~= 'none' and deadtype ~= 'stream' then return 0 end
if not resolution then
 redis.call('XADD',KEYS[#KEYS],'MAXLEN','~',ARGV[#ARGV-3],'*',
 'event_id',ARGV[6],'completion',ARGV[#ARGV-2],'dl_source','completion-outbox',
 'dl_reason',ARGV[#ARGV-1],'dl_dead_lettered_at',ARGV[#ARGV])
end
redis.call('SET',KEYS[3],ARGV[9],'NX')""",
).replace("for n=11,#KEYS do", "for n=11,#KEYS-1 do")


@dataclass(frozen=True, repr=False)
class OwedCompletion:
    """Validated cold record, never a Dispatchable."""

    proof: RetainedProof
    witness: TerminalWitness
    context: CompletionContext | None
    record: CompletionRecord | None
    resolution: CompletionResolution | None


class ProtectedMarkers(Markers):
    """Kernel-injected retention wrapper bound to one authenticated runtime."""

    supports_answer_outbox = False

    def __init__(
        self,
        redis: Redis,
        config: WorkerConfig,
        *,
        broker: AuthenticatedWorkerClient,
        manifest: Manifest,
        substrate: ProtectedSandboxSubstrate,
        launch_client: ProtectedLaunchClient,
    ) -> None:
        if (
            type(broker) is not AuthenticatedWorkerClient
            or type(manifest) is not Manifest
            or type(substrate) is not ProtectedSandboxSubstrate
            or type(launch_client) is not ProtectedLaunchClient
        ):
            _refuse()
        self._namespace = derive_lane_namespace(manifest)
        if (
            redis is not broker.lane().runs
            or config.key_prefix != self._namespace.prefix
            or config.stream != self._namespace.stream
            or config.consumer_group != self._namespace.group
        ):
            _refuse()
        super().__init__(redis, config)
        self._broker, self._manifest = broker, manifest
        self._substrate, self._launch_client = substrate, launch_client
        self._confirmed: dict[tuple[str, str], OwedCompletion] = {}
        self._deleted: dict[tuple[str, str], OwedCompletion] = {}

    def settlement_key(self, event_id: str) -> str:
        return f"{self._namespace.prefix}:settlement:{event_id}"

    def context_key(self, event_id: str, generation: str) -> str:
        return f"{self._namespace.prefix}:completion-context:{event_id}:{generation}"

    def resolution_key(self, event_id: str, generation: str) -> str:
        return f"{self._namespace.prefix}:completion-resolution:{event_id}:{generation}"

    def repair_pending_key(self) -> str:
        return f"{self._namespace.prefix}:completion-repair:pending"

    def _common(self, proof: RetainedProof, generation: str) -> dict[str, Any]:
        i = proof.intent.as_dict()
        m = self._manifest.as_dict()
        return dict(
            schema_version=1,
            event_id=i["event_id"],
            intent_digest=delivery_digest(DeliveryIdentity(**i["identity"])),
            manifest_digest=self._manifest.digest,
            runtime_id=m["runtime_id"],
            runtime_generation=m["runtime_generation"],
            broker_run_id=m["broker_identity"]["run_id"],
            envelope_sha256=i["envelope_sha256"],
            payload_sha256=i["payload_sha256"],
            committed_stream_id=proof.receipt.as_dict()["stream_id"],
            completion_generation=generation,
        )

    async def _raw(self, key: str) -> bytes | None:
        raw = await self._redis.execute_command(  # type: ignore[no-untyped-call]
            "GET", key, NEVER_DECODE=True
        )
        if raw is not None and (type(raw) is not bytes or len(raw) > 65536):
            _refuse()
        return cast(bytes | None, raw)

    async def restore_owed(self, event_id: str) -> OwedCompletion | None:
        """Restore immutable original proof and exact context without live scope."""
        try:
            proof = await restore_committed_event(
                self._broker, event_id, trusted_manifest=self._manifest
            )
            raw = await self._raw(self.settlement_key(event_id))
            if proof is None or raw is None:
                return None
            witness = TerminalWitness(raw)
            w = witness.as_dict()
            if (
                any(w[k] != v for k, v in self._common(proof, w["completion_generation"]).items())
                or w["delivery_stream"] != self._namespace.stream
                or w["delivery_group"] != self._namespace.group
            ):
                return None
            cr = await self._raw(self.context_key(event_id, w["completion_generation"]))
            rr = await self._raw(self.resolution_key(event_id, w["completion_generation"]))
            context = CompletionContext(cr) if cr is not None else None
            resolution = CompletionResolution(rr) if rr is not None else None
            if resolution is not None:
                r = resolution.as_dict()
                if (
                    r["event_id"] != event_id
                    or r["completion_generation"] != w["completion_generation"]
                ):
                    return None
            record = None
            if context is not None:
                c = context.as_dict()
                if (
                    context.digest != w["completion_context_sha256"]
                    or any(
                        c[k] != v
                        for k, v in self._common(proof, w["completion_generation"]).items()
                    )
                    or _digest(c["completion_record_json"]) != w["completion_record_sha256"]
                ):
                    return None
                record = CompletionRecord.model_validate_json(c["completion_record_json"])
            if context is None and resolution is None:
                return None
            return OwedCompletion(proof, witness, context, record, resolution)
        except Exception:  # noqa: BLE001  Retain owed work on malformed proof or outage.
            return None

    def _operation(self, owed: OwedCompletion) -> tuple[list[str], list[Any]]:
        w = owed.witness.as_dict()
        event_id, gen = w["event_id"], w["completion_generation"]
        keys = [
            self.settlement_key(event_id),
            self.context_key(event_id, gen),
            self.resolution_key(event_id, gen),
            self.repair_pending_key(),
            self._config.completion_key(event_id),
            self._config.completions_pending_key(),
            self._config.done_key(event_id),
            self._config.delivery_lease_key(
                w["delivery_stream"], w["delivery_group"], w["delivery_entry_id"]
            ),
            self._config.delivery_state_key(
                w["delivery_stream"], w["delivery_group"], w["delivery_entry_id"]
            ),
            w["delivery_stream"],
        ]
        args: list[Any] = [
            w["broker_run_id"],
            owed.witness.canonical_bytes,
            owed.context.canonical_bytes if owed.context else b"",
            owed.context.as_dict()["completion_record_json"] if owed.context else "",
            gen,
            event_id,
            w["marker_value"],
            self._done_ttl_s(),
            "",
            "",
            "",
            "",
            "",
        ]
        for key, raw in owed.proof.proof_snapshots:
            keys.append(key)
            args.extend([raw if raw is not None else b"", "1" if raw is not None else "0"])
        return keys, args

    async def settle_fenced(
        self,
        event_id: str,
        record: CompletionRecord,
        *,
        stream: str,
        group: str,
        entry_id: str,
        owner: str,
        generation: int,
        marker_value: DoneMarkerValue,
    ) -> str | None:
        try:
            scope = protected_execution._CURRENT.get()
            if (
                scope is None
                or not protected_execution._locally_valid(scope, self._broker, self._manifest)
                or scope.client is not self._broker
                or scope.manifest.canonical_bytes != self._manifest.canonical_bytes
                or (scope.stream, scope.group, scope.entry_id, scope.owner, scope.generation)
                != (stream, group, entry_id, owner, generation)
                or stream != self._namespace.stream
                or group != self._namespace.group
                or scope.delivery.turn.event_id != event_id
                or scope.revoked.is_set()
                or record.event_id != event_id
                or record.event.event_id != event_id
            ):
                return None
            proof = await restore_committed_event(
                self._broker, event_id, trusted_manifest=self._manifest
            )
            if (
                proof is None
                or proof.intent.canonical_bytes != scope.delivery.intent.canonical_bytes
                or proof.envelope.canonical_bytes != scope.delivery.envelope.canonical_bytes
            ):
                return None
            retirement = await self._substrate.retire_delivery(scope.delivery)
            if retirement is None or not self._launch_client.is_owned_retirement(
                retirement, delivery=scope.delivery
            ):
                return None
            if not protected_execution._locally_valid(scope, self._broker, self._manifest):
                return None
            raw = record.model_dump_json()
            old = await self.restore_owed(event_id)
            if old is not None:
                w = old.witness.as_dict()
                if (
                    old.context is None
                    or old.context.as_dict()["completion_record_json"] != raw
                    or (w["delivery_entry_id"], w["delivery_owner"], w["delivery_generation"])
                    != (entry_id, owner, str(generation))
                    or w["retirement_ref"] != retirement.reference
                    or w["retirement_proof_sha256"] != retirement.digest
                ):
                    return None
                owed = old
            else:
                gen = uuid.uuid4().hex
                common = self._common(proof, gen)
                context = CompletionContext(_encode(dict(**common, completion_record_json=raw)))
                witness = TerminalWitness(
                    _encode(
                        dict(
                            **common,
                            delivery_stream=stream,
                            delivery_group=group,
                            delivery_entry_id=entry_id,
                            delivery_owner=owner,
                            delivery_generation=str(generation),
                            completion_record_sha256=_digest(raw),
                            completion_context_sha256=context.digest,
                            marker_value=marker_value,
                            retirement_ref=retirement.reference,
                            retirement_proof_sha256=retirement.digest,
                        )
                    )
                )
                owed = OwedCompletion(proof, witness, context, record, None)
            keys, args = self._operation(owed)
            args[8:13] = [owner, str(generation), group, entry_id, scope.consumer]
            if not protected_execution._locally_valid(scope, self._broker, self._manifest):
                return None
            result = await self._redis.eval(_SETTLE, len(keys), *keys, *args)
            return owed.witness.as_dict()["completion_generation"] if result == 1 else None
        except Exception:  # noqa: BLE001  No cleanup/proof outage can grant terminal ownership.
            return None

    async def repair_completion(self, event_id: str) -> OwedCompletion | None:
        owed = await self.restore_owed(event_id)
        if owed is None:
            return None
        keys, args = self._operation(owed)
        gen = owed.witness.as_dict()["completion_generation"]
        args[8:10] = [
            self._resolution(event_id, gen, "emitted"),
            self._resolution(event_id, gen, "graveyard"),
        ]
        result = await self._redis.eval(_REPAIR, len(keys), *keys, *args)
        return owed if result == 1 else None

    def _resolution(self, event_id: str, generation: str, resolution: str) -> bytes:
        return CompletionResolution(
            _encode(
                dict(
                    schema_version=1,
                    event_id=event_id,
                    completion_generation=generation,
                    resolution=resolution,
                )
            )
        ).canonical_bytes

    async def is_terminal(self, event_id: str) -> bool:
        if await self.restore_owed(event_id) is not None:
            return True
        # Missing original proof parks; never turn proof loss into execution permission.
        proof = await restore_committed_event(
            self._broker, event_id, trusted_manifest=self._manifest
        )
        if proof is None:
            _refuse()
        if await self._raw(self.settlement_key(event_id)) is not None:
            _refuse()  # Contradictory terminal evidence parks for owner investigation.
        return await super().is_terminal(event_id)

    async def clear_completion(self, event_id: str, *, generation: str) -> bool:
        owed = self._confirmed.get((event_id, generation))
        if owed is None:
            return False
        keys, args = self._operation(owed)
        args[8] = self._resolution(event_id, generation, "emitted")
        result = await self._redis.eval(_CLEAR, len(keys), *keys, *args)
        if result == 1:
            self._confirmed.pop((event_id, generation), None)
        return bool(result == 1)

    async def dead_letter_completion(
        self, record: CompletionRecord, *, generation: str, reason: str
    ) -> bool:
        owed = self._deleted.get((record.event_id, generation))
        if (
            owed is None
            or owed.record is None
            or owed.context is None
            or owed.witness.as_dict()["completion_generation"] != generation
            or owed.record.event != record.event
            or reason != "thread deleted at provider"
        ):
            return False
        keys, args = self._operation(owed)
        args[8] = self._resolution(record.event_id, generation, "graveyard")
        keys.append(self._namespace.dead)
        args.extend(
            [
                self._config.dead_letter_maxlen,
                record.event.model_dump_json(),
                reason,
                datetime.now(UTC).isoformat(),
            ]
        )
        result = await self._redis.eval(_GRAVEYARD, len(keys), *keys, *args)
        if result == 1:
            self._deleted.pop((record.event_id, generation), None)
        return bool(result == 1)

    async def mark_done(self, *args: Any, **kwargs: Any) -> None:
        _refuse()

    async def mark_done_without_completion(self, *args: Any, **kwargs: Any) -> None:
        _refuse()

    async def settle_fenced_without_completion(self, *args: Any, **kwargs: Any) -> bool:
        _refuse()
        return False

    async def mark_completion_pending(self, *args: Any, **kwargs: Any) -> str:
        _refuse()
        return ""

    async def settle_fenced_and_publish(self, *args: Any, **kwargs: Any) -> str | None:
        _refuse()
        return None

    async def publish_successor(self, *args: Any, **kwargs: Any) -> None:
        _refuse()

    async def drop_pending_member(self, *args: Any, **kwargs: Any) -> None:
        _refuse()
