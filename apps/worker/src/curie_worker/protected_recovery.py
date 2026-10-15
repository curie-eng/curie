"""Closed recovery metadata; parsing alone issues no protected authority."""

from __future__ import annotations

import asyncio
import hashlib
import json
import math
import re
import time
from collections.abc import AsyncIterator, Mapping
from contextlib import asynccontextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from datetime import datetime
from typing import TYPE_CHECKING, cast
from weakref import WeakValueDictionary

from channel_protocol.reply import TurnCompleted
from curie_protected_hooks.admission_records import Envelope
from curie_protected_hooks.authority_records import Manifest
from curie_protected_hooks.execution_namespace import derive_lane_namespace
from curie_protected_hooks.worker_transport import AuthenticatedWorkerClient
from curie_protected_hooks.worker_verification import (
    Dispatchable,
    RetainedProof,
    restore_committed_event,
    verify_delivery,
)
from curie_telemetry import TRACEPARENT_STREAM_FIELD, canonicalize_traceparent, inject_trace_context

from .capacity_wait import (
    _RECONCILE_LUA,
    _WAKE_LUA,
    WAIT_GENERATION_FIELD,
    WAIT_REPLY_REF_FIELD,
    WAIT_TERMINAL_ONLY_FIELD,
    CapacityWaitStore,
)
from .config import WorkerConfig
from .delivery_lease import DeliveryLease
from .protected_binding import current_protected_delivery, protected_delivery_scope

if TYPE_CHECKING:
    from .protected_markers import OwedCompletion, ProtectedMarkers


class RecoveryUnavailable(RuntimeError):
    """Fixed diagnostic, without copied payload or internal ownership token."""


def _refuse() -> None:
    raise RecoveryUnavailable("protected recovery unavailable") from None


@dataclass(frozen=True, slots=True)
class CapacityCopyMetadata:
    """Syntactic information only; never Dispatchable, lease or start proof."""

    source_fields: tuple[tuple[str, str], ...] = field(repr=False)
    generation: int
    reply_ref: str | None
    terminal_only: bool
    traceparent: str | None


def parse_capacity_fields(fields: Mapping[str, str]) -> CapacityCopyMetadata:
    required = {"payload", "protected_envelope", WAIT_GENERATION_FIELD}
    allowed = required | {WAIT_REPLY_REF_FIELD, WAIT_TERMINAL_ONLY_FIELD, TRACEPARENT_STREAM_FIELD}
    if (
        not required <= fields.keys() <= allowed
        or any(type(k) is not str or type(v) is not str for k, v in fields.items())
        or any(len(value.encode("utf-8")) > 262144 for value in fields.values())
    ):
        _refuse()
    generation = fields[WAIT_GENERATION_FIELD]
    if (
        not generation.isascii()
        or not generation.isdecimal()
        or generation.startswith("0")
        or len(generation) > 19
        or int(generation) > 9223372036854775807
        or (WAIT_REPLY_REF_FIELD in fields and not fields[WAIT_REPLY_REF_FIELD])
        or (WAIT_TERMINAL_ONLY_FIELD in fields and fields[WAIT_TERMINAL_ONLY_FIELD] != "1")
    ):
        _refuse()
    carrier = fields.get(TRACEPARENT_STREAM_FIELD)
    traceparent = canonicalize_traceparent(carrier)
    if carrier is not None and traceparent is None:
        _refuse()
    return CapacityCopyMetadata(
        (("payload", fields["payload"]), ("protected_envelope", fields["protected_envelope"])),
        int(generation),
        fields.get(WAIT_REPLY_REF_FIELD),
        WAIT_TERMINAL_ONLY_FIELD in fields,
        traceparent,
    )


# Fixed owner-written fields, including the two fields missing from the public DTO.
WAIT_SNAPSHOT_FIELDS = (
    "fields",
    "state",
    "generation",
    "wake_entry",
    "reply_ref",
    "terminal_only",
    "cause",
    "deadline_ms",
    "grant_epoch",
    "grant_confirmed",
    "grant_unknown",
)
_IMMUTABLE_WAIT_FIELDS = (
    "fields",
    "generation",
    "wake_entry",
    "reply_ref",
    "terminal_only",
    "cause",
    "deadline_ms",
)


@dataclass(frozen=True, slots=True, repr=False, weakref_slot=True)
class ValidatedRecoveryCopy:
    """Private issued context; a DTO constructor cannot install this scope."""

    delivery: Dispatchable
    metadata: CapacityCopyMetadata
    entry_id: str
    entry_fields: tuple[tuple[str, str], ...]
    wait_snapshot: tuple[tuple[str, str | None], ...]
    expired: bool
    _issuer: ProtectedRecoveryAdapter


@dataclass(frozen=True, slots=True, repr=False)
class RecoveryGateInputs:
    """Fixed data only for parent's single atomic ownership Lua gate."""

    entry_fields: tuple[tuple[str, str], ...]
    wait_key: str
    wait_snapshot: tuple[tuple[str, str | None], ...]


_READ_COPY = """
if redis.call('TYPE',KEYS[1]).ok~='hash' then return {} end
if redis.call('TYPE',KEYS[2]).ok~='stream' then return {} end
local rows=redis.call('XRANGE',KEYS[2],ARGV[1],ARGV[1],'COUNT',1)
if #rows~=1 then return {} end
local fields={}
for n=2,#ARGV do fields[#fields+1]=redis.call('HGET',KEYS[1],ARGV[n]) or false end
return {rows[1][2],fields,redis.call('TIME')}
"""
_CURRENT_RECOVERY: ContextVar[ValidatedRecoveryCopy | None] = ContextVar(
    "protected_recovery_copy", default=None
)


def _text(value: object) -> str:
    if type(value) is bytes:
        return value.decode("utf-8", errors="strict")
    if type(value) is not str:
        raise RecoveryUnavailable("protected recovery unavailable")
    return value


def _pairs(pairs: list[tuple[str, object]]) -> dict[str, object]:
    value: dict[str, object] = {}
    for key, item in pairs:
        if key in value:
            raise RecoveryUnavailable("protected recovery unavailable")
        value[key] = item
    return value


@dataclass(frozen=True, slots=True, repr=False, weakref_slot=True)
class RetainedCopyCleanup:
    """Private retained cleanup data; never a turn or execution-context provider."""

    owed: OwedCompletion
    metadata: CapacityCopyMetadata
    entry_id: str
    entry_fields: tuple[tuple[str, str], ...]
    wait_snapshot: tuple[tuple[str, str | None], ...]
    lease: DeliveryLease
    _issuer: ProtectedRecoveryAdapter


class ProtectedRecoveryAdapter:
    """Verify actual closed current copies without changing row, PEL or admission."""

    def __init__(
        self,
        client: AuthenticatedWorkerClient,
        manifest: Manifest,
        config: WorkerConfig,
        *,
        markers: ProtectedMarkers | None = None,
    ):
        if type(client) is not AuthenticatedWorkerClient or type(manifest) is not Manifest:
            _refuse()
        ns = derive_lane_namespace(manifest)
        if type(config) is not WorkerConfig or (
            config.key_prefix,
            config.stream,
            config.consumer_group,
            config.dead_letter_stream_name(),
        ) != (ns.prefix, ns.stream, ns.group, ns.dead):
            _refuse()
        self._client, self._manifest, self._config = client, manifest, config
        self._issued: WeakValueDictionary[int, ValidatedRecoveryCopy] = WeakValueDictionary()
        from .protected_markers import ProtectedMarkers

        if markers is not None and (
            type(markers) is not ProtectedMarkers
            or markers._broker is not client
            or markers._manifest.canonical_bytes != manifest.canonical_bytes
        ):
            _refuse()
        self._markers = markers
        self._cleanup_issued: WeakValueDictionary[int, RetainedCopyCleanup] = WeakValueDictionary()

    def _wait_key(self, event_id: str) -> str:
        return self._config.key_prefix + ":capacity_wait:record:" + event_id

    async def _read(
        self,
        event_id: str,
        entry_id: str,
        fields: Mapping[str, str],
    ) -> tuple[tuple[tuple[str, str | None], ...], int] | None:
        raw = await self._client.lane().runs.eval(
            _READ_COPY,
            2,
            self._wait_key(event_id),
            self._config.stream,
            entry_id,
            *WAIT_SNAPSHOT_FIELDS,
        )
        if type(raw) is not list or len(raw) != 3 or len(raw[0]) != len(fields) * 2:
            return None
        row = {_text(raw[0][n]): _text(raw[0][n + 1]) for n in range(0, len(raw[0]), 2)}
        if row != fields or len(raw[1]) != len(WAIT_SNAPSHOT_FIELDS):
            return None
        snapshot = tuple(
            (name, None if item is None else _text(item))
            for name, item in zip(WAIT_SNAPSHOT_FIELDS, raw[1], strict=True)
        )
        if any(
            value is not None
            and len(value.encode("utf-8")) > (300000 if name == "fields" else 16384)
            for name, value in snapshot
        ):
            return None
        now_ms = int(raw[2][0]) * 1000 + int(raw[2][1]) // 1000
        return snapshot, now_ms

    def _matches(
        self,
        metadata: CapacityCopyMetadata,
        entry_id: str,
        snapshot: tuple[tuple[str, str | None], ...],
    ) -> bool:
        wait = dict(snapshot)
        original = json.loads(wait["fields"] or "", object_pairs_hook=_pairs)
        if type(original) is not dict or any(type(v) is not str for v in original.values()):
            return False
        if (
            not {"payload", "protected_envelope"}
            <= original.keys()
            <= {
                "payload",
                "protected_envelope",
                WAIT_GENERATION_FIELD,
                WAIT_REPLY_REF_FIELD,
                WAIT_TERMINAL_ONLY_FIELD,
                TRACEPARENT_STREAM_FIELD,
            }
        ):
            return False
        if any(original.get(name) != value for name, value in metadata.source_fields):
            return False
        terminal = wait["terminal_only"] == "1"
        return (
            wait["generation"] == str(metadata.generation)
            and wait["wake_entry"] == entry_id
            and wait["state"] in {"woken", "active"}
            and (wait["reply_ref"] or None) == metadata.reply_ref
            and wait["terminal_only"] in {None, "", "1"}
            and terminal == metadata.terminal_only
            and (
                wait["cause"] in {"capacity_wait_expired", "delivery_exhausted", "grant_unknown"}
                if terminal
                else wait["cause"] in {None, ""}
            )
            and wait["deadline_ms"] is not None
            and wait["deadline_ms"].isascii()
            and wait["deadline_ms"].isdecimal()
            and 0 < int(wait["deadline_ms"]) <= 9223372036854775807
        )

    async def verify(
        self, entry_id: str, fields: Mapping[str, str], *, lease: DeliveryLease | None = None
    ) -> ValidatedRecoveryCopy | None:
        try:
            metadata = parse_capacity_fields(fields)
            delivery = await verify_delivery(
                self._client, dict(metadata.source_fields), trusted_manifest=self._manifest
            )
            if type(delivery) is not Dispatchable:
                return None
            if entry_id == delivery.intent.as_dict()["reserved_stream_id"]:
                return None
            loaded = await self._read(delivery.turn.event_id, entry_id, fields)
            if loaded is None or not self._matches(metadata, entry_id, loaded[0]):
                return None
            snapshot, now = loaded
            if dict(snapshot)["state"] == "active" and not self._lease_matches(lease, entry_id):
                return None
            copy = ValidatedRecoveryCopy(
                delivery,
                metadata,
                entry_id,
                tuple(sorted(fields.items())),
                snapshot,
                now >= int(dict(snapshot)["deadline_ms"] or "0"),
                self,
            )
            self._issued[id(copy)] = copy
            return copy
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001  Private broker/payload exceptions never escape.
            return None

    async def eligible(self, entry_id: str, fields: Mapping[str, str]) -> bool:
        """Read-only shared transport selection; no issued context or lease authority.

        Active wakes can be selected only through the shared live-owner and
        positive-idle transfer fences. Handler verification still requires its
        actual newly acquired lease before installing an active copy scope.
        """
        try:
            metadata = parse_capacity_fields(fields)
            delivery = await verify_delivery(
                self._client,
                dict(metadata.source_fields),
                trusted_manifest=self._manifest,
            )
            if (
                type(delivery) is not Dispatchable
                or entry_id == delivery.intent.as_dict()["reserved_stream_id"]
            ):
                return False
            loaded = await self._read(delivery.turn.event_id, entry_id, fields)
            return loaded is not None and self._matches(metadata, entry_id, loaded[0])
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001  Selection cannot turn proof outage into permission.
            return False

    def _cleanup_lease_matches(self, lease: DeliveryLease, entry_id: str) -> bool:
        return (
            type(lease) is DeliveryLease
            and (lease.stream, lease.group, lease.entry_id)
            == (self._config.stream, self._config.consumer_group, entry_id)
            and bool(lease.owner)
            and lease.generation > 0
            and not lease.lost.is_set()
            and not lease.acknowledged.is_set()
            and not lease.entry_vanished.is_set()
            and lease.local_deadline_monotonic > time.monotonic()
        )

    async def _retained_cleanup_data(
        self,
        entry_id: str,
        fields: Mapping[str, str],
    ) -> tuple[OwedCompletion, CapacityCopyMetadata, tuple[tuple[str, str | None], ...]] | None:
        if self._markers is None:
            return None
        metadata = parse_capacity_fields(fields)
        source = dict(metadata.source_fields)
        envelope = Envelope(source["protected_envelope"].encode())
        owed = await self._markers.restore_owed(envelope.as_dict()["event_id"])
        if (
            owed is None
            or entry_id == owed.proof.intent.as_dict()["reserved_stream_id"]
            or source["protected_envelope"].encode() != owed.proof.envelope.canonical_bytes
            or hashlib.sha256(source["payload"].encode()).hexdigest()
            != owed.proof.envelope.as_dict()["payload_sha256"]
        ):
            return None
        loaded = await self._read(owed.witness.as_dict()["event_id"], entry_id, fields)
        if loaded is None or not self._matches(metadata, entry_id, loaded[0]):
            return None
        return owed, metadata, loaded[0]

    async def cleanup_eligible(self, entry_id: str, fields: Mapping[str, str]) -> bool:
        """Read-only transport selection; never issues a cleanup or positive scope."""
        try:
            return await self._retained_cleanup_data(entry_id, fields) is not None
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001  Unknown terminal evidence remains held.
            return False

    async def verify_cleanup(
        self,
        entry_id: str,
        fields: Mapping[str, str],
        lease: DeliveryLease,
    ) -> RetainedCopyCleanup | None:
        try:
            if not self._cleanup_lease_matches(lease, entry_id):
                return None
            data = await self._retained_cleanup_data(entry_id, fields)
            if data is None or not self._cleanup_lease_matches(lease, entry_id):
                return None
            owed, metadata, snapshot = data
            issued = RetainedCopyCleanup(
                owed,
                metadata,
                entry_id,
                tuple(sorted(fields.items())),
                snapshot,
                lease,
                self,
            )
            self._cleanup_issued[id(issued)] = issued
            return issued
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001  Restoration is never absence/cleanup authority.
            return None

    def cleanup_is_issued(self, issued: RetainedCopyCleanup, lease: DeliveryLease) -> bool:
        return (
            type(issued) is RetainedCopyCleanup
            and issued._issuer is self
            and self._cleanup_issued.get(id(issued)) is issued
            and issued.lease is lease
            and self._cleanup_lease_matches(lease, issued.entry_id)
        )

    def _lease_matches(self, lease: DeliveryLease | None, entry_id: str) -> bool:
        return (
            type(lease) is DeliveryLease
            and (lease.stream, lease.group, lease.entry_id)
            == (self._config.stream, self._config.consumer_group, entry_id)
            and not lease.lost.is_set()
            and not lease.acknowledged.is_set()
            and not lease.entry_vanished.is_set()
            and lease.local_deadline_monotonic > time.monotonic()
            and lease.remaining_s() > 0
        )


def current_protected_recovery() -> ValidatedRecoveryCopy | None:
    return _CURRENT_RECOVERY.get()


@asynccontextmanager
async def recovery_delivery_scope(copy: ValidatedRecoveryCopy) -> AsyncIterator[None]:
    if (
        type(copy) is not ValidatedRecoveryCopy
        or type(copy._issuer) is not ProtectedRecoveryAdapter
        or copy._issuer._issued.get(id(copy)) is not copy
    ):
        raise RecoveryUnavailable("protected recovery unavailable")
    token = _CURRENT_RECOVERY.set(copy)
    try:
        async with protected_delivery_scope(copy.delivery):
            yield
    finally:
        _CURRENT_RECOVERY.reset(token)


async def recovery_gate_inputs(
    client: AuthenticatedWorkerClient, manifest: Manifest, lease: DeliveryLease
) -> RecoveryGateInputs | None:
    """Refresh recognized snapshot; caller MUST atomically verify it with ownership.

    This does not itself authorize effects. Parent Lua compares all returned row
    and wait bytes, server TIME/deadline, settlement absence, epoch/PEL/lease/gen
    together immediately before each positive operation.
    """
    copy = _CURRENT_RECOVERY.get()
    if copy is None:
        return None
    if (
        type(copy) is not ValidatedRecoveryCopy
        or type(copy._issuer) is not ProtectedRecoveryAdapter
        or copy._issuer._issued.get(id(copy)) is not copy
        or current_protected_delivery() is not copy.delivery
    ):
        raise RecoveryUnavailable("protected recovery unavailable")
    issuer = copy._issuer
    if (
        issuer._client is not client
        or issuer._manifest.canonical_bytes != manifest.canonical_bytes
        or not issuer._lease_matches(lease, copy.entry_id)
        or copy.metadata.terminal_only
    ):
        raise RecoveryUnavailable("protected recovery unavailable")
    loaded = await issuer._read(copy.delivery.turn.event_id, copy.entry_id, dict(copy.entry_fields))
    if loaded is None or not issuer._matches(copy.metadata, copy.entry_id, loaded[0]):
        raise RecoveryUnavailable("protected recovery unavailable")
    snapshot, now = loaded
    old, fresh = dict(copy.wait_snapshot), dict(snapshot)
    if (
        any(old[name] != fresh[name] for name in _IMMUTABLE_WAIT_FIELDS)
        or now >= int(fresh["deadline_ms"] or "0")
        or fresh["grant_unknown"] == "1"
    ):
        raise RecoveryUnavailable("protected recovery unavailable")
    return RecoveryGateInputs(
        copy.entry_fields,
        issuer._wait_key(copy.delivery.turn.event_id),
        snapshot,
    )


@dataclass(frozen=True, slots=True, repr=False)
class GraveyardDiagnostic:
    """Known diagnostic grammar; never terminal, refund, dispatch or replay proof."""

    source_fields: tuple[tuple[str, str], ...] | None
    event_id: str | None
    original_id: str | None
    delivery_count: int | None
    completion: str | None


def parse_graveyard_fields(fields: Mapping[str, str]) -> GraveyardDiagnostic:
    if (
        not fields
        or any(type(k) is not str or type(v) is not str for k, v in fields.items())
        or any(len(v.encode("utf-8")) > 262144 for v in fields.values())
    ):
        _refuse()
    base = {"dl_reason", "dl_dead_lettered_at"}
    if not base <= fields.keys():
        _refuse()
    timestamp = datetime.fromisoformat(fields["dl_dead_lettered_at"])
    if timestamp.tzinfo is None or not fields["dl_reason"]:
        _refuse()
    if fields.get("dl_source") == "completion-outbox":
        allowed = base | {"event_id", "completion", "dl_source", "dl_delivery_count"}
        required = allowed - {"dl_delivery_count"}
        if not required <= fields.keys() <= allowed or fields.get("dl_delivery_count", "1") != "1":
            _refuse()
        completion = TurnCompleted.model_validate_json(fields["completion"])
        if completion.event_id != fields["event_id"]:
            _refuse()
        return GraveyardDiagnostic(None, completion.event_id, None, 1, fields["completion"])
    required = base | {"payload", "protected_envelope", "dl_original_id", "dl_delivery_count"}
    allowed = required | {
        WAIT_GENERATION_FIELD,
        WAIT_REPLY_REF_FIELD,
        WAIT_TERMINAL_ONLY_FIELD,
        TRACEPARENT_STREAM_FIELD,
    }
    if not required <= fields.keys() <= allowed:
        _refuse()
    count = fields["dl_delivery_count"]
    if (
        not count.isascii()
        or not count.isdecimal()
        or count.startswith("0")
        or len(count) > 19
        or int(count) > 9223372036854775807
    ):
        _refuse()
    if (
        re.fullmatch(r"(?:0|[1-9][0-9]{0,19})-(?:0|[1-9][0-9]{0,19})", fields["dl_original_id"])
        is None
        or fields["dl_original_id"] == "0-0"
    ):
        _refuse()
    primary = tuple((name, fields[name]) for name in ("payload", "protected_envelope"))
    copied = {
        k: v for k, v in fields.items() if k not in base | {"dl_original_id", "dl_delivery_count"}
    }
    if WAIT_GENERATION_FIELD in copied:
        parse_capacity_fields(copied)
    elif copied.keys() != {"payload", "protected_envelope"}:
        _refuse()
    return GraveyardDiagnostic(primary, None, fields["dl_original_id"], int(count), None)


@dataclass(frozen=True, slots=True, repr=False)
class ObservedGraveyardEntry:
    """Original retained proof accompanies observation, never execution permission."""

    entry_id: str
    diagnostic: GraveyardDiagnostic
    proof: RetainedProof


class ProtectedGraveyardObserver:
    """Bounded read-only inspection; no group, ACK, requeue, refund or start method."""

    def __init__(self, client: AuthenticatedWorkerClient, manifest: Manifest):
        if type(client) is not AuthenticatedWorkerClient or type(manifest) is not Manifest:
            _refuse()
        self._client, self._manifest = client, manifest
        self._namespace = derive_lane_namespace(manifest)

    async def observe(
        self, *, limit: int = 64, wall_budget_s: float = 5
    ) -> tuple[ObservedGraveyardEntry, ...]:
        if (
            type(limit) is not int
            or not 1 <= limit <= 64
            or not math.isfinite(wall_budget_s)
            or not 0 < wall_budget_s <= 30
        ):
            _refuse()
        observed: list[ObservedGraveyardEntry] = []
        try:
            async with asyncio.timeout(wall_budget_s):
                rows = await self._client.lane().runs.xrevrange(self._namespace.dead, count=limit)
                for raw_entry_id, raw_fields in rows or []:
                    try:
                        if raw_fields is None:
                            continue
                        fields = {_text(k): _text(v) for k, v in raw_fields.items()}
                        entry_id = _text(raw_entry_id)
                        diagnostic = parse_graveyard_fields(fields)
                        envelope = None
                        if diagnostic.source_fields is not None:
                            source = dict(diagnostic.source_fields)
                            envelope = Envelope(source["protected_envelope"].encode("utf-8"))
                            event_id = envelope.as_dict()["event_id"]
                        else:
                            event_id = diagnostic.event_id
                        if type(event_id) is not str:
                            continue
                        proof = await restore_committed_event(
                            self._client,
                            event_id,
                            trusted_manifest=self._manifest,
                        )
                        if proof is None:
                            continue
                        if envelope is not None and (
                            proof.envelope.canonical_bytes != envelope.canonical_bytes
                            or hashlib.sha256(source["payload"].encode("utf-8")).hexdigest()
                            != proof.envelope.as_dict()["payload_sha256"]
                        ):
                            continue
                        observed.append(ObservedGraveyardEntry(entry_id, diagnostic, proof))
                    except asyncio.CancelledError:
                        raise
                    except Exception:  # noqa: BLE001  Missing/corrupt observation is never authority.
                        continue
        except TimeoutError:
            pass
        return tuple(observed)


# The two fixed scheduling programs retain shared transport semantics. The
# prepended read-only gate must finish before ANY shared scheduling write.
def _scheduling_lua(shared: str, types: tuple[str, ...], argument_count: int) -> str:
    count = len(types)
    return (
        f"""
local types=cjson.decode('{json.dumps(types)}')
for n=1,{count} do
 local actual=redis.call('TYPE',KEYS[n]).ok
 if actual~='none' and actual~=types[n] then return {{0,'protected_pending'}} end
end
local permanent={count + 1}
local stream={count + 2}
if redis.call('TYPE',KEYS[permanent]).ok~='none' then return {{0,'protected_pending'}} end
if redis.call('TYPE',KEYS[stream]).ok~='stream' then return {{0,'protected_pending'}} end
if redis.call('EXISTS',KEYS[{6 if count == 8 else 3}])~=0
 or redis.call('HGET',KEYS[{7 if count == 8 else 4}],'done')=='1'
 then return {{0,'protected_pending'}} end
if string.match(redis.call('INFO','server'),'run_id:([0-9a-f]+)')~=ARGV[{argument_count + 1}]
 then return {{0,'protected_pending'}} end
local wait=cjson.decode(ARGV[{argument_count + 2}])
if #redis.call('HGETALL',KEYS[1])~=#wait*2 then return {{0,'protected_pending'}} end
for _,pair in ipairs(wait) do
 if redis.call('HGET',KEYS[1],pair[1])~=pair[2] then return {{0,'protected_pending'}} end
end
local proof=cjson.decode(ARGV[{argument_count + 3}])
if #proof~=#KEYS-stream then return {{0,'protected_pending'}} end
for index,pair in ipairs(proof) do
 local key=KEYS[stream+index]
 if pair[1]~=key then return {{0,'protected_pending'}} end
 local kind=redis.call('TYPE',key).ok
 if kind~='none' and kind~='string' then return {{0,'protected_pending'}} end
 local actual=redis.call('GET',key)
 if pair[2]==cjson.null then
  if actual then return {{0,'protected_pending'}} end
 elseif actual~=pair[2] then return {{0,'protected_pending'}} end
end
"""
        + shared
    )


_PROTECTED_WAKE = _scheduling_lua(
    _WAKE_LUA,
    ("hash", "zset", "zset", "zset", "string", "string", "hash", "zset"),
    6,
)
_PROTECTED_RECONCILE = _scheduling_lua(
    _RECONCILE_LUA,
    ("hash", "zset", "string", "hash", "zset"),
    8,
)
_WAIT_HASH_FIELDS = frozenset(
    {
        *WAIT_SNAPSHOT_FIELDS,
        "first_wait_ms",
        "due_ms",
        "deferrals",
        "notice_pending",
        "expiry_notice_pending",
        "ever_granted",
    }
)


class ProtectedCapacityWaitStore(CapacityWaitStore):
    """Bounded scheduling with original proof and permanent-settlement fences.

    Presence of any permanent record holds scheduling; it does not manufacture
    terminal completion or refund. No-start terminal copies remain diagnostic.
    """

    def __init__(self, client: AuthenticatedWorkerClient, manifest: Manifest, config: WorkerConfig):
        ProtectedRecoveryAdapter(client, manifest, config)
        self._client, self._manifest, self._namespace = (
            client,
            manifest,
            derive_lane_namespace(manifest),
        )
        super().__init__(client.lane().runs, config)

    async def _scheduling_inputs(self, event_id: str) -> tuple[list[str], list[str]] | None:
        stored = await self._redis.hgetall(self._record(event_id))
        raw = cast(dict[str, str], stored)
        if (
            not raw
            or not set(raw) <= _WAIT_HASH_FIELDS
            or any(type(k) is not str or type(v) is not str for k, v in raw.items())
            or sum(len(v.encode("utf-8")) for v in raw.values()) > 1048576
            or raw.get("state") not in {"waiting", "woken", "active"}
        ):
            return None
        generation = raw.get("generation", "")
        if (
            not re.fullmatch(r"[1-9][0-9]{0,18}", generation)
            or int(generation) >= 9223372036854775807
        ):
            return None
        source = json.loads(raw.get("fields", ""), object_pairs_hook=_pairs)
        if (
            type(source) is not dict
            or not {"payload", "protected_envelope"} <= source.keys()
            or not source.keys()
            <= {
                "payload",
                "protected_envelope",
                WAIT_GENERATION_FIELD,
                WAIT_REPLY_REF_FIELD,
                WAIT_TERMINAL_ONLY_FIELD,
                TRACEPARENT_STREAM_FIELD,
            }
            or any(type(v) is not str for v in source.values())
        ):
            return None
        proof = await restore_committed_event(
            self._client, event_id, trusted_manifest=self._manifest
        )
        if (
            proof is None
            or source["protected_envelope"].encode() != proof.envelope.canonical_bytes
            or hashlib.sha256(source["payload"].encode()).hexdigest()
            != proof.envelope.as_dict()["payload_sha256"]
        ):
            return None
        observation = await self._client.observe()
        keys = [self._namespace.prefix + ":settlement:" + event_id, self._config.stream]
        keys.extend(key for key, _ in proof.proof_snapshots)
        snapshots = [
            [key, value.decode("utf-8") if value is not None else None]
            for key, value in proof.proof_snapshots
        ]
        args = [
            observation.run_id,
            json.dumps(sorted(raw.items()), separators=(",", ":")),
            json.dumps(snapshots, separators=(",", ":")),
        ]
        return keys, args

    async def _schedule(self, *, reconcile: bool, limit: int, wall_budget_s: float) -> int:
        if (
            type(limit) is not int
            or not 1 <= limit <= 64
            or not math.isfinite(wall_budget_s)
            or not 0 < wall_budget_s <= 30
        ):
            _refuse()
        appended = 0
        try:
            async with asyncio.timeout(wall_budget_s):
                if reconcile:
                    observation = await self._client.observe()
                    ids = await self._redis.zrangebyscore(
                        self._flight,
                        "-inf",
                        observation.now_ms,
                        start=0,
                        num=limit,
                    )
                else:
                    ids = await self._redis.zrangebyscore(
                        self._due, "-inf", "+inf", start=0, num=limit
                    )
                for event_id in ids:
                    try:
                        event_id = _text(event_id)
                        gate = await self._scheduling_inputs(event_id)
                        if gate is None:
                            continue
                        extra_keys, extra_args = gate
                        carrier: dict[str, str] = {}
                        inject_trace_context(carrier)
                        trace_field = (
                            TRACEPARENT_STREAM_FIELD if TRACEPARENT_STREAM_FIELD in carrier else ""
                        )
                        trace_value = carrier.get(TRACEPARENT_STREAM_FIELD, "")
                        if reconcile:
                            keys = [
                                self._record(event_id),
                                self._flight,
                                self._config.done_key(event_id),
                                self._config.completion_key(event_id),
                                self._active,
                            ]
                            args = [
                                event_id,
                                self._config.stream,
                                self._config.consumer_group,
                                WAIT_GENERATION_FIELD,
                                WAIT_TERMINAL_ONLY_FIELD,
                                WAIT_REPLY_REF_FIELD,
                                trace_field,
                                trace_value,
                            ]
                            script = _PROTECTED_RECONCILE
                        else:
                            keys = [
                                self._record(event_id),
                                self._due,
                                self._waiting,
                                self._notices,
                                self._notice_lock(event_id),
                                self._config.done_key(event_id),
                                self._config.completion_key(event_id),
                                self._flight,
                            ]
                            args = [
                                self._config.stream,
                                event_id,
                                WAIT_GENERATION_FIELD,
                                WAIT_REPLY_REF_FIELD,
                                trace_field,
                                trace_value,
                            ]
                            script = _PROTECTED_WAKE
                        keys.extend(extra_keys)
                        args.extend(extra_args)
                        result = await self._redis.eval(script, len(keys), *keys, *args)
                        if int(result[0]) == 1:
                            appended += 1
                    except asyncio.CancelledError:
                        raise
                    except Exception:  # noqa: BLE001  Retain unknown/malformed/outage wait proof.
                        continue
        except TimeoutError:
            pass
        return appended

    async def wake_due(self, limit: int = 64, *, wall_budget_s: float = 5) -> int:
        return await self._schedule(reconcile=False, limit=limit, wall_budget_s=wall_budget_s)

    async def reconcile_lost_wakes(self, limit: int = 64, *, wall_budget_s: float = 5) -> int:
        return await self._schedule(reconcile=True, limit=limit, wall_budget_s=wall_budget_s)
