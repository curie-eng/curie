"""Actual protected delivery ownership fence, @spec PROTECTED-HOOK-LANE-6/7.

The context carries an already acquired real lease. It issues no qualification,
delivery or start authority: every positive call also verifies its original
delivery and artifact, and this check re-reads the live broker ownership fence.
"""

from __future__ import annotations

import asyncio
import hashlib
import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from typing import NoReturn

from curie_protected_hooks.admission_records import _encode
from curie_protected_hooks.authority_records import Manifest
from curie_protected_hooks.worker_transport import AuthenticatedWorkerClient
from curie_protected_hooks.worker_verification import Dispatchable

from .config import WorkerConfig
from .delivery_lease import DeliveryLease
from .protected_binding import _CURRENT_DELIVERY
from .protected_recovery import (
    ValidatedRecoveryCopy,
    current_protected_recovery,
    recovery_gate_inputs,
)


class ExecutionOwnershipUnavailable(RuntimeError):
    """Fixed refusal; ownership tokens never enter diagnostics."""

    def __init__(self) -> None:
        super().__init__("Protected execution ownership unavailable")


def _fail() -> NoReturn:
    try:
        raise ExecutionOwnershipUnavailable() from None
    except ExecutionOwnershipUnavailable as refusal:
        refusal.__cause__ = None
        refusal.__context__ = None
        raise


@dataclass(frozen=True, slots=True, repr=False)
class _ExecutionScope:
    client: AuthenticatedWorkerClient
    manifest: Manifest
    delivery: Dispatchable
    lease: DeliveryLease
    prefix: str
    consumer: str
    stream: str
    group: str
    entry_id: str
    owner: str
    generation: int
    deadline_ms: int
    source_payload: str
    source_envelope: str
    recovery: ValidatedRecoveryCopy | None = None
    revoked: asyncio.Event = field(default_factory=asyncio.Event)


_CURRENT: ContextVar[_ExecutionScope | None] = ContextVar("protected_execution_owner", default=None)

_CHECK_OWNERSHIP = """
local recovery = #KEYS == 5 and #ARGV == 12
if not recovery and (#KEYS ~= 4 or #ARGV ~= 10) then return 0 end
local info = redis.call('INFO', 'server')
if string.match(info, 'run_id:([0-9a-f]+)') ~= ARGV[7] then return 0 end
if redis.call('EXISTS', KEYS[4]) ~= 0 then return 0 end
if redis.call('GET', KEYS[1]) ~= ARGV[1] then return 0 end
if redis.call('PTTL', KEYS[1]) <= 0 then return 0 end
if redis.call('HGET', KEYS[2], 'gen') ~= ARGV[2] then return 0 end
if redis.call('HGET', KEYS[2], 'deadline_ms') ~= ARGV[6] then return 0 end
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
if not tonumber(ARGV[6]) or now >= tonumber(ARGV[6]) then return 0 end
local pending = redis.call('XPENDING', KEYS[3], ARGV[3], ARGV[4], ARGV[4], 1)
if #pending ~= 1 or pending[1][2] ~= ARGV[5] then return 0 end
-- Bind this immutable current row once while verifying its actual lease/PEL.
-- After binding, stream trimming does not erase accepted delivery authority.
if recovery or ARGV[10] == '1' then
  local rows = redis.call('XRANGE', KEYS[3], ARGV[4], ARGV[4], 'COUNT', 1)
  if #rows ~= 1 then return 0 end
  local expected = nil
  if recovery then
    if redis.call('TYPE', KEYS[5]).ok ~= 'hash' then return 0 end
    local ok, pairs = pcall(cjson.decode, ARGV[11])
    if not ok or type(pairs) ~= 'table' or #pairs < 3 or #pairs > 6 then return 0 end
    expected = {}
    for _, pair in ipairs(pairs) do
      if type(pair) ~= 'table' or #pair ~= 2 or type(pair[1]) ~= 'string'
         or type(pair[2]) ~= 'string' or expected[pair[1]] ~= nil then return 0 end
      expected[pair[1]] = pair[2]
    end
    if #rows[1][2] ~= #pairs * 2 then return 0 end
    local names = {'fields','state','generation','wake_entry','reply_ref','terminal_only',
                   'cause','deadline_ms','grant_epoch','grant_confirmed','grant_unknown'}
    local snapshot_ok, snapshot = pcall(cjson.decode, ARGV[12])
    if not snapshot_ok or type(snapshot) ~= 'table' or #snapshot ~= #names then return 0 end
    local wait = {}
    for index, name in ipairs(names) do
      local pair = snapshot[index]
      if type(pair) ~= 'table' or #pair ~= 2 or pair[1] ~= name then return 0 end
      local wanted = pair[2]
      if wanted ~= cjson.null and type(wanted) ~= 'string' then return 0 end
      local actual = redis.call('HGET', KEYS[5], name)
      if wanted == cjson.null then
        if actual then return 0 end
      elseif actual ~= wanted then return 0 end
      wait[name] = actual
    end
    if wait['wake_entry'] ~= ARGV[4] or wait['terminal_only'] == '1'
       or wait['grant_unknown'] == '1'
       or (wait['state'] ~= 'woken' and wait['state'] ~= 'active')
       or not tonumber(wait['deadline_ms']) or now >= tonumber(wait['deadline_ms'])
       then return 0 end
  end
  local payload, envelope = nil, nil
  for i = 1, #rows[1][2], 2 do
    local key, value = rows[1][2][i], rows[1][2][i+1]
    if key == 'payload' then
      if payload then return 0 end
      payload = value
    elseif key == 'protected_envelope' then
      if envelope then return 0 end
      envelope = value
    else
      if not expected or (key ~= 'curie_capacity_wait_generation'
         and key ~= 'curie_capacity_wait_reply_ref' and key ~= 'curie_capacity_wait_terminal_only'
         and key ~= 'traceparent') then return 0 end
    end
    if expected and expected[key] ~= value then return 0 end
    if expected then expected[key] = nil end
  end
  if payload ~= ARGV[8] or envelope ~= ARGV[9] then return 0 end
  if expected and next(expected) ~= nil then return 0 end
end
return 1
"""


def _locally_valid(
    scope: _ExecutionScope, client: AuthenticatedWorkerClient, manifest: Manifest
) -> bool:
    return (
        client is scope.client
        and type(manifest) is Manifest
        and manifest.canonical_bytes == scope.manifest.canonical_bytes
        and _CURRENT_DELIVERY.get() is scope.delivery
        and current_protected_recovery() is scope.recovery
        and (scope.recovery is None or scope.recovery.delivery is scope.delivery)
        and not scope.revoked.is_set()
        and not scope.lease.lost.is_set()
        and not scope.lease.acknowledged.is_set()
        and not scope.lease.entry_vanished.is_set()
        and scope.lease.local_deadline_monotonic > time.monotonic()
        and scope.lease.remaining_s() > 0
        and (
            scope.lease.stream,
            scope.lease.group,
            scope.lease.entry_id,
            scope.lease.owner,
            scope.lease.generation,
            scope.lease.budget.deadline_ms,
        )
        == (
            scope.stream,
            scope.group,
            scope.entry_id,
            scope.owner,
            scope.generation,
            scope.deadline_ms,
        )
    )


async def _check(
    scope: _ExecutionScope,
    client: AuthenticatedWorkerClient,
    manifest: Manifest,
    *,
    bind_entry: bool = False,
) -> None:
    try:
        if not _locally_valid(scope, client, manifest):
            _fail()
        async with asyncio.timeout(min(2.0, scope.lease.remaining_s())):
            gate = await recovery_gate_inputs(client, manifest, scope.lease)
            if (gate is None) != (scope.recovery is None):
                _fail()
            keys = [
                f"{scope.prefix}:lease:{scope.stream}:{scope.group}:{scope.entry_id}",
                f"{scope.prefix}:delivery:{scope.stream}:{scope.group}:{scope.entry_id}",
                scope.stream,
                f"{scope.prefix}:settlement:{scope.delivery.turn.event_id}",
            ]
            extra: list[str] = []
            if gate is not None:
                keys.append(gate.wait_key)
                extra = [_encode(gate.entry_fields).decode(), _encode(gate.wait_snapshot).decode()]
            result = await client.lane().runs.eval(
                _CHECK_OWNERSHIP,
                len(keys),
                *keys,
                scope.owner,
                str(scope.generation),
                scope.group,
                scope.entry_id,
                scope.consumer,
                str(scope.deadline_ms),
                manifest.as_dict()["broker_identity"]["run_id"],
                scope.source_payload,
                scope.source_envelope,
                "1" if bind_entry else "0",
                *extra,
            )
        if type(result) is not int or result != 1:
            _fail()
        if not _locally_valid(scope, client, manifest):
            _fail()
    except asyncio.CancelledError:
        raise
    except Exception:  # noqa: BLE001  Broker/lease diagnostics contain private tokens.
        _fail()


@asynccontextmanager
async def protected_execution_scope(
    client: AuthenticatedWorkerClient,
    *,
    trusted_manifest: Manifest,
    config: WorkerConfig,
    lease: DeliveryLease,
) -> AsyncIterator[None]:
    """Enter only with the real acquired handler lease and verified delivery."""
    if (
        type(client) is not AuthenticatedWorkerClient
        or type(trusted_manifest) is not Manifest
        or type(config) is not WorkerConfig
        or type(lease) is not DeliveryLease
    ):
        _fail()
    delivery = _CURRENT_DELIVERY.get()
    if type(delivery) is not Dispatchable:
        _fail()
    sources = delivery.source_fields
    if (
        type(sources) is not tuple
        or len(sources) != 2
        or any(type(pair) is not tuple or len(pair) != 2 for pair in sources)
        or any(type(value) is not str for pair in sources for value in pair)
        or {pair[0] for pair in sources} != {"payload", "protected_envelope"}
    ):
        _fail()
    originals = dict(sources)
    try:
        if not (
            0 < len(originals["payload"].encode("utf-8", errors="strict")) <= 262144
            and 0 < len(originals["protected_envelope"].encode("utf-8", errors="strict")) <= 16384
        ):
            _fail()
    except UnicodeError:
        _fail()
    m = trusted_manifest.as_dict()
    generation_key = hashlib.sha256(_encode([m["runtime_id"], m["runtime_generation"]])).hexdigest()
    prefix, group = f"protected:lane:worker:{generation_key}", f"protected-{generation_key}"
    if (
        config.key_prefix != prefix
        or config.consumer_group != group
        or config.stream != "curie:runs"
        or (lease.stream, lease.group) != (config.stream, config.consumer_group)
        or not lease.entry_id
        or not lease.owner
        or type(lease.generation) is not int
        or lease.generation < 1
        or not config.consumer_name
        or delivery.envelope.as_dict()["manifest_digest"] != trusted_manifest.digest
    ):
        _fail()
    scope = _ExecutionScope(
        client,
        trusted_manifest,
        delivery,
        lease,
        prefix,
        config.consumer_name,
        lease.stream,
        lease.group,
        lease.entry_id,
        lease.owner,
        lease.generation,
        lease.budget.deadline_ms,
        originals["payload"],
        originals["protected_envelope"],
        current_protected_recovery(),
    )
    await _check(scope, client, trusted_manifest, bind_entry=True)
    token = _CURRENT.set(scope)
    try:
        yield
    finally:
        scope.revoked.set()
        _CURRENT.reset(token)


async def require_current_execution(
    client: AuthenticatedWorkerClient, trusted_manifest: Manifest
) -> None:
    """Recheck actual PEL/lease/generation/budget/epoch immediately before a positive RPC."""
    scope = _CURRENT.get()
    if scope is None:
        _fail()
    await _check(scope, client, trusted_manifest)
