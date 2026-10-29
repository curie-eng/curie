"""The ``thread_ts -> sandbox_id`` affinity store, on Valkey.

One key per thread route, atomic claim-or-lose semantics so two workers racing
the same first message converge on a single sandbox, and TTLs so an abandoned
thread's route expires (the substrate's reaper then deletes the orphaned
claim). Valkey is never mocked in tests (repo test discipline); the store runs
against the compose-stack instance.
"""

from __future__ import annotations

import json
import time
from collections.abc import Mapping
from dataclasses import asdict
from decimal import Decimal
from typing import Literal

import redis
from curie_internal.keyspace import SANDBOX_KEY_PREFIX_DEFAULT
from redis.asyncio import Redis as AsyncRedis

from .quota import resource_vector
from .types import (
    EXECUTOR_THREAD_KEY_PREFIX,
    AdmissionReservation,
    CapacityInventory,
    PressureCandidate,
    PressureScanResult,
    RouteRecord,
    RouteState,
)

# SCAN COUNT is an approximate database work hint, not a result limit. The
# caller permits eight pages, roughly 65,000 examined database keys, while its
# separate 256 record cap counts matching route keys before safety filtering.
_PRESSURE_SCAN_COUNT_HINT = 8192

# Delete the route only if it still points at the claim the caller owns; a
# racing re-claim must not have its fresh route deleted by a stale releaser.
_DELETE_IF_CLAIM = """
local raw = redis.call('GET', KEYS[1])
if not raw then return 0 end
local ok, record = pcall(cjson.decode, raw)
if not ok then return 0 end
if record['claim_name'] == ARGV[1] then
    redis.call('DEL', KEYS[1])
    return 1
end
return 0
"""

# Refresh the TTL only while the route is still LIVE on the caller's claim, so
# a suspend or handoff that lands mid-turn keeps its own TTL (#3188).
_TOUCH_IF_LIVE_CLAIM = """
local raw = redis.call('GET', KEYS[1])
if not raw then return 0 end
local ok, record = pcall(cjson.decode, raw)
if not ok then return 0 end
local state = record['state'] or ARGV[2]
if record['claim_name'] ~= ARGV[1] or state ~= ARGV[2] then return 0 end
return redis.call('EXPIRE', KEYS[1], ARGV[3])
"""

_REPLACE_IF_GENERATION = """
local raw = redis.call('GET', KEYS[1])
if not raw then return 0 end
local ok, current = pcall(cjson.decode, raw)
if not ok then return 0 end
local generation = current['generation'] or 0
if current['claim_name'] ~= ARGV[1] or generation ~= tonumber(ARGV[2]) then
    return 0
end
redis.call('SET', KEYS[1], ARGV[3], 'EX', ARGV[4])
return 1
"""

_DETACH_IF_UNCHANGED = """
if redis.call('GET', KEYS[2]) ~= ARGV[4] then return 0 end
local raw = redis.call('GET', KEYS[1])
if not raw then return 0 end
local ok, current = pcall(cjson.decode, raw)
if not ok then return 0 end
local generation = current['generation'] or 0
if current['claim_name'] ~= ARGV[1] or generation ~= tonumber(ARGV[2]) then
    return 0
end
if redis.call('PEXPIRETIME', KEYS[1]) ~= tonumber(ARGV[3]) then return 0 end
redis.call('DEL', KEYS[1])
return 1
"""

_ADMISSION_RESERVE = """
local function digits(value, scale)
    local whole, fraction = string.match(value, '^(%d+)%.?(%d*)$')
    if not whole then error('invalid admission quantity') end
    return whole .. fraction .. string.rep('0', scale - #fraction)
end
local function sum(a, b)
    local af = string.match(a, '%.(%d+)$') or ''
    local bf = string.match(b, '%.(%d+)$') or ''
    local scale = math.max(#af, #bf)
    a, b = digits(a, scale), digits(b, scale)
    local width = math.max(#a, #b)
    a, b = string.rep('0', width - #a) .. a, string.rep('0', width - #b) .. b
    local carry, result = 0, ''
    for i = width, 1, -1 do
        local n = tonumber(string.sub(a, i, i)) + tonumber(string.sub(b, i, i)) + carry
        result, carry = tostring(n % 10) .. result, math.floor(n / 10)
    end
    result = tostring(carry) .. result
    if scale > 0 then
        result = string.sub(result, 1, #result - scale) .. '.' .. string.sub(result, -scale)
    end
    result = string.gsub(result, '^0+(%d)', '%1')
    return result
end
local function greater(a, b)
    local af = string.match(a, '%.(%d+)$') or ''
    local bf = string.match(b, '%.(%d+)$') or ''
    local scale = math.max(#af, #bf)
    a, b = digits(a, scale), digits(b, scale)
    a, b = string.gsub(a, '^0+', ''), string.gsub(b, '^0+', '')
    return #a > #b or (#a == #b and a > b)
end
local occupied, hard = cjson.decode(ARGV[1]), cjson.decode(ARGV[2])
local suspended, observed_at = cjson.decode(ARGV[5]), tonumber(ARGV[6])
local rows = redis.call('HGETALL', KEYS[1])
for i = 1, #rows, 2 do
    local name, row = rows[i], cjson.decode(rows[i + 1])
    local current = occupied[name] or {}
    if not (suspended[name] and observed_at > row.updated_at and row.state == 'bound') then
        for key, value in pairs(row.resources) do
            if greater(value, current[key] or '0') then current[key] = value end
        end
    end
    occupied[name] = current
end
local used = {}
for key, _ in pairs(hard) do used[key] = '0' end
for _, resources in pairs(occupied) do
    for key, _ in pairs(hard) do used[key] = sum(used[key], resources[key] or '0') end
end
local row = cjson.decode(ARGV[4])
local accepted = redis.call('HEXISTS', KEYS[1], ARGV[3]) == 0
for key, limit in pairs(hard) do
    if greater(sum(used[key], row.resources[key] or '0'), limit) then accepted = false end
end
if accepted and ARGV[7] == 'reserve' then redis.call('HSET', KEYS[1], ARGV[3], ARGV[4]) end
return cjson.encode({accepted=accepted, used=used})
"""

_ADMISSION_UPDATE = """
local raw = redis.call('HGET', KEYS[1], ARGV[1])
if not raw then return 0 end
local row, expected = cjson.decode(raw), cjson.decode(ARGV[2])
if row.owner_token ~= expected.owner_token or row.state ~= expected.state
    or row.updated_at ~= expected.updated_at then return 0 end
if ARGV[3] == '' then redis.call('HDEL', KEYS[1], ARGV[1])
else redis.call('HSET', KEYS[1], ARGV[1], ARGV[3]) end
return 1
"""


class AffinityStore:
    """Thread-to-sandbox route records with atomic acquire and guarded delete."""

    def __init__(
        self,
        client: redis.Redis,
        *,
        pressure_client: AsyncRedis,
        key_prefix: str = SANDBOX_KEY_PREFIX_DEFAULT,
    ) -> None:
        self._redis = client
        self._pressure_redis = pressure_client
        self._prefix = key_prefix
        self._delete_if_claim = client.register_script(_DELETE_IF_CLAIM)
        self._replace_if_generation = client.register_script(_REPLACE_IF_GENERATION)
        self._touch_if_live_claim = client.register_script(_TOUCH_IF_LIVE_CLAIM)
        self._admission_reserve = client.register_script(_ADMISSION_RESERVE)
        self._admission_update = client.register_script(_ADMISSION_UPDATE)

    def _admission_key(self, namespace: str, quota_name: str) -> str:
        return f"{self._prefix}:admission:{namespace}:{quota_name}"

    def admission_entries(self, namespace: str, quota_name: str) -> dict[str, AdmissionReservation]:
        rows = self._redis.hgetall(self._admission_key(namespace, quota_name))
        result = {}
        for name, raw in rows.items():
            row = AdmissionReservation(**json.loads(raw))
            if row.state not in {"preparing", "submitted", "bound", "retiring"}:
                raise ValueError("invalid sandbox admission state")
            resource_vector(row.resources)
            result[name.decode() if isinstance(name, bytes) else str(name)] = row
        return result

    def admission_update(
        self,
        namespace: str,
        quota_name: str,
        name: str,
        expected: AdmissionReservation,
        replacement: AdmissionReservation | None,
    ) -> bool:
        return bool(self._admission_update(
            keys=[self._admission_key(namespace, quota_name)],
            args=[
                name, json.dumps(asdict(expected)),
                "" if replacement is None else json.dumps(asdict(replacement)),
            ],
        ))

    def reserve_admission(
        self,
        namespace: str,
        quota_name: str,
        name: str,
        reservation: AdmissionReservation,
        hard: Mapping[str, str],
        inventory: CapacityInventory,
        *,
        operation: Literal["reserve", "inspect"],
    ) -> tuple[bool, dict[str, str]]:
        """Charge observed occupants and every outstanding reservation atomically."""

        occupied: dict[str, dict[str, str]] = {}
        owners_with_pods = {pod.claim_name for pod in inventory.pods.values()}
        suspended = {
            claim: True for claim in inventory.suspended_claims if claim not in owners_with_pods
        }
        identities = inventory.claims | frozenset(
            owner if owner is not None else f"sandbox:{name}"
            for name, owner in inventory.sandboxes.items()
        )
        for identity in identities:
            if identity in suspended:
                continue
            footprint = inventory.claim_resources.get(identity)
            if footprint is not None:
                occupied[identity] = resource_vector(footprint)
        pod_totals: dict[str, dict[str, Decimal]] = {}
        for uid, pod in inventory.pods.items():
            identity = pod.claim_name or f"pod:{uid}"
            totals = pod_totals.setdefault(identity, {})
            for key, value in resource_vector(pod.resources).items():
                totals[key] = totals.get(key, Decimal(0)) + Decimal(value)
        for identity, totals in pod_totals.items():
            current = occupied.setdefault(identity, {})
            for key, amount in totals.items():
                current[key] = format(max(amount, Decimal(current.get(key, "0"))), "f")
        known = self.admission_entries(namespace, quota_name)
        if any(
            identity not in occupied and identity not in suspended and identity not in known
            for identity in identities
        ):
            raise ValueError("sandbox admission inventory lacks an occupied footprint")
        raw = self._admission_reserve(
            keys=[self._admission_key(namespace, quota_name)],
            args=[json.dumps(occupied), json.dumps(resource_vector(hard)), name,
                  json.dumps(asdict(reservation)), json.dumps(suspended), inventory.observed_at,
                  operation],
        )
        result = json.loads(raw)
        return bool(result["accepted"]), result["used"]

    def _key(self, thread_key: str) -> str:
        return f"{self._prefix}:route:{thread_key}"

    def _claim_cred_key(self, claim_name: str) -> str:
        return f"{self._prefix}:claim-cred:{claim_name}"

    def _claim_cred_retiring_key(self, claim_name: str) -> str:
        return f"{self._prefix}:claim-cred-retiring:{claim_name}"

    def mark_claim_credential_retiring(self, claim_name: str, *, ttl_s: int) -> None:
        self._redis.set(self._claim_cred_retiring_key(claim_name), "1", nx=True, ex=ttl_s)

    def claim_credential_retiring(self, claim_name: str) -> bool:
        return bool(self._redis.exists(self._claim_cred_retiring_key(claim_name)))

    def remember_claim_credential(
        self, claim_name: str, agent: str, credential: str, *, ttl_s: int
    ) -> None:
        """Keep a deleted claim's boot credential until the API accepts the report."""

        self._redis.set(
            self._claim_cred_key(claim_name),
            f"{agent}\n{credential}",
            ex=ttl_s,
        )

    def claim_credential(self, claim_name: str) -> tuple[str, str] | None:
        raw = self._redis.get(self._claim_cred_key(claim_name))
        if raw is None:
            return None
        text = raw.decode() if isinstance(raw, bytes) else str(raw)
        agent, separator, credential = text.partition("\n")
        if not separator or not agent or not credential:
            return None
        return agent, credential

    def forget_claim_credential(self, claim_name: str) -> None:
        self._redis.delete(
            self._claim_cred_key(claim_name), self._claim_cred_retiring_key(claim_name)
        )

    def iter_claim_credentials(self) -> list[tuple[str, str, str]]:
        """``(claim_name, agent, credential)`` for reports that have not landed."""

        found: list[tuple[str, str, str]] = []
        prefix = f"{self._prefix}:claim-cred:"
        for key in self._redis.scan_iter(match=f"{prefix}*"):
            name = key.decode() if isinstance(key, bytes) else str(key)
            claim_name = name.removeprefix(prefix)
            pair = self.claim_credential(claim_name)
            if pair is not None:
                found.append((claim_name, pair[0], pair[1]))
        return found

    def get(self, thread_key: str) -> RouteRecord | None:
        raw = self._redis.get(self._key(thread_key))
        if raw is None:
            return None
        text = raw.decode("utf-8") if isinstance(raw, bytes) else str(raw)
        return RouteRecord.from_json(text)

    def put_if_absent(self, thread_key: str, record: RouteRecord, ttl_seconds: int) -> bool:
        """Record the route unless one exists. Returns False when the caller
        lost the race (an existing route wins; the caller should adopt it and
        release its own claim)."""

        result = self._redis.set(self._key(thread_key), record.to_json(), nx=True, ex=ttl_seconds)
        return bool(result)

    def replace(self, thread_key: str, record: RouteRecord, ttl_seconds: int) -> None:
        """Overwrite the route unconditionally (suspend/resume transitions)."""

        self._redis.set(self._key(thread_key), record.to_json(), ex=ttl_seconds)

    def replace_if_generation(
        self,
        thread_key: str,
        *,
        expected_claim: str,
        expected_generation: int,
        record: RouteRecord,
        ttl_seconds: int,
    ) -> bool:
        """CAS a route only while both the old claim and generation still match."""

        return bool(
            self._replace_if_generation(
                keys=[self._key(thread_key)],
                args=[
                    expected_claim,
                    expected_generation,
                    record.to_json(),
                    ttl_seconds,
                ],
            )
        )

    def touch(self, thread_key: str, ttl_seconds: int) -> bool:
        """Refresh the route TTL on activity. Returns False if no route."""

        return bool(self._redis.expire(self._key(thread_key), ttl_seconds))

    def touch_if_live_claim(self, thread_key: str, claim_name: str, ttl_seconds: int) -> bool:
        """Refresh the TTL only of a LIVE route that still names ``claim_name``."""

        return bool(
            self._touch_if_live_claim(
                keys=[self._key(thread_key)],
                args=[claim_name, RouteState.LIVE.value, ttl_seconds],
            )
        )

    def delete_if_claim(self, thread_key: str, claim_name: str) -> bool:
        """Delete the route only when it still names ``claim_name``."""

        return bool(self._delete_if_claim(keys=[self._key(thread_key)], args=[claim_name]))

    async def pressure_get(self, thread_key: str) -> RouteRecord | None:
        """Read a route on the independently bounded pressure connection."""

        raw = await self._pressure_redis.get(self._key(thread_key))
        if raw is None:
            return None
        text = raw.decode("utf-8") if isinstance(raw, bytes) else str(raw)
        return RouteRecord.from_json(text)

    async def pressure_candidates(
        self, *, max_pages: int, max_records: int, deadline: float
    ) -> PressureScanResult:
        """Return an expiry ordered inventory only after a bounded full scan."""

        cursor = 0
        pages = 0
        records = 0
        candidates: list[PressureCandidate] = []
        match = f"{self._prefix}:route:*"
        route_prefix = f"{self._prefix}:route:"
        while True:
            if pages >= max_pages or records >= max_records or time.monotonic() >= deadline:
                return PressureScanResult((), "scan-incomplete")
            cursor, keys = await self._pressure_redis.scan(
                cursor=cursor,
                match=match,
                count=_PRESSURE_SCAN_COUNT_HINT,
            )
            pages += 1
            if records + len(keys) > max_records:
                return PressureScanResult((), "scan-incomplete")
            records += len(keys)
            if keys:
                pipeline = self._pressure_redis.pipeline(transaction=False)
                key_texts = [
                    key.decode("utf-8") if isinstance(key, bytes) else str(key) for key in keys
                ]
                for key_text in key_texts:
                    pipeline.get(key_text)
                    pipeline.pexpiretime(key_text)
                try:
                    values = await pipeline.execute()
                except redis.ResponseError as exc:
                    detail = str(exc).lower()
                    if "unknown command" in detail and "pexpiretime" in detail:
                        return PressureScanResult((), "expiry-unsupported")
                    raise
                for index, key_text in enumerate(key_texts):
                    raw = values[index * 2]
                    expires_at = values[index * 2 + 1]
                    if raw is None or not isinstance(expires_at, int) or expires_at <= 0:
                        continue
                    text = raw.decode("utf-8") if isinstance(raw, bytes) else str(raw)
                    if not key_text.startswith(route_prefix):
                        continue
                    try:
                        record = RouteRecord.from_json(text)
                    except (ValueError, TypeError, KeyError):
                        continue
                    if record.state is not RouteState.LIVE:
                        continue
                    if (
                        record.handle.workspace_repo is not None
                        or record.handle.workspace_materialized_head is not None
                        or record.handle.publication_visible_outcome_revision != 0
                    ):
                        continue
                    candidates.append(
                        PressureCandidate(
                            thread_key=key_text[len(route_prefix) :],
                            record=record,
                            expires_at_ms=expires_at,
                        )
                    )
            if cursor == 0:
                break
        candidates.sort(key=lambda item: (item.expires_at_ms, item.thread_key))
        return PressureScanResult(tuple(candidates), "complete")

    async def detach_if_unchanged(
        self,
        thread_key: str,
        *,
        expected_claim: str,
        expected_generation: int,
        expected_expires_at_ms: int,
        lock_key: str,
        lock_token: str,
    ) -> bool:
        """Atomically detach an exact idle route while its victim lock is owned."""

        return bool(
            await self._pressure_redis.eval(
                _DETACH_IF_UNCHANGED,
                2,
                self._key(thread_key),
                lock_key,
                expected_claim,
                expected_generation,
                expected_expires_at_ms,
                lock_token,
            )
        )

    def live_claim_names(self, thread_keys_scan_count: int = 500) -> set[str]:
        """All claim names currently referenced by any unexpired route.

        Used by the reaper: a cluster-side claim whose name is not in this set
        has no live route and is an orphan.
        """

        inventory = self.route_inventory(thread_keys_scan_count)
        return set().union(*inventory.values())

    def executor_routes(self, scan_count: int = 500) -> dict[str, RouteRecord]:
        """Every unexpired executor route (``action-exec:<id>``) by thread key.

        @spec AUTOMATED-REMEDIATION-12: what a live executor loop sweeps after a
        holder crashed. Turn routes are never matched.
        """

        route_prefix = f"{self._prefix}:route:"
        routes: dict[str, RouteRecord] = {}
        for key in self._redis.scan_iter(
            match=f"{route_prefix}{EXECUTOR_THREAD_KEY_PREFIX}*", count=scan_count
        ):
            key_text = key.decode("utf-8") if isinstance(key, bytes) else str(key)
            raw = self._redis.get(key)
            if raw is None:
                continue
            text = raw.decode("utf-8") if isinstance(raw, bytes) else str(raw)
            try:
                record = RouteRecord.from_json(text)
            except (ValueError, TypeError, KeyError):
                continue
            routes[key_text[len(route_prefix) :]] = record
        return routes

    def route_inventory(self, thread_keys_scan_count: int = 500) -> dict[RouteState, set[str]]:
        """Authoritative unexpired route claims grouped by persisted state."""

        inventory: dict[RouteState, set[str]] = {state: set() for state in RouteState}
        for key in self._redis.scan_iter(
            match=f"{self._prefix}:route:*", count=thread_keys_scan_count
        ):
            raw = self._redis.get(key)
            if raw is None:
                continue
            text = raw.decode("utf-8") if isinstance(raw, bytes) else str(raw)
            try:
                record = RouteRecord.from_json(text)
            except (ValueError, TypeError, KeyError):
                continue
            inventory[record.state].add(record.handle.claim_name)
        return inventory

    def mark_suspended(
        self, thread_key: str, history_ref: str | None, ttl_seconds: int
    ) -> RouteRecord:
        """Transition the route to SUSPENDED, recording the history ref the
        resume path will inject as ``CURIE_HISTORY_REF``."""

        record = self.get(thread_key)
        if record is None:
            raise KeyError(thread_key)
        handle = record.handle
        updated = RouteRecord(
            handle=type(handle)(
                **{**handle.__dict__, "history_ref": history_ref},
            ),
            state=RouteState.SUSPENDED,
        )
        self.replace(thread_key, updated, ttl_seconds)
        return updated
