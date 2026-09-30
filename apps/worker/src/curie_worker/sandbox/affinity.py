"""The ``thread_ts -> sandbox_id`` affinity store, on Valkey.

One key per thread route, atomic claim-or-lose semantics so two workers racing
the same first message converge on a single sandbox, and TTLs so an abandoned
thread's route expires (the substrate's reaper then deletes the orphaned
claim). Valkey is never mocked in tests (repo test discipline); the store runs
against the compose-stack instance.
"""

from __future__ import annotations

import time

import redis
from redis.asyncio import Redis as AsyncRedis

from .types import PressureCandidate, PressureScanResult, RouteRecord, RouteState

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


class AffinityStore:
    """Thread-to-sandbox route records with atomic acquire and guarded delete."""

    def __init__(
        self,
        client: redis.Redis,
        *,
        pressure_client: AsyncRedis,
        key_prefix: str = "curie:sandbox",
    ) -> None:
        self._redis = client
        self._pressure_redis = pressure_client
        self._prefix = key_prefix
        self._delete_if_claim = client.register_script(_DELETE_IF_CLAIM)
        self._replace_if_generation = client.register_script(_REPLACE_IF_GENERATION)
        self._touch_if_live_claim = client.register_script(_TOUCH_IF_LIVE_CLAIM)

    def _key(self, thread_key: str) -> str:
        return f"{self._prefix}:route:{thread_key}"

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

        result = self._redis.set(
            self._key(thread_key), record.to_json(), nx=True, ex=ttl_seconds
        )
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

    def touch_if_live_claim(
        self, thread_key: str, claim_name: str, ttl_seconds: int
    ) -> bool:
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
                    key.decode("utf-8") if isinstance(key, bytes) else str(key)
                    for key in keys
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

    def route_inventory(
        self, thread_keys_scan_count: int = 500
    ) -> dict[RouteState, set[str]]:
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
