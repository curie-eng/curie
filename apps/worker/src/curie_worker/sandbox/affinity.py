"""The ``thread_ts -> sandbox_id`` affinity store, on Valkey.

One key per thread route, atomic claim-or-lose semantics so two workers racing
the same first message converge on a single sandbox, and TTLs so an abandoned
thread's route expires (the substrate's reaper then deletes the orphaned
claim). Valkey is never mocked in tests (repo test discipline); the store runs
against the compose-stack instance.
"""

from __future__ import annotations

import redis

from .types import RouteRecord, RouteState

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

# The ADR-0122 d3 applied transition, fenced INSIDE the store. A warm bind's
# route is written PENDING with the conversation credential before the first
# event; the flip to APPLIED must observe, in one atomic predicate, that the
# route is still LIVE (a concurrent ``mark_suspended`` loses), still names the
# caller's claim and generation (every replacing writer loses), still carries
# the same credential, and is still PENDING. A route already APPLIED for that
# credential answers 2 so a retry that lost its first response is idempotent;
# every other shape answers 0 and writes nothing. The claim + generation CAS
# above cannot express the state half, and the kernel's suspend runs after its
# per-thread lock is released, so no lock closes that window: only this
# predicate does.
_MARK_ADOPTION_APPLIED = """
local raw = redis.call('GET', KEYS[1])
if not raw then return 0 end
local ok, current = pcall(cjson.decode, raw)
if not ok then return 0 end
if current['state'] ~= 'live' then return 0 end
local generation = current['generation'] or 0
if current['claim_name'] ~= ARGV[1] or generation ~= tonumber(ARGV[2]) then
    return 0
end
if current['token'] ~= ARGV[3] then return 0 end
local adoption = current['adoption_state']
if adoption == 'applied' then return 2 end
if adoption ~= 'pending' then return 0 end
redis.call('SET', KEYS[1], ARGV[4], 'EX', ARGV[5])
return 1
"""

# Clear the adopting-event marker once the adopting turn's fate is known. Same
# fence as the applied transition (live, claim, generation) plus the marker
# itself, so a stale settler cannot clear a marker a later owner wrote.
_CLEAR_ADOPTING_EVENT = """
local raw = redis.call('GET', KEYS[1])
if not raw then return 0 end
local ok, current = pcall(cjson.decode, raw)
if not ok then return 0 end
if current['state'] ~= 'live' then return 0 end
local generation = current['generation'] or 0
if current['claim_name'] ~= ARGV[1] or generation ~= tonumber(ARGV[2]) then
    return 0
end
if current['adoption_state'] ~= 'applied' then return 0 end
if current['adopting_event_id'] ~= ARGV[3] then return 0 end
redis.call('SET', KEYS[1], ARGV[4], 'EX', ARGV[5])
return 1
"""


class AffinityStore:
    """Thread-to-sandbox route records with atomic acquire and guarded delete."""

    def __init__(self, client: redis.Redis, *, key_prefix: str = "curie:sandbox") -> None:
        self._redis = client
        self._prefix = key_prefix
        self._delete_if_claim = client.register_script(_DELETE_IF_CLAIM)
        self._replace_if_generation = client.register_script(_REPLACE_IF_GENERATION)
        self._mark_adoption_applied = client.register_script(_MARK_ADOPTION_APPLIED)
        self._clear_adopting_event = client.register_script(_CLEAR_ADOPTING_EVENT)

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

    def mark_adoption_applied(
        self,
        thread_key: str,
        *,
        expected_claim: str,
        expected_generation: int,
        expected_token: str,
        record: RouteRecord,
        ttl_seconds: int,
    ) -> int:
        """Flip a PENDING warm-bind route to APPLIED under the full predicate.

        Returns 1 when this call wrote ``record``, 2 when the route was already
        APPLIED for ``expected_token`` on the same claim and generation (no
        write; the caller's retry is idempotent), and 0 when the fence was
        lost: the route is missing, not LIVE, names another claim or
        generation, carries another credential, or is not PENDING. The token
        comparison happens inside Valkey against the stored copy, so the
        credential is never compared in worker memory here; it is the same
        value the route already holds.
        """

        return int(
            self._mark_adoption_applied(
                keys=[self._key(thread_key)],
                args=[
                    expected_claim,
                    expected_generation,
                    expected_token,
                    record.to_json(),
                    ttl_seconds,
                ],
            )
        )

    def clear_adopting_event(
        self,
        thread_key: str,
        *,
        expected_claim: str,
        expected_generation: int,
        expected_event_id: str,
        record: RouteRecord,
        ttl_seconds: int,
    ) -> bool:
        """Write ``record`` only while the route is LIVE and APPLIED on the same
        claim and generation and still carries ``expected_event_id`` as its
        adopting event. False when the fence was lost (nothing written)."""

        return bool(
            self._clear_adopting_event(
                keys=[self._key(thread_key)],
                args=[
                    expected_claim,
                    expected_generation,
                    expected_event_id,
                    record.to_json(),
                    ttl_seconds,
                ],
            )
        )

    def touch(self, thread_key: str, ttl_seconds: int) -> bool:
        """Refresh the route TTL on activity. Returns False if no route."""

        return bool(self._redis.expire(self._key(thread_key), ttl_seconds))

    def delete_if_claim(self, thread_key: str, claim_name: str) -> bool:
        """Delete the route only when it still names ``claim_name``."""

        return bool(self._delete_if_claim(keys=[self._key(thread_key)], args=[claim_name]))

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
