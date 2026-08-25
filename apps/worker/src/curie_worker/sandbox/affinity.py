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


class AffinityStore:
    """Thread-to-sandbox route records with atomic acquire and guarded delete."""

    def __init__(self, client: redis.Redis, *, key_prefix: str = "curie:sandbox") -> None:
        self._redis = client
        self._prefix = key_prefix
        self._delete_if_claim = client.register_script(_DELETE_IF_CLAIM)

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
