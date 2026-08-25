"""AffinityStore behavior against the real compose-stack Valkey."""

from __future__ import annotations

import pytest
import redis
from curie_worker.sandbox import AffinityStore, RouteRecord, RouteState, SandboxHandle


def _handle(thread: str = "T1", claim: str = "claim-a") -> SandboxHandle:
    return SandboxHandle(
        thread_key=thread,
        claim_name=claim,
        sandbox_name=f"sbx-{claim}",
        namespace="ns",
        service_fqdn=f"sbx-{claim}.ns.svc.cluster.local",
        port=8080,
        session_id="sess-1",
    )


def test_round_trip_and_ttl(affinity: AffinityStore, redis_client: redis.Redis) -> None:
    record = RouteRecord(handle=_handle())
    assert affinity.put_if_absent("T1", record, ttl_seconds=60)

    loaded = affinity.get("T1")
    assert loaded is not None
    assert loaded.handle == record.handle
    assert loaded.state is RouteState.LIVE
    assert affinity.touch("T1", ttl_seconds=90)
    assert affinity.get("missing") is None
    assert not affinity.touch("missing", ttl_seconds=90)


def test_put_if_absent_loses_race(affinity: AffinityStore) -> None:
    first = RouteRecord(handle=_handle(claim="claim-a"))
    second = RouteRecord(handle=_handle(claim="claim-b"))
    assert affinity.put_if_absent("T1", first, ttl_seconds=60)
    assert not affinity.put_if_absent("T1", second, ttl_seconds=60)

    loaded = affinity.get("T1")
    assert loaded is not None
    assert loaded.handle.claim_name == "claim-a"


def test_delete_if_claim_guards_against_stale_releaser(affinity: AffinityStore) -> None:
    affinity.put_if_absent("T1", RouteRecord(handle=_handle(claim="claim-a")), ttl_seconds=60)

    # A stale releaser holding the wrong claim name must not delete the route.
    assert not affinity.delete_if_claim("T1", "claim-stale")
    assert affinity.get("T1") is not None

    assert affinity.delete_if_claim("T1", "claim-a")
    assert affinity.get("T1") is None
    # Second delete is a no-op, not an error.
    assert not affinity.delete_if_claim("T1", "claim-a")


def test_mark_suspended_records_history_ref(affinity: AffinityStore) -> None:
    affinity.put_if_absent("T1", RouteRecord(handle=_handle()), ttl_seconds=60)

    updated = affinity.mark_suspended("T1", "sdk-session-123", ttl_seconds=120)
    assert updated.state is RouteState.SUSPENDED
    assert updated.handle.history_ref == "sdk-session-123"

    loaded = affinity.get("T1")
    assert loaded is not None
    assert loaded.state is RouteState.SUSPENDED
    assert loaded.handle.history_ref == "sdk-session-123"


def test_live_claim_names_skips_expired_routes(affinity: AffinityStore) -> None:
    affinity.put_if_absent("T1", RouteRecord(handle=_handle("T1", "claim-a")), ttl_seconds=60)
    affinity.put_if_absent("T2", RouteRecord(handle=_handle("T2", "claim-b")), ttl_seconds=60)
    assert affinity.live_claim_names() == {"claim-a", "claim-b"}

    affinity.delete_if_claim("T2", "claim-b")
    assert affinity.live_claim_names() == {"claim-a"}


def test_route_inventory_uses_persisted_state_not_process_memory(
    affinity: AffinityStore,
) -> None:
    affinity.put_if_absent("T1", RouteRecord(handle=_handle("T1", "claim-a")), 60)
    affinity.put_if_absent("T2", RouteRecord(handle=_handle("T2", "claim-b")), 60)
    affinity.mark_suspended("T2", "history-example", 120)

    assert affinity.route_inventory() == {
        RouteState.LIVE: {"claim-a"},
        RouteState.SUSPENDED: {"claim-b"},
    }


# --- #1388: why a non-positive TTL is refused at boot rather than at the store ---
#
# Observed against the real Valkey 8.1.8 on the compose dev stack
# (`docker compose -f compose.dev.yaml up -d valkey`, localhost:26379) on
# 2026-08-07:
#
#   SET k v EX 0    -> redis.exceptions.ResponseError: invalid expire time in 'set' command
#   SET k v EX -1   -> redis.exceptions.ResponseError: invalid expire time in 'set' command
#   SET k v EX 10**20 -> redis.exceptions.ResponseError: value is not an integer or out of range
#   EXPIRE k 0      -> 1 (True), and the key is DELETED
#
# These two tests pin that behavior, not the guard: they assert what the store
# does when a bad TTL reaches it, which is the reason CURIE_ROUTE_TTL_SECONDS is
# bounded in the worker's env loader (run.py) instead. ResponseError is not in
# the kernel's _attempt catch tuple, so the first form escapes unclassified; the
# EXPIRE form never raises at all and silently drops the route on a touch.


def test_zero_ttl_put_if_absent_raises_valkey_response_error(affinity: AffinityStore) -> None:
    with pytest.raises(redis.exceptions.ResponseError) as exc:
        affinity.put_if_absent("T1", RouteRecord(handle=_handle()), ttl_seconds=0)
    assert "invalid expire time" in str(exc.value)


def test_zero_ttl_touch_reports_success_and_deletes_the_route(affinity: AffinityStore) -> None:
    assert affinity.put_if_absent("T1", RouteRecord(handle=_handle()), ttl_seconds=60)

    # The quietest failure of the three: touch() reports the refresh succeeded
    # while EXPIRE has already removed the route the thread was pinned to.
    assert affinity.touch("T1", ttl_seconds=0)
    assert affinity.get("T1") is None
