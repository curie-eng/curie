"""AffinityStore behavior against the real compose-stack Valkey."""

from __future__ import annotations

import asyncio
import time
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from dataclasses import replace
from typing import Any

import pytest
import redis
from curie_worker.sandbox import AffinityStore, RouteRecord, RouteState, SandboxHandle
from redis.asyncio import Redis as AsyncRedis


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


def test_replace_if_generation_is_an_atomic_claim_and_generation_fence(
    affinity: AffinityStore,
) -> None:
    original = RouteRecord(handle=_handle(claim="claim-a"))
    replacement = RouteRecord(
        handle=SandboxHandle(
            **{
                **_handle(claim="claim-b").__dict__,
                "workspace_repo": "acme-corp/acme-bot",
                "generation": 1,
            }
        )
    )
    assert affinity.put_if_absent("T1", original, ttl_seconds=60)

    assert not affinity.replace_if_generation(
        "T1",
        expected_claim="claim-stale",
        expected_generation=0,
        record=replacement,
        ttl_seconds=60,
    )
    assert not affinity.replace_if_generation(
        "T1",
        expected_claim="claim-a",
        expected_generation=1,
        record=replacement,
        ttl_seconds=60,
    )
    assert affinity.replace_if_generation(
        "T1",
        expected_claim="claim-a",
        expected_generation=0,
        record=replacement,
        ttl_seconds=60,
    )
    assert affinity.get("T1") == replacement


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


def _safe_pressure_record(thread: str, claim: str) -> RouteRecord:
    return RouteRecord(
        handle=replace(
            _handle(thread, claim),
            history_ref=f"https://api.example.com/state/transcript/{thread}",
            token=f"token-{thread}",
        )
    )


class _RecordingPressureRedis:
    """Observe pressure calls while every command still reaches real Valkey."""

    def __init__(self, client: AsyncRedis) -> None:
        self.client = client
        self.scan_calls = 0
        self.nonempty_pages = 0
        self.pipeline_calls = 0
        self.eval_calls = 0
        self.register_script_calls = 0

    def __getattr__(self, name: str) -> Any:
        return getattr(self.client, name)

    async def scan(self, *args: object, **kwargs: object) -> Any:
        self.scan_calls += 1
        result = await self.client.scan(*args, **kwargs)
        if result[1]:
            self.nonempty_pages += 1
        return result

    def pipeline(self, *args: object, **kwargs: object) -> Any:
        self.pipeline_calls += 1
        return self.client.pipeline(*args, **kwargs)

    async def eval(self, *args: object, **kwargs: object) -> Any:
        self.eval_calls += 1
        return await self.client.eval(*args, **kwargs)

    def register_script(self, *args: object, **kwargs: object) -> Any:
        self.register_script_calls += 1
        return self.client.register_script(*args, **kwargs)


class _UnknownExpiryPipeline:
    """Replace only the expiry command while the real pipeline reaches Valkey."""

    def __init__(self, pipeline: Any) -> None:
        self._pipeline = pipeline
        self.expiry_commands = 0

    def get(self, key: str) -> Any:
        return self._pipeline.get(key)

    def pexpiretime(self, key: str) -> Any:
        self.expiry_commands += 1
        return self._pipeline.execute_command("PEXPIRETIME_TEST_UNSUPPORTED", key)

    async def execute(self) -> Any:
        return await self._pipeline.execute()


class _UnknownExpiryPressureRedis:
    def __init__(self, client: AsyncRedis) -> None:
        self.client = client
        self.pipelines: list[_UnknownExpiryPipeline] = []

    def __getattr__(self, name: str) -> Any:
        return getattr(self.client, name)

    def pipeline(self, *args: object, **kwargs: object) -> _UnknownExpiryPipeline:
        pipeline = _UnknownExpiryPipeline(self.client.pipeline(*args, **kwargs))
        self.pipelines.append(pipeline)
        return pipeline


@asynccontextmanager
async def _pressure_store(
    redis_client: redis.Redis,
    pressure_redis_factory: Callable[[], AsyncRedis],
    key_prefix: str,
    *,
    record: bool = False,
) -> AsyncIterator[tuple[AffinityStore, _RecordingPressureRedis | None]]:
    pressure_client = pressure_redis_factory()
    await pressure_client.ping()
    recording = _RecordingPressureRedis(pressure_client) if record else None
    store = AffinityStore(
        redis_client,
        pressure_client=recording or pressure_client,  # type: ignore[arg-type]
        key_prefix=key_prefix,
    )
    try:
        yield store, recording
    finally:
        await pressure_client.aclose()


def test_pressure_inventory_finishes_below_both_caps_with_one_pipeline_per_page(
    redis_client: redis.Redis,
    pressure_redis_factory: Callable[[], AsyncRedis],
    key_prefix: str,
) -> None:
    async def go() -> None:
        async with _pressure_store(
            redis_client,
            pressure_redis_factory,
            key_prefix,
            record=True,
        ) as (affinity, recording):
            assert recording is not None
            retry = recording.client.connection_pool.connection_kwargs["retry"]
            assert retry.get_retries() == 0
            for number in range(3):
                thread_key = f"T-complete-{number}"
                assert affinity.put_if_absent(
                    thread_key,
                    _safe_pressure_record(thread_key, f"claim-complete-{number}"),
                    ttl_seconds=60,
                )

            result = await affinity.pressure_candidates(
                max_pages=8,
                max_records=256,
                deadline=time.monotonic() + 2.0,
            )

            assert result.outcome == "complete"
            assert len(result.candidates) == 3
            assert 1 <= recording.scan_calls < 8
            assert recording.pipeline_calls == recording.nonempty_pages

    asyncio.run(go())


def test_pressure_inventory_completes_with_large_unrelated_database(
    redis_client: redis.Redis,
    pressure_redis_factory: Callable[[], AsyncRedis],
    key_prefix: str,
) -> None:
    async def go() -> None:
        async with _pressure_store(
            redis_client,
            pressure_redis_factory,
            key_prefix,
            record=True,
        ) as (affinity, recording):
            assert recording is not None
            padding = {
                f"{key_prefix}:unrelated:{number}": "padding"
                for number in range(5_001)
            }
            redis_client.mset(padding)
            for number in range(3):
                thread_key = f"T-padded-{number}"
                assert affinity.put_if_absent(
                    thread_key,
                    _safe_pressure_record(thread_key, f"claim-padded-{number}"),
                    ttl_seconds=60,
                )

            result = await affinity.pressure_candidates(
                max_pages=8,
                max_records=256,
                deadline=time.monotonic() + 2.0,
            )

            assert result.outcome == "complete"
            assert [candidate.thread_key for candidate in result.candidates] == [
                "T-padded-0",
                "T-padded-1",
                "T-padded-2",
            ]
            assert recording.scan_calls <= 8

    asyncio.run(go())


def test_pressure_inventory_maps_real_unknown_expiry_command_to_finite_outcome(
    redis_client: redis.Redis,
    pressure_redis_factory: Callable[[], AsyncRedis],
    key_prefix: str,
) -> None:
    async def go() -> None:
        pressure_client = pressure_redis_factory()
        await pressure_client.ping()
        substituted = _UnknownExpiryPressureRedis(pressure_client)
        affinity = AffinityStore(
            redis_client,
            pressure_client=substituted,  # type: ignore[arg-type]
            key_prefix=key_prefix,
        )
        try:
            assert affinity.put_if_absent(
                "T-no-expiry-command",
                _safe_pressure_record(
                    "T-no-expiry-command",
                    "claim-no-expiry-command",
                ),
                ttl_seconds=60,
            )

            result = await affinity.pressure_candidates(
                max_pages=8,
                max_records=256,
                deadline=time.monotonic() + 2.0,
            )

            assert result.outcome == "expiry-unsupported"
            assert result.candidates == ()
            assert substituted.pipelines
            assert sum(
                pipeline.expiry_commands for pipeline in substituted.pipelines
            ) == 1
        finally:
            await pressure_client.aclose()

    asyncio.run(go())


@pytest.mark.parametrize(
    ("max_pages", "max_records", "deadline_delta"),
    [
        pytest.param(0, 256, 2.0, id="page-cap"),
        pytest.param(8, 0, 2.0, id="record-cap"),
        pytest.param(8, 256, -1.0, id="deadline"),
    ],
)
def test_pressure_inventory_returns_no_candidates_when_a_bound_is_exhausted(
    redis_client: redis.Redis,
    pressure_redis_factory: Callable[[], AsyncRedis],
    key_prefix: str,
    max_pages: int,
    max_records: int,
    deadline_delta: float,
) -> None:
    async def go() -> None:
        async with _pressure_store(
            redis_client, pressure_redis_factory, key_prefix
        ) as (affinity, _recording):
            assert affinity.put_if_absent(
                "T-bounded",
                _safe_pressure_record("T-bounded", "claim-bounded"),
                ttl_seconds=60,
            )

            result = await affinity.pressure_candidates(
                max_pages=max_pages,
                max_records=max_records,
                deadline=time.monotonic() + deadline_delta,
            )

            assert result.outcome == "scan-incomplete"
            assert result.candidates == ()
            assert affinity.live_claim_names() == {"claim-bounded"}

    asyncio.run(go())


def test_pressure_candidates_use_absolute_expiry_order_after_restart(
    redis_client: redis.Redis,
    pressure_redis_factory: Callable[[], AsyncRedis],
    key_prefix: str,
) -> None:
    async def go() -> None:
        pressure_client = pressure_redis_factory()
        await pressure_client.ping()
        try:
            affinity = AffinityStore(
                redis_client,
                pressure_client=pressure_client,
                key_prefix=key_prefix,
            )
            records = {
                "T-later": _safe_pressure_record("T-later", "claim-later"),
                "T-tie-b": _safe_pressure_record("T-tie-b", "claim-tie-b"),
                "T-tie-a": _safe_pressure_record("T-tie-a", "claim-tie-a"),
            }
            for thread_key, record in records.items():
                assert affinity.put_if_absent(thread_key, record, ttl_seconds=60)

            now_ms = time.time_ns() // 1_000_000
            expires = {
                "T-later": now_ms + 50_000,
                "T-tie-b": now_ms + 40_000,
                "T-tie-a": now_ms + 40_000,
            }
            for thread_key, expires_at_ms in expires.items():
                assert redis_client.pexpireat(
                    affinity._key(thread_key),  # noqa: SLF001
                    expires_at_ms,
                )

            first = await affinity.pressure_candidates(
                max_pages=8,
                max_records=256,
                deadline=time.monotonic() + 2.0,
            )
            assert first.outcome == "complete"
            assert [candidate.thread_key for candidate in first.candidates] == [
                "T-tie-a",
                "T-tie-b",
                "T-later",
            ]
            assert [candidate.expires_at_ms for candidate in first.candidates] == [
                expires["T-tie-a"],
                expires["T-tie-b"],
                expires["T-later"],
            ]

            restarted = AffinityStore(
                redis_client,
                pressure_client=pressure_client,
                key_prefix=affinity._prefix,  # noqa: SLF001
            )
            after_restart = await restarted.pressure_candidates(
                max_pages=8,
                max_records=256,
                deadline=time.monotonic() + 2.0,
            )
            assert after_restart == first
        finally:
            await pressure_client.aclose()

    asyncio.run(go())


def test_pressure_candidates_fail_closed_at_the_record_cap(
    redis_client: redis.Redis,
    pressure_redis_factory: Callable[[], AsyncRedis],
    key_prefix: str,
) -> None:
    async def go() -> None:
        async with _pressure_store(
            redis_client, pressure_redis_factory, key_prefix
        ) as (affinity, _recording):
            for number in range(3):
                thread_key = f"T-{number}"
                assert affinity.put_if_absent(
                    thread_key,
                    _safe_pressure_record(thread_key, f"claim-{number}"),
                    ttl_seconds=60,
                )

            result = await affinity.pressure_candidates(
                max_pages=8,
                max_records=2,
                deadline=time.monotonic() + 2.0,
            )
            assert result.outcome == "scan-incomplete"
            assert result.candidates == ()
            assert affinity.live_claim_names() == {
                "claim-0",
                "claim-1",
                "claim-2",
            }

    asyncio.run(go())


def test_pressure_candidates_exclude_unsafe_and_malformed_routes(
    redis_client: redis.Redis,
    pressure_redis_factory: Callable[[], AsyncRedis],
    key_prefix: str,
) -> None:
    async def go() -> None:
        async with _pressure_store(
            redis_client, pressure_redis_factory, key_prefix
        ) as (affinity, _recording):
            safe = _safe_pressure_record("T-safe", "claim-safe")
            assert affinity.put_if_absent("T-safe", safe, ttl_seconds=60)

            suspended = replace(
                _safe_pressure_record("T-suspended", "claim-suspended"),
                state=RouteState.SUSPENDED,
            )
            assert affinity.put_if_absent("T-suspended", suspended, ttl_seconds=60)
            workspace = _safe_pressure_record("T-workspace", "claim-workspace")
            workspace = replace(
                workspace,
                handle=replace(
                    workspace.handle,
                    workspace_repo="acme-corp/acme-bot",
                ),
            )
            assert affinity.put_if_absent("T-workspace", workspace, ttl_seconds=60)
            publication = _safe_pressure_record("T-publication", "claim-publication")
            publication = replace(
                publication,
                handle=replace(
                    publication.handle,
                    publication_visible_outcome_revision=1,
                ),
            )
            assert affinity.put_if_absent("T-publication", publication, ttl_seconds=60)

            no_expiry = _safe_pressure_record("T-no-expiry", "claim-no-expiry")
            redis_client.set(
                affinity._key("T-no-expiry"),  # noqa: SLF001
                no_expiry.to_json(),
            )
            redis_client.set(
                affinity._key("T-malformed"),  # noqa: SLF001
                "not-json",
                ex=60,
            )

            result = await affinity.pressure_candidates(
                max_pages=8,
                max_records=256,
                deadline=time.monotonic() + 2.0,
            )
            assert result.outcome == "complete"
            assert [candidate.thread_key for candidate in result.candidates] == [
                "T-safe"
            ]

    asyncio.run(go())


@pytest.mark.parametrize(
    "mismatch",
    ["claim", "generation", "lock-key", "lock-token", "expiry"],
)
def test_detach_if_unchanged_fences_every_route_and_lock_fact(
    redis_client: redis.Redis,
    pressure_redis_factory: Callable[[], AsyncRedis],
    key_prefix: str,
    mismatch: str,
) -> None:
    async def go() -> None:
        async with _pressure_store(
            redis_client, pressure_redis_factory, key_prefix
        ) as (affinity, _recording):
            thread_key = f"T-{mismatch}"
            record = _safe_pressure_record(thread_key, "claim-a")
            record = replace(record, handle=replace(record.handle, generation=7))
            assert affinity.put_if_absent(thread_key, record, ttl_seconds=60)
            candidates = await affinity.pressure_candidates(
                max_pages=8,
                max_records=256,
                deadline=time.monotonic() + 2.0,
            )
            candidate = candidates.candidates[0]
            lock_key = f"test:lock:{mismatch}"
            redis_client.set(lock_key, "owned-token", px=60_000)

            expected_claim = record.handle.claim_name
            expected_generation = record.handle.generation
            expected_expiry = candidate.expires_at_ms
            expected_lock_key = lock_key
            expected_lock_token = "owned-token"
            if mismatch == "claim":
                expected_claim = "claim-stale"
            elif mismatch == "generation":
                expected_generation += 1
            elif mismatch == "lock-key":
                expected_lock_key = f"{lock_key}:other"
            elif mismatch == "lock-token":
                expected_lock_token = "stale-token"
            else:
                assert affinity.touch(thread_key, ttl_seconds=30)

            assert not await affinity.detach_if_unchanged(
                thread_key,
                expected_claim=expected_claim,
                expected_generation=expected_generation,
                expected_expires_at_ms=expected_expiry,
                lock_key=expected_lock_key,
                lock_token=expected_lock_token,
            )
            assert affinity.get(thread_key) == record

    asyncio.run(go())


def test_detach_if_unchanged_removes_only_the_exact_owned_route(
    redis_client: redis.Redis,
    pressure_redis_factory: Callable[[], AsyncRedis],
    key_prefix: str,
) -> None:
    async def go() -> None:
        async with _pressure_store(
            redis_client, pressure_redis_factory, key_prefix
        ) as (affinity, _recording):
            record = _safe_pressure_record("T-owned", "claim-owned")
            assert affinity.put_if_absent("T-owned", record, ttl_seconds=60)
            candidates = await affinity.pressure_candidates(
                max_pages=8,
                max_records=256,
                deadline=time.monotonic() + 2.0,
            )
            candidate = candidates.candidates[0]
            lock_key = "test:lock:owned"
            redis_client.set(lock_key, "owned-token", px=60_000)

            assert await affinity.detach_if_unchanged(
                "T-owned",
                expected_claim=record.handle.claim_name,
                expected_generation=record.handle.generation,
                expected_expires_at_ms=candidate.expires_at_ms,
                lock_key=lock_key,
                lock_token="owned-token",
            )
            assert affinity.get("T-owned") is None

    asyncio.run(go())


def test_detach_uses_one_direct_eval_without_a_script_cache_retry(
    redis_client: redis.Redis,
    pressure_redis_factory: Callable[[], AsyncRedis],
    key_prefix: str,
) -> None:
    async def go() -> None:
        async with _pressure_store(
            redis_client,
            pressure_redis_factory,
            key_prefix,
            record=True,
        ) as (affinity, recording):
            assert recording is not None
            record = _safe_pressure_record("T-eval", "claim-eval")
            assert affinity.put_if_absent("T-eval", record, ttl_seconds=60)
            candidates = await affinity.pressure_candidates(
                max_pages=8,
                max_records=256,
                deadline=time.monotonic() + 2.0,
            )
            candidate = candidates.candidates[0]
            lock_key = "test:lock:eval"
            redis_client.set(lock_key, "owned-token", px=60_000)

            assert await affinity.detach_if_unchanged(
                "T-eval",
                expected_claim=record.handle.claim_name,
                expected_generation=record.handle.generation,
                expected_expires_at_ms=candidate.expires_at_ms,
                lock_key=lock_key,
                lock_token="owned-token",
            )

            assert recording.register_script_calls == 0
            assert recording.eval_calls == 1

    asyncio.run(go())


def test_route_replacement_after_scan_preserves_the_reused_sandbox(
    redis_client: redis.Redis,
    pressure_redis_factory: Callable[[], AsyncRedis],
    key_prefix: str,
) -> None:
    async def go() -> None:
        async with _pressure_store(
            redis_client, pressure_redis_factory, key_prefix
        ) as (affinity, _recording):
            original = _safe_pressure_record("T-replaced", "claim-original")
            assert affinity.put_if_absent("T-replaced", original, ttl_seconds=60)
            candidates = await affinity.pressure_candidates(
                max_pages=8,
                max_records=256,
                deadline=time.monotonic() + 2.0,
            )
            candidate = candidates.candidates[0]
            replacement = replace(
                original,
                handle=replace(
                    original.handle,
                    claim_name="claim-reused",
                    sandbox_name="sbx-claim-reused",
                    generation=original.handle.generation + 1,
                ),
            )
            affinity.replace("T-replaced", replacement, ttl_seconds=60)
            lock_key = "test:lock:replacement"
            redis_client.set(lock_key, "owned-token", px=60_000)

            assert not await affinity.detach_if_unchanged(
                "T-replaced",
                expected_claim=original.handle.claim_name,
                expected_generation=original.handle.generation,
                expected_expires_at_ms=candidate.expires_at_ms,
                lock_key=lock_key,
                lock_token="owned-token",
            )
            assert affinity.get("T-replaced") == replacement

    asyncio.run(go())
