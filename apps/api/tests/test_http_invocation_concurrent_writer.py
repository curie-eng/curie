"""Another identity writer committing mid-composition restarts it, within a bound.

@spec HTTP-INVOCATION-EXECUTION-4 HTTP-INVOCATION-EXECUTION-8.
Every resolved observation advances the tenant lifecycle generation, so the
concurrent writer here is another sender's committed observation.
"""

from __future__ import annotations

import asyncio
import threading
import time
import uuid
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from types import SimpleNamespace
from typing import Any

import pytest
import redis
import redis.asyncio as redis_async
from curie_api import identity_fence, principal_authority
from curie_api.config import get_settings
from curie_api.identity import http_invocation
from curie_api.identity.http_transport import HttpIdentityClient
from curie_api.main import create_app
from curie_api.schemas.http_identity import FactsRefusal
from curie_internal.identity_authority import IDENTITY_CALLER_WAIT_SECONDS
from fastapi.testclient import TestClient
from role_policy_support import run
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from apps.api.tests import test_http_invocation_membership as membership
from apps.api.tests import test_http_turn_authority as turn_authority
from apps.api.tests.test_http_invocation_membership import synthetic_provider as synthetic_provider


def another_writer(scope: dict[str, Any]) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-4: commit a lifecycle advance as a real fenced writer."""

    async def advance(factory: async_sessionmaker[AsyncSession]) -> None:
        """@spec HTTP-INVOCATION-EXECUTION-4."""
        client = redis_async.Redis.from_url(get_settings().valkey_dsn())
        try:
            async with identity_fence.mutation(factory, client) as writer, writer.begin():
                _, row = await principal_authority.lock_scope(writer, uuid.UUID(scope["tenant"]))
                assert row is not None
                row.lifecycle_generation += 1
        finally:
            await client.aclose()

    run(advance)


def writers(scope: dict[str, Any], *, times: int | None) -> tuple[Callable[[], None], list[int]]:
    """@spec HTTP-INVOCATION-EXECUTION-4: commit up to ``times`` writers, one per call."""
    committed: list[int] = []

    def write() -> None:
        """@spec HTTP-INVOCATION-EXECUTION-4."""
        if times is None or len(committed) < times:
            another_writer(scope)
            committed.append(1)

    return write, committed


@pytest.mark.parametrize("window", ["lookup", "reader"])
def test_mention_while_another_writer_commits_restarts_and_queues_once(
    window: str, clean_db: None, valkey: redis.Redis, runs_stream: str,
    synthetic_provider: SimpleNamespace, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-4: both stale windows restart with a fresh lookup."""
    provider = synthetic_provider
    with turn_authority.queued_invocation(provider, valkey, runs_stream) as invocation:
        write, committed = writers(invocation.scope, times=1)
        if window == "lookup":
            provider.during_lookup = write
        else:
            locks: list[int] = []
            original = identity_fence.lock_identity_reader

            async def reader(session: AsyncSession) -> None:
                """@spec HTTP-INVOCATION-EXECUTION-4: commit after this caller's writer."""
                locks.append(1)
                if len(locks) == 2:
                    membership.joined(write)
                await original(session)

            # Only this composition's readers: other routes take the same shared lock.
            fence = SimpleNamespace(**{**vars(identity_fence), "lock_identity_reader": reader})
            monkeypatch.setattr(http_invocation, "identity_fence", fence)
        token = membership._credential(invocation.client, invocation.scope)
        accepted = membership._post(
            invocation.client, token, *membership._submission(invocation.scope),
        )
        assert accepted.status_code == 200, accepted.text
        assert committed == [1]
        assert len(provider.requests) == 3
        assert len({request["request_id"] for request in provider.requests}) == 3
        assert len(valkey.xrange(runs_stream)) == 2


def test_mention_stays_unavailable_while_writers_keep_committing(
    clean_db: None, valkey: redis.Redis, runs_stream: str, synthetic_provider: SimpleNamespace,
) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-4: three stale attempts are unavailable, nothing queued."""
    provider = synthetic_provider
    with turn_authority.queued_invocation(provider, valkey, runs_stream) as invocation:
        token = membership._credential(invocation.client, invocation.scope)
        before = turn_authority.delivery_state(valkey, runs_stream, invocation.scope["binding"])
        write, committed = writers(invocation.scope, times=None)
        provider.during_lookup = write
        refused = membership._post(
            invocation.client, token, *membership._submission(invocation.scope),
        )
        assert refused.status_code == 503
        assert refused.json() == {"detail": "identity_authority_unavailable"}
        assert len(committed) == 3
        assert len(provider.requests) == 1 + 3
        assert turn_authority.delivery_state(
            valkey, runs_stream, invocation.scope["binding"],
        ) == before


def test_current_check_while_another_writer_commits_continues(
    clean_db: None, valkey: redis.Redis, runs_stream: str, synthetic_provider: SimpleNamespace,
) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-4/8: the worker recheck restarts instead of refusing."""
    provider = synthetic_provider
    with turn_authority.queued_invocation(provider, valkey, runs_stream) as invocation:
        before = turn_authority.delivery_state(valkey, runs_stream, invocation.scope["binding"])
        write, committed = writers(invocation.scope, times=1)
        provider.during_lookup = write
        request = turn_authority.request_for(invocation)
        turn_authority.assert_response(
            turn_authority.post_check(invocation.client, request), request,
            decision="continue", reason="current_authority",
        )
        assert committed == [1]
        assert len(provider.requests) == 3
        assert turn_authority.delivery_state(
            valkey, runs_stream, invocation.scope["binding"],
        ) == before


# A short injected composition budget keeps these deadline tests fast; the
# production path reads the same module setting on every composition.
BUDGET = 1.5
SLOW = 0.6
# Monotonic slack for the event loop to deliver the deadline and fail closed.
SLACK = 0.3


def timed(monkeypatch: pytest.MonkeyPatch) -> list[tuple[str, float]]:
    """@spec HTTP-INVOCATION-EXECUTION-4: elapsed time from composition entry to its outcome."""
    spans: list[tuple[str, float]] = []
    original = http_invocation.current_source_authority

    @asynccontextmanager
    async def measured(*args: Any, **kwargs: Any) -> AsyncIterator[Any]:
        """@spec HTTP-INVOCATION-EXECUTION-4."""
        start = time.monotonic()
        settled = False
        try:
            async with original(*args, **kwargs) as current:
                spans.append(("yielded", time.monotonic() - start))
                settled = True
                yield current
        finally:
            if not settled:
                spans.append(("failed", time.monotonic() - start))

    monkeypatch.setattr(http_invocation, "current_source_authority", measured)
    return spans


def slow(then: Callable[[], None] | None = None) -> Callable[[], None]:
    """@spec HTTP-INVOCATION-EXECUTION-4: a slow provider answer with fresh valid facts."""

    def respond() -> None:
        """@spec HTTP-INVOCATION-EXECUTION-4."""
        time.sleep(SLOW)
        if then is not None:
            then()

    return respond


def holder(scope: dict[str, Any], seconds: float) -> tuple[threading.Thread, threading.Event]:
    """@spec HTTP-INVOCATION-EXECUTION-4: an identity writer that holds the fence lock."""
    held = threading.Event()

    async def hold(factory: async_sessionmaker[AsyncSession]) -> None:
        """@spec HTTP-INVOCATION-EXECUTION-4."""
        client = redis_async.Redis.from_url(get_settings().valkey_dsn())
        try:
            async with identity_fence.mutation(factory, client) as writer, writer.begin():
                _, row = await principal_authority.lock_scope(writer, uuid.UUID(scope["tenant"]))
                assert row is not None
                held.set()
                await asyncio.sleep(seconds)
        finally:
            await client.aclose()

    return threading.Thread(target=run, args=(hold,), name="synthetic-lock-holder"), held


def test_slow_lookups_with_stale_captures_exhaust_one_budget(
    clean_db: None, valkey: redis.Redis, runs_stream: str,
    synthetic_provider: SimpleNamespace, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-4: retries share one deadline and fail closed after it."""
    provider = synthetic_provider
    monkeypatch.setattr(http_invocation, "_COMPOSITION_BUDGET_SECONDS", BUDGET)
    with turn_authority.queued_invocation(provider, valkey, runs_stream) as invocation:
        token = membership._credential(invocation.client, invocation.scope)
        before = turn_authority.delivery_state(valkey, runs_stream, invocation.scope["binding"])
        write, committed = writers(invocation.scope, times=2)
        provider.during_lookup = slow(write)
        spans = timed(monkeypatch)
        refused = membership._post(
            invocation.client, token, *membership._submission(invocation.scope),
        )
        assert refused.status_code == 503, refused.text
        assert refused.json() == {"detail": "identity_authority_unavailable"}
        assert committed == [1, 1]
        [(outcome, elapsed)] = spans
        assert outcome == "failed"
        assert elapsed < BUDGET + SLACK
        assert turn_authority.delivery_state(
            valkey, runs_stream, invocation.scope["binding"],
        ) == before


@pytest.mark.parametrize("phase", ["capture", "revalidation"])
def test_identity_lock_wait_past_the_budget_fails_closed(
    phase: str, clean_db: None, valkey: redis.Redis, runs_stream: str,
    synthetic_provider: SimpleNamespace, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-4: lock waits, including final revalidation, are bounded."""
    provider = synthetic_provider
    monkeypatch.setattr(http_invocation, "_COMPOSITION_BUDGET_SECONDS", BUDGET)
    with turn_authority.queued_invocation(provider, valkey, runs_stream) as invocation:
        token = membership._credential(invocation.client, invocation.scope)
        before = turn_authority.delivery_state(valkey, runs_stream, invocation.scope["binding"])
        thread, held = holder(invocation.scope, BUDGET + 1.0)
        locks: list[int] = []
        original = identity_fence.lock_identity_reader

        async def reader(session: AsyncSession) -> None:
            """@spec HTTP-INVOCATION-EXECUTION-4: another writer holds the fence meanwhile."""
            locks.append(1)
            if len(locks) == (1 if phase == "capture" else 2):
                thread.start()
                assert held.wait(5)
            await original(session)

        fence = SimpleNamespace(**{**vars(identity_fence), "lock_identity_reader": reader})
        monkeypatch.setattr(http_invocation, "identity_fence", fence)
        spans = timed(monkeypatch)
        try:
            refused = membership._post(
                invocation.client, token, *membership._submission(invocation.scope),
            )
        finally:
            if thread.is_alive() or held.is_set():
                thread.join(timeout=10)
        assert refused.status_code == 503, refused.text
        [(outcome, elapsed)] = spans
        assert outcome == "failed"
        assert elapsed < BUDGET + SLACK
        assert turn_authority.delivery_state(
            valkey, runs_stream, invocation.scope["binding"],
        ) == before


def test_uncontended_slow_lookup_within_the_budget_queues_once(
    clean_db: None, valkey: redis.Redis, runs_stream: str,
    synthetic_provider: SimpleNamespace, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-4: the budget does not refuse an in-time composition."""
    provider = synthetic_provider
    monkeypatch.setattr(http_invocation, "_COMPOSITION_BUDGET_SECONDS", BUDGET)
    with turn_authority.queued_invocation(provider, valkey, runs_stream) as invocation:
        token = membership._credential(invocation.client, invocation.scope)
        provider.during_lookup = slow()
        spans = timed(monkeypatch)
        accepted = membership._post(
            invocation.client, token, *membership._submission(invocation.scope),
        )
        assert accepted.status_code == 200, accepted.text
        [(outcome, elapsed)] = spans
        assert outcome == "yielded"
        assert SLOW <= elapsed < BUDGET
        assert len(provider.requests) == 2
        assert len(valkey.xrange(runs_stream)) == 2


def test_production_composition_budget_is_below_the_caller_wait() -> None:
    """@spec HTTP-INVOCATION-EXECUTION-4: derived from the shared caller wait, not retyped."""
    assert 0 < http_invocation._COMPOSITION_BUDGET_SECONDS < IDENTITY_CALLER_WAIT_SECONDS


def test_one_unavailable_lookup_is_retried_within_the_composition_budget(
    clean_db: None, valkey: redis.Redis, runs_stream: str,
    synthetic_provider: SimpleNamespace, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-4: one unavailable lookup retries inside the budget."""
    provider = synthetic_provider
    real = HttpIdentityClient.lookup
    pending = True
    calls: list[int] = []

    async def lookup(client: HttpIdentityClient, **kwargs: Any) -> Any:
        """@spec HTTP-INVOCATION-EXECUTION-4: the first refusal does not call the provider."""
        nonlocal pending
        calls.append(1)
        if pending:
            pending = False
            return FactsRefusal(code="unavailable")
        return await real(client, **kwargs)

    monkeypatch.setattr(HttpIdentityClient, "lookup", lookup)
    # The helper's own POST is the composition under test.
    with turn_authority.queued_invocation(provider, valkey, runs_stream):
        pass
    assert len(calls) == 2


def test_unavailable_lookups_exhaust_the_composition_attempts(
    clean_db: None, valkey: redis.Redis, runs_stream: str,
    synthetic_provider: SimpleNamespace, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-4: unavailable lookups exhaust the three attempts."""
    provider = synthetic_provider
    scope = membership._scope(provider.url)
    scope["trusted"] = ()
    module = membership._runtime_api()
    identity_client = HttpIdentityClient(tls_context=provider.tls)
    calls: list[int] = []

    async def lookup(client: HttpIdentityClient, **kwargs: Any) -> Any:
        """@spec HTTP-INVOCATION-EXECUTION-4: refuse without calling the provider."""
        calls.append(1)
        return FactsRefusal(code="unavailable")

    monkeypatch.setattr(HttpIdentityClient, "lookup", lookup)
    try:
        with TestClient(create_app(
            http_identity_runtime=membership._runtime(module, scope),
            http_identity_client=identity_client,
        )) as client:
            credential = membership._credential(client, scope)
            before = turn_authority.delivery_state(valkey, runs_stream, scope["binding"])
            refused = membership._post(client, credential, *membership._submission(scope))
            assert refused.status_code == 503, refused.text
            assert refused.json() == {"detail": "identity_authority_unavailable"}
            assert len(calls) == 3
            assert turn_authority.delivery_state(
                valkey, runs_stream, scope["binding"],
            ) == before
        assert identity_client.close_status().state == "closed"
    finally:
        keys = membership._keys(valkey, scope["binding"])
        if keys:
            valkey.delete(*keys)


def test_non_unavailable_lookup_refusal_is_not_retried(
    clean_db: None, valkey: redis.Redis, runs_stream: str,
    synthetic_provider: SimpleNamespace, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-4: a non-unavailable lookup refusal is not retried."""
    provider = synthetic_provider
    scope = membership._scope(provider.url)
    scope["trusted"] = ()
    module = membership._runtime_api()
    identity_client = HttpIdentityClient(tls_context=provider.tls)
    calls: list[int] = []

    async def lookup(client: HttpIdentityClient, **kwargs: Any) -> Any:
        """@spec HTTP-INVOCATION-EXECUTION-4: refuse without calling the provider."""
        calls.append(1)
        return FactsRefusal(code="identity_stale")

    monkeypatch.setattr(HttpIdentityClient, "lookup", lookup)
    try:
        with TestClient(create_app(
            http_identity_runtime=membership._runtime(module, scope),
            http_identity_client=identity_client,
        )) as client:
            credential = membership._credential(client, scope)
            before = turn_authority.delivery_state(valkey, runs_stream, scope["binding"])
            refused = membership._post(client, credential, *membership._submission(scope))
            assert refused.status_code == 503, refused.text
            assert refused.json() == {"detail": "identity_authority_unavailable"}
            assert len(calls) == 1
            assert turn_authority.delivery_state(
                valkey, runs_stream, scope["binding"],
            ) == before
        assert identity_client.close_status().state == "closed"
    finally:
        keys = membership._keys(valkey, scope["binding"])
        if keys:
            valkey.delete(*keys)
