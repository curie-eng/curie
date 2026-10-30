"""Another identity writer committing mid-composition restarts it, within a bound.

@spec HTTP-INVOCATION-EXECUTION-4 HTTP-INVOCATION-EXECUTION-8.
Every resolved observation advances the tenant lifecycle generation, so the
concurrent writer here is another sender's committed observation.
"""

from __future__ import annotations

import uuid
from collections.abc import Callable
from types import SimpleNamespace
from typing import Any

import pytest
import redis
import redis.asyncio as redis_async
from curie_api import identity_fence, principal_authority
from curie_api.config import get_settings
from curie_api.identity import http_invocation
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
