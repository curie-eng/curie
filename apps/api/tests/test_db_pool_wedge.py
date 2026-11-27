"""A wedged main pool, end to end through the real application (#5995).

The application runs its real lifespan against the suite's real Postgres and
Valkey. The wedge is the production one: a session outside the application
holds the identity fence's advisory lock, and fenced write requests park on
that lock, each holding a main pool connection, until all 15 are taken.
Bounds are set the way an operator sets them, through the environment.
"""

from __future__ import annotations

import contextlib
import time
from collections.abc import Callable, Iterator
from concurrent.futures import ThreadPoolExecutor
from typing import Any

import anyio.from_thread
import asyncpg
import pytest
from curie_api import identity_fence
from curie_api.config import get_settings
from curie_api.main import create_app
from curie_internal.valkey import Redis
from curie_test_support.static_shell import wait_until
from fastapi.testclient import TestClient
from sqlalchemy import make_url

CAPACITY = 15
FENCED_WRITE = "/teams"


@pytest.fixture
def wedge_env(_disposable_db: Any, monkeypatch: pytest.MonkeyPatch) -> Iterator[Any]:
    def apply(**values: object) -> None:
        for name, value in values.items():
            monkeypatch.setenv(name, str(value))
        get_settings.cache_clear()

    yield apply
    monkeypatch.undo()
    get_settings.cache_clear()
    client = Redis.from_url(get_settings().valkey_dsn())
    try:
        client.delete(identity_fence.FENCE_KEY)
    finally:
        client.close()


@contextlib.contextmanager
def outside_fence_lock(url: str) -> Iterator[Callable[[], None]]:
    """Hold the fence's session lock from a connection the app does not own.

    The connection lives on its own event loop thread; the yielded callable
    closes it, which releases the lock. It is always closed on exit.
    """
    dsn = make_url(url).set(drivername="postgresql").render_as_string(hide_password=False)
    with anyio.from_thread.start_blocking_portal() as portal:
        connection = portal.call(asyncpg.connect, dsn)

        def release() -> None:
            if not connection.is_closed():
                portal.call(connection.close)

        try:
            portal.call(connection.execute, "SELECT pg_advisory_lock(2250220)")
            yield release
        finally:
            release()


def test_health_without_a_lifespan_is_ok() -> None:
    """An app served without its lifespan has no pool watch; that is healthy."""
    client = TestClient(create_app())
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_wedged_pool_fails_readiness_and_times_out_requests(wedge_env: Any) -> None:
    wedge_env(
        DB_POOL_TIMEOUT_SECONDS=1,
        DB_POOL_WATCH_INTERVAL_SECONDS=0.2,
        DB_POOL_PROBE_TIMEOUT_SECONDS=0.5,
        DB_POOL_READY_STARVED_SECONDS=1,
        DB_POOL_RESTART_STARVED_SECONDS=2,
        DB_LOCK_TIMEOUT_SECONDS=0,
        IDENTITY_FENCE_ACQUIRE_TIMEOUT_SECONDS=60,
    )
    headers = {"X-API-Key": get_settings().api_key}
    with (
        TestClient(create_app(), raise_server_exceptions=False) as client,
        outside_fence_lock(get_settings().database_url) as release,
        ThreadPoolExecutor(max_workers=CAPACITY) as requests,
    ):
        pool = client.app.state.engine.pool
        try:
            parked = [
                requests.submit(client.post, FENCED_WRITE, headers=headers) for _ in range(CAPACITY)
            ]
            wait_until(
                lambda: pool.checkedout() >= CAPACITY, 15, "fenced writes parked on the lock"
            )
            wait_until(lambda: client.get("/ready").status_code == 503, 10, "/ready to fail")
            assert client.get("/ready").json() == {"detail": "Database pool is exhausted"}
            started = time.monotonic()
            refused = client.get("/agents", headers=headers)
            assert refused.status_code >= 500
            assert time.monotonic() - started < 4
            wait_until(lambda: client.get("/health").status_code == 503, 10, "/health to fail")
            assert client.get("/health").json() == {"detail": "Database pool is wedged"}
            assert not any(request.done() for request in parked)
        finally:
            release()
        assert all(request.result(timeout=30).status_code < 500 for request in parked)
        wait_until(
            lambda: (
                client.get("/ready").status_code == 200 and client.get("/health").status_code == 200
            ),
            10,
            "/ready and /health to recover",
        )
        assert client.get("/agents", headers=headers).status_code == 200


def test_lock_timeout_unwedges_the_pool_without_a_restart(wedge_env: Any) -> None:
    wedge_env(
        DB_POOL_TIMEOUT_SECONDS=3,
        DB_POOL_WATCH_INTERVAL_SECONDS=0.2,
        DB_POOL_PROBE_TIMEOUT_SECONDS=0.5,
        DB_POOL_READY_STARVED_SECONDS=3,
        DB_POOL_RESTART_STARVED_SECONDS=6,
        DB_LOCK_TIMEOUT_SECONDS=1,
        IDENTITY_FENCE_ACQUIRE_TIMEOUT_SECONDS=60,
    )
    headers = {"X-API-Key": get_settings().api_key}
    health: list[int] = []
    with (
        TestClient(create_app(), raise_server_exceptions=False) as client,
        outside_fence_lock(get_settings().database_url) as release,
        ThreadPoolExecutor(max_workers=CAPACITY) as requests,
    ):
        try:
            started = time.monotonic()
            parked = [
                requests.submit(client.post, FENCED_WRITE, headers=headers) for _ in range(CAPACITY)
            ]

            def settled() -> bool:
                health.append(client.get("/health").status_code)
                return all(request.done() for request in parked)

            wait_until(settled, 15, "fenced writes to fail on the lock timeout")
            assert time.monotonic() - started < 6
            assert all(request.result().status_code >= 500 for request in parked)
            assert set(health) == {200}
            # The outside lock is still held; the pool serves again by itself.
            assert client.get("/agents", headers=headers).status_code == 200
            assert client.get("/health").status_code == 200
        finally:
            release()
