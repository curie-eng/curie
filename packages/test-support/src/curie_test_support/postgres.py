"""Shared Postgres connect-and-skip helper for the test suites.

Local developer loops can skip an unreachable Postgres, but required CI must
surface the original connection error, exactly like the Valkey helper.
"""

from __future__ import annotations

import os

import pytest
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncEngine


async def pg_connect_or_skip(engine: AsyncEngine) -> None:
    """Open and close one connection, skipping only in optional local loops.

    An unreachable Postgres skips a local test only when
    ``CI_REQUIRE_POSTGRES_TESTS`` is absent. Its presence, even empty, makes the
    original error fail the required CI job instead. The caller owns the engine
    and must dispose it.

    Both error classes count as unreachable. asyncpg raises a refused or timed
    out connect as a bare ``OSError`` that SQLAlchemy does not wrap; SQLAlchemy
    raises its own errors (such as a pool timeout) as ``SQLAlchemyError``. A
    server that answers but rejects the login raises asyncpg's own error, which
    is neither, so a wrong password fails the test even locally.
    """
    try:
        async with engine.connect():
            pass
    except (SQLAlchemyError, OSError) as exc:
        if "CI_REQUIRE_POSTGRES_TESTS" in os.environ:
            raise
        url = engine.url.render_as_string(hide_password=True)
        pytest.skip(f"Postgres not reachable at {url}: {exc}")
