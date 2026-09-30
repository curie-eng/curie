"""Contract tests for the shared Postgres test helper."""

from __future__ import annotations

import asyncio
import socket

import pytest
from curie_test_support.postgres import pg_connect_or_skip
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import create_async_engine

# A recognizable credential that must never appear in a skip reason.
LEAK_MARKER = "do-not-leak"

# asyncpg raises a refused connect as a bare OSError (ConnectionRefusedError on
# Linux, a timeout where the SYN is dropped); SQLAlchemy wraps only DBAPI errors.
# What pg_connect_or_skip promises is that a required run RAISES rather than
# skips, whichever of these the platform produces.
UNREACHABLE = (SQLAlchemyError, OSError)


def _unreachable_url(listener: socket.socket) -> str:
    port = int(listener.getsockname()[1])
    return f"postgresql+asyncpg://postgres:{LEAK_MARKER}@127.0.0.1:{port}/postgres"


def _bind_unlistening_port() -> socket.socket:
    """Bind a loopback port without listening, preventing a port-reuse race."""
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.bind(("127.0.0.1", 0))
    return listener


async def _connect(url: str) -> None:
    engine = create_async_engine(url)
    try:
        await pg_connect_or_skip(engine)
    finally:
        await engine.dispose()


def test_pg_connect_or_skip_skips_an_unreachable_postgres_locally(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("CI_REQUIRE_POSTGRES_TESTS", raising=False)

    with _bind_unlistening_port() as listener:
        with pytest.raises(pytest.skip.Exception, match="Postgres not reachable") as info:
            asyncio.run(_connect(_unreachable_url(listener)))

    assert LEAK_MARKER not in str(info.value)


def test_pg_connect_or_skip_fails_for_an_unreachable_postgres_when_required(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("CI_REQUIRE_POSTGRES_TESTS", "")

    with _bind_unlistening_port() as listener:
        with pytest.raises(UNREACHABLE):
            asyncio.run(_connect(_unreachable_url(listener)))
