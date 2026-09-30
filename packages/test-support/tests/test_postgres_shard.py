"""Shard-level negative control for the shared Postgres test helper.

This module deliberately does not import ``curie_test_support.postgres``: it
drives the helper only through a child pytest process, so the assertions here
are what fail when the lever is missing, not a collection error.
"""

from __future__ import annotations

import os
import socket
import subprocess
import sys
from pathlib import Path


def _bind_unlistening_port() -> socket.socket:
    """Bind a loopback port without listening, preventing a port-reuse race."""
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.bind(("127.0.0.1", 0))
    return listener


def _unreachable_url(listener: socket.socket) -> str:
    port = int(listener.getsockname()[1])
    return f"postgresql+asyncpg://postgres:postgres@127.0.0.1:{port}/postgres"


_INNER_TEST = """
import asyncio
import os

from curie_test_support.postgres import pg_connect_or_skip
from sqlalchemy.ext.asyncio import create_async_engine


async def _reach() -> None:
    engine = create_async_engine(os.environ["TEST_DATABASE_URL"])
    try:
        await pg_connect_or_skip(engine)
    finally:
        await engine.dispose()


def test_reaches_postgres() -> None:
    asyncio.run(_reach())
"""


def _run_inner_shard(
    tmp_path: Path, url: str, *, required: bool
) -> subprocess.CompletedProcess[str]:
    """Run one pytest process on an unreachable database, isolated from the repo config."""
    test_file = tmp_path / "test_inner.py"
    test_file.write_text(_INNER_TEST)
    environment = {
        key: value
        for key, value in os.environ.items()
        if key not in {"CI_REQUIRE_POSTGRES_TESTS", "PYTEST_ADDOPTS"}
    }
    environment["TEST_DATABASE_URL"] = url
    if required:
        environment["CI_REQUIRE_POSTGRES_TESTS"] = "1"
    return subprocess.run(
        [
            sys.executable,
            "-m",
            "pytest",
            "-p",
            "no:cacheprovider",
            "--rootdir",
            str(tmp_path),
            "-c",
            "/dev/null",
            "-rs",
            str(test_file),
        ],
        cwd=tmp_path,
        env=environment,
        capture_output=True,
        text=True,
        check=False,
    )


def test_unreachable_database_fails_the_shard_when_required(tmp_path: Path) -> None:
    """Negative control: a required shard with no Postgres must exit non-zero."""
    with _bind_unlistening_port() as listener:
        completed = _run_inner_shard(tmp_path, _unreachable_url(listener), required=True)

    assert completed.returncode != 0, completed.stdout
    assert "1 failed" in completed.stdout, completed.stdout
    assert "skipped" not in completed.stdout, completed.stdout


def test_unreachable_database_skips_the_shard_when_not_required(tmp_path: Path) -> None:
    with _bind_unlistening_port() as listener:
        completed = _run_inner_shard(tmp_path, _unreachable_url(listener), required=False)

    assert completed.returncode == 0, completed.stdout
    assert "1 skipped" in completed.stdout, completed.stdout
