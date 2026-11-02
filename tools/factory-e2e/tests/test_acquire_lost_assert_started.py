"""Unit coverage for the waiting acquire_lost start skip.

These cases stay outside the admitted kind runtime module. That module is
split into contiguous collection-order shards, and extra cases there move
the successor regression away from the continuation cases ahead of it.
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))

from test_c4_admitted_agent import assert_started  # noqa: E402


def _acquire_lost_run(status: str) -> tuple[Any, Any]:
    row = {
        "id": "00000000-0000-4000-8000-000000000001",
        "agent_id": "acme-agent",
        "status": status,
        "started_at": None,
        "last_deferral_reason": "acquire_lost",
        "execution_attempts": 0,
        "runtime_epoch": 0,
    }

    class Run:
        def request(self, _ticket: object) -> dict[str, Any]:
            return row

        def worker_logs(self) -> str:
            return ""

    return Run(), type("Ticket", (), {"kind": "gitlab"})()


def test_waiting_acquire_lost_skips_the_start_assertion() -> None:
    run, ticket = _acquire_lost_run("waiting")
    with pytest.raises(pytest.skip.Exception):
        assert_started(run, ticket)


def test_expired_acquire_lost_still_fails_assert_started() -> None:
    run, ticket = _acquire_lost_run("expired")
    with pytest.raises(AssertionError, match="labelled request did not execute"):
        assert_started(run, ticket)
