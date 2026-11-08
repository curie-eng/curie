"""Exercise admitted startup observations without provisioning a cluster."""

from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

sys.path.insert(0, str(Path(__file__).resolve().parent))

import pytest  # noqa: E402
import test_c4_admitted_agent as admitted  # noqa: E402
from c4_admitted_kind import AdmittedKind, Agent, Ticket  # noqa: E402


class Observations:
    def __init__(self, *rows: dict[str, Any]) -> None:
        self.rows = rows
        self.reads = 0

    def request(self, ticket: Ticket) -> dict[str, Any]:
        row = self.rows[min(self.reads, len(self.rows) - 1)]
        self.reads += 1
        return row

    def worker_logs(self) -> str:
        return ""


@pytest.fixture
def clock(monkeypatch: pytest.MonkeyPatch) -> Any:
    clock = SimpleNamespace(now=0.0)

    def sleep(seconds: float) -> None:
        clock.now += seconds

    monkeypatch.setattr(
        admitted,
        "time",
        SimpleNamespace(
            monotonic=lambda: clock.now,
            sleep=sleep,
        ),
    )
    return clock


@pytest.fixture
def ticket() -> Ticket:
    agent = Agent("acme-agent", "acme-bot", "curie", "marker", "deployment", "bundle")
    return Ticket("gitlab", 1, "https://example.com/issue/1", agent)


def request_row(**changes: Any) -> dict[str, Any]:
    return {
        "id": "00000000-0000-4000-8000-000000000001",
        "agent_id": "acme-agent",
        "status": "waiting",
        "started_at": None,
        "last_deferral_reason": "acquire_lost",
        "dispatch_generation": 2,
        "execution_attempts": 0,
        "runtime_epoch": 0,
        **changes,
    }


def test_lapsed_acquisition_waits_for_redispatched_start(clock: Any, ticket: Ticket) -> None:
    started = request_row(
        status="running",
        started_at="2026-11-02T00:00:01Z",
        execution_attempts=1,
        runtime_epoch=1,
    )
    run = Observations(request_row(), started)
    assert admitted.assert_started(cast(AdmittedKind, run), ticket) == started
    assert run.reads == 2
    assert clock.now == 1


def test_lapsed_acquisition_without_start_still_expires(clock: Any, ticket: Ticket) -> None:
    run = Observations(request_row())
    with pytest.raises(AssertionError, match="produced no execution disposition"):
        admitted.assert_started(cast(AdmittedKind, run), ticket)
    assert clock.now == 180


@pytest.mark.parametrize(
    "changes",
    [{"status": "expired"}, {"last_deferral_reason": "not_started:factory_binding_missing"}],
    ids=["terminal-acquire-lost", "authority-refusal"],
)
def test_other_dispositions_still_fail_immediately(
    clock: Any,
    ticket: Ticket,
    changes: dict[str, Any],
) -> None:
    run = Observations(request_row(**changes))
    with pytest.raises(AssertionError, match="labelled request did not execute"):
        admitted.assert_started(cast(AdmittedKind, run), ticket)
    assert run.reads == 1
    assert clock.now == 0


@pytest.mark.parametrize(
    "changes",
    [{"agent_id": "other-agent"}, {"execution_attempts": 2}, {"runtime_epoch": 0}],
    ids=["wrong-agent", "duplicate-execution", "missing-epoch"],
)
def test_redispatched_start_keeps_execution_guards(
    clock: Any,
    ticket: Ticket,
    changes: dict[str, Any],
) -> None:
    started = (
        request_row(
            status="running",
            started_at="2026-11-02T00:00:01Z",
            execution_attempts=1,
            runtime_epoch=1,
        )
        | changes
    )
    run = Observations(request_row(), started)
    with pytest.raises(AssertionError):
        admitted.assert_started(cast(AdmittedKind, run), ticket)
    assert run.reads == 2
    assert clock.now == 1
