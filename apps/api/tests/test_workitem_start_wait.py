"""Real worker-produced start waits through HTTP and Postgres (#5155)."""

from __future__ import annotations

from datetime import timedelta

import pytest
from curie_api.config import get_settings
from curie_api.workitems import lifecycle
from curie_worker.workitem_dispatch import WorkItemConflict

from apps.worker.tests.start_wait_fixtures import (
    StartWaitCase,
    assert_uncounted_wait,
    start_wait,
    start_wait_database,
    timeout_lookalike,
)

__all__ = ["start_wait", "start_wait_database"]


@pytest.mark.parametrize("start_wait", ["upload"], indirect=True)
def test_six_worker_timeouts_keep_waiting_until_the_original_deadline(
    start_wait: StartWaitCase,
) -> None:
    case = start_wait

    async def exercise() -> None:
        for delivery in range(1, 7):
            await case.deliver()
            row = await case.state()
            assert_uncounted_wait(case, row)
            assert row["dispatch_generation"] == case.initial["dispatch_generation"] + delivery
            assert row["dispatch_not_before"] == case.now + timedelta(
                seconds=get_settings().work_item_backoff_base_seconds
            )
            assert case.payloads[-1]["capacity"] is False
            assert not case.payloads[-1]["reason"].startswith("not_started:")
            diagnostic = case.payloads[-1]["start_failure_detail"]
            assert isinstance(diagnostic, str) and diagnostic
            assert row["last_start_failure_detail"] == diagnostic
            summary = await case.http.get(
                f"/work-items/{row['work_item_id']}",
                headers={"X-API-Key": "example-api-key"},
            )
            assert summary.status_code == 200, summary.text
            assert summary.json()["state"] == "waiting"
            assert diagnostic in summary.json()["actionable_cause"]
        generation = await case.publish()
        case.now = case.initial["wait_deadline"]
        with pytest.raises(WorkItemConflict, match="waiting_deadline_elapsed"):
            await case.dispatch.acquire(
                case.request_id,
                owner=case.h.config.consumer_name,
                generation=generation,
                wake=case.wake,
                context=None,
            )
        before = await case.state()
        async with case.sessions() as session:
            expired = await lifecycle.expire_waiting(
                session,
                work_item_id=before["work_item_id"],
                request_id=case.request_id,
                expected_work_item_version=before["work_item_version"],
                expected_request_version=before["version"],
            )
            assert isinstance(expired, lifecycle.WorkItemOutcome)
            await session.commit()
        final = await case.state()
        assert final["status"] == "expired"
        assert final["terminal_cause"] == "capacity_wait_expired"
        assert final["wait_deadline"] == case.initial["wait_deadline"]
        assert final["start_deferrals"] == final["capacity_deferrals"] == 0
        assert final["execution_attempts"] == 0
        assert final["last_start_failure_detail"] == diagnostic
        summary = await case.http.get(
            f"/work-items/{final['work_item_id']}",
            headers={"X-API-Key": "example-api-key"},
        )
        assert summary.status_code == 200, summary.text
        assert summary.json()["state"] == "expired"
        assert diagnostic in summary.json()["actionable_cause"]
        assert case.h.runner.opened == []

    case.run(exercise())


@pytest.mark.parametrize(
    "start_wait", ["bad-ref", "auth", "archive-size", "lookalike"], indirect=True
)
def test_permanent_worker_workspace_faults_still_fail_at_five_starts(
    start_wait: StartWaitCase,
) -> None:
    case = start_wait

    async def exercise() -> None:
        assert get_settings().work_item_start_deferral_limit == 5
        for attempt in range(1, 6):
            await case.deliver()
            row = await case.state()
            assert row["start_deferrals"] == attempt
            assert row["capacity_deferrals"] == 0
            assert row["execution_attempts"] == 0
            assert row["started_at"] is None and row["execution_deadline"] is None
            assert row["wait_deadline"] == case.initial["wait_deadline"]
            assert row["status"] == ("failed" if attempt == 5 else "waiting")
            assert case.payloads[-1]["capacity"] is False
            assert case.payloads[-1]["reason"].startswith("not_started:")
            if case.fault == "lookalike":
                assert case.payloads[-1]["start_failure_detail"] == timeout_lookalike()
        assert row["terminal_cause"] == "start_failed"
        assert case.h.runner.opened == []
        assert case.keys() == []

    case.run(exercise())
