from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from curie_api import factory_ci
from curie_api.forges.github.ci import CiDetail
from curie_api.forges.github.code_host import normalize_checks
from curie_api.forges.types import CheckSource, CheckState, NormalizedCheck

HEAD = "a1" * 20
OTHER_HEAD = "b2" * 20
CHECK = "schema-check"
EARLIER = "2026-01-01T12:00:00Z"
LATER = "2026-01-01T12:00:01Z"
PUBLISHED = datetime(2026, 1, 1, 12, tzinfo=UTC)


# GitHub exposes id, head_sha, started_at and check_suite on each check run:
# https://docs.github.com/en/rest/checks/runs#list-check-runs-for-a-git-reference
def _run(conclusion: str | None, **fields: Any) -> dict[str, Any]:
    run: dict[str, Any] = {
        "id": 10,
        "name": CHECK,
        "status": "completed" if conclusion is not None else "in_progress",
        "conclusion": conclusion,
        "head_sha": HEAD,
        "started_at": LATER,
        "check_suite": {"id": 202},
        "app": {"slug": "github-actions"},
        "details_url": "https://github.com/acme-corp/acme-bot/actions/runs/202",
    }
    run.update(fields)
    return run


def _checks(
    *runs: dict[str, Any], statuses: tuple[dict[str, Any], ...] = ()
) -> tuple[NormalizedCheck, ...]:
    return normalize_checks(
        CiDetail(
            state="observed",
            reason=None,
            head_sha=HEAD,
            check_runs=list(runs),
            statuses=list(statuses),
        ),
        HEAD,
    )


def _decide(checks: tuple[NormalizedCheck, ...]) -> factory_ci.Verdict:
    return factory_ci.decide(
        factory_ci.CiView(HEAD, checks),
        now=PUBLISHED + timedelta(seconds=30),
        published_at=PUBLISHED,
        execution_deadline=PUBLISHED + timedelta(seconds=1800),
        ci_wait_seconds=1200,
        changed_paths=["README.md"],
        python_ci=None,
        metadata_ci=None,
    )


@pytest.mark.parametrize("reverse", [False, True], ids=["old-first", "new-first"])
@pytest.mark.parametrize(
    ("cancelled_fields", "success_fields"),
    [
        ({"id": 9, "started_at": EARLIER}, {"id": 10, "started_at": LATER}),
        ({"id": 9, "started_at": LATER}, {"id": 10, "started_at": LATER}),
        ({"id": 10, "started_at": EARLIER}, {"id": 9, "started_at": LATER}),
    ],
    ids=["later-time", "numeric-id-tiebreak", "time-before-id"],
)
def test_superseded_cancelled_run_does_not_fail_ci(
    reverse: bool, cancelled_fields: dict[str, Any], success_fields: dict[str, Any]
) -> None:
    runs = [
        _run("cancelled", check_suite={"id": 101}, **cancelled_fields),
        _run("success", **success_fields),
    ]
    if reverse:
        runs.reverse()

    checks = _checks(*runs)

    assert [(check.check_id, check.state) for check in checks] == [
        (str(success_fields["id"]), CheckState.SUCCESS)
    ]
    assert _decide(checks).kind == "green"


@pytest.mark.parametrize("missing_side", [0, 1, None], ids=["old", "new", "both"])
def test_a_later_timestamp_proves_supersession_without_ids(missing_side: int | None) -> None:
    runs = [_run("cancelled", id=9, started_at=EARLIER), _run("success")]
    for index in range(2):
        if missing_side is None or missing_side == index:
            del runs[index]["id"]

    checks = _checks(*runs)

    assert [check.state for check in checks] == [CheckState.SUCCESS]
    assert _decide(checks).kind == "green"


@pytest.mark.parametrize(
    ("cancelled_fields", "success_fields"),
    [
        ({"started_at": LATER}, {"started_at": EARLIER}),
        ({"id": 10}, {"id": 9}),
        ({"id": 10}, {"id": 10}),
        ({"started_at": None}, {}),
        ({}, {"started_at": None}),
        ({"started_at": None}, {"started_at": None}),
        ({"started_at": "invalid"}, {}),
        ({}, {"started_at": "invalid"}),
        ({"started_at": "2026-01-01T12:00:00"}, {}),
        ({"started_at": 1}, {}),
        ({}, {"started_at": False}),
        ({"id": None}, {}),
        ({"id": 9}, {"id": None}),
        ({"id": "9"}, {}),
        ({"id": 9}, {"id": "10"}),
        ({"id": True}, {}),
        ({"id": 9}, {"id": 0}),
        ({"id": -1}, {}),
    ],
    ids=[
        "newest-cancelled",
        "greater-cancelled-id",
        "equal-id",
        "null-old-time",
        "null-new-time",
        "null-both-times",
        "invalid-old-time",
        "invalid-new-time",
        "naive-time",
        "numeric-time",
        "boolean-time",
        "null-old-id",
        "null-new-id",
        "string-old-id",
        "string-new-id",
        "boolean-id",
        "zero-id",
        "negative-id",
    ],
)
def test_a_cancellation_without_proven_supersession_still_fails(
    cancelled_fields: dict[str, Any], success_fields: dict[str, Any]
) -> None:
    checks = _checks(_run("cancelled", **cancelled_fields), _run("success", **success_fields))

    assert [check.state for check in checks] == [CheckState.CANCELLED, CheckState.SUCCESS]
    assert _decide(checks).kind == "failing"


@pytest.mark.parametrize("field", ["started_at", "id"])
@pytest.mark.parametrize("missing_side", [0, 1])
def test_an_absent_ordering_field_does_not_prove_supersession(
    field: str, missing_side: int
) -> None:
    runs = [_run("cancelled", id=9), _run("success")]
    del runs[missing_side][field]

    checks = _checks(*runs)

    assert [check.state for check in checks] == [CheckState.CANCELLED, CheckState.SUCCESS]
    assert _decide(checks).kind == "failing"


@pytest.mark.parametrize("newer_fields", [{"name": "other-check"}, {"head_sha": OTHER_HEAD}])
def test_a_later_run_on_another_key_or_head_does_not_supersede_a_cancellation(
    newer_fields: dict[str, Any],
) -> None:
    checks = _checks(_run("cancelled", id=9, started_at=EARLIER), _run("success", **newer_fields))

    assert [check.state for check in checks] == [CheckState.CANCELLED, CheckState.SUCCESS]
    assert _decide(checks).kind == "failing"


@pytest.mark.parametrize(
    ("conclusion", "state", "verdict"),
    [(None, CheckState.PENDING, "pending"), ("failure", CheckState.FAILURE, "failing")],
)
def test_a_later_pending_or_failing_run_keeps_its_own_verdict(
    conclusion: str | None, state: CheckState, verdict: str
) -> None:
    checks = _checks(_run("cancelled", id=9, started_at=EARLIER), _run(conclusion))

    assert [check.state for check in checks] == [state]
    assert _decide(checks).kind == verdict


def test_an_older_failure_is_retained_after_same_named_success() -> None:
    checks = _checks(_run("failure", id=9, started_at=EARLIER), _run("success"))

    assert [check.state for check in checks] == [CheckState.FAILURE, CheckState.SUCCESS]
    assert _decide(checks).kind == "failing"


def test_commit_statuses_are_retained_and_do_not_supersede_a_cancelled_run() -> None:
    # https://docs.github.com/en/rest/commits/statuses#get-the-combined-status-for-a-specific-reference
    statuses = (
        {"id": 11, "context": CHECK, "state": "failure", "created_at": EARLIER},
        {"id": 12, "context": CHECK, "state": "success", "created_at": LATER},
        {"id": 13, "context": "other-status", "state": "pending", "created_at": LATER},
    )
    checks = _checks(_run("cancelled", id=9, started_at=EARLIER), statuses=statuses)

    assert [(check.source, check.key, check.state) for check in checks] == [
        (CheckSource.RUN, CHECK, CheckState.CANCELLED),
        (CheckSource.STATUS, f"status:{CHECK}", CheckState.FAILURE),
        (CheckSource.STATUS, f"status:{CHECK}", CheckState.SUCCESS),
        (CheckSource.STATUS, "status:other-status", CheckState.PENDING),
    ]
    assert _decide(checks).kind == "failing"
