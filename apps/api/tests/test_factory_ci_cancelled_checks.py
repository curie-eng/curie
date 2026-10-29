# ruff: noqa: F811

from __future__ import annotations

import uuid
from datetime import timedelta
from pathlib import Path
from typing import Any

import pytest
from forge_fakes.github_comments import (  # noqa: F401
    HEAD_A,
    _CommentServer,
    _rows,
    admitted,
    check_run,
    ci_entry,
    comments,
)
from test_factory_ci_wait import (
    _attach_fix,
    _body,
    _ci_turns,
    _database_now,
    _published,
    _reconcile,
    _reconcile_later,
    _set_base_ref,
    _set_metadata_ci,
    _terminal,
)

pytestmark = pytest.mark.usefixtures("clean_db")

CHECK = "schema-check"
EARLIER = "2026-01-01T12:00:00Z"
LATER = "2026-01-01T12:00:01Z"


# https://docs.github.com/en/rest/checks/runs#list-check-runs-for-a-git-reference
def _run(conclusion: str, *, run_id: int, started_at: str, suite_id: int) -> dict[str, Any]:
    return {
        **check_run(CHECK, conclusion=conclusion, run_id=run_id, started_at=started_at),
        "head_sha": HEAD_A,
        "check_suite": {"id": suite_id},
        "app": {"slug": "github-actions"},
        "details_url": f"https://github.com/acme-corp/acme-bot/actions/runs/{suite_id}",
    }


def _require_checks(sink: _CommentServer) -> None:
    sink.protections["main"] = {"required_status_checks": {"contexts": [CHECK, "lint"]}}


def _publications(request_id: uuid.UUID) -> list[dict[str, Any]]:
    return _rows(
        "SELECT revision_number, changed_paths, metadata_updated_at "
        "FROM curie.publications WHERE execution_request_id = :request "
        "ORDER BY revision_number",
        {"request": request_id},
    )


@pytest.mark.parametrize("reverse", [False, True], ids=["old-first", "new-first"])
def test_superseded_cancellation_completes_without_a_repair_or_metadata_revision(
    admitted: Any, reverse: bool
) -> None:
    client, github, sink = admitted
    number = 9810
    runs = [
        _run("cancelled", run_id=9, started_at=EARLIER, suite_id=101),
        _run("success", run_id=10, started_at=LATER, suite_id=202),
    ]
    if reverse:
        runs.reverse()
    sink.ci_script = [ci_entry(*runs, check_run("lint"))]
    _require_checks(sink)
    published = _published(client, github, sink, number)
    _set_base_ref(published["work_item_id"], "main")

    _reconcile()

    assert _terminal(number) == ("completed", "completed")
    assert _ci_turns(published["id"]) == []
    assert _publications(published["id"]) == [
        {"revision_number": 1, "changed_paths": ["README.md"], "metadata_updated_at": None}
    ]
    assert sink.ci_observations == [HEAD_A]
    assert sink.reruns == []
    assert _body(sink, published["id"]).startswith(f"Completed: {published['pr_url']}")


@pytest.mark.parametrize("failure", ["newest-cancelled", "older-failure"])
def test_an_unsuperseded_failure_still_dispatches_a_repair(admitted: Any, failure: str) -> None:
    client, github, sink = admitted
    number = 9811
    runs = (
        [
            _run("success", run_id=9, started_at=EARLIER, suite_id=101),
            _run("cancelled", run_id=10, started_at=LATER, suite_id=202),
        ]
        if failure == "newest-cancelled"
        else [
            _run("failure", run_id=9, started_at=EARLIER, suite_id=101),
            _run("success", run_id=10, started_at=LATER, suite_id=202),
        ]
    )
    sink.ci_script = [ci_entry(*runs, check_run("lint"))]
    _require_checks(sink)
    published = _published(client, github, sink, number)
    _set_base_ref(published["work_item_id"], "main")

    _reconcile()

    assert _terminal(number) == ("running", None)
    turns = _ci_turns(published["id"])
    assert [turn["event_id"] for turn in turns] == [f"work-item-{published['id']}-ci-2"]
    assert CHECK in turns[0]["text"]
    assert len(_publications(published["id"])) == 1


@pytest.mark.parametrize("fresh", [False, True], ids=["not-rerun", "fresh-success"])
def test_supersession_preserves_metadata_revision_freshness(
    admitted: Any, monkeypatch: pytest.MonkeyPatch, tmp_path: Path, fresh: bool
) -> None:
    client, github, sink = admitted
    number = 9812
    _set_metadata_ci(monkeypatch, tmp_path / "metadata-ci", [CHECK])
    published = _published(client, github, sink, number)
    _set_base_ref(published["work_item_id"], "main")
    _require_checks(sink)
    old = _database_now() - timedelta(minutes=2)
    _attach_fix(
        published["work_item_id"],
        published["id"],
        revision=2,
        head_sha=HEAD_A,
        title="Update the pull request description",
        paths=[],
        base_sha=HEAD_A,
    )
    metadata_at = _publications(published["id"])[1]["metadata_updated_at"]
    later = metadata_at + timedelta(seconds=1) if fresh else old + timedelta(seconds=1)
    sink.ci_script = [
        ci_entry(
            _run("cancelled", run_id=9, started_at=old.isoformat(), suite_id=101),
            _run("success", run_id=10, started_at=later.isoformat(), suite_id=202),
            check_run("lint", started_at=old.isoformat()),
        )
    ]

    _reconcile()

    assert _ci_turns(published["id"]) == []
    if fresh:
        assert _terminal(number) == ("completed", "completed")
        assert _body(sink, published["id"]).startswith("Completed:")
    else:
        assert _terminal(number) == ("running", None)

        _reconcile_later(3700)

        assert _terminal(number) == ("failed", "ci_unverified")
        assert "Reason: checks_not_rerun" in _body(sink, published["id"])
        assert _ci_turns(published["id"]) == []
