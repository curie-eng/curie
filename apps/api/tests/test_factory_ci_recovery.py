"""ADR 0269 read-only CI recovery through authenticated API and reconciler.

The HTTP forge fixture replays GitHub REST shapes documented in
forge_fakes.github_comments. Postgres, Valkey and production wiring are real.
"""

# ruff: noqa: F811

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
from curie_api.config import get_settings
from forge_fakes.github import LABEL, REPO, REPO_ID
from forge_fakes.github_comments import (
    HEAD_A,
    HEAD_B,
    _rows,
    admitted,  # noqa: F401
    check_run,
    ci_empty,
    ci_entry,
    ci_green,
    ci_pending,
    comments,  # noqa: F401
    commit_status,
)
from test_factory_ci_wait import (
    _attach_fix,
    _body,
    _ci_turns,
    _clock_offset,
    _epoch,
    _execute,
    _finish,
    _published,
    _racing_pass,
    _reconcile,
    _set_base_ref,
    _set_metadata_ci,
    _stream_turns,
    _terminal,
)

pytestmark = pytest.mark.usefixtures("clean_db")


def _observation(request_id: Any) -> dict[str, Any]:
    rows = _rows(
        "SELECT * FROM curie.factory_ci_observations WHERE request_id = :id", {"id": request_id}
    )
    assert len(rows) == 1
    return rows[0]


def _snapshot(request_id: Any) -> str:
    return str(
        _rows(
            "SELECT row_to_json(r)::text AS bytes FROM curie.execution_requests r WHERE id = :id",
            {"id": request_id},
        )[0]["bytes"]
    )


def _view(client: Any, item_id: Any) -> dict[str, Any]:
    response = client.get(f"/work-items/{item_id}", headers={"X-API-Key": get_settings().api_key})
    assert response.status_code == 200, response.text
    return response.json()


def _failed(admitted: Any, number: int, cause: str = "ci_timeout") -> dict[str, Any]:
    client, github, sink = admitted
    sink.ci_script = [ci_entry(check_status=502 if cause == "ci_timeout" else 403)]
    published = _published(client, github, sink, number)
    with _clock_offset(3700):
        _reconcile()
    assert _terminal(number) == ("failed", cause)
    assert _observation(published["id"])["status"] == "observing"
    return published


def _later(offset: int = 3730) -> None:
    with _clock_offset(offset):
        _reconcile()


@pytest.mark.parametrize("cause", ["ci_timeout", "ci_unverified"])
def test_later_green_records_item_completion_without_changing_failed_request(
    admitted: Any,
    cause: str,
) -> None:
    client, _github, sink = admitted
    number = 9810
    published = _failed(admitted, number, cause)
    request_id = published["id"]
    before = _snapshot(request_id)
    original = _observation(request_id)
    writes_before = len(sink.requests)
    _later(3705)
    assert [r for r in sink.requests[writes_before:] if r[0] != "GET"] == []
    assert _observation(request_id) == original
    turns = _stream_turns()
    publications = _rows("SELECT id FROM curie.publications")
    watching = _body(sink, request_id)
    assert "Could not complete" in watching
    assert "Checks are still being watched until" in watching
    assert "curie-factory:needs-human" in sink.issue_labels[number]
    assert _view(client, published["work_item_id"])["state"] == "failed"
    assert client.get(f"/work-items/{published['work_item_id']}").status_code == 401

    prior_writes = len(sink.requests)
    _later()
    assert [r for r in sink.requests[prior_writes:] if r[0] != "GET"] == []
    sink.ci_script = [ci_green()]
    # Every helper invocation builds a new engine, Redis client and reconciler.
    _later(3760)
    result = _view(client, published["work_item_id"])
    assert result["state"] == "published", (
        _observation(request_id)["status"],
        _observation(request_id)["reason"],
    )
    assert result["actionable_cause"] == "ci_observed_after_run"
    assert result["ci_observation"]["status"] == "completed"
    assert result["ci_observation"]["head_sha"] == HEAD_A
    assert result["ci_observation"]["observed_at"] is not None
    assert result["pr"]["status"] == "open"
    assert result["requests"][-1]["status"] == "failed"
    assert _snapshot(request_id) == before
    assert _observation(request_id)["expires_at"] == original["expires_at"]
    assert _stream_turns() == turns
    assert _rows("SELECT id FROM curie.publications") == publications
    assert _rows("SELECT id FROM curie.execution_request_model_usage") == []
    assert _ci_turns(request_id) == [] and sink.reruns == []
    body = _body(sink, request_id)
    assert "Could not complete" in body and "after the run ended" in body
    assert HEAD_A in body and "Checks are still being watched" not in body
    assert sink.issue_labels[number] == {LABEL, "curie-factory:pr-open"} or (
        sink.issue_labels[number] == {"curie-factory:pr-open"}
    )
    writes = list(sink.requests)
    _later(3790)
    assert _snapshot(request_id) == before
    assert _body(sink, request_id) == body
    assert [r for r in sink.requests[len(writes) :] if r[0] != "GET"] == []


def test_expiry_is_finite_and_restart_cannot_extend_or_recover_it(admitted: Any) -> None:
    client, _github, sink = admitted
    published = _failed(admitted, 9811)
    original = _observation(published["id"])
    before = _snapshot(published["id"])
    _later(3700 + 86401)
    expired = _observation(published["id"])
    assert expired["status"] == "expired"
    assert expired["expires_at"] == original["expires_at"]
    assert (expired["expires_at"] - original["created_at"]).total_seconds() == 86400
    assert "Checks were not verified within 24 hours" in _body(sink, published["id"])
    assert "Checks are still being watched" not in _body(sink, published["id"])
    assert "curie-factory:needs-human" in sink.issue_labels[9811]
    sink.ci_script = [ci_green()]
    reads = len(sink.ci_observations)
    writes = len(sink.requests)
    _later(3700 + 86500)
    assert _observation(published["id"]) == expired
    assert [r for r in sink.requests[writes:] if r[0] != "GET"] == []
    assert _view(client, published["work_item_id"])["state"] == "failed"
    assert len(sink.ci_observations) == reads + 1  # the API detail's independent live CI read
    assert _snapshot(published["id"]) == before


@pytest.mark.parametrize(
    "control",
    [
        "pending",
        "failure",
        "cancelled",
        "empty",
        "foreign",
        "closed",
        "tracker",
        "deleted",
        "unlabelled",
        "unreadable_tracker",
        "incomplete_listing",
    ],
)
def test_negative_controls_do_not_complete_the_item(admitted: Any, control: str) -> None:
    client, _github, sink = admitted
    published = _failed(admitted, 9812)
    if control in {"pending", "failure", "cancelled"}:
        older = check_run("build", run_id=10, started_at="2026-01-01T00:00:00Z")
        newer = check_run(
            "build",
            run_id=11,
            started_at="2026-01-01T00:01:00Z",
            status="in_progress" if control == "pending" else "completed",
            conclusion="failure" if control == "failure" else "cancelled",
        )
        sink.ci_script = [ci_entry(older, newer)]
    elif control == "empty":
        sink.ci_script = [ci_empty()]
    elif control == "incomplete_listing":
        sink.ci_script = [ci_entry(check_run("build"), total=2)]
    elif control == "foreign":
        sink.ci_script = [ci_green()]
        sink.pull_script = [
            {
                "head": {
                    "sha": HEAD_B,
                    "ref": "curie/update",
                    "repo": {"id": REPO_ID, "full_name": REPO},
                }
            }
        ]
    elif control == "closed":
        sink.ci_script = [ci_green()]
        sink.pull_script = [{"state": "closed", "merged": False}]
    elif control == "deleted":
        sink.ci_script = [ci_green()]
        sink.issue_statuses[9812] = 404
    elif control == "unlabelled":
        sink.ci_script = [ci_green()]
        sink.issue_overrides[9812] = {"labels": []}
    elif control == "unreadable_tracker":
        sink.ci_script = [ci_green()]
        sink.issue_overrides[9812] = {"state": None}
    else:
        sink.ci_script = [ci_green()]
        sink.issue_overrides[9812] = {"state": "closed"}
    before = _snapshot(published["id"])
    _later()
    assert _observation(published["id"])["status"] != "completed"
    assert _view(client, published["work_item_id"])["state"] == "failed"
    assert _snapshot(published["id"]) == before
    assert "curie-factory:needs-human" in sink.issue_labels[9812]


@pytest.mark.parametrize("conclusion", ["failure", "cancelled"])
def test_superseded_runs_and_paginated_checks_can_prove_later_green(
    admitted: Any,
    conclusion: str,
) -> None:
    client, _github, sink = admitted
    published = _failed(admitted, 9813)
    older = check_run("build", conclusion=conclusion, run_id=10, started_at="2026-01-01T00:00:00Z")
    newer = check_run("build", run_id=11, started_at="2026-01-01T00:01:00Z")
    older["app"] = newer["app"] = {"id": 1, "slug": "acme-ci"}
    sink.ci_script = [ci_entry(older, *[check_run(f"check-{i}") for i in range(100)], newer)]
    _later()
    assert (HEAD_A, "check-runs", 2) in sink.ci_pages
    assert _view(client, published["work_item_id"])["state"] == "published"
    assert _observation(published["id"])["status"] == "completed"


@pytest.mark.parametrize(
    "application_ids", [(1, 2), (None, None)], ids=["different_apps", "unknown_apps"]
)
def test_distinct_app_failure_is_not_superseded_by_another_apps_success(
    admitted: Any,
    application_ids: tuple[int | None, int | None],
) -> None:
    _client, _github, sink = admitted
    published = _failed(admitted, 9814)
    # GitHub check-run app is nullable in its published REST OpenAPI schema:
    # https://docs.github.com/en/rest/checks/runs#get-a-check-run
    failed = {
        **check_run("build", conclusion="failure", started_at="2026-01-01T00:00:00Z"),
        "app": {"id": application_ids[0]} if application_ids[0] is not None else None,
    }
    passing = {
        **check_run("build", started_at="2026-01-01T00:01:00Z"),
        "app": {"id": application_ids[1]} if application_ids[1] is not None else None,
    }
    sink.ci_script = [ci_entry(failed, passing)]
    _later()
    assert _observation(published["id"])["status"] == "observing"


@pytest.mark.parametrize("status", ["in_progress", "completed"], ids=["running", "failed"])
def test_an_independent_workflow_cannot_supply_recovery_proof(admitted: Any, status: str) -> None:
    _client, _github, sink = admitted
    published = _failed(admitted, 9814)
    before = _snapshot(published["id"])
    earlier = check_run(
        "build", status=status, conclusion="failure", started_at="2026-01-01T00:00:00Z"
    )
    earlier["check_suite"] = {"id": 101}
    later = check_run("build", started_at="2026-01-01T00:01:00Z")
    earlier["app"] = later["app"] = {"id": 1, "slug": "github-actions"}
    sink.ci_script = [ci_entry(earlier, later)]

    _later()

    assert _observation(published["id"])["status"] == "observing"
    assert _snapshot(published["id"]) == before


def test_a_paginated_snapshot_change_can_recover_after_a_stable_read(admitted: Any) -> None:
    _client, _github, sink = admitted
    published = _failed(admitted, 9814)
    sink.ci_script = [ci_entry(*[check_run(f"check-{index}") for index in range(101)])]
    sink.ci_page_totals[(HEAD_A, "check-runs", 2)] = 102

    _later()

    assert _observation(published["id"])["status"] == "observing"
    assert _observation(published["id"])["reason"] == "listing_incomplete"
    sink.ci_page_totals.clear()
    _later(3760)
    assert _observation(published["id"])["status"] == "completed"


def test_newest_status_is_required_and_old_failure_can_be_superseded(admitted: Any) -> None:
    _client, _github, sink = admitted
    published = _failed(admitted, 9815)
    old = {**commit_status("external", "success"), "id": 10, "created_at": "2026-01-01T00:00:00Z"}
    new = {**commit_status("external", "pending"), "id": 11, "created_at": "2026-01-01T00:01:00Z"}
    sink.ci_script = [ci_entry(statuses=(old, new))]
    _later()
    assert _observation(published["id"])["status"] == "observing"
    newest = {**new, "state": "success", "id": 12, "created_at": "2026-01-01T00:02:00Z"}
    sink.ci_script = [ci_entry(statuses=({**old, "state": "failure"}, new, newest))]
    _later(3760)
    assert _observation(published["id"])["status"] == "completed"


def test_duplicate_reconcilers_claim_one_durable_observation(admitted: Any) -> None:
    _client, _github, sink = admitted
    published = _failed(admitted, 9816)
    before = _snapshot(published["id"])
    sink.ci_script = [ci_green()]
    with _clock_offset(3730):
        _racing_pass(2)
    assert _observation(published["id"])["status"] == "completed"
    assert _snapshot(published["id"]) == before


def test_authorized_head_needs_its_own_proof_within_the_original_window(admitted: Any) -> None:
    _client, _github, sink = admitted
    published = _failed(admitted, 9817)
    original = _observation(published["id"])
    _attach_fix(
        published["work_item_id"],
        published["id"],
        revision=2,
        head_sha=HEAD_B,
        title="Update documentation",
        paths=["README.md"],
    )
    sink.pull_script = [
        {"head": {"sha": HEAD_B, "ref": "curie/update", "repo": {"id": REPO_ID, "full_name": REPO}}}
    ]
    sink.ci_scripts = {HEAD_A: [ci_green()], HEAD_B: [ci_pending()]}
    _later()
    pending = _observation(published["id"])
    assert pending["status"] == "observing" and pending["head_sha"] == HEAD_B
    assert pending["expires_at"] == original["expires_at"]
    sink.ci_scripts[HEAD_B] = [ci_green()]
    _later(3760)
    assert _observation(published["id"])["status"] == "completed"
    assert _observation(published["id"])["head_sha"] == HEAD_B


def test_cancellation_during_green_read_cannot_commit_completion(admitted: Any) -> None:
    _client, _github, sink = admitted
    published = _failed(admitted, 9818)
    sink.ci_script = [ci_green()]

    def cancel() -> None:
        sink.on_ci_read = None
        _execute(
            "UPDATE curie.work_items SET cancelled_at = clock_timestamp(), version = version + 1 "
            "WHERE id = :id",
            {"id": published["work_item_id"]},
        )

    sink.on_ci_read = cancel
    before = _snapshot(published["id"])
    _later()
    assert _observation(published["id"])["status"] == "invalidated"
    assert _snapshot(published["id"]) == before


@pytest.mark.parametrize(
    "cause", ["ci_failed", "ci_fix_unpublished", "merge_conflict", "pr_closed"]
)
def test_other_terminal_causes_never_start_recovery(admitted: Any, cause: str) -> None:
    client, github, sink = admitted
    published = _published(client, github, sink, 9819)
    response = _finish(
        client,
        published["id"],
        _epoch(published["id"]),
        cause,
        3 if cause == "ci_fix_unpublished" else None,
    )
    assert response.status_code == 200, response.text
    before = _snapshot(published["id"])
    sink.ci_script = [ci_green()]
    _reconcile()
    assert _rows("SELECT request_id FROM curie.factory_ci_observations") == []
    assert _snapshot(published["id"]) == before
    assert _view(client, published["work_item_id"])["state"] == "failed"


def test_required_trigger_and_permission_stay_explicit_until_proved_repaired(admitted: Any) -> None:
    _client, _github, sink = admitted
    published = _failed(admitted, 9820, "ci_unverified")
    _set_base_ref(published["work_item_id"], "main")
    _later()
    assert _observation(published["id"])["reason"] == "github_forbidden_list_check_runs"
    sink.ci_script = [ci_green()]
    sink.rules["main"] = (
        200,
        [
            {
                "type": "required_status_checks",
                "parameters": {"required_status_checks": [{"context": "label-gate"}]},
            }
        ],
    )
    _later(3760)
    assert _observation(published["id"])["status"] == "observing"
    assert "required_check_absent" in _observation(published["id"])["reason"]
    assert "Checks are still being watched until" in _body(sink, published["id"])
    sink.ci_script = [ci_entry(check_run("build"), check_run("label-gate"))]
    _later(3790)
    assert _observation(published["id"])["status"] == "completed"


def test_stale_metadata_cannot_recover_until_fresh_checks_exist(
    admitted: Any, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _client, _github, sink = admitted
    published = _failed(admitted, 9821)
    _attach_fix(
        published["work_item_id"],
        published["id"],
        revision=2,
        head_sha=HEAD_A,
        base_sha=HEAD_A,
        title="Update pull request description",
        paths=[],
    )
    sink.ci_script = [ci_green()]
    _later()
    assert _observation(published["id"])["status"] == "observing"
    assert _observation(published["id"])["reason"] == "metadata_ci_not_configured"
    _set_metadata_ci(monkeypatch, tmp_path / "metadata", ["body-guard"])
    sink.ci_script = [ci_entry(check_run("body-guard", started_at="2026-01-01T00:00:00Z"))]
    _later(3760)
    assert _observation(published["id"])["status"] == "observing"
    assert _observation(published["id"])["reason"] == "checks_not_rerun"
    fresh = (datetime.now(UTC) + timedelta(seconds=5)).isoformat()
    sink.ci_script = [ci_entry(check_run("body-guard", started_at=fresh))]
    _later(3790)
    assert _observation(published["id"])["status"] == "completed"


@pytest.fixture
def migration_admitted(isolated_migration_db: Any, request: pytest.FixtureRequest) -> Any:
    # The guard under test is 0163. The application refuses to start below
    # 0167, so the client starts at that floor. Downgrade to 0162 still passes
    # through 0163, and 0167 only drops origin when no kickoff row exists.
    isolated_migration_db.at("0167")
    return request.getfixturevalue("admitted")


def test_additive_migration_preserves_existing_failed_execution(migration_admitted: Any) -> None:
    from _migration_support import alembic_config
    from alembic import command

    published = _failed(migration_admitted, 9822)
    before = _snapshot(published["id"])
    config = alembic_config()
    with pytest.raises(RuntimeError, match="factory_ci_observation_records_present"):
        command.downgrade(config, "0162")
    assert _snapshot(published["id"]) == before
    _execute(
        "DELETE FROM curie.factory_ci_observations WHERE request_id = :id", {"id": published["id"]}
    )
    command.downgrade(config, "0162")
    assert _snapshot(published["id"]) == before
    command.upgrade(config, "head")
    assert _snapshot(published["id"]) == before
    assert _rows("SELECT request_id FROM curie.factory_ci_observations") == []


@pytest.mark.parametrize(
    "table", ["work_items", "execution_requests", "thread_publication_lineages"]
)
def test_a_stale_version_cannot_commit_proof_and_the_next_scan_can(
    admitted: Any, table: str
) -> None:
    _client, _github, sink = admitted
    published = _failed(admitted, 9823)
    identifier = {
        "work_items": published["work_item_id"],
        "execution_requests": published["id"],
        "thread_publication_lineages": _observation(published["id"])["lineage_id"],
    }[table]

    def advance() -> None:
        sink.on_ci_read = None
        _execute(
            f"UPDATE curie.{table} SET version = version + 1 WHERE id = :id", {"id": identifier}
        )

    sink.on_ci_read = advance
    sink.ci_script = [ci_green()]
    _later()
    assert _observation(published["id"])["status"] == "observing"
    assert _observation(published["id"])["reason"] == "observation_snapshot_changed"
    _later(3760)
    assert _observation(published["id"])["status"] == "completed"


def test_an_unhealthy_required_base_stays_unverified_until_green(
    admitted: Any,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    from forge_fakes.github import forges_document, forges_env_json, github_repository_binding
    from forge_fakes.github_comments import HEAD_C
    from test_factory_progress import verification

    client, github, sink = admitted
    config = forges_document(
        api_url=get_settings().github_api_url,
        bindings=[
            github_repository_binding(ci={"required": {"key": "python", "paths": ["apps/api"]}})
        ],
    )
    monkeypatch.setenv("CURIE_FORGES", forges_env_json(config, tmp_path / "policy"))
    get_settings.cache_clear()
    sink.ci_script = [ci_entry(check_status=502)]
    published = _published(client, github, sink, 9824)
    _set_base_ref(published["work_item_id"], "main")
    _execute(
        "UPDATE curie.publications SET changed_paths = CAST(:paths AS jsonb) "
        "WHERE execution_request_id = :id",
        {"id": published["id"], "paths": '["apps/api/src/example.py"]'},
    )
    response = verification(
        client,
        published["id"],
        {
            "check": "unit_tests",
            "command": "uv run pytest apps/api/tests -q",
            "outcome": "passed",
            "exit_status": 0,
            "missing_binaries": [],
            "blocked_services": [],
        },
    )
    assert response.status_code == 201, response.text
    with _clock_offset(3700):
        _reconcile()
    assert _terminal(9824) == ("failed", "ci_timeout")
    failed = {**check_run("python", conclusion="failure"), "app": {"slug": "github-actions"}}
    sink.branches["main"] = HEAD_C
    sink.ci_scripts = {HEAD_A: [ci_entry(failed)], HEAD_C: [ci_entry(failed)]}
    _later()
    row = _observation(published["id"])
    assert row["status"] == "observing" and row["reason"] == "required_python_ci_failed_on_base"
    assert "Checks are still being watched until" in _body(sink, published["id"])
    green = {**check_run("python"), "app": {"slug": "github-actions"}}
    sink.ci_scripts = {HEAD_A: [ci_entry(green)], HEAD_C: [ci_entry(green)]}
    _later(3760)
    assert _observation(published["id"])["status"] == "completed"


def test_paginated_statuses_keep_the_newest_failure_until_superseded(admitted: Any) -> None:
    _client, _github, sink = admitted
    published = _failed(admitted, 9825)
    other = tuple({**commit_status(f"external-{i}", "success"), "id": 2000 + i} for i in range(100))
    failure = {
        **commit_status("required", "failure"),
        "id": 10,
        "created_at": "2026-01-01T00:00:00Z",
    }
    sink.ci_script = [ci_entry(statuses=(*other, failure))]
    _later()
    assert (HEAD_A, "status", 2) in sink.ci_pages
    assert _observation(published["id"])["status"] == "observing"
    success = {**failure, "state": "success", "id": 11, "created_at": "2026-01-01T00:01:00Z"}
    sink.ci_script = [ci_entry(statuses=(*other, failure, success))]
    _later(3760)
    assert _observation(published["id"])["status"] == "completed"
