"""Wall-clock budget for the required cluster upgrade matrix (#2823).

The helper is the consumer path the `e2e-required` job will run: it reads a
GitHub Actions jobs payload, writes a job summary, and exits nonzero when the
longest shard job (which includes the image bake) exceeds 20 minutes.

The jobs JSON shape is the documented
`GET /repos/{owner}/{repo}/actions/runs/{run_id}/jobs` response
(https://docs.github.com/en/rest/actions/workflow-jobs#list-jobs-for-a-workflow-run):
`total_count`, `jobs[].name`, `jobs[].started_at`, `jobs[].completed_at`, and
`jobs[].steps[].name` / `started_at` / `completed_at`. Observed on public run
35356180678 (next, 2026-09-18): shard jobs are named
`E2E cluster upgrade matrix (sNN)`, the bake step is
`Build the candidate images locally in parallel`, and the listing job
`E2E cluster upgrade matrix shards` must not count as a shard.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import threading
from datetime import UTC, datetime, timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

import yaml

REPO_ROOT = Path(__file__).resolve().parents[3]
HELPER = REPO_ROOT / "tools" / "e2e-ci-selection" / "assert_upgrade_matrix_budget.py"
WORKFLOW = REPO_ROOT / ".github" / "workflows" / "ci.yaml"

BUDGET_SECONDS = 20 * 60
BAKE_STEP = "Build the candidate images locally in parallel"
RUN_STEP = "Run the cluster upgrade matrix"
# Observed GitHub jobs API timestamps use a trailing Z (run 35356180678).
ORIGIN = datetime(2026, 9, 18, 14, 0, 0, tzinfo=UTC)


def _iso(moment: datetime) -> str:
    return moment.strftime("%Y-%m-%dT%H:%M:%SZ")


def _step(name: str, start: datetime, seconds: int) -> dict[str, Any]:
    end = start + timedelta(seconds=seconds)
    return {
        "name": name,
        "status": "completed",
        "conclusion": "success",
        "number": 1,
        "started_at": _iso(start),
        "completed_at": _iso(end),
    }


def _job(
    name: str,
    *,
    job_seconds: int,
    bake_seconds: int = 90,
    run_seconds: int = 600,
    start: datetime | None = None,
) -> dict[str, Any]:
    started = start or ORIGIN
    bake_start = started + timedelta(seconds=30)
    run_start = bake_start + timedelta(seconds=bake_seconds + 20)
    return {
        "id": abs(hash(name)) % 10_000_000,
        "run_id": 35356180678,
        "name": name,
        "status": "completed",
        "conclusion": "success",
        "started_at": _iso(started),
        "completed_at": _iso(started + timedelta(seconds=job_seconds)),
        "steps": [
            _step(BAKE_STEP, bake_start, bake_seconds),
            _step(RUN_STEP, run_start, run_seconds),
        ],
    }


def _payload(*jobs: dict[str, Any]) -> dict[str, Any]:
    return {"total_count": len(jobs), "jobs": list(jobs)}


def _run(
    tmp_path: Path,
    payload: dict[str, Any] | None = None,
    *,
    extra_args: list[str] | None = None,
    env_updates: dict[str, str] | None = None,
    jobs_path: Path | None = None,
) -> subprocess.CompletedProcess[str]:
    summary = tmp_path / "summary.md"
    command = [sys.executable, str(HELPER), "--summary", str(summary)]
    if payload is not None:
        jobs_file = tmp_path / "jobs.json"
        jobs_file.write_text(json.dumps(payload), encoding="utf-8")
        command.extend(["--jobs-json", str(jobs_file)])
    elif jobs_path is not None:
        command.extend(["--jobs-json", str(jobs_path)])
    if extra_args:
        command.extend(extra_args)
    environment = os.environ.copy()
    environment.pop("GITHUB_STEP_SUMMARY", None)
    environment.pop("GITHUB_TOKEN", None)
    if env_updates:
        environment.update(env_updates)
    return subprocess.run(
        command,
        cwd=tmp_path,
        env=environment,
        capture_output=True,
        text=True,
        check=False,
    )


def _summary(tmp_path: Path) -> str:
    return (tmp_path / "summary.md").read_text(encoding="utf-8")


def test_under_budget_shard_plus_bake_passes_and_writes_the_seconds(tmp_path: Path) -> None:
    # Observed longest shard on run 35356180678 was 1073s, bake 86s.
    payload = _payload(
        _job(
            "E2E cluster upgrade matrix shards",
            job_seconds=6,
            bake_seconds=0,
            run_seconds=1,
        ),
        _job(
            "E2E cluster upgrade matrix (s07)",
            job_seconds=1073,
            bake_seconds=86,
            run_seconds=838,
        ),
        _job(
            "E2E cluster upgrade matrix (s12)",
            job_seconds=569,
            bake_seconds=99,
            run_seconds=340,
        ),
        _job(
            "Python (ruff + mypy + pytest)",
            job_seconds=2400,
            bake_seconds=0,
            run_seconds=1,
        ),
    )
    completed = _run(tmp_path, payload)
    output = completed.stdout + completed.stderr
    assert completed.returncode == 0, output
    summary = _summary(tmp_path)
    assert "1073" in summary
    assert "86" in summary
    assert "924" in summary
    assert "1200" in summary
    assert "s07" in summary
    assert "within budget" in summary.lower()
    assert "E2E cluster upgrade matrix shards" not in summary
    assert "Python" not in summary
    assert "::notice" in completed.stdout


def test_over_budget_shard_plus_bake_fails_and_still_writes_the_seconds(tmp_path: Path) -> None:
    payload = _payload(
        _job(
            "E2E cluster upgrade matrix (s01)",
            job_seconds=900,
            bake_seconds=80,
            run_seconds=700,
        ),
        _job(
            "E2E cluster upgrade matrix (s07)",
            job_seconds=1920,
            bake_seconds=180,
            run_seconds=1500,
        ),
    )
    completed = _run(tmp_path, payload)
    output = completed.stdout + completed.stderr
    assert completed.returncode != 0, output
    summary = _summary(tmp_path)
    assert "1920" in summary
    assert "180" in summary
    assert "1680" in summary
    assert "1200" in summary
    assert "over budget" in summary.lower()
    assert "::error" in completed.stdout


def test_listing_job_alone_is_not_a_shard(tmp_path: Path) -> None:
    payload = _payload(
        _job("E2E cluster upgrade matrix shards", job_seconds=6, bake_seconds=0, run_seconds=1),
    )
    completed = _run(tmp_path, payload)
    assert completed.returncode != 0
    assert "no upgrade matrix shard jobs" in (completed.stdout + completed.stderr).lower()


def test_missing_timestamps_fail_closed(tmp_path: Path) -> None:
    job = _job("E2E cluster upgrade matrix (s01)", job_seconds=900)
    job["completed_at"] = None
    completed = _run(tmp_path, _payload(job))
    assert completed.returncode != 0
    assert "timestamp" in (completed.stdout + completed.stderr).lower()


def test_truncated_jobs_page_fails_closed(tmp_path: Path) -> None:
    payload = _payload(_job("E2E cluster upgrade matrix (s01)", job_seconds=900))
    payload["total_count"] = 47
    completed = _run(tmp_path, payload)
    assert completed.returncode != 0
    assert "truncated" in (completed.stdout + completed.stderr).lower()


def test_exactly_budget_is_within_budget(tmp_path: Path) -> None:
    payload = _payload(
        _job(
            "E2E cluster upgrade matrix (s01)",
            job_seconds=1300,
            bake_seconds=200,
            run_seconds=1000,
        ),
    )
    completed = _run(tmp_path, payload)
    assert completed.returncode == 0, completed.stdout + completed.stderr
    assert "1200" in _summary(tmp_path)
    assert "within budget" in _summary(tmp_path).lower()


def test_one_second_over_budget_fails(tmp_path: Path) -> None:
    payload = _payload(
        _job(
            "E2E cluster upgrade matrix (s01)",
            job_seconds=1300,
            bake_seconds=200,
            run_seconds=1001,
        ),
    )
    completed = _run(tmp_path, payload)
    assert completed.returncode != 0
    assert "over budget" in _summary(tmp_path).lower()


def test_full_job_over_budget_still_passes_when_bake_plus_run_is_inside(
    tmp_path: Path,
) -> None:
    # Observed on PR 2845 after rebase (run 35525521589): s07 job 1259s,
    # bake 299s, matrix-run 827s. Kind/setup jitter must not fail the 20
    # minute bake-plus-run budget.
    payload = _payload(
        _job(
            "E2E cluster upgrade matrix (s07)",
            job_seconds=1259,
            bake_seconds=299,
            run_seconds=827,
        ),
    )
    completed = _run(tmp_path, payload)
    assert completed.returncode == 0, completed.stdout + completed.stderr
    summary = _summary(tmp_path)
    assert "1259" in summary
    assert "1126" in summary
    assert "within budget" in summary.lower()


def test_missing_bake_or_run_step_fails_closed(tmp_path: Path) -> None:
    job = _job("E2E cluster upgrade matrix (s01)", job_seconds=900)
    job["steps"] = [step for step in job["steps"] if step["name"] != BAKE_STEP]
    completed = _run(tmp_path, _payload(job))
    assert completed.returncode != 0
    assert "missing bake or matrix-run" in (completed.stdout + completed.stderr).lower()


def test_fetch_paginates_jobs_and_uses_documented_headers(tmp_path: Path) -> None:
    page1 = _payload(_job("E2E cluster upgrade matrix (s01)", job_seconds=800))
    page1["total_count"] = 2
    page2 = _payload(_job("E2E cluster upgrade matrix (s02)", job_seconds=1100, bake_seconds=95))
    page2["total_count"] = 2
    seen: list[dict[str, str]] = []

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            seen.append(
                {
                    "path": self.path,
                    "accept": self.headers.get("Accept", ""),
                    "authorization": self.headers.get("Authorization", ""),
                    "api_version": self.headers.get("X-GitHub-Api-Version", ""),
                    "method": self.command,
                }
            )
            if "page=2" in self.path:
                body = json.dumps(page2).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return
            body = json.dumps(page1).encode()
            host, port = self.server.server_address
            next_url = f"http://{host}:{port}{self.path}&page=2"
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Link", f'<{next_url}>; rel="next"')
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, format: str, *args: object) -> None:
            del format, args

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        host, port = server.server_address
        completed = _run(
            tmp_path,
            extra_args=["--api-url", f"http://{host}:{port}"],
            env_updates={
                "GITHUB_REPOSITORY": "curie-eng/curie",
                "GITHUB_RUN_ID": "35356180678",
                "GITHUB_TOKEN": "example-token",
            },
        )
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)

    output = completed.stdout + completed.stderr
    assert completed.returncode == 0, output
    assert len(seen) == 2
    first = seen[0]
    assert first["method"] == "GET"
    assert first["accept"] == "application/vnd.github+json"
    assert first["authorization"] == "Bearer example-token"
    assert first["api_version"] == "2022-11-28"
    assert "per_page=100" in first["path"]
    summary = _summary(tmp_path)
    assert "1100" in summary
    assert "s02" in summary
    assert "95" in summary


def test_e2e_required_runs_the_wall_clock_helper() -> None:
    workflow = yaml.safe_load(WORKFLOW.read_text())
    job = workflow["jobs"]["e2e-required"]
    permissions = job.get("permissions")
    assert isinstance(permissions, dict)
    assert permissions.get("contents") == "read"
    assert permissions.get("actions") == "read"
    named = {
        step.get("name"): step
        for step in job["steps"]
        if isinstance(step, dict) and isinstance(step.get("name"), str)
    }
    checkout = next(
        step
        for step in job["steps"]
        if isinstance(step, dict)
        and str(step.get("uses", "")).startswith("actions/checkout@")
    )
    assert checkout["with"]["persist-credentials"] is False
    # Outcome failure must not hide the seconds: a 32 minute shard that also
    # failed a scenario would otherwise skip the annotation (#2823).
    assert checkout.get("if") == "${{ !cancelled() }}"
    assert "Assert upgrade matrix wall clock" in named
    budget = named["Assert upgrade matrix wall clock"]
    run = budget["run"]
    assert "tools/e2e-ci-selection/assert_upgrade_matrix_budget.py" in run
    assert "continue-on-error" not in budget
    assert budget.get("if") == "${{ !cancelled() }}"
    # Outcome matching stays the gate for selected results; wall clock is a
    # later step so a budget miss cannot skip the selected-outcome negative
    # control.
    outcome_index = next(
        i
        for i, step in enumerate(job["steps"])
        if isinstance(step, dict) and step.get("name") == "Require exact selected outcomes"
    )
    budget_index = next(
        i
        for i, step in enumerate(job["steps"])
        if isinstance(step, dict) and step.get("name") == "Assert upgrade matrix wall clock"
    )
    assert outcome_index < budget_index
