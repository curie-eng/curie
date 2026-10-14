#!/usr/bin/env python3
"""@spec CI-UPGRADE-MATRIX-BUDGET: measure the upgrade matrix wall clock.

Reads a GitHub Actions jobs payload (the documented
``GET /repos/{owner}/{repo}/actions/runs/{run_id}/jobs`` response) and adds the
shared image build job's wall clock to each matrix shard's run step. The images
now build once in a separate job before the shards start.

Always writes the seconds to the job summary. An overrun emits a warning;
untrustworthy measurements still fail. A completed shard can omit its
matrix-run step timestamps for a few seconds after the job finishes
(run 36726083300, attempt 1). The jobs API is fetched again for that miss.
A durable miss still fails.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.request
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal

BUDGET_SECONDS = 20 * 60
SHARD_PREFIX = "E2E cluster upgrade matrix ("
IMAGES_JOB = "Build CI images (no push)"
RUN_STEP = "Run the cluster upgrade matrix"
DEFAULT_API_URL = "https://api.github.com"
API_VERSION = "2022-11-28"
USER_AGENT = "curie-upgrade-matrix-budget/2823"


class BudgetError(Exception):
    """The jobs payload cannot support a budget decision."""


@dataclass(frozen=True)
class ShardTiming:
    name: str
    shard: str
    job_seconds: int
    images_seconds: int
    run_seconds: int | None

    @property
    def shard_plus_images_seconds(self) -> int:
        if self.run_seconds is None:
            raise BudgetError(f"missing matrix-run step timestamps on {self.shard}")
        return self.images_seconds + self.run_seconds


def parse_iso8601(value: object, label: str) -> datetime:
    # Observed GitHub jobs API timestamps use a trailing Z (run 35356180678).
    # https://docs.github.com/en/rest/actions/workflow-jobs#list-jobs-for-a-workflow-run
    if not isinstance(value, str) or not value.strip():
        raise BudgetError(f"missing {label} timestamp")
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        moment = datetime.fromisoformat(text)
    except ValueError as exc:
        raise BudgetError(f"invalid {label} timestamp") from exc
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=UTC)
    return moment


def duration_seconds(start: object, end: object, label: str) -> int:
    started = parse_iso8601(start, f"{label} started_at")
    completed = parse_iso8601(end, f"{label} completed_at")
    elapsed = int((completed - started).total_seconds())
    if elapsed < 0:
        raise BudgetError(f"negative {label} duration")
    return elapsed


def step_seconds(job: Mapping[str, Any], step_name: str) -> int | None:
    steps = job.get("steps")
    if not isinstance(steps, list):
        return None
    for step in steps:
        if not isinstance(step, dict) or step.get("name") != step_name:
            continue
        if step.get("started_at") in (None, "") or step.get("completed_at") in (None, ""):
            return None
        return duration_seconds(step.get("started_at"), step.get("completed_at"), step_name)
    return None


def is_shard_job(name: object) -> bool:
    if not (isinstance(name, str) and name.startswith(SHARD_PREFIX) and name.endswith(")")):
        return False
    shard = name[len(SHARD_PREFIX) : -1]
    # A skipped matrix job keeps the unexpanded GitHub expression as its name.
    return "${{" not in shard and "matrix." not in shard


def shard_id(name: str) -> str:
    return name[len(SHARD_PREFIX) : -1]


def load_jobs(payload: object) -> list[dict[str, Any]]:
    if not isinstance(payload, dict):
        raise BudgetError("jobs payload must be an object")
    jobs = payload.get("jobs")
    if not isinstance(jobs, list):
        raise BudgetError("jobs payload is missing a jobs array")
    total = payload.get("total_count")
    if not isinstance(total, int):
        raise BudgetError("jobs payload is missing total_count")
    if total != len(jobs):
        raise BudgetError(
            f"jobs payload is truncated: total_count={total} but {len(jobs)} jobs were loaded"
        )
    typed: list[dict[str, Any]] = []
    for job in jobs:
        if not isinstance(job, dict):
            raise BudgetError("jobs payload contains a non-object job")
        typed.append(job)
    return typed


def timings_from_jobs(jobs: list[dict[str, Any]]) -> list[ShardTiming]:
    shard_jobs = [job for job in jobs if is_shard_job(job.get("name"))]
    if not shard_jobs:
        raise BudgetError("no upgrade matrix shard jobs")
    images_jobs = [job for job in jobs if job.get("name") == IMAGES_JOB]
    if (
        len(images_jobs) != 1
        or images_jobs[0].get("status") != "completed"
        or images_jobs[0].get("conclusion") != "success"
    ):
        raise BudgetError("expected one successful shared image build job")
    images_job = images_jobs[0]
    images_seconds = duration_seconds(
        images_job.get("started_at"), images_job.get("completed_at"), IMAGES_JOB
    )
    rows: list[ShardTiming] = []
    for job in shard_jobs:
        name = job.get("name")
        assert isinstance(name, str)
        rows.append(
            ShardTiming(
                name=name,
                shard=shard_id(name),
                job_seconds=duration_seconds(job.get("started_at"), job.get("completed_at"), name),
                images_seconds=images_seconds,
                run_seconds=step_seconds(job, RUN_STEP),
            )
        )
    return rows


def format_clock(seconds: int) -> str:
    minutes, remainder = divmod(seconds, 60)
    hours, minutes = divmod(minutes, 60)
    if hours:
        return f"{hours}h{minutes:02d}m{remainder:02d}s"
    return f"{minutes}m{remainder:02d}s"


def optional_seconds(value: int | None) -> str:
    return str(value) if value is not None else "n/a"


def render_summary(rows: list[ShardTiming], longest: ShardTiming, over: bool) -> str:
    lines = [
        "## Upgrade matrix wall clock",
        "",
        f"Budget: {BUDGET_SECONDS} seconds (20 minutes). "
        "The asserted number is the shared image build job plus the matrix run "
        "on the slowest shard (#2823, #2778).",
        "",
        "| Shard | Job seconds | Image build seconds | Matrix run seconds | Build plus run |",
        "| --- | ---: | ---: | ---: | ---: |",
    ]
    for row in sorted(rows, key=lambda item: (-item.shard_plus_images_seconds, item.shard)):
        lines.append(
            f"| {row.shard} | {row.job_seconds} | {row.images_seconds} | "
            f"{optional_seconds(row.run_seconds)} | {row.shard_plus_images_seconds} |"
        )
    result = "over budget" if over else "within budget"
    asserted = longest.shard_plus_images_seconds
    lines.extend(
        [
            "",
            f"Longest image build plus matrix run: {asserted} seconds "
            f"({format_clock(asserted)}) on {longest.shard} "
            f"(shard job {longest.job_seconds}s, image build {longest.images_seconds}s, "
            f"matrix run {optional_seconds(longest.run_seconds)}s).",
            f"Result: {result}",
            "",
        ]
    )
    return "\n".join(lines)


def write_summary(path: Path | None, body: str) -> None:
    if path is None:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(body)
        if not body.endswith("\n"):
            handle.write("\n")


def next_link(headers: Mapping[str, str]) -> str | None:
    # https://docs.github.com/en/rest/using-the-rest-api/using-pagination-in-the-rest-api
    link = headers.get("Link") or headers.get("link")
    if not link:
        return None
    for part in link.split(","):
        if 'rel="next"' not in part and "rel=next" not in part:
            continue
        start = part.find("<")
        end = part.find(">", start + 1)
        if start >= 0 and end > start:
            return part[start + 1 : end].strip()
    return None


def github_headers(token: str) -> dict[str, str]:
    return {
        "Accept": "application/vnd.github+json",
        "Authorization": f"Bearer {token}",
        "X-GitHub-Api-Version": API_VERSION,
        "User-Agent": USER_AGENT,
    }


def fetch_json(url: str, token: str) -> tuple[Any, Mapping[str, str]]:
    request = urllib.request.Request(url, headers=github_headers(token), method="GET")
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            body = response.read()
            headers = {str(key): str(value) for key, value in response.headers.items()}
    except urllib.error.HTTPError as exc:
        raise BudgetError(f"jobs API returned HTTP {exc.code}") from exc
    except urllib.error.URLError as exc:
        raise BudgetError("jobs API request failed") from exc
    try:
        payload = json.loads(body.decode("utf-8"))
    except json.JSONDecodeError as exc:
        raise BudgetError("jobs API returned non-JSON") from exc
    return payload, headers


def fetch_jobs(api_url: str, repository: str, run_id: str, token: str) -> list[dict[str, Any]]:
    url = f"{api_url.rstrip('/')}/repos/{repository}/actions/runs/{run_id}/jobs?per_page=100"
    collected: list[dict[str, Any]] = []
    total: int | None = None
    while url:
        payload, headers = fetch_json(url, token)
        if not isinstance(payload, dict):
            raise BudgetError("jobs API returned a non-object page")
        page_total = payload.get("total_count")
        if not isinstance(page_total, int):
            raise BudgetError("jobs API page is missing total_count")
        if total is None:
            total = page_total
        elif page_total != total:
            raise BudgetError("jobs API pages disagree on total_count")
        jobs = payload.get("jobs")
        if not isinstance(jobs, list):
            raise BudgetError("jobs API page is missing a jobs array")
        for job in jobs:
            if not isinstance(job, dict):
                raise BudgetError("jobs API returned a non-object job")
            collected.append(job)
        url = next_link(headers) or ""
    if total is None or total != len(collected):
        raise BudgetError(
            f"jobs payload is truncated: total_count={total} but {len(collected)} jobs were loaded"
        )
    return collected


def read_jobs_json(path: Path) -> list[dict[str, Any]]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise BudgetError("jobs JSON is malformed") from exc
    return load_jobs(payload)


def emit(message: str, *, kind: Literal["error", "warning", "notice"]) -> None:
    print(f"::{kind} title=Upgrade matrix wall clock::{message}")
    print(message, file=sys.stderr)


def resolve_summary(explicit: str | None) -> Path | None:
    if explicit:
        return Path(explicit)
    env_path = os.environ.get("GITHUB_STEP_SUMMARY")
    if env_path:
        return Path(env_path)
    return None


def _fail(summary_path: Path | None, exc: BudgetError) -> int:
    write_summary(summary_path, f"## Upgrade matrix wall clock\n\nResult: failed ({exc})\n")
    emit(str(exc), kind="error")
    return 1


def load_measured_rows(
    *,
    jobs_json: Path | None,
    api_url: str,
    attempts: int,
    interval_seconds: float,
) -> list[ShardTiming]:
    # --jobs-json is a complete fixture. Retrying it cannot change the bytes.
    # The missing-timestamp error is raised when the run duration is read, not
    # when the job list is parsed, so the retry has to force that read.
    fetches = 1 if jobs_json is not None else attempts
    last_error: BudgetError | None = None
    for attempt in range(fetches):
        try:
            if jobs_json is not None:
                jobs = read_jobs_json(jobs_json)
            else:
                repository = os.environ.get("GITHUB_REPOSITORY", "")
                run_id = os.environ.get("GITHUB_RUN_ID", "")
                token = os.environ.get("GITHUB_TOKEN", "")
                if not repository or not run_id or not token:
                    raise BudgetError(
                        "GITHUB_REPOSITORY, GITHUB_RUN_ID, and GITHUB_TOKEN are "
                        "required without --jobs-json"
                    )
                jobs = fetch_jobs(api_url, repository, run_id, token)
            rows = timings_from_jobs(jobs)
            for row in rows:
                _ = row.shard_plus_images_seconds
        except BudgetError as exc:
            last_error = exc
            retryable = str(exc).startswith("missing matrix-run step timestamps")
            if jobs_json is not None or not retryable or attempt + 1 >= fetches:
                raise
            time.sleep(interval_seconds)
            continue
        return rows
    if last_error is None:
        raise BudgetError("jobs payload could not be measured")
    raise last_error


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--jobs-json", type=Path)
    parser.add_argument("--summary", type=Path)
    parser.add_argument("--api-url", default=os.environ.get("GITHUB_API_URL", DEFAULT_API_URL))
    parser.add_argument("--fetch-attempts", type=int, default=4)
    parser.add_argument("--fetch-interval-seconds", type=float, default=3)
    args = parser.parse_args(argv)
    if args.fetch_attempts < 1:
        print("fetch-attempts must be at least 1", file=sys.stderr)
        return 2

    summary_path = resolve_summary(str(args.summary) if args.summary else None)
    try:
        rows = load_measured_rows(
            jobs_json=args.jobs_json,
            api_url=args.api_url,
            attempts=args.fetch_attempts,
            interval_seconds=args.fetch_interval_seconds,
        )
        longest = max(rows, key=lambda row: (row.shard_plus_images_seconds, row.shard))
        asserted = longest.shard_plus_images_seconds
    except BudgetError as exc:
        return _fail(summary_path, exc)
    over = asserted > BUDGET_SECONDS
    write_summary(summary_path, render_summary(rows, longest, over))
    if over:
        emit(
            (
                f"longest image build plus matrix run {asserted}s on {longest.shard} "
                f"exceeds {BUDGET_SECONDS}s budget"
            ),
            kind="warning",
        )
        return 0
    emit(
        (
            f"longest image build plus matrix run {asserted}s on {longest.shard} "
            f"(budget {BUDGET_SECONDS}s)"
        ),
        kind="notice",
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
