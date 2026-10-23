"""Replay recorded GitHub evidence through the release kind gate for #4863.

Capture provenance: fixtures/kind-ci-history.json, source field, recorded
2026-10-23 from run 39653199666 and its attempt-1 jobs endpoint.
"""

import importlib.util
import json
import os
import shlex
import sqlite3
import subprocess
import sys
import time
from copy import deepcopy
from pathlib import Path
from types import ModuleType
from typing import Any, cast

import pytest

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "release/kind_ci.py"
HISTORY: dict[str, Any] = json.loads(
    (Path(__file__).parent / "fixtures/kind-ci-history.json").read_text()
)
REPOSITORY = cast(str, HISTORY["run"]["repository"]["full_name"])
COMMIT = cast(str, HISTORY["commit"]["sha"])
TREE = cast(str, HISTORY["commit"]["commit"]["tree"]["sha"])
TAG = "v0.0.0-test"
RUN_ID = 7601
RUN_URL = f"https://github.com/{REPOSITORY}/actions/runs/{RUN_ID}"
PREFIX = f"repos/{REPOSITORY}"
RUNS = f"{PREFIX}/actions/workflows/ci.yaml/runs"
DISPATCH = f"{PREFIX}/actions/workflows/ci.yaml/dispatches"
STATUS = f"{PREFIX}/actions/runs/{RUN_ID}"
SHARDS = "s01 s02 s03 s04 s05 s06 s07 s08 s09 s11 s13 s14 s15 s16 s17 s18 s19 s20 s21".split()
REQUIRED = frozenset(
    [
        "Rust convergence regressions (kind)",
        "E2E parity ladder (cluster rung on kind, fake model)",
        "E2E approval resume after worker and store restarts (kind)",
        "E2E released chart upgrade negative control (nil unsafe helpers)",
        "E2E cluster upgrade matrix shards",
        "E2E required",
        *(f"E2E chart runtime regressions (kind, Calico, {leg})" for leg in ("chart", "workload")),
        *(
            f"E2E scripted factory (kind, scripted model, {suite})"
            for suite in ("scenario", "initial-binding", "static-token-rotation", "fresh-install")
        ),
        *(f"Admitted agent runtime regressions (kind, shard {n}/5)" for n in range(1, 6)),
        *(f"E2E cluster upgrade matrix ({shard})" for shard in SHARDS),
    ]
)

GH = r'''#!/usr/bin/env python3
import json, os, sqlite3, sys, time
from urllib.parse import parse_qs, urlsplit
args = sys.argv[1:]
assert args[0] == "api", args
assert os.environ["GH_TOKEN"] == "test-token"
endpoint = next(a for a in args if a.startswith(("repos/", "https://api.github.com/")))
url = urlsplit(endpoint)
path = url.path.removeprefix("/")
query = parse_qs(url.query)
fields = [(args[i], args[i+1]) for i in range(len(args)-1)
          if args[i] in ("-f", "-F", "--field", "--raw-field")]
for _, field in fields:
    key, value = field.split("=", 1)
    query[key] = [value]
method = "POST" if fields else "GET"
for flag in ("--method", "-X"):
    if flag in args:
        method = args[args.index(flag)+1]
page = int(query.get("page", ["1"])[0])
db = sqlite3.connect(os.environ["TEST_GH_DB"])
count = db.execute("SELECT count(*) FROM calls WHERE method=? AND path=? AND page=?",
                   (method, path, page)).fetchone()[0]
db.execute("INSERT INTO calls VALUES (?, ?, ?, ?)", (method, path, page, json.dumps(args)))
db.commit()
rows = db.execute("SELECT body, status, delay FROM replies WHERE method=? AND path=? AND page=? "
                  "ORDER BY rowid", (method, path, page)).fetchall()
assert rows, (method, path, query)
body, status, delay = rows[min(count, len(rows)-1)]
time.sleep(delay)
if "--include" in args or "-i" in args:
    print(f"HTTP/2 {status}\r")
    last = db.execute("SELECT max(page) FROM replies WHERE method=? AND path=?",
                      (method, path)).fetchone()[0]
    if page < last:
        size = query.get("per_page", ["100"])[0]
        print(f'Link: <https://api.github.com/{path}?per_page={size}&page={page+1}>; rel="next"\r')
    print("Content-Type: application/json\r\n\r")
print(body)
if status >= 400:
    print(f"gh: request failed (HTTP {status})", file=sys.stderr)
sys.exit(1 if status >= 400 else 0)
'''


class GitHub:
    def __init__(self, directory: Path) -> None:
        executable = directory / "gh"
        executable.write_text(GH)
        executable.chmod(0o755)
        self.database = directory / "github.sqlite"
        with sqlite3.connect(self.database) as db:
            db.execute("CREATE TABLE replies (method, path, page, body, status, delay)")
            db.execute("CREATE TABLE calls (method, path, page, args)")
        self.env = {
            **os.environ,
            "PATH": f"{directory}:{os.environ['PATH']}",
            "GH_TOKEN": "test-token",
            "GH_REPO": REPOSITORY,
            "GITHUB_SHA": COMMIT,
            "TEST_GH_DB": str(self.database),
        }
        for ref in (TAG, COMMIT):
            self.reply(f"{PREFIX}/commits/{ref}", HISTORY["commit"])
        self.history([HISTORY["run"]])
        self.jobs(HISTORY["run"]["id"], HISTORY["jobs"])
        # https://docs.github.com/en/rest/actions/workflows#create-a-workflow-dispatch-event
        self.reply(DISPATCH, {"workflow_run_id": RUN_ID, "html_url": RUN_URL}, method="POST")
        self.reply(STATUS, {"status": "completed", "conclusion": "success"})

    def reply(
        self, path: str, body: Any, *, method: str = "GET", page: int = 1,
        status: int = 200, append: bool = False, delay: float = 0,
    ) -> None:
        with sqlite3.connect(self.database) as db:
            if not append:
                db.execute(
                    "DELETE FROM replies WHERE method=? AND path=? AND page=?", (method, path, page)
                )
            db.execute(
                "INSERT INTO replies VALUES (?, ?, ?, ?, ?, ?)",
                (method, path, page, json.dumps(body), status, delay),
            )

    def history(
        self, runs: list[dict[str, Any]], *, page: int = 1, total: int | None = None
    ) -> None:
        self.reply(
            RUNS, {"total_count": len(runs) if total is None else total, "workflow_runs": runs},
            page=page,
        )

    def jobs(
        self, run_id: int, jobs: list[dict[str, Any]], *, page: int = 1, total: int | None = None
    ) -> None:
        for suffix in ("jobs", "attempts/1/jobs"):
            self.reply(
                f"{PREFIX}/actions/runs/{run_id}/{suffix}",
                {"total_count": len(jobs) if total is None else total, "jobs": jobs}, page=page,
            )

    def calls(self, method: str | None = None) -> list[tuple[str, list[str]]]:
        with sqlite3.connect(self.database) as db:
            rows = db.execute("SELECT method, path, args FROM calls ORDER BY rowid").fetchall()
        return [(path, json.loads(args)) for verb, path, args in rows if method in (None, verb)]

    def cli(self, command: str, *extra: str) -> subprocess.CompletedProcess[str]:
        args = [
            "python3", str(SCRIPT), command, "--repo", REPOSITORY, "--commit", TAG,
            "--timeout-seconds", "8",
        ]
        if command == "run":
            args += ["--ref", TAG, "--poll-seconds", "0"]
        return subprocess.run(
            [*args, *extra], cwd=ROOT, env=self.env, capture_output=True, text=True, timeout=12
        )


@pytest.fixture
def github(tmp_path: Path) -> GitHub:
    return GitHub(tmp_path)


@pytest.fixture
def kind_ci() -> ModuleType:
    spec = importlib.util.spec_from_file_location("release_kind_ci", SCRIPT)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def reasons(
    module: ModuleType, *, run: dict[str, Any] | None = None,
    jobs: list[dict[str, Any]] | None = None, tree: str = TREE,
) -> list[str]:
    return cast(
        list[str],
        module.qualification_reasons(
            HISTORY["run"] if run is None else run, HISTORY["jobs"] if jobs is None else jobs,
            candidate_tree=tree, target_tree=TREE, required_names=REQUIRED,
        ),
    )


def assert_dispatch(github: GitHub) -> None:
    posts = github.calls("POST")
    assert len(posts) == 1, posts
    path, args = posts[0]
    assert path == DISPATCH
    fields = [
        (args[i], args[i + 1]) for i in range(len(args) - 1)
        if args[i] in ("-f", "-F", "--field", "--raw-field")
    ]
    assert sorted(value for _, value in fields) == sorted([f"ref={TAG}", "return_run_details=true"])
    assert any(
        flag in ("-F", "--field") and value == "return_run_details=true" for flag, value in fields
    )
    assert any(path == STATUS for path, _ in github.calls("GET"))


def test_canonical_required_jobs_ignore_environment_override(
    kind_ci: ModuleType, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("CURIE_E2E_SHARDS_OVERRIDE", "s01 nosetup fresh-n")
    assert kind_ci.required_job_names(ROOT, deadline=time.monotonic() + 8) == REQUIRED
    assert len(REQUIRED) == 36
    assert len(HISTORY["jobs"]) == 80
    assert reasons(kind_ci) == []


@pytest.mark.parametrize(
    "body", [None, "not JSON", "[]", '[{"id":"s01"},{"id":"s01"}]', '[{"id":"invalid"}]']
)
def test_invalid_or_unavailable_shard_listing_fails_closed(
    kind_ci: ModuleType, tmp_path: Path, body: str | None
) -> None:
    if body is not None:
        script = tmp_path / "cli/scripts/cluster-upgrade-matrix.sh"
        script.parent.mkdir(parents=True)
        script.write_text("printf '%s\\n' " + shlex.quote(body) + "\n")
    with pytest.raises((ValueError, RuntimeError, OSError, subprocess.SubprocessError)):
        kind_ci.required_job_names(tmp_path, deadline=time.monotonic() + 8)


@pytest.mark.parametrize("name", sorted(REQUIRED))
@pytest.mark.parametrize(
    "state", ["missing", "skipped", "failure", "cancelled", "neutral", "timed_out",
              "in_progress", "no-conclusion", "conflict"]
)
def test_each_required_job_must_supply_complete_success(
    kind_ci: ModuleType, name: str, state: str
) -> None:
    jobs = deepcopy(HISTORY["jobs"])
    job = next(job for job in jobs if job["name"] == name)
    if state == "missing":
        jobs.remove(job)
    elif state == "in_progress":
        job["status"] = state
    elif state == "no-conclusion":
        job.pop("conclusion")
    elif state == "conflict":
        jobs.append({**job, "conclusion": "failure"})
    else:
        job["conclusion"] = state
    assert reasons(kind_ci, jobs=jobs), (name, state)


@pytest.mark.parametrize("change", [
    {"status": "in_progress"}, {"status": None}, {"conclusion": "failure"},
    {"conclusion": "cancelled"}, {"conclusion": "skipped"}, {"conclusion": None},
    {"path": ".github/workflows/nightly-kind.yaml"}, {"id": None}, {"id": True},
    {"head_sha": None},
])
def test_run_itself_must_be_successful_ci_with_identity(
    kind_ci: ModuleType, change: dict[str, Any]
) -> None:
    assert reasons(kind_ci, run={**HISTORY["run"], **change})


@pytest.mark.parametrize("smoke_only", [True, False])
def test_smoke_or_unresolved_upgrade_matrix_is_insufficient(
    kind_ci: ModuleType, smoke_only: bool
) -> None:
    jobs = [
        job for job in HISTORY["jobs"]
        if not job["name"].startswith("E2E cluster upgrade matrix (")
    ]
    jobs.append({
        "name": "E2E cluster upgrade matrix (s01)" if smoke_only else
        "E2E cluster upgrade matrix (${{ matrix.shard }})",
        "status": "completed", "conclusion": "success" if smoke_only else "skipped",
    })
    assert reasons(kind_ci, jobs=jobs)


@pytest.mark.parametrize("same_sha,same_tree", [(True, True), (False, True), (False, False)])
def test_select_uses_tree_equality_and_never_posts(
    github: GitHub, same_sha: bool, same_tree: bool
) -> None:
    run = {**HISTORY["run"], "head_sha": COMMIT if same_sha else "a" * 40}
    github.history([run])
    github.reply(
        f"{PREFIX}/commits/{run['head_sha']}",
        {"sha": run["head_sha"], "commit": {"tree": {"sha": TREE if same_tree else "b" * 40}}},
    )
    result = github.cli("select")
    assert result.returncode == (0 if same_tree else 3), result.stderr
    assert json.loads(result.stdout) == {
        "selected_run_id": run["id"] if same_tree else None,
        "selected_run_url": run["html_url"] if same_tree else None,
    }
    assert not github.calls("POST")


@pytest.mark.parametrize("late_success", [True, False])
def test_select_follows_run_and_job_pagination(github: GitHub, late_success: bool) -> None:
    github.history(
        [{**HISTORY["run"], "id": n, "conclusion": "failure"} for n in range(1, 101)], total=101
    )
    github.history([HISTORY["run"]], page=2, total=101)
    jobs = deepcopy(HISTORY["jobs"])
    late = jobs.pop(next(i for i, job in enumerate(jobs) if job["name"] == "E2E required"))
    jobs += [
        {"name": f"unrelated {n}", "status": "completed", "conclusion": "skipped"}
        for n in range(100 - len(jobs))
    ]
    github.jobs(HISTORY["run"]["id"], jobs, total=101)
    github.jobs(
        HISTORY["run"]["id"], [{**late, "conclusion": "success" if late_success else "skipped"}],
        page=2, total=101,
    )
    result = github.cli("select")
    assert result.returncode == (0 if late_success else 3), result.stderr
    assert not github.calls("POST")


def test_selector_does_not_combine_incomplete_runs(github: GitHub) -> None:
    runs = [{**HISTORY["run"], "id": n} for n in (1001, 1002)]
    github.history(runs)
    for run, missing in zip(runs, sorted(REQUIRED), strict=False):
        github.jobs(run["id"], [job for job in HISTORY["jobs"] if job["name"] != missing])
    result = github.cli("select")
    assert result.returncode == 3, result.stderr
    assert json.loads(result.stdout)["selected_run_id"] is None
    assert not github.calls("POST")


def test_run_reuses_recorded_proof_and_logs_url_without_dispatch(github: GitHub) -> None:
    result = github.cli("run")
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout) == {
        "mode": "reused", "run_id": HISTORY["run"]["id"], "run_url": HISTORY["run"]["html_url"]
    }
    assert HISTORY["run"]["html_url"] in result.stderr
    assert not github.calls("POST")


@pytest.mark.parametrize("history", [[], [{**HISTORY["run"], "conclusion": "failure"}]])
def test_fallback_dispatches_once_and_recovers_after_502(
    github: GitHub, history: list[dict[str, Any]]
) -> None:
    github.history(history)
    github.reply(STATUS, {"message": "Bad Gateway"}, status=502)
    for state in ("queued", "in_progress", "completed"):
        github.reply(
            STATUS, {"status": state, "conclusion": "success" if state == "completed" else None},
            append=True,
        )
    result = github.cli("run")
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout) == {"mode": "dispatched", "run_id": RUN_ID, "run_url": RUN_URL}
    assert RUN_URL in result.stderr
    assert_dispatch(github)
    assert sum(path == STATUS for path, _ in github.calls("GET")) == 4


@pytest.mark.parametrize("run_id", [None, True, 0, -1, 1.5, "7601"])
def test_invalid_dispatch_id_cannot_be_polled(github: GitHub, run_id: object) -> None:
    github.history([])
    github.reply(DISPATCH, {"workflow_run_id": run_id, "html_url": RUN_URL}, method="POST")
    result = github.cli("run")
    assert result.returncode == 1, result.stderr
    assert len(github.calls("POST")) == 1
    assert not any(path == STATUS for path, _ in github.calls())


@pytest.mark.parametrize("status,body", [
    (200, {"status": "completed", "conclusion": conclusion})
    for conclusion in ("failure", "cancelled", "skipped", "neutral", "timed_out", None)
] + [(401, {"message": "Bad credentials"}), (403, {"message": "Forbidden"})])
def test_unsuccessful_child_or_nonretryable_api_error_keeps_gate_red(
    github: GitHub, status: int, body: dict[str, Any]
) -> None:
    github.history([])
    github.reply(STATUS, body, status=status)
    result = github.cli("run")
    assert result.returncode == 1, result.stderr
    assert_dispatch(github)
    assert sum(path == STATUS for path, _ in github.calls()) == 1


@pytest.mark.parametrize("path,body", [(RUNS, {"workflow_runs": None}),
    (f"{PREFIX}/commits/{TAG}", {"commit": {}}),
    (f"{PREFIX}/actions/runs/{HISTORY['run']['id']}/jobs", {"jobs": "malformed"})])
def test_malformed_required_api_data_is_an_error_without_dispatch(
    github: GitHub, path: str, body: dict[str, Any]
) -> None:
    github.reply(path, body)
    if path.endswith("/jobs"):
        github.reply(path.replace("/jobs", "/attempts/1/jobs"), body)
    result = github.cli("run")
    assert result.returncode == 1, result.stderr
    assert not github.calls("POST")


def test_ambiguous_dispatch_failure_is_not_retried(github: GitHub) -> None:
    github.history([])
    github.reply(DISPATCH, {"message": "Bad Gateway"}, method="POST", status=502)
    result = github.cli("run")
    assert result.returncode == 1, result.stderr
    assert len(github.calls("POST")) == 1
    assert not any(path == STATUS for path, _ in github.calls())


@pytest.mark.parametrize("failure", ["5xx", "active", "slow-read"])
def test_one_deadline_bounds_errors_active_children_and_subprocesses(
    github: GitHub, failure: str
) -> None:
    github.history([])
    if failure == "slow-read":
        github.reply(f"{PREFIX}/commits/{TAG}", HISTORY["commit"], delay=5)
    else:
        github.reply(
            STATUS, {"message": "Unavailable"} if failure == "5xx" else
            {"status": "in_progress", "conclusion": None},
            status=503 if failure == "5xx" else 200,
        )
    started = time.monotonic()
    result = github.cli("run", "--timeout-seconds", "1", "--poll-seconds", "60")
    assert result.returncode == 1, result.stderr
    assert time.monotonic() - started < 3
    if failure == "slow-read":
        assert not github.calls("POST")
    else:
        assert_dispatch(github)


def test_main_exposes_the_same_selector(
    kind_ci: ModuleType, github: GitHub, monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    for key, value in github.env.items():
        monkeypatch.setenv(key, value)
    assert kind_ci.main(
        ["select", "--repo", REPOSITORY, "--commit", TAG, "--timeout-seconds", "8"]
    ) == 0
    assert json.loads(capsys.readouterr().out)["selected_run_id"] == HISTORY["run"]["id"]
