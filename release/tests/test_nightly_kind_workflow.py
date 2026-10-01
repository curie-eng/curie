"""Execute the nightly workflow shell with GitHub as the external boundary."""

import json
import os
import re
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any, TypedDict, cast

import pytest
import yaml

WORKFLOW = Path(__file__).resolve().parents[2] / ".github/workflows/nightly-kind.yaml"
REPOSITORY = "curie-eng/curie"
CI_RUN_ID = "7601"
CI_RUN_URL = f"https://github.com/{REPOSITORY}/actions/runs/{CI_RUN_ID}"
NIGHTLY_RUN_URL = f"https://github.com/{REPOSITORY}/actions/runs/9100"


def _workflow() -> dict[str, Any]:
    return cast(
        dict[str, Any], yaml.load(WORKFLOW.read_text(encoding="utf-8"), Loader=yaml.BaseLoader)
    )


class GitHubCall(TypedDict):
    args: list[str]
    repo: str | None


@dataclass
class JobResult:
    returncode: int
    outputs: dict[str, str]
    log: str


@pytest.fixture
def github(tmp_path: Path) -> dict[str, str]:
    executable = tmp_path / "gh"
    executable.write_text(
        """#!/usr/bin/env python3
import json
import os
import sys

args = sys.argv[1:]
with open(os.environ["TEST_GH_CALLS"], "a", encoding="utf-8") as calls:
    calls.write(json.dumps({"args": args, "repo": os.environ.get("GH_REPO")}) + "\\n")
if args[:1] == ["api"]:
    print(os.environ["TEST_DISPATCH_RESPONSE"])
    sys.exit(int(os.environ.get("TEST_DISPATCH_EXIT", "0")))
if args[:2] == ["run", "watch"]:
    print(os.environ.get("TEST_WATCH_RESULT", "success"))
    sys.exit(int(os.environ.get("TEST_WATCH_EXIT", "0")))
if args[:2] == ["issue", "create"]:
    sys.exit(int(os.environ.get("TEST_ISSUE_EXIT", "0")))
if args[:2] == ["workflow", "run"]:
    print("Dispatched without observing the result")
    sys.exit(0)
raise SystemExit("Unexpected GitHub command: " + repr(args))
""",
        encoding="utf-8",
    )
    executable.chmod(0o755)
    environment = {
        key: value for key, value in os.environ.items() if key not in {"GH_REPO", "GH_TOKEN"}
    }
    return {
        **environment,
        "PATH": f"{tmp_path}:{environment['PATH']}",
        "TEST_GH_CALLS": str(tmp_path / "calls.jsonl"),
        # GitHub documents these response fields when return_run_details is true:
        # https://docs.github.com/en/rest/actions/workflows#create-a-workflow-dispatch-event
        "TEST_DISPATCH_RESPONSE": json.dumps(
            {"workflow_run_id": int(CI_RUN_ID), "html_url": CI_RUN_URL}
        ),
    }


def _run_job(
    name: str,
    tmp_path: Path,
    environment: dict[str, str],
    needs_result: str = "failure",
    needs_outputs: dict[str, str] | None = None,
    verify_result: str = "skipped",
) -> JobResult:
    job = _workflow()["jobs"][name]
    context = {
        "github.repository": REPOSITORY,
        "github.server_url": "https://github.com",
        "github.run_id": "9100",
        "secrets.GITHUB_TOKEN": "test-token",
        "needs.dispatch.result": needs_result,
        "needs.verify.result": verify_result,
        **{f"needs.dispatch.outputs.{key}": value for key, value in (needs_outputs or {}).items()},
    }

    def render(value: str) -> str:
        return re.sub(r"\$\{\{\s*(.*?)\s*\}\}", lambda match: context.get(match[1], ""), value)

    log = ""
    returncode = 0
    for index, step in enumerate(job["steps"]):
        output = tmp_path / f"{name}.{index}.output"
        output.write_text("", encoding="utf-8")
        result = subprocess.run(
            ["bash", "--noprofile", "--norc", "-e", "-o", "pipefail", "-c", step["run"]],
            cwd=tmp_path,
            env={
                **environment,
                **{key: render(value) for key, value in job.get("env", {}).items()},
                **{key: render(value) for key, value in step.get("env", {}).items()},
                "GITHUB_OUTPUT": str(output),
            },
            capture_output=True,
            text=True,
            timeout=10,
        )
        log += result.stdout + result.stderr
        for line in output.read_text(encoding="utf-8").splitlines():
            key, separator, value = line.partition("=")
            assert separator, line
            context[f"steps.{step.get('id')}.outputs.{key}"] = value
        returncode = result.returncode
        if returncode:
            break
    return JobResult(
        returncode,
        {key: render(value) for key, value in job.get("outputs", {}).items()},
        log,
    )


def _calls(environment: dict[str, str], command: list[str]) -> list[GitHubCall]:
    path = Path(environment["TEST_GH_CALLS"])
    return [
        call
        for line in path.read_text(encoding="utf-8").splitlines()
        if (call := cast(GitHubCall, json.loads(line)))["args"][: len(command)] == command
    ]


def _issue_body(environment: dict[str, str]) -> str:
    issues = _calls(environment, ["issue", "create"])
    assert len(issues) == 1
    arguments = issues[0]["args"]
    assert arguments[arguments.index("--repo") + 1] == REPOSITORY
    return arguments[arguments.index("--body") + 1]


def test_nightly_tracks_the_exact_dispatched_ci_run(tmp_path: Path, github: dict[str, str]) -> None:
    result = _run_job("dispatch", tmp_path, github)

    assert result.returncode == 0, result.log
    dispatches = _calls(github, ["api"])
    assert len(dispatches) == 1
    arguments = dispatches[0]["args"]
    assert arguments[arguments.index("--method") + 1] == "POST"
    assert f"repos/{REPOSITORY}/actions/workflows/ci.yaml/dispatches" in arguments
    assert arguments[arguments.index("-f") + 1] == "ref=next"
    assert arguments[arguments.index("-F") + 1] == "return_run_details=true"
    assert dispatches[0]["repo"] == REPOSITORY
    verify = _run_job("verify", tmp_path, github, needs_outputs=result.outputs)
    assert verify.returncode == 0, verify.log
    watches = _calls(github, ["run", "watch"])
    assert [call["args"] for call in watches] == [["run", "watch", CI_RUN_ID, "--exit-status"]]
    assert result.outputs == {"run_id": CI_RUN_ID, "run_url": CI_RUN_URL}
    assert not _calls(github, ["run", "list"])
    assert not _calls(github, ["issue", "create"])


@pytest.mark.parametrize(
    "conclusion,exit_code", [("failure", "1"), ("cancelled", "1"), ("watch error", "8")]
)
def test_failed_ci_keeps_nightly_red_and_files_the_exact_run(
    tmp_path: Path, github: dict[str, str], conclusion: str, exit_code: str
) -> None:
    github.update(TEST_WATCH_RESULT=conclusion, TEST_WATCH_EXIT=exit_code)
    dispatch = _run_job("dispatch", tmp_path, github)
    assert dispatch.returncode == 0, dispatch.log
    verify = _run_job("verify", tmp_path, github, needs_outputs=dispatch.outputs)
    assert verify.returncode != 0, verify.log

    report = _run_job(
        "report",
        tmp_path,
        github,
        needs_result="success",
        needs_outputs=dispatch.outputs,
        verify_result="failure",
    )
    assert report.returncode == 0, report.log
    body = _issue_body(github)
    assert CI_RUN_URL in body
    assert NIGHTLY_RUN_URL in body
    assert verify.returncode != 0


@pytest.mark.parametrize("run_id", [None, "", "unrelated", 0])
def test_missing_or_invalid_run_id_fails_before_watching(
    tmp_path: Path, github: dict[str, str], run_id: str | int | None
) -> None:
    github["TEST_DISPATCH_RESPONSE"] = json.dumps(
        {"workflow_run_id": run_id, "html_url": CI_RUN_URL}
    )
    dispatch = _run_job("dispatch", tmp_path, github)
    assert dispatch.returncode != 0, dispatch.log
    assert not _calls(github, ["run", "watch"])

    report = _run_job("report", tmp_path, github, needs_outputs=dispatch.outputs)
    assert report.returncode == 0, report.log
    assert NIGHTLY_RUN_URL in _issue_body(github)


@pytest.mark.parametrize("response", ["", "not JSON", "{}"])
def test_dispatch_failure_files_an_issue_without_guessing_a_ci_run(
    tmp_path: Path, github: dict[str, str], response: str
) -> None:
    github.update(TEST_DISPATCH_EXIT="27", TEST_DISPATCH_RESPONSE=response)
    dispatch = _run_job("dispatch", tmp_path, github)
    assert dispatch.returncode != 0, dispatch.log
    assert not _calls(github, ["run", "watch"])

    report = _run_job("report", tmp_path, github, needs_outputs=dispatch.outputs)
    assert report.returncode == 0, report.log
    body = _issue_body(github)
    assert NIGHTLY_RUN_URL in body
    assert CI_RUN_URL not in body


def test_issue_filing_failure_does_not_turn_the_nightly_green(
    tmp_path: Path, github: dict[str, str]
) -> None:
    github.update(TEST_WATCH_EXIT="1", TEST_ISSUE_EXIT="9")
    dispatch = _run_job("dispatch", tmp_path, github)
    assert dispatch.returncode == 0, dispatch.log
    verify = _run_job("verify", tmp_path, github, needs_outputs=dispatch.outputs)
    assert verify.returncode != 0, verify.log
    report = _run_job(
        "report",
        tmp_path,
        github,
        needs_result="success",
        needs_outputs=dispatch.outputs,
        verify_result="failure",
    )
    assert report.returncode != 0, report.log
    assert CI_RUN_URL in _issue_body(github)


@pytest.mark.parametrize(
    "dispatch_result,verify_result",
    [
        ("failure", "skipped"),
        ("cancelled", "skipped"),
        ("success", "failure"),
        ("success", "cancelled"),
    ],
)
def test_separate_report_job_can_report_timeout_or_cancellation(
    tmp_path: Path, github: dict[str, str], dispatch_result: str, verify_result: str
) -> None:
    outputs = {}
    if dispatch_result == "success":
        dispatch = _run_job("dispatch", tmp_path, github)
        assert dispatch.returncode == 0, dispatch.log
        outputs = dispatch.outputs
    report = _run_job(
        "report",
        tmp_path,
        github,
        needs_result=dispatch_result,
        needs_outputs=outputs,
        verify_result=verify_result,
    )
    assert report.returncode == 0, report.log
    body = _issue_body(github)
    if dispatch_result == "success":
        assert CI_RUN_URL in body
    else:
        assert CI_RUN_URL not in body
    assert f"Dispatch result: {dispatch_result}" in body
    assert f"Verification result: {verify_result}" in body


def test_reporting_is_independent_bounded_and_cannot_swallow_dispatch_failure() -> None:
    workflow = _workflow()
    dispatch = workflow["jobs"]["dispatch"]
    verify = workflow["jobs"]["verify"]
    report = workflow["jobs"]["report"]
    assert verify["needs"] == "dispatch"
    assert set(report["needs"]) == {"dispatch", "verify"}
    assert "always()" in report["if"]
    assert "needs.dispatch.result != 'success'" in report["if"]
    assert "needs.verify.result != 'success'" in report["if"]
    assert (workflow.get("permissions") or {}).get("issues") != "write"
    assert report["permissions"]["issues"] == "write"
    assert dispatch["permissions"].get("issues") != "write"
    assert verify["permissions"].get("issues") != "write"
    assert sum(int(job["timeout-minutes"]) for job in (dispatch, verify, report)) < 24 * 60
    for job in (dispatch, verify, report):
        assert 0 < int(job["timeout-minutes"]) < 24 * 60
        assert job.get("continue-on-error", "false") == "false"
        assert all(step.get("continue-on-error", "false") == "false" for step in job["steps"])
        assert all("uses" not in step for step in job["steps"])
